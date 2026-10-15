package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slog"

	"github.com/tus/lockfile"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

// --- test doubles ---------------------------------------------------------

type fakeUpload struct {
	info      handler.FileInfo
	content   string
	readerErr error
	readCalls int
}

func (u *fakeUpload) WriteChunk(_ context.Context, _ int64, _ io.Reader) (int64, error) {
	return 0, nil
}

func (u *fakeUpload) GetInfo(_ context.Context) (handler.FileInfo, error) {
	return u.info, nil
}

func (u *fakeUpload) GetReader(_ context.Context) (io.ReadCloser, error) {
	u.readCalls++
	if u.readerErr != nil {
		return nil, u.readerErr
	}
	return io.NopCloser(strings.NewReader(u.content)), nil
}

func (u *fakeUpload) FinishUpload(_ context.Context) error { return nil }

type fakeStore struct {
	uploads         map[string]*fakeUpload
	getCalls        map[string]int
	quarantineCalls map[string]int
	quarantineErr   map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		uploads:         map[string]*fakeUpload{},
		getCalls:        map[string]int{},
		quarantineCalls: map[string]int{},
		quarantineErr:   map[string]error{},
	}
}

func (s *fakeStore) GetUpload(_ context.Context, id string) (handler.Upload, error) {
	s.getCalls[id]++
	upload, ok := s.uploads[id]
	if !ok {
		return nil, handler.ErrNotFound
	}
	return upload, nil
}

func (s *fakeStore) QuarantineUpload(_ context.Context, id, _ string) error {
	s.quarantineCalls[id]++
	if err := s.quarantineErr[id]; err != nil {
		return err
	}
	return nil
}

type failingLedger struct {
	err error
}

func (l *failingLedger) Append(Event) error { return l.err }
func (l *failingLedger) Close() error       { return nil }

// --- helpers --------------------------------------------------------------

func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func finishedUpload(content string) *fakeUpload {
	return &fakeUpload{
		content: content,
		info: handler.FileInfo{
			Size:   int64(len(content)),
			Offset: int64(len(content)),
		},
	}
}

func writeManifestFile(t *testing.T, dir string, entries []Entry) string {
	t.Helper()
	path := filepath.Join(dir, "manifest.json")
	data, err := json.Marshal(jsonManifest{Entries: entries})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0664))
	return path
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestVerifier(t *testing.T, store Store, locker handler.Locker, manifestPath, ledgerPath, quarantineDir string) *Verifier {
	t.Helper()
	v, err := New(Config{
		ManifestPath:  manifestPath,
		LedgerPath:    ledgerPath,
		QuarantineDir: quarantineDir,
		Store:         store,
		Locker:        locker,
		LockTimeout:   2 * time.Second,
		Logger:        quietLogger(),
	})
	require.NoError(t, err)
	return v
}

func entryFor(id, content string) Entry {
	return Entry{ID: id, Size: int64(len(content)), Digest: sha256Hex(content)}
}

// --- TR-5.1: full decision matrix ----------------------------------------

func TestVerifierDecisionMatrix(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["ok"] = finishedUpload("hello")
	store.uploads["size-bad"] = finishedUpload("hello world")
	store.uploads["digest-bad"] = finishedUpload("HELLO")
	store.uploads["unfinished"] = &fakeUpload{
		content: "hello",
		info:    handler.FileInfo{Size: 11, Offset: 5},
	}
	store.uploads["read-error"] = finishedUpload("hello")
	store.uploads["read-error"].readerErr = errors.New("disk read failure")
	// "missing" is intentionally absent from the store.

	entries := []Entry{
		entryFor("ok", "hello"),
		{ID: "size-bad", Size: 5, Digest: sha256Hex("hello")},
		{ID: "digest-bad", Size: 5, Digest: sha256Hex("hello")},
		entryFor("missing", "missing-content"),
		entryFor("unfinished", "hello world"),
		entryFor("read-error", "hello"),
	}
	manifestPath := writeManifestFile(t, tmp, entries)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v := newTestVerifier(t, store, nil, manifestPath, ledgerPath, quarantineDir)
	report, status, err := v.Run(context.Background())
	require.NoError(t, err)

	a.Equal(6, report.Total)
	a.Equal(1, report.Verified)
	a.Equal(2, report.Quarantined)
	a.Equal(1, report.Missing)
	a.Equal(1, report.Unfinished)
	a.Equal(1, report.Errors)
	a.Equal(0, report.Skipped)
	a.NotEmpty(report.RunID)

	// Exactly the two mismatching uploads are moved into quarantine.
	a.Equal(map[string]int{"size-bad": 1, "digest-bad": 1}, store.quarantineCalls)
	a.Empty(store.quarantineCalls["ok"])

	event, ok := status.Status("ok")
	a.True(ok)
	a.Equal(StatusVerified, event.Status)
	a.EqualValues(5, event.ActualSize)
	a.Equal(sha256Hex("hello"), event.ActualDigest)

	sizeEvent, ok := status.Status("size-bad")
	a.True(ok)
	a.Equal(StatusQuarantined, sizeEvent.Status)
	a.EqualValues(11, sizeEvent.ActualSize)
	a.Empty(sizeEvent.ActualDigest, "hashing must be skipped on size mismatch")
	a.Contains(sizeEvent.Detail, "size mismatch")

	digestEvent, ok := status.Status("digest-bad")
	a.True(ok)
	a.Equal(StatusQuarantined, digestEvent.Status)
	a.EqualValues(5, digestEvent.ActualSize)
	a.Equal(sha256Hex("HELLO"), digestEvent.ActualDigest)

	missingEvent, ok := status.Status("missing")
	a.True(ok)
	a.Equal(StatusMissing, missingEvent.Status)

	unfinishedEvent, ok := status.Status("unfinished")
	a.True(ok)
	a.Equal(StatusUnfinished, unfinishedEvent.Status)
	a.EqualValues(5, unfinishedEvent.ActualSize)

	errorEvent, ok := status.Status("read-error")
	a.True(ok)
	a.Equal(StatusError, errorEvent.Status)
	a.Contains(errorEvent.Detail, "disk read failure")

	// All events share the run id.
	for _, id := range []string{"ok", "size-bad", "digest-bad", "missing", "unfinished", "read-error"} {
		e, _ := status.Status(id)
		a.Equal(report.RunID, e.RunID)
	}
}

// TestVerifierEmptyUpload verifies that a finished zero-byte upload matches
// the SHA-256 digest of the empty string and is not falsely quarantined.
func TestVerifierEmptyUpload(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["empty"] = &fakeUpload{
		content: "",
		info:    handler.FileInfo{Size: 0, Offset: 0},
	}

	entries := []Entry{entryFor("empty", "")}
	manifestPath := writeManifestFile(t, tmp, entries)
	v := newTestVerifier(t, store, nil, manifestPath,
		filepath.Join(tmp, ".verification", "ledger.jsonl"),
		filepath.Join(tmp, ".quarantine"))

	report, status, err := v.Run(context.Background())
	require.NoError(t, err)
	a.Equal(1, report.Verified)
	a.Equal(0, report.Quarantined)

	event, ok := status.Status("empty")
	a.True(ok)
	a.Equal(StatusVerified, event.Status)
	// SHA-256 of the empty string.
	a.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", event.ActualDigest)
	a.Empty(store.quarantineCalls)
}

// --- TR-5.2: safety order on ledger / quarantine failures ----------------

func TestVerifierAbortsWhenLedgerFails(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["bad"] = finishedUpload("hello world")

	entries := []Entry{{ID: "bad", Size: 5, Digest: sha256Hex("hello")}}
	manifestPath := writeManifestFile(t, tmp, entries)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")

	v := newTestVerifier(t, store, nil, manifestPath, ledgerPath, filepath.Join(tmp, ".quarantine"))
	v.openLedger = func(_ string) (ledgerAppender, error) {
		return &failingLedger{err: errors.New("ledger unavailable")}, nil
	}

	report, _, err := v.Run(context.Background())
	require.Error(t, err)
	a.Contains(err.Error(), "ledger write failure")

	// The quarantine move must never happen without a durable event.
	a.Empty(store.quarantineCalls)
	a.Equal(0, report.Quarantined)
}

func TestVerifierAbortsWhenQuarantineMoveFails(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["bad"] = finishedUpload("hello world")
	store.uploads["later"] = finishedUpload("later")
	store.quarantineErr["bad"] = errors.New("rename failed")

	entries := []Entry{
		{ID: "bad", Size: 5, Digest: sha256Hex("hello")},
		entryFor("later", "later"),
	}
	manifestPath := writeManifestFile(t, tmp, entries)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")

	v := newTestVerifier(t, store, nil, manifestPath, ledgerPath, filepath.Join(tmp, ".quarantine"))
	report, status, err := v.Run(context.Background())
	require.Error(t, err)
	a.Contains(err.Error(), "failed to quarantine")

	// The decision was persisted before the failed move.
	event, ok := status.Status("bad")
	a.True(ok)
	a.Equal(StatusQuarantined, event.Status)
	a.Equal(1, store.quarantineCalls["bad"])

	// The run aborts immediately: "later" is never processed.
	a.Equal(0, store.getCalls["later"])
	_, ok = status.Status("later")
	a.False(ok)
	a.Equal(0, report.Verified)
}

// --- TR-5.3: manifest failure has zero side effects -----------------------

func TestVerifierManifestFailureNoSideEffects(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["ok"] = finishedUpload("hello")

	// Invalid manifest (bad digest).
	manifestPath := writeManifestFile(t, tmp, []Entry{{ID: "ok", Size: 5, Digest: "not-a-digest"}})
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v := newTestVerifier(t, store, nil, manifestPath, ledgerPath, quarantineDir)
	_, _, err := v.Run(context.Background())
	require.Error(t, err)

	// No ledger directory, no quarantine directory, nothing moved.
	_, err = os.Stat(filepath.Join(tmp, ".verification"))
	a.True(os.IsNotExist(err))
	_, err = os.Stat(quarantineDir)
	a.True(os.IsNotExist(err))
	a.Empty(store.quarantineCalls)

	// A manifest file that does not exist at all has the same behavior.
	v2 := newTestVerifier(t, store, nil, filepath.Join(tmp, "nope.json"), ledgerPath, quarantineDir)
	_, _, err = v2.Run(context.Background())
	a.Error(err)
	_, err = os.Stat(filepath.Join(tmp, ".verification"))
	a.True(os.IsNotExist(err))
}

// --- TR-5.4: incremental rerun --------------------------------------------

func TestVerifierIncrementalRerun(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["A"] = finishedUpload("hello")
	store.uploads["B"] = finishedUpload("hello")

	// Run 1: A and B verify against "hello", D is missing.
	entries1 := []Entry{
		entryFor("A", "hello"),
		entryFor("B", "hello"),
		entryFor("D", "ghost"),
	}
	manifest1 := writeManifestFile(t, tmp, entries1)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v := newTestVerifier(t, store, nil, manifest1, ledgerPath, quarantineDir)
	report1, _, err := v.Run(context.Background())
	require.NoError(t, err)
	a.Equal(2, report1.Verified)
	a.Equal(1, report1.Missing)

	eventsAfterRun1 := countLedgerLines(t, ledgerPath)
	a.Equal(3, eventsAfterRun1)

	// Run 2: A unchanged, B changed (now mismatches), C new, D still missing.
	store.uploads["C"] = finishedUpload("new")
	entries2 := []Entry{
		entryFor("A", "hello"),
		{ID: "B", Size: 5, Digest: sha256Hex("HELLO")},
		entryFor("C", "new"),
		entryFor("D", "ghost"),
	}
	manifest2 := writeManifestFile(t, tmp, entries2)

	getCallsARun2 := store.getCalls["A"]
	v2 := newTestVerifier(t, store, nil, manifest2, ledgerPath, quarantineDir)
	report2, status, err := v2.Run(context.Background())
	require.NoError(t, err)

	a.Equal(1, report2.Skipped, "unchanged verified A is skipped")
	a.Equal(1, report2.Quarantined, "changed B is re-checked and quarantined")
	a.Equal(1, report2.Verified, "new C is verified")
	a.Equal(1, report2.Missing, "previously missing D is retried")

	// A is never opened during run 2.
	a.Equal(getCallsARun2, store.getCalls["A"], "unchanged entry must not be read")
	// B, C and D are processed.
	a.Greater(store.getCalls["B"], 1)
	a.Equal(1, store.getCalls["C"])
	a.Greater(store.getCalls["D"], 1)

	// Exactly three new events (B, C, D), none for A.
	a.Equal(6, countLedgerLines(t, ledgerPath))
	eventA, ok := status.Status("A")
	require.True(t, ok)
	a.Equal(report1.RunID, eventA.RunID, "A must keep its run-1 event")
	a.Equal(report2.RunID, status.events["B"].RunID)
}

// --- TR-5.5: sticky quarantine --------------------------------------------

func TestVerifierStickyQuarantine(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["X"] = finishedUpload("hello")

	// Run 1: manifest disagrees with the stored content.
	entries1 := []Entry{{ID: "X", Size: 5, Digest: sha256Hex("other")}}
	manifest1 := writeManifestFile(t, tmp, entries1)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v := newTestVerifier(t, store, nil, manifest1, ledgerPath, quarantineDir)
	_, status, err := v.Run(context.Background())
	require.NoError(t, err)
	a.True(status.IsQuarantined("X"))
	a.Equal(1, store.quarantineCalls["X"])

	getCallsAfterRun1 := store.getCalls["X"]
	linesAfterRun1 := countLedgerLines(t, ledgerPath)

	// Run 2: the manifest now matches the actual content perfectly.
	entries2 := []Entry{entryFor("X", "hello")}
	manifest2 := writeManifestFile(t, tmp, entries2)
	v2 := newTestVerifier(t, store, nil, manifest2, ledgerPath, quarantineDir)
	report2, status2, err := v2.Run(context.Background())
	require.NoError(t, err)

	a.Equal(1, report2.Skipped)
	a.Equal(getCallsAfterRun1, store.getCalls["X"], "quarantined upload must not be re-opened")
	a.Equal(linesAfterRun1, countLedgerLines(t, ledgerPath), "no new event for quarantined upload")
	a.Equal(1, store.quarantineCalls["X"], "quarantine is never reversed")
	a.True(status2.IsQuarantined("X"))
	blocked, err := status2.DownloadBlocked(context.Background(), "X")
	a.NoError(err)
	a.True(blocked)
}

// --- TR-5.6: restart equivalence via ledger replay ------------------------

func TestVerifierRestartEquivalence(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	store := newFakeStore()
	store.uploads["A"] = finishedUpload("hello")
	store.uploads["B"] = finishedUpload("corrupt")

	entries := []Entry{
		entryFor("A", "hello"),
		{ID: "B", Size: 7, Digest: sha256Hex("different")},
	}
	manifestPath := writeManifestFile(t, tmp, entries)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v := newTestVerifier(t, store, nil, manifestPath, ledgerPath, quarantineDir)
	_, status, err := v.Run(context.Background())
	require.NoError(t, err)
	a.True(status.IsQuarantined("B"))

	// Simulate a restart: build a fresh verifier over the same directories
	// with an unchanged manifest.
	v2 := newTestVerifier(t, store, nil, manifestPath, ledgerPath, quarantineDir)
	report2, status2, err := v2.Run(context.Background())
	require.NoError(t, err)

	a.Equal(2, report2.Skipped)
	a.Equal(0, report2.Verified)
	a.Equal(0, report2.Quarantined)
	a.Equal(2, countLedgerLines(t, ledgerPath), "no duplicate events")
	a.Equal(1, store.quarantineCalls["B"], "no duplicate moves")

	a.True(status2.IsQuarantined("B"))
	blocked, err := status2.DownloadBlocked(context.Background(), "B")
	a.NoError(err)
	a.True(blocked)
	blocked, err = status2.DownloadBlocked(context.Background(), "A")
	a.NoError(err)
	a.False(blocked)
}

// --- TR-5.7: locking against concurrent access ----------------------------

func TestVerifierLockTimeoutRecordsError(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	// Use a real file store and file locker so flock semantics apply.
	store := filestore.New(tmp)
	ctx := context.Background()

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: "locked-id", Size: int64(len("hello"))})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader("hello"))
	require.NoError(t, err)

	// Hold the lock externally for the whole test.
	external := lockfile.Lockfile(filepath.Join(tmp, "locked-id.lock"))
	require.NoError(t, external.TryLock())
	defer external.Unlock()

	locker := filelocker.New(tmp)
	locker.AcquirerPollInterval = 5 * time.Millisecond
	locker.HolderPollInterval = 5 * time.Millisecond

	entries := []Entry{entryFor("locked-id", "hello")}
	manifestPath := writeManifestFile(t, tmp, entries)
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(tmp, ".quarantine")

	v, err := New(Config{
		ManifestPath:  manifestPath,
		LedgerPath:    ledgerPath,
		QuarantineDir: quarantineDir,
		Store:         store,
		Locker:        locker,
		LockTimeout:   100 * time.Millisecond,
		Logger:        quietLogger(),
	})
	require.NoError(t, err)

	report, status, err := v.Run(context.Background())
	require.NoError(t, err)
	a.Equal(1, report.Errors)
	event, ok := status.Status("locked-id")
	a.True(ok)
	a.Equal(StatusError, event.Status)
	a.Contains(event.Detail, "lock")
}

// --- misc ------------------------------------------------------------------

func TestNewVerifierValidatesConfig(t *testing.T) {
	a := assert.New(t)
	store := newFakeStore()

	base := func() Config {
		return Config{
			ManifestPath:  "manifest.json",
			LedgerPath:    "ledger.jsonl",
			QuarantineDir: ".quarantine",
			Store:         store,
		}
	}

	cfg := base()
	cfg.ManifestPath = ""
	_, err := New(cfg)
	a.Error(err)

	cfg = base()
	cfg.LedgerPath = ""
	_, err = New(cfg)
	a.Error(err)

	cfg = base()
	cfg.QuarantineDir = ""
	_, err = New(cfg)
	a.Error(err)

	cfg = base()
	cfg.Store = nil
	_, err = New(cfg)
	a.Error(err)

	v, err := New(base())
	require.NoError(t, err)
	a.Equal(defaultLockTimeout, v.config.LockTimeout)
	a.NotNil(v.config.Logger)
	a.NotNil(v.openLedger)
}

func countLedgerLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	if len(data) == 0 {
		return 0
	}
	return strings.Count(string(data), "\n")
}
