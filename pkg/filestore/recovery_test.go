package filestore

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tus/lockfile"
	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/handler"
)

// ---------- helpers ----------

func testRecovery(t *testing.T, dir, ledgerPath string) Recovery {
	t.Helper()
	r := NewRecovery(New(dir), ledgerPath)
	r.Logger = log.New(io.Discard, "", 0)
	return r
}

// hookEnvelope mirrors the JSON body tusd sends to hook endpoints.
type hookEnvelope struct {
	Type  string `json:"Type"`
	Event struct {
		Upload handler.FileInfo `json:"Upload"`
	} `json:"Event"`
}

func receiptLine(t *testing.T, hookType string, info handler.FileInfo) string {
	t.Helper()
	var env hookEnvelope
	env.Type = hookType
	env.Event.Upload = info
	data, err := json.Marshal(env)
	require.NoError(t, err)
	return string(data)
}

func bareInfoLine(t *testing.T, info handler.FileInfo) string {
	t.Helper()
	data, err := json.Marshal(info)
	require.NoError(t, err)
	return string(data)
}

func writeLedger(t *testing.T, path string, lines ...string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o664))
}

// writeBin writes upload binary data at the default on-disk location.
func writeBin(t *testing.T, dir, id, content string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(id))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o775))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o664))
}

func readInfoFile(t *testing.T, dir, id string) handler.FileInfo {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(id)+".info"))
	require.NoError(t, err)
	var info handler.FileInfo
	require.NoError(t, json.Unmarshal(data, &info))
	return info
}

func markerPath(dir, id string) string {
	return filepath.Join(dir, defaultRecoveryStateDirName, mismatchesDirName, markerID(id)+".json")
}

func readMarker(t *testing.T, dir, id string) MismatchMarker {
	t.Helper()
	data, err := os.ReadFile(markerPath(dir, id))
	require.NoError(t, err)
	var marker MismatchMarker
	require.NoError(t, json.Unmarshal(data, &marker))
	return marker
}

func quarantineEntries(t *testing.T, dir string) []fs.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, defaultRecoveryStateDirName, quarantineDirName))
	require.NoError(t, err)
	return entries
}

// ---------- ledger parsing ----------

func TestReadReceiptLedger(t *testing.T) {
	a := assert.New(t)

	ledger := strings.Join([]string{
		"",
		"# a comment",
		receiptLine(t, "post-create", handler.FileInfo{ID: "a", Size: 10}),
		bareInfoLine(t, handler.FileInfo{ID: "b", Size: 20}),
		"not-json",
		`{"Size": 5}`,          // missing ID
		`{"ID":"c","Size":-1}`, // negative size
		receiptLine(t, "post-terminate", handler.FileInfo{ID: "a", Size: 10}),
		receiptLine(t, "post-create", handler.FileInfo{ID: "a", Size: 10}),
	}, "\n")

	receipts, invalid, err := ReadReceiptLedger(strings.NewReader(ledger))
	require.NoError(t, err)
	a.Equal(4, len(receipts))
	a.Equal(3, invalid)
	a.Equal([]int{3, 4, 8, 9}, []int{receipts[0].Line, receipts[1].Line, receipts[2].Line, receipts[3].Line})

	state := foldLedger(receipts)
	a.Contains(state.active, "a") // create after terminate wins
	a.Contains(state.active, "b")
	a.NotContains(state.terminated, "a")

	terminated := foldLedger([]LedgerReceipt{
		{HookType: "post-create", Info: handler.FileInfo{ID: "x"}},
		{HookType: "post-terminate", Info: handler.FileInfo{ID: "x"}},
	})
	a.NotContains(terminated.active, "x")
	a.Contains(terminated.terminated, "x")
}

// ---------- recovery happy paths ----------

// TestRecoveryMissingInfoRebuilds is the core scenario: binary data is on
// disk, metadata is gone, and the upload 404s. Recovery rebuilds the .info
// file and the upload can be opened and resumed through the real FileStore.
func TestRecoveryMissingInfoRebuilds(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")

	id := "aaa00000000000000000000000000001"
	writeBin(t, dir, id, strings.Repeat("x", 100)) // declared 200, 100 on disk
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{
		ID:   id,
		Size: 200,
		MetaData: handler.MetaData{
			"filename": "video.mp4",
		},
	}))

	store := New(dir)
	_, err := store.GetUpload(ctx, id)
	a.ErrorIs(err, handler.ErrNotFound)

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)
	a.Zero(report.Mismatched)

	upload, err := store.GetUpload(ctx, id)
	require.NoError(t, err)
	info, err := upload.GetInfo(ctx)
	require.NoError(t, err)
	a.EqualValues(200, info.Size)
	a.False(info.SizeIsDeferred)
	a.EqualValues(100, info.Offset)
	a.Equal("video.mp4", info.MetaData["filename"])
	a.Equal("filestore", info.Storage["Type"])
	a.Equal(filepath.Join(dir, id), info.Storage[StorageKeyPath])
	a.Equal(filepath.Join(dir, id+".info"), info.Storage[StorageKeyInfoPath])

	// The client can resume: append the remaining bytes.
	n, err := upload.WriteChunk(ctx, 100, strings.NewReader(strings.Repeat("y", 100)))
	require.NoError(t, err)
	a.EqualValues(100, n)
	upload2, err := store.GetUpload(ctx, id)
	require.NoError(t, err)
	info2, err := upload2.GetInfo(ctx)
	require.NoError(t, err)
	a.EqualValues(200, info2.Offset)
}

// TestRecoveryCompletedUpload: a fully uploaded binary whose metadata was
// lost is recovered as a completed upload.
func TestRecoveryCompletedUpload(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "bbb00000000000000000000000000002"
	writeBin(t, dir, id, "all-bytes-present")
	writeLedger(t, ledgerPath, bareInfoLine(t, handler.FileInfo{ID: id, Size: 17}))

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)

	info := readInfoFile(t, dir, id)
	a.EqualValues(17, info.Offset)
	a.EqualValues(17, info.Size)
}

// TestRecoveryDeferredLength: with a deferred length, any data size is
// consistent and the recovered upload stays resumable.
func TestRecoveryDeferredLength(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "ccc00000000000000000000000000003"
	writeBin(t, dir, id, "partial")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{
		ID:             id,
		SizeIsDeferred: true,
	}))

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)

	info := readInfoFile(t, dir, id)
	a.True(info.SizeIsDeferred)
	a.EqualValues(7, info.Offset)
}

// TestRecoveryHealthyUploadUntouched: existing valid metadata is never
// rewritten.
func TestRecoveryHealthyUploadUntouched(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	store := New(dir)
	id := "ddd00000000000000000000000000004"

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, Size: 50})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader("12345"))
	require.NoError(t, err)

	infoPath := filepath.Join(dir, id+".info")
	before, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	mtime := statOfPath(t, infoPath).ModTime()

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 50}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.AlreadyHealthy)
	a.Zero(report.RecoveredMissingInfo + report.RecoveredCorruptInfo + report.Mismatched)

	after, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	a.Equal(before, after)
	a.Equal(mtime, statOfPath(t, infoPath).ModTime())
}

// TestRecoveryDeferredReceiptConcreteMetadata: concrete metadata is a newer
// state than a post-create receipt that still had deferred length.
func TestRecoveryDeferredReceiptConcreteMetadata(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	store := New(dir)
	id := "ddd10000000000000000000000000004"

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, Size: 50})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader("12345"))
	require.NoError(t, err)

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{
		ID:             id,
		SizeIsDeferred: true,
	}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.AlreadyHealthy)
}

// ---------- corrupt metadata ----------

// TestRecoveryCorruptInfoQuarantined: unreadable metadata is preserved in
// quarantine (never deleted) and rebuilt.
func TestRecoveryCorruptInfoQuarantined(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "eee00000000000000000000000000005"
	writeBin(t, dir, id, strings.Repeat("z", 40))

	corrupt := []byte("{this is not json")
	infoPath := filepath.Join(dir, id+".info")
	require.NoError(t, os.WriteFile(infoPath, corrupt, 0o664))

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 100}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredCorruptInfo)
	a.Equal(1, report.QuarantinedFiles)

	// Upload is usable again with the offset taken from disk.
	upload, err := New(dir).GetUpload(ctx, id)
	require.NoError(t, err)
	info, err := upload.GetInfo(ctx)
	require.NoError(t, err)
	a.EqualValues(40, info.Offset)
	a.EqualValues(100, info.Size)

	// The corrupt bytes and a sidecar are preserved in quarantine.
	entries := quarantineEntries(t, dir)
	var infoFiles, sidecars [][]byte
	for _, entry := range entries {
		data, readErr := os.ReadFile(filepath.Join(dir, defaultRecoveryStateDirName, quarantineDirName, entry.Name()))
		require.NoError(t, readErr)
		if strings.HasSuffix(entry.Name(), ".info") {
			infoFiles = append(infoFiles, data)
		} else {
			sidecars = append(sidecars, data)
		}
	}
	a.Len(infoFiles, 1)
	a.Equal(corrupt, infoFiles[0])
	a.Len(sidecars, 1)
	a.Contains(string(sidecars[0]), id)
}

// TestRecoveryCorruptInfoWithoutDataKeptInPlace: corrupt metadata whose binary
// data is missing must not be quarantined: evidence is preserved in place and
// the mismatch is flagged.
func TestRecoveryCorruptInfoWithoutDataKeptInPlace(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "eee10000000000000000000000000006"
	corrupt := []byte("broken")
	infoPath := filepath.Join(dir, id+".info")
	require.NoError(t, os.WriteFile(infoPath, corrupt, 0o664))

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 100}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Mismatched)
	a.Zero(report.RecoveredCorruptInfo)

	stillThere, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	a.Equal(corrupt, stillThere)
	a.Empty(quarantineEntries(t, dir))
	marker := readMarker(t, dir, id)
	a.Equal(ReasonDataMissing, marker.Reason)
}

// TestRecoveryEmptyDocumentTreatedAsCorrupt: a parseable but empty metadata
// document (e.g. a torn "null" write) is rebuilt like corrupt metadata.
func TestRecoveryEmptyDocumentTreatedAsCorrupt(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	for _, payload := range [][]byte{[]byte("null"), []byte("{}")} {
		dir := t.TempDir()
		ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
		id := "e" + strings.Repeat("0", 30) + "1"
		writeBin(t, dir, id, "ab")
		require.NoError(t, os.WriteFile(filepath.Join(dir, id+".info"), payload, 0o664))
		writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

		report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
		require.NoError(t, err, "payload %q", payload)
		a.Equal(1, report.RecoveredCorruptInfo, "payload %q", payload)

		upload, err := New(dir).GetUpload(ctx, id)
		require.NoError(t, err)
		info, err := upload.GetInfo(ctx)
		require.NoError(t, err)
		a.EqualValues(2, info.Offset)
	}
}

// ---------- mismatches ----------

func TestRecoveryDataMissingFlagged(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "f0000000000000000000000000000007"
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Mismatched)
	a.Equal(1, report.MismatchReasons[ReasonDataMissing])
	a.Equal(1, report.MismatchMarkersWritten)
	a.NoFileExists(filepath.Join(dir, id+".info"))

	marker := readMarker(t, dir, id)
	a.Equal(id, marker.ID)
	a.EqualValues(10, marker.DeclaredSize)

	// After the data appears, the next run recovers and resolves the marker.
	writeBin(t, dir, id, "10 bytes!!")
	report, err = testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)
	a.Equal(1, report.MismatchMarkersResolved)
	a.NoFileExists(markerPath(dir, id))
}

func TestRecoveryDataOverflowFlagged(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "f0000000000000000000000000000008"
	content := strings.Repeat("q", 150)
	writeBin(t, dir, id, content) // declared only 100

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 100}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Mismatched)
	a.Equal(1, report.MismatchReasons[ReasonDataOverflow])
	a.NoFileExists(filepath.Join(dir, id+".info"))

	// Data is never touched.
	data, err := os.ReadFile(filepath.Join(dir, id))
	require.NoError(t, err)
	a.Equal(content, string(data))

	marker := readMarker(t, dir, id)
	a.EqualValues(150, marker.DataSize)
}

func TestRecoveryUnsafeIDFlagged(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "evil.info")
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: "../evil", Size: 1}))

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Mismatched)
	a.Equal(1, report.MismatchReasons[ReasonUnsafeID])
	a.NoFileExists(outside)
	a.NoFileExists(filepath.Join(dir, "..", "evil.info"))
}

func TestRecoveryMetadataConflictFlagged(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	store := New(dir)
	id := "f0000000000000000000000000000009"

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, Size: 100})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader(strings.Repeat("a", 10)))
	require.NoError(t, err)
	infoPath := filepath.Join(dir, id+".info")
	before, err := os.ReadFile(infoPath)
	require.NoError(t, err)

	// Ledger claims a different declared size.
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 200}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Mismatched)
	a.Equal(1, report.MismatchReasons[ReasonMetadataConflict])

	after, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	a.Equal(before, after, "valid metadata must never be overwritten on conflict")

	// With a corrected ledger, the same upload is healthy and marker clears.
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 100}))
	report, err = testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.AlreadyHealthy)
	a.Equal(1, report.MismatchMarkersResolved)
}

// TestRecoveryMarkerFirstSeenPreserved: repeated runs keep the original
// detection timestamp and refresh last-seen.
func TestRecoveryMarkerFirstSeenPreserved(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "f000000000000000000000000000000a"
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

	_, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	first := readMarker(t, dir, id)

	time.Sleep(10 * time.Millisecond)
	_, err = testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	second := readMarker(t, dir, id)

	a.Equal(first.FirstRecordedAt, second.FirstRecordedAt)
	a.True(second.LastSeenAt.After(first.FirstRecordedAt) || !second.LastSeenAt.Before(second.FirstRecordedAt))
}

// ---------- non-interference ----------

func TestRecoveryUntrackedFilesLeftAlone(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	store := New(dir)
	unknownID := "99990000000000000000000000000000"

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: unknownID, Size: 30})
	require.NoError(t, err)
	content := "not-in-any-ledger-data"
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader(content))
	require.NoError(t, err)
	infoPath := filepath.Join(dir, unknownID+".info")
	infoBefore, err := os.ReadFile(infoPath)
	require.NoError(t, err)

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: "other", Size: 1}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.UntrackedInfoFiles)
	a.GreaterOrEqual(report.UntrackedBinFiles, 1)

	infoAfter, err := os.ReadFile(infoPath)
	require.NoError(t, err)
	a.Equal(infoBefore, infoAfter)
	data, err := os.ReadFile(filepath.Join(dir, unknownID))
	require.NoError(t, err)
	a.Equal(content, string(data))
}

func TestRecoveryTerminatedUploadNotRecovered(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "aa11000000000000000000000000000b"
	writeBin(t, dir, id, "leftover bytes") // terminated uploads may leave data behind

	writeLedger(t, ledgerPath, receiptLine(t, "post-terminate", handler.FileInfo{ID: id, Size: 14}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.LedgerTerminatedIDs)
	a.Zero(report.LedgerCreateReceipts)
	a.Zero(report.Mismatched)
	a.NoFileExists(filepath.Join(dir, id+".info"))

	data, err := os.ReadFile(filepath.Join(dir, id))
	require.NoError(t, err)
	a.Equal("leftover bytes", string(data))
}

// TestRecoveryCustomStoragePath honors a storage path chosen via the
// pre-create hook, the same way NewUpload does.
func TestRecoveryCustomStoragePath(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "cc22000000000000000000000000000c"
	customRel := "shards/" + id + ".bin"
	binAbs := filepath.Join(dir, filepath.FromSlash(customRel))
	require.NoError(t, os.MkdirAll(filepath.Dir(binAbs), 0o775))
	require.NoError(t, os.WriteFile(binAbs, []byte("custom-path-data"), 0o664))

	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{
		ID:      id,
		Size:    100,
		Storage: map[string]string{StorageKeyPath: customRel},
	}))
	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)
	a.Zero(report.UntrackedBinFiles)

	upload, err := New(dir).GetUpload(ctx, id)
	require.NoError(t, err)
	info, err := upload.GetInfo(ctx)
	require.NoError(t, err)
	a.EqualValues(16, info.Offset)
	a.Equal(binAbs, info.Storage[StorageKeyPath])
}

// ---------- operational properties ----------

func TestRecoveryIdempotent(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	store := New(dir)

	missingID := "10000000000000000000000000000001"
	corruptID := "10000000000000000000000000000002"
	healthyID := "10000000000000000000000000000003"

	writeBin(t, dir, missingID, "12")
	writeBin(t, dir, corruptID, "34")
	require.NoError(t, os.WriteFile(filepath.Join(dir, corruptID+".info"), []byte("garbage"), 0o664))
	healthy, err := store.NewUpload(ctx, handler.FileInfo{ID: healthyID, Size: 10})
	require.NoError(t, err)
	_, err = healthy.WriteChunk(ctx, 0, strings.NewReader("56"))
	require.NoError(t, err)

	writeLedger(t, ledgerPath,
		receiptLine(t, "post-create", handler.FileInfo{ID: missingID, Size: 10}),
		receiptLine(t, "post-create", handler.FileInfo{ID: corruptID, Size: 10}),
		receiptLine(t, "post-create", handler.FileInfo{ID: healthyID, Size: 10}),
	)

	first, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, first.RecoveredMissingInfo)
	a.Equal(1, first.RecoveredCorruptInfo)
	a.Equal(1, first.AlreadyHealthy)

	missingInfo, err := os.ReadFile(filepath.Join(dir, missingID+".info"))
	require.NoError(t, err)
	corruptInfo, err := os.ReadFile(filepath.Join(dir, corruptID+".info"))
	require.NoError(t, err)
	quarantineAfterFirst := quarantineEntries(t, dir)

	second, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(3, second.AlreadyHealthy)
	a.Zero(second.RecoveredMissingInfo + second.RecoveredCorruptInfo + second.Mismatched)
	a.Zero(second.QuarantinedFiles)

	missingInfo2, err := os.ReadFile(filepath.Join(dir, missingID+".info"))
	require.NoError(t, err)
	corruptInfo2, err := os.ReadFile(filepath.Join(dir, corruptID+".info"))
	require.NoError(t, err)
	a.Equal(missingInfo, missingInfo2)
	a.Equal(corruptInfo, corruptInfo2)
	a.Len(quarantineEntries(t, dir), len(quarantineAfterFirst), "quarantine must not grow on rerun")
}

func TestRecoveryDryRunMakesNoChanges(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "20000000000000000000000000000001"
	writeBin(t, dir, id, "x")
	infoPath := filepath.Join(dir, id+".info")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

	r := testRecovery(t, dir, ledgerPath)
	r.DryRun = true
	report, err := r.Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)
	a.NoFileExists(infoPath)
	a.NoDirExists(filepath.Join(dir, defaultRecoveryStateDirName))
}

func TestRecoveryGlobalLock(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: "x", Size: 1}))

	stateDir := filepath.Join(dir, defaultRecoveryStateDirName)
	require.NoError(t, os.MkdirAll(stateDir, 0o775))
	held := lockfile.Lockfile(filepath.Join(stateDir, recoveryLockName))
	require.NoError(t, held.TryLock())
	defer func() { _ = held.Unlock() }()

	_, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	a.ErrorIs(err, ErrRecoveryAlreadyRunning)
}

func TestRecoveryStaleTempFilesRemoved(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "30000000000000000000000000000001"
	writeBin(t, dir, id, "x")

	stale := filepath.Join(dir, "."+id+".info"+recoveryTempMarker+"stale")
	require.NoError(t, os.WriteFile(stale, []byte("partial"), 0o664))
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.StaleTempFilesRemoved)
	a.Equal(1, report.RecoveredMissingInfo)
	a.NoFileExists(stale)
}

func TestRecoveryInvalidLedgerLines(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "40000000000000000000000000000001"
	writeBin(t, dir, id, "x")
	writeLedger(t, ledgerPath,
		"{broken",
		`{"Size": 1}`,
		receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}),
	)

	report, err := testRecovery(t, dir, ledgerPath).Recover(ctx)
	require.NoError(t, err)
	a.Equal(2, report.LedgerInvalidLines)
	a.Equal(1, report.LedgerCreateReceipts)
	a.Equal(1, report.RecoveredMissingInfo)
}

func TestRecoveryMissingInputs(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	_, err := testRecovery(t, dir, filepath.Join(t.TempDir(), "nope.jsonl")).Recover(ctx)
	a.Error(err)

	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: "x", Size: 1}))
	_, err = testRecovery(t, filepath.Join(t.TempDir(), "no-such-dir"), ledgerPath).Recover(ctx)
	a.Error(err)
}

// TestRecoveryPerUploadLockTimeout: while tusd holds an upload lock, the
// upload is skipped for this run instead of being modified.
func TestRecoveryPerUploadLockTimeout(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	dir := t.TempDir()
	ledgerPath := filepath.Join(t.TempDir(), "ledger.jsonl")
	id := "50000000000000000000000000000001"
	writeBin(t, dir, id, "x")
	writeLedger(t, ledgerPath, receiptLine(t, "post-create", handler.FileInfo{ID: id, Size: 10}))

	locker := filelocker.New(dir)
	lock, err := locker.NewLock(id)
	require.NoError(t, err)
	require.NoError(t, lock.Lock(ctx, func() {}))

	r := testRecovery(t, dir, ledgerPath)
	r.Locker = locker
	r.LockTimeout = 200 * time.Millisecond
	report, err := r.Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.LockTimeouts)
	a.NoFileExists(filepath.Join(dir, id+".info"))

	// After the lock is released, the next run recovers.
	require.NoError(t, lock.Unlock())
	r2 := testRecovery(t, dir, ledgerPath)
	r2.Locker = locker
	report, err = r2.Recover(ctx)
	require.NoError(t, err)
	a.Equal(1, report.RecoveredMissingInfo)
}

func statOfPath(t *testing.T, path string) fs.FileInfo {
	t.Helper()
	st, err := os.Stat(path)
	require.NoError(t, err)
	return st
}
