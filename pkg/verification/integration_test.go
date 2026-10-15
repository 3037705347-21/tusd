package verification_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
	"github.com/tus/tusd/v2/pkg/verification"
)

func digestHex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func writeIntegrationManifest(t *testing.T, dir string, entries []verification.Entry) string {
	t.Helper()
	payload := struct {
		Entries []verification.Entry `json:"entries"`
	}{Entries: entries}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	path := filepath.Join(dir, "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0664))
	return path
}

func createFileOnDisk(t *testing.T, ctx context.Context, store filestore.FileStore, id, content string) {
	t.Helper()
	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, Size: int64(len(content))})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader(content))
	require.NoError(t, err)
}

func newTusdHandler(t *testing.T, uploadDir string, blocker handler.DownloadBlocker) http.Handler {
	t.Helper()
	store := filestore.New(uploadDir)
	locker := filelocker.New(uploadDir)

	composer := handler.NewStoreComposer()
	store.UseIn(composer)
	locker.UseIn(composer)

	h, err := handler.NewHandler(handler.Config{
		StoreComposer:   composer,
		DownloadBlocker: blocker,
	})
	require.NoError(t, err)
	return h
}

func getDownload(t *testing.T, h http.Handler, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/"+id, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// TestIntegrationQuarantineBlocksDownloadAndSurvivesRestart drives the full
// chain with a real file store, file locker and HTTP handler: a manifest run
// quarantines a corrupted upload, its download is refused with 403, and the
// block (without duplicate work) survives a simulated restart.
func TestIntegrationQuarantineBlocksDownloadAndSurvivesRestart(t *testing.T) {
	a := assert.New(t)

	uploadDir := t.TempDir()
	manifestDir := t.TempDir()
	ctx := context.Background()

	store := filestore.New(uploadDir)
	createFileOnDisk(t, ctx, store, "good", "hello")
	createFileOnDisk(t, ctx, store, "bad", "HELLO")

	// The manifest claims "bad" contains "hello", which is wrong.
	entries := []verification.Entry{
		{ID: "good", Size: 5, Digest: digestHex("hello")},
		{ID: "bad", Size: 5, Digest: digestHex("hello")},
	}
	manifestPath := writeIntegrationManifest(t, manifestDir, entries)
	ledgerPath := filepath.Join(uploadDir, ".verification", "ledger.jsonl")
	quarantineDir := filepath.Join(uploadDir, ".quarantine")

	locker := filelocker.New(uploadDir)
	verifier, err := verification.New(verification.Config{
		ManifestPath:  manifestPath,
		LedgerPath:    ledgerPath,
		QuarantineDir: quarantineDir,
		Store:         store,
		Locker:        locker,
		LockTimeout:   5 * time.Second,
	})
	require.NoError(t, err)

	report, status, err := verifier.Run(ctx)
	require.NoError(t, err)
	a.Equal(1, report.Verified)
	a.Equal(1, report.Quarantined)

	// The corrupted data file was physically quarantined.
	quarantinedData, err := os.ReadFile(filepath.Join(quarantineDir, "bad"))
	require.NoError(t, err)
	a.Equal("HELLO", string(quarantinedData))
	_, err = os.Stat(filepath.Join(uploadDir, "bad"))
	a.True(os.IsNotExist(err))

	// Serve with the replay-derived blocker.
	h := newTusdHandler(t, uploadDir, status)

	wGood := getDownload(t, h, "good")
	a.Equal(http.StatusOK, wGood.Code)
	body, err := io.ReadAll(wGood.Body)
	require.NoError(t, err)
	a.Equal("hello", string(body))

	wBad := getDownload(t, h, "bad")
	a.Equal(http.StatusForbidden, wBad.Code)
	a.Contains(wBad.Body.String(), "ERR_UPLOAD_QUARANTINED")

	ledgerAfterRun1, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)

	// Simulate a restart: a fresh verifier over the same directories, with
	// the unchanged manifest. Nothing must be re-hashed or moved.
	store2 := filestore.New(uploadDir)
	locker2 := filelocker.New(uploadDir)
	verifier2, err := verification.New(verification.Config{
		ManifestPath:  manifestPath,
		LedgerPath:    ledgerPath,
		QuarantineDir: quarantineDir,
		Store:         store2,
		Locker:        locker2,
		LockTimeout:   5 * time.Second,
	})
	require.NoError(t, err)

	report2, status2, err := verifier2.Run(ctx)
	require.NoError(t, err)
	a.Equal(2, report2.Skipped)
	a.Equal(0, report2.Verified)
	a.Equal(0, report2.Quarantined)

	ledgerAfterRun2, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	a.Equal(string(ledgerAfterRun1), string(ledgerAfterRun2), "incremental rerun must not append events")

	// Quarantine data is still exactly where it was moved.
	quarantinedData2, err := os.ReadFile(filepath.Join(quarantineDir, "bad"))
	require.NoError(t, err)
	a.Equal("HELLO", string(quarantinedData2))

	// The freshly reconstructed blocker keeps enforcing the same decisions.
	h2 := newTusdHandler(t, uploadDir, status2)
	a.Equal(http.StatusForbidden, getDownload(t, h2, "bad").Code)
	a.Equal(http.StatusOK, getDownload(t, h2, "good").Code)
}

// TestIntegrationNoManifestMeansNoBlocking proves that without a verification
// run the handler serves uploads normally, including a "corrupted" one.
func TestIntegrationWithoutVerificationEverythingDownloads(t *testing.T) {
	a := assert.New(t)

	uploadDir := t.TempDir()
	ctx := context.Background()

	store := filestore.New(uploadDir)
	createFileOnDisk(t, ctx, store, "anything", "whatever")

	// No blocker installed (verification never ran).
	h := newTusdHandler(t, uploadDir, nil)

	w := getDownload(t, h, "anything")
	a.Equal(http.StatusOK, w.Code)
	a.Equal("whatever", w.Body.String())
}
