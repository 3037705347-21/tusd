//go:build !windows

package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tus/lockfile"
	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/handler"
)

// ---------- helpers ----------

func testReconciler(t *testing.T, dir string) Reconciler {
	t.Helper()
	r := NewReconciler(New(dir))
	r.Logger = log.New(io.Discard, "", 0)
	return r
}

// createCompleteUpload creates an upload with the given ID and writes all of
// content in one chunk, so that the upload is completed.
func createCompleteUpload(t *testing.T, ctx context.Context, store FileStore, id, content string) {
	t.Helper()
	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, Size: int64(len(content))})
	require.NoError(t, err)
	n, err := upload.WriteChunk(ctx, 0, strings.NewReader(content))
	require.NoError(t, err)
	require.EqualValues(t, len(content), n)
}

// readUpload returns the upload's current bytes through the public FileStore
// API, the same way tusd reads them.
func readUpload(t *testing.T, ctx context.Context, store FileStore, id string) string {
	t.Helper()
	upload, err := store.GetUpload(ctx, id)
	require.NoError(t, err)
	reader, err := upload.GetReader(ctx)
	require.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(data)
}

func statOf(t *testing.T, path string) fs.FileInfo {
	t.Helper()
	st, err := os.Stat(path)
	require.NoError(t, err)
	return st
}

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	return statOf(t, path).Sys().(*syscall.Stat_t).Ino
}

func linkCount(t *testing.T, path string) uint64 {
	t.Helper()
	return uint64(statOf(t, path).Sys().(*syscall.Stat_t).Nlink)
}

func contentDigest(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func blobPathFor(dir, content string) string {
	h := contentDigest(content)
	return filepath.Join(dir, defaultBlobsDirName, h[:2], h)
}

// assertNoTempLinks fails if a crashed-run temporary link remains anywhere.
func assertNoTempLinks(t *testing.T, root string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") && strings.Contains(d.Name(), dedupeTempMarker) {
			t.Errorf("unexpected stale temporary link: %s", path)
		}
		return nil
	})
}

// ---------- tests ----------

// TestReconcileDeduplicates is the end-to-end happy path: identical completed
// uploads end up sharing one blob inode, different content does not, and all
// uploads remain fully readable through FileStore. A second run is a no-op
// (idempotency).
func TestReconcileDeduplicates(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)

	contentX := strings.Repeat("same-content-line\n", 64)
	contentY := "entirely-different-content"

	ids := []string{
		"aaa00000000000000000000000000001", // A: complete, content X
		"aaa00000000000000000000000000002", // B: complete, content X (duplicate)
		"aaa00000000000000000000000000003", // C: complete, content Y
		"aaa00000000000000000000000000004", // D: still in progress
		"aaa00000000000000000000000000005", // E: completed empty upload
	}
	for _, id := range ids[:3] {
		content := contentX
		if id == ids[2] {
			content = contentY
		}
		createCompleteUpload(t, ctx, store, id, content)
	}

	// In-progress upload D: declared size is twice what has been written.
	dUpload, err := store.NewUpload(ctx, handler.FileInfo{ID: ids[3], Size: int64(2 * len(contentX))})
	require.NoError(t, err)
	_, err = dUpload.WriteChunk(ctx, 0, strings.NewReader(contentX))
	require.NoError(t, err)
	dInodeBefore := inodeOf(t, filepath.Join(tmp, ids[3]))

	// Empty completed upload E.
	createCompleteUpload(t, ctx, store, ids[4], "")

	// Completed upload with the same content in a nested ID directory.
	const nestedID = "proj/nested0000000000000000000001"
	createCompleteUpload(t, ctx, store, nestedID, contentX)

	// Corrupt .info file: must be reported and left untouched.
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "garbage.info"), []byte("{not-json"), DefaultFilePerm))
	// Binary without an .info file: orphan state must not crash or be touched.
	orphanPath := filepath.Join(tmp, "orphanbinary")
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), DefaultFilePerm))

	r := testReconciler(t, tmp)
	report, err := r.Reconcile(ctx)
	require.NoError(t, err)

	a.Equal(7, report.UploadsScanned) // A,B,C,D,E,garbage,nested
	a.Equal(4, report.UploadsDeduplicated)
	a.Equal(0, report.AlreadyLinked)
	a.Equal(2, report.BlobsCreated)
	a.Equal(2, report.SkippedIncomplete) // D (in progress), E (empty)
	a.Equal(1, report.SkippedErrors)     // garbage.info
	a.Equal(0, report.SkippedUnsupported)
	a.Equal(0, report.LockTimeouts)
	a.Equal(int64(2*len(contentX)), report.BytesReclaimed) // B and nested merged
	a.Equal(0, report.BlobsRemoved)

	// Blob layout and inode sharing.
	blobX := blobPathFor(tmp, contentX)
	blobY := blobPathFor(tmp, contentY)
	a.Equal(4, int(linkCount(t, blobX))) // blob + A + B + nested
	a.Equal(2, int(linkCount(t, blobY))) // blob + C

	a.True(os.SameFile(statOf(t, filepath.Join(tmp, ids[0])), statOf(t, blobX)))
	a.True(os.SameFile(statOf(t, filepath.Join(tmp, ids[1])), statOf(t, blobX)))
	a.True(os.SameFile(statOf(t, filepath.Join(tmp, nestedID)), statOf(t, blobX)))
	a.True(os.SameFile(statOf(t, filepath.Join(tmp, ids[2])), statOf(t, blobY)))
	a.Equal(dInodeBefore, inodeOf(t, filepath.Join(tmp, ids[3])))

	// Blob bytes are exactly the upload content.
	blobBytes, err := os.ReadFile(blobX)
	require.NoError(t, err)
	a.Equal(contentX, string(blobBytes))

	// Every upload still reads its own data via the public API.
	a.Equal(contentX, readUpload(t, ctx, store, ids[0]))
	a.Equal(contentX, readUpload(t, ctx, store, ids[1]))
	a.Equal(contentY, readUpload(t, ctx, store, ids[2]))
	a.Equal(contentX, readUpload(t, ctx, store, ids[3])) // partial read unchanged
	a.Equal("", readUpload(t, ctx, store, ids[4]))
	a.Equal(contentX, readUpload(t, ctx, store, nestedID))

	// The unrelated orphan binary and corrupt info are untouched.
	a.Equal("orphan", string(mustReadFile(t, orphanPath)))
	a.Equal("{not-json", string(mustReadFile(t, filepath.Join(tmp, "garbage.info"))))
	assertNoTempLinks(t, tmp)

	// Second run: fully idempotent, no more changes.
	report2, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(7, report2.UploadsScanned)
	a.Equal(0, report2.UploadsDeduplicated)
	a.Equal(4, report2.AlreadyLinked)
	a.Equal(0, report2.BlobsCreated)
	a.Equal(0, report2.BlobsRemoved)
	a.Equal(int64(0), report2.BytesReclaimed)
	a.Equal(2, report2.SkippedIncomplete)
	a.Equal(1, report2.SkippedErrors)
	assertNoTempLinks(t, tmp)

	a.Equal(contentX, readUpload(t, ctx, store, ids[0]))
	a.Equal(contentX, readUpload(t, ctx, store, ids[1]))
	a.Equal(contentY, readUpload(t, ctx, store, ids[2]))
	a.Equal(contentX, readUpload(t, ctx, store, nestedID))
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// TestReconcileTerminateSafety verifies the reference-counting guarantee:
// terminating one upload never removes data that another upload references;
// the blob is garbage collected only once the last upload is gone.
func TestReconcileTerminateSafety(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)

	contentX := strings.Repeat("shared-terminate-content\n", 16)
	contentY := "terminate-other-content"
	idA := "bbb00000000000000000000000000001"
	idB := "bbb00000000000000000000000000002"
	idC := "bbb00000000000000000000000000003"
	createCompleteUpload(t, ctx, store, idA, contentX)
	createCompleteUpload(t, ctx, store, idB, contentX)
	createCompleteUpload(t, ctx, store, idC, contentY)

	r := testReconciler(t, tmp)
	_, err := r.Reconcile(ctx)
	require.NoError(t, err)
	blobX := blobPathFor(tmp, contentX)
	blobY := blobPathFor(tmp, contentY)
	a.Equal(3, int(linkCount(t, blobX)))

	// Terminate A: B must keep reading the full content, the blob survives.
	uploadA, err := store.GetUpload(ctx, idA)
	require.NoError(t, err)
	require.NoError(t, store.AsTerminatableUpload(uploadA).Terminate(ctx))

	_, err = store.GetUpload(ctx, idA)
	a.ErrorIs(err, handler.ErrNotFound)
	a.Equal(contentX, readUpload(t, ctx, store, idB))
	a.Equal(contentY, readUpload(t, ctx, store, idC))
	a.Equal(2, int(linkCount(t, blobX)))

	report, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(0, report.BlobsRemoved)
	a.Equal(2, report.AlreadyLinked)
	_, err = os.Stat(blobX)
	a.NoError(err)

	// Terminate B (the last upload of X): the blob becomes collectable.
	uploadB, err := store.GetUpload(ctx, idB)
	require.NoError(t, err)
	require.NoError(t, store.AsTerminatableUpload(uploadB).Terminate(ctx))
	a.Equal(1, int(linkCount(t, blobX)))

	report, err = r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(1, report.BlobsRemoved)
	_, err = os.Stat(blobX)
	a.ErrorIs(err, fs.ErrNotExist)

	// C is unaffected; its blob stays.
	a.Equal(contentY, readUpload(t, ctx, store, idC))
	a.Equal(2, int(linkCount(t, blobY)))
}

// TestReconcileCrashRecovery seeds every intermediate state that a crashed
// run can leave behind and asserts that a rerun converges without any upload
// ever reading empty or missing data.
func TestReconcileCrashRecovery(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)

	contentX := strings.Repeat("crash-shared-content\n", 10)
	contentZ := "content-with-terminated-upload"
	idA := "ccc00000000000000000000000000001"
	idB := "ccc00000000000000000000000000002"
	createCompleteUpload(t, ctx, store, idA, contentX)
	createCompleteUpload(t, ctx, store, idB, contentX)

	binA := filepath.Join(tmp, idA)
	binB := filepath.Join(tmp, idB)
	blobX := blobPathFor(tmp, contentX)

	// State 1: blob was linked from A but no upload path was switched yet.
	require.NoError(t, os.MkdirAll(filepath.Dir(blobX), DefaultDirPerm))
	require.NoError(t, os.Link(binA, blobX))

	// State 2: a stale temporary link (crash between link and rename) next to B.
	staleTmp := filepath.Join(tmp, "."+idB+dedupeTempMarker+"0123456789abcdef0123456789abcdef")
	require.NoError(t, os.Link(binA, staleTmp))

	// State 3: orphan blob with nlink 1 (upload terminated after blob create).
	blobZ := blobPathFor(tmp, contentZ)
	require.NoError(t, os.MkdirAll(filepath.Dir(blobZ), DefaultDirPerm))
	require.NoError(t, os.WriteFile(blobZ, []byte(contentZ), DefaultFilePerm))
	a.Equal(1, int(linkCount(t, blobZ)))

	r := testReconciler(t, tmp)
	report, err := r.Reconcile(ctx)
	require.NoError(t, err)

	a.Equal(1, report.TempLinksRemoved)
	a.Equal(1, report.AlreadyLinked)       // A already shares the blob
	a.Equal(1, report.UploadsDeduplicated) // B merged
	a.Equal(0, report.BlobsCreated)
	a.Equal(1, report.BlobsRemoved) // orphan Z collected
	a.Equal(int64(len(contentX)), report.BytesReclaimed)

	// Both uploads read the full content; no empty reads.
	a.Equal(contentX, readUpload(t, ctx, store, idA))
	a.Equal(contentX, readUpload(t, ctx, store, idB))
	a.True(os.SameFile(statOf(t, binA), statOf(t, blobX)))
	a.True(os.SameFile(statOf(t, binB), statOf(t, blobX)))
	_, err = os.Stat(blobZ)
	a.ErrorIs(err, fs.ErrNotExist)
	_, err = os.Stat(staleTmp)
	a.ErrorIs(err, fs.ErrNotExist)
	assertNoTempLinks(t, tmp)
}

// TestReconcileExternalPathSkipped ensures uploads whose binary lives outside
// the store directory (custom Storage.Path from a pre-create hook) are skipped
// rather than linked across directories.
func TestReconcileExternalPathSkipped(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	external := t.TempDir()
	store := New(tmp)

	contentX := "external-or-internal-content"
	extBin := filepath.Join(external, "external.bin")
	extUpload, err := store.NewUpload(ctx, handler.FileInfo{
		ID:      "ddd-external000000000000000001",
		Size:    int64(len(contentX)),
		Storage: map[string]string{StorageKeyPath: extBin},
	})
	require.NoError(t, err)
	n, err := extUpload.WriteChunk(ctx, 0, strings.NewReader(contentX))
	require.NoError(t, err)
	a.EqualValues(len(contentX), n)

	createCompleteUpload(t, ctx, store, "ddd-internal000000000000000001", contentX)

	r := testReconciler(t, tmp)
	report, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(1, report.SkippedUnsupported)
	a.Equal(1, report.UploadsDeduplicated)
	a.Equal(1, int(linkCount(t, extBin))) // untouched, still a single copy
	a.Equal(contentX, string(mustReadFile(t, extBin)))
}

// TestReconcileFileLocker checks integration with the same per-upload locks
// tusd uses: a locked upload is skipped for this run and converged after the
// lock is released.
func TestReconcileFileLocker(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)
	contentX := strings.Repeat("locked-content\n", 8)
	idA := "eee00000000000000000000000000001"
	idB := "eee00000000000000000000000000002"
	createCompleteUpload(t, ctx, store, idA, contentX)
	createCompleteUpload(t, ctx, store, idB, contentX)

	locker := filelocker.New(tmp)
	heldLock, err := locker.NewLock(idA)
	require.NoError(t, err)
	require.NoError(t, heldLock.Lock(ctx, func() {}))

	r := testReconciler(t, tmp)
	r.Locker = locker
	r.LockTimeout = 50 * time.Millisecond

	report, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(1, report.LockTimeouts)
	a.Equal(1, report.UploadsDeduplicated) // B merged
	a.Equal(1, report.BlobsCreated)

	binA := filepath.Join(tmp, idA)
	binB := filepath.Join(tmp, idB)
	a.False(os.SameFile(statOf(t, binA), statOf(t, binB))) // A untouched while locked
	a.Equal(contentX, readUpload(t, ctx, store, idA))
	a.Equal(contentX, readUpload(t, ctx, store, idB))

	// After releasing the lock, another run converges A as well.
	require.NoError(t, heldLock.Unlock())
	report, err = r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(0, report.LockTimeouts)
	a.Equal(1, report.UploadsDeduplicated) // A merged now
	a.Equal(1, report.AlreadyLinked)       // B already linked
	a.True(os.SameFile(statOf(t, binA), statOf(t, binB)))
	a.Equal(contentX, readUpload(t, ctx, store, idA))
	a.Equal(contentX, readUpload(t, ctx, store, idB))
}

// TestReconcileAlreadyRunning verifies that a second concurrent run fails
// immediately with a clear error instead of racing on the same directory.
func TestReconcileAlreadyRunning(t *testing.T) {
	a := assert.New(t)

	tmp := t.TempDir()
	blobsDir := filepath.Join(tmp, defaultBlobsDirName)
	require.NoError(t, os.MkdirAll(blobsDir, DefaultDirPerm))

	held := lockfile.Lockfile(filepath.Join(blobsDir, reconcileLockName))
	require.NoError(t, held.TryLock())
	defer func() { _ = held.Unlock() }()

	r := testReconciler(t, tmp)
	_, err := r.Reconcile(context.Background())
	a.ErrorIs(err, ErrReconcileAlreadyRunning)
}

// TestReconcileDryRun ensures a dry run changes nothing on disk.
func TestReconcileDryRun(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)
	contentX := strings.Repeat("dry-run-content\n", 4)
	idA := "fff00000000000000000000000000001"
	idB := "fff00000000000000000000000000002"
	createCompleteUpload(t, ctx, store, idA, contentX)
	createCompleteUpload(t, ctx, store, idB, contentX)

	inodeABefore := inodeOf(t, filepath.Join(tmp, idA))
	inodeBBefore := inodeOf(t, filepath.Join(tmp, idB))

	r := testReconciler(t, tmp)
	r.DryRun = true
	report, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(2, report.UploadsDeduplicated)
	a.Equal(2, report.BlobsCreated) // estimate
	a.Equal(0, report.BlobsRemoved)

	// No blob directory, no lock, no relinking, no temp links.
	_, err = os.Stat(filepath.Join(tmp, defaultBlobsDirName))
	a.ErrorIs(err, fs.ErrNotExist)
	a.Equal(inodeABefore, inodeOf(t, filepath.Join(tmp, idA)))
	a.Equal(inodeBBefore, inodeOf(t, filepath.Join(tmp, idB)))
	a.Equal(contentX, readUpload(t, ctx, store, idA))
	a.Equal(contentX, readUpload(t, ctx, store, idB))

	// A real run after the dry run works normally.
	r2 := testReconciler(t, tmp)
	report, err = r2.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(2, report.UploadsDeduplicated)
	a.Equal(1, report.BlobsCreated)
	a.Equal(int64(len(contentX)), report.BytesReclaimed)
	a.Equal(contentX, readUpload(t, ctx, store, idA))
	a.Equal(contentX, readUpload(t, ctx, store, idB))
}

// TestReconcileDeferredLength verifies that an unfinished deferred-length
// upload is skipped and that it is deduplicated once the length is declared.
func TestReconcileDeferredLength(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	tmp := t.TempDir()
	store := New(tmp)
	id := "aaaadeferred000000000000000000001"
	content := strings.Repeat("deferred-content\n", 5)

	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: id, SizeIsDeferred: true})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader(content))
	require.NoError(t, err)

	r := testReconciler(t, tmp)
	report, err := r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(1, report.SkippedIncomplete)
	a.Equal(0, report.UploadsDeduplicated)

	require.NoError(t, store.AsLengthDeclarableUpload(upload).DeclareLength(ctx, int64(len(content))))

	report, err = r.Reconcile(ctx)
	require.NoError(t, err)
	a.Equal(0, report.SkippedIncomplete)
	a.Equal(1, report.UploadsDeduplicated)
	a.Equal(1, report.BlobsCreated)
	a.Equal(content, readUpload(t, ctx, store, id))
}

// TestReconcileMissingDirectory fails fast on an invalid store path.
func TestReconcileMissingDirectory(t *testing.T) {
	tmp := t.TempDir()
	r := NewReconciler(New(filepath.Join(tmp, "does-not-exist")))
	r.Logger = log.New(io.Discard, "", 0)
	_, err := r.Reconcile(context.Background())
	require.Error(t, err)
}
