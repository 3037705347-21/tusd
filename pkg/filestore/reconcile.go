package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tus/lockfile"
	"github.com/tus/tusd/v2/internal/uid"
	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	// defaultBlobsDirName is the name of the content-addressed blob directory
	// inside the upload directory.
	defaultBlobsDirName = ".blobs"

	// reconcileLockName serializes concurrent reconciliation runs.
	reconcileLockName = ".reconcile.lock"

	// dedupeTempMarker is part of the temporary hard links created next to an
	// upload binary before the atomic rename. Stale entries from a crashed run
	// are recognized (and removed) by this marker on the next run.
	dedupeTempMarker = ".__tusd-dedupe__."

	// defaultReconcileLockTimeout is how long the reconciler waits for a
	// single in-use upload lock before skipping that upload.
	defaultReconcileLockTimeout = 30 * time.Second
)

// ErrReconcileAlreadyRunning is returned by Reconcile when another
// reconciliation process already holds the global lock for the blob
// directory.
var ErrReconcileAlreadyRunning = errors.New("filestore: a reconciliation is already running for this directory")

// Reconciler deduplicates the binary files of completed uploads in a FileStore
// directory.
//
// Identical content is merged into a single physical copy inside a hidden
// content-addressed blob directory using hard links. Every upload keeps its
// own path and keeps reading its own bytes, while the filesystem link count
// acts as the reference count: terminating one upload only removes one name,
// so data referenced by other uploads is never deleted.
//
// Reconcile is idempotent and crash-safe: it only links before it unlinks,
// switches upload paths atomically with rename(2), and therefore never leaves
// an upload pointing at missing or empty content. It is meant to be run
// repeatedly, e.g. as a cron job. Only completed uploads (declared size known
// and the binary file matching that size) are considered; in-progress
// uploads are never touched.
type Reconciler struct {
	// Path is the FileStore directory to reconcile. It is populated by
	// NewReconciler.
	Path string

	// DirModePerm is used when creating blob shard directories. If zero, the
	// store's default is used.
	DirModePerm fs.FileMode

	// BlobsDirName is the name of the blob directory inside Path. If empty,
	// ".blobs" is used.
	BlobsDirName string

	// Locker optionally provides the same per-upload locks that tusd uses
	// (e.g. filelocker.New(Path)). When set, the reconciler acquires each
	// upload's lock before touching its files, excluding concurrent
	// PATCH/DELETE/concat requests. May be nil, in which case reconciliation
	// remains safe but relies on completed uploads being immutable in tusd.
	Locker handler.Locker

	// LockTimeout bounds the wait for a single in-use upload lock. If zero,
	// 30 seconds is used. A locked upload is skipped for this run and picked
	// up on the next run.
	LockTimeout time.Duration

	// GarbageCollect removes blobs from the blob directory that no upload
	// references anymore (link count of one). NewReconciler enables this by
	// default. Disabling it only affects reclamation of unreferenced blobs;
	// deduplication itself does not depend on it.
	GarbageCollect bool

	// DryRun reports what Reconcile would do without creating, linking,
	// renaming or deleting anything.
	DryRun bool

	// Logger receives per-upload decisions and warnings. If nil, output is
	// discarded.
	Logger *log.Logger
}

// ReconcileReport summarizes the result of one reconciliation run. In dry-run
// mode the counters describe what a real run would do.
type ReconcileReport struct {
	// UploadsScanned is the number of .info files that were considered.
	UploadsScanned int
	// UploadsDeduplicated is the number of uploads whose binary was linked to
	// a content-addressed blob (or would be in dry-run mode).
	UploadsDeduplicated int
	// AlreadyLinked is the number of uploads that already shared their blob.
	AlreadyLinked int
	// SkippedIncomplete counts uploads that were left untouched because they
	// are not completed yet (in progress, deferred length, or empty).
	SkippedIncomplete int
	// SkippedUnsupported counts uploads whose binary lives outside the store
	// directory or on a filesystem that does not support hard links.
	SkippedUnsupported int
	// SkippedErrors counts uploads skipped due to unreadable or invalid state
	// (missing binary, corrupt .info, ...). Nothing is deleted in this case.
	SkippedErrors int
	// LockTimeouts counts uploads skipped because their upload lock could not
	// be acquired within LockTimeout.
	LockTimeouts int
	// TempLinksRemoved is the number of stale temporary links from a previous
	// crashed run that were cleaned up.
	TempLinksRemoved int
	// BlobsCreated is the number of new content-addressed blobs.
	BlobsCreated int
	// BlobsRemoved is the number of unreferenced blobs reclaimed by garbage
	// collection.
	BlobsRemoved int
	// BytesReclaimed is the approximate number of bytes whose physical
	// blocks were freed because a duplicated copy was merged into a blob.
	BytesReclaimed int64
}

// String renders the report as a human-readable summary.
func (r ReconcileReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "uploads scanned:          %d\n", r.UploadsScanned)
	fmt.Fprintf(&b, "uploads deduplicated:     %d\n", r.UploadsDeduplicated)
	fmt.Fprintf(&b, "uploads already linked:   %d\n", r.AlreadyLinked)
	fmt.Fprintf(&b, "skipped (not completed):  %d\n", r.SkippedIncomplete)
	fmt.Fprintf(&b, "skipped (unsupported):    %d\n", r.SkippedUnsupported)
	fmt.Fprintf(&b, "skipped (errors):         %d\n", r.SkippedErrors)
	fmt.Fprintf(&b, "skipped (lock timeouts):  %d\n", r.LockTimeouts)
	fmt.Fprintf(&b, "stale temp links removed: %d\n", r.TempLinksRemoved)
	fmt.Fprintf(&b, "blobs created:            %d\n", r.BlobsCreated)
	fmt.Fprintf(&b, "blobs removed:            %d\n", r.BlobsRemoved)
	fmt.Fprintf(&b, "bytes reclaimed:          %d\n", r.BytesReclaimed)
	return b.String()
}

// NewReconciler creates a Reconciler for the given store with safe defaults
// (".blobs" directory, 30s lock timeout, garbage collection enabled).
func NewReconciler(store FileStore) Reconciler {
	return Reconciler{
		Path:           store.Path,
		DirModePerm:    store.DirModePerm,
		BlobsDirName:   defaultBlobsDirName,
		LockTimeout:    defaultReconcileLockTimeout,
		GarbageCollect: true,
		Logger:         log.New(io.Discard, "", 0),
	}
}

// Reconcile runs one idempotent deduplication pass and returns a report. A
// non-nil error indicates a global failure (e.g. invalid directory, another
// run in progress); per-upload problems are counted in the report and never
// abort the run.
func (r Reconciler) Reconcile(ctx context.Context) (ReconcileReport, error) {
	report := ReconcileReport{}

	if r.Logger == nil {
		r.Logger = log.New(io.Discard, "", 0)
	}
	if r.BlobsDirName == "" {
		r.BlobsDirName = defaultBlobsDirName
	}
	if r.LockTimeout <= 0 {
		r.LockTimeout = defaultReconcileLockTimeout
	}
	dirPerm := r.DirModePerm
	if dirPerm == 0 {
		dirPerm = os.FileMode(DefaultDirPerm) & os.ModePerm
	}

	root, err := filepath.Abs(r.Path)
	if err != nil {
		return report, fmt.Errorf("filestore: resolving upload directory: %w", err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return report, fmt.Errorf("filestore: cannot access upload directory %q: %w", root, err)
	}
	if !rootInfo.IsDir() {
		return report, fmt.Errorf("filestore: upload directory %q is not a directory", root)
	}

	blobsDir := filepath.Join(root, r.BlobsDirName)

	// Dry runs are strictly read-only: no blob directory, no global lock and
	// no per-upload locks are acquired, so running them never disturbs a live
	// tusd process.
	if !r.DryRun {
		if err := os.MkdirAll(blobsDir, dirPerm); err != nil {
			return report, fmt.Errorf("filestore: creating blob directory: %w", err)
		}

		globalLock := lockfile.Lockfile(filepath.Join(blobsDir, reconcileLockName))
		if err := globalLock.TryLock(); err != nil {
			if errors.Is(err, lockfile.ErrBusy) {
				return report, ErrReconcileAlreadyRunning
			}
			return report, fmt.Errorf("filestore: acquiring reconciliation lock: %w", err)
		}
		defer func() {
			_ = globalLock.Unlock()
		}()

		removed, err := cleanupTempLinks(root, blobsDir)
		if err != nil {
			return report, err
		}
		report.TempLinksRemoved = removed
	}

	blobInodes, err := indexBlobs(blobsDir)
	if err != nil {
		return report, err
	}

	infoFiles, err := listInfoFiles(root, blobsDir)
	if err != nil {
		return report, err
	}

	for _, infoPath := range infoFiles {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.UploadsScanned++
		r.processUpload(ctx, root, blobsDir, infoPath, dirPerm, blobInodes, &report)
	}

	if !r.DryRun && r.GarbageCollect {
		removed, err := collectGarbage(blobsDir, reconcileLockName, r.Logger)
		if err != nil {
			return report, err
		}
		report.BlobsRemoved = removed
	}

	return report, nil
}

// processUpload handles a single .info file. Errors are counted in the report
// and logged; they never propagate, so one bad upload cannot abort the run.
func (r Reconciler) processUpload(
	ctx context.Context,
	root, blobsDir, infoPath string,
	dirPerm fs.FileMode,
	blobInodes map[inodeIdentity]string,
	report *ReconcileReport,
) {
	logSkip := func(reason string, args ...any) {
		args = append([]any{filepath.Base(infoPath)}, args...)
		r.Logger.Printf("upload %s skipped ("+reason+")", args...)
	}

	data, err := os.ReadFile(infoPath)
	if err != nil {
		report.SkippedErrors++
		logSkip("unreadable info: %v", err)
		return
	}

	var info handler.FileInfo
	if err := json.Unmarshal(data, &info); err != nil {
		report.SkippedErrors++
		logSkip("invalid info: %v", err)
		return
	}

	// Resolve the binary path the same way FileStore.GetUpload does.
	id := info.ID
	if id == "" {
		if rel, relErr := filepath.Rel(root, infoPath); relErr == nil {
			id = strings.TrimSuffix(rel, ".info")
		}
	}
	var binPath string
	if info.Storage != nil && info.Storage[StorageKeyPath] != "" {
		p := info.Storage[StorageKeyPath]
		if filepath.IsAbs(p) {
			binPath = p
		} else {
			binPath = filepath.Join(root, p)
		}
	} else {
		binPath = filepath.Join(root, id)
	}

	if !pathWithin(root, blobsDir, binPath) {
		report.SkippedUnsupported++
		logSkip("binary %q is outside the upload directory", binPath)
		return
	}

	binStat, err := os.Stat(binPath)
	if err != nil {
		report.SkippedErrors++
		logSkip("cannot stat binary: %v", err)
		return
	}
	if !binStat.Mode().IsRegular() {
		report.SkippedUnsupported++
		logSkip("binary is not a regular file")
		return
	}

	// Only completed uploads are eligible. This mirrors the offset logic in
	// FileStore.GetUpload: the declared size must be known, positive and
	// equal to the binary's current size. Such files cannot be appended to
	// through tusd anymore.
	if info.SizeIsDeferred || info.Size <= 0 || binStat.Size() != info.Size {
		report.SkippedIncomplete++
		logSkip("upload not completed (size %d, bytes on disk %d, deferred %v)", info.Size, binStat.Size(), info.SizeIsDeferred)
		return
	}

	// Acquire the per-upload lock outside the actual mutation so that a
	// concurrent PATCH/DELETE in tusd is excluded. Dry runs stay lock-free.
	var lock handler.Lock
	if !r.DryRun && r.Locker != nil {
		lock, err = r.Locker.NewLock(id)
		if err != nil {
			report.SkippedErrors++
			logSkip("cannot create lock: %v", err)
			return
		}

		lockCtx, cancel := context.WithTimeout(ctx, r.LockTimeout)
		lockErr := lock.Lock(lockCtx, func() {})
		cancel()
		if lockErr != nil {
			report.LockTimeouts++
			logSkip("lock not acquired: %v", lockErr)
			return
		}
		defer func() {
			_ = lock.Unlock()
		}()

		// Re-check the state under the lock; the upload could have been
		// terminated or modified while we waited.
		binStat, err = os.Stat(binPath)
		if err != nil {
			report.SkippedErrors++
			logSkip("cannot stat binary after lock: %v", err)
			return
		}
		if !binStat.Mode().IsRegular() || info.SizeIsDeferred || info.Size <= 0 || binStat.Size() != info.Size {
			report.SkippedIncomplete++
			logSkip("upload changed while waiting for the lock")
			return
		}
	}

	// Fast path: the binary already shares an inode with a blob in the store,
	// so there is no need to hash it again.
	if key, ok := inodeKey(binStat); ok {
		if _, linked := blobInodes[key]; linked {
			report.AlreadyLinked++
			return
		}
	}

	sum, err := hashFile(binPath)
	if err != nil {
		report.SkippedErrors++
		logSkip("hashing failed: %v", err)
		return
	}
	blobPath := filepath.Join(blobsDir, sum[:2], sum)

	if r.DryRun {
		r.planDryRun(binPath, blobPath, binStat, blobInodes, report)
		return
	}

	// Ensure the blob exists. link(2) is atomic and creates no partial
	// content: it either adds a name for the existing inode or fails. If a
	// blob with this content hash already exists (created by a concurrent or
	// previous run), verify its size and reuse it.
	created := false
	if err := os.MkdirAll(filepath.Dir(blobPath), dirPerm); err != nil {
		report.SkippedErrors++
		logSkip("creating blob shard: %v", err)
		return
	}
	if linkErr := os.Link(binPath, blobPath); linkErr != nil {
		if !errors.Is(linkErr, fs.ErrExist) {
			if linkUnsupported(linkErr) {
				report.SkippedUnsupported++
			} else {
				report.SkippedErrors++
			}
			logSkip("linking blob: %v", linkErr)
			return
		}
		existingStat, statErr := os.Stat(blobPath)
		if statErr != nil || !existingStat.Mode().IsRegular() || existingStat.Size() != info.Size {
			report.SkippedErrors++
			logSkip("existing blob for hash %s is missing or has an unexpected size", sum)
			return
		}
	} else {
		created = true
		report.BlobsCreated++
		if key, ok := inodeKey(binStat); ok {
			blobInodes[key] = blobPath
		}
	}

	blobStat, err := os.Stat(blobPath)
	if err != nil {
		report.SkippedErrors++
		logSkip("cannot stat blob: %v", err)
		return
	}

	// The upload's binary is already the same inode as the blob (typical for
	// the first copy that the blob was linked from): nothing to switch.
	if os.SameFile(binStat, blobStat) {
		if created {
			report.UploadsDeduplicated++
		} else {
			report.AlreadyLinked++
		}
		return
	}

	// Switching the upload onto the blob merges the second (and further)
	// copy. If the old inode had no other name, its blocks are reclaimed.
	if n, ok := hardLinkCount(binStat); ok && n == 1 {
		report.BytesReclaimed += binStat.Size()
	}

	tmpPath := filepath.Join(filepath.Dir(binPath), "."+filepath.Base(binPath)+dedupeTempMarker+uid.Uid())
	if err := os.Link(blobPath, tmpPath); err != nil {
		if linkUnsupported(err) {
			report.SkippedUnsupported++
		} else {
			report.SkippedErrors++
		}
		logSkip("creating temporary link: %v", err)
		return
	}

	// rename(2) atomically replaces the upload path. The old directory entry
	// disappears only after the new one (with identical content) is in place,
	// so the upload is readable at every point in time.
	if err := os.Rename(tmpPath, binPath); err != nil {
		_ = os.Remove(tmpPath)
		report.SkippedErrors++
		logSkip("switching to blob: %v", err)
		return
	}

	// Without a shared locker a concurrent tusd termination could have
	// removed the .info file while we worked. If so, remove the link we just
	// installed so that we leave the store in the post-termination state
	// instead of an orphan binary.
	if _, err := os.Stat(infoPath); err != nil {
		if removeErr := os.Remove(binPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			r.Logger.Printf("upload %s was terminated during reconciliation and the leftover link could not be removed: %v", filepath.Base(infoPath), removeErr)
		} else {
			r.Logger.Printf("upload %s was terminated during reconciliation; removed the link installed for it", filepath.Base(infoPath))
		}
		return
	}

	report.UploadsDeduplicated++
}

// planDryRun records what a real run would do without touching the filesystem.
func (r Reconciler) planDryRun(binPath, blobPath string, binStat fs.FileInfo, blobInodes map[inodeIdentity]string, report *ReconcileReport) {
	if key, ok := inodeKey(binStat); ok {
		// The fast path above already handled existing links; a key match
		// here is unexpected, stay conservative.
		if _, linked := blobInodes[key]; linked {
			report.AlreadyLinked++
			return
		}
	}

	blobStat, err := os.Stat(blobPath)
	if errors.Is(err, fs.ErrNotExist) {
		// A real run would create the blob from this inode (no space freed by
		// the first copy) and possibly merge later copies in the same run.
		report.BlobsCreated++
		report.UploadsDeduplicated++
		return
	}
	if err != nil || !os.SameFile(binStat, blobStat) {
		report.UploadsDeduplicated++
		if n, ok := hardLinkCount(binStat); ok && n == 1 {
			report.BytesReclaimed += binStat.Size()
		}
	} else {
		report.AlreadyLinked++
	}
}

// cleanupTempLinks removes leftover temporary hard links from crashed runs.
// The blob directory itself is skipped.
func cleanupTempLinks(root, blobsDir string) (int, error) {
	removed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == blobsDir {
			return fs.SkipDir
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".") && strings.Contains(name, dedupeTempMarker) {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
}

// indexBlobs maps the inode of every regular blob file to its path so that
// already-deduplicated uploads can be recognized without re-hashing them. A
// missing blob directory yields an empty map.
func indexBlobs(blobsDir string) (map[inodeIdentity]string, error) {
	inodes := map[inodeIdentity]string{}

	_, err := os.Stat(blobsDir)
	if errors.Is(err, fs.ErrNotExist) {
		return inodes, nil
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: accessing blob directory: %w", err)
	}

	err = filepath.WalkDir(blobsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() == reconcileLockName {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if key, ok := inodeKey(info); ok {
			inodes[key] = path
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("filestore: indexing blobs: %w", err)
	}
	return inodes, nil
}

// listInfoFiles returns the sorted .info file paths below root, excluding the
// blob directory.
func listInfoFiles(root, blobsDir string) ([]string, error) {
	var infoFiles []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == blobsDir {
			return fs.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".info") {
			infoFiles = append(infoFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("filestore: scanning upload directory: %w", err)
	}
	sort.Strings(infoFiles)
	return infoFiles, nil
}

// collectGarbage deletes blob files whose link count is one (only the blob
// directory entry itself remains, so no upload references them anymore) and
// removes emptied shard directories.
func collectGarbage(blobsDir, lockName string, logger *log.Logger) (int, error) {
	removed := 0

	var shardDirs []string
	err := filepath.WalkDir(blobsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != blobsDir {
				shardDirs = append(shardDirs, path)
			}
			return nil
		}
		if d.Name() == lockName {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		nlinks, ok := hardLinkCount(info)
		if !ok {
			// Link counts are unavailable (e.g. Windows): never guess.
			return nil
		}
		if nlinks > 1 {
			return nil
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		logger.Printf("garbage collected unreferenced blob %s", d.Name())
		removed++
		return nil
	})
	if err != nil {
		return removed, fmt.Errorf("filestore: garbage collecting blobs: %w", err)
	}

	// Remove now-empty shard directories (deepest first). Remove only ever
	// succeeds on an empty directory, so populated shards are left alone.
	sort.Slice(shardDirs, func(i, j int) bool { return len(shardDirs[i]) > len(shardDirs[j]) })
	for _, dir := range shardDirs {
		if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
			// A non-empty directory is expected; ignore all remove errors.
			_ = err
		}
	}

	return removed, nil
}

// hashFile computes the lowercase hex SHA-256 of the file at path.
func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// pathWithin reports whether target is located at or below root while staying
// outside blobsDir, using lexical path analysis (custom storage paths from
// pre-create hooks are admin-controlled).
func pathWithin(root, blobsDir, target string) bool {
	absTarget := target
	if !filepath.IsAbs(absTarget) {
		absTarget = filepath.Join(root, absTarget)
	}
	absTarget = filepath.Clean(absTarget)

	if absTarget == root {
		return false
	}
	rel, err := filepath.Rel(root, absTarget)
	if err != nil {
		return false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}

	blobsRel, err := filepath.Rel(root, blobsDir)
	if err == nil && (rel == blobsRel || strings.HasPrefix(rel, blobsRel+string(filepath.Separator))) {
		return false
	}
	return true
}
