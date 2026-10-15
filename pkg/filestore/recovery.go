package filestore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
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
	// defaultRecoveryStateDirName is the name of the hidden directory inside
	// the upload directory that holds the recovery lock, mismatch markers and
	// quarantined metadata.
	defaultRecoveryStateDirName = ".recovery"

	// recoveryLockName serializes concurrent recovery runs.
	recoveryLockName = "lock"

	// quarantineDirName holds corrupt .info files that were replaced during a
	// recovery. Nothing in there is ever deleted automatically.
	quarantineDirName = "quarantine"

	// mismatchesDirName holds one marker file per upload that could not be
	// recovered.
	mismatchesDirName = "mismatches"

	// reportFileName is atomically rewritten after every real recovery run.
	reportFileName = "recovery-report.json"

	// recoveryTempMarker is part of the temporary file name created next to a
	// target .info file before the atomic rename. Stale entries from a crashed
	// run are recognized (and removed) by this marker on the next run.
	recoveryTempMarker = ".__tusd-recover__."

	// defaultRecoveryLockTimeout is how long the recovery waits for a single
	// in-use upload lock before skipping that upload.
	defaultRecoveryLockTimeout = 30 * time.Second
)

// Mismatch reason codes recorded in MismatchMarker.Reason. They are part of
// the on-disk marker format and should not be renamed.
const (
	// ReasonUnsafeID: the receipt's upload ID cannot be mapped safely inside
	// the upload directory (path traversal).
	ReasonUnsafeID = "unsafe-upload-id"
	// ReasonInvalidReceipt: the receipt lacks the fields needed for recovery.
	ReasonInvalidReceipt = "invalid-receipt"
	// ReasonDataMissing: neither a valid metadata file nor the binary data
	// file exists on disk.
	ReasonDataMissing = "data-missing"
	// ReasonDataNotRegular: the binary data path exists but is not a regular
	// file.
	ReasonDataNotRegular = "data-not-regular"
	// ReasonDataOverflow: the binary file is larger than the declared final
	// size recorded in the receipt.
	ReasonDataOverflow = "data-size-overflow"
	// ReasonInfoNotRegular: the metadata path exists but is not a regular
	// file and cannot be quarantined.
	ReasonInfoNotRegular = "info-not-regular"
	// ReasonMetadataConflict: valid metadata exists but contradicts the
	// receipt (declared size or data path). It is never overwritten.
	ReasonMetadataConflict = "metadata-conflict"
)

// ErrRecoveryAlreadyRunning is returned by Recover when another recovery
// process already holds the global lock for the upload directory.
var ErrRecoveryAlreadyRunning = errors.New("filestore: a recovery is already running for this directory")

// LedgerReceipt is one upload record from the upstream hook receipt ledger.
type LedgerReceipt struct {
	// HookType is the hook that produced the receipt ("post-create",
	// "pre-create", "post-terminate", ...) or the empty string for receipts
	// that only contain upload information.
	HookType string
	// Info is the upload information recorded by the upstream system.
	Info handler.FileInfo
	// Line is the 1-based line number of the receipt inside the ledger.
	Line int
}

// MismatchMarker is written for an upload that the ledger records but that
// could not be recovered automatically. Nothing on disk is deleted when a
// marker is written; the marker only flags the upload for manual review.
type MismatchMarker struct {
	// ID is the upload ID from the ledger.
	ID string `json:"id"`
	// Reason is one of the Reason* constants.
	Reason string `json:"reason"`
	// Detail is a human-readable explanation.
	Detail string `json:"detail"`
	// DataPath is the resolved binary data path, when known.
	DataPath string `json:"data_path,omitempty"`
	// DeclaredSize is the final size recorded in the receipt, when known.
	DeclaredSize int64 `json:"declared_size,omitempty"`
	// SizeIsDeferred mirrors the receipt's deferred-length flag.
	SizeIsDeferred bool `json:"size_is_deferred,omitempty"`
	// DataSize is the observed binary file size, when it could be stat'd.
	DataSize int64 `json:"data_size,omitempty"`
	// LedgerLine is the receipt's line number in the ledger.
	LedgerLine int `json:"ledger_line,omitempty"`
	// FirstRecordedAt is when this mismatch was first observed.
	FirstRecordedAt time.Time `json:"first_recorded_at"`
	// LastSeenAt is when this mismatch was last observed.
	LastSeenAt time.Time `json:"last_seen_at"`
}

// RecoveryReport summarizes one recovery run. In dry-run mode the counters
// describe what a real run would do.
type RecoveryReport struct {
	// LedgerEntries is the number of valid create/terminate receipts read.
	LedgerEntries int
	// LedgerCreateReceipts is the number of distinct upload IDs currently
	// considered created according to the ledger.
	LedgerCreateReceipts int
	// LedgerTerminatedIDs is the number of upload IDs whose last ledger
	// record is a termination. They are never recovered.
	LedgerTerminatedIDs int
	// LedgerInvalidLines is the number of ledger lines that could not be
	// parsed or validated.
	LedgerInvalidLines int

	// UntrackedInfoFiles counts metadata files on disk for uploads that the
	// ledger does not know about. They are only observed, never touched.
	UntrackedInfoFiles int
	// UntrackedBinFiles counts binary data files on disk for uploads that
	// the ledger does not know about. They are only observed, never touched.
	UntrackedBinFiles int

	// AlreadyHealthy counts uploads with valid metadata and data that were
	// left untouched.
	AlreadyHealthy int
	// RecoveredMissingInfo counts uploads whose missing metadata was rebuilt.
	RecoveredMissingInfo int
	// RecoveredCorruptInfo counts uploads whose corrupt metadata was moved
	// to quarantine and rebuilt.
	RecoveredCorruptInfo int
	// QuarantinedFiles counts metadata files moved into quarantine.
	QuarantinedFiles int

	// Mismatched counts receipts that could not be recovered.
	Mismatched int
	// MismatchReasons breaks Mismatched down by Reason* code.
	MismatchReasons map[string]int
	// MismatchMarkersWritten counts marker files (re)written.
	MismatchMarkersWritten int
	// MismatchMarkersResolved counts stale markers removed because the
	// upload is healthy or recoverable again.
	MismatchMarkersResolved int

	// LockTimeouts counts uploads skipped because their upload lock could
	// not be acquired within LockTimeout.
	LockTimeouts int
	// SkippedErrors counts uploads skipped due to unexpected I/O errors.
	SkippedErrors int
	// StaleTempFilesRemoved counts leftover temporary files from crashed
	// runs that were cleaned up.
	StaleTempFilesRemoved int
}

// String renders the report as a human-readable summary.
func (r RecoveryReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "ledger entries:               %d\n", r.LedgerEntries)
	fmt.Fprintf(&b, "ledger uploads (created):     %d\n", r.LedgerCreateReceipts)
	fmt.Fprintf(&b, "ledger uploads (terminated):  %d\n", r.LedgerTerminatedIDs)
	fmt.Fprintf(&b, "ledger invalid lines:         %d\n", r.LedgerInvalidLines)
	fmt.Fprintf(&b, "untracked info files (left):  %d\n", r.UntrackedInfoFiles)
	fmt.Fprintf(&b, "untracked data files (left):  %d\n", r.UntrackedBinFiles)
	fmt.Fprintf(&b, "already healthy:              %d\n", r.AlreadyHealthy)
	fmt.Fprintf(&b, "recovered (missing metadata): %d\n", r.RecoveredMissingInfo)
	fmt.Fprintf(&b, "recovered (corrupt metadata): %d\n", r.RecoveredCorruptInfo)
	fmt.Fprintf(&b, "metadata files quarantined:   %d\n", r.QuarantinedFiles)
	fmt.Fprintf(&b, "mismatched (flagged only):    %d\n", r.Mismatched)
	for _, reason := range sortedReasonKeys(r.MismatchReasons) {
		fmt.Fprintf(&b, "  - %-22s %d\n", reason+":", r.MismatchReasons[reason])
	}
	fmt.Fprintf(&b, "mismatch markers written:     %d\n", r.MismatchMarkersWritten)
	fmt.Fprintf(&b, "mismatch markers resolved:    %d\n", r.MismatchMarkersResolved)
	fmt.Fprintf(&b, "skipped (lock timeouts):      %d\n", r.LockTimeouts)
	fmt.Fprintf(&b, "skipped (errors):             %d\n", r.SkippedErrors)
	fmt.Fprintf(&b, "stale temp files removed:     %d\n", r.StaleTempFilesRemoved)
	return b.String()
}

func sortedReasonKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Recovery rebuilds missing or corrupt upload metadata from an upstream hook
// receipt ledger after an unclean shutdown.
//
// The ledger is a JSON Lines file. Each line is either a hook request body as
// tusd sends it to hook endpoints:
//
//	{"Type":"post-create","Event":{"Upload":{"ID":"...","Size":123,...}}}
//
// or a bare upload information object:
//
//	{"ID":"...","Size":123,"MetaData":{"filename":"video.mp4"}}
//
// For every upload recorded as created in the ledger, Recovery compares the
// receipt with the files on disk:
//
//   - Valid metadata and data: left untouched.
//   - Metadata missing/corrupt but binary data present and consistent with
//     the receipt: metadata is rebuilt atomically, so clients can HEAD and
//     resume the upload again. Corrupt metadata is moved into quarantine
//     instead of being deleted.
//   - Receipt and disk disagree (missing data, data larger than declared,
//     conflicting metadata, unsafe ID): flagged with a marker file and left
//     completely untouched.
//
// Uploads whose last ledger record is a termination are not recovered.
// Uploads that are not mentioned in the ledger are never touched.
//
// Recovery is idempotent and crash-safe: all metadata is written to a
// temporary file, fsynced and atomically renamed into place; a global lock
// serializes concurrent runs, and stale temporary files are cleaned up on
// the next run. A failed or interrupted run can therefore never leave
// partial metadata behind.
type Recovery struct {
	// Path is the FileStore directory to recover. It is populated by
	// NewRecovery.
	Path string

	// LedgerPath is the path to the JSON Lines hook receipt ledger.
	LedgerPath string

	// DirModePerm is used when creating directories for uploads. If zero,
	// the store's default is used.
	DirModePerm fs.FileMode

	// FileModePerm is used when writing rebuilt metadata files. If zero, the
	// store's default is used.
	FileModePerm fs.FileMode

	// StateDirName is the name of the hidden directory inside Path holding
	// the lock, mismatch markers and quarantined files. If empty, ".recovery"
	// is used.
	StateDirName string

	// Locker optionally provides the same per-upload locks that tusd uses
	// (e.g. filelocker.New(Path)). When set, the recovery acquires each
	// upload's lock before touching its files, excluding concurrent
	// PATCH/DELETE requests. May be nil.
	Locker handler.Locker

	// LockTimeout bounds the wait for a single in-use upload lock. If zero,
	// 30 seconds is used. A locked upload is skipped for this run and picked
	// up on the next run.
	LockTimeout time.Duration

	// DryRun reports what Recover would do without creating, moving,
	// rewriting or deleting anything. Dry runs do not create the state
	// directory and do not acquire any locks.
	DryRun bool

	// Logger receives per-upload decisions and warnings. If nil, output is
	// discarded.
	Logger *log.Logger
}

// NewRecovery creates a Recovery for the given store and ledger with safe
// defaults (".recovery" state directory, 30s lock timeout).
func NewRecovery(store FileStore, ledgerPath string) Recovery {
	return Recovery{
		Path:         store.Path,
		LedgerPath:   ledgerPath,
		DirModePerm:  store.DirModePerm,
		FileModePerm: store.FileModePerm,
		StateDirName: defaultRecoveryStateDirName,
		LockTimeout:  defaultRecoveryLockTimeout,
		Logger:       log.New(io.Discard, "", 0),
	}
}

// wireReceipt models the accepted ledger line shapes.
type wireReceipt struct {
	// Type and Event are set for full hook request bodies.
	Type string `json:"Type"`
	// Event is present on hook request bodies and carries the upload.
	Event *struct {
		Upload handler.FileInfo `json:"Upload"`
	} `json:"Event"`
	// The promoted fields allow the same line to be a bare FileInfo object.
	handler.FileInfo
}

// ReadReceiptLedger parses a JSON Lines hook receipt ledger. Invalid lines
// are returned through invalidLines and do not abort parsing; a read or
// parse-level failure on the reader itself is returned as error.
func ReadReceiptLedger(r io.Reader) (receipts []LedgerReceipt, invalidLines int, err error) {
	scanner := bufio.NewScanner(r)
	// Receipts can contain sizable metadata, so allow large lines.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(bytes.TrimPrefix(scanner.Bytes(), []byte("\ufeff")))
		if len(line) == 0 || line[0] == '#' {
			continue
		}

		var wire wireReceipt
		if jerr := json.Unmarshal(line, &wire); jerr != nil {
			invalidLines++
			continue
		}

		info := wire.FileInfo
		if wire.Event != nil {
			info = wire.Event.Upload
		}
		if info.ID == "" || info.Size < 0 {
			invalidLines++
			continue
		}

		receipts = append(receipts, LedgerReceipt{
			HookType: wire.Type,
			Info:     info,
			Line:     lineNo,
		})
	}
	if err = scanner.Err(); err != nil {
		return nil, invalidLines, fmt.Errorf("filestore: reading receipt ledger: %w", err)
	}
	return receipts, invalidLines, nil
}

// isTerminationHook reports whether a receipt records the deletion of an
// upload.
func isTerminationHook(hookType string) bool {
	return hookType == "post-terminate" || hookType == "pre-terminate"
}

// ledgerState is the folded view of the ledger: the last receipt per upload
// ID decides whether the upload is created or terminated.
type ledgerState struct {
	active     map[string]LedgerReceipt
	terminated map[string]LedgerReceipt
}

func foldLedger(receipts []LedgerReceipt) ledgerState {
	state := ledgerState{
		active:     map[string]LedgerReceipt{},
		terminated: map[string]LedgerReceipt{},
	}
	for _, receipt := range receipts {
		id := receipt.Info.ID
		if isTerminationHook(receipt.HookType) {
			state.terminated[id] = receipt
			delete(state.active, id)
		} else {
			state.active[id] = receipt
			delete(state.terminated, id)
		}
	}
	return state
}

// Recover runs one idempotent recovery pass and returns a report. A non-nil
// error indicates a global failure (invalid directory or ledger, another run
// in progress); per-upload problems are counted in the report and never abort
// the run.
func (r Recovery) Recover(ctx context.Context) (RecoveryReport, error) {
	report := RecoveryReport{MismatchReasons: map[string]int{}}

	if r.Logger == nil {
		r.Logger = log.New(io.Discard, "", 0)
	}
	if r.StateDirName == "" {
		r.StateDirName = defaultRecoveryStateDirName
	}
	if r.LockTimeout <= 0 {
		r.LockTimeout = defaultRecoveryLockTimeout
	}
	dirPerm := r.DirModePerm
	if dirPerm == 0 {
		dirPerm = os.FileMode(DefaultDirPerm) & os.ModePerm
	}
	filePerm := r.FileModePerm
	if filePerm == 0 {
		filePerm = os.FileMode(DefaultFilePerm) & os.ModePerm
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

	ledgerFile, err := os.Open(r.LedgerPath)
	if err != nil {
		return report, fmt.Errorf("filestore: cannot open receipt ledger %q: %w", r.LedgerPath, err)
	}
	receipts, invalidLines, err := ReadReceiptLedger(ledgerFile)
	_ = ledgerFile.Close()
	if err != nil {
		return report, err
	}
	report.LedgerEntries = len(receipts)
	report.LedgerInvalidLines = invalidLines
	state := foldLedger(receipts)
	report.LedgerCreateReceipts = len(state.active)
	report.LedgerTerminatedIDs = len(state.terminated)

	stateDir := filepath.Join(root, r.StateDirName)
	quarantineDir := filepath.Join(stateDir, quarantineDirName)
	mismatchesDir := filepath.Join(stateDir, mismatchesDirName)
	lockPath := filepath.Join(stateDir, recoveryLockName)

	// A dry run is strictly read-only: no state directory, no global lock
	// and no per-upload locks are acquired, so it never disturbs a live tusd
	// process.
	var globalLock lockfile.Lockfile
	if !r.DryRun {
		if err := os.MkdirAll(stateDir, dirPerm); err != nil {
			return report, fmt.Errorf("filestore: creating recovery state directory: %w", err)
		}
		globalLock = lockfile.Lockfile(lockPath)
		if err := globalLock.TryLock(); err != nil {
			if errors.Is(err, lockfile.ErrBusy) {
				return report, ErrRecoveryAlreadyRunning
			}
			return report, fmt.Errorf("filestore: acquiring recovery lock: %w", err)
		}
		defer func() {
			_ = globalLock.Unlock()
		}()

		removed, cleanupErr := cleanupRecoveryTemp(root)
		if cleanupErr != nil {
			return report, cleanupErr
		}
		report.StaleTempFilesRemoved = removed

		if err := os.MkdirAll(quarantineDir, dirPerm); err != nil {
			return report, fmt.Errorf("filestore: creating quarantine directory: %w", err)
		}
		if err := os.MkdirAll(mismatchesDir, dirPerm); err != nil {
			return report, fmt.Errorf("filestore: creating mismatches directory: %w", err)
		}
	}

	index, err := indexOnDisk(root, stateDir)
	if err != nil {
		return report, err
	}

	// Observe files that belong to uploads the ledger does not know about.
	// They are only counted, never touched.
	for id := range index.infoByID {
		if _, known := state.active[id]; !known {
			report.UntrackedInfoFiles++
		}
	}
	activeBinRels := map[string]struct{}{}
	for _, receipt := range state.active {
		if rel, ok := binRelForReceipt(root, receipt.Info); ok {
			activeBinRels[rel] = struct{}{}
		}
	}
	for _, rel := range index.binRels {
		if _, tracked := activeBinRels[rel]; tracked {
			continue
		}
		if _, known := state.active[rel]; known {
			continue
		}
		report.UntrackedBinFiles++
	}

	ids := make([]string, 0, len(state.active))
	for id := range state.active {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	mismatched := map[string]MismatchMarker{}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		receipt := state.active[id]
		marker := r.processReceipt(ctx, root, quarantineDir, dirPerm, filePerm, receipt, &report)
		if marker != nil {
			mismatched[id] = *marker
		}
	}

	if !r.DryRun {
		written, resolved, err := reconcileMismatchMarkers(mismatchesDir, filePerm, mismatched)
		if err != nil {
			return report, err
		}
		report.MismatchMarkersWritten = written
		report.MismatchMarkersResolved = resolved

		reportData, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return report, err
		}
		if err := writeFileAtomic(filepath.Join(stateDir, reportFileName), dirPerm, filePerm, append(reportData, '\n')); err != nil {
			return report, fmt.Errorf("filestore: writing recovery report: %w", err)
		}
	}

	return report, nil
}

// diskIndex contains the read-only view of the files currently stored below
// the upload directory.
type diskIndex struct {
	// infoByID maps upload ID -> absolute .info path.
	infoByID map[string]string
	// binRels lists binary data files as paths relative to root (POSIX
	// style), excluding all metadata and recovery state.
	binRels []string
}

// indexOnDisk scans root, skipping the recovery state directory.
func indexOnDisk(root, stateDir string) (diskIndex, error) {
	index := diskIndex{infoByID: map[string]string{}}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == stateDir {
			return fs.SkipDir
		}
		if d.IsDir() {
			return nil
		}

		name := d.Name()
		if name == recoveryLockName {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}

		if strings.HasSuffix(name, ".info") {
			id := filepath.ToSlash(strings.TrimSuffix(filepath.ToSlash(rel), ".info"))
			index.infoByID[id] = path
			return nil
		}
		if strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".stop") {
			return nil
		}
		// Hidden files and recovery leftovers are never treated as data.
		if strings.HasPrefix(name, ".") || strings.Contains(name, recoveryTempMarker) {
			return nil
		}
		index.binRels = append(index.binRels, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return index, fmt.Errorf("filestore: scanning upload directory: %w", err)
	}
	return index, nil
}

// binRelForReceipt returns the binary path of a receipt as a slash-separated
// path relative to root when it lives below root.
func binRelForReceipt(root string, info handler.FileInfo) (string, bool) {
	binPath := resolveBinPath(root, info.ID, info)
	rel, err := filepath.Rel(root, binPath)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	return rel, true
}

// safeInfoPath maps an upload ID to its .info path and rejects IDs that would
// escape the upload directory.
func safeInfoPath(root, id string) (string, bool) {
	if id == "" || filepath.IsAbs(id) {
		return "", false
	}
	clean := filepath.Clean(id)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false
	}
	infoPath := filepath.Join(root, filepath.FromSlash(id)+".info")
	rel, err := filepath.Rel(root, infoPath)
	if err != nil {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == ".." || strings.HasPrefix(rel, "../") || rel == ".info" {
		return "", false
	}
	return infoPath, true
}

// resolveBinPath resolves the binary data path exactly like
// FileStore.NewUpload does.
func resolveBinPath(root, id string, info handler.FileInfo) string {
	if info.Storage != nil && info.Storage[StorageKeyPath] != "" {
		p := info.Storage[StorageKeyPath]
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(root, filepath.FromSlash(p))
	}
	return filepath.Join(root, filepath.FromSlash(id))
}

// resolveExistingBinPath resolves the binary path recorded in an existing
// .info file, exactly like FileStore.GetUpload does.
func resolveExistingBinPath(root, id string, info handler.FileInfo) string {
	if info.Storage != nil && info.Storage[StorageKeyPath] != "" {
		return info.Storage[StorageKeyPath]
	}
	return filepath.Join(root, filepath.FromSlash(id))
}

// processReceipt handles a single ledger upload. It returns a non-nil marker
// when the upload could not be recovered; per-upload errors are counted.
func (r Recovery) processReceipt(
	ctx context.Context,
	root, quarantineDir string,
	dirPerm, filePerm fs.FileMode,
	receipt LedgerReceipt,
	report *RecoveryReport,
) *MismatchMarker {
	id := receipt.Info.ID
	logf := func(format string, args ...any) {
		r.Logger.Printf("upload %s "+format, append([]any{id}, args...)...)
	}
	mark := func(reason, detail string, binPath string, binSize int64) *MismatchMarker {
		report.Mismatched++
		report.MismatchReasons[reason]++
		logf("flagged (%s: %s)", reason, detail)
		return &MismatchMarker{
			ID:              id,
			Reason:          reason,
			Detail:          detail,
			DataPath:        binPath,
			DeclaredSize:    receipt.Info.Size,
			SizeIsDeferred:  receipt.Info.SizeIsDeferred,
			DataSize:        binSize,
			LedgerLine:      receipt.Line,
			FirstRecordedAt: time.Now().UTC(),
			LastSeenAt:      time.Now().UTC(),
		}
	}

	infoPath, ok := safeInfoPath(root, id)
	if !ok {
		return mark(ReasonUnsafeID, "upload ID does not map to a path inside the upload directory", "", 0)
	}
	binPath := resolveBinPath(root, id, receipt.Info)

	infoData, infoErr := os.ReadFile(infoPath)
	switch {
	case infoErr == nil:
		// Metadata exists. Unreadable JSON (or an empty "null" document
		// without any of the fields FileStore.NewUpload always writes) is
		// the crash-damage case.
		var existing handler.FileInfo
		jerr := json.Unmarshal(infoData, &existing)
		if jerr != nil || (existing.ID == "" && existing.Storage == nil) {
			return r.recoverCorruptInfo(ctx, quarantineDir, dirPerm, filePerm, receipt, infoPath, binPath, report, mark, logf)
		}
		return r.keepHealthyInfo(root, receipt, existing, infoPath, binPath, report, mark, logf)

	case errors.Is(infoErr, fs.ErrNotExist):
		// Metadata is missing: the main 404-after-crash case.
		return r.recoverMissingInfo(ctx, dirPerm, filePerm, receipt, infoPath, binPath, report, mark, logf)

	default:
		report.SkippedErrors++
		logf("skipped (cannot read metadata: %v)", infoErr)
		return nil
	}
}

// validateData checks the binary data file against the receipt's declared
// size. It returns the file size when the data is usable.
func validateData(receipt handler.FileInfo, binPath string) (int64, error) {
	stat, err := os.Stat(binPath)
	if err != nil {
		return 0, err
	}
	if !stat.Mode().IsRegular() {
		return stat.Size(), errDataNotRegular
	}
	if !receipt.SizeIsDeferred && receipt.Size > 0 && stat.Size() > receipt.Size {
		return stat.Size(), fmt.Errorf("%w: data is %d bytes, declared final size is %d bytes", errDataOverflow, stat.Size(), receipt.Size)
	}
	return stat.Size(), nil
}

var (
	errDataNotRegular = errors.New("data path is not a regular file")
	errDataOverflow   = errors.New("data size exceeds declared size")
)

// acquireUploadLock attempts to take tusd's per-upload lock. It returns a
// release function (nil if no lock was taken) and ok=false when the upload
// must be skipped this run.
func (r Recovery) acquireUploadLock(ctx context.Context, id string, report *RecoveryReport, logf func(string, ...any)) (func(), bool) {
	if r.DryRun || r.Locker == nil {
		return nil, true
	}
	lock, err := r.Locker.NewLock(id)
	if err != nil {
		report.SkippedErrors++
		logf("skipped (cannot create lock: %v)", err)
		return nil, false
	}

	lockCtx, cancel := context.WithTimeout(ctx, r.LockTimeout)
	defer cancel()
	lockErr := lock.Lock(lockCtx, func() {})
	if lockErr != nil {
		if errors.Is(lockErr, handler.ErrNotFound) {
			// The upload's directory does not exist (e.g. slashed ID with
			// no data yet). There is nothing concurrent to protect.
			return nil, true
		}
		report.LockTimeouts++
		logf("skipped (lock not acquired: %v)", lockErr)
		return nil, false
	}
	return func() { _ = lock.Unlock() }, true
}

// recoverMissingInfo rebuilds metadata when the .info file is absent but the
// binary data is present and consistent with the receipt.
func (r Recovery) recoverMissingInfo(
	ctx context.Context,
	dirPerm, filePerm fs.FileMode,
	receipt LedgerReceipt,
	infoPath, binPath string,
	report *RecoveryReport,
	mark func(string, string, string, int64) *MismatchMarker,
	logf func(string, ...any),
) *MismatchMarker {
	binSize, err := validateData(receipt.Info, binPath)
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return mark(ReasonDataMissing, "neither metadata nor binary data found on disk", binPath, 0)
		case errors.Is(err, errDataNotRegular):
			return mark(ReasonDataNotRegular, "binary data path is not a regular file", binPath, binSize)
		case errors.Is(err, errDataOverflow):
			return mark(ReasonDataOverflow, err.Error(), binPath, binSize)
		default:
			report.SkippedErrors++
			logf("skipped (cannot stat binary data: %v)", err)
			return nil
		}
	}

	release, ok := r.acquireUploadLock(ctx, receipt.Info.ID, report, logf)
	if !ok {
		return nil
	}
	if release != nil {
		defer release()
		// Re-check after acquiring the lock: a concurrent request could have
		// created metadata or terminated the upload.
		binSize, err = validateData(receipt.Info, binPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return mark(ReasonDataMissing, "upload disappeared while waiting for its lock", binPath, 0)
			}
			if errors.Is(err, errDataOverflow) {
				return mark(ReasonDataOverflow, err.Error(), binPath, binSize)
			}
			report.SkippedErrors++
			logf("skipped (data changed while waiting for the lock: %v)", err)
			return nil
		}
		if _, statErr := os.Stat(infoPath); statErr == nil {
			// Metadata appeared concurrently; let the next run classify it.
			logf("metadata appeared while waiting for the lock; left untouched")
			return nil
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			report.SkippedErrors++
			logf("skipped (cannot re-stat metadata: %v)", statErr)
			return nil
		}
	}

	info := buildRecoveredInfo(receipt.Info, receipt.Info.ID, infoPath, binPath, binSize)
	if r.DryRun {
		report.RecoveredMissingInfo++
		logf("would recover missing metadata (data on disk: %d bytes)", binSize)
		return nil
	}
	if err := writeInfoAtomic(infoPath, dirPerm, filePerm, info); err != nil {
		report.SkippedErrors++
		logf("skipped (writing metadata: %v)", err)
		return nil
	}

	report.RecoveredMissingInfo++
	logf("recovered missing metadata (data on disk: %d bytes)", binSize)
	return nil
}

// recoverCorruptInfo moves unreadable metadata to quarantine and rebuilds it
// when the binary data matches the receipt. If the data does not validate,
// nothing is moved or overwritten.
func (r Recovery) recoverCorruptInfo(
	ctx context.Context,
	quarantineDir string,
	dirPerm, filePerm fs.FileMode,
	receipt LedgerReceipt,
	infoPath, binPath string,
	report *RecoveryReport,
	mark func(string, string, string, int64) *MismatchMarker,
	logf func(string, ...any),
) *MismatchMarker {
	if st, statErr := os.Stat(infoPath); statErr != nil || !st.Mode().IsRegular() {
		if statErr == nil {
			return mark(ReasonInfoNotRegular, "metadata path is not a regular file", binPath, 0)
		}
		report.SkippedErrors++
		logf("skipped (cannot stat corrupt metadata: %v)", statErr)
		return nil
	}

	binSize, err := validateData(receipt.Info, binPath)
	if err != nil {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return mark(ReasonDataMissing, "metadata is corrupt and binary data is missing; metadata kept in place", binPath, 0)
		case errors.Is(err, errDataNotRegular):
			return mark(ReasonDataNotRegular, "binary data path is not a regular file; metadata kept in place", binPath, binSize)
		case errors.Is(err, errDataOverflow):
			return mark(ReasonDataOverflow, err.Error()+"; metadata kept in place", binPath, binSize)
		default:
			report.SkippedErrors++
			logf("skipped (cannot stat binary data: %v)", err)
			return nil
		}
	}

	release, ok := r.acquireUploadLock(ctx, receipt.Info.ID, report, logf)
	if !ok {
		return nil
	}
	if release != nil {
		defer release()
		binSize, err = validateData(receipt.Info, binPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return mark(ReasonDataMissing, "upload disappeared while waiting for its lock; metadata kept in place", binPath, 0)
			}
			if errors.Is(err, errDataOverflow) {
				return mark(ReasonDataOverflow, err.Error()+"; metadata kept in place", binPath, binSize)
			}
			report.SkippedErrors++
			logf("skipped (data changed while waiting for the lock: %v)", err)
			return nil
		}
	}

	info := buildRecoveredInfo(receipt.Info, receipt.Info.ID, infoPath, binPath, binSize)
	if r.DryRun {
		report.RecoveredCorruptInfo++
		report.QuarantinedFiles++
		logf("would quarantine corrupt metadata and rebuild it (data on disk: %d bytes)", binSize)
		return nil
	}

	quarantinedPath, sidecarPath, err := prepareQuarantine(quarantineDir, dirPerm, filePerm, receipt, binPath, binSize)
	if err != nil {
		report.SkippedErrors++
		logf("skipped (preparing quarantine: %v); corrupt metadata left in place", err)
		return nil
	}

	if err := swapInRebuiltInfo(infoPath, quarantinedPath, dirPerm, filePerm, info); err != nil {
		report.SkippedErrors++
		logf("skipped (rebuilding metadata: %v); corrupt metadata left in place", err)
		return nil
	}

	report.QuarantinedFiles++
	report.RecoveredCorruptInfo++
	logf("quarantined corrupt metadata to %s (sidecar: %s) and rebuilt it (data on disk: %d bytes)", quarantinedPath, sidecarPath, binSize)
	return nil
}

// keepHealthyInfo verifies existing valid metadata against the receipt. It is
// never overwritten; a contradiction is flagged instead.
func (r Recovery) keepHealthyInfo(
	root string,
	receipt LedgerReceipt,
	existing handler.FileInfo,
	infoPath, binPath string,
	report *RecoveryReport,
	mark func(string, string, string, int64) *MismatchMarker,
	logf func(string, ...any),
) *MismatchMarker {
	id := receipt.Info.ID

	if existing.ID != "" && existing.ID != id {
		return mark(ReasonMetadataConflict, fmt.Sprintf("metadata records upload ID %q, ledger records %q; metadata left untouched", existing.ID, id), binPath, 0)
	}

	existingBinPath := resolveExistingBinPath(root, id, existing)
	receiptBinPath := binPath
	if receipt.Info.Storage != nil && receipt.Info.Storage[StorageKeyPath] != "" {
		if filepath.Clean(existingBinPath) != filepath.Clean(receiptBinPath) {
			return mark(ReasonMetadataConflict, fmt.Sprintf("metadata points at data path %q, ledger at %q; metadata left untouched", existingBinPath, receiptBinPath), existingBinPath, 0)
		}
	}

	// Declared-size compatibility. A concrete size in the ledger must agree
	// with concrete metadata. A deferred ledger size with concrete metadata
	// is normal: the length was declared after creation.
	if !receipt.Info.SizeIsDeferred && receipt.Info.Size > 0 {
		switch {
		case existing.SizeIsDeferred:
			return mark(ReasonMetadataConflict, fmt.Sprintf("ledger records declared size %d but metadata still has deferred length; metadata left untouched", receipt.Info.Size), existingBinPath, 0)
		case existing.Size > 0 && existing.Size != receipt.Info.Size:
			return mark(ReasonMetadataConflict, fmt.Sprintf("metadata declares size %d, ledger records %d; metadata left untouched", existing.Size, receipt.Info.Size), existingBinPath, 0)
		}
	}

	stat, err := os.Stat(existingBinPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return mark(ReasonDataMissing, "valid metadata but binary data missing; metadata left untouched", existingBinPath, 0)
		}
		report.SkippedErrors++
		logf("skipped (cannot stat binary data: %v)", err)
		return nil
	}
	if !stat.Mode().IsRegular() {
		return mark(ReasonDataNotRegular, "binary data path is not a regular file; metadata left untouched", existingBinPath, stat.Size())
	}
	if !existing.SizeIsDeferred && existing.Size > 0 && stat.Size() > existing.Size {
		return mark(ReasonDataOverflow, fmt.Sprintf("binary data is %d bytes, metadata declares %d; metadata left untouched", stat.Size(), existing.Size), existingBinPath, stat.Size())
	}

	if r.DryRun {
		report.AlreadyHealthy++
		return nil
	}
	report.AlreadyHealthy++
	return nil
}

// buildRecoveredInfo constructs the canonical metadata written to a rebuilt
// .info file. The offset is taken from the actual file size, mirroring
// FileStore.GetUpload.
func buildRecoveredInfo(receipt handler.FileInfo, id, infoPath, binPath string, binSize int64) handler.FileInfo {
	info := receipt
	info.ID = id
	info.Offset = binSize
	info.Storage = map[string]string{
		"Type":             "filestore",
		StorageKeyPath:     binPath,
		StorageKeyInfoPath: infoPath,
	}
	return info
}

// writeInfoAtomic marshals info and writes it atomically.
func writeInfoAtomic(infoPath string, dirPerm, filePerm fs.FileMode, info handler.FileInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return writeFileAtomic(infoPath, dirPerm, filePerm, data)
}

// quarantineSidecar describes a quarantined metadata file.
type quarantineSidecar struct {
	ID             string    `json:"id"`
	QuarantinedAt  time.Time `json:"quarantined_at"`
	Reason         string    `json:"reason"`
	DataPath       string    `json:"data_path"`
	ObservedSize   int64     `json:"observed_data_size"`
	DeclaredSize   int64     `json:"declared_size,omitempty"`
	SizeIsDeferred bool      `json:"size_is_deferred,omitempty"`
	LedgerLine     int       `json:"ledger_line,omitempty"`
}

// prepareQuarantine picks a unique quarantine destination and writes a JSON
// sidecar describing the incident. No upload file is touched yet, so a
// failure here leaves everything in place.
func prepareQuarantine(quarantineDir string, dirPerm, filePerm fs.FileMode, receipt LedgerReceipt, binPath string, binSize int64) (string, string, error) {
	if err := os.MkdirAll(quarantineDir, dirPerm); err != nil {
		return "", "", err
	}

	nameBase := markerID(receipt.Info.ID) + "." + time.Now().UTC().Format("20060102T150405.000000000Z")
	dst := filepath.Join(quarantineDir, nameBase+".info")
	for n := 1; ; n++ {
		if _, err := os.Stat(dst); errors.Is(err, fs.ErrNotExist) {
			break
		} else if err != nil {
			return "", "", err
		}
		dst = filepath.Join(quarantineDir, fmt.Sprintf("%s-%d.info", nameBase, n))
	}
	sidecarPath := strings.TrimSuffix(dst, ".info") + ".json"

	sidecar := quarantineSidecar{
		ID:             receipt.Info.ID,
		QuarantinedAt:  time.Now().UTC(),
		Reason:         "metadata unreadable, rebuilt from hook receipt ledger",
		DataPath:       binPath,
		ObservedSize:   binSize,
		DeclaredSize:   receipt.Info.Size,
		SizeIsDeferred: receipt.Info.SizeIsDeferred,
		LedgerLine:     receipt.Line,
	}
	data, err := json.MarshalIndent(sidecar, "", "  ")
	if err != nil {
		return "", "", err
	}
	if err := writeFileAtomic(sidecarPath, dirPerm, filePerm, append(data, '\n')); err != nil {
		return "", "", err
	}
	return dst, sidecarPath, nil
}

// swapInRebuiltInfo stages the rebuilt metadata, atomically moves the corrupt
// metadata to its quarantine destination and installs the rebuilt file.
//
// The observable state transitions are: corrupt .info (already unusable) ->
// missing .info (same 404/error behavior) -> valid .info. If the process
// crashes between the two renames, the next run finds the binary data plus
// the receipt and rebuilds again; the corrupt bytes are preserved in
// quarantine. On failure of the final rename the corrupt metadata is moved
// back so a failed run never changes observable state.
func swapInRebuiltInfo(infoPath, quarantinePath string, dirPerm, filePerm fs.FileMode, info handler.FileInfo) error {
	dir := filepath.Dir(infoPath)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}

	tmpPath := filepath.Join(dir, "."+filepath.Base(infoPath)+recoveryTempMarker+uid.Uid())
	if err := writeFileAtomicPath(tmpPath, filePerm, mustMarshalInfo(info)); err != nil {
		return err
	}

	if err := os.Rename(infoPath, quarantinePath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("moving corrupt metadata to quarantine: %w", err)
	}
	if err := os.Rename(tmpPath, infoPath); err != nil {
		if rbErr := os.Rename(quarantinePath, infoPath); rbErr != nil {
			return fmt.Errorf("installing rebuilt metadata: %v (rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("installing rebuilt metadata: %w (corrupt metadata restored)", err)
	}
	syncDir(dir)
	return nil
}

func mustMarshalInfo(info handler.FileInfo) []byte {
	data, err := json.Marshal(info)
	if err != nil {
		panic(err)
	}
	return data
}

// writeFileAtomic writes content to path via a temporary file in the same
// directory, fsync and an atomic rename. The destination directory must
// already exist.
func writeFileAtomic(path string, dirPerm, filePerm fs.FileMode, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return err
	}
	tmpPath := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+recoveryTempMarker+uid.Uid())
	if err := writeFileAtomicPath(tmpPath, filePerm, content); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

// writeFileAtomicPath creates a fully written, fsynced file at tmpPath.
func writeFileAtomicPath(tmpPath string, filePerm fs.FileMode, content []byte) error {
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	if _, err := f.Write(content); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// cleanupRecoveryTemp removes leftover temporary files from crashed runs.
func cleanupRecoveryTemp(root string) (int, error) {
	removed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), recoveryTempMarker) {
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, fmt.Errorf("filestore: cleaning stale recovery temp files: %w", err)
	}
	return removed, nil
}

// markerID maps an upload ID to a safe, collision-free file name component.
func markerID(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

// reconcileMismatchMarkers (re)writes one marker per currently mismatched
// upload and removes markers of uploads that no longer mismatch.
func reconcileMismatchMarkers(dir string, filePerm fs.FileMode, current map[string]MismatchMarker) (written, resolved int, err error) {
	if err := os.MkdirAll(dir, filePerm); err != nil {
		return 0, 0, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("filestore: reading mismatch markers: %w", err)
	}
	existing := map[string]fs.DirEntry{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		existing[entry.Name()] = entry
	}

	currentNames := map[string]struct{}{}
	now := time.Now().UTC()
	for id, marker := range current {
		name := markerID(id) + ".json"
		currentNames[name] = struct{}{}

		// Preserve the first-seen timestamp of an ongoing mismatch.
		if old, ok := existing[name]; ok && !old.IsDir() {
			if oldData, readErr := os.ReadFile(filepath.Join(dir, name)); readErr == nil {
				var oldMarker MismatchMarker
				if json.Unmarshal(oldData, &oldMarker) == nil && !oldMarker.FirstRecordedAt.IsZero() {
					marker.FirstRecordedAt = oldMarker.FirstRecordedAt
				}
			}
		}
		if marker.FirstRecordedAt.IsZero() {
			marker.FirstRecordedAt = now
		}
		marker.LastSeenAt = now

		data, mErr := json.MarshalIndent(marker, "", "  ")
		if mErr != nil {
			return written, resolved, mErr
		}
		if wErr := writeFileAtomic(filepath.Join(dir, name), 0, filePerm, append(data, '\n')); wErr != nil {
			return written, resolved, wErr
		}
		written++
	}

	for name := range existing {
		if _, keep := currentNames[name]; keep {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return written, resolved, err
		}
		resolved++
	}
	return written, resolved, nil
}
