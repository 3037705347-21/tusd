package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/exp/slog"

	"github.com/tus/tusd/v2/internal/uid"
	"github.com/tus/tusd/v2/pkg/handler"
)

// hashBufferSize is the fixed size of the buffer used to stream upload
// contents through the SHA-256 hasher, so that memory usage is independent of
// the upload size.
const hashBufferSize = 256 * 1024

// defaultLockTimeout is used when Config.LockTimeout is not set.
const defaultLockTimeout = 20 * time.Second

// Store is the subset of data store functionality required by a verification
// run. filestore.FileStore satisfies this interface.
type Store interface {
	// GetUpload fetches the upload like handler.DataStore.GetUpload.
	GetUpload(ctx context.Context, id string) (handler.Upload, error)
	// QuarantineUpload moves the upload's data file into quarantineDir.
	QuarantineUpload(ctx context.Context, id, quarantineDir string) error
}

// ledgerAppender is the ledger surface used by a run. It is a small seam that
// allows tests to inject a failing ledger without touching the real file.
type ledgerAppender interface {
	Append(event Event) error
	Close() error
}

// Config configures a Verifier.
type Config struct {
	// ManifestPath is the path to the JSON manifest to verify against.
	ManifestPath string
	// LedgerPath is the path to the append-only JSONL ledger.
	LedgerPath string
	// QuarantineDir is the directory into which mismatched data files are
	// moved. It must reside on the same file system as the uploads.
	QuarantineDir string
	// Store provides access to the stored uploads. Required.
	Store Store
	// Locker optionally serializes verification of an upload against
	// concurrent uploads and downloads. nil disables locking.
	Locker handler.Locker
	// LockTimeout limits how long the verifier waits for an upload lock.
	// Defaults to 20s.
	LockTimeout time.Duration
	// Logger is used for structured run logging. Defaults to slog.Default().
	Logger *slog.Logger
}

// Verifier runs manifest-driven content verification passes.
type Verifier struct {
	config Config
	logger *slog.Logger

	// openLedger opens the append-only ledger. It defaults to OpenLedger and
	// exists as an unexported seam for failure-injection tests.
	openLedger func(path string) (ledgerAppender, error)
}

// New creates a Verifier after validating its configuration.
func New(config Config) (*Verifier, error) {
	if config.ManifestPath == "" {
		return nil, errors.New("verification: ManifestPath is required")
	}
	if config.LedgerPath == "" {
		return nil, errors.New("verification: LedgerPath is required")
	}
	if config.QuarantineDir == "" {
		return nil, errors.New("verification: QuarantineDir is required")
	}
	if config.Store == nil {
		return nil, errors.New("verification: Store is required")
	}
	if config.LockTimeout <= 0 {
		config.LockTimeout = defaultLockTimeout
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	v := &Verifier{
		config: config,
		logger: config.Logger,
	}
	v.openLedger = func(path string) (ledgerAppender, error) {
		return OpenLedger(path)
	}

	return v, nil
}

// Report summarizes the outcome of a verification run.
type Report struct {
	// RunID uniquely identifies this run and is attached to every event.
	RunID string
	// Total is the number of entries in the manifest.
	Total int
	// Verified is the number of uploads whose size and digest matched.
	Verified int
	// Quarantined is the number of uploads that mismatched and were moved
	// into quarantine.
	Quarantined int
	// Missing is the number of manifest entries without a corresponding
	// upload.
	Missing int
	// Unfinished is the number of uploads that have not been completed yet.
	Unfinished int
	// Errors is the number of entries that could not be verified due to a
	// lock or I/O error.
	Errors int
	// Skipped is the number of entries that were not re-checked because they
	// were unchanged since the last successful verification or are already
	// quarantined (sticky).
	Skipped int
}

// Run performs one verification pass.
//
// The order is deliberately strict: the manifest is fully loaded and validated
// before anything is written; the ledger is opened and replayed before the
// first upload is touched; for every mismatched upload the quarantine decision
// is appended and fsynced to the ledger before the data file is moved. A
// manifest, ledger or quarantine-move failure aborts the run with an error and
// without performing any further state changes.
//
// Besides the report, Run returns the StatusStore reconstructed from the
// ledger, which can be used to block downloads of quarantined uploads.
func (v *Verifier) Run(ctx context.Context) (*Report, *StatusStore, error) {
	// Phase 1: load the manifest completely. No state has been touched yet at
	// this point, so any failure here has zero side effects.
	manifest, err := LoadManifest(v.config.ManifestPath)
	if err != nil {
		return nil, nil, err
	}

	// Phase 2: open the ledger and reconstruct the current state. No uploads
	// are touched before this succeeds.
	ledger, err := v.openLedger(v.config.LedgerPath)
	if err != nil {
		return nil, nil, err
	}
	defer ledger.Close()

	status, err := ReplayLedger(v.config.LedgerPath)
	if err != nil {
		return nil, nil, err
	}

	runID := newRunID()
	report := &Report{RunID: runID, Total: len(manifest.Entries)}

	v.logger.InfoContext(ctx, "VerificationRunStart",
		"run_id", runID,
		"entries", report.Total,
		"manifest", v.config.ManifestPath,
		"ledger", v.config.LedgerPath,
		"quarantine_dir", v.config.QuarantineDir,
	)

	// Phase 3: verify entries in manifest order.
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return report, status, fmt.Errorf("verification: run %s cancelled: %w", runID, err)
		}

		if previous, ok := status.Status(entry.ID); ok {
			switch {
			case previous.Status == StatusQuarantined:
				// Sticky quarantine: never re-check, never restore.
				report.Skipped++
				v.logger.InfoContext(ctx, "VerificationEntrySkipped",
					"run_id", runID, "id", entry.ID, "reason", "already-quarantined")
				continue
			case previous.Status == StatusVerified &&
				previous.ExpectedSize == entry.Size &&
				previous.ExpectedDigest == entry.Digest:
				// Incremental run: an unchanged entry that verified before is
				// not read again and produces no new event.
				report.Skipped++
				v.logger.InfoContext(ctx, "VerificationEntrySkipped",
					"run_id", runID, "id", entry.ID, "reason", "unchanged")
				continue
			}
		}

		event, recorded, fatalErr := v.processEntry(ctx, runID, entry, ledger)
		if recorded {
			// Keep the in-memory current state in sync with the durable
			// ledger. On a ledger write failure the event was not persisted,
			// so the state must not be updated.
			status.events[entry.ID] = event

			switch event.Status {
			case StatusVerified:
				report.Verified++
			case StatusQuarantined:
				report.Quarantined++
			case StatusMissing:
				report.Missing++
			case StatusUnfinished:
				report.Unfinished++
			case StatusError:
				report.Errors++
			}

			v.logger.InfoContext(ctx, "VerificationEntryChecked",
				"run_id", runID, "id", entry.ID, "status", event.Status, "detail", event.Detail)
		}
		if fatalErr != nil {
			return report, status, fatalErr
		}
	}

	v.logger.InfoContext(ctx, "VerificationRunComplete",
		"run_id", runID,
		"total", report.Total,
		"verified", report.Verified,
		"quarantined", report.Quarantined,
		"missing", report.Missing,
		"unfinished", report.Unfinished,
		"errors", report.Errors,
		"skipped", report.Skipped,
	)

	return report, status, nil
}

// processEntry locks the upload (if a locker is configured), inspects it,
// records the outcome durably in the ledger and, if the upload mismatched,
// moves its data into quarantine while the lock is held.
//
// The boolean return value reports whether the event was durably recorded in
// the ledger. The error is fatal to the run (ledger write or quarantine move
// failed); per-upload inspection problems are encoded in an error event
// instead.
func (v *Verifier) processEntry(ctx context.Context, runID string, entry Entry, ledger ledgerAppender) (Event, bool, error) {
	var lock handler.Lock
	if v.config.Locker != nil {
		var locked bool
		var preEvent Event
		lock, locked, preEvent = v.acquireLock(ctx, runID, entry)
		if !locked {
			// Missing/error determined while trying to lock; record and move
			// on to the next entry.
			return v.record(ledger, preEvent)
		}
	}
	if lock != nil {
		defer func() { _ = lock.Unlock() }()
	}

	event := v.inspectUpload(ctx, runID, entry)

	// Safety order for mismatches: persist the decision first, only then move
	// the file. If the ledger write fails, no state is changed.
	recorded, _, err := v.record(ledger, event)
	if err != nil {
		return recorded, false, err
	}

	if event.Status != StatusQuarantined {
		return recorded, true, nil
	}

	if err := v.config.Store.QuarantineUpload(ctx, entry.ID, v.config.QuarantineDir); err != nil {
		return recorded, true, fmt.Errorf("verification: failed to quarantine upload %q after recording ledger event: %w", entry.ID, err)
	}

	return recorded, true, nil
}

// acquireLock attempts to acquire the upload lock. If the lock cannot be
// acquired because the upload does not exist (file-based lockers report
// ErrNotFound when the lock's parent directory is missing) a missing event is
// returned. Other lock problems are returned as error events.
func (v *Verifier) acquireLock(ctx context.Context, runID string, entry Entry) (handler.Lock, bool, Event) {
	lock, err := v.config.Locker.NewLock(entry.ID)
	if err != nil {
		event := newEvent(runID, entry, StatusError)
		event.Detail = "failed to create upload lock: " + err.Error()
		return nil, false, event
	}

	lockCtx, cancel := context.WithTimeout(ctx, v.config.LockTimeout)
	defer cancel()

	// A no-op release callback is intentional: the verifier holds the lock
	// only for the duration of the check and releases it itself. A nil
	// callback could panic the file locker's polling goroutine if another
	// process requested release.
	if err := lock.Lock(lockCtx, func() {}); err != nil {
		if errors.Is(err, handler.ErrNotFound) {
			if _, getErr := v.config.Store.GetUpload(ctx, entry.ID); errors.Is(getErr, handler.ErrNotFound) {
				return nil, false, newEvent(runID, entry, StatusMissing)
			}
		}
		event := newEvent(runID, entry, StatusError)
		event.Detail = "failed to acquire upload lock: " + err.Error()
		return nil, false, event
	}

	return lock, true, Event{}
}

// inspectUpload reads and hashes the upload while its lock is held and returns
// the resulting event without touching the ledger.
func (v *Verifier) inspectUpload(ctx context.Context, runID string, entry Entry) Event {
	event := newEvent(runID, entry, StatusError)

	upload, err := v.config.Store.GetUpload(ctx, entry.ID)
	if err != nil {
		if errors.Is(err, handler.ErrNotFound) {
			return newEvent(runID, entry, StatusMissing)
		}
		event.Detail = "failed to open upload: " + err.Error()
		return event
	}

	info, err := upload.GetInfo(ctx)
	if err != nil {
		event.Detail = "failed to read upload info: " + err.Error()
		return event
	}

	// Only fully completed uploads can be verified against their full digest.
	if info.SizeIsDeferred || info.Offset != info.Size {
		unfinished := newEvent(runID, entry, StatusUnfinished)
		unfinished.ActualSize = info.Offset
		unfinished.Detail = fmt.Sprintf("upload is not finished (offset=%d size=%d deferred=%t)", info.Offset, info.Size, info.SizeIsDeferred)
		return unfinished
	}

	event.ActualSize = info.Offset

	// Compare the size first. A mismatch quarantines without spending time on
	// hashing the content.
	if info.Offset != entry.Size {
		mismatch := newEvent(runID, entry, StatusQuarantined)
		mismatch.ActualSize = info.Offset
		mismatch.Detail = fmt.Sprintf("size mismatch: expected %d, got %d", entry.Size, info.Offset)
		return mismatch
	}

	reader, err := upload.GetReader(ctx)
	if err != nil {
		event.Detail = "failed to open upload content: " + err.Error()
		return event
	}
	defer reader.Close()

	hasher := sha256.New()
	buffer := make([]byte, hashBufferSize)
	if _, err := io.CopyBuffer(hasher, reader, buffer); err != nil {
		event.Detail = "failed to read upload content: " + err.Error()
		return event
	}

	actualDigest := hex.EncodeToString(hasher.Sum(nil))
	event.ActualDigest = actualDigest

	if actualDigest != entry.Digest {
		event.Status = StatusQuarantined
		event.Detail = "digest mismatch"
		return event
	}

	event.Status = StatusVerified
	return event
}

// record appends the event to the ledger. The boolean indicates whether the
// event was durably persisted. On append failure the error is returned so the
// run can abort without performing any further state changes.
func (v *Verifier) record(ledger ledgerAppender, event Event) (Event, bool, error) {
	if err := ledger.Append(event); err != nil {
		return event, false, fmt.Errorf("verification: aborting run after ledger write failure for %q: %w", event.ID, err)
	}
	return event, true, nil
}

// newEvent constructs an Event with the expected values taken from the
// manifest entry.
func newEvent(runID string, entry Entry, status string) Event {
	return Event{
		RunID:          runID,
		ID:             entry.ID,
		ExpectedSize:   entry.Size,
		ExpectedDigest: entry.Digest,
		Status:         status,
	}
}

// newRunID generates a unique identifier for a verification run.
func newRunID() string {
	return fmt.Sprintf("%d-%s", time.Now().UTC().UnixNano(), uid.Uid()[:12])
}
