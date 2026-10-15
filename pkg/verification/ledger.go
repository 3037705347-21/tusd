package verification

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/tus/tusd/v2/pkg/handler"
)

// Ledger status values.
const (
	// StatusVerified means the stored upload's size and digest matched the
	// manifest.
	StatusVerified = "verified"
	// StatusQuarantined means the stored upload did not match the manifest and
	// its data has been moved into quarantine.
	StatusQuarantined = "quarantined"
	// StatusMissing means the manifest references an upload that does not exist
	// in the store.
	StatusMissing = "missing"
	// StatusUnfinished means the upload exists but has not been completed yet,
	// so its full content cannot be verified.
	StatusUnfinished = "unfinished"
	// StatusError means the upload could not be verified due to a lock or I/O
	// error; it is not quarantined.
	StatusError = "error"
)

var validStatuses = map[string]struct{}{
	StatusVerified:    {},
	StatusQuarantined: {},
	StatusMissing:     {},
	StatusUnfinished:  {},
	StatusError:       {},
}

// ledgerDirPerm and ledgerFilePerm are used when creating the ledger file and
// its parent directory.
const (
	ledgerDirPerm  fs.FileMode = 0775
	ledgerFilePerm fs.FileMode = 0664
)

// Event is a single, append-only ledger record describing the verification
// outcome for one upload in one run.
type Event struct {
	// Time is the UTC timestamp at which the event was recorded.
	Time time.Time `json:"time"`
	// RunID identifies the verification run that produced the event.
	RunID string `json:"run_id"`
	// ID is the upload ID that was verified.
	ID string `json:"id"`
	// ExpectedSize and ExpectedDigest are the manifest values.
	ExpectedSize int64 `json:"expected_size"`
	// ExpectedDigest is the lower-case hex-encoded expected SHA-256 digest.
	ExpectedDigest string `json:"expected_digest"`
	// ActualSize is the stored content's size, when available.
	ActualSize int64 `json:"actual_size,omitempty"`
	// ActualDigest is the stored content's lower-case hex-encoded SHA-256
	// digest, when computed. It is empty when the size already mismatched and
	// hashing was skipped.
	ActualDigest string `json:"actual_digest,omitempty"`
	// Status is one of the Status* constants.
	Status string `json:"status"`
	// Detail contains additional human-readable information, e.g. an error
	// reason.
	Detail string `json:"detail,omitempty"`
}

// Ledger is an append-only JSONL ledger of verification events. Every append
// is flushed to disk (fsync) before the method returns, so callers can rely on
// the event being durable before performing follow-up state changes such as
// moving a file into quarantine.
type Ledger struct {
	path string
	file *os.File
}

// OpenLedger opens (creating it if necessary) the ledger file at the given
// path. Parent directories are created as needed. Failure to create or open the
// ledger is returned as an error before any uploads are touched.
func OpenLedger(path string) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Dir(path), ledgerDirPerm); err != nil {
		return nil, fmt.Errorf("verification: failed to create ledger directory for %q: %w", path, err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, ledgerFilePerm)
	if err != nil {
		return nil, fmt.Errorf("verification: failed to open ledger %q: %w", path, err)
	}

	return &Ledger{path: path, file: file}, nil
}

// Path returns the ledger file path.
func (l *Ledger) Path() string {
	return l.path
}

// Append writes a single event as one JSON line and fsyncs the file. If Time is
// zero it is set to the current UTC time. Any encoding, write or sync error is
// returned; callers must treat this as fatal and must not perform the state
// change the event describes.
func (l *Ledger) Append(event Event) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("verification: failed to encode ledger event for %q: %w", event.ID, err)
	}
	data = append(data, '\n')

	n, err := l.file.Write(data)
	if err != nil {
		return fmt.Errorf("verification: failed to append ledger event for %q: %w", event.ID, err)
	}
	if n != len(data) {
		return fmt.Errorf("verification: short write appending ledger event for %q: wrote %d of %d bytes", event.ID, n, len(data))
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("verification: failed to sync ledger after event for %q: %w", event.ID, err)
	}

	return nil
}

// Close releases the underlying file handle.
func (l *Ledger) Close() error {
	return l.file.Close()
}

// StatusStore is the current verification state reconstructed by replaying the
// ledger from the beginning: for each upload ID only the last event counts.
type StatusStore struct {
	events map[string]Event
}

// Compile-time check that StatusStore can be used as a handler.DownloadBlocker.
var _ handler.DownloadBlocker = (*StatusStore)(nil)

// Status returns the last recorded event for the given upload ID.
func (s *StatusStore) Status(id string) (Event, bool) {
	event, ok := s.events[id]
	return event, ok
}

// IsQuarantined reports whether the upload's last recorded status is
// quarantined.
func (s *StatusStore) IsQuarantined(id string) bool {
	event, ok := s.events[id]
	return ok && event.Status == StatusQuarantined
}

// DownloadBlocked implements handler.DownloadBlocker. Uploads whose last ledger
// status is quarantined are refused regardless of their physical location.
func (s *StatusStore) DownloadBlocked(_ context.Context, id string) (bool, error) {
	return s.IsQuarantined(id), nil
}

// ReplayLedger reads the ledger from the beginning and reconstructs the current
// status. Parsing is fail-closed: any malformed line, unknown status, missing
// required field or invalid digest aborts the replay with an error instead of
// silently ignoring potentially tampered state. A non-existent ledger is
// treated as an empty store.
func ReplayLedger(path string) (*StatusStore, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &StatusStore{events: map[string]Event{}}, nil
		}
		return nil, fmt.Errorf("verification: failed to open ledger %q for replay: %w", path, err)
	}
	defer file.Close()

	store := &StatusStore{events: make(map[string]Event)}
	scanner := bufio.NewScanner(file)
	// Allow reasonably large single lines; default 64KB is plenty, but set an
	// explicit larger limit for robustness.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Bytes()
		if len(line) == 0 {
			return nil, fmt.Errorf("verification: empty line %d in ledger %q", lineNo, path)
		}

		var event Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("verification: malformed JSON on line %d in ledger %q: %w", lineNo, path, err)
		}

		if err := validateEvent(event, lineNo, path); err != nil {
			return nil, err
		}

		// Sticky quarantine is also enforced against a tampered ledger: once
		// an upload has been recorded as quarantined, no subsequent line may
		// move it back to another status.
		if previous, exists := store.events[event.ID]; exists &&
			previous.Status == StatusQuarantined && event.Status != StatusQuarantined {
			return nil, fmt.Errorf("verification: event on line %d in ledger %q regresses id %q from quarantined to %q", lineNo, path, event.ID, event.Status)
		}

		store.events[event.ID] = event
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("verification: failed to read ledger %q: %w", path, err)
	}

	return store, nil
}

func validateEvent(event Event, lineNo int, path string) error {
	if event.ID == "" {
		return fmt.Errorf("verification: event on line %d in ledger %q has an empty id", lineNo, path)
	}
	if _, ok := validStatuses[event.Status]; !ok {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has invalid status %q", event.ID, lineNo, path, event.Status)
	}
	if event.RunID == "" {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has an empty run_id", event.ID, lineNo, path)
	}
	if event.Time.IsZero() {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has an empty timestamp", event.ID, lineNo, path)
	}
	if event.ExpectedSize < 0 {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has a negative expected size", event.ID, lineNo, path)
	}
	if event.ActualSize < 0 {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has a negative actual size", event.ID, lineNo, path)
	}
	if err := validateSHA256Digest(event.ExpectedDigest); err != nil {
		return fmt.Errorf("verification: event for %q on line %d in ledger %q has an invalid expected digest: %w", event.ID, lineNo, path, err)
	}
	if event.ActualDigest != "" {
		if err := validateSHA256Digest(event.ActualDigest); err != nil {
			return fmt.Errorf("verification: event for %q on line %d in ledger %q has an invalid actual digest: %w", event.ID, lineNo, path, err)
		}
	}
	return nil
}
