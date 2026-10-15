// Package migrate provides tooling for moving uploads of the filestore
// backend from one local directory ("old disk") to another ("new disk") in
// batches.
//
// It consists of three parts:
//
//   - Journal, which durably records the migration state of every upload in
//     the new directory.
//   - Migrator, which copies uploads in resumable batches, verifies the new
//     copies (size and, by default, SHA-256) before flipping their state to
//     promoted, and can reconcile a directory pair that was left in an
//     inconsistent state by a crash.
//   - Store, a handler.DataStore wrapper that routes online requests to the
//     old or new directory based on the journal, never exposing a half-written
//     copy and falling back to the old copy on corruption.
//
// During the entire migration the old directory is treated as read-only by
// this package; old copies are removed only through the explicit CleanupOld
// step after every upload has been promoted.
package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
)

// journalDirName is the directory (directly inside the new upload directory)
// that holds all migration bookkeeping. Its name starts with a dot so that
// directory scans for uploads skip it.
const journalDirName = ".tusd-migrate"

// State is the migration state of a single upload.
type State string

const (
	// StateCopying means that a copy to the new directory is in progress (or
	// was interrupted by a crash). Temporary files may exist and the record is
	// a candidate for being retried.
	StateCopying State = "copying"
	// StatePromoted means that a verified copy exists in the new directory and
	// reads are routed to it. The old copy is still kept until CleanupOld.
	StatePromoted State = "promoted"
	// StateFailed means that a verification or copy attempt failed. The old
	// copy remains authoritative and a later migration run retries the upload.
	StateFailed State = "failed"
	// StateNative means that the upload was created directly in the new
	// directory (new uploads during the migration window are never written to
	// the old one).
	StateNative State = "native"
)

// IsTerminal reports whether the state describes an upload that is fully
// served from the new directory.
func (s State) IsTerminal() bool {
	return s == StatePromoted || s == StateNative
}

// Record is the durable migration state of a single upload.
type Record struct {
	// ID is the upload ID as used by tusd (may contain slashes).
	ID string `json:"id"`
	// State is the current migration state.
	State State `json:"state"`
	// Size is the verified size of the new binary copy in bytes.
	Size int64 `json:"size,omitempty"`
	// SHA256 is the hex-encoded SHA-256 digest of the verified new copy. It is
	// empty when hash verification is disabled.
	SHA256 string `json:"sha256,omitempty"`
	// Attempts counts how often a copy was (re)started.
	Attempts int `json:"attempts,omitempty"`
	// LastError describes the most recent failure.
	LastError string `json:"last_error,omitempty"`
	// UpdatedAt is when the record was last written.
	UpdatedAt time.Time `json:"updated_at"`
}

// Journal stores migration records as one JSON file per upload inside the new
// upload directory.
type Journal struct {
	rootDir       string
	recordsDir    string
	tmpDir        string
	quarantineDir string

	// DirModePerm is used when creating directories. If zero,
	// filestore.DefaultDirPerm is used.
	DirModePerm fs.FileMode
	// FileModePerm is used when creating record files. If zero,
	// filestore.DefaultFilePerm is used.
	FileModePerm fs.FileMode
}

// NewJournal opens (and, if necessary, creates) the migration journal inside
// the given new upload directory.
func NewJournal(newDir string) (*Journal, error) {
	dirMode := os.FileMode(filestore.DefaultDirPerm) & os.ModePerm
	fileMode := os.FileMode(filestore.DefaultFilePerm) & os.ModePerm

	j := &Journal{
		rootDir:      filepath.Join(newDir, journalDirName),
		DirModePerm:  dirMode,
		FileModePerm: fileMode,
	}
	j.recordsDir = filepath.Join(j.rootDir, "records")
	j.tmpDir = filepath.Join(j.rootDir, "tmp")
	j.quarantineDir = filepath.Join(j.rootDir, "quarantine")

	for _, dir := range []string{j.rootDir, j.recordsDir, j.tmpDir, j.quarantineDir} {
		if err := os.MkdirAll(dir, dirMode); err != nil {
			return nil, fmt.Errorf("creating migration journal directory %s: %w", dir, err)
		}
	}

	return j, nil
}

// RootDir returns the root directory of the journal.
func (j *Journal) RootDir() string { return j.rootDir }

// RecordsDir returns the directory holding the per-upload state files.
func (j *Journal) RecordsDir() string { return j.recordsDir }

// TmpDir returns the directory for temporary copy files. All files in this
// directory are invisible to the serving path.
func (j *Journal) TmpDir() string { return j.tmpDir }

// QuarantineDir returns the root directory for quarantined half copies.
func (j *Journal) QuarantineDir() string { return j.quarantineDir }

// recordFile returns the path of the state file for an upload ID. The file
// name is the SHA-256 digest of the ID so that IDs containing slashes or
// other special characters map onto a single safe file name.
func (j *Journal) recordFile(id string) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(j.recordsDir, hex.EncodeToString(sum[:])+".json")
}

// Get returns the record for the given upload ID. A missing record is not an
// error: ok is false and err is nil. A corrupted record file is reported as an
// error.
func (j *Journal) Get(id string) (rec Record, ok bool, err error) {
	data, err := os.ReadFile(j.recordFile(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("reading migration record for %s: %w", id, err)
	}

	if err := json.Unmarshal(data, &rec); err != nil {
		return Record{}, false, fmt.Errorf("parsing migration record for %s: %w", id, err)
	}
	return rec, true, nil
}

// Put atomically writes the record (setting UpdatedAt to the current time when
// it is zero).
func (j *Journal) Put(rec Record) error {
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now().UTC()
	}
	data, err := json.MarshalIndent(&rec, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding migration record for %s: %w", rec.ID, err)
	}
	if err := writeFileAtomic(j.recordFile(rec.ID), data, j.DirModePerm, j.FileModePerm); err != nil {
		return fmt.Errorf("writing migration record for %s: %w", rec.ID, err)
	}
	return nil
}

// Delete removes the record for the given upload ID. A missing record is not
// an error.
func (j *Journal) Delete(id string) error {
	if err := os.Remove(j.recordFile(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("deleting migration record for %s: %w", id, err)
	}
	return nil
}

// List returns all records sorted by upload ID. A corrupted record file is
// reported as an error instead of being silently skipped.
func (j *Journal) List() ([]Record, error) {
	entries, err := os.ReadDir(j.recordsDir)
	if err != nil {
		return nil, fmt.Errorf("listing migration records: %w", err)
	}

	records := make([]Record, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(j.recordsDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("reading migration record %s: %w", entry.Name(), err)
		}
		var rec Record
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("parsing migration record %s: %w", entry.Name(), err)
		}
		records = append(records, rec)
	}

	sort.Slice(records, func(i, k int) bool {
		return records[i].ID < records[k].ID
	})
	return records, nil
}

// QuarantineUploadDir returns the directory into which half copies of the
// given upload are moved during reconciliation, creating it on demand.
func (j *Journal) QuarantineUploadDir(id string) (string, error) {
	sum := sha256.Sum256([]byte(id))
	dir := filepath.Join(j.quarantineDir, hex.EncodeToString(sum[:]))
	if err := os.MkdirAll(dir, j.DirModePerm); err != nil {
		return "", fmt.Errorf("creating quarantine directory for %s: %w", id, err)
	}
	return dir, nil
}
