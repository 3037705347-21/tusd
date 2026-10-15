// Package verification implements manifest-driven content verification for
// uploads stored by the file data store.
//
// A verification run compares the uploads referenced by a manifest (upload ID
// -> expected size and SHA-256 digest) against the data already stored on disk.
// Uploads whose size or digest does not match are quarantined: the decision is
// first persisted in an append-only ledger and the data file is then moved out
// of its download location. The ledger can be replayed to reconstruct the
// current status (e.g. to keep blocking downloads after a restart) and allows
// subsequent runs to skip entries that have not changed since the last
// successful verification.
package verification

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// digestBytes is the required length of a SHA-256 digest in bytes (32).
const digestBytes = 32

// validateSHA256Digest reports whether digest is a valid hex-encoded SHA-256
// digest (64 hex characters).
func validateSHA256Digest(digest string) error {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != digestBytes {
		return fmt.Errorf("invalid SHA-256 digest: %q", digest)
	}
	return nil
}

// Entry is a single manifest record mapping an upload ID to the expected size
// and SHA-256 digest of its content.
type Entry struct {
	// ID is the upload ID as used by the data store.
	ID string `json:"id"`
	// Size is the expected content length in bytes. Must be non-negative.
	Size int64 `json:"size"`
	// Digest is the expected hex-encoded SHA-256 digest. Both upper- and
	// lower-case hex are accepted; the value is normalized to lower-case.
	Digest string `json:"digest"`
}

// Manifest is the fully loaded and validated verification manifest.
type Manifest struct {
	// Entries contains the manifest records in their original order.
	Entries []Entry
	// byID indexes entries by ID for incremental lookups.
	byID map[string]Entry
}

// Get returns the manifest entry for the given ID and whether it exists.
func (m *Manifest) Get(id string) (Entry, bool) {
	entry, ok := m.byID[id]
	return entry, ok
}

// jsonManifest is the on-disk JSON representation of the manifest. Unknown
// top-level fields are ignored by encoding/json.
type jsonManifest struct {
	Version int     `json:"version,omitempty"`
	Entries []Entry `json:"entries"`
}

// LoadManifest reads, parses and validates the manifest at the given path. It
// performs no file system writes: the manifest is fully loaded before any
// ledger or quarantine operation takes place, so a failure here leaves the
// existing storage state untouched.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("verification: failed to read manifest %q: %w", path, err)
	}

	var raw jsonManifest
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("verification: failed to parse manifest %q: %w", path, err)
	}

	return NewManifest(raw.Entries)
}

// NewManifest validates a set of entries and constructs a Manifest. Entries
// must have a non-empty ID, a non-negative size and a 64 character hex-encoded
// SHA-256 digest; duplicate IDs are rejected. Digests are normalized to
// lower-case.
func NewManifest(entries []Entry) (*Manifest, error) {
	manifest := &Manifest{
		Entries: make([]Entry, 0, len(entries)),
		byID:    make(map[string]Entry, len(entries)),
	}

	for i, entry := range entries {
		if entry.ID == "" {
			return nil, fmt.Errorf("verification: manifest entry %d has an empty id", i)
		}
		if entry.Size < 0 {
			return nil, fmt.Errorf("verification: manifest entry %q has a negative size", entry.ID)
		}

		digest := strings.ToLower(entry.Digest)
		if err := validateSHA256Digest(digest); err != nil {
			return nil, fmt.Errorf("verification: manifest entry %q has an invalid SHA-256 digest: %q", entry.ID, entry.Digest)
		}
		entry.Digest = digest

		if _, exists := manifest.byID[entry.ID]; exists {
			return nil, fmt.Errorf("verification: manifest contains duplicate entry for id %q", entry.ID)
		}

		manifest.byID[entry.ID] = entry
		manifest.Entries = append(manifest.Entries, entry)
	}

	return manifest, nil
}
