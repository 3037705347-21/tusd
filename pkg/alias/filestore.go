package alias

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/tus/lockfile"

	"github.com/tus/tusd/v2/internal/uid"
	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	// manifestVersion is the current on-disk format version of the manifest.
	manifestVersion = 1

	// lockTimeout bounds how long Apply waits for another process that is
	// applying a batch at the same time.
	lockTimeout = 10 * time.Second
	// lockPollInterval is the interval between lock acquisition attempts.
	lockPollInterval = 50 * time.Millisecond

	// DefaultDirPerm is the directory permission used when creating the
	// manifest directory.
	DefaultDirPerm fs.FileMode = 0o775
	// DefaultFilePerm is the file permission used for the manifest and its
	// lock file.
	DefaultFilePerm fs.FileMode = 0o664
)

// FileStore persists the alias manifest as a single JSON file on the local
// file system. It is independent of the core upload data store and can be used
// alongside any handler.DataStore (including S3, GCS and Azure), as long as
// all tusd instances share the manifest location.
//
// Writes are atomic (temporary file, fsync, rename) and guarded by both an
// in-process mutex and an inter-process lock file, so a failed or interrupted
// batch can never leave a half-written manifest behind. Reads rely on the
// atomic rename and therefore do not need to take the lock.
type FileStore struct {
	// Path is the location of the manifest file, e.g. "/srv/tusd/aliases.json".
	// The parent directory is created on demand when applying batches.
	Path string
	// Core is used to verify that every referenced canonical upload exists.
	Core UploadExister

	// DirModePerm is the file mode used when creating the manifest directory.
	// If zero, DefaultDirPerm is used.
	DirModePerm fs.FileMode
	// FileModePerm is the file mode used for the manifest file and its lock
	// file. If zero, DefaultFilePerm is used.
	FileModePerm fs.FileMode

	mu sync.Mutex
}

// NewFileStore creates a file-backed alias manifest at the given path.
func NewFileStore(path string, core UploadExister) *FileStore {
	return &FileStore{
		Path:         path,
		Core:         core,
		DirModePerm:  DefaultDirPerm,
		FileModePerm: DefaultFilePerm,
	}
}

// manifest is the on-disk representation.
type manifest struct {
	// Version identifies the on-disk format.
	Version int `json:"version"`
	// Bindings maps alias to canonical upload ID.
	Bindings map[string]string `json:"bindings"`
}

// ResolveAlias implements Store. A missing manifest file means that no aliases
// are bound yet; a corrupt manifest file is reported as an error instead of
// being silently treated as empty.
func (s *FileStore) ResolveAlias(_ context.Context, alias string) (string, bool, error) {
	bindings, err := s.load()
	if err != nil {
		return "", false, err
	}
	canonicalID, found := bindings[alias]
	return canonicalID, found, nil
}

// Apply implements Store. The whole batch is validated first; only then is the
// manifest replaced atomically. Any error leaves the previous file in place.
func (s *FileStore) Apply(ctx context.Context, changes []Change) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirMode := s.DirModePerm
	if dirMode == 0 {
		dirMode = DefaultDirPerm
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), dirMode); err != nil {
		return Result{}, err
	}

	unlock, err := s.acquireLock(ctx)
	if err != nil {
		return Result{}, err
	}
	defer unlock()

	current, err := s.load()
	if err != nil {
		return Result{}, err
	}

	next, result, err := plan(ctx, s.Core, current, changes)
	if err != nil {
		return Result{}, err
	}

	// The batch converged to the current state: do not rewrite the file so
	// repeated runs are stable and do not churn the manifest.
	if result.Empty() {
		return result, nil
	}

	if err := s.write(next); err != nil {
		return Result{}, err
	}

	return result, nil
}

// List implements Store.
func (s *FileStore) List(_ context.Context) ([]Binding, error) {
	bindings, err := s.load()
	if err != nil {
		return nil, err
	}
	return bindingsToList(bindings), nil
}

// load reads and decodes the manifest. A missing file yields an empty manifest.
func (s *FileStore) load() (map[string]string, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, err
	}

	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, errors.New("alias manifest at " + s.Path + " is corrupt: " + err.Error())
	}
	if m.Version != manifestVersion {
		return nil, errors.New("alias manifest at " + s.Path + " has unsupported version")
	}
	if m.Bindings == nil {
		m.Bindings = map[string]string{}
	}
	return m.Bindings, nil
}

// write replaces the manifest atomically: it writes to a temporary file in the
// same directory, flushes it to disk and renames it over the old manifest.
// Readers therefore only ever observe either the old or the complete new file.
func (s *FileStore) write(bindings map[string]string) (err error) {
	dir := filepath.Dir(s.Path)
	fileMode := s.FileModePerm
	if fileMode == 0 {
		fileMode = DefaultFilePerm
	}

	data, err := json.MarshalIndent(manifest{
		Version:  manifestVersion,
		Bindings: bindings,
	}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmpPath := filepath.Join(dir, "."+filepath.Base(s.Path)+".tmp-"+uid.Uid())
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// Best effort cleanup; never mask the original error.
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}

	if err = os.Rename(tmpPath, s.Path); err != nil {
		return err
	}

	// Sync the directory so the rename itself is durable. A failure here does
	// not invalidate the manifest contents, hence it is ignored.
	if dirFile, dirErr := os.Open(dir); dirErr == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	return nil
}

// acquireLock takes the inter-process lock for the manifest. The returned
// function releases it.
func (s *FileStore) acquireLock(ctx context.Context) (func() error, error) {
	lock := lockfile.Lockfile(s.Path + ".lock")

	deadline := time.Now().Add(lockTimeout)
	for {
		err := lock.TryLock()
		if err == nil {
			return lock.Unlock, nil
		}

		// ErrBusy means another process is applying a batch. ErrNotExist can
		// be observed briefly while another process is creating the lock file.
		// Both are retried until the timeout.
		if err != lockfile.ErrBusy && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockPollInterval):
		}

		if time.Now().After(deadline) {
			return nil, errors.New("timed out waiting for the alias manifest lock: " + handler.ErrLockTimeout.Error())
		}
	}
}
