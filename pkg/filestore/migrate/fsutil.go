package migrate

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// writeFileAtomic writes data to path using a freshly created temporary file
// in the same directory: the file is fsynced, then atomically renamed into
// place, and finally the containing directory is fsynced so that the rename is
// persisted. A crash at any point leaves either the previous complete file or
// the new complete file behind, never a truncated one.
func writeFileAtomic(path string, data []byte, dirMode, fileMode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return err
	}

	tmp, err := createTempFile(filepath.Dir(path), filepath.Base(path), fileMode)
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	return syncDir(filepath.Dir(path))
}

// createTempFile creates a new temporary file with a unique name in dir. The
// name starts with prefix so that callers can recognize their own files.
func createTempFile(dir string, prefix string, fileMode fs.FileMode) (*os.File, error) {
	var random [8]byte
	for i := 0; i < 100; i++ {
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		name := filepath.Join(dir, fmt.Sprintf(".%s.tmp-%s", prefix, hex.EncodeToString(random[:])))
		// O_EXCL guards against the astronomically unlikely name collision.
		file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_EXCL, fileMode)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not allocate a unique temporary file name in %s", dir)
}

// syncDir flushes the directory metadata of dir to disk. On platforms where
// syncing a directory is not permitted the error is ignored.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		// The directory is guaranteed to be usable (we just wrote into it); a
		// failure to open it for syncing must not fail the whole operation on
		// platforms without directory fsync support.
		return nil //nolint:nilerr
	}
	defer d.Close()

	if err := d.Sync(); err != nil && errors.Is(err, fs.ErrInvalid) {
		// Some platforms (e.g. Windows) do not support syncing directories.
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

// removeIgnoreMissing removes path, treating a missing file as success.
// Directories are only removed when empty.
func removeIgnoreMissing(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
