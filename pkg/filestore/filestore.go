// Package filestore provide a storage backend based on the local file system.
//
// FileStore is a storage backend used as a handler.DataStore in handler.NewHandler.
// It stores the uploads in a directory specified in two different files: The
// `[id].info` files are used to store the fileinfo in JSON format. The
// `[id]` files without an extension contain the raw binary data uploaded.
// No cleanup is performed so you may want to run a cronjob to ensure your disk
// is not filled up with old and finished uploads.
//
// Related to the filestore is the package filelocker, which provides a file-based
// locking mechanism. The use of some locking method is recommended and further
// explained in https://tus.github.io/tusd/advanced-topics/locks/.
package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tus/tusd/v2/internal/uid"
	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	// StorageKeyPath is the key of the path of uploaded file in handler.FileInfo.Storage
	StorageKeyPath = "Path"
	// StorageKeyInfoPath is the key of the path of .info file in handler.FileInfo.Storage
	StorageKeyInfoPath = "InfoPath"
)

const DefaultDirPerm = 0775
const DefaultFilePerm = 0664

// See the handler.DataStore interface for documentation about the different
// methods.
type FileStore struct {
	// Path is the relative or absolute path to store files in. FileStore does not
	// check whether the path exists; use os.MkdirAll in this case on your own.
	Path string

	// DirModePerm is the file mode (e.g. 0775) used when creating directories
	// for uploads. Only the permission bits are used. If zero, DefaultDirPerm
	// is used by New.
	DirModePerm fs.FileMode

	// FileModePerm is the file mode (e.g. 0664) used when creating upload files
	// and their .info files. Only the permission bits are used. If zero,
	// DefaultFilePerm is used by New.
	FileModePerm fs.FileMode
}

// New creates a new file based storage backend. The directory specified will
// be used as the only storage entry. This method does not check
// whether the path exists, use os.MkdirAll to ensure.
// The returned store uses DefaultDirPerm and DefaultFilePerm; set DirModePerm
// and FileModePerm on the result to override.
func New(path string) FileStore {
	return FileStore{
		Path:         path,
		DirModePerm:  os.FileMode(DefaultDirPerm) & os.ModePerm,
		FileModePerm: os.FileMode(DefaultFilePerm) & os.ModePerm,
	}
}

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store FileStore) UseIn(composer *handler.StoreComposer) {
	composer.UseCore(store)
	composer.UseTerminater(store)
	composer.UseConcater(store)
	composer.UseLengthDeferrer(store)
	composer.UseContentServer(store)
}

func (store FileStore) NewUpload(ctx context.Context, info handler.FileInfo) (handler.Upload, error) {
	if info.ID == "" {
		info.ID = uid.Uid()
	}

	// The .info file's location can directly be deduced from the upload ID
	infoPath := store.infoPath(info.ID)
	// The binary file's location might be modified by the pre-create hook.
	var binPath string
	if info.Storage != nil && info.Storage[StorageKeyPath] != "" {
		// filepath.Join treats absolute and relative paths the same, so we must
		// handle them on our own. Absolute paths get used as-is, while relative
		// paths are joined to the storage path.
		if filepath.IsAbs(info.Storage[StorageKeyPath]) {
			binPath = info.Storage[StorageKeyPath]
		} else {
			binPath = filepath.Join(store.Path, info.Storage[StorageKeyPath])
		}
	} else {
		binPath = store.defaultBinPath(info.ID)
	}

	info.Storage = map[string]string{
		"Type":             "filestore",
		StorageKeyPath:     binPath,
		StorageKeyInfoPath: infoPath,
	}

	// Create binary file with no content
	if err := createFile(binPath, store.DirModePerm, store.FileModePerm, nil); err != nil {
		return nil, err
	}

	upload := &fileUpload{
		info:         info,
		infoPath:     infoPath,
		binPath:      binPath,
		dirModePerm:  store.DirModePerm,
		fileModePerm: store.FileModePerm,
	}

	// writeInfo creates the file by itself if necessary
	if err := upload.writeInfo(); err != nil {
		return nil, err
	}

	return upload, nil
}

func (store FileStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	info, infoPath, err := store.readInfo(id)
	if err != nil {
		return nil, err
	}

	binPath := store.binPath(id, info)
	stat, err := os.Stat(binPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Interpret os.ErrNotExist as 404 Not Found
			err = handler.ErrNotFound
		}
		return nil, err
	}

	info.Offset = stat.Size()

	return &fileUpload{
		info:         info,
		binPath:      binPath,
		infoPath:     infoPath,
		dirModePerm:  store.DirModePerm,
		fileModePerm: store.FileModePerm,
	}, nil
}

// readInfo loads and parses the .info file for the upload with the given ID.
// A missing info file is reported as handler.ErrNotFound.
func (store FileStore) readInfo(id string) (info handler.FileInfo, infoPath string, err error) {
	infoPath = store.infoPath(id)
	data, err := os.ReadFile(infoPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Interpret os.ErrNotExist as 404 Not Found
			err = handler.ErrNotFound
		}
		return handler.FileInfo{}, "", err
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return handler.FileInfo{}, "", err
	}
	return info, infoPath, nil
}

// binPath resolves the path of the binary data file for the given upload. If
// the info file contains a custom path to the binary file, it is used as-is;
// otherwise the default location based on the upload ID is used.
func (store FileStore) binPath(id string, info handler.FileInfo) string {
	// No filepath.Join here because the joining already happened in NewUpload.
	// Duplicate joining with relative paths leads to incorrect paths.
	if info.Storage != nil && info.Storage[StorageKeyPath] != "" {
		return info.Storage[StorageKeyPath]
	}
	return store.defaultBinPath(id)
}

// QuarantineUpload moves the binary data file of the upload with the given ID
// into quarantineDir, preserving any sub-directory structure implied by the ID
// (e.g. ID "a/b" is moved to quarantineDir/a/b). The .info file remains at
// its original location so that metadata is preserved for forensic analysis.
// After quarantining, GetUpload returns ErrNotFound because the data is no
// longer available at its original location.
//
// If the upload (info file or data file) does not exist, handler.ErrNotFound is
// returned. If a file already exists at the quarantine destination, an error is
// returned and the original data file is left untouched. QuarantineDir should
// reside on the same file system as the upload directory so that the move is
// an atomic rename.
func (store FileStore) QuarantineUpload(ctx context.Context, id, quarantineDir string) error {
	info, _, err := store.readInfo(id)
	if err != nil {
		return err
	}

	binPath := store.binPath(id, info)
	if _, err := os.Stat(binPath); err != nil {
		if os.IsNotExist(err) {
			return handler.ErrNotFound
		}
		return err
	}

	destPath := filepath.Join(quarantineDir, id)

	// Defense in depth: ensure a crafted ID containing ".." segments cannot
	// resolve the quarantine destination outside of the quarantine directory.
	rel, err := filepath.Rel(quarantineDir, destPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("quarantine destination %q escapes quarantine directory %q", destPath, quarantineDir)
	}

	// POSIX rename would silently replace an existing destination. Refuse to
	// overwrite data in the quarantine directory.
	if _, err := os.Stat(destPath); err == nil {
		return fmt.Errorf("quarantine destination already exists: %s", destPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(destPath), store.DirModePerm); err != nil {
		return fmt.Errorf("failed to create quarantine directory for %s: %s", destPath, err)
	}

	if err := os.Rename(binPath, destPath); err != nil {
		return fmt.Errorf("failed to quarantine upload %s: %s", id, err)
	}

	return nil
}

func (store FileStore) AsTerminatableUpload(upload handler.Upload) handler.TerminatableUpload {
	return upload.(*fileUpload)
}

func (store FileStore) AsLengthDeclarableUpload(upload handler.Upload) handler.LengthDeclarableUpload {
	return upload.(*fileUpload)
}

func (store FileStore) AsConcatableUpload(upload handler.Upload) handler.ConcatableUpload {
	return upload.(*fileUpload)
}

func (store FileStore) AsServableUpload(upload handler.Upload) handler.ServableUpload {
	return upload.(*fileUpload)
}

// defaultBinPath returns the path to the file storing the binary data, if it is
// not customized using the pre-create hook.
func (store FileStore) defaultBinPath(id string) string {
	return filepath.Join(store.Path, id)
}

// infoPath returns the path to the .info file storing the file's info.
func (store FileStore) infoPath(id string) string {
	return filepath.Join(store.Path, id+".info")
}

type fileUpload struct {
	// info stores the current information about the upload
	info handler.FileInfo
	// infoPath is the path to the .info file
	infoPath string
	// binPath is the path to the binary file (which has no extension)
	binPath string

	dirModePerm  fs.FileMode
	fileModePerm fs.FileMode
}

func (upload *fileUpload) GetInfo(ctx context.Context) (handler.FileInfo, error) {
	return upload.info, nil
}

func (upload *fileUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	file, err := os.OpenFile(upload.binPath, os.O_WRONLY|os.O_APPEND, upload.fileModePerm)
	if err != nil {
		return 0, err
	}
	// Avoid the use of defer file.Close() here to ensure no errors are lost
	// See https://github.com/tus/tusd/issues/698.

	n, err := io.Copy(file, src)
	upload.info.Offset += n
	if err != nil {
		file.Close()
		return n, err
	}

	return n, file.Close()
}

func (upload *fileUpload) GetReader(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(upload.binPath)
}

func (upload *fileUpload) Terminate(ctx context.Context) error {
	// We ignore errors indicating that the files cannot be found because we want
	// to delete them anyways. The files might be removed by a cron job for cleaning up
	// or some file might have been removed when tusd crashed during the termination.
	err := os.Remove(upload.binPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	err = os.Remove(upload.infoPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func (upload *fileUpload) ConcatUploads(ctx context.Context, uploads []handler.Upload) (err error) {
	file, err := os.OpenFile(upload.binPath, os.O_WRONLY|os.O_APPEND, upload.fileModePerm)
	if err != nil {
		return err
	}
	defer func() {
		// Ensure that close error is propagated, if it occurs.
		// See https://github.com/tus/tusd/issues/698.
		cerr := file.Close()
		if err == nil {
			err = cerr
		}
	}()

	for _, partialUpload := range uploads {
		if err := partialUpload.(*fileUpload).appendTo(file); err != nil {
			return err
		}
	}

	return
}

func (upload *fileUpload) appendTo(file *os.File) error {
	src, err := os.Open(upload.binPath)
	if err != nil {
		return err
	}

	if _, err := io.Copy(file, src); err != nil {
		src.Close()
		return err
	}

	return src.Close()
}

func (upload *fileUpload) DeclareLength(ctx context.Context, length int64) error {
	upload.info.Size = length
	upload.info.SizeIsDeferred = false
	return upload.writeInfo()
}

// writeInfo updates the entire information. Everything will be overwritten.
func (upload *fileUpload) writeInfo() error {
	data, err := json.Marshal(upload.info)
	if err != nil {
		return err
	}
	return createFile(upload.infoPath, upload.dirModePerm, upload.fileModePerm, data)
}

func (upload *fileUpload) FinishUpload(ctx context.Context) error {
	return nil
}

func (upload *fileUpload) ServeContent(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	http.ServeFile(w, r, upload.binPath)

	return nil
}

// createFile creates the file with the content. If the corresponding directory does not exist,
// it is created. If the file already exists, its content is removed.
func createFile(path string, dirPerm fs.FileMode, filePerm fs.FileMode, content []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		if os.IsNotExist(err) {
			// An upload ID containing slashes is mapped onto different directories on disk,
			// for example, `myproject/uploadA` should be put into a folder called `myproject`.
			// If we get an error indicating that a directory is missing, we try to create it.
			if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
				return fmt.Errorf("failed to create directory for %s: %s", path, err)
			}

			// Try creating the file again.
			file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
			if err != nil {
				// If that still doesn't work, error out.
				return err
			}
		} else {
			return err
		}
	}

	if content != nil {
		if _, err := file.Write(content); err != nil {
			return err
		}
	}

	return file.Close()
}
