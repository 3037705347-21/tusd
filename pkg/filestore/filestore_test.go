package filestore

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tus/tusd/v2/pkg/handler"
)

// Test interface implementation of Filestore
var _ handler.DataStore = FileStore{}
var _ handler.TerminaterDataStore = FileStore{}
var _ handler.ConcaterDataStore = FileStore{}
var _ handler.LengthDeferrerDataStore = FileStore{}

func TestFilestore(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	// Create new upload
	upload, err := store.NewUpload(ctx, handler.FileInfo{
		Size: 42,
		MetaData: map[string]string{
			"hello": "world",
		},
	})
	a.NoError(err)
	a.NotEqual(nil, upload)

	// Check info without writing
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(0, info.Offset)
	a.Equal(handler.MetaData{"hello": "world"}, info.MetaData)
	a.Equal(3, len(info.Storage))
	a.Equal("filestore", info.Storage["Type"])
	a.Equal(filepath.Join(tmp, info.ID), info.Storage[StorageKeyPath])
	a.Equal(filepath.Join(tmp, info.ID+".info"), info.Storage[StorageKeyInfoPath])

	// Write data to upload
	bytesWritten, err := upload.WriteChunk(ctx, 0, strings.NewReader("hello world"))
	a.NoError(err)
	a.EqualValues(len("hello world"), bytesWritten)

	// Check new offset
	info, err = upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(11, info.Offset)

	// Read content
	reader, err := upload.GetReader(ctx)
	a.NoError(err)

	content, err := io.ReadAll(reader)
	a.NoError(err)
	a.Equal("hello world", string(content))
	reader.(io.Closer).Close()

	// Serve content
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Range", "bytes=0-4")

	err = store.AsServableUpload(upload).ServeContent(context.Background(), w, r)
	a.Nil(err)

	a.Equal(http.StatusPartialContent, w.Code)
	a.Equal("5", w.Header().Get("Content-Length"))
	a.Equal("text/plain; charset=utf-8", w.Header().Get("Content-Type"))
	a.Equal("bytes 0-4/11", w.Header().Get("Content-Range"))
	a.NotEqual("", w.Header().Get("Last-Modified"))
	a.Equal("hello", w.Body.String())

	// Terminate upload
	a.NoError(store.AsTerminatableUpload(upload).Terminate(ctx))

	// Test if upload is deleted
	upload, err = store.GetUpload(ctx, info.ID)
	a.Equal(nil, upload)
	a.Equal(handler.ErrNotFound, err)
}

// TestCreateDirectories tests whether an upload with a slash in its ID causes
// the correct directories to be created.
func TestCreateDirectories(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	// Create new upload
	upload, err := store.NewUpload(ctx, handler.FileInfo{
		ID:   "hello/world/123",
		Size: 42,
		MetaData: map[string]string{
			"hello": "world",
		},
	})
	a.NoError(err)
	a.NotEqual(nil, upload)

	// Check info without writing
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(0, info.Offset)
	a.Equal(handler.MetaData{"hello": "world"}, info.MetaData)
	a.Equal(3, len(info.Storage))
	a.Equal("filestore", info.Storage["Type"])
	a.Equal(filepath.Join(tmp, info.ID), info.Storage[StorageKeyPath])
	a.Equal(filepath.Join(tmp, info.ID+".info"), info.Storage[StorageKeyInfoPath])

	// Write data to upload
	bytesWritten, err := upload.WriteChunk(ctx, 0, strings.NewReader("hello world"))
	a.NoError(err)
	a.EqualValues(len("hello world"), bytesWritten)

	// Check new offset
	info, err = upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(11, info.Offset)

	// Read content
	reader, err := upload.GetReader(ctx)
	a.NoError(err)

	content, err := io.ReadAll(reader)
	a.NoError(err)
	a.Equal("hello world", string(content))
	reader.(io.Closer).Close()

	// Check that the file and directory exists on disk
	statInfo, err := os.Stat(filepath.Join(tmp, "hello/world/123"))
	a.NoError(err)
	a.True(statInfo.Mode().IsRegular())
	a.EqualValues(11, statInfo.Size())
	statInfo, err = os.Stat(filepath.Join(tmp, "hello/world/"))
	a.NoError(err)
	a.True(statInfo.Mode().IsDir())

	// Terminate upload
	a.NoError(store.AsTerminatableUpload(upload).Terminate(ctx))

	// Test if upload is deleted
	upload, err = store.GetUpload(ctx, info.ID)
	a.Equal(nil, upload)
	a.Equal(handler.ErrNotFound, err)
}

func TestNotFound(t *testing.T) {
	a := assert.New(t)

	store := New("./path")
	ctx := context.Background()

	upload, err := store.GetUpload(ctx, "upload-that-does-not-exist")
	a.Error(err)
	a.Equal(handler.ErrNotFound, err)
	a.Equal(nil, upload)
}

func TestConcatUploads(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-concat-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	// Create new upload to hold concatenated upload
	finUpload, err := store.NewUpload(ctx, handler.FileInfo{Size: 9})
	a.NoError(err)
	a.NotEqual(nil, finUpload)

	finInfo, err := finUpload.GetInfo(ctx)
	a.NoError(err)
	finId := finInfo.ID

	// Create three uploads for concatenating
	partialUploads := make([]handler.Upload, 3)
	contents := []string{
		"abc",
		"def",
		"ghi",
	}
	for i := range 3 {
		upload, err := store.NewUpload(ctx, handler.FileInfo{Size: 3})
		a.NoError(err)

		n, err := upload.WriteChunk(ctx, 0, strings.NewReader(contents[i]))
		a.NoError(err)
		a.EqualValues(3, n)

		partialUploads[i] = upload
	}

	err = store.AsConcatableUpload(finUpload).ConcatUploads(ctx, partialUploads)
	a.NoError(err)

	// Check offset
	finUpload, err = store.GetUpload(ctx, finId)
	a.NoError(err)

	info, err := finUpload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(9, info.Size)
	a.EqualValues(9, info.Offset)

	// Read content
	reader, err := finUpload.GetReader(ctx)
	a.NoError(err)

	content, err := io.ReadAll(reader)
	a.NoError(err)
	a.Equal("abcdefghi", string(content))
	reader.(io.Closer).Close()
}

func TestDeclareLength(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-declare-length-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	upload, err := store.NewUpload(ctx, handler.FileInfo{
		Size:           0,
		SizeIsDeferred: true,
	})
	a.NoError(err)
	a.NotEqual(nil, upload)

	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(0, info.Size)
	a.Equal(true, info.SizeIsDeferred)

	err = store.AsLengthDeclarableUpload(upload).DeclareLength(ctx, 100)
	a.NoError(err)

	updatedInfo, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(100, updatedInfo.Size)
	a.Equal(false, updatedInfo.SizeIsDeferred)
}

// TestCustomRelativePath tests whether the upload's destination can be customized
// relative to the storage directory.
func TestCustomRelativePath(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	// Create new upload
	upload, err := store.NewUpload(ctx, handler.FileInfo{
		ID:   "folder1/info",
		Size: 42,
		Storage: map[string]string{
			"Path": "./folder2/bin",
		},
	})
	a.NoError(err)
	a.NotEqual(nil, upload)

	// Check info without writing
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(0, info.Offset)
	a.Equal(3, len(info.Storage))
	a.Equal("filestore", info.Storage["Type"])
	a.Equal(filepath.Join(tmp, "./folder2/bin"), info.Storage[StorageKeyPath])
	a.Equal(filepath.Join(tmp, "./folder1/info.info"), info.Storage[StorageKeyInfoPath])

	// Write data to upload
	bytesWritten, err := upload.WriteChunk(ctx, 0, strings.NewReader("hello world"))
	a.NoError(err)
	a.EqualValues(len("hello world"), bytesWritten)

	// Check new offset
	info, err = upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(11, info.Offset)

	// Read content
	reader, err := upload.GetReader(ctx)
	a.NoError(err)

	content, err := io.ReadAll(reader)
	a.NoError(err)
	a.Equal("hello world", string(content))
	reader.(io.Closer).Close()

	// Check that the output file and info file exist on disk
	statInfo, err := os.Stat(filepath.Join(tmp, "folder2/bin"))
	a.NoError(err)
	a.True(statInfo.Mode().IsRegular())
	a.EqualValues(11, statInfo.Size())
	statInfo, err = os.Stat(filepath.Join(tmp, "folder1/info.info"))
	a.NoError(err)
	a.True(statInfo.Mode().IsRegular())

	// Terminate upload
	a.NoError(store.AsTerminatableUpload(upload).Terminate(ctx))

	// Test if upload is deleted
	upload, err = store.GetUpload(ctx, info.ID)
	a.Equal(nil, upload)
	a.Equal(handler.ErrNotFound, err)
}

// TestCustomAbsolutePath tests whether the upload's destination can be customized
// using an absolute path to the storage directory.
func TestCustomAbsolutePath(t *testing.T) {
	a := assert.New(t)

	tmp1, err := os.MkdirTemp("", "tusd-filestore-")
	a.NoError(err)

	tmp2, err := os.MkdirTemp("", "tusd-filestore-")
	a.NoError(err)

	store := New(tmp1)
	ctx := context.Background()

	// Create new upload, but the Path property points to a directory
	// outside of the directory given to FileStore
	binPath := filepath.Join(tmp2, "dir/my-upload.bin")
	upload, err := store.NewUpload(ctx, handler.FileInfo{
		ID:   "my-upload",
		Size: 42,
		Storage: map[string]string{
			"Path": binPath,
		},
	})
	a.NoError(err)
	a.NotEqual(nil, upload)

	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.EqualValues(42, info.Size)
	a.EqualValues(0, info.Offset)
	a.Equal(3, len(info.Storage))
	a.Equal("filestore", info.Storage["Type"])
	a.Equal(binPath, info.Storage[StorageKeyPath])
	a.Equal(filepath.Join(tmp1, "my-upload.info"), info.Storage[StorageKeyInfoPath])

	statInfo, err := os.Stat(binPath)
	a.NoError(err)
	a.True(statInfo.Mode().IsRegular())
}

// createFinishedUpload is a test helper that creates an upload with the given
// ID and content in the store.
func createFinishedUpload(a *assert.Assertions, store FileStore, ctx context.Context, id, content string) handler.Upload {
	upload, err := store.NewUpload(ctx, handler.FileInfo{
		ID:   id,
		Size: int64(len(content)),
	})
	a.NoError(err)

	if content != "" {
		_, err = upload.WriteChunk(ctx, 0, strings.NewReader(content))
		a.NoError(err)
	}

	return upload
}

// TestQuarantineUpload verifies that quarantining moves the data file into the
// quarantine directory while the .info file stays in place.
func TestQuarantineUpload(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-quarantine-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()
	quarantineDir := filepath.Join(tmp, ".quarantine")

	content := "hello quarantine"
	upload := createFinishedUpload(a, store, ctx, "quarantine-me", content)
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	binPath := info.Storage[StorageKeyPath]
	infoPath := info.Storage[StorageKeyInfoPath]

	// Original data and info files exist
	_, err = os.ReadFile(binPath)
	a.NoError(err)
	_, err = os.ReadFile(infoPath)
	a.NoError(err)

	a.NoError(store.QuarantineUpload(ctx, info.ID, quarantineDir))

	// Data file is gone from its original location
	_, err = os.Stat(binPath)
	a.True(os.IsNotExist(err))

	// .info file remains in place
	_, err = os.ReadFile(infoPath)
	a.NoError(err)

	// Quarantined file exists with identical content
	quarantined, err := os.ReadFile(filepath.Join(quarantineDir, info.ID))
	a.NoError(err)
	a.Equal(content, string(quarantined))

	// GetUpload now reports the upload as not found
	gotUpload, err := store.GetUpload(ctx, info.ID)
	a.Equal(handler.ErrNotFound, err)
	a.Nil(gotUpload)
}

// TestQuarantineUploadWithSubDirectory verifies that sub-directory structure
// implied by the upload ID is preserved inside the quarantine directory.
func TestQuarantineUploadWithSubDirectory(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-quarantine-nested-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()
	quarantineDir := filepath.Join(tmp, ".quarantine")

	content := "nested content"
	upload := createFinishedUpload(a, store, ctx, "project-a/upload-b", content)
	info, err := upload.GetInfo(ctx)
	a.NoError(err)

	a.NoError(store.QuarantineUpload(ctx, info.ID, quarantineDir))

	quarantined, err := os.ReadFile(filepath.Join(quarantineDir, "project-a", "upload-b"))
	a.NoError(err)
	a.Equal(content, string(quarantined))

	// Original path and info file
	_, err = os.Stat(filepath.Join(tmp, "project-a", "upload-b"))
	a.True(os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tmp, "project-a", "upload-b.info"))
	a.NoError(err)
}

// TestQuarantineUploadPathEscape verifies that an ID containing ".." segments
// cannot make the quarantine destination escape the quarantine directory.
func TestQuarantineUploadPathEscape(t *testing.T) {
	a := assert.New(t)

	base, err := os.MkdirTemp("", "tusd-filestore-quarantine-escape-")
	a.NoError(err)
	uploadDir := filepath.Join(base, "uploads")
	store := New(uploadDir)
	ctx := context.Background()

	// Simulate an upload created with the crafted ID "../escape": its info
	// and data files resolve into base/ instead of the upload directory.
	binPath := filepath.Join(base, "escape")
	infoPath := filepath.Join(base, "escape.info")
	info := handler.FileInfo{
		ID:   "../escape",
		Size: 1,
		Storage: map[string]string{
			"Type":             "filestore",
			StorageKeyPath:     binPath,
			StorageKeyInfoPath: infoPath,
		},
	}
	infoData, err := json.Marshal(info)
	a.NoError(err)
	a.NoError(os.WriteFile(infoPath, infoData, os.FileMode(DefaultFilePerm)))
	a.NoError(os.WriteFile(binPath, []byte("x"), os.FileMode(DefaultFilePerm)))

	quarantineDir := filepath.Join(uploadDir, ".quarantine")
	err = store.QuarantineUpload(ctx, "../escape", quarantineDir)
	a.Error(err)

	// The data file must remain untouched outside the quarantine directory.
	data, err := os.ReadFile(binPath)
	a.NoError(err)
	a.Equal("x", string(data))
}

// TestQuarantineUploadMissing verifies that quarantining missing uploads
// returns ErrNotFound.
func TestQuarantineUploadMissing(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-quarantine-missing-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()

	// Neither info nor data file exists
	a.Equal(handler.ErrNotFound, store.QuarantineUpload(ctx, "does-not-exist", filepath.Join(tmp, ".quarantine")))

	// Info file exists but data file is gone (already quarantined/removed).
	upload, err := store.NewUpload(ctx, handler.FileInfo{ID: "ghost", Size: 3})
	a.NoError(err)
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	a.NoError(os.Remove(info.Storage[StorageKeyPath]))

	err = store.QuarantineUpload(ctx, "ghost", filepath.Join(tmp, ".quarantine"))
	a.Equal(handler.ErrNotFound, err)
}

// TestQuarantineUploadDestinationExists verifies that an existing quarantine
// destination causes an error and leaves the original file untouched.
func TestQuarantineUploadDestinationExists(t *testing.T) {
	a := assert.New(t)

	tmp, err := os.MkdirTemp("", "tusd-filestore-quarantine-exists-")
	a.NoError(err)

	store := New(tmp)
	ctx := context.Background()
	quarantineDir := filepath.Join(tmp, ".quarantine")

	content := "do not overwrite me"
	upload := createFinishedUpload(a, store, ctx, "dup-id", content)
	info, err := upload.GetInfo(ctx)
	a.NoError(err)
	binPath := info.Storage[StorageKeyPath]

	// Pre-create the destination file.
	destPath := filepath.Join(quarantineDir, "dup-id")
	a.NoError(os.MkdirAll(quarantineDir, os.FileMode(DefaultDirPerm)))
	a.NoError(os.WriteFile(destPath, []byte("EXISTING"), os.FileMode(DefaultFilePerm)))

	err = store.QuarantineUpload(ctx, "dup-id", quarantineDir)
	a.Error(err)

	// Original data file is untouched and keeps its content.
	original, err := os.ReadFile(binPath)
	a.NoError(err)
	a.Equal(content, string(original))

	// Destination is untouched as well.
	existing, err := os.ReadFile(destPath)
	a.NoError(err)
	a.Equal("EXISTING", string(existing))
}
