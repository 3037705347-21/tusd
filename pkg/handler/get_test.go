package handler_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/tus/tusd/v2/pkg/handler"
)

// stubDownloadBlocker is a test double for the DownloadBlocker interface.
type stubDownloadBlocker struct {
	blocked  bool
	err      error
	calls    int
	calledID string
}

func (b *stubDownloadBlocker) DownloadBlocked(_ context.Context, id string) (bool, error) {
	b.calls++
	b.calledID = id
	return b.blocked, b.err
}

type closingStringReader struct {
	*strings.Reader
	closed bool
}

func (reader *closingStringReader) Close() error {
	reader.closed = true
	return nil
}

func TestGet(t *testing.T) {
	SubTest(t, "Download", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		reader := &closingStringReader{
			Reader: strings.NewReader("hello"),
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		locker := NewMockFullLocker(ctrl)
		lock := NewMockFullLock(ctrl)
		upload := NewMockFullUpload(ctrl)

		gomock.InOrder(
			locker.EXPECT().NewLock("yes").Return(lock, nil),
			lock.EXPECT().Lock(gomock.Any(), gomock.Any()).Return(nil),
			store.EXPECT().GetUpload(gomock.Any(), "yes").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{
				Offset: 5,
				Size:   20,
				MetaData: map[string]string{
					"filename": "file.jpg\"evil",
					"filetype": "image/jpeg",
				},
			}, nil),
			upload.EXPECT().GetReader(gomock.Any()).Return(reader, nil),
			lock.EXPECT().Unlock().Return(nil),
		)

		composer := NewStoreComposer()
		composer.UseCore(store)
		composer.UseLocker(locker)

		handler, _ := NewHandler(Config{
			StoreComposer: composer,
		})

		(&httpTest{
			Method: "GET",
			URL:    "yes",
			ResHeader: map[string]string{
				"Content-Length":      "5",
				"Content-Type":        "image/jpeg",
				"Content-Disposition": `inline;filename="file.jpg\"evil"`,
			},
			Code:    http.StatusOK,
			ResBody: "hello",
		}).Run(handler, t)

		if !reader.closed {
			t.Error("expected reader to be closed")
		}
	})

	SubTest(t, "EmptyDownload", func(t *testing.T, store *MockFullDataStore, composer *StoreComposer) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		upload := NewMockFullUpload(ctrl)

		gomock.InOrder(
			store.EXPECT().GetUpload(gomock.Any(), "yes").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{
				Offset: 0,
			}, nil),
		)

		handler, _ := NewHandler(Config{
			StoreComposer: composer,
		})

		(&httpTest{
			Method: "GET",
			URL:    "yes",
			ResHeader: map[string]string{
				"Content-Length":      "0",
				"Content-Disposition": `attachment`,
			},
			Code:    http.StatusNoContent,
			ResBody: "",
		}).Run(handler, t)
	})

	SubTest(t, "InvalidFileType", func(t *testing.T, store *MockFullDataStore, composer *StoreComposer) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		upload := NewMockFullUpload(ctrl)

		gomock.InOrder(
			store.EXPECT().GetUpload(gomock.Any(), "yes").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{
				Offset: 0,
				MetaData: map[string]string{
					"filetype": "non a valid mime type",
				},
			}, nil),
		)

		handler, _ := NewHandler(Config{
			StoreComposer: composer,
		})

		(&httpTest{
			Method: "GET",
			URL:    "yes",
			ResHeader: map[string]string{
				"Content-Length":      "0",
				"Content-Type":        "application/octet-stream",
				"Content-Disposition": `attachment`,
			},
			Code:    http.StatusNoContent,
			ResBody: "",
		}).Run(handler, t)
	})

	SubTest(t, "BlockedDownload", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		// The blocker reports the upload as quarantined. No expectations for
		// GetUpload or locks are set, so any data store access fails the
		// test via gomock.
		blocker := &stubDownloadBlocker{blocked: true}

		composer := NewStoreComposer()
		composer.UseCore(store)

		handler, _ := NewHandler(Config{
			StoreComposer:   composer,
			DownloadBlocker: blocker,
		})

		(&httpTest{
			Method:  "GET",
			URL:     "yes",
			Code:    http.StatusForbidden,
			ResBody: "ERR_UPLOAD_QUARANTINED: upload is quarantined and cannot be downloaded\n",
		}).Run(handler, t)

		if blocker.calls != 1 {
			t.Errorf("expected blocker to be called once, got %d", blocker.calls)
		}
		if blocker.calledID != "yes" {
			t.Errorf("expected blocker to receive id 'yes', got %q", blocker.calledID)
		}
	})

	SubTest(t, "BlockerAllowsDownload", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		reader := &closingStringReader{
			Reader: strings.NewReader("hello"),
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		locker := NewMockFullLocker(ctrl)
		lock := NewMockFullLock(ctrl)
		upload := NewMockFullUpload(ctrl)

		gomock.InOrder(
			locker.EXPECT().NewLock("yes").Return(lock, nil),
			lock.EXPECT().Lock(gomock.Any(), gomock.Any()).Return(nil),
			store.EXPECT().GetUpload(gomock.Any(), "yes").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{
				Offset: 5,
				Size:   20,
			}, nil),
			upload.EXPECT().GetReader(gomock.Any()).Return(reader, nil),
			lock.EXPECT().Unlock().Return(nil),
		)

		blocker := &stubDownloadBlocker{blocked: false}

		composer := NewStoreComposer()
		composer.UseCore(store)
		composer.UseLocker(locker)

		handler, _ := NewHandler(Config{
			StoreComposer:   composer,
			DownloadBlocker: blocker,
		})

		(&httpTest{
			Method:    "GET",
			URL:       "yes",
			Code:      http.StatusOK,
			ResBody:   "hello",
			ResHeader: map[string]string{"Content-Length": "5"},
		}).Run(handler, t)

		if blocker.calls != 1 {
			t.Errorf("expected blocker to be called once, got %d", blocker.calls)
		}
	})

	SubTest(t, "DownloadBlockerError", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		// If the blocker itself fails, the download must fail with 500 and
		// the data store must never be touched.
		blocker := &stubDownloadBlocker{err: errors.New("blocker unavailable")}

		composer := NewStoreComposer()
		composer.UseCore(store)

		handler, _ := NewHandler(Config{
			StoreComposer:   composer,
			DownloadBlocker: blocker,
		})

		(&httpTest{
			Method:  "GET",
			URL:     "yes",
			Code:    http.StatusInternalServerError,
			ResBody: "ERR_INTERNAL_SERVER_ERROR: blocker unavailable\n",
		}).Run(handler, t)
	})

	SubTest(t, "NotWhitelistedFileType", func(t *testing.T, store *MockFullDataStore, composer *StoreComposer) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		upload := NewMockFullUpload(ctrl)

		gomock.InOrder(
			store.EXPECT().GetUpload(gomock.Any(), "yes").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{
				Offset: 0,
				MetaData: map[string]string{
					"filetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document.v1",
					"filename": "invoice.docx",
				},
			}, nil),
		)

		handler, _ := NewHandler(Config{
			StoreComposer: composer,
		})

		(&httpTest{
			Method: "GET",
			URL:    "yes",
			ResHeader: map[string]string{
				"Content-Length":      "0",
				"Content-Type":        "application/vnd.openxmlformats-officedocument.wordprocessingml.document.v1",
				"Content-Disposition": `attachment;filename="invoice.docx"`,
			},
			Code:    http.StatusNoContent,
			ResBody: "",
		}).Run(handler, t)
	})
}
