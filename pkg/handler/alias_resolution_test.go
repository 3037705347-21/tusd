package handler_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/tus/tusd/v2/pkg/handler"
)

// stubAliasResolver is a test double for handler.AliasResolver.
type stubAliasResolver struct {
	mu        sync.Mutex
	canonical string
	found     bool
	err       error
	requested []string
}

func (r *stubAliasResolver) ResolveAlias(_ context.Context, alias string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requested = append(r.requested, alias)
	return r.canonical, r.found, r.err
}

func TestGetResolvesAliasToCanonicalUpload(t *testing.T) {
	SubTest(t, "GET", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		reader := &closingStringReader{Reader: strings.NewReader("hello")}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		upload := NewMockFullUpload(ctrl)
		resolver := &stubAliasResolver{canonical: "canonical-id", found: true}

		gomock.InOrder(
			store.EXPECT().GetUpload(gomock.Any(), "canonical-id").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{Offset: 5, Size: 5}, nil),
			upload.EXPECT().GetReader(gomock.Any()).Return(reader, nil),
		)

		composer := NewStoreComposer()
		composer.UseCore(store)
		handler, _ := NewHandler(Config{
			StoreComposer: composer,
			AliasResolver: resolver,
		})

		(&httpTest{
			Method:    http.MethodGet,
			URL:       "legacy-name",
			Code:      http.StatusOK,
			ResBody:   "hello",
			ResHeader: map[string]string{"Content-Length": "5"},
		}).Run(handler, t)

		if got := resolver.requested; len(got) != 1 || got[0] != "legacy-name" {
			t.Fatalf("resolver queried with %v, want [legacy-name]", got)
		}
	})
}

func TestHeadResolvesAliasToCanonicalUpload(t *testing.T) {
	SubTest(t, "HEAD", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		upload := NewMockFullUpload(ctrl)
		resolver := &stubAliasResolver{canonical: "canonical-id", found: true}

		gomock.InOrder(
			store.EXPECT().GetUpload(gomock.Any(), "canonical-id").Return(upload, nil),
			upload.EXPECT().GetInfo(gomock.Any()).Return(FileInfo{Offset: 7, Size: 7}, nil),
		)

		composer := NewStoreComposer()
		composer.UseCore(store)
		handler, _ := NewHandler(Config{
			StoreComposer: composer,
			AliasResolver: resolver,
		})

		(&httpTest{
			Method:    http.MethodHead,
			URL:       "legacy-name",
			Code:      http.StatusOK,
			ResHeader: map[string]string{"Upload-Length": "7", "Upload-Offset": "7"},
		}).Run(handler, t)
	})
}

func TestReadUsesRequestIDWhenAliasNotBound(t *testing.T) {
	SubTest(t, "NotBound", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		resolver := &stubAliasResolver{found: false}

		store.EXPECT().GetUpload(gomock.Any(), "unknown").Return(nil, ErrNotFound)

		composer := NewStoreComposer()
		composer.UseCore(store)
		handler, _ := NewHandler(Config{
			StoreComposer: composer,
			AliasResolver: resolver,
		})

		(&httpTest{
			Method: http.MethodGet,
			URL:    "unknown",
			Code:   http.StatusNotFound,
		}).Run(handler, t)
	})
}

func TestReadFailsWhenAliasResolutionFails(t *testing.T) {
	SubTest(t, "ResolverError", func(t *testing.T, store *MockFullDataStore, _ *StoreComposer) {
		resolver := &stubAliasResolver{err: errors.New("manifest unavailable")}

		// The data store must never be contacted when resolution fails.
		composer := NewStoreComposer()
		composer.UseCore(store)
		handler, _ := NewHandler(Config{
			StoreComposer: composer,
			AliasResolver: resolver,
		})

		(&httpTest{
			Method: http.MethodGet,
			URL:    "legacy-name",
			Code:   http.StatusInternalServerError,
		}).Run(handler, t)
	})
}
