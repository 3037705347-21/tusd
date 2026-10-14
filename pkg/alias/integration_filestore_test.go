package alias_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tus/tusd/v2/pkg/alias"
	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

// startServer wires a real filestore together with a file-backed alias
// manifest and serves it over HTTP.
func startAliasServer(t *testing.T) (*httptest.Server, *alias.FileStore, *filestore.FileStore, string) {
	t.Helper()

	dir := t.TempDir()
	core := filestore.New(dir)

	composer := handler.NewStoreComposer()
	core.UseIn(composer)

	manifestPath := filepath.Join(dir, "aliases.json")
	resolver := alias.NewFileStore(manifestPath, core)

	h, err := handler.NewHandler(handler.Config{
		BasePath:      "/files/",
		StoreComposer: composer,
		AliasResolver: resolver,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	// Mount the handler behind its base path like cmd/tusd does in production.
	server := httptest.NewServer(http.StripPrefix("/files/", h))
	t.Cleanup(server.Close)

	return server, resolver, &core, dir
}

func createCanonicalUpload(t *testing.T, core *filestore.FileStore, id, content string) {
	t.Helper()
	ctx := context.Background()

	upload, err := core.NewUpload(ctx, handler.FileInfo{
		ID:   id,
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("NewUpload(%s): %v", id, err)
	}
	if n, err := upload.WriteChunk(ctx, 0, strings.NewReader(content)); err != nil || n != int64(len(content)) {
		t.Fatalf("WriteChunk(%s): n=%d err=%v", id, n, err)
	}
	if err := upload.FinishUpload(ctx); err != nil {
		t.Fatalf("FinishUpload(%s): %v", id, err)
	}
}

func get(t *testing.T, server *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(server.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestAliasManifestEndToEnd(t *testing.T) {
	server, resolver, core, _ := startAliasServer(t)

	const (
		canonical1 = "canonical-1"
		canonical2 = "canonical-2"
		oldLink    = "2023/old-report"
	)
	createCanonicalUpload(t, core, canonical1, "first file contents")
	createCanonicalUpload(t, core, canonical2, "second file contents")

	// Add: the legacy link reads the canonical file.
	mustApply(t, resolver, []alias.Change{upsert(oldLink, canonical1)})

	if code, body := get(t, server, "/files/"+oldLink); code != http.StatusOK || body != "first file contents" {
		t.Fatalf("GET alias after bind = %d %q", code, body)
	}
	if code, body := get(t, server, "/files/"+canonical1); code != http.StatusOK || body != "first file contents" {
		t.Fatalf("GET canonical = %d %q", code, body)
	}
	resp, err := http.Head(server.URL + "/files/" + oldLink)
	if err != nil {
		t.Fatalf("HEAD alias: %v", err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Upload-Length") != "19" {
		t.Fatalf("HEAD alias = %d, Upload-Length=%q", resp.StatusCode, resp.Header.Get("Upload-Length"))
	}
	resp.Body.Close()

	// Rebind: the same old link now serves the second file.
	mustApply(t, resolver, []alias.Change{upsert(oldLink, canonical2)})
	if code, body := get(t, server, "/files/"+oldLink); code != http.StatusOK || body != "second file contents" {
		t.Fatalf("GET alias after rebind = %d %q", code, body)
	}

	// A batch with a dangling target is rejected entirely and the current
	// binding keeps serving its file.
	if _, err := resolver.Apply(context.Background(), []alias.Change{
		upsert("other-alias", "object-that-does-not-exist"),
	}); err == nil {
		t.Fatal("expected dangling target batch to be rejected")
	}
	if code, body := get(t, server, "/files/"+oldLink); code != http.StatusOK || body != "second file contents" {
		t.Fatalf("GET alias after rejected batch = %d %q", code, body)
	}

	// A batch that conflicts with itself is rejected entirely.
	if _, err := resolver.Apply(context.Background(), []alias.Change{
		upsert("conflicting", canonical1),
		upsert("conflicting", canonical2),
	}); err == nil {
		t.Fatal("expected conflicting batch to be rejected")
	}
	if code, _ := get(t, server, "/files/conflicting"); code != http.StatusNotFound {
		t.Fatalf("conflicting alias must not be bound, got %d", code)
	}

	// Re-running an applied batch is a no-op (convergence).
	batch := []alias.Change{upsert("stable-alias", canonical1)}
	mustApply(t, resolver, batch)
	result := mustApply(t, resolver, batch)
	if !result.Empty() {
		t.Fatalf("re-applying a batch must converge, got %+v", result)
	}

	// Unbind: old links stop resolving.
	mustApply(t, resolver, []alias.Change{unbind(oldLink)})
	if code, _ := get(t, server, "/files/"+oldLink); code != http.StatusNotFound {
		t.Fatalf("unbound alias expected 404, got %d", code)
	}
	// The canonical file itself remains readable.
	if code, body := get(t, server, "/files/"+canonical2); code != http.StatusOK || body != "second file contents" {
		t.Fatalf("GET canonical after unbind = %d %q", code, body)
	}
}

func TestAliasManifestSurvivesRestart(t *testing.T) {
	server, resolver, core, dir := startAliasServer(t)
	createCanonicalUpload(t, core, "canonical-1", "persisted")

	mustApply(t, resolver, []alias.Change{upsert("legacy", "canonical-1")})

	// Simulate a restart: a new store instance reads the same manifest file.
	restartedResolver := alias.NewFileStore(filepath.Join(dir, "aliases.json"), core)
	got, found, err := restartedResolver.ResolveAlias(context.Background(), "legacy")
	if err != nil || !found || got != "canonical-1" {
		t.Fatalf("resolve after restart = %q, %v, %v", got, found, err)
	}

	// The HTTP server is backed by the same file, so a new resolver instance
	// would serve the same links.
	if code, body := get(t, server, "/files/legacy"); code != http.StatusOK || body != "persisted" {
		t.Fatalf("GET legacy = %d %q", code, body)
	}
}
