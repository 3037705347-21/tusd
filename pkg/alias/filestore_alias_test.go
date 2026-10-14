package alias_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tus/tusd/v2/pkg/alias"
	"github.com/tus/tusd/v2/pkg/handler"
)

func newFileStore(t *testing.T, core *fakeCore) (*alias.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "aliases.json")
	return alias.NewFileStore(path, core), path
}

func TestFileStorePersistsAcrossInstances(t *testing.T) {
	core := newFakeCore("c1", "c2")
	store, path := newFileStore(t, core)

	mustApply(t, store, []alias.Change{
		upsert("a", "c1"),
		upsert("b", "c2"),
	})

	// A fresh instance (e.g. a restarted process) observes the same manifest.
	reloaded := alias.NewFileStore(path, core)
	bindings := mustList(t, reloaded)
	if len(bindings) != 2 {
		t.Fatalf("expected 2 persisted bindings, got %+v", bindings)
	}
	if got, found, _ := reloaded.ResolveAlias(context.Background(), "a"); !found || got != "c1" {
		t.Fatalf("reloaded resolve = %q, %v", got, found)
	}
}

func TestFileStoreMissingManifestMeansNoAliases(t *testing.T) {
	store, _ := newFileStore(t, newFakeCore())

	if _, found, err := store.ResolveAlias(context.Background(), "a"); err != nil || found {
		t.Fatalf("missing manifest: got found=%v err=%v", found, err)
	}
	if bindings, err := store.List(context.Background()); err != nil || len(bindings) != 0 {
		t.Fatalf("missing manifest: bindings=%v err=%v", bindings, err)
	}
}

func TestFileStoreReRunDoesNotRewriteManifest(t *testing.T) {
	core := newFakeCore("c1")
	store, path := newFileStore(t, core)
	batch := []alias.Change{upsert("a", "c1")}

	mustApply(t, store, batch)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}

	// Guarantee a measurable difference even on coarse file systems.
	time.Sleep(20 * time.Millisecond)

	result := mustApply(t, store, batch)
	if !result.Empty() {
		t.Fatalf("re-run must be a no-op, got %+v", result)
	}
	infoAfter, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat manifest after re-run: %v", err)
	}
	if !infoAfter.ModTime().Equal(info.ModTime()) {
		t.Fatal("manifest was rewritten although the batch converged")
	}
}

func TestFileStoreRejectedBatchLeavesManifestUntouched(t *testing.T) {
	core := newFakeCore("c1")
	store, path := newFileStore(t, core)
	mustApply(t, store, []alias.Change{upsert("a", "c1")})

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	// Conflicting batch: a missing target and contradictory changes.
	applyErrorFor(t, store, []alias.Change{
		upsert("a", "missing-target"),
		upsert("b", "c1"),
		upsert("b", "missing-target"),
	})

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest after rejected batch: %v", err)
	}
	if string(original) != string(after) {
		t.Fatal("manifest file changed although the batch was rejected")
	}

	// The store still serves the previous, consistent manifest.
	if got, found, _ := store.ResolveAlias(context.Background(), "a"); !found || got != "c1" {
		t.Fatalf("old binding must survive rejected batch, got %q, %v", got, found)
	}
	if _, found, _ := store.ResolveAlias(context.Background(), "b"); found {
		t.Fatal("rejected binding must not be visible")
	}
}

func TestFileStoreWriteFailureLeavesNoManifest(t *testing.T) {
	core := newFakeCore("c1")
	dir := t.TempDir()

	// Point the manifest at a path whose parent is an ordinary file, so
	// creating the directory (and the temp file) fails.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o664); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	store := alias.NewFileStore(filepath.Join(blocker, "aliases.json"), core)

	if _, err := store.Apply(context.Background(), []alias.Change{upsert("a", "c1")}); err == nil {
		t.Fatal("expected write failure, got nil")
	}

	// Nothing was applied and there is no readable manifest at that path.
	if _, err := store.List(context.Background()); err == nil {
		t.Fatal("expected the manifest to be unreadable at the broken path, got no error")
	}

	// A subsequent valid apply to a healthy path succeeds.
	healthy := alias.NewFileStore(filepath.Join(dir, "aliases.json"), core)
	mustApply(t, healthy, []alias.Change{upsert("a", "c1")})
}

func TestFileStoreCorruptManifestFailsClosed(t *testing.T) {
	core := newFakeCore("c1")
	store, path := newFileStore(t, core)

	if err := os.WriteFile(path, []byte("{not json"), 0o664); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}

	if _, _, err := store.ResolveAlias(context.Background(), "a"); err == nil {
		t.Fatal("ResolveAlias must report a corrupt manifest instead of treating it as empty")
	}
	if _, err := store.List(context.Background()); err == nil {
		t.Fatal("List must report a corrupt manifest")
	}
	if _, err := store.Apply(context.Background(), []alias.Change{upsert("a", "c1")}); err == nil {
		t.Fatal("Apply must not overwrite a corrupt manifest")
	}
}

func TestFileStoreConcurrentAppliesConverge(t *testing.T) {
	core := newFakeCore("canonical")
	store, _ := newFileStore(t, core)

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "alias-" + itoa(i)
			_, err := store.Apply(context.Background(), []alias.Change{upsert(name, "canonical")})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply: %v", err)
		}
	}

	bindings := mustList(t, store)
	if len(bindings) != n {
		t.Fatalf("expected %d converged bindings, got %d", n, len(bindings))
	}
	for _, binding := range bindings {
		if binding.CanonicalID != "canonical" {
			t.Fatalf("binding %+v does not point to the canonical upload", binding)
		}
	}

	// Idempotency under concurrency: re-applying everything changes nothing.
	var wg2 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			result, err := store.Apply(context.Background(), []alias.Change{
				upsert("alias-"+itoa(i), "canonical"),
			})
			if err != nil {
				t.Errorf("concurrent re-apply: %v", err)
				return
			}
			if !result.Empty() {
				t.Errorf("re-apply reported changes: %+v", result)
			}
		}(i)
	}
	wg2.Wait()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func TestStoresSatisfyAliasResolver(t *testing.T) {
	var _ handler.AliasResolver = (*alias.FileStore)(nil)
	var _ handler.AliasResolver = alias.NewMemoryStore(newFakeCore())
}
