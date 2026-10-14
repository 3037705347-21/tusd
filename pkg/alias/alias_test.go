package alias_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tus/tusd/v2/pkg/alias"
	"github.com/tus/tusd/v2/pkg/handler"
)

// fakeCore is an UploadExister backed by an in-memory set of upload IDs.
type fakeCore struct {
	exists   map[string]bool
	failWith error
}

func newFakeCore(ids ...string) *fakeCore {
	core := &fakeCore{exists: map[string]bool{}}
	for _, id := range ids {
		core.exists[id] = true
	}
	return core
}

func (c *fakeCore) GetUpload(_ context.Context, id string) (handler.Upload, error) {
	if c.failWith != nil {
		return nil, c.failWith
	}
	if c.exists[id] {
		// plan only checks for existence, so no concrete upload is needed.
		return nil, nil
	}
	return nil, handler.ErrNotFound
}

func upsert(a, c string) alias.Change {
	return alias.Change{Op: alias.OpUpsert, Alias: a, CanonicalID: c}
}

func unbind(a string) alias.Change {
	return alias.Change{Op: alias.OpUnbind, Alias: a}
}

func mustApply(t *testing.T, store alias.Store, changes []alias.Change) alias.Result {
	t.Helper()
	result, err := store.Apply(context.Background(), changes)
	if err != nil {
		t.Fatalf("Apply(%v) returned unexpected error: %v", changes, err)
	}
	return result
}

func applyErrorFor(t *testing.T, store alias.Store, changes []alias.Change) *alias.ApplyError {
	t.Helper()
	_, err := store.Apply(context.Background(), changes)
	if err == nil {
		t.Fatalf("Apply(%v) expected error, got nil", changes)
	}
	var applyErr *alias.ApplyError
	if !errors.As(err, &applyErr) {
		t.Fatalf("Apply(%v) expected *alias.ApplyError, got %T: %v", changes, err, err)
	}
	return applyErr
}

func TestApplyAddsAndResolves(t *testing.T) {
	store := alias.NewMemoryStore(newFakeCore("canonical-1"))

	result := mustApply(t, store, []alias.Change{upsert("legacy-a", "canonical-1")})
	if len(result.Upserted) != 1 || result.Upserted[0].Alias != "legacy-a" {
		t.Fatalf("unexpected result: %+v", result)
	}

	got, found, err := store.ResolveAlias(context.Background(), "legacy-a")
	if err != nil || !found || got != "canonical-1" {
		t.Fatalf("ResolveAlias = %q, %v, %v; want canonical-1, true, nil", got, found, err)
	}

	if _, found, _ := store.ResolveAlias(context.Background(), "missing"); found {
		t.Fatal("ResolveAlias unexpectedly found unbound alias")
	}
}

func TestApplyRejectsDanglingTarget(t *testing.T) {
	core := newFakeCore("canonical-1")
	store := alias.NewMemoryStore(core)

	_, err := store.Apply(context.Background(), []alias.Change{upsert("legacy-a", "missing-object")})
	if !errors.Is(err, alias.ErrTargetNotFound) {
		t.Fatalf("expected ErrTargetNotFound, got %v", err)
	}

	if _, found, _ := store.ResolveAlias(context.Background(), "legacy-a"); found {
		t.Fatal("alias must not be bound when the batch is rejected")
	}
}

func TestApplyRejectsBatchConflicts(t *testing.T) {
	core := newFakeCore("c1", "c2")

	for _, changes := range [][]alias.Change{
		{upsert("a", "c1"), upsert("a", "c2")},
		{upsert("a", "c1"), unbind("a")},
		{unbind("a"), upsert("a", "c1")},
	} {
		store := alias.NewMemoryStore(core)
		err := applyErrorFor(t, store, changes)
		if !errors.Is(err, alias.ErrConflict) {
			t.Fatalf("changes %v: expected ErrConflict, got %v", changes, err)
		}
		if _, found, _ := store.ResolveAlias(context.Background(), "a"); found {
			t.Fatalf("changes %v: conflicting batch must not be applied", changes)
		}
	}
}

func TestApplyAllowsIdempotentDuplicates(t *testing.T) {
	store := alias.NewMemoryStore(newFakeCore("c1"))

	result := mustApply(t, store, []alias.Change{
		upsert("a", "c1"),
		upsert("a", "c1"),
		unbind("b"),
		unbind("b"),
	})
	if len(result.Upserted) != 1 {
		t.Fatalf("expected exactly one upsert, got %+v", result)
	}
}

func TestApplyRebindsAndUnbinds(t *testing.T) {
	core := newFakeCore("c1", "c2")
	store := alias.NewMemoryStore(core)

	mustApply(t, store, []alias.Change{upsert("a", "c1")})

	// Rebind to another canonical upload.
	result := mustApply(t, store, []alias.Change{upsert("a", "c2")})
	if got, _, _ := store.ResolveAlias(context.Background(), "a"); got != "c2" {
		t.Fatalf("expected rebind to c2, got %q", got)
	}
	if len(result.Upserted) != 1 || result.Upserted[0].CanonicalID != "c2" {
		t.Fatalf("unexpected rebind result: %+v", result)
	}

	// Unbind removes the binding; unbinding a missing alias is a no-op.
	result = mustApply(t, store, []alias.Change{unbind("a"), unbind("never-existed")})
	if len(result.Unbound) != 1 || result.Unbound[0] != "a" {
		t.Fatalf("unexpected unbind result: %+v", result)
	}
	if _, found, _ := store.ResolveAlias(context.Background(), "a"); found {
		t.Fatal("alias still resolved after unbind")
	}
}

func TestApplyRerunConverges(t *testing.T) {
	core := newFakeCore("c1", "c2")
	store := alias.NewMemoryStore(core)

	batch := []alias.Change{
		upsert("a", "c1"),
		upsert("b", "c2"),
		unbind("c"),
	}

	first := mustApply(t, store, batch)
	if first.Empty() {
		t.Fatal("first application must change the manifest")
	}

	second := mustApply(t, store, batch)
	if !second.Empty() {
		t.Fatalf("re-running the same batch must be a no-op, got %+v", second)
	}

	third := mustApply(t, store, batch)
	if !third.Empty() {
		t.Fatalf("third run must still converge, got %+v", third)
	}

	bindings := mustList(t, store)
	if len(bindings) != 2 || bindings[0].Alias != "a" || bindings[1].Alias != "b" {
		t.Fatalf("unexpected converged manifest: %+v", bindings)
	}
}

func TestApplyRejectsAliasChainsAndCycles(t *testing.T) {
	// "a" exists both as a legacy alias name and (in other deployments) as a
	// real upload ID; here we only seed the canonical object it points to.
	core := newFakeCore("c1")
	store := alias.NewMemoryStore(core)
	mustApply(t, store, []alias.Change{upsert("a", "c1")})

	// Binding b to "a" would create b -> a -> c1: targets must be canonical.
	err := applyErrorFor(t, store, []alias.Change{upsert("b", "a")})
	if !errors.Is(err, alias.ErrConflict) {
		t.Fatalf("expected ErrConflict for alias chain, got %v", err)
	}
	if _, found, _ := store.ResolveAlias(context.Background(), "b"); found {
		t.Fatal("chain-forming binding must not be applied")
	}

	// Self binding is invalid as well.
	err = applyErrorFor(t, store, []alias.Change{upsert("c1", "c1")})
	if !errors.Is(err, alias.ErrInvalidChange) {
		t.Fatalf("expected ErrInvalidChange for self binding, got %v", err)
	}

	// If the same batch unbinds the alias "a" and "a" is a real canonical
	// upload, binding b to it is valid in the resulting manifest.
	core.exists["a"] = true
	mustApply(t, store, []alias.Change{
		unbind("a"),
		upsert("b", "a"),
	})
	got, found, _ := store.ResolveAlias(context.Background(), "b")
	if !found || got != "a" {
		t.Fatalf("expected b to resolve to canonical upload a, got %q, %v", got, found)
	}
}

func TestApplyValidatesEntireResultingManifest(t *testing.T) {
	core := newFakeCore("c1", "c2")
	store := alias.NewMemoryStore(core)
	mustApply(t, store, []alias.Change{upsert("a", "c1"), upsert("b", "c1")})

	// c1 disappears; an unrelated batch must not be applied while the
	// resulting manifest would still contain dangling aliases.
	core.exists["c1"] = false
	err := applyErrorFor(t, store, []alias.Change{upsert("d", "c2")})
	if !errors.Is(err, alias.ErrTargetNotFound) {
		t.Fatalf("expected ErrTargetNotFound, got %v", err)
	}

	messages := err.Error()
	if !strings.Contains(messages, `"a"`) || !strings.Contains(messages, `"b"`) {
		t.Fatalf("expected every dangling alias to be reported, got: %s", messages)
	}
	if _, found, _ := store.ResolveAlias(context.Background(), "d"); found {
		t.Fatal("batch must not be applied while dangling entries remain")
	}

	// Repairing the manifest (unbind the dangling entries) makes it converge.
	core.exists["c1"] = true
	mustApply(t, store, []alias.Change{unbind("a"), unbind("b"), upsert("d", "c2")})
}

func TestApplyInfrastructureErrorFailsClosed(t *testing.T) {
	core := newFakeCore("c1")
	inventoryErr := errors.New("inventory unavailable")
	core.failWith = inventoryErr
	store := alias.NewMemoryStore(core)

	_, err := store.Apply(context.Background(), []alias.Change{upsert("a", "c1")})
	if !errors.Is(err, inventoryErr) {
		t.Fatalf("expected infrastructure error to be surfaced, got %v", err)
	}
	if _, found, _ := store.ResolveAlias(context.Background(), "a"); found {
		t.Fatal("manifest must not change when existence checks fail")
	}
}

func TestApplyRejectsInvalidChanges(t *testing.T) {
	core := newFakeCore("c1")
	store := alias.NewMemoryStore(core)

	invalid := []alias.Change{
		{Op: alias.OpUpsert, Alias: "", CanonicalID: "c1"},
		{Op: alias.OpUpsert, Alias: "a", CanonicalID: ""},
		{Op: alias.Operation("delete"), Alias: "a", CanonicalID: "c1"},
		{Op: alias.OpUpsert, Alias: "not url safe", CanonicalID: "c1"},
	}
	for _, change := range invalid {
		err := applyErrorFor(t, store, []alias.Change{change})
		if !errors.Is(err, alias.ErrInvalidChange) {
			t.Errorf("change %+v: expected ErrInvalidChange, got %v", change, err)
		}
	}
}

func TestApplyAggregatesViolations(t *testing.T) {
	store := alias.NewMemoryStore(newFakeCore())

	err := applyErrorFor(t, store, []alias.Change{
		upsert("a", "missing-1"),
		upsert("b", "missing-2"),
	})
	if len(err.Violations) < 2 {
		t.Fatalf("expected all dangling aliases to be reported, got %d: %v", len(err.Violations), err)
	}
}

func TestParseChanges(t *testing.T) {
	changes, err := alias.ParseChanges(strings.NewReader(`{
		"changes": [
			{"op": "upsert", "alias": "a", "canonical_id": "c1"},
			{"op": "unbind", "alias": "b"}
		]
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 2 || changes[0].Op != alias.OpUpsert || changes[1].Alias != "b" {
		t.Fatalf("unexpected changes: %+v", changes)
	}

	for _, input := range []string{
		``,
		`{"changes": []} garbage`,
		`{"changes": [{"op": "upsert", "alias": "a", "canonical": "c1"}]}`,
	} {
		if _, err := alias.ParseChanges(strings.NewReader(input)); err == nil {
			t.Errorf("expected error parsing %q", input)
		}
	}
}

func mustList(t *testing.T, store alias.Store) []alias.Binding {
	t.Helper()
	bindings, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return bindings
}
