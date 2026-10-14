package alias

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore keeps the alias manifest in memory. It is safe for concurrent
// use and is mainly intended for tests and embedded deployments that already
// keep their state in memory.
type MemoryStore struct {
	core UploadExister

	mu       sync.RWMutex
	bindings map[string]string
}

// NewMemoryStore creates an in-memory manifest that verifies canonical uploads
// against the given core data store.
func NewMemoryStore(core UploadExister) *MemoryStore {
	return &MemoryStore{
		core:     core,
		bindings: map[string]string{},
	}
}

// ResolveAlias implements Store.
func (s *MemoryStore) ResolveAlias(_ context.Context, alias string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	canonicalID, found := s.bindings[alias]
	return canonicalID, found, nil
}

// Apply implements Store.
func (s *MemoryStore) Apply(ctx context.Context, changes []Change) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := make(map[string]string, len(s.bindings))
	for alias, canonicalID := range s.bindings {
		current[alias] = canonicalID
	}

	next, result, err := plan(ctx, s.core, current, changes)
	if err != nil {
		return Result{}, err
	}

	s.bindings = next
	return result, nil
}

// List implements Store.
func (s *MemoryStore) List(_ context.Context) ([]Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return bindingsToList(s.bindings), nil
}

func bindingsToList(bindings map[string]string) []Binding {
	aliases := make([]string, 0, len(bindings))
	for alias := range bindings {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)

	list := make([]Binding, 0, len(aliases))
	for _, alias := range aliases {
		list = append(list, Binding{Alias: alias, CanonicalID: bindings[alias]})
	}
	return list
}
