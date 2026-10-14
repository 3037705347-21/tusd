// Package alias provides an alias manifest for uploads that were created under
// different (legacy) naming schemes. Each alias is bound to exactly one
// canonical upload ID, so that old links keep resolving to the same file.
//
// The manifest is changed in batches: a batch may add bindings, rebind an
// existing alias to another canonical upload, or unbind aliases. A batch is
// applied atomically and is idempotent. Before anything is written, the
// resulting manifest is validated as a whole:
//
//   - Every change must be well formed (non-empty alias, known operation,
//     non-empty target for upserts).
//   - A single batch must not contain contradictory changes for the same alias
//     (two different targets, or both an upsert and an unbind).
//   - After the batch is applied, no target may itself be an alias. Aliases
//     therefore always resolve in a single hop to a canonical upload; chains
//     and cycles are impossible.
//   - Every canonical upload referenced by the resulting manifest must exist
//     in the core data store.
//
// If any of these checks fail, or if persisting the new manifest fails, the
// whole batch is rejected and the previously stored manifest is left
// untouched. Re-applying the same batch is a no-op, so repeated runs converge
// to the same state.
package alias

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/tus/tusd/v2/pkg/handler"
)

// Operation is the kind of change applied to a single alias within a batch.
type Operation string

const (
	// OpUpsert binds an alias to a canonical upload. It adds the binding if it
	// does not exist yet or rebinds the alias if it does.
	OpUpsert Operation = "upsert"
	// OpUnbind removes the binding for an alias. Unbinding an alias that is
	// not bound is a no-op and does not cause the batch to fail.
	OpUnbind Operation = "unbind"
)

// Change is a single entry in an alias batch.
type Change struct {
	// Op is the operation to perform (OpUpsert or OpUnbind).
	Op Operation `json:"op"`
	// Alias is the legacy identifier that clients may use in old links.
	Alias string `json:"alias"`
	// CanonicalID is the canonical upload ID that the alias points to. It must
	// be non-empty for OpUpsert and is ignored for OpUnbind.
	CanonicalID string `json:"canonical_id,omitempty"`
}

// Binding is one alias-to-canonical-upload entry of the manifest.
type Binding struct {
	Alias       string `json:"alias"`
	CanonicalID string `json:"canonical_id"`
}

// Result describes the net effect of applying a batch.
type Result struct {
	// Upserted lists the bindings that were added or that were rebound to a
	// different canonical upload.
	Upserted []Binding
	// Unbound lists the aliases whose binding was removed.
	Unbound []string
}

// Empty reports whether the batch left the manifest unchanged.
func (r Result) Empty() bool {
	return len(r.Upserted) == 0 && len(r.Unbound) == 0
}

// ViolationKind categorizes a reason for which a batch was rejected.
type ViolationKind string

const (
	// ViolationInvalidChange is a syntactically invalid change (empty alias,
	// unknown operation, empty target, alias pointing at itself or an alias
	// that is not URL-safe).
	ViolationInvalidChange ViolationKind = "invalid_change"
	// ViolationBatchConflict marks contradictory changes for the same alias
	// inside one batch.
	ViolationBatchConflict ViolationKind = "batch_conflict"
	// ViolationAliasChain marks a target that is itself an alias in the
	// resulting manifest, which would create an alias chain or cycle.
	ViolationAliasChain ViolationKind = "alias_chain"
	// ViolationTargetNotFound marks a canonical upload that does not exist in
	// the core data store.
	ViolationTargetNotFound ViolationKind = "target_not_found"
)

// Sentinel errors that can be matched with errors.Is.
var (
	// ErrInvalidChange is returned for syntactically invalid changes.
	ErrInvalidChange = errors.New("invalid alias change")
	// ErrConflict is returned when a batch contradicts itself or would create
	// an alias chain.
	ErrConflict = errors.New("alias conflict")
	// ErrTargetNotFound is returned when a canonical upload referenced by the
	// manifest does not exist.
	ErrTargetNotFound = errors.New("canonical upload not found")
)

// Violation describes a single reason for rejecting a batch.
type Violation struct {
	// Kind is the category of the violation.
	Kind ViolationKind
	// Alias is the alias involved (may be empty for alias-less errors).
	Alias string
	// Target is the canonical upload ID involved (if any).
	Target string
	// Message is a human readable description.
	Message string
}

func (v Violation) Error() string {
	return v.Message
}

// Err maps the violation kind to the corresponding sentinel error.
func (v Violation) Err() error {
	switch v.Kind {
	case ViolationBatchConflict, ViolationAliasChain:
		return ErrConflict
	case ViolationTargetNotFound:
		return ErrTargetNotFound
	default:
		return ErrInvalidChange
	}
}

// ApplyError aggregates all violations that caused a batch to be rejected so
// that a single run reports every problem at once.
type ApplyError struct {
	Violations []Violation
}

func (e *ApplyError) Error() string {
	if e == nil || len(e.Violations) == 0 {
		return "alias batch rejected"
	}
	msg := "alias batch rejected:"
	for _, v := range e.Violations {
		msg += "\n  - " + v.Message
	}
	return msg
}

// Unwrap allows errors.Is to match the sentinel errors for every contained
// violation.
func (e *ApplyError) Unwrap() []error {
	seen := map[error]struct{}{}
	errs := make([]error, 0, len(e.Violations))
	for _, v := range e.Violations {
		if err := v.Err(); err != nil {
			if _, ok := seen[err]; !ok {
				seen[err] = struct{}{}
				errs = append(errs, err)
			}
		}
	}
	return errs
}

// Store persists an alias manifest and resolves aliases.
type Store interface {
	// ResolveAlias maps an alias to its canonical upload ID. If the alias is
	// not bound, found is false and no error is returned. It satisfies the
	// handler.AliasResolver interface.
	ResolveAlias(ctx context.Context, alias string) (canonicalID string, found bool, err error)
	// Apply validates and atomically applies a batch of changes. Either all
	// changes take effect or none do. Applying an already applied batch again
	// is a no-op.
	Apply(ctx context.Context, changes []Change) (Result, error)
	// List returns all bindings currently stored in the manifest, sorted by
	// alias.
	List(ctx context.Context) ([]Binding, error)
}

// UploadExister can look up uploads by ID. handler.DataStore satisfies this
// interface; it is used to ensure that aliases never point at missing objects.
type UploadExister interface {
	GetUpload(ctx context.Context, id string) (handler.Upload, error)
}

// reURLSafeID mirrors the ID character set accepted by the tus handler so that
// every bound alias is usable as a path segment in old upload links. See
// pkg/handler/unrouted_handler.go (reValidUploadId).
var reURLSafeID = regexp.MustCompile(`^[A-Za-z0-9\-._~%!$'()*+,;=/:@]+$`)

// plan validates the batch against the current manifest and, if valid, returns
// the resulting manifest together with the net effect of the batch. It does
// not perform any I/O beyond checking that referenced canonical uploads exist.
//
// All violations are collected and returned together. A non-violation error
// (e.g. the core data store being unavailable) is returned separately and
// fails the batch without further checks.
func plan(ctx context.Context, core UploadExister, current map[string]string, changes []Change) (next map[string]string, result Result, err error) {
	var violations []Violation
	addViolation := func(kind ViolationKind, alias, target, format string, args ...any) {
		violations = append(violations, Violation{
			Kind:    kind,
			Alias:   alias,
			Target:  target,
			Message: fmt.Sprintf(format, args...),
		})
	}

	// effective holds the last relevant change per alias after checking the
	// batch for internal contradictions.
	type indexedChange struct {
		change Change
		index  int
	}
	effective := map[string]indexedChange{}

	for i, change := range changes {
		if change.Alias == "" {
			addViolation(ViolationInvalidChange, "", change.CanonicalID,
				"change #%d has an empty alias", i+1)
			continue
		}

		switch change.Op {
		case OpUpsert:
			if change.CanonicalID == "" {
				addViolation(ViolationInvalidChange, change.Alias, "",
					"upsert for alias %q is missing canonical_id", change.Alias)
				continue
			}
			if change.CanonicalID == change.Alias {
				addViolation(ViolationInvalidChange, change.Alias, change.CanonicalID,
					"alias %q must not point to itself", change.Alias)
				continue
			}
			if !reURLSafeID.MatchString(change.Alias) {
				addViolation(ViolationInvalidChange, change.Alias, change.CanonicalID,
					"alias %q contains characters that are not safe to use in a URL path", change.Alias)
				continue
			}
		case OpUnbind:
			// Targets are irrelevant for unbind.
		default:
			addViolation(ViolationInvalidChange, change.Alias, change.CanonicalID,
				"unknown operation %q for alias %q (want %q or %q)",
				string(change.Op), change.Alias, OpUpsert, OpUnbind)
			continue
		}

		if prev, ok := effective[change.Alias]; ok {
			switch {
			case prev.change.Op == OpUnbind && change.Op == OpUnbind:
				// Duplicate unbind, keep either.
			case prev.change.Op == OpUpsert && change.Op == OpUpsert && prev.change.CanonicalID == change.CanonicalID:
				// Idempotent duplicate upsert, keep either.
			default:
				addViolation(ViolationBatchConflict, change.Alias, change.CanonicalID,
					"alias %q is changed multiple times in the same batch with conflicting operations or targets",
					change.Alias)
				continue
			}
		}
		effective[change.Alias] = indexedChange{change: change, index: i}
	}

	if len(violations) > 0 {
		return nil, Result{}, &ApplyError{Violations: violations}
	}

	// Build the candidate manifest from the current state. Changes are applied
	// in their original order; at most one effective change per alias remains.
	ordered := make([]indexedChange, 0, len(effective))
	for _, c := range effective {
		ordered = append(ordered, c)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].index < ordered[j].index })

	next = make(map[string]string, len(current))
	for alias, canonicalID := range current {
		next[alias] = canonicalID
	}
	for _, c := range ordered {
		if c.change.Op == OpUnbind {
			delete(next, c.change.Alias)
		} else {
			next[c.change.Alias] = c.change.CanonicalID
		}
	}

	// The resulting manifest must never contain chains: every target has to be
	// a canonical upload, never another alias. This rules out cycles as well.
	aliases := make([]string, 0, len(next))
	for alias := range next {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)

	for _, alias := range aliases {
		target := next[alias]
		if _, targetIsAlias := next[target]; targetIsAlias {
			addViolation(ViolationAliasChain, alias, target,
				"alias %q must point to the canonical upload %q, but %q is itself an alias",
				alias, target, target)
		}
	}
	if len(violations) > 0 {
		return nil, Result{}, &ApplyError{Violations: violations}
	}

	// Every target in the resulting manifest must exist. The check covers the
	// whole manifest (not just the touched entries) so that a batch can never
	// leave dangling aliases behind; batches that unbind or rebind the broken
	// entries first will converge again.
	targets := map[string]struct{}{}
	for _, target := range next {
		targets[target] = struct{}{}
	}
	sortedTargets := make([]string, 0, len(targets))
	for target := range targets {
		sortedTargets = append(sortedTargets, target)
	}
	sort.Strings(sortedTargets)

	missing := map[string][]string{}
	for _, target := range sortedTargets {
		if _, err := core.GetUpload(ctx, target); err != nil {
			if errors.Is(err, handler.ErrNotFound) {
				missing[target] = []string{}
				continue
			}
			// An infrastructure error fails the batch closed; the manifest is
			// left untouched regardless of how far the checks progressed.
			return nil, Result{}, fmt.Errorf("checking existence of canonical upload %q: %w", target, err)
		}
	}
	if len(missing) > 0 {
		for _, alias := range aliases {
			if _, ok := missing[next[alias]]; ok {
				addViolation(ViolationTargetNotFound, alias, next[alias],
					"alias %q points to canonical upload %q, which does not exist",
					alias, next[alias])
			}
		}
		return nil, Result{}, &ApplyError{Violations: violations}
	}

	// Compute the net effect relative to the current manifest.
	for _, alias := range aliases {
		oldTarget, existed := current[alias]
		switch {
		case !existed:
			result.Upserted = append(result.Upserted, Binding{Alias: alias, CanonicalID: next[alias]})
		case oldTarget != next[alias]:
			result.Upserted = append(result.Upserted, Binding{Alias: alias, CanonicalID: next[alias]})
		}
	}
	for alias := range current {
		if _, ok := next[alias]; !ok {
			result.Unbound = append(result.Unbound, alias)
		}
	}
	sort.Slice(result.Upserted, func(i, j int) bool { return result.Upserted[i].Alias < result.Upserted[j].Alias })
	sort.Strings(result.Unbound)

	return next, result, nil
}
