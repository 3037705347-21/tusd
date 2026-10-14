package handler

import "context"

// AliasResolver is implemented by stores that keep a manifest of aliases.
// An alias is a legacy upload identifier that is bound to exactly one
// canonical upload.
//
// The handler consults the resolver for read requests (HEAD and GET): if the
// identifier in the request URL is an alias, the request is served from the
// canonical upload it is bound to, so old links keep reading the same file.
// If the identifier is not an alias (found is false), it is used as-is.
// Mutating requests (PATCH and DELETE) are not redirected and always use the
// canonical upload identifier.
type AliasResolver interface {
	// ResolveAlias maps an alias to its canonical upload identifier. If the
	// alias is not bound, found must be false without returning an error.
	ResolveAlias(ctx context.Context, alias string) (canonicalID string, found bool, err error)
}
