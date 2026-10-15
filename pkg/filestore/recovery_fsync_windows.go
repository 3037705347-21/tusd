//go:build windows

package filestore

// syncDir is a no-op on Windows, where directory handles cannot be used to
// fsync directory entry changes. Metadata writes remain atomic because they
// are installed with MoveFile (rename), so readers never observe partial
// metadata.
func syncDir(dir string) {}
