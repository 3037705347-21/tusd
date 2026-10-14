//go:build windows

package filestore

import "io/fs"

// inodeIdentity uniquely identifies an inode within a filesystem.
type inodeIdentity struct {
	dev uint64
	ino uint64
}

// The standard library does not expose inode numbers or hard-link counts
// through os.FileInfo.Sys on Windows. The reconciler therefore falls back to
// hashing every upload on each run and never garbage-collects blobs based on
// link counts there. The deduplication itself still works (os.Link creates
// NTFS hard links).
func inodeKey(fs.FileInfo) (inodeIdentity, bool) {
	return inodeIdentity{}, false
}

func hardLinkCount(fs.FileInfo) (uint64, bool) {
	return 0, false
}

func linkUnsupported(error) bool {
	return false
}
