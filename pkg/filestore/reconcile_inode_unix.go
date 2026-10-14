//go:build !windows

package filestore

import (
	"errors"
	"io/fs"
	"syscall"
)

// inodeIdentity uniquely identifies an inode within a filesystem.
type inodeIdentity struct {
	dev uint64
	ino uint64
}

// inodeKey returns a stable identity for the file described by info. The
// identity can be used to detect whether two paths point at the same inode
// (i.e. the same physical content) without having to hash the file again.
func inodeKey(info fs.FileInfo) (inodeIdentity, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return inodeIdentity{}, false
	}
	return inodeIdentity{dev: uint64(st.Dev), ino: uint64(st.Ino)}, true
}

// hardLinkCount returns the number of directory entries (hard links) that
// reference the file's inode, as reported by the filesystem.
func hardLinkCount(info fs.FileInfo) (uint64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Nlink), true
}

// linkUnsupported reports whether a link/rename failure indicates that the
// underlying filesystem does not support hard links (for example old NFS
// versions or bind mounts across devices). Such uploads are skipped instead
// of aborting the whole reconciliation.
func linkUnsupported(err error) bool {
	return errors.Is(err, syscall.EXDEV) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.ENOTSUP)
}
