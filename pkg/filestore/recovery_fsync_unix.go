//go:build !windows

package filestore

import "os"

// syncDir flushes directory entry changes (renames, creates, unlinks) to
// stable storage. It is best effort: on platforms or filesystems where the
// fsync fails, the atomic rename already guarantees that readers never see
// torn metadata.
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}
