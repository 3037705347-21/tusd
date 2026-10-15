package migrate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJournalPutGetRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	j, err := NewJournal(tmp)
	require.NoError(t, err)

	rec := Record{
		ID:       "upload-1",
		State:    StatePromoted,
		Size:     1234,
		SHA256:   "deadbeef",
		Attempts: 2,
	}
	require.NoError(t, j.Put(rec))

	// UpdatedAt is populated automatically.
	got, ok, err := j.Get("upload-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "upload-1", got.ID)
	assert.Equal(t, StatePromoted, got.State)
	assert.EqualValues(t, 1234, got.Size)
	assert.Equal(t, "deadbeef", got.SHA256)
	assert.Equal(t, 2, got.Attempts)
	assert.False(t, got.UpdatedAt.IsZero())

	// A freshly opened journal sees the persisted state.
	j2, err := NewJournal(tmp)
	require.NoError(t, err)
	got2, ok, err := j2.Get("upload-1")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, got, got2)

	// No temporary files are left behind in the records directory.
	entries, err := os.ReadDir(j.RecordsDir())
	require.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, strings.Contains(entry.Name(), ".tmp-"), "stale temp file: %s", entry.Name())
	}
}

func TestJournalMissingAndSlashedIDs(t *testing.T) {
	tmp := t.TempDir()

	j, err := NewJournal(tmp)
	require.NoError(t, err)

	// A missing record reports ok=false without error.
	_, ok, err := j.Get("does-not-exist")
	require.NoError(t, err)
	assert.False(t, ok)

	// IDs containing slashes (sub-directories) map to distinct flat files.
	for _, id := range []string{"a/b/c", "a/b/d", "x", "a/b"} {
		require.NoError(t, j.Put(Record{ID: id, State: StateCopying}))
	}

	records, err := j.List()
	require.NoError(t, err)
	require.Len(t, records, 4)
	assert.Equal(t, "a/b", records[0].ID)
	assert.Equal(t, "a/b/c", records[1].ID)
	assert.Equal(t, "a/b/d", records[2].ID)
	assert.Equal(t, "x", records[3].ID)

	got, ok, err := j.Get("a/b/c")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, StateCopying, got.State)

	// Delete removes the record and treats repeated deletion as success.
	require.NoError(t, j.Delete("a/b/c"))
	require.NoError(t, j.Delete("a/b/c"))
	_, ok, err = j.Get("a/b/c")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestJournalCorruptRecord(t *testing.T) {
	tmp := t.TempDir()

	j, err := NewJournal(tmp)
	require.NoError(t, err)
	require.NoError(t, j.Put(Record{ID: "good", State: StateNative}))

	// Fabricate a corrupted record file by writing directly to its path.
	require.NoError(t, os.WriteFile(j.recordFile("corrupt"), []byte("{not json"), 0664))

	_, _, err = j.Get("corrupt")
	assert.Error(t, err)

	_, err = j.List()
	assert.Error(t, err)

	// The intact record can still be read.
	got, ok, err := j.Get("good")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, StateNative, got.State)
}

func TestJournalDirectories(t *testing.T) {
	tmp := t.TempDir()

	j, err := NewJournal(tmp)
	require.NoError(t, err)

	assert.Equal(t, filepath.Join(tmp, journalDirName), j.RootDir())
	for _, dir := range []string{j.RecordsDir(), j.TmpDir(), j.QuarantineDir()} {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	}

	qdir, err := j.QuarantineUploadDir("some/upload")
	require.NoError(t, err)
	info, err := os.Stat(qdir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestStateIsTerminal(t *testing.T) {
	assert.True(t, StatePromoted.IsTerminal())
	assert.True(t, StateNative.IsTerminal())
	assert.False(t, StateCopying.IsTerminal())
	assert.False(t, StateFailed.IsTerminal())
}
