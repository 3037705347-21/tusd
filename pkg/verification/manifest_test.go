package verification

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validDigest = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

func writeManifest(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0664))
	return path
}

// dirSnapshot returns a sorted list of entries in dir, used to assert that
// failed manifest loads have no file system side effects.
func dirSnapshot(t *testing.T, dir string) []string {
	t.Helper()
	var entries []string
	require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		require.NoError(t, err)
		entries = append(entries, rel)
		return nil
	}))
	sort.Strings(entries)
	return entries
}

func TestLoadManifestValid(t *testing.T) {
	a := assert.New(t)

	tmp := t.TempDir()

	// Contains an upper-case digest, an unknown top-level field and an unknown
	// field within an entry; all must be accepted.
	path := writeManifest(t, tmp, "manifest.json", `{
		"version": 1,
		"unknown": "ignored",
		"entries": [
			{"id": "abc", "size": 123, "digest": "AABBCCDDEEFF00112233445566778899AABBCCDDEEFF00112233445566778899", "extra": true},
			{"id": "d/e/f", "size": 0, "digest": "`+validDigest+`"}
		]
	}`)

	manifest, err := LoadManifest(path)
	a.NoError(err)
	require.Len(t, manifest.Entries, 2)

	a.Equal("abc", manifest.Entries[0].ID)
	a.EqualValues(123, manifest.Entries[0].Size)
	a.Equal("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899", manifest.Entries[0].Digest)

	entry, ok := manifest.Get("d/e/f")
	a.True(ok)
	a.EqualValues(0, entry.Size)
	a.Equal(validDigest, entry.Digest)

	_, ok = manifest.Get("missing")
	a.False(ok)
}

func TestLoadManifestEmpty(t *testing.T) {
	a := assert.New(t)

	tmp := t.TempDir()

	// Explicit empty entries array.
	path := writeManifest(t, tmp, "empty.json", `{"entries":[]}`)
	manifest, err := LoadManifest(path)
	a.NoError(err)
	a.Empty(manifest.Entries)

	// Missing entries field is equivalent to an empty manifest.
	path = writeManifest(t, tmp, "empty2.json", `{}`)
	manifest, err = LoadManifest(path)
	a.NoError(err)
	a.Empty(manifest.Entries)
}

func TestLoadManifestInvalid(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"invalid JSON", `{not json`},
		{"empty id", `{"entries":[{"id":"","size":1,"digest":"` + validDigest + `"}]}`},
		{"duplicate id", `{"entries":[{"id":"a","size":1,"digest":"` + validDigest + `"},{"id":"a","size":2,"digest":"` + validDigest + `"}]}`},
		{"negative size", `{"entries":[{"id":"a","size":-1,"digest":"` + validDigest + `"}]}`},
		{"digest too short", `{"entries":[{"id":"a","size":1,"digest":"abc"}]}`},
		{"digest non hex", `{"entries":[{"id":"a","size":1,"digest":"zzbbccddeeff00112233445566778899aabbccddeeff00112233445566778899"}]}`},
		{"digest odd length", `{"entries":[{"id":"a","size":1,"digest":"abb"}]}`},
		{"wrong size type", `{"entries":[{"id":"a","size":"1","digest":"` + validDigest + `"}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			tmp := t.TempDir()
			path := writeManifest(t, tmp, "manifest.json", tc.content)

			before := dirSnapshot(t, tmp)
			_, err := LoadManifest(path)
			a.Error(err)
			after := dirSnapshot(t, tmp)
			a.Equal(before, after, "failed manifest load must not modify the directory")
		})
	}
}

func TestLoadManifestMissingFile(t *testing.T) {
	a := assert.New(t)

	tmp := t.TempDir()
	before := dirSnapshot(t, tmp)

	_, err := LoadManifest(filepath.Join(tmp, "does-not-exist.json"))
	a.Error(err)
	a.True(errors.Is(err, os.ErrNotExist))
	a.Equal(before, dirSnapshot(t, tmp))
}
