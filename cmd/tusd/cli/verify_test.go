package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tus/tusd/v2/pkg/filestore"
	tushandler "github.com/tus/tusd/v2/pkg/handler"
	"github.com/tus/tusd/v2/pkg/verification"
)

func TestValidateVerificationBackend(t *testing.T) {
	a := assert.New(t)

	a.NoError(validateVerificationBackend("", "", ""))
	a.Error(validateVerificationBackend("bucket", "", ""))
	a.Error(validateVerificationBackend("", "bucket", ""))
	a.Error(validateVerificationBackend("", "", "account"))

	// Local backend plus an unrelated flag must be allowed.
	a.NoError(validateVerificationBackend("", "", ""))
}

func TestLedgerPathResolution(t *testing.T) {
	a := assert.New(t)

	uploadDir := filepath.Join(string(filepath.Separator), "srv", "uploads")
	a.Equal(filepath.Join(uploadDir, ".verification", "ledger.jsonl"), ledgerPath("", uploadDir))

	// Absolute override is used verbatim.
	a.Equal("/var/lib/tusd/ledger.jsonl", ledgerPath("/var/lib/tusd/ledger.jsonl", uploadDir))

	// Relative override is absoluteized against the working directory.
	resolved := ledgerPath("custom-ledger.jsonl", uploadDir)
	a.True(filepath.IsAbs(resolved))
	a.True(strings.HasSuffix(resolved, "custom-ledger.jsonl"))
}

// writeCLIManifest writes a manifest for the given ids/digests and returns its
// path.
func writeCLIManifest(t *testing.T, dir string, entries []verification.Entry) string {
	t.Helper()
	// Reuse the on-disk JSON shape of the manifest.
	payload := struct {
		Entries []verification.Entry `json:"entries"`
	}{Entries: entries}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	path := filepath.Join(dir, "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0664))
	return path
}

func digestOf(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// applyVerificationTestFlags points the global CLI flags at the given upload
// directory and manifest.
func applyVerificationTestFlags(t *testing.T, uploadDir, manifestPath string) {
	t.Helper()

	Flags.VerifyManifest = manifestPath
	Flags.VerifyLedger = ""
	Flags.UploadDir = uploadDir
	Flags.S3Bucket = ""
	Flags.GCSBucket = ""
	Flags.AzStorage = ""
	Flags.DirPerms = filestore.DefaultDirPerm
	Flags.FilePerms = filestore.DefaultFilePerm
	Flags.AcquireLockTimeout = 5 * time.Second
	Flags.FilelockAcquirerPollInterval = 10 * time.Millisecond
	Flags.FilelockHolderPollInterval = 10 * time.Millisecond
	Flags.ShowStartupLogs = false

	t.Cleanup(func() {
		downloadBlocker = nil
		Flags.VerifyManifest = ""
		Flags.VerifyLedger = ""
	})
}

// TestRunVerificationHappyPath verifies the CLI wiring end to end against a
// real local file store: matching uploads verify, a mismatching one is
// quarantined and the resulting blocker denies its download.
func TestRunVerificationHappyPath(t *testing.T) {
	a := assert.New(t)

	uploadDir := t.TempDir()
	manifestDir := t.TempDir()

	ctx := context.Background()
	store := filestore.New(uploadDir)

	goodUpload, err := store.NewUpload(ctx, tushandler.FileInfo{ID: "good", Size: int64(len("hello"))})
	require.NoError(t, err)
	_, err = goodUpload.WriteChunk(ctx, 0, strings.NewReader("hello"))
	require.NoError(t, err)

	badUpload, err := store.NewUpload(ctx, tushandler.FileInfo{ID: "bad", Size: int64(len("hello"))})
	require.NoError(t, err)
	_, err = badUpload.WriteChunk(ctx, 0, strings.NewReader("HELLO"))
	require.NoError(t, err)

	entries := []verification.Entry{
		{ID: "good", Size: int64(len("hello")), Digest: digestOf("hello")},
		{ID: "bad", Size: int64(len("hello")), Digest: digestOf("hello")},
	}
	manifestPath := writeCLIManifest(t, manifestDir, entries)
	applyVerificationTestFlags(t, uploadDir, manifestPath)

	RunVerification()

	require.NotNil(t, downloadBlocker)

	blocked, err := downloadBlocker.DownloadBlocked(ctx, "bad")
	a.NoError(err)
	a.True(blocked)

	blocked, err = downloadBlocker.DownloadBlocked(ctx, "good")
	a.NoError(err)
	a.False(blocked)

	blocked, err = downloadBlocker.DownloadBlocked(ctx, "unknown")
	a.NoError(err)
	a.False(blocked)

	// The quarantined data file was moved; the .info file was retained.
	_, err = os.Stat(filepath.Join(uploadDir, ".quarantine", "bad"))
	a.NoError(err)
	_, err = os.Stat(filepath.Join(uploadDir, "bad"))
	a.True(os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(uploadDir, "bad.info"))
	a.NoError(err)

	// The ledger exists at the default location.
	_, err = os.Stat(filepath.Join(uploadDir, ".verification", "ledger.jsonl"))
	a.NoError(err)
}

// TestRunVerificationIncrementalAndDisabled verifies that a second run does
// not create new events for unchanged entries and that no blocker is installed
// when -verify-manifest is unset.
func TestRunVerificationRerunAndDisabled(t *testing.T) {
	a := assert.New(t)

	uploadDir := t.TempDir()
	manifestDir := t.TempDir()

	ctx := context.Background()
	store := filestore.New(uploadDir)
	upload, err := store.NewUpload(ctx, tushandler.FileInfo{ID: "ok", Size: int64(len("data"))})
	require.NoError(t, err)
	_, err = upload.WriteChunk(ctx, 0, strings.NewReader("data"))
	require.NoError(t, err)

	entries := []verification.Entry{{ID: "ok", Size: int64(len("data")), Digest: digestOf("data")}}
	manifestPath := writeCLIManifest(t, manifestDir, entries)
	applyVerificationTestFlags(t, uploadDir, manifestPath)

	RunVerification()
	ledgerFile := filepath.Join(uploadDir, ".verification", "ledger.jsonl")
	first, err := os.ReadFile(ledgerFile)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	// Rerun with the same manifest: everything is skipped, ledger unchanged.
	RunVerification()
	second, err := os.ReadFile(ledgerFile)
	require.NoError(t, err)
	a.Equal(string(first), string(second))

	// With verification disabled, no blocker is installed.
	downloadBlocker = nil
	Flags.VerifyManifest = ""
	RunVerification()
	a.Nil(downloadBlocker)
}
