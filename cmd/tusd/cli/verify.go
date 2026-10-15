package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/exp/slog"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
	tushandler "github.com/tus/tusd/v2/pkg/handler"
	"github.com/tus/tusd/v2/pkg/verification"
)

// Verification ledger and quarantine locations relative to the upload
// directory. They are hidden entries and are never enumerated by tusd itself.
const (
	verificationLedgerDir  = ".verification"
	verificationLedgerFile = "ledger.jsonl"
	quarantineDirName      = ".quarantine"
)

// downloadBlocker is installed on the handler config when a verification run
// marked uploads as quarantined. It stays nil when verification is disabled.
var downloadBlocker tushandler.DownloadBlocker

// validateVerificationBackend ensures that manifest verification is only
// combined with the local file storage backend, since the verifier needs
// direct filesystem access and in-place quarantine moves.
func validateVerificationBackend(s3Bucket, gcsBucket, azStorage string) error {
	switch {
	case s3Bucket != "":
		return errors.New("content verification (-verify-manifest) is only supported with the local file storage backend, but S3 is configured (-s3-bucket)")
	case gcsBucket != "":
		return errors.New("content verification (-verify-manifest) is only supported with the local file storage backend, but Google Cloud Storage is configured (-gcs-bucket)")
	case azStorage != "":
		return errors.New("content verification (-verify-manifest) is only supported with the local file storage backend, but Azure Blob Storage is configured (-azure-storage)")
	}
	return nil
}

// ledgerPath resolves the ledger path: the -verify-ledger value if set
// (absoluteized), otherwise <upload-dir>/.verification/ledger.jsonl.
func ledgerPath(flagValue, uploadDir string) string {
	if flagValue == "" {
		return filepath.Join(uploadDir, verificationLedgerDir, verificationLedgerFile)
	}
	if filepath.IsAbs(flagValue) {
		return flagValue
	}
	abs, err := filepath.Abs(flagValue)
	if err != nil {
		return flagValue
	}
	return abs
}

// RunVerification executes the startup content verification pass when
// -verify-manifest is provided. It runs after CreateComposer and before Serve:
// uploads that do not match the manifest are recorded in the ledger and moved
// into quarantine, and the resulting status store is used to block their
// downloads. Manifest, ledger or quarantine failures terminate the process
// before the HTTP server starts. With -verify-manifest unset this function
// does nothing and leaves all existing behavior unchanged.
func RunVerification() {
	if Flags.VerifyManifest == "" {
		return
	}

	if err := validateVerificationBackend(Flags.S3Bucket, Flags.GCSBucket, Flags.AzStorage); err != nil {
		stderr.Fatalf("Unable to start content verification: %s", err)
	}

	uploadDir, err := filepath.Abs(Flags.UploadDir)
	if err != nil {
		stderr.Fatalf("Unable to resolve upload directory for content verification: %s", err)
	}

	manifestPath, err := filepath.Abs(Flags.VerifyManifest)
	if err != nil {
		stderr.Fatalf("Unable to resolve manifest path: %s", err)
	}

	ledgerFilePath := ledgerPath(Flags.VerifyLedger, uploadDir)
	quarantineDir := filepath.Join(uploadDir, quarantineDirName)

	store := filestore.New(uploadDir)
	store.DirModePerm = os.FileMode(Flags.DirPerms) & os.ModePerm
	store.FileModePerm = os.FileMode(Flags.FilePerms) & os.ModePerm

	locker := filelocker.New(uploadDir)
	locker.AcquirerPollInterval = Flags.FilelockAcquirerPollInterval
	locker.HolderPollInterval = Flags.FilelockHolderPollInterval

	verifier, err := verification.New(verification.Config{
		ManifestPath:  manifestPath,
		LedgerPath:    ledgerFilePath,
		QuarantineDir: quarantineDir,
		Store:         store,
		Locker:        locker,
		LockTimeout:   Flags.AcquireLockTimeout,
		Logger:        slog.Default(),
	})
	if err != nil {
		stderr.Fatalf("Unable to initialize content verification: %s", err)
	}

	printStartupLog("Running content verification with manifest %s (ledger: %s)\n", manifestPath, ledgerFilePath)

	report, status, err := verifier.Run(context.Background())
	if err != nil {
		stderr.Fatalf("Content verification failed: %s", err)
	}

	printStartupLog(
		"Content verification run %s complete: total=%d verified=%d quarantined=%d missing=%d unfinished=%d errors=%d skipped=%d\n",
		report.RunID, report.Total, report.Verified, report.Quarantined, report.Missing, report.Unfinished, report.Errors, report.Skipped,
	)

	downloadBlocker = status
}
