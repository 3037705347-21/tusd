// Command tusd-recover rebuilds missing or corrupt upload metadata in a
// filestore directory from an upstream hook receipt ledger, for example after
// an unclean shutdown that left binary upload data on disk while the .info
// metadata was lost or partially written.
//
// The ledger is a JSON Lines file. Each line is either a hook request body as
// tusd sends it to hook endpoints:
//
//	{"Type":"post-create","Event":{"Upload":{"ID":"...","Size":123,...}}}
//
// or a bare upload information object:
//
//	{"ID":"...","Size":123,"MetaData":{"filename":"video.mp4"}}
//
// Only uploads recorded in the ledger are considered. Uploads with valid
// metadata are left untouched; uploads whose metadata is missing or corrupt
// but whose binary data matches the receipt are repaired atomically and
// become resumable again; mismatches are flagged with marker files under the
// state directory and nothing is deleted. The command is safe to run
// repeatedly (e.g. from cron), also while tusd keeps serving the same
// directory:
//
//	tusd-recover -dir ./uploads -ledger ./receipts.jsonl
//
// A dry run reports the planned actions without changing anything:
//
//	tusd-recover -dir ./uploads -ledger ./receipts.jsonl -dry-run -verbose
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tus/tusd/v2/pkg/filelocker"
	"github.com/tus/tusd/v2/pkg/filestore"
)

func main() {
	dir := flag.String("dir", "", "filestore upload directory to recover (required, same as tusd's -upload-dir)")
	ledger := flag.String("ledger", "", "path to the JSON Lines hook receipt ledger (required)")
	stateName := flag.String("state-dir", ".recovery", "name of the state directory created inside -dir (lock, markers, quarantine)")
	lockTimeout := flag.Duration("lock-timeout", 30*time.Second, "how long to wait for an in-use upload lock before skipping it this run")
	dryRun := flag.Bool("dry-run", false, "only report what would be done; do not create, move, rewrite or delete anything")
	verbose := flag.Bool("verbose", false, "log per-upload decisions, quarantines and flagged uploads")
	flag.Parse()

	if *dir == "" || *ledger == "" {
		flag.Usage()
		os.Exit(2)
	}

	store := filestore.New(*dir)
	recovery := filestore.NewRecovery(store, *ledger)
	recovery.StateDirName = *stateName
	recovery.LockTimeout = *lockTimeout
	recovery.DryRun = *dryRun
	// Share tusd's per-upload file locks when it is configured with
	// filelocker (the recommended setup). This excludes concurrent
	// PATCH/DELETE requests while an upload's metadata is rebuilt.
	recovery.Locker = filelocker.New(*dir)
	if *verbose {
		recovery.Logger = log.New(os.Stderr, "tusd-recover: ", log.LstdFlags)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	report, err := recovery.Recover(ctx)
	if err != nil {
		log.Fatalf("tusd-recover: %v", err)
	}

	if *dryRun {
		log.New(os.Stdout, "", 0).Print("Dry run (no changes were made):\n" + report.String())
	} else {
		log.New(os.Stdout, "", 0).Print(report.String())
	}
}
