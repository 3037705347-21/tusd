// Command tusd-dedupe merges identical content of completed uploads in a
// filestore directory into a single physical copy using hard links.
//
// It is a thin wrapper around filestore.Reconciler and is safe to run
// repeatedly (e.g. from cron) while tusd keeps serving the same directory:
//
//	tusd-dedupe -dir ./uploads
//
// Only completed uploads are considered; in-progress uploads are left alone.
// Every upload keeps reading its own data, and terminating one upload does not
// remove content that other uploads still reference.
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
	dir := flag.String("dir", "", "filestore upload directory to reconcile (required, same as tusd's -upload-dir)")
	blobsName := flag.String("blobs", ".blobs", "name of the content-addressed blob directory created inside -dir")
	lockTimeout := flag.Duration("lock-timeout", 30*time.Second, "how long to wait for an in-use upload lock before skipping it this run")
	noGC := flag.Bool("no-gc", false, "do not remove blobs that are no longer referenced by any upload")
	dryRun := flag.Bool("dry-run", false, "only report what would be done; do not create, link, rename or delete anything")
	verbose := flag.Bool("verbose", false, "log per-upload decisions and skipped uploads")
	flag.Parse()

	if *dir == "" {
		flag.Usage()
		os.Exit(2)
	}

	store := filestore.New(*dir)
	reconciler := filestore.NewReconciler(store)
	reconciler.BlobsDirName = *blobsName
	reconciler.LockTimeout = *lockTimeout
	reconciler.GarbageCollect = !*noGC
	reconciler.DryRun = *dryRun
	// Share tusd's per-upload file locks when it is configured with
	// filelocker (the recommended setup). This excludes concurrent
	// PATCH/DELETE/concat requests while an upload is being switched.
	reconciler.Locker = filelocker.New(*dir)
	if *verbose {
		reconciler.Logger = log.New(os.Stderr, "tusd-dedupe: ", log.LstdFlags)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	report, err := reconciler.Reconcile(ctx)
	if err != nil {
		log.Fatalf("tusd-dedupe: %v", err)
	}

	if *dryRun {
		log.New(os.Stdout, "", 0).Print("Dry run (no changes were made):\n" + report.String())
	} else {
		log.New(os.Stdout, "", 0).Print(report.String())
	}
}
