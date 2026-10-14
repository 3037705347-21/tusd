package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tus/tusd/v2/pkg/alias"
	"github.com/tus/tusd/v2/pkg/filestore"
)

const aliasCommandUsage = `tusd aliases - manage the alias manifest for legacy upload names

Usage:
  tusd aliases apply -file <changes.json> [-upload-dir <dir>] [-aliases-file <path>]
  tusd aliases list                    [-upload-dir <dir>] [-aliases-file <path>]

A change set file has the following format:

  {
    "changes": [
      {"op": "upsert", "alias": "2023/report", "canonical_id": "f8e2c1a0"},
      {"op": "unbind", "alias": "2022/draft"}
    ]
  }

"upsert" adds a binding or rebinds an existing alias to another canonical
upload; "unbind" removes a binding. The batch is validated and applied
atomically: if a change is contradictory, a canonical upload does not exist or
the manifest cannot be written, the whole batch is rejected and the manifest is
left unchanged. Re-running the same change set is a no-op.
`

// RunAliasCommand implements the `tusd aliases` subcommand. It is invoked
// before regular flag parsing so that it can use its own flag set.
func RunAliasCommand(args []string) {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, aliasCommandUsage)
		os.Exit(2)
	}

	switch args[0] {
	case "apply":
		runAliasApply(args[1:])
	case "list":
		runAliasList(args[1:])
	case "-h", "--help", "help":
		fmt.Print(aliasCommandUsage)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n%s", args[0], aliasCommandUsage)
		os.Exit(2)
	}
}

func aliasManifestFlags(fs *flag.FlagSet) (uploadDir, aliasesFile *string) {
	uploadDir = fs.String("upload-dir", "./data", "Directory in which the uploads are stored")
	aliasesFile = fs.String("aliases-file", "", "Location of the alias manifest (defaults to <upload-dir>/aliases.json)")
	return uploadDir, aliasesFile
}

func aliasManifestPath(uploadDir, aliasesFile string) string {
	if aliasesFile != "" {
		return aliasesFile
	}
	return filepath.Join(uploadDir, "aliases.json")
}

func runAliasApply(args []string) {
	fs := flag.NewFlagSet("aliases apply", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(fs.Output(), aliasCommandUsage) }
	uploadDir := fs.String("upload-dir", "./data", "Directory in which the uploads are stored")
	aliasesFile := fs.String("aliases-file", "", "Location of the alias manifest (defaults to <upload-dir>/aliases.json)")
	changeFile := fs.String("file", "", "Path to the JSON change set to apply")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	if *changeFile == "" {
		fmt.Fprintln(os.Stderr, "missing required -file argument")
		fmt.Fprint(os.Stderr, aliasCommandUsage)
		os.Exit(2)
	}

	f, err := os.Open(*changeFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open change set: %s\n", err)
		os.Exit(1)
	}
	defer f.Close()

	changes, err := alias.ParseChanges(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	// Canonical uploads are verified against the uploads in -upload-dir.
	core := filestore.New(*uploadDir)
	store := alias.NewFileStore(aliasManifestPath(*uploadDir, *aliasesFile), core)

	result, err := store.Apply(context.Background(), changes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Alias batch rejected, manifest unchanged: %s\n", err)
		os.Exit(1)
	}

	switch {
	case result.Empty():
		fmt.Println("Manifest already up to date, no changes applied.")
	default:
		fmt.Printf("Applied %d upsert(s) and %d unbind(s); manifest updated.\n",
			len(result.Upserted), len(result.Unbound))
		for _, binding := range result.Upserted {
			fmt.Printf("  upsert  %s -> %s\n", binding.Alias, binding.CanonicalID)
		}
		for _, aliasName := range result.Unbound {
			fmt.Printf("  unbind  %s\n", aliasName)
		}
	}
}

func runAliasList(args []string) {
	fs := flag.NewFlagSet("aliases list", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(fs.Output(), aliasCommandUsage) }
	uploadDir := fs.String("upload-dir", "./data", "Directory in which the uploads are stored")
	aliasesFile := fs.String("aliases-file", "", "Location of the alias manifest (defaults to <upload-dir>/aliases.json)")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	core := filestore.New(*uploadDir)
	store := alias.NewFileStore(aliasManifestPath(*uploadDir, *aliasesFile), core)

	bindings, err := store.List(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read alias manifest: %s\n", err)
		os.Exit(1)
	}

	if err := writeJSON(os.Stdout, bindings); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to write alias manifest: %s\n", err)
		os.Exit(1)
	}
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
