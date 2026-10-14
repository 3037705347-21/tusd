package main

import (
	"os"

	"github.com/tus/tusd/v2/cmd/tusd/cli"
)

func main() {
	// The alias manifest is managed out of band from the tus protocol, so it
	// gets its own subcommand with its own flags instead of server flags.
	if len(os.Args) > 1 && os.Args[1] == "aliases" {
		cli.RunAliasCommand(os.Args[2:])
		return
	}

	cli.ParseFlags()
	cli.PrepareGreeting()

	// Print version and other information and exit if the -version flag has been
	// passed else we will start the HTTP server
	if cli.Flags.ShowVersion {
		cli.ShowVersion()
	} else {
		cli.CreateComposer()
		cli.Serve()
	}
}
