package main

import (
	"flag"
	"fmt"

	"github.com/hoarddb/hoarddb-server/config"
)

// printUsage is --help: real examples first (AGENTS.md rule 8), then the flags
// the flag package generates. It writes where flag.Usage does, to stderr.
func printUsage() {
	output := flag.CommandLine.Output()
	// A failed write to stderr leaves nowhere to report the failure.
	_, _ = fmt.Fprintf(output, `hoarddb-server runs a hoardDB node. With no arguments it starts with the
built-in defaults and keeps its data in ./data.

Examples:
  hoarddb-server
      start with the defaults
  hoarddb-server --config /etc/hoarddb/config.yaml
      start with a YAML config file
  %s=/etc/hoarddb/config.yaml hoarddb-server
      the same, from the environment (the flag wins when both are given)
  HOARDDB_DATA_DIR=/var/lib/hoarddb hoarddb-server
      change one setting for one start
  hoarddb-server -v
      print the version

A config file is layered over the built-in defaults, and HOARDDB_* environment
variables are layered over the file. A file that is missing, is not valid YAML
or holds an unknown key stops the start with a message that says why. Every
setting, and a complete example file: the configuration page of the user docs.

Flags:
`, config.PathEnv)
	flag.PrintDefaults()
}
