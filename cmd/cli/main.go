package main

import (
	"os"

	"github.com/hoarddb/hoarddb-server/cli"
)

func main() {
	os.Exit(cli.RunMain(cli.ProcessInvocation()))
}
