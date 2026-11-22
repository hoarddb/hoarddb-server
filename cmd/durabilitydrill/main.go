// Command durabilitydrill is the client half of the durability drill
// (scripts/durability-drill.sh): it writes a load to a ring node and keeps a ledger of what
// the node acknowledged, reads every ledger id back after the node was killed and restarted,
// and judges the two against each other.
//
//	durabilitydrill ready --addr HOST:PORT --insecure
//	durabilitydrill write --addr HOST:PORT --insecure --ledger ledger.csv --round 1 --writers 4 --create
//	durabilitydrill read  --addr HOST:PORT --insecure --ledger ledger.csv --out seen.csv
//	durabilitydrill judge --ledger ledger.csv --observations seen.csv
//
// A write is acknowledged only when the node answered "applied"; a lost connection, a passed
// deadline, a refusal and every other answer are recorded as unknown, never as acknowledged.
// judge reaches no server and exits 0 only when every acknowledged write was read back, none
// twice, and the bucket holds nothing the ledger does not. The command imports ringclient
// and cluster, never clients/.
package main

import (
	"errors"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "ready":
		err = runReady(os.Args[2:], os.Stdout)
	case "write":
		err = runWrite(os.Args[2:], os.Stdout)
	case "read":
		err = runRead(os.Args[2:], os.Stdout)
	case "judge":
		err = runJudge(os.Args[2:], os.Stdout)
	case "delete-acknowledged":
		err = runDelete(os.Args[2:], os.Stdout)
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "durabilitydrill: unknown command %q\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	switch {
	case err == nil:
	case errors.Is(err, errDrillFailed):
		os.Exit(1) // the report above already says why
	default:
		fmt.Fprintln(os.Stderr, "durabilitydrill:", err)
		os.Exit(3)
	}
}

const usage = `usage: durabilitydrill ready|write|read|judge|delete-acknowledged [flags]

  durabilitydrill ready --addr 127.0.0.1:7433 --insecure --wait 120s
  durabilitydrill write --addr 127.0.0.1:7433 --insecure --ledger ledger.csv --round 1 --writers 4 --create
  durabilitydrill read  --addr 127.0.0.1:7433 --insecure --ledger ledger.csv --out seen.csv
  durabilitydrill judge --ledger ledger.csv --observations seen.csv

write runs until its connections are lost (the server was killed) or it gets SIGTERM, and
appends one row per write it sent to the ledger: acked only for an answer of "applied",
unknown for everything else. read reads every ledger id back and scans the whole bucket.
judge prints the counts and exits 0 on PASS, 1 on FAIL; any other failure exits 3.
delete-acknowledged removes one acknowledged document, to prove the drill then fails.

The whole drill, with the server started, killed and restarted for you:
  scripts/durability-drill.sh --dir /var/tmp --rounds 5 --writers 4

The password comes from DURABILITY_DRILL_PASSWORD (or HOARDDB_ROOT_PASSWORD), the user from
DURABILITY_DRILL_USER (default admin). Pass --fingerprint SHA256:... to pin the node, or
--insecure for a node you started yourself on loopback.
`
