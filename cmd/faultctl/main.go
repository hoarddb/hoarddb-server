// Command faultctl drives a ring node from a client VM during the fault runs of
// the ring fault plan section 4, and judges the result.
//
//	faultctl write  --addr HOST:PORT --db app --bucket Events --clients 4 --rate 50 --duration 60s --seed 7 > acks.csv
//	faultctl read   --addr HOST:PORT --db app --bucket Events --ids acks.csv > obs.csv
//	faultctl verify --acks acks.csv --obs obs.csv [--lost ids.txt] [--rf 1] [--down FROM_NS:TO_NS]
//
// write and read print one CSV row per attempt (see csvHeader). verify applies the five
// invariants of the harness ledger and prints one verdict block; it exits 0 only when every
// verdict is OK. The command imports ringclient and cluster, never the harness and never clients/.
package main

import (
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
	case "verify":
		err = runVerify(os.Args[2:], os.Stdout)
	case "digest":
		err = runDigest(os.Args[2:], os.Stdout)
	case "compare":
		err = runCompare(os.Args[2:], os.Stdout)
	case "write", "read":
		err = runLoad(os.Args[1], os.Args[2:], os.Stdout)
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "faultctl: unknown command %q\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "faultctl:", err)
		os.Exit(1)
	}
}

const usage = `usage: faultctl write|read|verify|digest|compare [flags]

  faultctl write  --addr 192.0.2.1:7433 --db app --bucket Events --create --clients 4 --rate 50 --duration 60s --seed 7 --insecure > acks.csv
  faultctl read   --addr 192.0.2.1:7433 --db app --bucket Events --ids acks.csv --insecure > obs.csv
  faultctl verify --acks acks.csv --obs obs.csv --lost lost-ids.txt --rf 1
  faultctl digest --store /var/lib/hoarddb/ring/badger --node node1 --out node1.json     (on a STOPPED node)
  faultctl compare --keys 5 --dir node1=/var/lib/hoarddb/ring/badger --dir node2=/path/to/node2/ring/badger node1.json node2.json

digest and compare never write to a data directory; compare exits 0 only when every pair two nodes
share is equal at the same applied position (the copy compare note).

The password comes from FAULTCTL_PASSWORD (or HOARDDB_ROOT_PASSWORD), the user from FAULTCTL_USER
(default root). Pass --fingerprint SHA256:... to pin the node, or --insecure on a test VM.
`
