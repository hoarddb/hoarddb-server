package main

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// runVerify is faultctl verify: it prints the verdict block and returns an error when any
// verdict failed, so the exit status is the answer.
func runVerify(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	acksPath := flags.String("acks", "", "CSV written by faultctl write (required)")
	obsPath := flags.String("obs", "", "CSV written by faultctl read (required)")
	lostPath := flags.String("lost", "", "declared loss set: one 'id reason' per line")
	replicationFactor := flags.Int("rf", 1, "replication factor of the run's database")
	maxRefused := flags.Int("max-refused", 50, "consecutive refusals a live node may give before it is a bug; 0 for no bound (a deliberately full disk)")
	down := flags.String("down", "", "windows when the node was down on purpose, FROM_NS:TO_NS[,FROM_NS:TO_NS] (Unix nanoseconds)")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parsing verify flags: %w", err)
	}
	if *acksPath == "" || *obsPath == "" {
		return fmt.Errorf("verify needs --acks and --obs, for example: faultctl verify --acks acks.csv --obs obs.csv")
	}
	acks, err := loadAttempts(*acksPath)
	if err != nil {
		return err
	}
	obs, err := loadAttempts(*obsPath)
	if err != nil {
		return err
	}
	lost, err := loadLost(*lostPath)
	if err != nil {
		return err
	}
	if err := requireAttempted(*acksPath, acks, lost); err != nil {
		return err
	}
	windows, err := parseDown(*down)
	if err != nil {
		return err
	}
	verdicts := verify(acks, obs, verifyOptions{Lost: lost, RF: *replicationFactor, MaxRefused: *maxRefused, Down: windows})
	var block strings.Builder
	failed := 0
	for _, found := range verdicts {
		status := "OK  "
		if !found.OK {
			status = "FAIL"
			failed++
		}
		fmt.Fprintf(&block, "%s %s: %s\n", status, found.Name, found.Detail)
		for _, line := range found.Excused {
			fmt.Fprintf(&block, "       excused %s\n", line)
		}
	}
	if failed == 0 {
		fmt.Fprintf(&block, "all %d invariants hold\n", len(verdicts))
	}
	if _, err := io.WriteString(out, block.String()); err != nil {
		return fmt.Errorf("printing the verdict block: %w", err)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d invariants failed", failed, len(verdicts))
	}
	return nil
}

// requireAttempted refuses a run that judged nothing: a header-only or all-refused acks file
// would otherwise print "0 acked writes checked ... all 5 invariants hold" and exit 0, which
// is how a load run that never started would pass.
func requireAttempted(path string, acks []attempt, lost map[string]string) error {
	if len(acks) == 0 {
		return fmt.Errorf("%s has no attempted writes, so there is nothing to verify: check that 'faultctl write' ran and wrote its --out file", path)
	}
	for _, row := range acks {
		if row.Outcome != "" {
			return nil
		}
	}
	if len(lost) == 0 {
		return fmt.Errorf("%s has %d attempted writes and none was acknowledged, so there is nothing to verify: every write was refused, or pass --lost if the run really lost everything on purpose", path, len(acks))
	}
	return nil
}
