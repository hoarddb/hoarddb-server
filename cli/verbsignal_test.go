package cli

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestVerbStopSignalsEndTheRun: cli/exitcodes.go says exit 4 is for SIGINT or SIGTERM, so
// both stop a verb's run through the one shared helper. The signal is sent to this process
// while the helper's context is live, as a terminal's Ctrl-C or a service manager's stop would.
func TestVerbStopSignalsEndTheRun(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, stop := verbStopContext()
			defer stop()
			if err := syscall.Kill(os.Getpid(), sig.(syscall.Signal)); err != nil {
				t.Fatalf("sending %v to the test process: %v", sig, err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatalf("%v did not end the verb's context", sig)
			}
		})
	}
}

// TestEveryRingVerbStopsOnTheSharedHelper: dump, ingest and restore take their stop signals
// from verbStopContext and register none of their own, so a verb cannot handle SIGINT and
// forget SIGTERM again.
func TestEveryRingVerbStopsOnTheSharedHelper(t *testing.T) {
	for _, file := range []string{"ringdump.go", "ringingest_run.go", "ringrestore.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(source), "signal.NotifyContext") {
			t.Errorf("%s registers its own stop signals; use verbStopContext", file)
		}
		if !strings.Contains(string(source), "verbStopContext()") {
			t.Errorf("%s does not take its stop signals from verbStopContext", file)
		}
	}
}
