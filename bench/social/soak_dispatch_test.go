package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// The soak drives the same workers as a leg, so the harness passes -mix with -mode soak. main used to
// send any run with a -mix to the leg runner first, so a soak ran as a one-off leg and printed a leg record.
// This test runs main in a child process and checks that -mode soak wins: it must fail on the missing
// -soak-dir, which only the soak driver asks for.
func TestSoakModeWithMixRunsTheSoakDriver(t *testing.T) {
	if os.Getenv("SOCIAL_TEST_RUN_MAIN") == "1" {
		os.Args = append([]string{"social"}, strings.Fields(os.Getenv("SOCIAL_TEST_ARGS"))...)
		main()
		return
	}
	child := exec.Command(os.Args[0], "-test.run=^TestSoakModeWithMixRunsTheSoakDriver$")
	child.Env = append(os.Environ(), "SOCIAL_TEST_RUN_MAIN=1", "SOCIAL_TEST_ARGS=-mode soak -mix sk -loop open -rate 10 -attach")
	output, err := child.CombinedOutput()
	if err == nil {
		t.Fatalf("-mode soak -mix sk with no -soak-dir succeeded: %s", output)
	}
	if !strings.Contains(string(output), "-soak-dir") {
		t.Fatalf("-mode soak -mix sk did not reach the soak driver (want its -soak-dir complaint): %s", output)
	}
}
