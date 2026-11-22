package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// clientBoundFraction is the CPU share above which a repetition is labelled client-bound:
	// the load client, not the database, starts to set the numbers.
	clientBoundFraction = 0.85
	// userHZ is the unit of /proc/<pid>/stat CPU times, 100 on every Linux architecture.
	userHZ = 100
)

// cpuProbe reads the CPU time this process has used, so the runner can say how much of the
// client machine a repetition kept busy.
type cpuProbe interface {
	cpuTime() (time.Duration, error)
}

// procProbe reads the stat file of a /proc/<pid> directory.
type procProbe struct{ dir string }

// cpuTime is the user plus system CPU time the process has used so far.
func (p procProbe) cpuTime() (time.Duration, error) {
	content, err := os.ReadFile(filepath.Join(p.dir, "stat"))
	if err != nil {
		return 0, fmt.Errorf("reading the process CPU time: %w", err)
	}
	// The command name is in parentheses and may contain spaces and parentheses, so fields
	// are counted from the last closing one; after it utime (14) and stime (15) are 11 and 12.
	text := string(content)
	closing := strings.LastIndexByte(text, ')')
	if closing < 0 {
		return 0, fmt.Errorf("process stat %q has no command name in parentheses", strings.TrimSpace(text))
	}
	fields := strings.Fields(text[closing+1:])
	if len(fields) < 13 {
		return 0, errors.New("process stat has too few fields after the command name")
	}
	var ticks int64
	for _, field := range fields[11:13] {
		count, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("reading CPU ticks %q from the process stat: %w", field, err)
		}
		ticks += count
	}
	return time.Duration(ticks) * time.Second / userHZ, nil
}

// clientBoundLabel is true when a repetition used more than clientBoundFraction of the client's CPU.
func clientBoundLabel(cpuFraction float64) bool { return cpuFraction > clientBoundFraction }

// cmdCPU runs a shell command that prints the CPU seconds a remote server process has used so
// far (the wrapper builds an ssh line), so the record can say what the engine's host spent.
type cmdCPU string

// serverCPUTimeout bounds one probe so a dead ssh cannot stall the start or end of a window.
const serverCPUTimeout = 15 * time.Second

func (c cmdCPU) cpuTime() (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), serverCPUTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "sh", "-c", string(c)).Output()
	if err != nil {
		return 0, fmt.Errorf("running -server-cpu-cmd %q: %w", string(c), err)
	}
	seconds, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0, fmt.Errorf("-server-cpu-cmd %q must print cumulative CPU seconds as one number, it printed %q: %w", string(c), strings.TrimSpace(string(out)), err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
