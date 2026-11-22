package main

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// freeFunc returns the free bytes of the data disk.
type freeFunc func(ctx context.Context) (int64, error)

// cmdFree runs a shell command that prints the free bytes of a remote data disk (the wrapper
// supplies an ssh df line) and reads the integer it prints.
func cmdFree(command string) freeFunc {
	return func(ctx context.Context) (int64, error) {
		out, err := exec.CommandContext(ctx, "sh", "-c", command).Output()
		if err != nil {
			return 0, fmt.Errorf("running -free-cmd %q: %w", command, err)
		}
		free, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("-free-cmd %q must print the free bytes as one integer, it printed %q: %w", command, strings.TrimSpace(string(out)), err)
		}
		return free, nil
	}
}

// diskGuard stops a run before a full disk crashes the engine. A full disk makes hoardDB die
// with SIGBUS, so the check runs every interval, and a disk it cannot read counts as full.
type diskGuard struct {
	free     freeFunc
	minFree  int64
	interval time.Duration
}

// watch checks until ctx ends or the first failing check, which it reports through abort
// before returning. It owns no other goroutine.
func (g diskGuard) watch(ctx context.Context, abort func(freeBytes int64, err error)) {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			free, err := g.free(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				abort(0, fmt.Errorf("cannot read the free disk space: %w", err))
				return
			}
			if free < g.minFree {
				abort(free, nil)
				return
			}
		}
	}
}
