package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// Mark events: the runner says when the measured window of a repetition opens and closes, so
// a wrapper on another machine can start its profile capture after the warm-up without
// guessing how long the warm-up took.
const (
	markMeasureStart = "measure-start"
	markMeasureEnd   = "measure-end"
)

// markFile returns a mark function that writes an empty file <dir>/<event>-v<vusers>-r<rep>
// for each event. It writes through a temporary name and renames, so a reader never sees a
// half-made file. A failure to write is returned to the caller, which reports it and goes on:
// a missing mark costs a profile, never a measurement.
func markFile(dir string) func(vusers, repetition int, event string) error {
	return func(vusers, repetition int, event string) error {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("making the mark directory: %w", err)
		}
		name := fmt.Sprintf("%s-v%d-r%d", event, vusers, repetition)
		partial := filepath.Join(dir, "."+name)
		if err := os.WriteFile(partial, nil, 0o644); err != nil {
			return fmt.Errorf("writing mark %s: %w", name, err)
		}
		if err := os.Rename(partial, filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("publishing mark %s: %w", name, err)
		}
		return nil
	}
}
