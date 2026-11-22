//go:build !linux

package main

import "testing"

// TestSyncWriteFlagIsDurable pins the one property -mode syncprobe depends on:
// the probe times a durable write, so the open flag must commit data. A zero
// flag would open the file buffered, time memcpy-speed writes and print them as
// the cost of a flush, which would make the durability witness pass on a disk
// that never flushed anything.
//
// Note that CI only compiles this test: the freebsd vet in scripts/build-matrix.sh
// type-checks it, and nothing runs it there. Run it on a BSD host after changing
// the constant.
func TestSyncWriteFlagIsDurable(t *testing.T) {
	if syncWriteFlag == 0 {
		t.Fatal("syncWriteFlag is 0: syncprobe would time buffered writes and report them as durable ones")
	}
}
