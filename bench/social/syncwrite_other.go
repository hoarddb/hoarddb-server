//go:build !linux

package main

import "os"

// syncWriteFlag makes one write at a time durable. Off Linux there is no
// O_DSYNC (BUG-0722's reason), so the probe opens with O_SYNC: the strongest
// flush the platform offers, which also commits the file's own metadata. It is
// never 0 — that would time buffered writes and report the result as the cost
// of a durable one.
const syncWriteFlag = os.O_SYNC
