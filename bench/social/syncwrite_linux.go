//go:build linux

package main

import "syscall"

// syncWriteFlag makes one write at a time durable. Linux names that
// O_DSYNC, which commits the data and only the metadata the write needs;
// see syncwrite_other.go for the platforms that have no O_DSYNC.
const syncWriteFlag = syscall.O_DSYNC
