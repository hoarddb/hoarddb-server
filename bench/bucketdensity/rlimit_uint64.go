//go:build !freebsd && !dragonfly

package main

// rlimitFrom converts the descriptor ceiling to the type syscall.Rlimit holds
// here: uint64 on linux, darwin, netbsd and openbsd. See rlimit_int64.go for
// the two BSDs that spell it int64.
func rlimitFrom(n uint64) uint64 { return n }
