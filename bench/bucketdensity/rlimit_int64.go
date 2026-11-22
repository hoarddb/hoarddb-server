//go:build freebsd || dragonfly

package main

// rlimitFrom converts the descriptor ceiling to the type syscall.Rlimit holds
// here: int64 on freebsd and dragonfly, where infinity is spelled -1. The value
// is a descriptor count, so it is well inside int64 for every ceiling a run
// would ask for. Named by the type, not by the platform list: netbsd and
// openbsd use uint64, so tagging this !linux would break them.
func rlimitFrom(n uint64) int64 { return int64(n) }
