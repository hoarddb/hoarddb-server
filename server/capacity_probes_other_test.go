//go:build !linux

package server

// platformProbeFailureRows is empty off Linux: the /proc/self/status parser
// those rows exercise is Linux-only.
func platformProbeFailureRows() []probeFailureRow { return nil }
