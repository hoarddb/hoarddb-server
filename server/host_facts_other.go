//go:build !linux

package server

// readHostFacts off Linux reports the CPU count alone. /proc and the cgroup
// files do not exist there, so memory and load are absent (the fields are
// omitempty) rather than an error or an invented number.
func readHostFacts(src hostSource) *HealthHost {
	return &HealthHost{CPUs: src.numCPU()}
}
