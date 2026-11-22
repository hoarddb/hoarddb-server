//go:build !linux

package server

import "testing"

// TestHostFactsOffLinuxReportCPUsOnly: no /proc here, so memory and load are
// absent, not an error and not a made-up number.
func TestHostFactsOffLinuxReportCPUsOnly(t *testing.T) {
	facts := readHostFacts(hostSource{numCPU: func() int { return 6 }})
	if facts.CPUs != 6 || facts.HostCPUs != 0 || facts.MemoryTotalBytes != 0 || facts.MemoryUsedBytes != 0 || facts.MemorySource != "" || facts.Load != nil {
		t.Errorf("facts = %+v, want CPUs 6 and nothing else", facts)
	}
}
