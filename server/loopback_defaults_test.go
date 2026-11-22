package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestPProfListenIsLoopback is SEC-1: the pprof server has no
// config field of its own, so its package default is the only thing keeping
// an unauthenticated profiler off the network.
func TestPProfListenIsLoopback(t *testing.T) {
	if !config.ListensOnLoopbackOnly(pprofListen) {
		t.Errorf("pprofListen = %q, want a loopback address", pprofListen)
	}
}
