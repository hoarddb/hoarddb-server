package server

import (
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestWithholdingAnOpcodeRacesNoReader pins the race the race gate found in
// TestAnOlderOwnerRefusesAForwardedClaimBeforeItsLog: the test deleted an entry
// of the package-level localHandlers map while a server goroutine of the same
// process was reading it. Withholding an opcode for a test must go through a
// synchronised seam, so a concurrent reader is safe.
func TestWithholdingAnOpcodeRacesNoReader(t *testing.T) {
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				implementsOpcode(transport.OpQueueClaim)
			}
		}
	}()
	for range 2000 {
		restore := withholdOpcode(transport.OpQueueClaim)
		if implementsOpcode(transport.OpQueueClaim) {
			t.Fatal("a withheld opcode is still reported as implemented")
		}
		restore()
	}
	close(stop)
	readers.Wait()
	if !implementsOpcode(transport.OpQueueClaim) {
		t.Fatal("the opcode was not restored")
	}
}

// withholdOpcode makes this build report op as not implemented and returns the
// function that restores it.
func withholdOpcode(op transport.Opcode) (restore func()) {
	withheldOpcodeForTesting.Store(&op)
	return func() { withheldOpcodeForTesting.Store(nil) }
}
