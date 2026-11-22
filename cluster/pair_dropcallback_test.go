package cluster

import (
	"sync"
	"testing"
)

// The node registers OnDrop after Raft has started, so a Drop from the state machine can run
// beside it. Under -race this fails when the callback field is read and written unguarded.
func TestOnDropMayBeRegisteredWhileDropsRun(t *testing.T) {
	var set PairSet
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			set.With(uint16(i%8), 1, func(*Pair) {})
			set.Drop(uint16(i%8), 1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			set.OnDrop(func(uint16, uint64) {})
		}
	}()
	wg.Wait()
}
