package wire

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Two clients, then thirty-two, each ask to move one document out of "Ada" into their own
// name, with the answer after. The pair's critical section serialises them: exactly one is
// applied, every other is refused with the winner's name in its document, and the log
// holds one update.
func TestTwoGuardedUpdatesOnOneDocumentSerialise(t *testing.T) {
	for _, writers := range []int{2, 32} {
		t.Run(fmt.Sprintf("%d writers", writers), func(t *testing.T) {
			fx := newGuardFixture(t)
			fx.mustWrite(testSlot, "s1") // Name = "Ada"
			before := fx.pairs.Snapshot(testSlot, testDB)
			loggedBefore := len(fx.logged())

			answers := make([]WriteAnswer, writers)
			var wait sync.WaitGroup
			start := make(chan struct{})
			for i := range writers {
				wait.Add(1)
				go func() {
					defer wait.Done()
					op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString(fmt.Sprintf("w%02d", i))))
					op.Return = cluster.ReturnAfter
					<-start
					answer, err := fx.writeOp(op, byte(i+1))
					if err != nil {
						t.Errorf("writer %d: %v", i, err)
						return
					}
					answers[i] = answer
				}()
			}
			close(start)
			wait.Wait()

			winner, applied := "", 0
			for i, answer := range answers {
				if !answer.NotApplied {
					applied++
					winner = fmt.Sprintf("w%02d", i)
				}
			}
			if applied != 1 {
				t.Fatalf("%d of %d guarded updates were applied, want exactly 1", applied, writers)
			}
			for i, answer := range answers {
				if !answer.NotApplied {
					continue
				}
				scalars, err := DecodeScalarSet(answer.Result)
				if err != nil || string(scalars[2]) != string(guardString(winner)) {
					t.Errorf("writer %d was refused with Name %q (%v), want the winner's %q", i, scalars[2], err, winner)
				}
			}
			after := fx.pairs.Snapshot(testSlot, testDB)
			if after.AppendedSeq != before.AppendedSeq+1 || len(fx.logged()) != loggedBefore+1 {
				t.Fatalf("appended seq %d -> %d and %d -> %d log records, want exactly one of each", before.AppendedSeq, after.AppendedSeq, loggedBefore, len(fx.logged()))
			}
		})
	}
}
