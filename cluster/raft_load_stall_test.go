package cluster

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestStateLoadedStaysShutWhileTheFSMStallsMidReplay (review N1): a node restarts with six
// commands to replay and the FSM stalls on the third create-database (a schema publish that
// waits on a pair lock, a starved FSM goroutine). The library's AppliedIndex has moved on by
// then, so a rule built on it and a quiet state_seq opens the replay gate on a half-replayed
// state; StateLoaded must stay false until the FSM applied all six. Break: put back the
// "handed and state_seq stable for two steps" fallback.
func TestStateLoadedStaysShutWhileTheFSMStallsMidReplay(t *testing.T) {
	for _, stall := range []time.Duration{250 * time.Millisecond, 600 * time.Millisecond} {
		t.Run(stall.String(), func(t *testing.T) {
			dir := t.TempDir()
			first := openOrFail(t, quickRaft(t, dir, "n1"))
			for i := int64(1); i <= 5; i++ {
				createDatabase(t, first, "d"+string(rune('0'+i)), i)
			}
			want := first.StateSeq()
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			var seen atomic.Int32
			cfg := quickRaft(t, dir, "n1")
			cfg.Bootstrap = false
			cfg.ApplyGate = func(kind byte) {
				if kind == CommandCreateDatabase && seen.Add(1) == 3 {
					time.Sleep(stall)
				}
			}
			re := openOrFail(t, cfg)
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if re.StateLoaded() {
					if re.StateSeq() != want || len(re.State().Databases) != 5 {
						t.Fatalf("StateLoaded opened mid-replay: state_seq %d want %d, databases %d want 5", re.StateSeq(), want, len(re.State().Databases))
					}
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("StateLoaded never opened")
		})
	}
}
