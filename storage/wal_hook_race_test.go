package storage

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestSettingTheBeforeSyncHookWhileAppendsRunIsRaceFree is meaningful under
// -race: SetBeforeSyncHook is exported, so it can be called from a goroutine
// other than the ones appending.
func TestSettingTheBeforeSyncHookWhileAppendsRunIsRaceFree(t *testing.T) {
	w := openReaderTestWAL(t, t.TempDir(), false, 64<<10)
	defer func() { _ = w.Close() }()

	const (
		appenders   = 4
		perAppender = 100
	)
	var hookCalls atomic.Int64
	var appendersDone sync.WaitGroup
	for range appenders {
		appendersDone.Add(1)
		go func() {
			defer appendersDone.Done()
			for range perAppender {
				if _, err := w.Append("docs", OpInsert, []byte("k"), []byte("v")); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	setterDone := make(chan struct{})
	go func() {
		defer close(setterDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			w.SetBeforeSyncHook(func(uint64) { hookCalls.Add(1) })
			w.SetBeforeSyncHook(nil)
		}
	}()

	appendersDone.Wait()
	close(stop)
	<-setterDone
	if got := w.LastSequence(); got != appenders*perAppender {
		t.Errorf("LastSequence = %d after the appends, want %d", got, appenders*perAppender)
	}
}
