package storage

import (
	"fmt"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// TestAFlushThatCouldNotNameItsTableRefusesWritesUntilARestart: [GUARD the write refusal of a store
// whose flush could not record its table in the MANIFEST says writes stay refused until the node is
// restarted, never that the store 'accepts writes again'; red when it takes the general text]
// (review R2 F-9: the store does not retry that flush, so the general promise is false).
func TestAFlushThatCouldNotNameItsTableRefusesWritesUntilARestart(t *testing.T) {
	cause := fmt.Errorf("%w: flushing a memtable failed 3 times in a row: %w", badger.ErrWritesRefused,
		fmt.Errorf("naming table 1: %w", badger.ErrFlushNotNamed))
	text := (&StoreRefusesWritesError{Dir: "/data/store", Cause: cause}).Error()
	isTrue(t, !strings.Contains(text, "accepts writes again"), "the refusal promises writes come back by themselves: %s", text)
	isTrue(t, strings.Contains(text, "until the node is restarted"), "the refusal does not say a restart is needed: %s", text)
}
