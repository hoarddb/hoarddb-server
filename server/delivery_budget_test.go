package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestDeliveryBudgetEqualsTheLargeCommitThreshold pins claim-ack-spec §4.4:
// the store's delivery budget and the owner's large-commit record threshold
// are computed the same way from the smallest memtable any member may run, so
// the store cannot write a batch the cluster would not have recorded as large,
// and the two cannot drift apart.
func TestDeliveryBudgetEqualsTheLargeCommitThreshold(t *testing.T) {
	budgetBytes, budgetKeys := store.DeliveryBudget()
	if budgetBytes != largeCommitBytes {
		t.Errorf("the delivery budget is %d bytes, largeCommitBytes is %d", budgetBytes, largeCommitBytes)
	}
	if budgetKeys != largeCommitEntries {
		t.Errorf("the delivery budget is %d keys, largeCommitEntries is %d", budgetKeys, largeCommitEntries)
	}
}
