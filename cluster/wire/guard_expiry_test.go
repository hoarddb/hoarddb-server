package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// An expired document is absent (expiry spec 3.2), and a guard on an absent document is
// NOT_FOUND before any comparison (guarded-update spec 8), so the guard must be judged at the
// attempt's instant, not against the stored fields of a document that has expired.
func TestGuardTreatsAnExpiredDocumentAsAbsent(t *testing.T) {
	rig := newExpiryRig(t)
	rig.apply(expiryInsert(100))
	guarded := cluster.Op{
		Class: cluster.OpDelete, BucketID: testBucket, EncID: claim34EncID0(),
		Guard: []cluster.GuardTerm{{FieldID: expiryOtherField, Value: []byte{0x0a}}},
	}

	guarded.ExpireNow = 50
	live, err := rig.applier.CheckGuard(testSlot, testDB, guarded)
	if err != nil || live.NotFound || live.Refused {
		t.Fatalf("a guard before the document expires: %+v, %v, want it to hold", live, err)
	}
	guarded.ExpireNow = 1000
	gone, err := rig.applier.CheckGuard(testSlot, testDB, guarded)
	if err != nil || !gone.NotFound {
		t.Fatalf("a guard after the document expired: %+v, %v, want NotFound", gone, err)
	}
}
