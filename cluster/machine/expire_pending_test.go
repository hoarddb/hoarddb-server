package machine

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// A pending table can carry the bucket's expiry declaration: it is inert until the published
// version reaches it, exactly as the pending field table is, so `set expire at` is visible on
// every node at the bump and on none before it.
func TestPendingExpireIsEffectiveOnlyOnceThePendingVersionIsPublished(t *testing.T) {
	now := ExpireTable{BucketField: 4}
	next := ExpireTable{BucketField: 5}
	d := Declaration{Version: 3, Expire: now, Pending: &Pending{Version: 4, Expire: &next}}
	if got := EffectiveExpire(d, 3); got.BucketField != 4 {
		t.Fatalf("before the bump the effective expiry is field %d, want 4", got.BucketField)
	}
	if got := EffectiveExpire(d, 4); got.BucketField != 5 {
		t.Fatalf("at the bump the effective expiry is field %d, want 5", got.BucketField)
	}
	if folded := Fold(d); folded.Expire.BucketField != 5 || folded.Pending != nil {
		t.Fatalf("Fold left expiry %+v pending %v, want field 5 and no pending", folded.Expire, folded.Pending)
	}
}

func TestAPendingWithoutAnExpireSectionKeepsTheCurrentExpiry(t *testing.T) {
	d := Declaration{Version: 3, Expire: ExpireTable{BucketField: 4}, Pending: &Pending{Version: 4}}
	if got := EffectiveExpire(d, 9); got.BucketField != 4 {
		t.Fatalf("a pending that does not mention expiry changed it to field %d", got.BucketField)
	}
	if folded := Fold(d); folded.Expire.BucketField != 4 {
		t.Fatalf("Fold dropped the expiry: %+v", folded.Expire)
	}
}

func TestAPendingExpireSectionMayClearTheExpiry(t *testing.T) {
	cleared := ExpireTable{}
	d := Declaration{Version: 3, Expire: ExpireTable{BucketField: 4}, Pending: &Pending{Version: 4, Expire: &cleared}}
	if got := EffectiveExpire(d, 4); got.BucketField != 0 {
		t.Fatalf("`drop expire` left field %d in force", got.BucketField)
	}
}

// The compiled system table lists eight fields and no expiry table; the field-id check admits the
// expiry table (field 11) for the system bucket by name, and no other bucket may write it.
func TestOnlyTheSystemBucketMayWriteTheExpiryTableField(t *testing.T) {
	decl, published := SystemView(1)
	admission := Admission{Present: true, Decl: decl, Seen: published, Published: published, PayloadBucketID: SystemBucketID, WrittenFieldIDs: []uint16{3, 5, 8, SystemExpireField}}
	if got := Fence(admission); got.Code != 0 {
		t.Fatalf("a write of field 11 to the system bucket was refused: %+v", got)
	}
	admission.WrittenFieldIDs = []uint16{9}
	if got := Fence(admission); got.Code != cluster.CodeFieldType {
		t.Fatalf("a write of field 9, which the system table does not declare: %+v, want FIELD_TYPE", got)
	}
}

// A user bucket never writes field 11: the id is admitted for the system bucket by name, so the same id
// on a user bucket's update is still FIELD_TYPE. (A check that admits id 11 for every bucket would let a
// document write land under the id a declaration uses for its expiry table.)
func TestAUserBucketMayNotWriteFieldEleven(t *testing.T) {
	decl := Declaration{BucketID: 0x1234, Version: 1, State: StateActive, Fields: FieldTable{NextFieldID: 4,
		Entries: []FieldEntry{{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}, {ID: 3, Type: "datetime", Name: "At"}}}}
	admission := Admission{Present: true, Decl: decl, Seen: 1, Published: 1, PayloadBucketID: decl.BucketID, AdmittedVersion: 1, WrittenFieldIDs: []uint16{2, 3}}
	if got := Fence(admission); got.Code != 0 {
		t.Fatalf("a write of declared fields was refused: %+v", got)
	}
	admission.WrittenFieldIDs = []uint16{SystemExpireField}
	if got := Fence(admission); got.Code != cluster.CodeFieldType {
		t.Fatalf("an update of field 11 on a user bucket: %+v, want FIELD_TYPE", got)
	}
}
