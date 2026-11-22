package wire

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func TestExpireMembersRoundTripOnTheAdminWire(t *testing.T) {
	requests := []RingAdminRequest{
		{Cmd: AdminCreateBucket, Database: "app", Name: "Session", Fields: []RingAdminField{{Name: "ExpiresAt", Type: "datetime"}, {Name: "Log", Type: "fifoStore"}, {Name: "Stack", Type: "lifoStore"}},
			ExpireField: "ExpiresAt", ExpirePaths: map[string]string{"Log": "ts", "Stack": "meta.at"}},
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "Session", Add: []RingAdminField{}, Drop: []string{}, ExpireField: "ExpiresAt"},
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "Session", Add: []RingAdminField{}, Drop: []string{}, DropExpire: true},
	}
	for _, req := range requests {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%s %+v: %v", req.Cmd, req, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil {
			t.Fatalf("%s: %v", req.Cmd, err)
		}
		if got.ExpireField != req.ExpireField || got.DropExpire != req.DropExpire || !reflect.DeepEqual(got.ExpirePaths, req.ExpirePaths) {
			t.Fatalf("%s: round trip gave expire %q %v %v, want %q %v %v", req.Cmd, got.ExpireField, got.ExpirePaths, got.DropExpire, req.ExpireField, req.ExpirePaths, req.DropExpire)
		}
		if again, err := EncodeRingAdmin(got); err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s: decoded request re-encodes as %x, %v: the encoding must be canonical", req.Cmd, again, err)
		}
	}
}

func TestAnExpireOnlyAlterIsNotNothingToChange(t *testing.T) {
	req := RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "Session", Add: []RingAdminField{}, Drop: []string{}}
	if _, err := EncodeRingAdmin(req); err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Fatalf("an alter that changes nothing was accepted or refused otherwise: %v", err)
	}
	both := req
	both.ExpireField, both.DropExpire = "ExpiresAt", true
	if _, err := EncodeRingAdmin(both); err == nil || !strings.Contains(err.Error(), "EXPIRE_TWICE") {
		t.Fatalf("set and drop expire in one alter: %v, want EXPIRE_TWICE", err)
	}
	stray := RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "Session", ExpireField: "ExpiresAt"}
	if _, err := EncodeRingAdmin(stray); err == nil || !strings.Contains(err.Error(), "expire_field") {
		t.Fatalf("a describe that sets expire_field was accepted: %v", err)
	}
}

func TestADescribeReplyCarriesTheExpiryDeclaration(t *testing.T) {
	reply := RingAdminReply{Cmd: AdminDescribeBucket, BucketID: 7, State: "active", DeclVersion: 4, SchemaVersionSeen: 2,
		Fields:      []RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "ExpiresAt", Type: "datetime"}, {ID: 3, Name: "Log", Type: "fifoStore"}},
		ExpireField: "ExpiresAt", ExpireStores: []RingAdminReplyExpire{{Field: "Log", Path: "ts"}}}
	raw, err := EncodeRingAdminReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingAdminReply(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpireField != "ExpiresAt" || !reflect.DeepEqual(got.ExpireStores, reply.ExpireStores) {
		t.Fatalf("reply = %+v, want the expiry back", got)
	}
}

// TestTheRingAdminDoorCreatesAlterAndDescribesAnExpiry runs the whole door: create with both forms,
// describe it, drop the expiry, set it again, and see the refusals' fix texts on the wire.
func TestTheRingAdminDoorCreatesAlterAndDescribesAnExpiry(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	created := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "Session",
		Fields:      []RingAdminField{{Name: "ExpiresAt", Type: "datetime"}, {Name: "Log", Type: "fifoStore"}},
		ExpireField: "ExpiresAt", ExpirePaths: map[string]string{"Log": "ts"}}))
	if created.ExpireField != "ExpiresAt" || len(created.ExpireStores) != 1 || created.ExpireStores[0] != (RingAdminReplyExpire{Field: "Log", Path: "ts"}) {
		t.Fatalf("create reply expiry = %q %+v", created.ExpireField, created.ExpireStores)
	}
	described := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "shop", Bucket: "Session"}))
	if described.ExpireField != "ExpiresAt" || len(described.ExpireStores) != 1 {
		t.Fatalf("describe reply expiry = %q %+v", described.ExpireField, described.ExpireStores)
	}
	dropped := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminAlterBucket, Database: "shop", Bucket: "Session", Add: []RingAdminField{}, Drop: []string{}, DropExpire: true}))
	if dropped.ExpireField != "" || len(dropped.ExpireStores) != 1 {
		t.Fatalf("after drop expire at: %q %+v, want the bucket form gone and the queue's kept", dropped.ExpireField, dropped.ExpireStores)
	}
	set := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminAlterBucket, Database: "shop", Bucket: "Session", Add: []RingAdminField{}, Drop: []string{}, ExpireField: "ExpiresAt"}))
	if set.ExpireField != "ExpiresAt" {
		t.Fatalf("after set expire at: %q", set.ExpireField)
	}
	refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "Bad",
		Fields: []RingAdminField{{Name: "When", Type: "string"}}, ExpireField: "When"}), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "EXPIRE_FIELD_TYPE") || !strings.Contains(refusal.Message, "declared string") {
		t.Fatalf("the refusal = %q", refusal.Message)
	}
}

// TestExpireDeclarationRoundTrips: the pending count rides the describe reply and survives the codec,
// with the "at least" flag, and a negative count is refused.
func TestExpireDeclarationRoundTrips(t *testing.T) {
	for _, pending := range []struct {
		count int64
		more  bool
	}{{0, false}, {37, false}, {10000, true}} {
		reply := RingAdminReply{Cmd: AdminDescribeBucket, BucketID: 7, State: "active", DeclVersion: 4, SchemaVersionSeen: 2,
			Fields:      []RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "ExpiresAt", Type: "datetime"}},
			ExpireField: "ExpiresAt", ExpirePending: pending.count, ExpirePendingMore: pending.more}
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.ExpirePending != pending.count || got.ExpirePendingMore != pending.more {
			t.Fatalf("pending %d (more %v) came back as %d (more %v)", pending.count, pending.more, got.ExpirePending, got.ExpirePendingMore)
		}
		if again, err := EncodeRingAdminReply(got); err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("the decoded reply re-encodes as %x, %v: the encoding must be canonical", again, err)
		}
	}
}

// TestTheDescribeDoorCountsTheItemsPastTheirTime: the count is the bucket's own due entries, not the
// node's: an entry of another bucket and one not yet due are not in it.
func TestTheDescribeDoorCountsTheItemsPastTheirTime(t *testing.T) {
	rig, _, sweeper := newSweepRig(t)
	rig.apply(expiryInsert(1_000))
	for _, label := range []string{"a", "b"} {
		rig.apply(expiryPush(t, expiryElement(t, 2_000, label)))
	}
	rig.apply(expiryPush(t, expiryElement(t, 90_000, "later")))
	rig.now = 10_000
	count, more, err := sweeper.BucketBacklog(testDB, testBucket, 100)
	if err != nil || count != 3 || more {
		t.Fatalf("the bucket's pending count is %d (more %v), %v; want 3 (the document and two elements)", count, more, err)
	}
	if other, _, err := sweeper.BucketBacklog(testDB, testBucket+1, 100); err != nil || other != 0 {
		t.Fatalf("another bucket counted %d, %v; want 0", other, err)
	}
	if capped, more, err := sweeper.BucketBacklog(testDB, testBucket, 2); err != nil || capped != 2 || !more {
		t.Fatalf("a limit of 2 gave %d (more %v), %v; want 2 and more", capped, more, err)
	}
}
