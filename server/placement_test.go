package server

import (
	"encoding/hex"
	"testing"

	"github.com/hoarddb/hoarddb-server/ring"
)

// TestPlacementKeyIsTheFrozenEncoding pins the bytes the ring hashes for a
// bucket to the frozen (database, bucket) encoding (entity-placement §A.3,
// Q4; storage/testdata/slot-vectors.json). The hex strings are copied from
// that vector file, not produced by the code under test: a changed encoding
// moves every bucket of every cluster, so it must fail here. The reserved auth
// key stays its bare name, so the auth master did not move when placement became per-database.
func TestPlacementKeyIsTheFrozenEncoding(t *testing.T) {
	for _, vector := range []struct {
		database, bucket, keyHex string
	}{
		{database: "app", bucket: "users", keyHex: "03617070057573657273"},
		{database: "ab", bucket: "c", keyHex: "0261620163"},
		{database: "a", bucket: "bc", keyHex: "0161026263"},
		{database: "app", bucket: "sessions", keyHex: "036170700873657373696f6e73"},
	} {
		got := hex.EncodeToString([]byte(placementOf(vector.database, vector.bucket).encoded))
		if got != vector.keyHex {
			t.Errorf("placementOf(%q, %q) = %s, want %s", vector.database, vector.bucket, got, vector.keyHex)
		}
	}
	if got := reservedPlacement(authReservedKey).encoded; got != "_system.auth" {
		t.Errorf("the auth master's placement key = %q, want the bare %q", got, "_system.auth")
	}
}

// TestClusterPlacesByTheEncodedKey checks the chokepoint hands the ring the
// encoded key and nothing else: Cluster.Owner and Replicas of a placement key
// agree with ring.Ring over its bytes, for a bucket whose bare name has a
// different owner — so a chokepoint that hashed the name would disagree.
func TestClusterPlacesByTheEncodedKey(t *testing.T) {
	s := newTestServer(t)
	cluster := attachTwoNodeRing(t, s, fabricatedMemberAddr(t), fabricatedMemberAddr(t))
	reference := ring.New(1)
	for _, member := range cluster.Members() {
		if err := reference.AddNode(member); err != nil {
			t.Fatalf("building the reference ring: %v", err)
		}
	}
	for i := 0; i < 256; i++ {
		bucket := "b" + hex.EncodeToString([]byte{byte(i)})
		key := placementOf("app", bucket)
		byName, err := reference.Owner(bucket)
		if err != nil {
			t.Fatalf("reference owner of %q: %v", bucket, err)
		}
		byKey, err := reference.Owner(key.encoded)
		if err != nil {
			t.Fatalf("reference owner of app/%s: %v", bucket, err)
		}
		if byName.ID == byKey.ID {
			continue
		}
		owner, err := cluster.Owner(key)
		if err != nil {
			t.Fatalf("Cluster.Owner(app/%s): %v", bucket, err)
		}
		replicas, err := cluster.Replicas(key)
		if err != nil {
			t.Fatalf("Cluster.Replicas(app/%s): %v", bucket, err)
		}
		if owner.ID != byKey.ID || replicas[0].ID != byKey.ID {
			t.Fatalf("app/%s: Cluster.Owner = %s, Replicas[0] = %s, want the placement key's owner %s (its bare name's is %s)",
				bucket, owner.ID, replicas[0].ID, byKey.ID, byName.ID)
		}
		return
	}
	t.Fatal("precondition: no bucket b00..bff in app whose bare name and placement key have different owners")
}
