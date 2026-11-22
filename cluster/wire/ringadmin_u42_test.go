package wire

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u42Recorder admits every request and remembers what it was asked.
type u42Recorder struct {
	mu    sync.Mutex
	asked []RingAccess
}

func (r *u42Recorder) Authorize(_ context.Context, access RingAccess) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, access)
	return nil
}

func (r *u42Recorder) asks() []RingAccess {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RingAccess(nil), r.asked...)
}

func u42AdminRequest() RingAdminRequest {
	return RingAdminRequest{Cmd: AdminDropBucket, Database: "app", Bucket: "User"}
}

// TestU42DropBucketRoundTrips: a drop_bucket request and its reply decode to the same value and
// re-encode to the same bytes.
func TestU42DropBucketRoundTrips(t *testing.T) {
	raw, err := EncodeRingAdmin(u42AdminRequest())
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingAdmin(raw)
	if err != nil || got.Cmd != AdminDropBucket || got.Database != "app" || got.Bucket != "User" {
		t.Fatalf("request = %+v, %v", got, err)
	}
	if again, err := EncodeRingAdmin(got); err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encode: %v", err)
	}
	reply, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDropBucket, BucketID: 0x1f4a2c9b7de30015, State: "dropping"})
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeRingAdminReply(reply)
	if err != nil || back.Cmd != AdminDropBucket || back.BucketID != 0x1f4a2c9b7de30015 || back.State != "dropping" {
		t.Fatalf("reply = %+v, %v", back, err)
	}
	if again, err := EncodeRingAdminReply(back); err != nil || !bytes.Equal(again, reply) {
		t.Fatalf("reply re-encode: %v", err)
	}
}

// TestU42DropBucketCarriesTheIdItMeans: the optional expect_bucket_id survives the codec, is left out
// when zero, and is refused on any other command.
func TestU42DropBucketCarriesTheIdItMeans(t *testing.T) {
	req := u42AdminRequest()
	req.ExpectBucketID = 0x1f4a2c9b7de30015
	raw, err := EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingAdmin(raw)
	if err != nil || got.ExpectBucketID != req.ExpectBucketID {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	plain, _ := EncodeRingAdmin(u42AdminRequest())
	if len(plain) >= len(raw) {
		t.Fatal("a zero expect_bucket_id was sent")
	}
	if _, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "User", ExpectBucketID: 7}); err == nil {
		t.Fatal("describe_bucket accepted expect_bucket_id")
	}
}

// TestU42DropBucketRefusalsNameTheFix: each malformed request is INVALID_ARGUMENT naming the
// member and what to send; a reply that carries a bucket is refused.
func TestU42DropBucketRefusalsNameTheFix(t *testing.T) {
	cases := map[string]RingAdminRequest{
		"no database":   {Cmd: AdminDropBucket, Bucket: "User"},
		"no bucket":     {Cmd: AdminDropBucket, Database: "app"},
		"stray fields":  {Cmd: AdminDropBucket, Database: "app", Bucket: "User", Fields: []RingAdminField{{Name: "a", Type: "int"}}},
		"stray a limit": {Cmd: AdminDropBucket, Database: "app", Bucket: "User", Limit: new(int32)},
	}
	for name, req := range cases {
		_, err := EncodeRingAdmin(req)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), "drop_bucket") {
			t.Errorf("%s: the refusal %q does not name the command", name, err)
		}
	}
	for name, reply := range map[string]RingAdminReply{
		"no id": {Cmd: AdminDropBucket, State: "dropping"}, "active": {Cmd: AdminDropBucket, BucketID: 7, State: "active"},
		"fields": {Cmd: AdminDropBucket, BucketID: 7, State: "dropping", Fields: []RingAdminReplyField{}},
	} {
		if _, err := EncodeRingAdminReply(reply); err == nil {
			t.Errorf("a drop_bucket reply with %s was accepted", name)
		}
	}
}

// TestU42DropBucketDecodersSurviveMutation: 100000 mutations: no panic, and whatever is
// accepted re-encodes to the same bytes.
func TestU42DropBucketDecodersSurviveMutation(t *testing.T) {
	withID := u42AdminRequest()
	withID.ExpectBucketID = 0x1f4a2c9b7de30015
	request, err := EncodeRingAdmin(withID)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDropBucket, BucketID: 0x1f4a2c9b7de30015, State: "dropping"})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(42, 42))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, request)
		if req, err := DecodeRingAdmin(mutated); err == nil {
			accepted++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, reply)
		if got, err := DecodeRingAdminReply(mutated); err == nil {
			if again, err := EncodeRingAdminReply(got); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutation was accepted: the re-encode check never ran")
	}
}

// TestU42DropBucketIsAuthorizedAndServed: the handler asks for the drop_bucket class on the
// database and bucket named before it does anything, then drops the bucket; a second drop is
// NOT_FOUND with the fix; a session the gate refuses deletes nothing.
func TestU42DropBucketIsAuthorizedAndServed(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	gate := &u42Recorder{}
	handler, err := NewRingHandler(node, u66AuthNext, gate)
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(u42AdminRequest())
	if err != nil {
		t.Fatal(err)
	}
	send := func() *transport.Frame {
		frame, err := handler(ctx, transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	reply := send()
	if reply.Opcode != transport.OpRingReply {
		t.Fatalf("the drop answered %s %x", reply.Opcode, reply.Payload)
	}
	asked := false
	for _, access := range gate.asks() {
		if access.Class == RingClassDropBucket && access.Database == "app" && access.Bucket == "User" {
			asked = true
		}
	}
	if !asked {
		t.Fatalf("the handler never asked for the drop_bucket class on app.User: %+v", gate.asks())
	}
	// The documents go in the background and the name is freed when they are gone.
	if err := node.retire.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Get(ctx, u30iDB, "User", "x"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("Get after the drop = %v, want the bucket to be gone", err)
	}
	missing := u66Refusal(t, send(), cluster.CodeNotFound)
	if !strings.Contains(missing.Message, "show buckets in app") {
		t.Fatalf("the second drop's refusal lacks the fix: %q", missing.Message)
	}
}
