package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

func seedMessage(t *testing.T, payload []byte, err error) transport.Message {
	t.Helper()
	if err != nil {
		t.Fatalf("encoding a seed message: %v", err)
	}
	env, err := transport.DecodeEnvelope(payload)
	if err != nil {
		t.Fatalf("reading back a seed message: %v", err)
	}
	return transport.Message{Env: env, Body: payload[transport.EnvelopeSize:]}
}

func seedPayload(msg *transport.Message) []byte {
	return append(transport.EncodeEnvelope(msg.Env), msg.Body...)
}

func seedRegisterMessage(t *testing.T, epoch uint64, slot uint16, db uint64, id [16]byte) transport.Message {
	t.Helper()
	payload, err := transport.EncodeSeedRegister(transport.SeedRegister{Header: transport.Envelope{Epoch: epoch, Slot: slot, DBID: db}, SeedID: id})
	return seedMessage(t, payload, err)
}

// seedPull is the test receiver of T2: it registers, follows one redirect, acks for each
// chunk and keeps what it was sent in a scratch map. The real pull is T3.
type seedPull struct {
	t        *testing.T
	c        *harness.Cluster
	self     string
	slot     uint16
	db       uint64
	id       [16]byte
	scratch  map[string][]byte
	chunks   []transport.SeedChunk
	accepted transport.SeedAccept
	redirect string
}

func (p *seedPull) call(source string, msg transport.Message) *transport.Message {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := p.c.Node(p.self).SeedCall(ctx, source, msg)
	if err != nil {
		p.t.Fatalf("%s calling the seed channel of %s: %v", p.self, source, err)
	}
	return reply
}

// run pulls the pair from source and returns the node that served it.
func (p *seedPull) run(source string) string {
	p.t.Helper()
	epoch := uint64(p.c.Node(p.self).State().Epoch)
	for hop := 0; ; hop++ {
		reply := p.call(source, seedRegisterMessage(p.t, epoch, p.slot, p.db, p.id))
		if reply == nil {
			p.t.Fatalf("%s answered SEED_REGISTER with nothing", source)
		}
		switch reply.Env.Kind {
		case transport.KindSeedRedirect:
			if hop > 0 {
				p.t.Fatalf("a second redirect, to %q", source)
			}
			msg, err := transport.DecodeSeedRedirect(seedPayload(reply))
			if err != nil {
				p.t.Fatalf("decoding SEED_REDIRECT: %v", err)
			}
			p.redirect, source = msg.Source, msg.Source
			continue
		case transport.KindSeedAccept:
			accepted, err := transport.DecodeSeedAccept(seedPayload(reply))
			if err != nil {
				p.t.Fatalf("decoding SEED_ACCEPT: %v", err)
			}
			p.accepted = accepted
		default:
			p.t.Fatalf("%s answered SEED_REGISTER with kind 0x%02x", source, reply.Env.Kind)
		}
		break
	}
	sequence := transport.NewChunkSequence()
	for len(p.chunks) < 1000 {
		ack := seedMessage(p.t, mustAck(p.t, transport.SeedAck{
			Header: transport.Envelope{Epoch: epoch, Slot: p.slot, DBID: p.db}, SeedID: p.id, Applied: p.accepted.Cut, CreditBytes: p.accepted.WindowBytes,
		}), nil)
		reply := p.call(source, ack)
		if reply == nil {
			p.t.Fatalf("%s stopped before the final chunk", source)
		}
		if reply.Env.Kind != transport.KindSeedChunk {
			p.t.Fatalf("%s answered SEED_ACK with kind 0x%02x", source, reply.Env.Kind)
		}
		chunk, err := transport.DecodeSeedChunk(seedPayload(reply))
		if err != nil {
			p.t.Fatalf("decoding SEED_CHUNK: %v", err)
		}
		if err := sequence.Accept(chunk); err != nil {
			p.t.Fatalf("chunk order: %v", err)
		}
		for _, entry := range chunk.Entries {
			p.scratch[string(entry.Key)] = append([]byte(nil), entry.Value...)
		}
		p.chunks = append(p.chunks, chunk)
		if chunk.Final {
			if end := p.call(source, ack); end != nil {
				p.t.Fatalf("the ack of the final chunk got kind 0x%02x, want nothing", end.Env.Kind)
			}
			return source
		}
	}
	p.t.Fatal("1000 chunks and no final flag")
	return source
}

func mustAck(t *testing.T, ack transport.SeedAck) []byte { return transport.EncodeSeedAck(ack) }

// TestASeedStreamsFromTheOwnerAndASecondSourceIsRedirected: a node that holds nothing of a
// pair asks the pair's other copy for it, is redirected once to the owner, and pulls the
// owner's snapshot as a chunk stream in order, within the window, every chunk checked by
// its CRC and its index; what it keeps equals the owner's range byte for byte, and both
// sources hold no pin afterwards.
func TestASeedStreamsFromTheOwnerAndASecondSourceIsRedirected(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, Seed: wire.SeedOptions{ChunkEntries: 3, WindowBytes: 64 << 10}})
	id, owner, copyNode, outsider := proofID(t, c, "seed")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{id}
	for n := 0; len(ids) < 6 && n < 400000; n++ {
		other := fmt.Sprintf("seed-same-slot-%d", n)
		if got, err := wire.PlacementOf(proofDB, proofBucket, other); err == nil && got == slot {
			ids = append(ids, other)
		}
	}
	if len(ids) < 6 {
		t.Fatalf("found %d ids for slot %d", len(ids), slot)
	}
	for _, doc := range ids {
		if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: doc}, {Key: "name", Value: "Ada " + doc}}); err != nil {
			t.Fatalf("insert %s through %s: %v (background: %v)", doc, owner, err, c.BackgroundErrors())
		}
	}
	dbID := uint64(c.Node(owner).State().Databases[0].ID)
	wantPos, want, err := c.Node(owner).PairDump(slot, dbID)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) < 12 {
		t.Fatalf("the pair holds %d keys, want enough for several chunks", len(want))
	}

	pull := &seedPull{t: t, c: c, self: outsider, slot: slot, db: dbID, id: [16]byte{0x5e, 0xed, 1}, scratch: map[string][]byte{}}
	served := pull.run(copyNode)
	if pull.redirect != owner || served != owner {
		t.Fatalf("asked %s: redirected to %q and served by %q, want a redirect to the owner %s", copyNode, pull.redirect, served, owner)
	}
	if pull.accepted.Cut.Epoch != wantPos.Epoch || pull.accepted.Cut.Seq != wantPos.Seq || pull.accepted.Resume {
		t.Errorf("accepted cut %v resume %v, want the owner's position %s and resume=0", pull.accepted.Cut, pull.accepted.Resume, wantPos)
	}
	if len(pull.chunks) < 4 {
		t.Errorf("%d keys in chunks of 3 made %d chunks", len(want), len(pull.chunks))
	}
	for index, chunk := range pull.chunks {
		if chunk.Index != uint64(index) || chunk.Final != (index == len(pull.chunks)-1) || len(chunk.Entries) > 3 {
			t.Errorf("chunk %d: index %d final %v entries %d", index, chunk.Index, chunk.Final, len(chunk.Entries))
		}
	}
	if len(pull.scratch) != len(want) {
		t.Fatalf("the receiver kept %d keys, the owner holds %d", len(pull.scratch), len(want))
	}
	var got, expect []string
	for _, entry := range want {
		expect = append(expect, fmt.Sprintf("%x=%x", entry.Key, entry.Value))
		kept, found := pull.scratch[string(entry.Key)]
		if !found || !bytes.Equal(kept, entry.Value) {
			t.Errorf("key %x: kept %x (found %v), the owner holds %x", entry.Key, kept, found, entry.Value)
		}
	}
	for key, value := range pull.scratch {
		got = append(got, fmt.Sprintf("%x=%x", key, value))
	}
	sort.Strings(got)
	sort.Strings(expect)
	if fmt.Sprint(got) != fmt.Sprint(expect) {
		t.Error("the receiver's scratch store differs from the owner's snapshot")
	}
	for _, node := range []string{owner, copyNode} {
		if pins := c.Node(node).SeedPins(); pins != 0 {
			t.Errorf("%s still pins %d seeds after the final ack", node, pins)
		}
	}
}

// TestAnUnpinnedPeerOrANonMemberReachesNoSeedHandler: the seed channel has the admission of
// the data and control channels. A pinned key under a made-up name or under a member's name,
// with the true cluster id and with the zero one, is dropped before any handler runs; a
// genuine member with the cluster id is served; the same member with the zero cluster id is
// dropped.
func TestAnUnpinnedPeerOrANonMemberReachesNoSeedHandler(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	epoch := uint64(c.Node("n2").State().Epoch)
	register := seedRegisterMessage(t, epoch, 1, 1, [16]byte{1})
	for _, tc := range []struct {
		name, claim string
		zero        bool
	}{
		{"non-member", "ghost", false},
		{"non-member zero cluster id", "ghost", true},
		{"impersonating a member", "n1", false},
		{"impersonating a member zero cluster id", "n1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rogue := c.NewRogue(tc.claim, tc.zero)
			_, err := rogue.Call("n2", transport.ChannelSeed, register)
			var refusal *transport.WireError
			if err == nil || errors.As(err, &refusal) {
				t.Errorf("SEED_REGISTER as %q reached the handler (err %v); the connection must be dropped first", tc.claim, err)
			}
		})
	}
	if _, err := c.CallAs("n1", "n2", transport.ChannelSeed, register); err != nil {
		t.Errorf("a genuine member's SEED_REGISTER was not served: %v", err)
	}
	_, err := c.CallAsWithoutClusterID("n1", "n2", transport.ChannelSeed, register)
	var refusal *transport.WireError
	if err == nil || errors.As(err, &refusal) {
		t.Errorf("a member without the cluster id reached the seed handler: %v", err)
	}
	// A receiver-bound kind sent as a request is refused by the handler, not served.
	accept := seedMessage(t, transport.EncodeSeedAccept(transport.SeedAccept{Header: transport.Envelope{Epoch: epoch}}), nil)
	if _, err := c.CallAs("n1", "n2", transport.ChannelSeed, accept); err == nil {
		t.Error("SEED_ACCEPT was served as a request")
	}
}

// TestDialSeedBacksOffADeadSourceAndBoundsTheRegister: a source that stops answering costs
// one bounded REGISTER (2 s), and a source that is gone costs one dial and then a backoff,
// not a dial per call. A timeout is the caller's bound, not news about the peer, so the
// network (peer.Network.Call) does not back off after one.
func TestDialSeedBacksOffADeadSourceAndBoundsTheRegister(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true, RaftTimeout: time.Second})
	epoch := uint64(c.Node("n1").State().Epoch)
	register := seedRegisterMessage(t, epoch, 1, 1, [16]byte{2})
	c.Stall("n3", true)
	began := time.Now()
	if _, err := c.Node("n1").SeedCall(context.Background(), "n3", register); err == nil {
		t.Fatal("a stalled source answered")
	}
	if took := time.Since(began); took > 4*time.Second {
		t.Errorf("the REGISTER exchange took %s; it is bounded at 2 s", took)
	}
	c.Stall("n3", false)

	c.Kill("n2")
	if _, err := c.Node("n1").SeedCall(context.Background(), "n2", register); err == nil {
		t.Fatal("a killed source answered")
	}
	began = time.Now()
	_, err := c.Node("n1").SeedCall(context.Background(), "n2", register)
	if !errors.Is(err, peer.ErrBackoff) {
		t.Errorf("the second call to a dead source: %v, want a backoff", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Errorf("the call in backoff took %s, want it to fail at once", took)
	}
}

// TestAKilledSourceStopsCleanlyWithAPin: a node stopped mid-seed, with a pin open, closes
// without hanging or failing (the harness fails a kill whose node does not close). That
// Close releases the pin is proved by the unit test of SeedSource.Close; Badger itself does
// not refuse to close with a snapshot open.
func TestAKilledSourceStopsCleanlyWithAPin(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, _, outsider := proofID(t, c, "kill")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	dbID := uint64(c.Node(owner).State().Databases[0].ID)
	epoch := uint64(c.Node(owner).State().Epoch)
	// A source serves a pair it holds a ready record for; an idle pair's record is the interim
	// the owner's first pass over the new database writes (seeding S0), so wait for it.
	c.Await("the owner's record of the pair", 10*time.Second, func() bool {
		return c.Node(owner).SlotRecordState(slot, dbID) == machine.SlotReady
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := c.Node(outsider).SeedCall(ctx, owner, seedRegisterMessage(t, epoch, slot, dbID, [16]byte{9}))
	if err != nil || reply == nil || reply.Env.Kind != transport.KindSeedAccept {
		t.Fatalf("registering at the owner: %v (reply %v)", err, reply)
	}
	if pins := c.Node(owner).SeedPins(); pins != 1 {
		t.Fatalf("the owner holds %d pins, want 1", pins)
	}
	c.Kill(owner)
}

// TestASlowSeedAckIsNotCutOffAtTwoSeconds: only the data channel's replication calls are
// bounded at 2 s. A source that takes 3 s to answer one SEED_ACK (a large entry on a slow
// link, a stalled disk) must still be heard; the stream's bound is seed_idle_seconds.
func TestASlowSeedAckIsNotCutOffAtTwoSeconds(t *testing.T) {
	c := proofSetup(t, harness.Options{NoDetection: true})
	id, owner, _, outsider := proofID(t, c, "slow")
	slot, err := wire.PlacementOf(proofDB, proofBucket, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatal(err)
	}
	dbID := uint64(c.Node(owner).State().Databases[0].ID)
	epoch := uint64(c.Node(owner).State().Epoch)
	seedID := [16]byte{0x51}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reply, err := c.Node(outsider).SeedCall(ctx, owner, seedRegisterMessage(t, epoch, slot, dbID, seedID))
	if err != nil || reply == nil || reply.Env.Kind != transport.KindSeedAccept {
		t.Fatalf("registering: %v (reply %v)", err, reply)
	}
	accepted, err := transport.DecodeSeedAccept(seedPayload(reply))
	if err != nil {
		t.Fatal(err)
	}
	c.DelayChannel(outsider, owner, transport.ChannelSeed, 3*time.Second)
	ack := seedMessage(t, transport.EncodeSeedAck(transport.SeedAck{
		Header: transport.Envelope{Epoch: epoch, Slot: slot, DBID: dbID}, SeedID: seedID, Applied: accepted.Cut, CreditBytes: accepted.WindowBytes,
	}), nil)
	began := time.Now()
	reply, err = c.Node(outsider).SeedCall(ctx, owner, ack)
	if err != nil {
		t.Fatalf("a SEED_ACK answered after %s failed: %v", time.Since(began).Round(time.Millisecond), err)
	}
	if took := time.Since(began); took < 2500*time.Millisecond {
		t.Fatalf("the reply took %s, the 3 s delay did not apply and the test proves nothing", took)
	}
	if reply == nil || reply.Env.Kind != transport.KindSeedChunk {
		t.Fatalf("the slow ACK got %v, want a chunk", reply)
	}
}
