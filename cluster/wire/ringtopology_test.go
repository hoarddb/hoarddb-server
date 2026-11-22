package wire

import (
	"bytes"
	"errors"
	"math/rand"
	"runtime"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u50State is a cluster of n active data members node-a, node-b, ...
func u50State(n int) cluster.ClusterState {
	state := cluster.ClusterState{Epoch: 7, StateSeq: 42}
	for i := range n {
		id := "node-" + string(rune('a'+i))
		state.Members = append(state.Members, cluster.Member{ID: id, DataAddr: "127.0.0.1:" + string(rune('1'+i)) + "433", State: cluster.MemberActive, Suffrage: "voter"})
	}
	return state
}

func u50Reply(n int) RingTopologyReply {
	return RingTopologyReply{Epoch: 7, StateSeq: 42, Table: cluster.BuildTable(u50State(n)),
		Databases: []RingTopologyDatabase{{Name: "app", ID: 1, RF: 2}, {Name: "vault", ID: 2, RF: 1}}}
}

func u50EncodeReply(t *testing.T, reply RingTopologyReply) []byte {
	t.Helper()
	raw, err := EncodeRingTopologyReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func u50Refused(t *testing.T, what string, err error) {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
		t.Errorf("%s: got %v, want an INVALID_ARGUMENT refusal", what, err)
	}
}

// TestU50TheRequestIsStrict: [GUARD the request is accepted only if it re-encodes to the same bytes].
func TestU50TheRequestIsStrict(t *testing.T) {
	raw, err := EncodeRingTopology(RingTopologyRequest{Epoch: 7, StateSeq: 42})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingTopology(raw)
	if err != nil || got != (RingTopologyRequest{Epoch: 7, StateSeq: 42}) {
		t.Fatalf("round trip gave %+v, %v", got, err)
	}
	zero, err := EncodeRingTopology(RingTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingTopology(zero); err != nil {
		t.Errorf("the first request, epoch 0 and state_seq 0, was refused: %v", err)
	}
	marshal := func(doc bson.D) []byte {
		out, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	cases := map[string][]byte{
		"reordered":     marshal(bson.D{{Key: "state_seq", Value: int64(42)}, {Key: "epoch", Value: int64(7)}}),
		"extra member":  marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "limit", Value: int32(1)}}),
		"widened":       marshal(bson.D{{Key: "epoch", Value: int32(7)}, {Key: "state_seq", Value: int64(42)}}),
		"missing":       marshal(bson.D{{Key: "epoch", Value: int64(7)}}),
		"negative":      marshal(bson.D{{Key: "epoch", Value: int64(-1)}, {Key: "state_seq", Value: int64(42)}}),
		"duplicated":    marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}}),
		"truncated":     raw[:len(raw)-3],
		"trailing byte": append(append([]byte(nil), raw...), 0),
		"empty":         nil,
	}
	for name, body := range cases {
		_, err := DecodeRingTopology(body)
		u50Refused(t, name, err)
	}
	if _, err := EncodeRingTopology(RingTopologyRequest{Epoch: -1}); err == nil {
		t.Error("a negative epoch was encoded")
	}
}

// TestU50TheReplyRoundTripsTheRankingAndTheOwner: [GUARD the table is the full ranking, not
// truncated by RF, so a down name can be skipped by the driver].
func TestU50TheReplyRoundTripsTheRankingAndTheOwner(t *testing.T) {
	state := u50State(3)
	state.Members[1].State = cluster.MemberDown
	reply := u50Reply(3)
	reply.Table = cluster.BuildTable(state)
	raw := u50EncodeReply(t, reply)
	got, err := DecodeRingTopologyReply(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 7 || got.StateSeq != 42 || got.Unchanged || len(got.Databases) != 2 || got.Databases[0] != reply.Databases[0] {
		t.Fatalf("decoded %+v", got)
	}
	for slot := range uint16(8192) {
		want, wantOK := reply.Table.Owner(slot, 2)
		have, haveOK := got.Table.Owner(slot, 2)
		if want != have || wantOK != haveOK {
			t.Fatalf("slot %d: owner %v/%v, decoded %v/%v", slot, want, wantOK, have, haveOK)
		}
		if ranking := got.Table.Ranking(slot); len(ranking) != 3 {
			t.Fatalf("slot %d: the ranking has %d names, the wire table is never truncated by RF", slot, len(ranking))
		}
	}
	if again := u50EncodeReply(t, got); !bytes.Equal(again, raw) {
		t.Fatal("a decoded reply does not re-encode to the same bytes")
	}
	if !bytes.Equal(got.Table.Encode(), reply.Table.Encode()) {
		t.Fatal("the decoded table differs")
	}
}

// TestU50AnUnchangedReplyCarriesNoBlobs: [GUARD unchanged omits members, table and databases,
// and a reply that says unchanged with blobs, or changed without, is refused].
func TestU50AnUnchangedReplyCarriesNoBlobs(t *testing.T) {
	raw := u50EncodeReply(t, RingTopologyReply{Epoch: 7, StateSeq: 42, Unchanged: true})
	got, err := DecodeRingTopologyReply(raw)
	if err != nil || !got.Unchanged || got.Table != nil || got.Databases != nil {
		t.Fatalf("unchanged reply decoded to %+v, %v", got, err)
	}
	if len(raw) > 64 {
		t.Errorf("an unchanged reply is %d bytes", len(raw))
	}
	if _, err := EncodeRingTopologyReply(RingTopologyReply{Epoch: 7, StateSeq: 42, Unchanged: true, Table: cluster.BuildTable(u50State(1))}); err == nil {
		t.Error("an unchanged reply with a table was encoded")
	}
	if _, err := EncodeRingTopologyReply(RingTopologyReply{Epoch: 7, StateSeq: 42}); err == nil {
		t.Error("a changed reply with no table was encoded")
	}
	full := u50Reply(1)
	doc := bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "unchanged", Value: true},
		{Key: "members", Value: u50Members(full.Table.Members())}, {Key: "table", Value: full.Table.Encode()}}
	forged, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingTopologyReply(forged)
	u50Refused(t, "unchanged with blobs", err)
}

func u50Members(members []cluster.Member) []byte {
	return AppendTopologyMembers(nil, members)
}

// TestU50TheReplyIsStrict: [GUARD every layout deviation is refused: reordered or extra member,
// widened integer, non-canonical varint, truncated or padded blob].
func TestU50TheReplyIsStrict(t *testing.T) {
	reply := u50Reply(3)
	members := reply.Table.Members()
	table := reply.Table.Encode()
	build := func(epoch, seq, rf any, memberBlob, tableBlob []byte) []byte {
		raw, err := bson.Marshal(bson.D{{Key: "epoch", Value: epoch}, {Key: "state_seq", Value: seq}, {Key: "members", Value: memberBlob},
			{Key: "table", Value: tableBlob}, {Key: "databases", Value: bson.A{bson.D{{Key: "name", Value: "app"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: rf}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	good := build(int64(7), int64(42), int32(2), u50Members(members), table)
	if _, err := DecodeRingTopologyReply(good); err != nil {
		t.Fatalf("the control reply was refused: %v", err)
	}
	reversed := []cluster.Member{members[2], members[1], members[0]}
	extra := append(append([]cluster.Member(nil), members...), cluster.Member{ID: "node-z", DataAddr: "x:1", State: cluster.MemberActive})
	witness := append(append([]cluster.Member(nil), members...), cluster.Member{ID: "node-z", DataAddr: "x:1", State: cluster.MemberJoining})
	duplicate := []cluster.Member{members[0], members[0], members[1]}
	member := u50Members(members)
	longForm := append([]byte{3, byte(cluster.MemberActive), 0, 0x86, 0x00}, member[4:]...) // idlen written as a two-byte varint
	cases := map[string][]byte{
		"members reordered":          build(int64(7), int64(42), int32(2), u50Members(reversed), table),
		"an extra member":            build(int64(7), int64(42), int32(2), u50Members(extra), table),
		"a joining member":           build(int64(7), int64(42), int32(2), u50Members(witness), table),
		"a duplicated member":        build(int64(7), int64(42), int32(2), u50Members(duplicate), table),
		"epoch widened to int32":     build(int32(7), int64(42), int32(2), member, table),
		"rf widened to int64":        build(int64(7), int64(42), int64(2), member, table),
		"a non-canonical varint":     build(int64(7), int64(42), int32(2), longForm, table),
		"a truncated table":          build(int64(7), int64(42), int32(2), member, table[:len(table)-1]),
		"a padded table":             build(int64(7), int64(42), int32(2), member, append(append([]byte(nil), table...), 0)),
		"a truncated members blob":   build(int64(7), int64(42), int32(2), member[:len(member)-1], table),
		"an unknown state byte":      build(int64(7), int64(42), int32(2), append([]byte{3, 9}, member[2:]...), table),
		"a zero replication factor":  build(int64(7), int64(42), int32(0), member, table),
		"a truncated BSON document":  good[:len(good)-5],
		"a negative state_seq":       build(int64(7), int64(-1), int32(2), member, table),
		"a table of fewer rows":      build(int64(7), int64(42), int32(2), member, table[:len(table)/2]),
		"members not as the table's": build(int64(7), int64(42), int32(2), u50Members(members[:2]), table),
	}
	for name, body := range cases {
		_, err := DecodeRingTopologyReply(body)
		u50Refused(t, name, err)
	}
}

// TestU50TheReplyReportsItsMeasuredSize logs the size of the reply for 1 and 3 members and
// bounds it to the figures of conventions section 4: 2 bytes a slot per member at most.
func TestU50TheReplyReportsItsMeasuredSize(t *testing.T) {
	for _, n := range []int{1, 3} {
		reply := u50Reply(n)
		reply.Databases = nil
		raw := u50EncodeReply(t, reply)
		t.Logf("%d member(s): table %d bytes, whole reply %d bytes", n, len(reply.Table.Encode()), len(raw))
		if limit := 8192*(1+n) + 512; len(raw) > limit {
			t.Errorf("%d members: the reply is %d bytes, want at most %d", n, len(raw), limit)
		}
	}
}

func u50Mutate(random *rand.Rand, seed []byte) []byte {
	mutated := append([]byte(nil), seed...)
	for range 1 + random.Intn(3) {
		switch random.Intn(4) {
		case 0:
			mutated[random.Intn(len(mutated))] ^= byte(1 << random.Intn(8))
		case 1:
			mutated[random.Intn(len(mutated))] = byte(random.Intn(256))
		case 2:
			mutated = mutated[:random.Intn(len(mutated)+1)]
		default:
			at := random.Intn(len(mutated) + 1)
			mutated = append(mutated[:at], append([]byte{byte(random.Intn(256))}, mutated[at:]...)...)
		}
		if len(mutated) == 0 {
			mutated = append(mutated, 0)
		}
	}
	return mutated
}

// TestU50ADecoderNeverPanicsAndAcceptsOnlyWhatItWrites: [GUARD 100000 mutations of a request and of a
// reply: no panic, and anything accepted re-encodes to the same bytes].
func TestU50ADecoderNeverPanicsAndAcceptsOnlyWhatItWrites(t *testing.T) {
	request, err := EncodeRingTopology(RingTopologyRequest{Epoch: 7, StateSeq: 42})
	if err != nil {
		t.Fatal(err)
	}
	reply := u50EncodeReply(t, u50Reply(1))
	random := rand.New(rand.NewSource(50))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var inputBytes int
	for range 100000 {
		body := u50Mutate(random, request)
		inputBytes += len(body)
		if got, err := DecodeRingTopology(body); err == nil {
			again, encodeErr := EncodeRingTopology(got)
			if encodeErr != nil || !bytes.Equal(again, body) {
				t.Fatalf("accepted %x but re-encoded to %x (%v)", body, again, encodeErr)
			}
		}
	}
	for range 100000 {
		body := u50Mutate(random, reply)
		inputBytes += len(body)
		if got, err := DecodeRingTopologyReply(body); err == nil {
			again, encodeErr := EncodeRingTopologyReply(got)
			if encodeErr != nil || !bytes.Equal(again, body) {
				t.Fatalf("accepted a reply that re-encodes differently (%v)", encodeErr)
			}
		}
	}
	runtime.ReadMemStats(&after)
	if spent := after.TotalAlloc - before.TotalAlloc; spent > uint64(inputBytes)*16+(64<<20) {
		t.Errorf("200000 decodes of %d input bytes allocated %d bytes", inputBytes, spent)
	}
}

// TestU50ALyingMemberCountAllocatesNothingLarge: [GUARD the member count and the table length are
// checked against the bytes present before anything is allocated for them].
func TestU50ALyingMemberCountAllocatesNothingLarge(t *testing.T) {
	lying := append([]byte{0xff, 0xff, 0x03}, 0, 0) // 65535 members claimed, two bytes present
	raw, err := bson.Marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)},
		{Key: "members", Value: lying}, {Key: "table", Value: make([]byte, 8192)}})
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = DecodeRingTopologyReply(raw)
	runtime.ReadMemStats(&after)
	u50Refused(t, "a lying member count", err)
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 1<<20 {
		t.Errorf("a %d-byte reply made the decoder allocate %d bytes", len(raw), spent)
	}
}
