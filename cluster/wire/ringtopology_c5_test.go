package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// c5State is three data members and two witnesses: node-a votes and has a client address,
// node-b is down and votes, node-c is a non-voter, node-w is a voting witness and node-x a
// down witness that does not vote.
func c5State() cluster.ClusterState {
	return cluster.ClusterState{Epoch: 7, StateSeq: 42, Members: []cluster.Member{
		{ID: "node-a", DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434", State: cluster.MemberActive, Suffrage: "voter"},
		{ID: "node-b", DataAddr: "192.0.2.2:4433", State: cluster.MemberDown, Suffrage: "voter"},
		{ID: "node-c", DataAddr: "192.0.2.3:4433", ClientAddr: "c.example:7434", State: cluster.MemberActive, Suffrage: "nonvoter"},
		{ID: "node-w", DataAddr: "192.0.2.4:4433", State: cluster.MemberActive, Suffrage: "voter", Witness: true},
		{ID: "node-x", DataAddr: "192.0.2.5:4433", ClientAddr: "x.example:7434", State: cluster.MemberDown, Suffrage: "nonvoter", Witness: true},
	}}
}

func c5Witnesses(state cluster.ClusterState) []cluster.Member {
	var out []cluster.Member
	for _, m := range state.Members {
		if m.Witness {
			out = append(out, m)
		}
	}
	return out
}

func c5Reply() RingTopologyReply {
	state := c5State()
	// The witnesses go through TopologyWitnessRows as the server does (BUG-0876): a witness's
	// client address is not published, so node-x below decodes with an empty one.
	return RingTopologyReply{Epoch: 7, StateSeq: 42, Table: cluster.BuildTable(state), Witnesses: TopologyWitnessRows(c5Witnesses(state)),
		Databases: []RingTopologyDatabase{{Name: "app", ID: 1, RF: 2}}}
}

// TestC5TheReplyCarriesClientAddrVotersAndWitnesses: [GUARD every member's client address and voter flag, and the
// witness list, survive the wire, and the reply re-encodes to the same bytes].
func TestC5TheReplyCarriesClientAddrVotersAndWitnesses(t *testing.T) {
	reply := c5Reply()
	raw := u50EncodeReply(t, reply)
	got, err := DecodeRingTopologyReply(raw)
	if err != nil {
		t.Fatal(err)
	}
	members := got.Table.Members()
	want := []struct {
		id, client string
		voter      bool
	}{{"node-a", "a.example:7434", true}, {"node-b", "", true}, {"node-c", "c.example:7434", false}}
	if len(members) != len(want) {
		t.Fatalf("decoded %d ranked members, want %d", len(members), len(want))
	}
	for i, w := range want {
		if members[i].ID != w.id || members[i].ClientAddr != w.client || (members[i].Suffrage == "voter") != w.voter {
			t.Errorf("member %d decoded as %+v, want %+v", i, members[i], w)
		}
	}
	if len(got.Witnesses) != 2 {
		t.Fatalf("decoded %d witnesses, want 2", len(got.Witnesses))
	}
	w, x := got.Witnesses[0], got.Witnesses[1]
	if w.ID != "node-w" || !w.Witness || w.Suffrage != "voter" || w.State != cluster.MemberActive || w.DataAddr != "192.0.2.4:4433" || w.ClientAddr != "" {
		t.Errorf("witness w decoded as %+v", w)
	}
	if x.ID != "node-x" || !x.Witness || x.Suffrage == "voter" || x.State != cluster.MemberDown || x.ClientAddr != "" {
		t.Errorf("witness x decoded as %+v", x)
	}
	if again := u50EncodeReply(t, got); !bytes.Equal(again, raw) {
		t.Fatal("a decoded reply does not re-encode to the same bytes")
	}
}

// TestC5AReplyWithNoWitnessWritesNoWitnessField: [GUARD witnesses is absent rather than written empty, so a
// cluster without a witness pays nothing].
func TestC5AReplyWithNoWitnessWritesNoWitnessField(t *testing.T) {
	reply := c5Reply()
	reply.Witnesses = nil
	raw := u50EncodeReply(t, reply)
	if bytes.Contains(raw, []byte("witnesses")) {
		t.Error("a reply with no witness wrote the field")
	}
	reference := reply
	reference.Witnesses = TopologyWitnessRows(c5Witnesses(c5State()))
	if bytes.Equal(raw, u50EncodeReply(t, reference)) {
		t.Fatal("the reference reply with witnesses is the same as one without")
	}
	members := AppendTopologyMembers(nil, reply.Table.Members())
	if _, err := DecodeRingTopologyReply(c5Forge(t, members, nil)); err != nil {
		t.Fatalf("the control without a witnesses field was refused: %v", err)
	}
	_, err := DecodeRingTopologyReply(c5Forge(t, members, []byte{0}))
	u50Refused(t, "an empty witnesses blob", err)
}

func c5Forge(t *testing.T, members, witnesses []byte) []byte {
	t.Helper()
	reply := c5Reply()
	doc := bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "members", Value: members}}
	if witnesses != nil {
		doc = append(doc, bson.E{Key: "witnesses", Value: witnesses})
	}
	doc = append(doc, bson.E{Key: "table", Value: reply.Table.Encode()})
	doc = append(doc, bson.E{Key: "databases", Value: bson.A{bson.D{{Key: "name", Value: "app"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: int32(2)}}}})
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestC5TheWitnessAndMemberBlobsAreStrict: [GUARD a reserved flag bit, a client address over the bound, a witness
// that is ranked, unsorted, duplicated, joining or listed twice, and a padded or cut blob are all refused].
func TestC5TheWitnessAndMemberBlobsAreStrict(t *testing.T) {
	reply := c5Reply()
	members := AppendTopologyMembers(nil, reply.Table.Members())
	witnesses := AppendTopologyMembers(nil, reply.Witnesses)
	if _, err := DecodeRingTopologyReply(c5Forge(t, members, witnesses)); err != nil {
		t.Fatalf("the control reply was refused: %v", err)
	}
	witnessRows := reply.Witnesses
	reversed := []cluster.Member{witnessRows[1], witnessRows[0]}
	duplicated := []cluster.Member{witnessRows[0], witnessRows[0]}
	ranked := []cluster.Member{reply.Table.Members()[0]}
	ranked[0].Witness = true
	joining := []cluster.Member{witnessRows[0]}
	joining[0].State = cluster.MemberJoining
	notWitness := []cluster.Member{{ID: "node-z", DataAddr: "x:1", State: cluster.MemberActive}}
	longClient := []cluster.Member{{ID: "node-z", DataAddr: "x:1", ClientAddr: strings.Repeat("a", cluster.MaxClientAddrBytes+1), State: cluster.MemberActive, Witness: true}}
	withFlag := func(blob []byte, flags byte) []byte {
		out := append([]byte(nil), blob...)
		out[2] = flags // count, then state, then the flags byte of the first member
		return out
	}
	cases := map[string][]byte{
		"a reserved flag bit in members":   c5Forge(t, withFlag(members, 0x02), witnesses),
		"a reserved flag bit in witnesses": c5Forge(t, members, withFlag(witnesses, 0x80)),
		"witnesses reversed":               c5Forge(t, members, AppendTopologyMembers(nil, reversed)),
		"a witness twice":                  c5Forge(t, members, AppendTopologyMembers(nil, duplicated)),
		"a witness that is also ranked":    c5Forge(t, members, AppendTopologyMembers(nil, ranked)),
		"a joining witness":                c5Forge(t, members, AppendTopologyMembers(nil, joining)),
		"a client address over the bound":  c5Forge(t, members, AppendTopologyMembers(nil, longClient)),
		"a cut witnesses blob":             c5Forge(t, members, witnesses[:len(witnesses)-1]),
		"a padded witnesses blob":          c5Forge(t, members, append(append([]byte(nil), witnesses...), 0)),
		"members without the flags byte":   c5Forge(t, append([]byte{1, byte(cluster.MemberActive), 1, 'a', 1, 'b'}, 0), nil),
	}
	for name, body := range cases {
		_, err := DecodeRingTopologyReply(body)
		u50Refused(t, name, err)
	}
	if _, err := EncodeRingTopologyReply(RingTopologyReply{Epoch: 7, StateSeq: 42, Table: reply.Table, Witnesses: notWitness}); err == nil {
		t.Error("a witness row without the witness flag was encoded")
	}
	if _, err := EncodeRingTopologyReply(RingTopologyReply{Epoch: 7, StateSeq: 42, Unchanged: true, Witnesses: reply.Witnesses}); err == nil {
		t.Error("an unchanged reply with witnesses was encoded")
	}
}

// TestC5ALyingWitnessCountAllocatesNothingLarge: [GUARD the witness count is checked against the bytes present
// before anything is allocated for it].
func TestC5ALyingWitnessCountAllocatesNothingLarge(t *testing.T) {
	reply := c5Reply()
	members := AppendTopologyMembers(nil, reply.Table.Members())
	raw := c5Forge(t, members, append([]byte{0xff, 0xff, 0x03}, 0, 0))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := DecodeRingTopologyReply(raw)
	runtime.ReadMemStats(&after)
	u50Refused(t, "a lying witness count", err)
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 1<<20 {
		t.Errorf("a %d-byte reply made the decoder allocate %d bytes", len(raw), spent)
	}
}

// TestC5ADecoderNeverPanicsOnWitnessesAndClientAddrs: [GUARD 100000 mutations of a reply that carries client
// addresses, voter flags and witnesses: no panic, and anything accepted re-encodes to the same bytes].
func TestC5ADecoderNeverPanicsOnWitnessesAndClientAddrs(t *testing.T) {
	reply := u50EncodeReply(t, c5Reply())
	random := rand.New(rand.NewSource(5))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var inputBytes int
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
		t.Errorf("100000 decodes of %d input bytes allocated %d bytes", inputBytes, spent)
	}
}

// c5Source is a StateSource that always answers one state.
type c5Source struct{ state cluster.ClusterState }

func (s c5Source) State() cluster.ClusterState { return s.state }

// TestC5TheServerAnswersWithItsCommittedWitnessesAndClientAddrs: [GUARD the reply is built from the committed
// state: ranked members with their client address and suffrage, witnesses apart from the table].
func TestC5TheServerAnswersWithItsCommittedWitnessesAndClientAddrs(t *testing.T) {
	topo := NewClusterTopology("node-a", c5Source{state: c5State()})
	server := &ringServer{node: &Node{topology: topo}, authz: u70AllowAll{}}
	reply, err := server.serveTopology(t.Context(), RingTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Witnesses) != 2 || reply.Witnesses[0].ID != "node-w" || reply.Witnesses[1].ID != "node-x" {
		t.Fatalf("witnesses %+v", reply.Witnesses)
	}
	if got := reply.Table.Members(); len(got) != 3 || got[0].ClientAddr != "a.example:7434" || got[0].Suffrage != "voter" {
		t.Fatalf("ranked members %+v", got)
	}
	u50EncodeReply(t, reply)
}

// TestC5AWitnessNotYetActiveIsNotListed: [GUARD a joining or dead witness is left out of the reply, because the wire
// refuses such a row and one would make every TOPOLOGY answer fail].
func TestC5AWitnessNotYetActiveIsNotListed(t *testing.T) {
	state := c5State()
	state.Members = append(state.Members,
		cluster.Member{ID: "node-y", DataAddr: "192.0.2.6:4433", State: cluster.MemberJoining, Witness: true},
		cluster.Member{ID: "node-z", DataAddr: "192.0.2.7:4433", State: cluster.MemberDead, Witness: true})
	server := &ringServer{node: &Node{topology: NewClusterTopology("node-a", c5Source{state: state})}, authz: u70AllowAll{}}
	reply, err := server.serveTopology(context.Background(), RingTopologyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Witnesses) != 2 {
		t.Fatalf("witnesses %+v", reply.Witnesses)
	}
	u50EncodeReply(t, reply)
}

// TestC5ClientAddrForNeverCommitsAnAddressNobodyElseCanReach: [GUARD an empty advertise with a loopback, wildcard,
// hostless or unparsable tcp.listen commits no client address; an advertise is taken as written].
func TestC5ClientAddrForNeverCommitsAnAddressNobodyElseCanReach(t *testing.T) {
	cases := []struct{ advertise, listen, want string }{
		{"db1.example:7434", "0.0.0.0:7434", "db1.example:7434"},
		{"127.0.0.1:7434", "127.0.0.1:7434", "127.0.0.1:7434"},
		{"", "192.0.2.5:7434", "192.0.2.5:7434"},
		{"", "db1.example:7434", "db1.example:7434"},
		{"", "[2001:db8::5]:7434", "[2001:db8::5]:7434"},
		{"", "0.0.0.0:7434", ""},
		{"", "[::]:7434", ""},
		{"", ":7434", ""},
		{"", "127.0.0.1:7434", ""},
		{"", "127.1.2.3:7434", ""},
		{"", "[::1]:7434", ""},
		{"", "localhost:7434", ""},
		{"", "LocalHost:7434", ""},
		{"", "localhost.:7434", ""},
		{"", "[fe80::1%eth0]:7434", ""},
		{"", "[::ffff:127.0.0.1]:7434", ""},
		{"", "[::ffff:0.0.0.0]:7434", ""},
		{"", "192.0.2.5:0", ""},
		{"", "192.0.2.5:99999", ""},
		{"", "192.0.2.5:abc", ""},
		{"", "garbage", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := ClientAddrFor(c.advertise, c.listen); got != c.want {
			t.Errorf("ClientAddrFor(%q, %q) = %q, want %q", c.advertise, c.listen, got, c.want)
		}
	}
}

// TestC16AStaleEpochHintNamesTheAddressDriversConnectTo: [GUARD the owner_addr of a STALE_EPOCH detail is the owner's
// client address when it has one and empty when it has not, because the data address serves no driver].
func TestC16AStaleEpochHintNamesTheAddressDriversConnectTo(t *testing.T) {
	for _, c := range []struct{ client, want string }{{"owner.example:7434", "owner.example:7434"}, {"", ""}} {
		state := cluster.ClusterState{Epoch: 4, StateSeq: 9,
			Members:   []cluster.Member{{ID: "n1", DataAddr: "192.0.2.1:4433", ClientAddr: c.client, State: cluster.MemberActive}},
			Databases: []cluster.Database{{Name: "app", ID: 1, RF: 1}}}
		node := &Node{topology: NewClusterTopology("n1", c5Source{state: state})}
		err := node.checkClientEpoch(3, bucketRef{db: "app", dbID: 1, name: "Box"}, []byte("b1"))
		var detailed *ringDetailed
		if !errors.As(err, &detailed) {
			t.Fatalf("a stale epoch gave %v", err)
		}
		if got := detailed.detail.OwnerAddr; got != c.want || detailed.detail.Owner != "n1" {
			t.Errorf("client %q: owner %q at %q, want n1 at %q", c.client, detailed.detail.Owner, got, c.want)
		}
	}
}

// TestC16ANodeRefusesAClientAddrTheClusterCannotKeep: [GUARD OpenNode refuses a ClientAddr over the bound with the fix,
// before it opens anything].
func TestC16ANodeRefusesAClientAddrTheClusterCannotKeep(t *testing.T) {
	_, err := OpenNode(context.Background(), NodeConfig{Dir: t.TempDir(), ClientAddr: strings.Repeat("a", cluster.MaxClientAddrBytes+1)})
	if err == nil || !strings.Contains(err.Error(), "tcp.advertise") {
		t.Fatalf("an over-long client address gave %v", err)
	}
}

// TestC5TheReplyCarriesTheVoterCount: [GUARD the reply states how many members vote, counting a voter that is
// joining or dead and so is on neither list; it is absent when 0, never negative, and absent when unchanged].
func TestC5TheReplyCarriesTheVoterCount(t *testing.T) {
	reply := c5Reply()
	reply.Voters = 4
	raw := u50EncodeReply(t, reply)
	got, err := DecodeRingTopologyReply(raw)
	if err != nil || got.Voters != 4 {
		t.Fatalf("decoded %d voters, %v", got.Voters, err)
	}
	if again := u50EncodeReply(t, got); !bytes.Equal(again, raw) {
		t.Fatal("a decoded reply does not re-encode to the same bytes")
	}
	none := c5Reply()
	if bytes.Contains(u50EncodeReply(t, none), []byte("voters")) {
		t.Error("a reply with no voter count wrote the field")
	}
	none.Voters = -1
	if _, err := EncodeRingTopologyReply(none); err == nil {
		t.Error("a negative voter count was encoded")
	}
	unchanged := RingTopologyReply{Epoch: 7, StateSeq: 42, Unchanged: true, Voters: 3}
	if _, err := EncodeRingTopologyReply(unchanged); err == nil {
		t.Error("an unchanged reply with a voter count was encoded")
	}
}

// TestC5TheServerCountsEveryCommittedVoter: [GUARD the voter count is the committed voter list, so a voter that is
// joining or dead, on neither blob, is still counted].
func TestC5TheServerCountsEveryCommittedVoter(t *testing.T) {
	state := c5State()
	state.Members = append(state.Members, cluster.Member{ID: "node-y", DataAddr: "192.0.2.6:4433", State: cluster.MemberJoining, Suffrage: "voter"})
	state.Voters = []string{"node-a", "node-b", "node-w", "node-y"}
	server := &ringServer{node: &Node{topology: NewClusterTopology("node-a", c5Source{state: state})}, authz: u70AllowAll{}}
	reply, err := server.serveTopology(context.Background(), RingTopologyRequest{})
	if err != nil || reply.Voters != 4 {
		t.Fatalf("voters %d, %v", reply.Voters, err)
	}
}

// TestC5ARowTheStateCouldNeverHoldIsRefused: [GUARD the decoder refuses an empty data address and a data or client
// address that is not UTF-8 or holds NUL, because the state guarantees neither can happen].
func TestC5ARowTheStateCouldNeverHoldIsRefused(t *testing.T) {
	row := func(data, client string) []byte {
		return AppendTopologyMembers(nil, []cluster.Member{{ID: "node-a", DataAddr: data, ClientAddr: client, State: cluster.MemberActive, Suffrage: "voter"}})
	}
	build := func(blob []byte) []byte {
		raw, err := bson.Marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "members", Value: blob},
			{Key: "table", Value: cluster.BuildTable(cluster.ClusterState{Members: []cluster.Member{{ID: "node-a", DataAddr: "a:1", State: cluster.MemberActive}}}).Encode()}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := DecodeRingTopologyReply(build(row("a:1", "a:2"))); err != nil {
		t.Fatalf("the control was refused: %v", err)
	}
	for name, blob := range map[string][]byte{
		"an empty data address":     row("", ""),
		"a NUL in the data address": row("a\x00:1", ""),
		"bad UTF-8 in data address": row("a\xff:1", ""),
		"a NUL in the client":       row("a:1", "a\x00:2"),
		"bad UTF-8 in client":       row("a:1", "a\xfe:2"),
	} {
		_, err := DecodeRingTopologyReply(build(blob))
		u50Refused(t, name, err)
	}
}

// TestC5AHugeMemberCountIsRefusedBeforeItIsAllocated: [GUARD a members blob that claims more rows than a table can rank
// (65535) is refused before the member slice is made, even when the bytes for that many rows are present].
func TestC5AHugeMemberCountIsRefusedBeforeItIsAllocated(t *testing.T) {
	const rows = 5_000_000
	blob := binary.AppendUvarint(nil, rows)
	blob = append(blob, bytes.Repeat([]byte{byte(cluster.MemberActive), 0, 1, 'a', 0, 0}, rows)...)
	raw, err := bson.Marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "members", Value: blob},
		{Key: "table", Value: make([]byte, ringTopologySlots)}})
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = DecodeRingTopologyReply(raw)
	runtime.ReadMemStats(&after)
	u50Refused(t, "five million rows", err)
	// The document copy is the blob itself (30 MB); a member slice of 5 million rows would add 480 MB.
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 100<<20 {
		t.Errorf("a %d-byte reply made the decoder allocate %d bytes", len(raw), spent)
	}
}

// TestC5MutationsInsideTheBlobsReachTheParsers: [GUARD 100000 mutations of the members and witnesses bytes, and 20000 of
// the table bytes, inside a valid document: no panic, anything accepted re-encodes identically, and a decode
// allocates on average under 512 KiB].
func TestC5MutationsInsideTheBlobsReachTheParsers(t *testing.T) {
	reply := c5Reply()
	members := AppendTopologyMembers(nil, reply.Table.Members())
	witnesses := AppendTopologyMembers(nil, reply.Witnesses)
	table := reply.Table.Encode()
	random := rand.New(rand.NewSource(55))
	run := func(name string, iterations int, mutate func() (m, w, tab []byte)) {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		accepted := 0
		for range iterations {
			m, w, tab := mutate()
			body := c5ForgeWith(t, m, w, tab)
			if got, err := DecodeRingTopologyReply(body); err == nil {
				accepted++
				again, encodeErr := EncodeRingTopologyReply(got)
				if encodeErr != nil || !bytes.Equal(again, body) {
					t.Fatalf("%s: accepted a reply that re-encodes differently (%v)", name, encodeErr)
				}
			}
		}
		runtime.ReadMemStats(&after)
		if per := (after.TotalAlloc - before.TotalAlloc) / uint64(iterations); per > 512<<10 {
			t.Errorf("%s: a decode allocated %d bytes on average", name, per)
		}
		t.Logf("%s: %d of %d mutations were still valid", name, accepted, iterations)
	}
	run("members", 50000, func() ([]byte, []byte, []byte) { return u50Mutate(random, members), witnesses, table })
	run("witnesses", 50000, func() ([]byte, []byte, []byte) { return members, u50Mutate(random, witnesses), table })
	run("table", 20000, func() ([]byte, []byte, []byte) { return members, witnesses, u50Mutate(random, table) })
}

func c5ForgeWith(t *testing.T, members, witnesses, table []byte) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "epoch", Value: int64(7)}, {Key: "state_seq", Value: int64(42)}, {Key: "members", Value: members},
		{Key: "witnesses", Value: witnesses}, {Key: "table", Value: table},
		{Key: "databases", Value: bson.A{bson.D{{Key: "name", Value: "app"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: int32(2)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
