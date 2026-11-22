package wire

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u83FullHost() *RingAdminHost {
	return &RingAdminHost{
		CPUs: 4, HostCPUs: 16, Load: &RingAdminLoad{One: 1.02, Five: 0.98, Fifteen: 0.91},
		MemUsed: 6 << 30, MemTotal: 15 << 30, MemSource: "cgroup", Process: &RingAdminProcess{Used: 1 << 30, Limit: 15 << 30},
	}
}

func u83Replies() []RingAdminReply {
	return []RingAdminReply{
		{Cmd: AdminDescribeNode, Node: &RingAdminNode{NodeID: "node-a", Version: "1.1.0-dev", UptimeMS: 312000, MemberState: "active",
			Members: 1, Voters: 1, Epoch: 1, Leader: "node-a", DefaultRF: 1, Serving: ServingOK}},
		{Cmd: AdminDescribeNode, Node: &RingAdminNode{NodeID: "node-c", Version: "1.1.0-dev", UptimeMS: 1, MemberState: "joining",
			Members: 4, Voters: 3, Epoch: 4, Leader: "", DefaultRF: 2, Serving: ServingNoQuorum, Host: &RingAdminHost{CPUs: 1}}},
		{Cmd: AdminDescribeNode, Node: &RingAdminNode{NodeID: "node-b", Version: "x", MemberState: "",
			Members: 4, Voters: 3, Epoch: 4, Leader: "node-b", DefaultRF: 2, Serving: ServingLeaseLapsed, Host: u83FullHost()}},
		{Cmd: AdminDescribeNode, Node: &RingAdminNode{NodeID: "node-d", Version: "1.1.0-dev", UptimeMS: 5, MemberState: "active",
			Members: 3, Voters: 3, Epoch: 2, Leader: "node-d", DefaultRF: 1, Serving: ServingOK, StateRange: "1-1",
			RefusedPeers: []string{"node-e (raft channel) speaks 0-0 (writes 0)"}}},
	}
}

// TestU83DescribeNodeRoundTrips: [GUARD the request and every reply shape, with and without
// the host part and each of its optional members, are encoded and decoded to the same value].
func TestU83DescribeNodeRoundTrips(t *testing.T) {
	raw, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeNode})
	if err != nil {
		t.Fatal(err)
	}
	if req, err := DecodeRingAdmin(raw); err != nil || req.Cmd != AdminDescribeNode {
		t.Fatalf("request round trip gave %+v, %v", req, err)
	}
	for _, reply := range u83Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%+v: %v", reply.Node, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil {
			t.Fatalf("%+v: %v", reply.Node, err)
		}
		if !reflect.DeepEqual(got, reply) {
			t.Fatalf("round trip gave %+v (host %+v), want %+v (host %+v)", got.Node, got.Node.Host, reply.Node, reply.Node.Host)
		}
	}
}

// TestU83DescribeNodeRefusalsNameTheFix: [GUARD each limit of the request and of the reply
// is checked and the text names the member and what to send instead].
func TestU83DescribeNodeRefusalsNameTheFix(t *testing.T) {
	_, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeNode, Database: "app"})
	var coded *cluster.Error
	if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
		t.Fatalf("a describe_node naming a database was encoded or wrongly refused: %v", err)
	}
	for _, word := range []string{"describe_node", "database", "Remove"} {
		if !strings.Contains(coded.Message, word) {
			t.Errorf("the refusal %q does not mention %q", coded.Message, word)
		}
	}
	good := func() *RingAdminNode {
		node := *u83Replies()[0].Node
		return &node
	}
	with := func(edit func(*RingAdminNode)) RingAdminReply {
		node := good()
		edit(node)
		return RingAdminReply{Cmd: AdminDescribeNode, Node: node}
	}
	nan := math.NaN()
	cases := []struct {
		name  string
		reply RingAdminReply
		want  []string
	}{
		{"no node", RingAdminReply{Cmd: AdminDescribeNode}, []string{"describe_node", "node_id"}},
		{"extra bucket list", RingAdminReply{Cmd: AdminDescribeNode, Node: good(), Buckets: []RingAdminListed{}}, []string{"describe_node", "nothing else"}},
		{"node on another command", RingAdminReply{Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{}, Node: good()}, []string{"show_databases", "node"}},
		{"empty node id", with(func(n *RingAdminNode) { n.NodeID = "" }), []string{"node_id"}},
		{"empty version", with(func(n *RingAdminNode) { n.Version = "" }), []string{"version"}},
		{"negative uptime", with(func(n *RingAdminNode) { n.UptimeMS = -1 }), []string{"uptime_ms"}},
		{"negative members", with(func(n *RingAdminNode) { n.Members = -1 }), []string{"members"}},
		{"rf below one", with(func(n *RingAdminNode) { n.DefaultRF = 0 }), []string{"default_rf"}},
		{"unknown serving word", with(func(n *RingAdminNode) { n.Serving = "mostly" }), []string{"serving", "ok", "no_quorum", "lease_lapsed"}},
		{"long leader", with(func(n *RingAdminNode) { n.Leader = strings.Repeat("l", ringMaxName+1) }), []string{"leader"}},
		{"zero cpus", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{} }), []string{"host", "cpus"}},
		{"nan load", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, Load: &RingAdminLoad{One: nan}} }), []string{"host", "load"}},
		{"negative load", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, Load: &RingAdminLoad{Five: -1}} }), []string{"host", "load"}},
		{"used without total", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, MemUsed: 5} }), []string{"host", "mem_total"}},
		{"used above total", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, MemUsed: 9, MemTotal: 5, MemSource: "host"} }), []string{"host", "mem_used"}},
		{"total without source", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, MemTotal: 5} }), []string{"host", "mem_source"}},
		{"source without total", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, MemSource: "host"} }), []string{"host", "mem_source"}},
		{"unknown source", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, MemTotal: 5, MemSource: "vm"} }), []string{"host", "mem_source", "cgroup"}},
		{"process without limit", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, Process: &RingAdminProcess{Used: 5}} }), []string{"host", "process_limit"}},
		{"negative host cpus", with(func(n *RingAdminNode) { n.Host = &RingAdminHost{CPUs: 1, HostCPUs: -2} }), []string{"host", "host_cpus"}},
	}
	for _, tc := range cases {
		_, err := EncodeRingAdminReply(tc.reply)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: encoded or wrong error: %v", tc.name, err)
			continue
		}
		for _, word := range tc.want {
			if !strings.Contains(coded.Message, word) {
				t.Errorf("%s: the refusal %q does not mention %q", tc.name, coded.Message, word)
			}
		}
	}
}

// TestU83DescribeNodeDecoderIsStrict: [GUARD canonical layout: extras, order, types, written
// defaults and a host document that breaks its own rules are all refused].
func TestU83DescribeNodeDecoderIsStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	cmd := bson.E{Key: "cmd", Value: AdminDescribeNode}
	if _, err := DecodeRingAdmin(doc(cmd, bson.E{Key: "database", Value: "app"})); err == nil ||
		!strings.Contains(err.Error(), "also sets database") || !strings.Contains(err.Error(), "Remove database") {
		t.Errorf("a describe_node with a database member was refused with %v, want the member named", err)
	}
	for name, raw := range map[string][]byte{
		"extra member":    doc(cmd, bson.E{Key: "extra", Value: int32(1)}),
		"database":        doc(cmd, bson.E{Key: "database", Value: "app"}),
		"limit":           doc(cmd, bson.E{Key: "limit", Value: int32(5)}),
		"reordered":       doc(bson.E{Key: "extra", Value: int32(1)}, cmd),
		"cmd twice":       doc(cmd, cmd),
		"cmd as a number": doc(bson.E{Key: "cmd", Value: int32(1)}),
	} {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("request %s: accepted", name)
		}
	}
	node := func(host any, mutate func(bson.D) bson.D) []byte {
		elements := bson.D{{Key: "cmd", Value: AdminDescribeNode}, {Key: "node_id", Value: "node-a"}, {Key: "version", Value: "v"},
			{Key: "uptime_ms", Value: int64(5)}, {Key: "member_state", Value: "active"}, {Key: "members", Value: int32(1)},
			{Key: "voters", Value: int32(1)}, {Key: "epoch", Value: int64(1)}, {Key: "leader", Value: "node-a"},
			{Key: "default_rf", Value: int32(1)}, {Key: "serving", Value: "ok"}}
		if host != nil {
			elements = append(elements, bson.E{Key: "host", Value: host})
		}
		if mutate != nil {
			elements = mutate(elements)
		}
		raw, err := bson.Marshal(elements)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := DecodeRingAdminReply(node(nil, nil)); err != nil {
		t.Fatalf("the plain reply is refused: %v", err)
	}
	if _, err := DecodeRingAdminReply(node(bson.D{{Key: "cpus", Value: int32(2)}}, nil)); err != nil {
		t.Fatalf("a reply with a minimal host is refused: %v", err)
	}
	swap := func(a, b int) func(bson.D) bson.D {
		return func(d bson.D) bson.D { d[a], d[b] = d[b], d[a]; return d }
	}
	cases := map[string][]byte{
		"members as int64":     node(nil, func(d bson.D) bson.D { d[5].Value = int64(1); return d }),
		"uptime as int32":      node(nil, func(d bson.D) bson.D { d[3].Value = int32(5); return d }),
		"epoch as double":      node(nil, func(d bson.D) bson.D { d[7].Value = float64(1); return d }),
		"leader missing":       node(nil, func(d bson.D) bson.D { return append(d[:8:8], d[9:]...) }),
		"reordered members":    node(nil, swap(5, 6)),
		"extra member":         node(nil, func(d bson.D) bson.D { return append(d, bson.E{Key: "buckets", Value: int32(7)}) }),
		"host not a document":  node("x", nil),
		"host without cpus":    node(bson.D{{Key: "host_cpus", Value: int32(4)}}, nil),
		"host cpus as int64":   node(bson.D{{Key: "cpus", Value: int64(2)}}, nil),
		"host extra member":    node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "temperature", Value: int32(40)}}, nil),
		"host reordered":       node(bson.D{{Key: "host_cpus", Value: int32(4)}, {Key: "cpus", Value: int32(2)}}, nil),
		"host load partial":    node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "load_1", Value: 0.5}}, nil),
		"host load as int":     node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "load_1", Value: int32(1)}, {Key: "load_5", Value: 0.5}, {Key: "load_15", Value: 0.5}}, nil),
		"host total no source": node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "mem_total", Value: int64(9)}}, nil),
		"host process half":    node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "process_used", Value: int64(9)}}, nil),
		"host written default": node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "host_cpus", Value: int32(0)}}, nil),
		"host nested too deep": node(bson.D{{Key: "cpus", Value: int32(2)}, {Key: "mem_source", Value: bson.D{{Key: "a", Value: bson.D{}}}}}, nil),
	}
	for name, raw := range cases {
		if _, err := DecodeRingAdminReply(raw); err == nil {
			t.Errorf("reply %s: accepted", name)
		}
	}
}

// TestU83DescribeNodeDecodersSurviveMutation: [GUARD 100000 mutations: no panic, an accepted
// body re-encodes to the same bytes].
func TestU83DescribeNodeDecodersSurviveMutation(t *testing.T) {
	request, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeNode})
	if err != nil {
		t.Fatal(err)
	}
	var replies [][]byte
	for _, reply := range u83Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, raw)
	}
	rng := rand.New(rand.NewPCG(83, 83))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, request)
		if req, err := DecodeRingAdmin(mutated); err == nil {
			acceptedRequests++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, replies[rng.IntN(len(replies))])
		if reply, err := DecodeRingAdminReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeRingAdminReply(reply); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestU83DescribeNodeDecodeAllocationIsBounded: [GUARD a body that lies about its length, and
// a host document that lies about its own, allocate a bounded amount before they are refused].
func TestU83DescribeNodeDecodeAllocationIsBounded(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0}
	lie = append(lie, byte(len(AdminDescribeNode)+1), 0, 0, 0)
	lie = append(lie, AdminDescribeNode...)
	lie = append(lie, 0, 0x03, 'h', 'o', 's', 't', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdmin(lie)
			_, _ = DecodeRingAdminReply(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying describe_node body of %d bytes allocated %d bytes", len(lie), perOp)
	}
}

// u83Liveness is a fake Raft group and lease.
type u83Liveness struct {
	leader string
	lease  bool
}

func (l u83Liveness) Leader() string   { return l.leader }
func (l u83Liveness) LeaseValid() bool { return l.lease }

type u83Source struct{ state cluster.ClusterState }

func (s u83Source) State() cluster.ClusterState { return s.state }

func u83State(self cluster.MemberState, members ...string) cluster.ClusterState {
	state := cluster.ClusterState{Epoch: 4, StateSeq: 9, NextDatabaseID: 1, Voters: members[:min(len(members), 3)]}
	for i, id := range members {
		member := cluster.Member{ID: id, State: cluster.MemberActive, DataAddr: fmt.Sprintf("192.0.2.%d:7533", 10+i)}
		if id == "node-a" {
			member.State = self
		}
		state.Members = append(state.Members, member)
	}
	return state
}

func u83Fake(state cluster.ClusterState, live u83Liveness) *Node {
	return &Node{cfg: NodeConfig{NodeID: "node-a"}, topology: NewClusterTopology("node-a", u83Source{state}), live: live}
}

// TestU83DescribeNodeReportsTheCommittedState: [GUARD the member state is this node's own row,
// the counts, epoch and leader are the committed state's, and serving follows the lease and the
// leader]  on a one-member and a four-member fake state.
func TestU83DescribeNodeReportsTheCommittedState(t *testing.T) {
	started := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	facts := RingNodeFacts{Version: "1.1.0-test", StartedAt: started, Now: func() time.Time { return started.Add(312 * time.Second) }}
	cases := []struct {
		name string
		node *Node
		want RingAdminNode
	}{
		{"one node", u83Fake(u83State(cluster.MemberActive, "node-a"), u83Liveness{"node-a", true}),
			RingAdminNode{NodeID: "node-a", MemberState: "active", Members: 1, Voters: 1, Leader: "node-a", Serving: ServingOK}},
		{"joining member of four", u83Fake(u83State(cluster.MemberJoining, "node-a", "node-b", "node-c", "node-d"), u83Liveness{"node-b", true}),
			RingAdminNode{NodeID: "node-a", MemberState: "joining", Members: 4, Voters: 3, Leader: "node-b", Serving: ServingOK}},
		{"no leader", u83Fake(u83State(cluster.MemberActive, "node-a", "node-b", "node-c", "node-d"), u83Liveness{"", true}),
			RingAdminNode{NodeID: "node-a", MemberState: "active", Members: 4, Voters: 3, Serving: ServingNoQuorum}},
		{"lease lapsed beats no leader", u83Fake(u83State(cluster.MemberActive, "node-a", "node-b", "node-c", "node-d"), u83Liveness{"", false}),
			RingAdminNode{NodeID: "node-a", MemberState: "active", Members: 4, Voters: 3, Serving: ServingLeaseLapsed}},
		{"no row for this node", u83Fake(u83State(cluster.MemberActive, "node-b"), u83Liveness{"node-b", true}),
			RingAdminNode{NodeID: "node-a", MemberState: "", Members: 1, Voters: 1, Leader: "node-b", Serving: ServingOK}},
	}
	// The range is what this build declares, formatted as describeNode formats it, so the
	// next StateLevel bump does not fail this test.
	build := cluster.BuildStateRange()
	stateRange := fmt.Sprintf("%d-%d", build.Min, build.Max)
	for _, tc := range cases {
		got := tc.node.describeNode(facts)
		tc.want.Version, tc.want.UptimeMS, tc.want.Epoch, tc.want.DefaultRF, tc.want.StateRange = "1.1.0-test", 312000, 4, 1, stateRange
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
	// A clock before the start never reports a negative uptime.
	early := RingNodeFacts{Version: "v", StartedAt: started, Now: func() time.Time { return started.Add(-time.Hour) }}
	if got := cases[0].node.describeNode(early); got.UptimeMS != 0 {
		t.Errorf("uptime %d before the start, want 0", got.UptimeMS)
	}
	// The host part is the server's reading, and is left out when it has none.
	withHost := RingNodeFacts{Version: "v", StartedAt: started, Host: func() *RingAdminHost { return u83FullHost() }}
	if got := cases[0].node.describeNode(withHost); !reflect.DeepEqual(got.Host, u83FullHost()) {
		t.Errorf("host %+v, want the server's reading", got.Host)
	}
	noHost := RingNodeFacts{Version: "v", StartedAt: started, Host: func() *RingAdminHost { return nil }}
	if got := cases[0].node.describeNode(noHost); got.Host != nil {
		t.Errorf("host %+v, want none", got.Host)
	}
}

// u83Reads is how many Badger gets and iterators this process has started.
func u83Reads() int64 {
	var total int64
	for _, name := range []string{"badger_get_num_user", "badger_iterator_num_user"} {
		if counter, ok := expvar.Get(name).(*expvar.Int); ok {
			total += counter.Value()
		}
	}
	return total
}

// TestU83DescribeNodeOnARealNodeReadsNoBucket: [GUARD the answer comes from the committed
// view, the Raft group and the lease and never from a bucket: with 10000 buckets declared,
// serving the request starts no Badger get or iterator, and the facts are those of a live
// one-node cluster].
func TestU83DescribeNodeOnARealNodeReadsNoBucket(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	const buckets = 10_000
	for i := range buckets {
		if _, err := node.CreateBucket(ctx, u30iDB, fmt.Sprintf("B%05d", i)); err != nil {
			t.Fatal(err)
		}
	}
	handler, err := NewRingHandler(node, u66AuthNext, u70AllowAll{}, WithNodeFacts(RingNodeFacts{Version: "1.1.0-test", StartedAt: time.Now().Add(-time.Minute)}))
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminDescribeNode})
	if err != nil {
		t.Fatal(err)
	}
	before := u83Reads()
	frame, err := handler(ctx, transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
	if err != nil {
		t.Fatal(err)
	}
	if reads := u83Reads() - before; reads != 0 {
		t.Errorf("describe_node started %d Badger reads on a node with %d buckets, want 0", reads, buckets)
	}
	reply := u75Reply(t, frame)
	got := reply.Node
	if got == nil || got.NodeID != node.cfg.NodeID || got.MemberState != "active" || got.Members != 1 || got.Voters != 1 || got.Leader != node.cfg.NodeID ||
		got.Serving != ServingOK || got.DefaultRF != 1 || got.Epoch < 1 || got.Version != "1.1.0-test" || got.UptimeMS < 60_000 || got.Host != nil {
		t.Fatalf("a live one-node cluster described itself as %+v", got)
	}
}

// TestU83AFreshOneMemberNodeServesAtOnce: [GUARD a one-member node reports ok the moment it is
// open, with or without a known leader or a valid lease (a one-voter group serves a keyed
// request regardless); a four-member state with no leader still reports no_quorum].
func TestU83AFreshOneMemberNodeServesAtOnce(t *testing.T) {
	node := u30iOpen(t, t.TempDir())
	if got := node.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != ServingOK {
		t.Errorf("a node just opened reports serving %q", got.Serving)
	}
	one := u83Fake(u83State(cluster.MemberActive, "node-a"), u83Liveness{"", false})
	if got := one.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != ServingOK {
		t.Errorf("a one-member state with no leader and no lease reports serving %q", got.Serving)
	}
	four := u83Fake(u83State(cluster.MemberActive, "node-a", "node-b", "node-c", "node-d"), u83Liveness{"", true})
	if got := four.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != ServingNoQuorum {
		t.Errorf("four members, a valid lease and no leader report serving %q", got.Serving)
	}
}

// TestU83DefaultRFIsCreateDatabasesDefault: [GUARD describe_node and a create_database that
// names no rf read the one constant].
func TestU83DefaultRFIsCreateDatabasesDefault(t *testing.T) {
	node, _ := u63Fresh(t)
	handler, err := NewRingHandler(node, u66AuthNext, u70AllowAll{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: "rfdefault"})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
	if err != nil {
		t.Fatal(err)
	}
	created := u75Reply(t, frame)
	if created.RF != defaultReplicationFactor || node.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}).DefaultRF != defaultReplicationFactor {
		t.Errorf("create_database gave rf %d, constant %d", created.RF, defaultReplicationFactor)
	}
}

type u83BehindLiveness struct{ u83Liveness }

func (u83BehindLiveness) Behind() bool { return true }

// TestU83ABehindNodeIsNotReportedAsLeaseLapsed (pi re-review, F12's status surface): a node
// that saw a later epoch refuses requests with an epoch text, and describe_node says so
// whatever its lease says. [GUARD: report lease_lapsed]
func TestU83ABehindNodeIsNotReportedAsLeaseLapsed(t *testing.T) {
	state := u83State(cluster.MemberActive, "node-a", "node-b", "node-c", "node-d")
	for _, lease := range []bool{true, false} {
		node := u83Fake(state, u83Liveness{"node-b", lease})
		node.live = u83BehindLiveness{u83Liveness{"node-b", lease}}
		if got := node.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != ServingEpochBehind {
			t.Errorf("a behind node with lease %v reports serving %q, want %q", lease, got.Serving, ServingEpochBehind)
		}
	}
	lapsed := u83Fake(state, u83Liveness{"node-b", false})
	if got := lapsed.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}); got.Serving != ServingLeaseLapsed {
		t.Errorf("a node that is not behind and has no lease reports %q, want %q", got.Serving, ServingLeaseLapsed)
	}
}

// TestARefusedPeerWithALongIdDoesNotBreakDescribeNode: a handshake id may be 255 bytes, and the
// describe_node reply validates every refused-peer line at 255, so the line is clipped.
func TestARefusedPeerWithALongIdDoesNotBreakDescribeNode(t *testing.T) {
	row := cluster.RefusedPeer{NodeID: strings.Repeat("n", 255), Channel: "identity", Range: cluster.StateRange{Min: 9, Max: 9, Writes: 9}}
	line := refusedPeerLine(row)
	node := &RingAdminNode{NodeID: "a", Version: "v", MemberState: "active", DefaultRF: 1, Serving: ServingOK, StateRange: "1-1", RefusedPeers: []string{line}}
	if err := checkAdminNode(node); err != nil {
		t.Fatalf("the clipped line (%d bytes) fails the reply's own check: %v", len(line), err)
	}
	if !strings.Contains(line, "speaks 9-9 (writes 9)") {
		t.Fatalf("clipping lost the range: %s", line)
	}
}
