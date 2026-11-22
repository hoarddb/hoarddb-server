package server

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

// TestU83DescribeNodeIsOpenToEverySessionWithTheWholeReply: [GUARD a session with no grants, a
// reader of one database and the bootstrap root all get the node's facts, host part included,
// as the legacy health request gives them to every user; the facts are those of the running
// one-node server].
func TestU83DescribeNodeIsOpenToEverySessionWithTheWholeReply(t *testing.T) {
	qs, _, root := ringComposeServer(t)
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}
	sessions := map[string]*tcpServerClient{
		"root":    root,
		"nobody":  ringAdminUser(t, qs, "nobody"),
		"reader":  ringAdminUser(t, qs, "reader", Grant{Role: roleReader, DB: ringComposeDB}),
		"manager": ringAdminUser(t, qs, "usermanager", Grant{Role: roleUserManager, DB: "vault"}),
	}
	for name, session := range sessions {
		reply := ringAdmin(t, session, wire.RingAdminRequest{Cmd: wire.AdminDescribeNode})
		node := reply.Node
		if node == nil {
			t.Fatalf("%s: no node in %+v", name, reply)
		}
		if node.NodeID == "" || node.Version != version.Version || node.MemberState != "active" || node.Members != 1 || node.Voters != 1 ||
			node.Leader != node.NodeID || node.Serving != wire.ServingOK || node.DefaultRF != 1 || node.Epoch < 1 || node.UptimeMS < 0 {
			t.Errorf("%s: described the node as %+v", name, node)
		}
		if health.Host == nil {
			continue
		}
		if node.Host == nil || int(node.Host.CPUs) != health.Host.CPUs || (node.Host.Load == nil) != (health.Host.Load == nil) ||
			(node.Host.MemTotal > 0) != (health.Host.MemoryTotalBytes > 0) {
			t.Errorf("%s: host %+v (load %v), the legacy health request reports %+v (load %v)", name, node.Host, node.Host.Load, health.Host, health.Host.Load)
		}
	}
}

// TestU83DescribeNodeRefusesWhatItDoesNotTake: [GUARD a describe_node with an extra member is
// refused with INVALID_ARGUMENT naming the member, on the live listener].
func TestU83DescribeNodeRefusesWhatItDoesNotTake(t *testing.T) {
	_, _, root := ringComposeServer(t)
	raw, err := bson.Marshal(bson.D{{Key: "cmd", Value: wire.AdminDescribeNode}, {Key: "database", Value: ringComposeDB}})
	if err != nil {
		t.Fatal(err)
	}
	refusal := ringAdminRefusal(t, root.ringSend(transport.OpRingAdmin, raw), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "also sets database") || !strings.Contains(refusal.Message, "describe_node takes") {
		t.Errorf("the refusal %q does not name the member and the command", refusal.Message)
	}
}

// TestU83DescribeNodeFollowsTheLegacyHealthRule: [GUARD the class stands for the legacy health
// request and needs what it needs (no action, no scope); a session with no grants is allowed,
// and a context with no session, no identity or on the internode listener is refused].
func TestU83DescribeNodeFollowsTheLegacyHealthRule(t *testing.T) {
	if got := ringLegacyOpcode[wire.RingClassDescribeNode]; got != transport.OpHealthCheck {
		t.Fatalf("describe_node stands for %v, want the health request", got)
	}
	if rule := opRules[transport.OpHealthCheck]; rule.Action != ActionNone || rule.Scope != ScopeNone {
		t.Fatalf("the health request is now ruled %+v: describe_node must follow it", rule)
	}
	authz := newTestServer(t).RingAuthorizer()
	peer := &transport.Session{Peer: true}
	peer.SetIdentity(transport.Identity{Username: NodeUser, Grants: []transport.ScopedGrant{{Role: roleRoot, DB: "*"}}})
	cases := []struct {
		name  string
		ctx   context.Context
		allow bool
	}{
		{"no grants", u70Ctx(), true},
		{"a reader", u70Ctx(transport.ScopedGrant{Role: roleReader, DB: "app"}), true},
		{"no session", context.Background(), false},
		{"no identity", transport.WithSession(context.Background(), &transport.Session{}), false},
		{"internode", transport.WithSession(context.Background(), peer), false},
	}
	for _, tc := range cases {
		err := authz.Authorize(tc.ctx, wire.RingAccess{Class: wire.RingClassDescribeNode})
		if (err == nil) != tc.allow {
			t.Errorf("%s: %v, allow=%v", tc.name, err, tc.allow)
		}
	}
}

// TestU83TheHostPartIsTheLegacyHealthReading: [GUARD the host part carries the legacy host
// facts and the memory row the banner drew, and is absent when the server read no host].
func TestU83TheHostPartIsTheLegacyHealthReading(t *testing.T) {
	ratio := 0.25
	host := &HealthHost{CPUs: 4, HostCPUs: 16, MemoryTotalBytes: 16 << 30, MemoryUsedBytes: 6 << 30, MemorySource: MemorySourceCgroup,
		Load: &HealthLoad{OneMinute: 1.5, FiveMinutes: 1, FifteenMinutes: 0.5}}
	got := ringAdminHost(host, &ratio, 8<<30)
	want := &wire.RingAdminHost{CPUs: 4, HostCPUs: 16, Load: &wire.RingAdminLoad{One: 1.5, Five: 1, Fifteen: 0.5},
		MemUsed: 6 << 30, MemTotal: 16 << 30, MemSource: "cgroup", Process: &wire.RingAdminProcess{Used: 2 << 30, Limit: 8 << 30}}
	if got == nil || got.Load == nil || got.Process == nil {
		t.Fatalf("host %+v, want %+v", got, want)
	}
	if got.CPUs != want.CPUs || got.HostCPUs != want.HostCPUs || *got.Load != *want.Load || got.MemUsed != want.MemUsed ||
		got.MemTotal != want.MemTotal || got.MemSource != want.MemSource || *got.Process != *want.Process {
		t.Errorf("host %+v (process %+v), want %+v (process %+v)", got, got.Process, want, want.Process)
	}
	if bare := ringAdminHost(&HealthHost{CPUs: 2}, nil, 0); bare == nil || bare.Load != nil || bare.Process != nil || bare.MemTotal != 0 || bare.MemSource != "" {
		t.Errorf("a host with only CPUs came out as %+v", bare)
	}
	if ringAdminHost(nil, &ratio, 8<<30) != nil {
		t.Error("no host reading still produced a host part")
	}
	if ringAdminHost(&HealthHost{}, &ratio, 8<<30) != nil {
		t.Error("a reading with no CPUs (a host that could not be read) still produced a host part")
	}
}
