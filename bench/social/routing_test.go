package main

import (
	"slices"
	"testing"
)

func TestAddrListParsed(t *testing.T) {
	cases := []struct {
		name    string
		list    string
		want    []string
		wantErr bool
	}{
		{"one node", "192.0.2.1:7433", []string{"192.0.2.1:7433"}, false},
		{"three nodes with spaces", " a:1 , b:2,c:3 ", []string{"a:1", "b:2", "c:3"}, false},
		{"empty", "", nil, true},
		{"empty entry", "a:1,,b:2", nil, true},
		{"no port", "a:1,b", nil, true},
		{"duplicate", "a:1,b:2,a:1", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAddrList(tc.list)
			if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) {
				t.Fatalf("parseAddrList(%q) = %v, %v; want %v, error=%v", tc.list, got, err, tc.want, tc.wantErr)
			}
		})
	}
	if _, err := parseRouting("c"); err == nil {
		t.Error("-routing c accepted; only a and b exist")
	}
}

// hoardDialLog records which node each of 30 connections of a hoardDB engine
// was opened to.
func hoardDialLog(t *testing.T, routing string, addrs []string) (map[string]int, *hoardEngine) {
	t.Helper()
	opened := map[string]int{}
	fake := newFakeServer()
	cfg := &config{Engine: engineHoardDB, Database: "social", Addrs: addrs, Routing: routing, OpTimeout: 1}
	engine := &hoardEngine{cfg: cfg, dial: func(addr string) (wireCaller, func(), error) {
		opened[addr]++
		return fake.dial(nil, nil), func() {}, nil
	}}
	for range 30 {
		ops, err := engine.newSocialOps()
		if err != nil {
			t.Fatalf("opening a connection: %v", err)
		}
		ops.close()
	}
	return opened, engine
}

func TestRoutingA_UsesOneEndpoint(t *testing.T) {
	addrs := []string{"n1:7433", "n2:7433", "n3:7433"}
	opened, engine := hoardDialLog(t, routingA, addrs)
	if len(opened) != 1 || opened["n1:7433"] != 30 {
		t.Errorf("RP-A opened connections %v; want all 30 on the first node", opened)
	}
	if engine.target() != "n1:7433,n2:7433,n3:7433" {
		t.Errorf("target = %q; the record must name every node of the cluster", engine.target())
	}
}

func TestRoutingB_UsesEveryEndpoint(t *testing.T) {
	addrs := []string{"n1:7433", "n2:7433", "n3:7433"}
	opened, _ := hoardDialLog(t, routingB, addrs)
	for _, addr := range addrs {
		if opened[addr] != 10 {
			t.Errorf("RP-B opened %d connections to %s; want 10 of 30 (even round-robin): %v", opened[addr], addr, opened)
		}
	}
}
