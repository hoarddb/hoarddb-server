package main

import (
	"slices"
	"strings"
	"testing"
)

func TestParseFlagsRoutingAndConcern(t *testing.T) {
	cfg, err := parseFlags([]string{"-addr", "n1:7433, n2:7433,n3:7433", "-routing", "B", "-write-concern", "Majority"})
	if err != nil {
		t.Fatalf("parsing a three-node hoardDB run: %v", err)
	}
	if !slices.Equal(cfg.Addrs, []string{"n1:7433", "n2:7433", "n3:7433"}) || cfg.Routing != routingB || cfg.WriteConcern != "majority" {
		t.Errorf("addrs=%v routing=%q write concern=%q", cfg.Addrs, cfg.Routing, cfg.WriteConcern)
	}
	defaults, err := parseFlags(nil)
	if err != nil || defaults.Routing != routingA || defaults.WriteConcern != "" || len(defaults.Addrs) != 1 {
		t.Errorf("defaults: routing=%q concern=%q addrs=%v err=%v; a bare run is one node under RP-A at the node's concern", defaults.Routing, defaults.WriteConcern, defaults.Addrs, err)
	}
	failing := []struct {
		name string
		args []string
		want string
	}{
		{"routing", []string{"-routing", "c"}, "-routing"},
		{"write concern", []string{"-write-concern", "quorum"}, "-write-concern"},
		{"address list", []string{"-addr", "n1:7433,n1:7433"}, "twice"},
		{"concern on MongoDB", []string{"-engine", "mongo", "-write-concern", "all"}, "-mongo-wc"},
		{"read preference", []string{"-engine", "mongo", "-mongo-read-pref", "closest"}, "-mongo-read-pref"},
		{"MySQL read routing", []string{"-engine", "mysql", "-mysql-read-routing", "sideways"}, "-mysql-read-routing"},
	}
	for _, tc := range failing {
		if _, err := parseFlags(tc.args); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: parseFlags(%v) error = %v; want one naming %q", tc.name, tc.args, err, tc.want)
		}
	}
}

// TestLegRecordNamesRoutingAndNodes: a table groups legs by routing and counts
// nodes for its scaling rows, so the record must carry what the flags said.
func TestLegRecordNamesRoutingAndNodes(t *testing.T) {
	cfg := mixTestConfig("rh", "social_routing")
	cfg.Addrs, cfg.Routing, cfg.WriteConcern = []string{"n1:7433", "n2:7433"}, routingB, "majority"
	record := completedMixRecord(t, fakeHoardEngine(cfg), cfg)
	if record.Routing != "RP-B" || record.Nodes != 2 || record.WriteConcern != "majority" {
		t.Errorf("record routing=%q nodes=%d write concern=%q; want RP-B, 2, majority", record.Routing, record.Nodes, record.WriteConcern)
	}
	mongoNodes := (&config{Engine: engineMongo, MongoURI: "mongodb://a:1,b:2,c:3/?replicaSet=rs"}).nodeCount()
	mysqlNodes := (&config{Engine: engineMySQL, MySQLDSN: "u@tcp(a:1)/s,u@tcp(b:1)/s"}).nodeCount()
	if mongoNodes != 3 || mysqlNodes != 2 {
		t.Errorf("node counts: mongo %d, mysql %d; want 3 and 2", mongoNodes, mysqlNodes)
	}
}

func TestMySQLDSNListSplit(t *testing.T) {
	cfg := &config{MySQLDSN: "u:p@tcp(a:3306)/social, u:p@tcp(b:3306)/social"}
	if dsns := cfg.mysqlDSNs(); len(dsns) != 2 || dsns[1] != "u:p@tcp(b:3306)/social" {
		t.Errorf("mysqlDSNs = %q; want the source then the replica, trimmed", dsns)
	}
}
