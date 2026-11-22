package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// recordingMySQLEngine is a MySQL engine over three recording pools: a source and two replicas.
func recordingMySQLEngine(t *testing.T, routing string) (*mysqlEngine, []string) {
	t.Helper()
	servers := []string{t.Name() + "/source", t.Name() + "/replica1", t.Name() + "/replica2"}
	cfg := &config{Engine: engineMySQL, Routing: routing, OpTimeout: time.Second, Conc: 2}
	engine := &mysqlEngine{
		cfg: cfg, db: recordingPool(t, servers[0]), ids: newTimeOrderedIDs(), social: map[string]*sql.Stmt{},
		replicas: []*mysqlReplica{
			{db: recordingPool(t, servers[1]), statements: map[string]*sql.Stmt{}},
			{db: recordingPool(t, servers[2]), statements: map[string]*sql.Stmt{}},
		},
		reads: newRoundRobin(3, cfg.mysqlReadRouting()),
	}
	t.Cleanup(engine.close)
	return engine, servers
}

// runMySQLReadsAndWrites does 30 reads (R5) and 6 writes (W3) on the engine.
func runMySQLReadsAndWrites(t *testing.T, engine *mysqlEngine) {
	t.Helper()
	ops, err := engine.newSocialOps()
	if err != nil {
		t.Fatalf("opening the operations handle: %v", err)
	}
	ctx := context.Background()
	for range 30 {
		if _, err := ops.followCheck(ctx, 1, 2); err != nil {
			t.Fatalf("reading: %v", err)
		}
	}
	for range 6 {
		if err := ops.follow(ctx, 1, 2); err != nil {
			t.Fatalf("writing: %v", err)
		}
	}
}

func TestRoutingA_UsesOneEndpointMySQL(t *testing.T) {
	engine, servers := recordingMySQLEngine(t, routingA)
	runMySQLReadsAndWrites(t, engine)
	if got := countKind(statementsOn(servers[0]), "query"); got != 30 {
		t.Errorf("RP-A: the source ran %d reads; want all 30", got)
	}
	for _, replica := range servers[1:] {
		if statements := statementsOn(replica); len(statements) != 0 {
			t.Errorf("RP-A: replica %s ran %v; it must not be touched", replica, statements)
		}
	}
}

func TestRoutingB_UsesEveryEndpointMySQL(t *testing.T) {
	engine, servers := recordingMySQLEngine(t, routingB)
	runMySQLReadsAndWrites(t, engine)
	for _, server := range servers {
		if got := countKind(statementsOn(server), "query"); got != 10 {
			t.Errorf("RP-B: %s ran %d reads; want 10 of 30 (one pool per server, reads in turn)", server, got)
		}
	}
	for _, replica := range servers[1:] {
		if got := countKind(statementsOn(replica), "exec"); got != 0 {
			t.Errorf("RP-B: replica %s ran %d writes; writes go to the source only", replica, got)
		}
	}
	if got := countKind(statementsOn(servers[0]), "exec"); got == 0 {
		t.Error("RP-B: the source ran no writes")
	}
}

func TestMySQLReadRoutingFlagOverridesRouting(t *testing.T) {
	cases := []struct{ routing, flag, want string }{
		{routingA, "", routingA}, {routingB, "", routingB},
		{routingA, "roundrobin", routingB}, {routingB, "primary", routingA},
	}
	for _, tc := range cases {
		cfg := &config{Routing: tc.routing, MySQLReadRouting: tc.flag}
		if got := cfg.mysqlReadRouting(); got != tc.want {
			t.Errorf("-routing %s -mysql-read-routing %q gives %s; want %s", tc.routing, tc.flag, got, tc.want)
		}
	}
}

func TestRoutingMongoReadPreference(t *testing.T) {
	const replicaSet = "mongodb://a,b,c/?replicaSet=rs"
	cases := []struct {
		name, uri, routing, flag string
		wantMode                 readpref.Mode
		wantSource               string
	}{
		{"RP-A reads the primary", replicaSet, routingA, "", readpref.PrimaryMode, readPrefFromRouting},
		{"RP-B reads the nearest member", replicaSet, routingB, "", readpref.NearestMode, readPrefFromRouting},
		{"the flag beats the routing", replicaSet, routingB, "secondaryPreferred", readpref.SecondaryPreferredMode, readPrefFromFlag},
		{"the URI beats both", replicaSet + "&readPreference=secondary", routingB, "nearest", readpref.SecondaryMode, readPrefFromURI},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config{MongoURI: tc.uri, Routing: tc.routing, MongoReadPref: tc.flag, MongoWC: "w1j", Conc: 2}
			preference, source, err := resolveMongoReadPref(cfg)
			if err != nil {
				t.Fatalf("resolving: %v", err)
			}
			concern, _, err := resolveMongoConcern(cfg)
			if err != nil {
				t.Fatalf("resolving the write concern: %v", err)
			}
			if got := mongoClientOptions(cfg, concern, preference).ReadPreference.Mode(); got != tc.wantMode || source != tc.wantSource {
				t.Errorf("client read preference %v from %s; want %v from %s", got, source, tc.wantMode, tc.wantSource)
			}
		})
	}
	if _, _, err := resolveMongoReadPref(&config{MongoURI: "mongodb://a", MongoReadPref: "closest"}); err == nil {
		t.Error("-mongo-read-pref closest accepted; MongoDB has no such mode")
	}
}
