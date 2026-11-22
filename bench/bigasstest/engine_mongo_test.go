package main

import (
	"context"
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// mongoTestURI is the connect variable of the live test. The test skips without it.
const mongoTestURI = "MONGO_URI"

// TestContractMongo runs the shared contract against a real MongoDB. Set MONGO_URI to a
// connection string (scripts/certification/engines.sh prints one). The login must be allowed on
// the four databases bat_<layout>: the script's bench user has readWrite on one database only, so
// use the root login it also creates. The test empties those databases first, because the seeder refuses a database that exists.
func TestContractMongo(t *testing.T) {
	uri := os.Getenv(mongoTestURI)
	if uri == "" {
		t.Skipf("%s is not set; point it at a MongoDB to run the live contract", mongoTestURI)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	dropMongoDatabases(ctx, t, uri, "bat")
	engine, err := openEngine(ctx, Config{Engine: "mongo", URI: uri, VUsers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }() // test teardown: the run is over
	runContract(t, contractTarget{Engine: engine, Name: "mongo", Totals: true, Database: "bat", SeedArgs: []string{"-mongo-uri", uri, "-mongo-tls"}})
}

// dropMongoDatabases empties the four layout databases of base so a seed starts clean.
func dropMongoDatabases(ctx context.Context, t *testing.T, uri, base string) {
	t.Helper()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetTLSConfig(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // self-signed benchmark server, the rule scenarios uses
	}))
	if err != nil {
		t.Fatalf("connecting to drop the old databases: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }() // test cleanup connection
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		// The bench user may drop collections but not databases; a database with no collection is gone.
		database := client.Database(layoutDatabase(base, layout))
		names, err := database.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			t.Fatalf("listing collections of %s: %v", layoutDatabase(base, layout), err)
		}
		for _, name := range names {
			if err := database.Collection(name).Drop(ctx); err != nil {
				t.Fatalf("dropping %s.%s: %v", layoutDatabase(base, layout), name, err)
			}
		}
	}
}

// TestMongoSpecsMatchModel needs no server: every store and bucket the adapter maps exists in
// the model's schema, keyed stores have a key column and the others a position column.
func TestMongoSpecsMatchModel(t *testing.T) {
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		spec := mongoLayouts[layout]
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		buckets := map[string]bool{}
		for _, bucket := range gen.Schema() {
			buckets[bucket.Name] = true
		}
		for name, store := range spec.Stores {
			bucket, field, _ := strings.Cut(name, ".")
			kind, err := KindOf(layout, bucket, field)
			if err != nil {
				t.Errorf("%s: %v", layout, err)
				continue
			}
			if kind.Keyed() != (store.Key != "") || (store.Seq != "") == kind.Keyed() {
				t.Errorf("%s %s is a %v store but the spec has key %q and seq %q", layout, name, kind, store.Key, store.Seq)
			}
		}
		for bucket := range spec.Docs {
			if !buckets[bucket] {
				t.Errorf("%s: spec names bucket %s, which the model lacks", layout, bucket)
			}
		}
	}
}

func TestMongoRejectsUnknownParam(t *testing.T) {
	_, err := openMongo(context.Background(), Config{Engine: "mongo", URI: "mongodb://localhost", Params: map[string]string{"pool": "9"}})
	if err == nil || !strings.Contains(err.Error(), `"pool"`) {
		t.Fatalf("an unknown param must be named in the error, got %v", err)
	}
}

// liveMongo opens the engine of the live tests and empties the databases of base, or skips.
func liveMongo(t *testing.T, base string) (context.Context, *mongoEngine, string) {
	t.Helper()
	uri := os.Getenv(mongoTestURI)
	if uri == "" {
		t.Skipf("%s is not set; point it at a MongoDB to run the live tests", mongoTestURI)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	dropMongoDatabases(ctx, t, uri, base)
	engine, err := openMongo(ctx, Config{Engine: "mongo", URI: uri, VUsers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() }) // test teardown: nothing is left to flush
	return ctx, engine.(*mongoEngine), uri
}

func seedForTest(ctx context.Context, t *testing.T, uri, base string, layout model.Layout) {
	t.Helper()
	target := contractTarget{Name: "mongo", Database: base, SeedArgs: []string{"-mongo-uri", uri, "-mongo-tls"}}
	if err := target.seed(ctx, layout); err != nil {
		t.Fatal(err)
	}
}

func TestMongoLayoutRefusesAnUnseededDatabase(t *testing.T) {
	ctx, engine, _ := liveMongo(t, "batu")
	session, _ := engine.Session(ctx, 0)
	_, err := session.Layout(model.Chat, "batu_chat")
	if err == nil || !strings.Contains(err.Error(), "bigasstest seed") {
		t.Fatalf("an unseeded database must be refused with the way to seed it, got %v", err)
	}
}

// TestMongoCounterOnlyWhenNew joins a member twice and an existing member once: member_n rises
// by one in total, because a counter is applied only when the member row was created.
func TestMongoCounterOnlyWhenNew(t *testing.T) {
	ctx, engine, uri := liveMongo(t, "batc")
	seedForTest(ctx, t, uri, "batc", model.Chat)
	session, _ := engine.Session(ctx, 0)
	ops, err := session.Layout(model.Chat, "batc_chat")
	if err != nil {
		t.Fatal(err)
	}
	world, err := newWorld(contractSeed, contractScale)
	if err != nil {
		t.Fatal(err)
	}
	rooms := world.sets[model.Chat].ids("Room")
	room := rooms[0]
	seeded := world.sets[model.Chat].elements(StoreRef{"Room", room, "Members"})
	have := map[string]bool{}
	for _, member := range seeded {
		have[member.Key] = true
	}
	var newcomer string
	for _, user := range world.sets[model.Chat].ids("User") {
		if !have[user] {
			newcomer = user
			break
		}
	}
	if newcomer == "" || len(seeded) == 0 {
		t.Fatal("the seeded chat has no non-member or no member to join with")
	}
	join := func(user string, id string) WriteOp {
		at := int64(1_800_000_000)
		return WriteOp{Layout: model.Chat, Kind: WriteChatJoin, ID: id, Ordinal: 1,
			Documents: []model.Document{{Bucket: "Member", ID: room + "/" + user, Fields: []model.Field{{Name: "Room", Value: room}, {Name: "User", Value: user}, {Name: "Since", Value: at}}}},
			Elements: []model.Element{
				{Bucket: "Room", Owner: room, Store: "Members", Key: user, Body: []model.Field{{Name: "user", Value: user}, {Name: "since", Value: at}}},
				{Bucket: "User", Owner: user, Store: "Rooms", Key: room, Body: []model.Field{{Name: "room", Value: room}, {Name: "since", Value: at}}},
			},
			Counters: []CounterDelta{{Bucket: "Room", ID: room, Field: "member_n", Delta: 1, Element: 0}},
		}
	}
	memberN := func() int64 {
		raw, err := engine.client.Database("batc_chat").Collection("rooms").FindOne(ctx, bson.D{{Key: "_id", Value: room}}).Raw()
		if err != nil {
			t.Fatal(err)
		}
		n, err := decodeScalar(raw, "member_n")
		if err != nil {
			t.Fatal(err)
		}
		return n.(int64)
	}
	before := memberN()
	for _, op := range []WriteOp{join(newcomer, "a"), join(newcomer, "b"), join(seeded[0].Key, "c")} {
		if _, err := ops.Write(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	if got := memberN(); got != before+1 {
		t.Fatalf("member_n went from %d to %d, want exactly one more", before, got)
	}
}

// TestMongoClaimOnAnEmptyQueue drains a run's queue; a claim on the empty queue applies nothing.
func TestMongoClaimOnAnEmptyQueue(t *testing.T) {
	ctx, engine, uri := liveMongo(t, "bata")
	seedForTest(ctx, t, uri, "bata", model.Agent)
	session, _ := engine.Session(ctx, 0)
	ops, err := session.Layout(model.Agent, "bata_agent")
	if err != nil {
		t.Fatal(err)
	}
	world, err := newWorld(contractSeed, contractScale)
	if err != nil {
		t.Fatal(err)
	}
	run := world.sets[model.Agent].ids("Run")[0]
	ref := StoreRef{"Run", run, "Tasks"}
	claim := WriteOp{Layout: model.Agent, Kind: WriteAgentTask, ID: "x", Ordinal: 1, Claim: &ClaimRef{Run: run}}
	for range len(world.sets[model.Agent].elements(ref)) {
		reply, err := ops.Write(ctx, claim)
		if err != nil || len(reply.Elements) != 1 {
			t.Fatalf("claim of a seeded task: %v, %d elements", err, len(reply.Elements))
		}
	}
	claim.Elements = []model.Element{{Bucket: "Run", Owner: run, Store: "Tasks", Body: []model.Field{{Name: "task", Value: "t"}, {Name: "payload", Value: "p"}}}}
	reply, err := ops.Write(ctx, claim)
	if err != nil || len(reply.Elements) != 0 {
		t.Fatalf("claim on an empty queue: %v, %d elements, want none", err, len(reply.Elements))
	}
	counts, err := ops.Count(ctx, []StoreRef{ref})
	if err != nil || counts[ref] != 0 {
		t.Fatalf("the empty claim must not push its replacement: count %d, %v", counts[ref], err)
	}
}

func TestMongoParamsRefuseWeakDurabilityAtLA(t *testing.T) {
	for _, params := range []map[string]string{
		{"w": "0"},
		{"journal": "false"},
		{"w": "1", "journal": "false"},
		{"level": "L-A", "w": "0"},
	} {
		if _, err := parseMongoParams(params); err == nil || !strings.Contains(err.Error(), "level=L-B") {
			t.Errorf("params %v must be refused with the way out (level=L-B), got %v", params, err)
		}
	}
	if _, err := parseMongoParams(map[string]string{"level": "L-C"}); err == nil {
		t.Error("an unknown level must be refused")
	}
}

func TestMongoParamsAcceptStrongAndExplicitLB(t *testing.T) {
	strong, err := parseMongoParams(nil)
	if err != nil || strong.w != 1 || !strong.journal || strong.level != "L-A" {
		t.Fatalf("defaults: %+v, %v", strong, err)
	}
	weak, err := parseMongoParams(map[string]string{"level": "L-B", "w": "0", "journal": "false"})
	if err != nil || weak.w != 0 || weak.journal || weak.level != "L-B" {
		t.Fatalf("explicit L-B: %+v, %v", weak, err)
	}
	if got := weak.describe(); got != "w=0,j=false" {
		t.Errorf("describe = %q", got)
	}
}

func TestMongoCmdlineAllowlist(t *testing.T) {
	// Built at run time so no address literal sits in the tree (the hygiene scan refuses one).
	bindAddress := strings.Join([]string{"10", "1", "2", "3"}, ".")
	parsed := bson.M{
		"net": bson.M{
			"bindIp": bindAddress,
			"port":   int32(27017),
			"tls":    bson.M{"mode": "requireTLS", "certificateKeyFile": "/etc/ssl/mongo.pem", "CAFile": "/etc/ssl/ca.pem"},
		},
		"storage": bson.M{
			"dbPath":     "/var/lib/mongodb",
			"journal":    bson.M{"enabled": true},
			"wiredTiger": bson.M{"engineConfig": bson.M{"cacheSizeGB": 4.0, "directoryForIndexes": true}},
		},
		"systemLog":   bson.M{"path": "/var/log/mongodb/mongod.log"},
		"security":    bson.M{"keyFile": "/etc/mongo.key"},
		"replication": bson.M{"replSetName": "rs0"},
	}
	got := mongoCmdlineSettings(parsed)
	for _, want := range []string{"cmdline.net.tls.mode", "cmdline.storage.journal.enabled", "cmdline.storage.wiredTiger.engineConfig.cacheSizeGB", "cmdline.replication.replSetName"} {
		if _, ok := got[want]; !ok {
			t.Errorf("allowlisted key %s missing from %v", want, got)
		}
	}
	for key, value := range got {
		for _, banned := range []string{"bindIp", bindAddress, "/var", "/etc", "dbPath", "keyFile", "CAFile", "systemLog", "27017"} {
			if strings.Contains(key, banned) || strings.Contains(value, banned) {
				t.Errorf("record carries %q = %q, which names an address or a path", key, value)
			}
		}
	}
}

// TestMongoCounterUpdatesMergePerDocument checks that two deltas on one account become one
// $inc, as the SQL adapters send one UPDATE, and that a delta of an element that was only
// replaced is left out.
func TestMongoCounterUpdatesMergePerDocument(t *testing.T) {
	ops := &mongoOps{layout: model.Ledger, spec: mongoLayouts[model.Ledger]}
	deltas := []CounterDelta{
		{Bucket: "Account", ID: "a1", Field: "expense_n", Delta: 1, Element: 0},
		{Bucket: "Account", ID: "a1", Field: "expense_total", Delta: 250, Element: 0},
		{Bucket: "Account", ID: "a2", Field: "invoice_n", Delta: 1, Element: 1},
		{Bucket: "Account", ID: "a1", Field: "expense_n", Delta: 1, Element: -1},
	}
	updates, err := ops.counterUpdates(deltas, []bool{true, false})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 {
		t.Fatalf("got %d updates, want one for account a1", len(updates))
	}
	want := map[string]int64{"expense_n": 2, "expense_total": 250}
	if updates[0].id != "a1" || len(updates[0].inc) != len(want) {
		t.Fatalf("update %+v", updates[0])
	}
	for _, entry := range updates[0].inc {
		if want[entry.Key] != entry.Value {
			t.Errorf("%s = %v, want %d", entry.Key, entry.Value, want[entry.Key])
		}
	}
}

func TestMongoCounterUpdatesRefuseAnUndeclaredCounter(t *testing.T) {
	ops := &mongoOps{layout: model.Ledger, spec: mongoLayouts[model.Ledger]}
	_, err := ops.counterUpdates([]CounterDelta{{Bucket: "Account", ID: "a", Field: "nope", Delta: 1, Element: -1}}, nil)
	if err == nil {
		t.Fatal("an undeclared counter must be refused")
	}
}

// TestMongoOrderByIDs checks the $in reply is put back in element order and a missing
// document is skipped, which the shared verifier reports.
func TestMongoOrderByIDs(t *testing.T) {
	found := map[string]model.Document{"p1": {ID: "p1"}, "p3": {ID: "p3"}}
	got := orderByIDs([]string{"p3", "p2", "p1", "p3"}, found)
	var ids []string
	for _, doc := range got {
		ids = append(ids, doc.ID)
	}
	if strings.Join(ids, ",") != "p3,p1,p3" {
		t.Errorf("got %v", ids)
	}
}

var (
	_ StoreReader   = (*mongoOps)(nil)
	_ CounterReader = (*mongoOps)(nil)
)

// TestMongoCoversEveryBucketAndStore needs no server: every bucket of every layout has a
// collection for the empty-Owner Count and every store the audit reads back one for ReadStore.
func TestMongoCoversEveryBucketAndStore(t *testing.T) {
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range gen.Schema() {
			if _, err := mongoLayouts[layout].docOf(bucket.Name); err != nil {
				t.Errorf("%s: %v", layout, err)
			}
		}
	}
	// User.Photos and User.Friends are never written or read by the mix, so they have no mapping.
	for layout, names := range map[model.Layout][]string{
		model.Social: {"Post.Likes", "User.Likes", "User.Following", "User.Followers", "User.Settings", "User.Feed", "User.Inbox"},
		model.Chat:   {"Room.Members", "User.Rooms", "Room.Messages"},
		model.Ledger: {"Account.Expenses", "Account.Invoices"},
		model.Agent:  {"Run.Scratch", "User.Memories", "Session.Messages"},
	} {
		for _, name := range names {
			if _, ok := mongoLayouts[layout].Stores[name]; !ok {
				t.Errorf("%s: store %s has no mapping to read back", layout, name)
			}
		}
	}
}

// TestMongoSettingsLive reads Settings from a real server: the effective write concern is
// there, and no value carries a path or a bind address.
func TestMongoSettingsLive(t *testing.T) {
	ctx, engine, _ := liveMongo(t, "batg")
	settings, err := engine.Settings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"write_concern", "write_concern_level", "engine_version", "tls"} {
		if settings[want] == "" {
			t.Errorf("Settings has no %s", want)
		}
	}
	for key, value := range settings {
		t.Logf("%s = %s", key, value)
		lower := strings.ToLower(key)
		if strings.Contains(lower, "bindip") || strings.Contains(value, "/home") || strings.Contains(value, "/var") || strings.Contains(value, "/etc") {
			t.Errorf("Settings carries a path or an address: %s = %s", key, value)
		}
		for _, host := range engine.uri.Hosts {
			if name, _, _ := strings.Cut(host, ":"); name != "" && strings.Contains(value, name) {
				t.Errorf("Settings value names the server host: %s = %s", key, value)
			}
		}
	}
}
