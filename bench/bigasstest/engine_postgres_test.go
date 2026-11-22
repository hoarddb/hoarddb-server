package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestContractPostgres runs the shared contract against a real PostgreSQL server. It skips
// when POSTGRES_DSN is unset. The DSN names a role that may create databases; the four layout
// databases it makes carry a time-based name and are dropped afterwards.
func TestContractPostgres(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set; set it to postgres://user:password@host:5432/postgres?sslmode=require to run against a PostgreSQL server")
	}
	ctx := context.Background()
	base := fmt.Sprintf("bat_pg_%d", time.Now().UnixNano()%1_000_000_000)
	t.Cleanup(func() { dropLayoutDatabases(t, dsn, base) })
	engine, err := openEngine(ctx, Config{Engine: "postgres", URI: dsn, VUsers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }() // test teardown: the run is over
	runContract(t, contractTarget{
		Engine:   engine,
		Name:     "postgres",
		Totals:   true,
		Database: base,
		SeedArgs: []string{"-postgres-dsn", dsn, fmt.Sprintf("-postgres-tls=%t", !strings.Contains(dsn, "sslmode=disable"))},
	})
}

// dropLayoutDatabases removes what a contract run created.
func dropLayoutDatabases(t *testing.T, dsn, base string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Logf("cleanup: connecting to drop %s_*: %v", base, err)
		return
	}
	defer func() { _ = conn.Close(ctx) }() // test cleanup connection
	for _, layout := range benchLayouts {
		name := layoutDatabase(base, layout)
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Logf("cleanup: dropping %s: %v", name, err)
		}
	}
}

// seededPostgres opens the adapter, seeds the four layouts at the contract's scale and
// returns the per-layout operations and the engine. The databases are dropped afterwards.
func seededPostgres(t *testing.T) (*pgEngine, *world, map[model.Layout]LayoutOps) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	base := fmt.Sprintf("bat_pg_%d", time.Now().UnixNano()%1_000_000_000)
	t.Cleanup(func() { dropLayoutDatabases(t, dsn, base) })
	opened, err := openEngine(ctx, Config{Engine: "postgres", URI: dsn, VUsers: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	target := contractTarget{Engine: opened, Name: "postgres", Database: base,
		SeedArgs: []string{"-postgres-dsn", dsn, fmt.Sprintf("-postgres-tls=%t", !strings.Contains(dsn, "sslmode=disable"))}}
	for _, layout := range benchLayouts {
		if err := target.seed(ctx, layout); err != nil {
			t.Fatal(err)
		}
	}
	w, err := newWorld(contractSeed, contractScale)
	if err != nil {
		t.Fatal(err)
	}
	session, err := opened.Session(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	ops := map[model.Layout]LayoutOps{}
	for _, layout := range benchLayouts {
		if ops[layout], err = session.Layout(layout, layoutDatabase(base, layout)); err != nil {
			t.Fatal(err)
		}
	}
	return opened.(*pgEngine), w, ops
}

// scalar runs a query that returns one number.
func scalar(t *testing.T, ops LayoutOps, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := ops.(*pgLayout).db.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// TestPostgresCountersMatchRows: after every kind of write, each denormalised counter the
// writes touch equals what its rows say. The contract does not look at counters.
func TestPostgresCountersMatchRows(t *testing.T) {
	_, w, ops := seededPostgres(t)
	ctx := context.Background()
	for layout, kinds := range contractWrites {
		for _, kind := range kinds {
			mix, err := newMix(benchLayouts, string(kind))
			if err != nil {
				t.Fatal(err)
			}
			planner := newPlanner(w, mix, 1, 1, 7)
			for range 25 {
				req := planner.next()
				reply, err := ops[layout].Write(ctx, *req.Write)
				if err != nil {
					t.Fatalf("%s: %v", kind, err)
				}
				if reply.Commits == 0 && (req.Write.Claim == nil || len(reply.Elements) > 0) {
					t.Fatalf("%s: a write is one transaction, Commits is %d", kind, reply.Commits)
				}
			}
		}
	}
	mismatch := func(what string, got, want int64) {
		t.Helper()
		if got != want {
			t.Errorf("%s: counter %d, rows %d", what, got, want)
		}
	}
	social := ops[model.Social]
	mismatch("posts.likes_n", scalar(t, social, `SELECT coalesce(sum(likes_n), 0) FROM posts`), scalar(t, social, `SELECT count(*) FROM likes`))
	mismatch("posts.comments_n", scalar(t, social, `SELECT coalesce(sum(comments_n), 0) FROM posts`), scalar(t, social, `SELECT count(*) FROM comments`))
	mismatch("users.followers_n", scalar(t, social, `SELECT sum(followers_n) FROM users`), scalar(t, social, `SELECT count(*) FROM follows`))
	mismatch("users.following_n", scalar(t, social, `SELECT sum(following_n) FROM users`), scalar(t, social, `SELECT count(*) FROM follows`))
	if bad := scalar(t, social, `SELECT count(*) FROM posts p WHERE p.likes_n <> (SELECT count(*) FROM likes l WHERE l.post_id = p.id) OR p.comments_n <> (SELECT count(*) FROM comments c WHERE c.post_id = p.id)`); bad != 0 {
		t.Errorf("%d posts whose counters disagree with their rows", bad)
	}
	if bad := scalar(t, social, `SELECT count(*) FROM users u WHERE u.followers_n <> (SELECT count(*) FROM follows f WHERE f.followee_id = u.id) OR u.following_n <> (SELECT count(*) FROM follows f WHERE f.follower_id = u.id)`); bad != 0 {
		t.Errorf("%d users whose follow counters disagree with their rows", bad)
	}
	chat := ops[model.Chat]
	if bad := scalar(t, chat, `SELECT count(*) FROM rooms r WHERE r.member_n <> (SELECT count(*) FROM members m WHERE m.room_id = r.id) OR r.message_n <> (SELECT count(*) FROM messages x WHERE x.room_id = r.id)`); bad != 0 {
		t.Errorf("%d rooms whose counters disagree with their rows", bad)
	}
	ledger := ops[model.Ledger]
	if bad := scalar(t, ledger, `SELECT count(*) FROM accounts a WHERE a.expense_n <> (SELECT count(*) FROM expenses e WHERE e.account_id = a.id) OR a.expense_total <> (SELECT coalesce(sum(amount), 0) FROM expenses e WHERE e.account_id = a.id) OR a.invoice_n <> (SELECT count(*) FROM invoices i WHERE i.account_id = a.id) OR a.invoice_total <> (SELECT coalesce(sum(amount), 0) FROM invoices i WHERE i.account_id = a.id)`); bad != 0 {
		t.Errorf("%d accounts whose counters disagree with their rows", bad)
	}
	agent := ops[model.Agent]
	if bad := scalar(t, agent, `SELECT count(*) FROM sessions s WHERE s.message_n <> (SELECT count(*) FROM session_messages m WHERE m.session_id = s.id)`); bad != 0 {
		t.Errorf("%d sessions whose message counter disagrees with their rows", bad)
	}
	if bad := scalar(t, agent, `SELECT count(*) FROM users u WHERE u.memory_n <> (SELECT count(*) FROM memories m WHERE m.user_id = u.id)`); bad != 0 {
		t.Errorf("%d users whose memory counter disagrees with their rows", bad)
	}
}

// TestPostgresClaimEmptiesAndReturnsNothing: claims take tasks oldest first, each task once,
// and a claim on an empty queue applies nothing and returns no element.
func TestPostgresClaimEmptiesAndReturnsNothing(t *testing.T) {
	_, w, ops := seededPostgres(t)
	ctx := context.Background()
	run := w.sets[model.Agent].ids("Run")[0]
	seeded := w.sets[model.Agent].elements(StoreRef{"Run", run, "Tasks"})
	for i, want := range seeded {
		reply, err := ops[model.Agent].Write(ctx, WriteOp{Layout: model.Agent, Kind: WriteAgentTask, ID: "c", Ordinal: int64(i), Claim: &ClaimRef{Run: run}})
		if err != nil {
			t.Fatal(err)
		}
		if len(reply.Elements) != 1 || canonElement(reply.Elements[0]) != canonElement(want) {
			t.Fatalf("claim %d returned %v, want task %s", i, reply.Elements, canonElement(want))
		}
	}
	reply, err := ops[model.Agent].Write(ctx, WriteOp{Layout: model.Agent, Kind: WriteAgentTask, ID: "c", Ordinal: 99, Claim: &ClaimRef{Run: run}})
	if err != nil || len(reply.Elements) != 0 || reply.Commits != 0 {
		t.Fatalf("a claim on an empty queue must return no element and commit nothing: %+v %v", reply, err)
	}
	counts, err := ops[model.Agent].Count(ctx, []StoreRef{{"Run", run, "Tasks"}})
	if err != nil || counts[StoreRef{"Run", run, "Tasks"}] != 0 {
		t.Fatalf("the acknowledged tasks must not count: %v %v", counts, err)
	}
}

// TestPostgresConcurrentWritesDoNotDeadlock: many writers on the same few counter rows all
// succeed, because counter rows are updated in a fixed order.
func TestPostgresConcurrentWritesDoNotDeadlock(t *testing.T) {
	_, w, ops := seededPostgres(t)
	ctx := context.Background()
	users := w.sets[model.Social].ids("User")[:3]
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range 20 {
				from, to := users[(worker+n)%3], users[(worker+n+1)%3]
				op := WriteOp{Layout: model.Social, Kind: WriteSocialFollow, ID: fmt.Sprintf("f%d.%d", worker, n), Ordinal: int64(worker<<10 | n),
					Documents: []model.Document{{Bucket: "Follow", ID: from + ">" + to, Fields: []model.Field{{Name: "From", Value: from}, {Name: "To", Value: to}, {Name: "At", Value: int64(n)}}}},
					Elements: []model.Element{
						{Bucket: "User", Owner: from, Store: "Following", Key: to, Body: []model.Field{{Name: "user", Value: to}, {Name: "at", Value: int64(n)}}},
						{Bucket: "User", Owner: to, Store: "Followers", Key: from, Body: []model.Field{{Name: "user", Value: from}, {Name: "at", Value: int64(n)}}},
					},
					Counters: []CounterDelta{{Bucket: "User", ID: from, Field: "following_n", Delta: 1, Element: 0}, {Bucket: "User", ID: to, Field: "followers_n", Delta: 1, Element: 1}}}
				if _, err := ops[model.Social].Write(ctx, op); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestPostgresOpenRefusesBadConfig(t *testing.T) {
	ctx := context.Background()
	if _, err := openEngine(ctx, Config{Engine: "postgres"}); err == nil || !strings.Contains(err.Error(), "engines.sh uri postgres") {
		t.Errorf("an empty -uri must say where to get one, got %v", err)
	}
	if _, err := openEngine(ctx, Config{Engine: "postgres", URI: "postgres://x@127.0.0.1:1/x", Params: map[string]string{"colour": "red"}}); err == nil || !strings.Contains(err.Error(), "colour") {
		t.Errorf("an unknown param must be named, got %v", err)
	}
	if _, err := openEngine(ctx, Config{Engine: "postgres", URI: "postgres://x@127.0.0.1:1/x", Params: map[string]string{"pool_max": "0"}}); err == nil {
		t.Error("pool_max=0 must be refused")
	}
}

// TestPostgresSettingsAllowlist keeps addresses, paths and access rules out of the record,
// and keeps the durability facts the comparison needs.
func TestPostgresSettingsAllowlist(t *testing.T) {
	allowed := map[string]bool{}
	for _, name := range pgRecordedSettings {
		allowed[name] = true
		for _, banned := range []string{"listen", "file", "directory", "hba", "ident", "socket", "password_", "ssl_key", "ssl_cert", "ssl_ca", "ssl_crl", "ssl_dh", "port", "host"} {
			if strings.Contains(name, banned) {
				t.Errorf("setting %q names an address, a path or an access rule (%q)", name, banned)
			}
		}
	}
	for _, need := range []string{"fsync", "synchronous_commit", "full_page_writes", "wal_sync_method", "shared_buffers", "max_connections", "checkpoint_timeout", "ssl"} {
		if !allowed[need] {
			t.Errorf("the allowlist lacks %q", need)
		}
	}
}

var (
	_ StoreReader   = (*pgLayout)(nil)
	_ CounterReader = (*pgLayout)(nil)
)

// TestPostgresCoversEveryBucketAndStore needs no server: every bucket has a table for the
// empty-Owner Count, and every store the audit reads back is mapped.
func TestPostgresCoversEveryBucketAndStore(t *testing.T) {
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range gen.Schema() {
			if _, ok := pgMaps[layout].docs[bucket.Name]; !ok {
				t.Errorf("%s: bucket %s has no table for a document count", layout, bucket.Name)
			}
		}
	}
	for layout, names := range map[model.Layout][]string{
		model.Social: {"Post.Likes", "User.Likes", "User.Following", "User.Followers", "User.Settings"},
		model.Chat:   {"Room.Members", "User.Rooms"},
		model.Ledger: {"Account.Expenses", "Account.Invoices"},
		model.Agent:  {"Run.Scratch", "User.Memories"},
	} {
		for _, name := range names {
			if _, ok := pgMaps[layout].stores[name]; !ok {
				t.Errorf("%s: store %s has no mapping to read back", layout, name)
			}
		}
	}
}

// TestPostgresLimitZeroReadsEverything keeps "LIMIT 0" out of the feed and member queries.
func TestPostgresLimitZeroReadsEverything(t *testing.T) {
	for _, query := range []string{pgFeedSQL, pgRoomMembersSQL} {
		if !strings.Contains(query, "NULLIF($2") {
			t.Errorf("query does not treat limit 0 as no limit: %s", query)
		}
	}
}

// TestPostgresSettingsLive reads Settings from a real server: every allowlisted name that this
// server version has is there, and no listen address or path is.
func TestPostgresSettingsLive(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	engine, err := openEngine(ctx, Config{Engine: "postgres", URI: dsn, VUsers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }() // test teardown: the run is over
	settings, err := engine.Settings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range pgRecordedSettings {
		if _, ok := settings["pg."+name]; !ok {
			t.Errorf("allowlisted setting %s is not in this server's pg_settings", name)
		}
	}
	for key, value := range settings {
		t.Logf("%s = %s", key, value)
		if strings.Contains(key, "listen") || strings.Contains(key, "_file") || strings.Contains(key, "data_directory") {
			t.Errorf("Settings carries %s", key)
		}
	}
	if settings["pool_max_per_database"] == "" || settings["pool_max_source"] == "" {
		t.Error("Settings lacks the effective pool cap")
	}
}
