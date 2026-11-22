package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestMySQLAdapterRegistered(t *testing.T) {
	if _, ok := adapters["mysql"]; !ok {
		t.Fatal(`engine "mysql" has no adapter`)
	}
}

// TestContractMySQL runs the shared contract against a real MySQL server and skips when
// MYSQL_DSN is unset. MYSQL_DSN is a go-sql-driver DSN (user:password@tcp(host:3306)/), the line
// scripts/certification/engines.sh prints for "uri mysql". The databases it seeds carry a
// time-based name under the social_live_ prefix the engines.sh accounts may create, and
// are dropped afterwards.
func TestContractMySQL(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN is not set; set it to user:password@tcp(host:3306)/ to run against a MySQL server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	// The accounts engines.sh makes may create databases named social_live_*, so the default base
	// sits under that prefix; MYSQL_BASE overrides it for a server with other grants.
	base := os.Getenv("MYSQL_BASE")
	if base == "" {
		base = fmt.Sprintf("social_live_bat%d", time.Now().UnixNano()%1_000_000_000)
	}
	t.Cleanup(func() { dropMySQLDatabases(dsn, base) })
	eng, err := openEngine(ctx, Config{Engine: "mysql", URI: dsn, VUsers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }() // test teardown: the run is over
	runContract(t, contractTarget{Engine: eng, Name: "mysql", Totals: true, Database: base, SeedArgs: []string{"-mysql-dsn", dsn}})
}

// dropMySQLDatabases removes the four layout databases of a contract run.
func dropMySQLDatabases(dsn, base string) {
	target, err := mysql.ParseDSN(dsn)
	if err != nil {
		return
	}
	target.DBName = ""
	admin, err := sql.Open("mysql", target.FormatDSN())
	if err != nil {
		return
	}
	defer func() { _ = admin.Close() }()
	for _, layout := range benchLayouts {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + layoutDatabase(base, layout))
	}
}

func columnIndex(table []string, column string) bool {
	for _, have := range table {
		if have == column {
			return true
		}
	}
	return false
}

// TestMySQLMapsAgreeWithTheModel needs no server: every store of the four layouts the model
// seeds (the heap aside, which the ring client cannot seed either) has a table mapping whose
// columns exist, whose body fields are the model's in order, and whose kind (pushed or keyed)
// is the schema's.
func TestMySQLMapsAgreeWithTheModel(t *testing.T) {
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		first := map[string]model.Element{}
		for unit := range gen.Units() {
			for _, element := range unit.Elements {
				if _, ok := first[element.StoreName()]; !ok {
					first[element.StoreName()] = element
				}
			}
		}
		for name, sm := range mysqlStores[layout] {
			table, ok := mysqlTables[layout][sm.table]
			if !ok {
				t.Errorf("%s %s: table %s is not in mysqlTables", layout, name, sm.table)
				continue
			}
			for _, col := range append([]string{sm.ownerCol, sm.keyCol}, sm.fixed...) {
				if col != "" && !columnIndex(table, col) {
					t.Errorf("%s %s: column %s is not in table %s", layout, name, col, sm.table)
				}
			}
			bucket, store, _ := strings.Cut(name, ".")
			kind, err := KindOf(layout, bucket, store)
			if err != nil {
				t.Errorf("%s %s: %v", layout, name, err)
				continue
			}
			if kind.Keyed() == sm.pushed() {
				t.Errorf("%s %s: the schema says keyed=%v, the mapping is pushed=%v", layout, name, kind.Keyed(), sm.pushed())
			}
			for _, b := range sm.body {
				if !columnIndex(table, b.col) {
					t.Errorf("%s %s: body column %s is not in table %s", layout, name, b.col, sm.table)
				}
			}
			if element, seeded := first[name]; seeded {
				if len(element.Body) != len(sm.body) {
					t.Errorf("%s %s: the model's element has %d fields, the mapping %d", layout, name, len(element.Body), len(sm.body))
					continue
				}
				for i, field := range element.Body {
					if field.Name != sm.body[i].field {
						t.Errorf("%s %s: field %d is %s in the model and %s in the mapping", layout, name, i, field.Name, sm.body[i].field)
					}
				}
			}
		}
		for _, bucket := range gen.Schema() {
			for _, field := range bucket.Fields {
				if kind, err := KindOf(layout, bucket.Name, field.Name); err == nil {
					if _, ok := mysqlStores[layout][bucket.Name+"."+field.Name]; !ok {
						t.Errorf("%s: store %s.%s (kind %d) has no MySQL mapping", layout, bucket.Name, field.Name, kind)
					}
				}
			}
			dm, ok := mysqlDocs[layout][bucket.Name]
			if !ok {
				t.Errorf("%s: bucket %s has no MySQL table", layout, bucket.Name)
				continue
			}
			if _, ok := mysqlTables[layout][dm.table]; !ok {
				t.Errorf("%s: bucket %s maps to unknown table %s", layout, bucket.Name, dm.table)
			}
			if dm.write == nil {
				continue
			}
			for unit := range gen.Documents(bucket.Name) {
				if len(unit.Fields) != len(dm.write.fields) {
					t.Errorf("%s %s: the model's document has %d fields, the mapping %d", layout, bucket.Name, len(unit.Fields), len(dm.write.fields))
					break
				}
				for i, field := range unit.Fields {
					if field.Name != dm.write.fields[i].field {
						t.Errorf("%s %s: field %d is %s in the model and %s in the mapping", layout, bucket.Name, i, field.Name, dm.write.fields[i].field)
					}
				}
				break
			}
		}
	}
}

// TestMySQLPairedStoresNameOneRow checks that both sides of each pair, and the document, give
// the same row identity, so a write naming both inserts the row once.
func TestMySQLPairedStoresNameOneRow(t *testing.T) {
	stores := mysqlStores[model.Social]
	if a, b := stores["User.Following"].rowKey("u1", "u2"), stores["User.Followers"].rowKey("u2", "u1"); a != b {
		t.Errorf("follows: Following and Followers of one edge differ: %q %q", a, b)
	}
	if a, b := stores["Post.Likes"].rowKey("p1", "u2"), stores["User.Likes"].rowKey("u2", "p1"); a != b {
		t.Errorf("likes: the two sides differ: %q %q", a, b)
	}
	chat := mysqlStores[model.Chat]
	if a, b := chat["Room.Members"].rowKey("r1", "u2"), chat["User.Rooms"].rowKey("u2", "r1"); a != b {
		t.Errorf("members: the two sides differ: %q %q", a, b)
	}
}

func TestMySQLTargetRules(t *testing.T) {
	target, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/ignored"})
	if err != nil || target.TLSConfig != "skip-verify" || target.DBName != "" || !target.InterpolateParams {
		t.Fatalf("default: %+v %v", target, err)
	}
	if target, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/?tls=preferred"}); err != nil || target.TLSConfig != "preferred" {
		t.Fatalf("a tls= value of the DSN is kept: %+v %v", target, err)
	}
	if _, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/?tls=false"}); err == nil || !strings.Contains(err.Error(), "tls=off") {
		t.Fatalf("tls=false against tls=on must be refused with the fix: %v", err)
	}
	if target, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/", Params: map[string]string{"tls": "off"}}); err != nil || target.TLSConfig != "" {
		t.Fatalf("tls off: %+v %v", target, err)
	}
	if _, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/", Params: map[string]string{"pool": "9"}}); err == nil {
		t.Fatal("an unknown param was accepted")
	}
	if _, err := mysqlTarget(Config{URI: "bench:pw@tcp(h:3306)/?clientFoundRows=true"}); err == nil {
		t.Fatal("clientFoundRows=true was accepted")
	}
	if _, err := mysqlTarget(Config{}); err == nil {
		t.Fatal("an empty URI was accepted")
	}
}

func TestMySQLDatabaseNames(t *testing.T) {
	if err := validDatabaseName("bat_123_social"); err != nil {
		t.Fatal(err)
	}
	if err := validDatabaseName("bat`; drop"); err == nil {
		t.Fatal("a name with a quote was accepted")
	}
}

// mysqlLive is a seeded server for the tests that look at rows the contract does not read.
type mysqlLive struct {
	ctx  context.Context
	eng  Engine
	db   *sql.DB
	base string
	ops  map[model.Layout]LayoutOps
}

func newMySQLLive(t *testing.T) *mysqlLive {
	t.Helper()
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN is not set; set it to user:password@tcp(host:3306)/ to run against a MySQL server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	base := fmt.Sprintf("social_live_bat%d", time.Now().UnixNano()%1_000_000_000)
	t.Cleanup(func() { dropMySQLDatabases(dsn, base) })
	eng, err := openEngine(ctx, Config{Engine: "mysql", URI: dsn, VUsers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	target := contractTarget{Engine: eng, Name: "mysql", Database: base, SeedArgs: []string{"-mysql-dsn", dsn}}
	live := &mysqlLive{ctx: ctx, eng: eng, base: base, ops: map[model.Layout]LayoutOps{}}
	for _, layout := range benchLayouts {
		if err := target.seed(ctx, layout); err != nil {
			t.Fatal(err)
		}
	}
	session, err := eng.Session(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	for _, layout := range benchLayouts {
		if live.ops[layout], err = session.Layout(layout, layoutDatabase(base, layout)); err != nil {
			t.Fatal(err)
		}
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = ""
	parsed.TLSConfig = "skip-verify"
	if live.db, err = sql.Open("mysql", parsed.FormatDSN()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.db.Close() })
	return live
}

func (l *mysqlLive) number(t *testing.T, layout model.Layout, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := l.db.QueryRowContext(l.ctx, strings.ReplaceAll(query, "DB.", layoutDatabase(l.base, layout)+"."), args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func writeWith(id string, ordinal int64, elements []model.Element, counters []CounterDelta, docs []model.Document) WriteOp {
	return WriteOp{ID: id, Ordinal: ordinal, Elements: elements, Counters: counters, Documents: docs}
}

// TestMySQLWriteTransactions looks at what a write leaves in the rows the contract does not
// read: the counters, which move once per newly created row, and the commit and round trip
// counts of one transaction.
func TestMySQLWriteTransactions(t *testing.T) {
	live := newMySQLLive(t)
	field := func(name string, value any) model.Field { return model.Field{Name: name, Value: value} }

	likes := func() int64 { return live.number(t, model.Social, "SELECT likes_n FROM DB.posts WHERE id = 'p3'") }
	before := likes()
	like := writeWith("w9.1.1", 1, []model.Element{
		{Bucket: "Post", Owner: "p3", Store: "Likes", Key: "u39", Body: []model.Field{field("user", "u39"), field("at", int64(1))}},
		{Bucket: "User", Owner: "u39", Store: "Likes", Key: "p3", Body: []model.Field{field("post", "p3"), field("at", int64(1))}},
	}, []CounterDelta{{Bucket: "Post", ID: "p3", Field: "likes_n", Delta: 1, Element: 0}}, nil)
	existing := live.number(t, model.Social, "SELECT COUNT(*) FROM DB.likes WHERE post_id = 'p3' AND user_id = 'u39'")
	reply, err := live.ops[model.Social].Write(live.ctx, like)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Commits != 1 || reply.RoundTrips != 4 {
		t.Errorf("a like is BEGIN, one row for both sides, one counter update and COMMIT: got %d commits, %d round trips", reply.Commits, reply.RoundTrips)
	}
	wantAfterFirst := before + 1
	if existing == 1 {
		wantAfterFirst = before // u39 already liked p3 in the seed: a replacement is not a new like
	}
	if got := likes(); got != wantAfterFirst {
		t.Errorf("likes_n after the first like is %d, want %d", got, wantAfterFirst)
	}
	if _, err := live.ops[model.Social].Write(live.ctx, like); err != nil {
		t.Fatal(err)
	}
	if got := likes(); got != wantAfterFirst {
		t.Errorf("repeating a like moved likes_n to %d, want %d", got, wantAfterFirst)
	}
	if n := live.number(t, model.Social, "SELECT COUNT(*) FROM DB.likes WHERE post_id = 'p3' AND user_id = 'u39'"); n != 1 {
		t.Errorf("the like pair is %d rows, want 1", n)
	}

	expenseTotal := func() int64 {
		return live.number(t, model.Ledger, "SELECT expense_total FROM DB.accounts WHERE id = 'a1'")
	}
	expenseCount := func() int64 { return live.number(t, model.Ledger, "SELECT expense_n FROM DB.accounts WHERE id = 'a1'") }
	total, count := expenseTotal(), expenseCount()
	expense := writeWith("w9.1.2", 2, []model.Element{{Bucket: "Account", Owner: "a1", Store: "Expenses", Key: "4000000002-w9.1.2",
		Body: []model.Field{field("amount", int64(777)), field("category", "fees"), field("currency", "EUR")}}},
		[]CounterDelta{{Bucket: "Account", ID: "a1", Field: "expense_n", Delta: 1, Element: 0}, {Bucket: "Account", ID: "a1", Field: "expense_total", Delta: 777, Element: 0}}, nil)
	reply, err = live.ops[model.Ledger].Write(live.ctx, expense)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Commits != 1 || reply.RoundTrips != 4 {
		t.Errorf("an expense is BEGIN, the row, one update of both counters and COMMIT: got %d commits, %d round trips", reply.Commits, reply.RoundTrips)
	}
	if expenseTotal() != total+777 || expenseCount() != count+1 {
		t.Errorf("expense counters are %d and %d, want %d and %d", expenseTotal(), expenseCount(), total+777, count+1)
	}
	if _, err := live.ops[model.Ledger].Write(live.ctx, expense); err != nil {
		t.Fatal(err)
	}
	if expenseTotal() != total+777 || expenseCount() != count+1 {
		t.Errorf("repeating a put of the same key moved the counters to %d and %d", expenseTotal(), expenseCount())
	}

	// The agent memory write carries a users.memory_n delta, which the schema declares.
	memories := func() int64 { return live.number(t, model.Agent, "SELECT memory_n FROM DB.users WHERE id = 'u1'") }
	held := memories()
	memory := writeWith("w9.1.3", 3, []model.Element{{Bucket: "User", Owner: "u1", Store: "Memories", Key: "mw9.1.3",
		Body: []model.Field{field("text", "t"), field("vec", make([]byte, 256))}}},
		[]CounterDelta{{Bucket: "User", ID: "u1", Field: "memory_n", Delta: 1, Element: 0}}, nil)
	if _, err := live.ops[model.Agent].Write(live.ctx, memory); err != nil {
		t.Fatal(err)
	}
	if got := memories(); got != held+1 {
		t.Errorf("memory_n is %d after a new memory, want %d", got, held+1)
	}
	if _, err := live.ops[model.Agent].Write(live.ctx, memory); err != nil {
		t.Fatal(err)
	}
	if got := memories(); got != held+1 {
		t.Errorf("replacing the same memory moved memory_n to %d, want %d", got, held+1)
	}

	// A delta on a column the schema does not declare is not applied and is not an error.
	undeclared := memory
	undeclared.Counters = []CounterDelta{{Bucket: "User", ID: "u1", Field: "no_such_counter", Delta: 1, Element: 0}}
	if _, err := live.ops[model.Agent].Write(live.ctx, undeclared); err != nil {
		t.Fatalf("an undeclared counter must be skipped: %v", err)
	}
}

// TestMySQLClaimDrainsAQueue claims every task of one run, with no replacement: the next
// claim finds nothing, applies nothing and commits nothing, and Count sees only ready tasks.
func TestMySQLClaimDrainsAQueue(t *testing.T) {
	live := newMySQLLive(t)
	tasks := StoreRef{"Run", "run4", "Tasks"}
	counted := func() int64 {
		counts, err := live.ops[model.Agent].Count(live.ctx, []StoreRef{tasks})
		if err != nil {
			t.Fatal(err)
		}
		return counts[tasks]
	}
	if got := counted(); got != 10 {
		t.Fatalf("the seeded queue holds %d tasks, want 10", got)
	}
	for i := range 10 {
		reply, err := live.ops[model.Agent].Write(live.ctx, WriteOp{ID: fmt.Sprintf("w9.1.%d", i), Ordinal: int64(i), Claim: &ClaimRef{Run: "run4"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(reply.Elements) != 1 || bodyString(reply.Elements[0], "task") != fmt.Sprintf("t%d", i) {
			t.Fatalf("claim %d returned %+v, want task t%d (oldest first)", i, reply.Elements, i)
		}
		if reply.Commits != 1 {
			t.Fatalf("claim %d committed %d times", i, reply.Commits)
		}
	}
	reply, err := live.ops[model.Agent].Write(live.ctx, WriteOp{ID: "w9.1.99", Ordinal: 99, Claim: &ClaimRef{Run: "run4"},
		Elements: []model.Element{{Bucket: "Run", Owner: "run4", Store: "Tasks", Body: []model.Field{{Name: "task", Value: "tx"}, {Name: "payload", Value: "p"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Elements) != 0 || reply.Commits != 0 {
		t.Errorf("a claim on an empty queue returned %d elements and %d commits", len(reply.Elements), reply.Commits)
	}
	if got := counted(); got != 0 {
		t.Errorf("after draining, Count sees %d ready tasks and the replacement of an empty claim must not be pushed", got)
	}
	if rows := live.number(t, model.Agent, "SELECT COUNT(*) FROM DB.tasks WHERE run_id = 'run4' AND state = 2"); rows != 10 {
		t.Errorf("%d acknowledged rows, want 10", rows)
	}
}

// TestMySQLCounterRowsAreOrdered checks that two writes touching the same two users in
// opposite directions lock their counter rows in the same order, so they cannot deadlock.
func TestMySQLCounterRowsAreOrdered(t *testing.T) {
	declared := map[string]map[string]bool{"users": {"followers_n": true, "following_n": true}}
	lay := newMySQLLayout(model.Social, "x_social", declared)
	order := func(from, to string) []string {
		op := WriteOp{Counters: []CounterDelta{
			{Bucket: "User", ID: from, Field: "following_n", Delta: 1, Element: -1},
			{Bucket: "User", ID: to, Field: "followers_n", Delta: 1, Element: -1},
		}}
		rows, err := lay.counterRows(op, nil)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, row := range rows {
			ids = append(ids, row.id)
		}
		return ids
	}
	a, b := order("u9", "u1"), order("u1", "u9")
	if strings.Join(a, ",") != "u1,u9" || strings.Join(b, ",") != "u1,u9" {
		t.Errorf("counter rows not in id order: %v and %v", a, b)
	}
}

// A deadlock victim (error 1213) is the one thing MySQL asks a client to retry; 128 virtual users
// claiming and refilling the same run's queue hit it. The adapter reruns the whole transaction a
// bounded number of times and counts every rerun in Reply.Retries, which labels the record. Any
// other error is never retried.
func TestMySQLRetriesOnlyDeadlocksAndCountsThem(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	calls := 0
	retries, err := retryDeadlocks(func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("writing Run.Tasks: %w", deadlock)
		}
		return nil
	})
	if err != nil || retries != 2 || calls != 3 {
		t.Errorf("two deadlocks then success: retries %d calls %d err %v", retries, calls, err)
	}
	calls = 0
	retries, err = retryDeadlocks(func() error { calls++; return deadlock })
	if err == nil || calls != mysqlDeadlockAttempts || retries != mysqlDeadlockAttempts-1 || !strings.Contains(err.Error(), "retried") {
		t.Errorf("a deadlock every time must stop after %d attempts and say it retried: retries %d calls %d err %v", mysqlDeadlockAttempts, retries, calls, err)
	}
	calls = 0
	other := errors.New("duplicate key")
	retries, err = retryDeadlocks(func() error { calls++; return other })
	if !errors.Is(err, other) || calls != 1 || retries != 0 {
		t.Errorf("another error is never retried: retries %d calls %d err %v", retries, calls, err)
	}
}

var (
	_ StoreReader   = (*mysqlOps)(nil)
	_ CounterReader = (*mysqlOps)(nil)
)

// TestMySQLCoversEveryBucketAndStore needs no server: every bucket of every layout has a table
// for Count with an empty Owner and Store, and every store field has a mapping for ReadStore.
func TestMySQLCoversEveryBucketAndStore(t *testing.T) {
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range gen.Schema() {
			if _, ok := mysqlDocs[layout][bucket.Name]; !ok {
				t.Errorf("%s: bucket %s has no table for a document count", layout, bucket.Name)
			}
			for _, field := range bucket.Fields {
				if _, err := KindOf(layout, bucket.Name, field.Name); err != nil {
					continue
				}
				if _, ok := mysqlStores[layout][bucket.Name+"."+field.Name]; !ok {
					t.Errorf("%s: store %s.%s has no mapping to read back", layout, bucket.Name, field.Name)
				}
			}
		}
	}
}

func TestMySQLCounterColumns(t *testing.T) {
	got := mysqlCounterColumns(model.Ledger, "accounts")
	if strings.Join(got, ",") != "expense_n,expense_total,invoice_n,invoice_total" {
		t.Errorf("accounts counters = %v", got)
	}
	if got := mysqlCounterColumns(model.Social, "users"); strings.Join(got, ",") != "followers_n,following_n" {
		t.Errorf("users counters = %v", got)
	}
}
