package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// This file maps the stores and documents of the four layouts onto the PostgreSQL tables the
// scenarios binary seeds (bench/scenarios/schema.go). That mapping is package main there, so
// the table and column names are repeated here and checked against the live schema when a
// database is opened (checkPostgresSchema).

// Column types the adapter reads: the model has text, integers and the 256-byte vector.
const (
	pgText  byte = 's'
	pgInt   byte = 'i'
	pgBytes byte = 'b'
)

// pgBody ties a field of an element body (or of a document) to the column that holds it.
type pgBody struct {
	field, col string
	kind       byte
}

func pgS(field, col string) pgBody { return pgBody{field, col, pgText} }
func pgN(field, col string) pgBody { return pgBody{field, col, pgInt} }
func pgB(field, col string) pgBody { return pgBody{field, col, pgBytes} }

// pgStore is the table that holds one store of one bucket. Several stores can share a table:
// the SQL schema keeps one row for a pair of stores (Followers and Following, Post.Likes and
// User.Likes, Room.Members and User.Rooms) and indexes it both ways.
type pgStore struct {
	table string
	// ownerCol holds the owner document id. keyCol holds the element key of a keyed store;
	// it is empty for a fifo or lifo store, whose rows are ordered by the seq column.
	ownerCol, keyCol string
	// pk lists the primary key columns, the conflict target of a keyed put.
	pk []string
	// body lists the fields of the element body in model order. A field whose column is the
	// key column or the owner column is derived from the element key or owner.
	body []pgBody
	// extra are columns the schema needs and the model has no field for, written as literals.
	extra []pgFixed
	// live is the filter that selects the elements the store still holds, when the table
	// also keeps rows that were taken out (acknowledged tasks).
	live string
}

// pgFixed is a column written with a constant.
type pgFixed struct {
	col   string
	value int64
}

func (s pgStore) seqKeyed() bool { return s.keyCol == "" }

// pgDoc is the table that holds the documents of one bucket.
type pgDoc struct {
	table string
	// idCol holds the document id; it is empty for a document whose id is made of its
	// fields (Follow, Member), which are upserted.
	idCol string
	pk    []string
	// fields lists the document fields in model order.
	fields []pgBody
	// counters are the denormalised counter columns, written 0 for a new document.
	counters []string
	upsert   bool
}

// pgLayoutMap is every store and document of one layout. A store is named "Bucket.Store".
type pgLayoutMap struct {
	stores map[string]pgStore
	docs   map[string]pgDoc
}

func keyedStore(table, owner, key string, pk []string, body ...pgBody) pgStore {
	return pgStore{table: table, ownerCol: owner, keyCol: key, pk: pk, body: body}
}

func seqStore(table, owner string, body ...pgBody) pgStore {
	return pgStore{table: table, ownerCol: owner, pk: []string{owner, "seq"}, body: body}
}

var pgMaps = map[model.Layout]pgLayoutMap{
	model.Social: {
		stores: map[string]pgStore{
			"User.Feed":          seqStore("feed", "user_id", pgS("post", "post_id"), pgN("at", "at")),
			"User.Inbox":         seqStore("inbox", "user_id", pgS("post", "post_id"), pgS("author", "author"), pgN("at", "at")),
			"User.Notifications": seqStore("notifications", "user_id", pgS("kind", "kind"), pgS("actor", "actor"), pgS("post", "post_id"), pgN("at", "at")),
			"User.Photos":        seqStore("photos", "user_id", pgS("name", "name"), pgN("bytes", "bytes")),
			"User.Friends":       keyedStore("friends", "user_id", "friend_id", []string{"user_id", "friend_id"}, pgS("user", "friend_id"), pgN("since", "since")),
			"User.Following":     keyedStore("follows", "follower_id", "followee_id", []string{"follower_id", "followee_id"}, pgS("user", "followee_id"), pgN("at", "at")),
			"User.Followers":     keyedStore("follows", "followee_id", "follower_id", []string{"follower_id", "followee_id"}, pgS("user", "follower_id"), pgN("at", "at")),
			"User.Likes":         keyedStore("likes", "user_id", "post_id", []string{"post_id", "user_id"}, pgS("post", "post_id"), pgN("at", "at")),
			"User.Settings":      keyedStore("settings", "user_id", "setting", []string{"user_id", "setting"}, pgS("value", "value")),
			"Post.Comments":      seqStore("comments", "post_id", pgS("author", "author"), pgS("text", "text"), pgN("at", "at")),
			"Post.Likes":         keyedStore("likes", "post_id", "user_id", []string{"post_id", "user_id"}, pgS("user", "user_id"), pgN("at", "at")),
			"Group.Members":      keyedStore("group_members", "group_id", "user_id", []string{"group_id", "user_id"}, pgS("user", "user_id"), pgS("role", "role")),
			"Group.Wall":         seqStore("wall", "group_id", pgS("author", "author"), pgS("text", "text"), pgN("at", "at")),
		},
		docs: map[string]pgDoc{
			"User": {table: "users", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Name", "name"), pgS("Handle", "handle"), pgN("JoinedAt", "joined_at"), pgS("Bio", "bio")},
				counters: []string{"followers_n", "following_n"}},
			"Post": {table: "posts", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Author", "author"), pgS("Text", "text"), pgN("At", "at")},
				counters: []string{"likes_n", "comments_n"}},
			"Group": {table: "social_groups", idCol: "id", pk: []string{"id"},
				fields: []pgBody{pgS("Name", "name"), pgS("Owner", "owner")}},
			"Follow": {table: "follows", pk: []string{"follower_id", "followee_id"}, upsert: true,
				fields: []pgBody{pgS("From", "follower_id"), pgS("To", "followee_id"), pgN("At", "at")}},
		},
	},
	model.Chat: {
		stores: map[string]pgStore{
			"Room.Messages": seqStore("messages", "room_id", pgS("author", "author"), pgS("text", "text"), pgN("at", "at")),
			"Room.Members":  keyedStore("members", "room_id", "user_id", []string{"room_id", "user_id"}, pgS("user", "user_id"), pgN("since", "since")),
			"User.Rooms":    keyedStore("members", "user_id", "room_id", []string{"room_id", "user_id"}, pgS("room", "room_id"), pgN("since", "since")),
		},
		docs: map[string]pgDoc{
			"Room": {table: "rooms", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Name", "name")},
				counters: []string{"member_n", "message_n"}},
			"User": {table: "users", idCol: "id", pk: []string{"id"},
				fields: []pgBody{pgS("Name", "name")}},
			"Member": {table: "members", pk: []string{"room_id", "user_id"}, upsert: true,
				fields: []pgBody{pgS("Room", "room_id"), pgS("User", "user_id"), pgN("Since", "since")}},
		},
	},
	model.Ledger: {
		stores: map[string]pgStore{
			"Account.Expenses": keyedStore("expenses", "account_id", "expense_key", []string{"account_id", "expense_key"},
				pgN("amount", "amount"), pgS("category", "category"), pgS("currency", "currency")),
			"Account.Invoices": keyedStore("invoices", "account_id", "number", []string{"account_id", "number"},
				pgS("number", "number"), pgN("amount", "amount"), pgS("customer", "customer"), pgN("due", "due")),
		},
		docs: map[string]pgDoc{
			"Account": {table: "accounts", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Name", "name"), pgS("Currency", "currency")},
				counters: []string{"expense_n", "expense_total", "invoice_n", "invoice_total"}},
		},
	},
	model.Agent: {
		stores: map[string]pgStore{
			"Session.Messages": seqStore("session_messages", "session_id", pgS("role", "role"), pgS("text", "text"), pgN("at", "at")),
			"Run.Scratch":      keyedStore("scratch", "run_id", "scratch_key", []string{"run_id", "scratch_key"}, pgS("value", "value")),
			"Run.Tasks": func() pgStore {
				s := seqStore("tasks", "run_id", pgS("task", "task"), pgS("payload", "payload"))
				// state 0 is ready, 1 leased, 2 acknowledged: a store holds the tasks not yet acknowledged.
				s.extra = []pgFixed{{"state", 0}, {"lease_until", 0}}
				s.live = "state = 0"
				return s
			}(),
			"User.Memories": keyedStore("memories", "user_id", "memory_id", []string{"user_id", "memory_id"}, pgS("text", "text"), pgB("vec", "vec")),
		},
		docs: map[string]pgDoc{
			"User": {table: "users", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Name", "name")},
				counters: []string{"memory_n"}},
			"Session": {table: "sessions", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Owner", "owner")},
				counters: []string{"message_n"}},
			"Run": {table: "runs", idCol: "id", pk: []string{"id"},
				fields:   []pgBody{pgS("Session", "session")},
				counters: []string{"task_n"}},
		},
	},
}

// bodyCols lists the columns a store reads or writes besides the owner and the key, once each, in body order.
func (s pgStore) bodyCols() []pgBody {
	var out []pgBody
	for _, b := range s.body {
		if b.col == s.keyCol || b.col == s.ownerCol {
			continue
		}
		out = append(out, b)
	}
	return out
}

// columns is every column the adapter uses of the store's table.
func (s pgStore) columns() []string {
	cols := []string{s.ownerCol}
	if s.seqKeyed() {
		cols = append(cols, "seq")
	} else {
		cols = append(cols, s.keyCol)
	}
	for _, b := range s.bodyCols() {
		cols = append(cols, b.col)
	}
	for _, f := range s.extra {
		cols = append(cols, f.col)
	}
	return cols
}

// columns is every column the adapter uses of the document's table.
func (d pgDoc) columns() []string {
	var cols []string
	if d.idCol != "" {
		cols = append(cols, d.idCol)
	}
	for _, f := range d.fields {
		cols = append(cols, f.col)
	}
	return append(cols, d.counters...)
}

// expectedColumns is the column set the adapter needs of each table of a layout. Tables that
// several stores and documents share must be asked for the same columns.
func (m pgLayoutMap) expectedColumns() (map[string][]string, error) {
	out := map[string][]string{}
	add := func(table string, cols []string) error {
		sorted := slices.Clone(cols)
		slices.Sort(sorted)
		if prev, ok := out[table]; ok && !slices.Equal(prev, sorted) {
			return fmt.Errorf("table %s is mapped with two different column sets: %v and %v", table, prev, sorted)
		}
		out[table] = sorted
		return nil
	}
	for name, s := range m.stores {
		if err := add(s.table, s.columns()); err != nil {
			return nil, fmt.Errorf("store %s: %w", name, err)
		}
	}
	for name, d := range m.docs {
		if err := add(d.table, d.columns()); err != nil {
			return nil, fmt.Errorf("document %s: %w", name, err)
		}
	}
	return out, nil
}

// pgIdent quotes an identifier. The seeded names are lower case and need no quotes except
// that several (at, text, value, number) are words PostgreSQL treats specially in places.
func pgIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// pgPlaceholders renders $first..$(first+n-1) joined by commas.
func pgPlaceholders(first, n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", first+i)
	}
	return strings.Join(parts, ", ")
}

func pgIdents(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pgIdent(c)
	}
	return strings.Join(quoted, ", ")
}
