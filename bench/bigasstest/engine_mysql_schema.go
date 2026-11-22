package main

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// colKind is how a column is scanned and what Go type its value has in a model.Field.
type colKind int

const (
	kindText colKind = iota
	kindInt
	kindBytes
)

// bodyCol maps one field of an element or document body to a column.
type bodyCol struct {
	field, col string
	kind       colKind
}

// storeMap says where one store of a layout lives in the seeded MySQL tables. A pushed store
// (fifo, lifo) has no keyCol and is ordered by seq; a keyed store (hash, btree) is ordered by
// its key column. A paired store is one side of a table that holds both sides of a pair
// (follows, likes, members); flip says the store's owner column is the second column of the
// table's primary key, so that both sides name the same row.
type storeMap struct {
	table    string
	ownerCol string
	keyCol   string
	flip     bool
	body     []bodyCol
	// fixed are columns the schema needs that no body field supplies, written as literals.
	fixed []string
	// liveOnly restricts Count to rows still in play (tasks: state 0, a claimed task stays as a row).
	liveOnly string
}

func (s storeMap) pushed() bool { return s.keyCol == "" }

// rowKey identifies the table row an element names, the same for both sides of a pair.
func (s storeMap) rowKey(owner, key string) string {
	if s.flip {
		return s.table + "\x00" + key + "\x00" + owner
	}
	return s.table + "\x00" + owner + "\x00" + key
}

// docMap says where the documents of one bucket live.
type docMap struct {
	table string
	idCol string
	// write is set for the documents a write operation inserts.
	write *docWrite
}

// docWrite maps the fields of a written document. pk lists the primary key columns when the
// document has no id column (a follow edge or a membership row), in key order, with the field
// each takes. zero are counter columns a new row starts at 0.
type docWrite struct {
	fields []bodyCol
	pk     []string
	zero   []string
	upsert []string // columns replaced when the row exists; none means an existing row is left alone
}

func text(field, col string) bodyCol { return bodyCol{field, col, kindText} }
func num(field, col string) bodyCol  { return bodyCol{field, col, kindInt} }
func blob(field, col string) bodyCol { return bodyCol{field, col, kindBytes} }

// mysqlTables are the tables of the four layouts and their columns in order, as
// bench/scenarios/schema.go creates them. The adapter cannot import that package (it is
// package main), so the names are repeated here and checked against the live schema when a
// session opens a layout.
var mysqlTables = map[model.Layout]map[string][]string{
	model.Social: {
		"users":         {"id", "name", "handle", "joined_at", "bio", "followers_n", "following_n"},
		"posts":         {"id", "author", "text", "at", "likes_n", "comments_n"},
		"social_groups": {"id", "name", "owner"},
		"follows":       {"follower_id", "followee_id", "at"},
		"feed":          {"user_id", "seq", "post_id", "at"},
		"inbox":         {"user_id", "seq", "post_id", "author", "at"},
		"notifications": {"user_id", "seq", "kind", "actor", "post_id", "at"},
		"photos":        {"user_id", "seq", "name", "bytes"},
		"friends":       {"user_id", "friend_id", "since"},
		"likes":         {"post_id", "user_id", "at"},
		"settings":      {"user_id", "setting", "value"},
		"drafts":        {"user_id", "draft_id", "priority", "text"},
		"comments":      {"post_id", "seq", "author", "text", "at"},
		"group_members": {"group_id", "user_id", "role"},
		"wall":          {"group_id", "seq", "author", "text", "at"},
	},
	model.Chat: {
		"rooms":    {"id", "name", "member_n", "message_n"},
		"users":    {"id", "name"},
		"members":  {"room_id", "user_id", "since"},
		"messages": {"room_id", "seq", "author", "text", "at"},
	},
	model.Ledger: {
		"accounts": {"id", "name", "currency", "expense_n", "expense_total", "invoice_n", "invoice_total"},
		"expenses": {"account_id", "expense_key", "amount", "category", "currency"},
		"invoices": {"account_id", "number", "amount", "customer", "due"},
	},
	model.Agent: {
		"users":            {"id", "name", "memory_n"},
		"sessions":         {"id", "owner", "message_n"},
		"session_messages": {"session_id", "seq", "role", "text", "at"},
		"runs":             {"id", "session", "task_n"},
		"scratch":          {"run_id", "scratch_key", "value"},
		"tasks":            {"run_id", "seq", "task", "payload", "state", "lease_until"},
		"memories":         {"user_id", "memory_id", "text", "vec"},
	},
}

// mysqlIndexes are the secondary keys the reads rely on, checked when a layout opens.
var mysqlIndexes = map[model.Layout]map[string]string{
	model.Social: {"follows": "follows_by_followee"},
	model.Agent:  {"tasks": "tasks_ready"},
}

// mysqlStores maps "Bucket.Store" of each layout to its table.
var mysqlStores = map[model.Layout]map[string]storeMap{
	model.Social: {
		"User.Feed":          {table: "feed", ownerCol: "user_id", body: []bodyCol{text("post", "post_id"), num("at", "at")}},
		"User.Inbox":         {table: "inbox", ownerCol: "user_id", body: []bodyCol{text("post", "post_id"), text("author", "author"), num("at", "at")}},
		"User.Notifications": {table: "notifications", ownerCol: "user_id", body: []bodyCol{text("kind", "kind"), text("actor", "actor"), text("post", "post_id"), num("at", "at")}},
		"User.Photos":        {table: "photos", ownerCol: "user_id", body: []bodyCol{text("name", "name"), num("bytes", "bytes")}},
		"User.Friends":       {table: "friends", ownerCol: "user_id", keyCol: "friend_id", body: []bodyCol{text("user", "friend_id"), num("since", "since")}},
		"User.Settings":      {table: "settings", ownerCol: "user_id", keyCol: "setting", body: []bodyCol{text("value", "value")}},
		"User.Following":     {table: "follows", ownerCol: "follower_id", keyCol: "followee_id", body: []bodyCol{text("user", "followee_id"), num("at", "at")}},
		"User.Followers":     {table: "follows", ownerCol: "followee_id", keyCol: "follower_id", flip: true, body: []bodyCol{text("user", "follower_id"), num("at", "at")}},
		"User.Likes":         {table: "likes", ownerCol: "user_id", keyCol: "post_id", flip: true, body: []bodyCol{text("post", "post_id"), num("at", "at")}},
		"Post.Likes":         {table: "likes", ownerCol: "post_id", keyCol: "user_id", body: []bodyCol{text("user", "user_id"), num("at", "at")}},
		"Post.Comments":      {table: "comments", ownerCol: "post_id", body: []bodyCol{text("author", "author"), text("text", "text"), num("at", "at")}},
		"Group.Members":      {table: "group_members", ownerCol: "group_id", keyCol: "user_id", body: []bodyCol{text("user", "user_id"), text("role", "role")}},
		"Group.Wall":         {table: "wall", ownerCol: "group_id", body: []bodyCol{text("author", "author"), text("text", "text"), num("at", "at")}},
	},
	model.Chat: {
		"Room.Messages": {table: "messages", ownerCol: "room_id", body: []bodyCol{text("author", "author"), text("text", "text"), num("at", "at")}},
		"Room.Members":  {table: "members", ownerCol: "room_id", keyCol: "user_id", body: []bodyCol{text("user", "user_id"), num("since", "since")}},
		"User.Rooms":    {table: "members", ownerCol: "user_id", keyCol: "room_id", flip: true, body: []bodyCol{text("room", "room_id"), num("since", "since")}},
	},
	model.Ledger: {
		"Account.Expenses": {table: "expenses", ownerCol: "account_id", keyCol: "expense_key", body: []bodyCol{num("amount", "amount"), text("category", "category"), text("currency", "currency")}},
		"Account.Invoices": {table: "invoices", ownerCol: "account_id", keyCol: "number", body: []bodyCol{text("number", "number"), num("amount", "amount"), text("customer", "customer"), num("due", "due")}},
	},
	model.Agent: {
		"Session.Messages": {table: "session_messages", ownerCol: "session_id", body: []bodyCol{text("role", "role"), text("text", "text"), num("at", "at")}},
		"Run.Scratch":      {table: "scratch", ownerCol: "run_id", keyCol: "scratch_key", body: []bodyCol{text("value", "value")}},
		"Run.Tasks":        {table: "tasks", ownerCol: "run_id", body: []bodyCol{text("task", "task"), text("payload", "payload")}, fixed: []string{"state", "lease_until"}, liveOnly: "state = 0"},
		"User.Memories":    {table: "memories", ownerCol: "user_id", keyCol: "memory_id", body: []bodyCol{text("text", "text"), blob("vec", "vec")}},
	},
}

// mysqlDocs maps the buckets of each layout to their tables.
var mysqlDocs = map[model.Layout]map[string]docMap{
	model.Social: {
		"User": {table: "users", idCol: "id"},
		"Post": {table: "posts", idCol: "id", write: &docWrite{
			fields: []bodyCol{text("Author", "author"), text("Text", "text"), num("At", "at")}, zero: []string{"likes_n", "comments_n"}}},
		"Group": {table: "social_groups", idCol: "id"},
		"Follow": {table: "follows", write: &docWrite{
			fields: []bodyCol{text("From", "follower_id"), text("To", "followee_id"), num("At", "at")}, pk: []string{"follower_id", "followee_id"}, upsert: []string{"at"}}},
	},
	model.Chat: {
		"User": {table: "users", idCol: "id"},
		"Room": {table: "rooms", idCol: "id"},
		"Member": {table: "members", write: &docWrite{
			fields: []bodyCol{text("Room", "room_id"), text("User", "user_id"), num("Since", "since")}, pk: []string{"room_id", "user_id"}, upsert: []string{"since"}}},
	},
	model.Ledger: {
		"Account": {table: "accounts", idCol: "id"},
	},
	model.Agent: {
		"User":    {table: "users", idCol: "id"},
		"Session": {table: "sessions", idCol: "id"},
		"Run":     {table: "runs", idCol: "id"},
	},
}

// quoteIdent quotes an identifier for MySQL.
func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// validDatabaseName accepts the names the seeder creates: letters, digits and underscores.
func validDatabaseName(name string) error {
	if name == "" {
		return fmt.Errorf("the database name is empty")
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return fmt.Errorf("database name %q holds %q; the seeder only creates letters, digits and underscores", name, r)
		}
	}
	return nil
}
