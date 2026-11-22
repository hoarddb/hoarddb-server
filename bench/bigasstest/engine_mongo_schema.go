package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// This file hard-codes the MongoDB collections the adapter touches. The seeder's table mapping
// (bench/scenarios/schema.go) is package main and cannot be imported, so the names below are a
// copy that Layout checks against the live database before the first request: a missing
// collection or index stops the run at open.

// mongoField maps a field of a document or element body to a column of its collection. An
// empty Col marks a body field the collection does not store because it repeats the element
// key (a Likes element's "user"); it is rebuilt on read and skipped on write. Key marks the
// columns that identify a document with a compound key (the Follow and Member rows).
type mongoField struct {
	Name string
	Col  string
	Key  bool
}

// mongoStore is the collection one store is read from and written to.
type mongoStore struct {
	Coll string
	// Owner is the column holding the id of the document the element belongs to.
	Owner string
	// Key is the element key column of a hash or btree store, empty for a fifo.
	Key string
	// Seq is the position column of a fifo or lifo store.
	Seq  string
	Body []mongoField
	// Order is the column a page is sorted by, empty when the store has no order.
	Order string
	// Fixed are columns written with a constant (a task's lease state).
	Fixed []mongoFixed
	// Live is the filter that keeps acknowledged elements out of a count.
	Live string
	// Via names the row that already holds this element: "doc:<Bucket>" or "store:<Bucket.Store>".
	// A paired store (the schema keeps one row for both sides) is never written twice.
	Via string
}

type mongoFixed struct {
	Col   string
	Value int64
}

// mongoDocument is the collection one bucket's documents live in.
type mongoDocument struct {
	Coll string
	// Fields are the scalar fields in the model's order.
	Fields []mongoField
	// Counters are the denormalised counter columns.
	Counters []string
	// IDSeparator joins the Key fields into the document id when the collection has no _id of
	// its own ("From>To"); empty when _id is the id.
	IDSeparator string
}

// mongoLayout is everything the adapter knows about one layout.
type mongoLayout struct {
	Stores map[string]mongoStore
	Docs   map[string]mongoDocument
	// Extra are secondary indexes the reads and the audit depend on, by collection.
	Extra map[string][][]string
}

func fields(pairs ...string) []mongoField {
	out := make([]mongoField, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, mongoField{Name: pairs[i], Col: pairs[i+1]})
	}
	return out
}

func derived(name string) mongoField { return mongoField{Name: name} }

var mongoLayouts = map[model.Layout]mongoLayout{
	model.Social: {
		Stores: map[string]mongoStore{
			"User.Feed":          {Coll: "feed", Owner: "user_id", Seq: "seq", Order: "seq", Body: fields("post", "post_id", "at", "at")},
			"User.Inbox":         {Coll: "inbox", Owner: "user_id", Seq: "seq", Order: "seq", Body: fields("post", "post_id", "author", "author", "at", "at")},
			"User.Notifications": {Coll: "notifications", Owner: "user_id", Seq: "seq", Order: "seq", Body: fields("kind", "kind", "actor", "actor", "post", "post_id", "at", "at")},
			"Post.Comments":      {Coll: "comments", Owner: "post_id", Seq: "seq", Order: "seq", Body: fields("author", "author", "text", "text", "at", "at")},
			"Group.Wall":         {Coll: "wall", Owner: "group_id", Seq: "seq", Order: "seq", Body: fields("author", "author", "text", "text", "at", "at")},
			"Group.Members":      {Coll: "group_members", Owner: "group_id", Key: "user_id", Body: append([]mongoField{derived("user")}, fields("role", "role")...)},
			"User.Settings":      {Coll: "settings", Owner: "user_id", Key: "setting", Body: fields("value", "value")},
			"Post.Likes":         {Coll: "likes", Owner: "post_id", Key: "user_id", Body: append([]mongoField{derived("user")}, fields("at", "at")...)},
			"User.Likes":         {Coll: "likes", Owner: "user_id", Key: "post_id", Body: append([]mongoField{derived("post")}, fields("at", "at")...), Via: "store:Post.Likes"},
			"User.Following":     {Coll: "follows", Owner: "follower_id", Key: "followee_id", Body: append([]mongoField{derived("user")}, fields("at", "at")...), Via: "doc:Follow"},
			"User.Followers":     {Coll: "follows", Owner: "followee_id", Key: "follower_id", Body: append([]mongoField{derived("user")}, fields("at", "at")...), Via: "doc:Follow"},
		},
		Docs: map[string]mongoDocument{
			"User":   {Coll: "users", Fields: fields("Name", "name", "Handle", "handle", "JoinedAt", "joined_at", "Bio", "bio"), Counters: []string{"followers_n", "following_n"}},
			"Post":   {Coll: "posts", Fields: fields("Author", "author", "Text", "text", "At", "at"), Counters: []string{"likes_n", "comments_n"}},
			"Group":  {Coll: "social_groups", Fields: fields("Name", "name", "Owner", "owner")},
			"Follow": {Coll: "follows", Fields: []mongoField{{"From", "follower_id", true}, {"To", "followee_id", true}, {"At", "at", false}}, IDSeparator: ">"},
		},
		Extra: map[string][][]string{
			"follows": {{"followee_id", "follower_id"}},
			"likes":   {{"user_id", "post_id"}},
		},
	},
	model.Chat: {
		Stores: map[string]mongoStore{
			"Room.Messages": {Coll: "messages", Owner: "room_id", Seq: "seq", Order: "seq", Body: fields("author", "author", "text", "text", "at", "at")},
			"Room.Members":  {Coll: "members", Owner: "room_id", Key: "user_id", Body: append([]mongoField{derived("user")}, fields("since", "since")...), Via: "doc:Member"},
			"User.Rooms":    {Coll: "members", Owner: "user_id", Key: "room_id", Body: append([]mongoField{derived("room")}, fields("since", "since")...), Via: "doc:Member"},
		},
		Docs: map[string]mongoDocument{
			"Room":   {Coll: "rooms", Fields: fields("Name", "name"), Counters: []string{"member_n", "message_n"}},
			"User":   {Coll: "users", Fields: fields("Name", "name")},
			"Member": {Coll: "members", Fields: []mongoField{{"Room", "room_id", true}, {"User", "user_id", true}, {"Since", "since", false}}, IDSeparator: "/"},
		},
		Extra: map[string][][]string{"members": {{"user_id", "room_id"}}},
	},
	model.Ledger: {
		Stores: map[string]mongoStore{
			"Account.Expenses": {Coll: "expenses", Owner: "account_id", Key: "expense_key", Order: "expense_key", Body: fields("amount", "amount", "category", "category", "currency", "currency")},
			"Account.Invoices": {Coll: "invoices", Owner: "account_id", Key: "number", Body: append([]mongoField{derived("number")}, fields("amount", "amount", "customer", "customer", "due", "due")...)},
		},
		Docs: map[string]mongoDocument{
			"Account": {Coll: "accounts", Fields: fields("Name", "name", "Currency", "currency"), Counters: []string{"expense_n", "expense_total", "invoice_n", "invoice_total"}},
		},
	},
	model.Agent: {
		Stores: map[string]mongoStore{
			"Session.Messages": {Coll: "session_messages", Owner: "session_id", Seq: "seq", Order: "seq", Body: fields("role", "role", "text", "text", "at", "at")},
			"Run.Scratch":      {Coll: "scratch", Owner: "run_id", Key: "scratch_key", Body: fields("value", "value")},
			"Run.Tasks":        {Coll: "tasks", Owner: "run_id", Seq: "seq", Order: "seq", Body: fields("task", "task", "payload", "payload"), Fixed: []mongoFixed{{"state", 0}, {"lease_until", 0}}, Live: "state"},
			"User.Memories":    {Coll: "memories", Owner: "user_id", Key: "memory_id", Body: fields("text", "text", "vec", "vec")},
		},
		Docs: map[string]mongoDocument{
			"User":    {Coll: "users", Fields: fields("Name", "name"), Counters: []string{"memory_n"}},
			"Session": {Coll: "sessions", Fields: fields("Owner", "owner"), Counters: []string{"message_n"}},
			"Run":     {Coll: "runs", Fields: fields("Session", "session"), Counters: []string{"task_n"}},
		},
		Extra: map[string][][]string{"tasks": {{"run_id", "state", "seq"}}},
	},
}

// storeOf returns the collection spec of a store of a layout.
func (l mongoLayout) storeOf(bucket, store string) (mongoStore, error) {
	spec, ok := l.Stores[bucket+"."+store]
	if !ok {
		return mongoStore{}, fmt.Errorf("the MongoDB adapter has no collection for store %s.%s", bucket, store)
	}
	return spec, nil
}

// docOf returns the collection spec of a bucket of a layout.
func (l mongoLayout) docOf(bucket string) (mongoDocument, error) {
	spec, ok := l.Docs[bucket]
	if !ok {
		return mongoDocument{}, fmt.Errorf("the MongoDB adapter has no collection for bucket %s", bucket)
	}
	return spec, nil
}

// required lists every collection of the layout with the index key lists it must have.
func (l mongoLayout) required() map[string][][]string {
	out := map[string][][]string{}
	add := func(coll string, keys ...string) {
		if _, seen := out[coll]; !seen {
			out[coll] = nil
		}
		if len(keys) > 0 {
			out[coll] = append(out[coll], keys)
		}
	}
	for _, spec := range l.Stores {
		switch {
		case spec.Key != "":
			add(spec.Coll, spec.Owner, spec.Key)
		default:
			add(spec.Coll, spec.Owner, spec.Seq)
		}
	}
	for _, doc := range l.Docs {
		add(doc.Coll)
	}
	for coll, indexes := range l.Extra {
		for _, keys := range indexes {
			add(coll, keys...)
		}
	}
	return out
}

// checkMongoIndexes compares the live index key lists of one collection with what the adapter needs.
func checkMongoIndexes(coll string, want, have [][]string) error {
	for _, keys := range want {
		found := false
		for _, candidate := range have {
			if strings.Join(candidate, ",") == strings.Join(keys, ",") {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("collection %q has no index on (%s); it has %s", coll, strings.Join(keys, ", "), describeIndexes(have))
		}
	}
	return nil
}

func describeIndexes(indexes [][]string) string {
	names := make([]string, len(indexes))
	for i, keys := range indexes {
		names[i] = "(" + strings.Join(keys, ", ") + ")"
	}
	sort.Strings(names)
	return strings.Join(names, " ")
}
