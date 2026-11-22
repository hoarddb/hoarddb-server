package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// This file is the contract between the shared runner and the engine adapters. It is frozen
// at B0's merge: an adapter builder owns only engine_<name>*.go and engine_<name>_test.go and
// changes nothing here.
//
// Rules for every adapter:
//
//   - An adapter only maps operations onto its engine. It never verifies content, never retries
//     silently (a retry is counted in Reply.Retries), never pipelines sub-operations (each sub-operation is one round trip, counted in
//     Reply.RoundTrips) and never drops a field. The shared verifier checks every reply.
//   - Integers in a Reply are int64, text is string, an embedding vector is []byte (the
//     model.EncodeVector bytes). Field order inside a body follows the model's order.
//   - Elements are reported as model.Element with Bucket, Owner, Store, Key (empty for fifo and
//     lifo) and Body. Seq is not required.
//   - An element or document is stored and returned exactly as the WriteOp gives it: every
//     field, no extra field and no missing one. The seeded schemas have no room for a marker
//     column, so a written element is recognised by its content: the runner registers what
//     gen made for each write id and a read must return only seeded content or registered
//     content (every written body carries a value that is unique to its write, such as its
//     time, key or task name).
//   - Pages read oldest first on every engine.
//
// A read with ReadOp.Offset above 0 starts that many elements into an ordered store. An adapter
// uses a range where its engine has one (SQL and Mongo: seq >= Offset for a fifo, key > AfterKey for
// a btree), so the skipped rows are not scanned; an engine with only a continuation cursor reads
// forward from the first element and discards what the offset skips, counting those round trips.
// Each adapter's doc comment on its read says which it does.
//
// A Document in a Reply holds scalar fields only. Page sizes are the spec's, or the largest a
// quarter of the owners support (world.pages); Limit 0 means every element.
//
// Read operations and what a Reply must hold (the sizes named here are the spec limits in readLimits):
//
//	social.feed          Elements: User[e].Feed, first 10; Documents: the Post of each element's "post" field, same order
//	social.inbox         Elements: User[e].Inbox, first 20
//	social.followers     Elements: every element of User[e].Followers (hoardDB: the store; SQL and Mongo: follows_by_followee rows shaped as Followers elements)
//	social.follow_scan   Documents: every Follow document with To == e (record-only leg)
//	chat.room_page       Elements: Room[e].Messages first Limit and the first MemberLimit elements of Room[e].Members; Documents: the User document of each returned member
//	chat.user_rooms      Elements: the first Limit keys, in key order, of User[e].Rooms (the rooms a user joined; hoardDB: the store, SQL and Mongo: the members rows of the user)
//	ledger.account_page  Elements: Account[e].Expenses first 50 in key order
//	ledger.invoices_page Elements: the first Limit keys, in key order, of Account[e].Invoices
//	ledger.expenses_all  Elements: every element of Account[e].Expenses (the post-run audit's full read)
//	agent.memories       Elements: User[e].Memories, at most 100, any order
//	agent.session_page   Elements: Session[e].Messages first 20
//
// Write operations: WriteOp.Elements are applied in order (push for a fifo or lifo store, put by
// Key, replacing, for a hash or btree store); WriteOp.Documents are upserted (a repeat
// replaces, so repeating one is harmless). PreRead stores are read in full before
// the first write, because the application needs them to know its fan-out (the read is counted
// in RoundTrips, its content is not used). Counters are the denormalised counters a SQL or
// document schema keeps: an engine whose schema declares them applies each delta, and only when
// the element it names was newly created (Element index, or -1 for always). hoardDB derives them
// and ignores Counters. A SQL adapter writes the rows of one operation in one transaction and
// reports Commits; hoardDB reports one round trip per sub-operation.
//
// A Claim operation (agent.task) claims one task of Run[e].Tasks, acknowledges it, and then
// applies Elements (the replacement task). If the queue gives nothing, nothing is applied and
// the Reply has no Elements. Otherwise Reply.Elements holds the claimed task.

// Config is what an adapter is opened with. URI and Params come from -uri and repeated
// -param key=value flags; an adapter documents the keys it reads and rejects unknown ones.
type Config struct {
	Engine string
	URI    string
	Params map[string]string
	// VUsers is the number of sessions the level opens, so a pool can be sized to it.
	VUsers int
}

// Engine is one running database server under test.
type Engine interface {
	// Settings reads back the engine's own configuration for the result record. It must
	// include the keys engine_version and tls (the cipher, or "off").
	Settings(ctx context.Context) (map[string]string, error)
	// Session opens one connection (or pool slot) for virtual user number index.
	Session(ctx context.Context, index int) (Session, error)
	Close() error
}

// Session is one virtual user's connection.
type Session interface {
	// Layout returns the operations on one layout. database is the full database name,
	// <base>_<layout>, as the scenarios binary seeded it.
	Layout(l model.Layout, database string) (LayoutOps, error)
	Close() error
}

// LayoutOps runs the operations of one layout.
type LayoutOps interface {
	Read(ctx context.Context, op ReadOp) (Reply, error)
	Write(ctx context.Context, op WriteOp) (Reply, error)
	// Count returns the number of elements of each store, or of documents when Owner and
	// Store are empty, for the post-run audit.
	Count(ctx context.Context, refs []StoreRef) (map[StoreRef]int64, error)
}

// StoreReader is the read-back the post-run audit needs. Every adapter implements it: it
// returns every element of one store of one document, with Key set for keyed stores, in any
// order. It runs after the measured window and is not counted.
type StoreReader interface {
	ReadStore(ctx context.Context, ref StoreRef) ([]model.Element, error)
}

// CounterReader is implemented by an adapter whose engine declares the denormalised counters
// (the SQL and Mongo adapters; hoardDB derives its counters and does not implement it). It
// returns the counter fields of one document of Bucket by name. The audit checks every counter
// a write touched against the seeded value plus the deltas the run applied.
type CounterReader interface {
	ReadCounters(ctx context.Context, bucket, id string) (map[string]int64, error)
}

// EmbedReader is implemented by an adapter that can hold the native layout variant (the ring
// adapter). ReadEmbeds returns every element of one store with its body exactly as the engine
// holds it, embedded copies included, in any order and with Key set for keyed stores; it is not
// counted. ReadStore of the same adapter returns the common shape. The post-run audit compares
// the copies of every element the run wrote with the document they were taken from.
type EmbedReader interface {
	ReadEmbeds(ctx context.Context, ref StoreRef) ([]model.Element, error)
}

// SumReader is implemented by an adapter whose engine maintains declared aggregates (the ring
// adapter, with -param aggregates=on). ReadSum returns the engine's own sum of the numeric
// element field over one store of one document, as the engine answers it, without reading the
// elements. The post-run audit compares it with the sum of the elements it reads back. It is not
// counted.
type SumReader interface {
	ReadSum(ctx context.Context, ref StoreRef, field string) (int64, error)
}

// StoreRef names the store Store of the document Owner of Bucket. An empty Owner and Store
// name the documents of Bucket.
type StoreRef struct{ Bucket, Owner, Store string }

// ReadKind names a read operation, "<layout>.<operation>".
type ReadKind string

// The read operations, see the table at the top of this file.
const (
	ReadSocialFeed       ReadKind = "social.feed"
	ReadSocialInbox      ReadKind = "social.inbox"
	ReadSocialFollowers  ReadKind = "social.followers"
	ReadSocialFollowScan ReadKind = "social.follow_scan"
	ReadChatRoomPage     ReadKind = "chat.room_page"
	ReadChatUserRooms    ReadKind = "chat.user_rooms"
	ReadLedgerPage       ReadKind = "ledger.account_page"
	ReadLedgerInvoices   ReadKind = "ledger.invoices_page"
	ReadLedgerAll        ReadKind = "ledger.expenses_all"
	// ReadLedgerTotals is the sum and count of an account's expenses by category, a record-only
	// read (-op ledger.totals_by_category): hoardDB answers it from its declared aggregates, the
	// other engines with a GROUP BY or a $group over the same rows. Reply.Totals holds the rows.
	ReadLedgerTotals  ReadKind = "ledger.totals_by_category"
	ReadAgentMemories ReadKind = "agent.memories"
	ReadAgentSession  ReadKind = "agent.session_page"
)

// WriteKind names a write operation, "<layout>.<operation>".
type WriteKind string

// The write operations of each layout.
const (
	WriteSocialPost      WriteKind = "social.post"
	WriteSocialComment   WriteKind = "social.comment"
	WriteSocialLike      WriteKind = "social.like"
	WriteSocialFollow    WriteKind = "social.follow"
	WriteSocialGroupPost WriteKind = "social.group_post"
	WriteSocialSettings  WriteKind = "social.settings"
	WriteChatMessage     WriteKind = "chat.message"
	WriteChatJoin        WriteKind = "chat.join"
	WriteLedgerExpense   WriteKind = "ledger.expense"
	WriteLedgerInvoice   WriteKind = "ledger.invoice"
	WriteAgentMessage    WriteKind = "agent.message"
	WriteAgentTask       WriteKind = "agent.task"
	WriteAgentScratch    WriteKind = "agent.scratch"
	WriteAgentMemory     WriteKind = "agent.memory"
)

// ReadOp is one read. Entity is the id of the document the read starts from. Limit is the
// page size, 0 for every element.
type ReadOp struct {
	Layout model.Layout
	Kind   ReadKind
	Entity string
	Limit  int
	// MemberLimit is the page size of the member read of chat.room_page (0 for every member).
	// It is never set by any other read.
	MemberLimit int
	// Offset is how many elements of an ordered store (fifo, lifo or btree) the page skips: 0
	// starts at the first element, as every read did before -page-depth. A read with an Offset
	// has a Limit above 0 and ends inside the owner's seeded elements, so it never sees a
	// written element. Only the store the read pages through takes it: the Messages of
	// chat.room_page (not its members), never a hash store. A page at an Offset is the elements
	// Offset to Offset+Limit-1 in the store's order, the seeded ones.
	Offset int
	// AfterKey is, when Offset is above 0 and the store is a btree (ledger.account_page), the
	// key of the element just before the page, so an engine with ranges reads "key > AfterKey"
	// and never scans the skipped rows. It is empty for a fifo or lifo store, whose seeded Seq
	// counts 0, 1, 2, ... in order: the page is the range seq >= Offset.
	AfterKey string
}

// CounterDelta is a denormalised counter to add to Field of the document ID of Bucket.
type CounterDelta struct {
	Bucket, ID, Field string
	Delta             int64
	// Element is the index into WriteOp.Elements whose creation (not replacement) triggers
	// the delta, or -1 when the delta always applies.
	Element int
}

// ClaimRef asks for a claim and acknowledgement on Run's task queue.
type ClaimRef struct{ Run string }

// SeqBase is added to a write's ordinal to make the seq a SQL adapter stores, so every
// written row sorts after every seeded one.
const SeqBase int64 = 1 << 40

// WriteOp is one write. Every id, key and body is made by gen; the adapter only maps it.
type WriteOp struct {
	Layout model.Layout
	Kind   WriteKind
	// ID is the write id, w<repetition>.<vuser>.<n>.
	ID string
	// Ordinal is unique within the repetition. The seq of an element of this write is SeqBase+Ordinal.
	Ordinal   int64
	PreRead   []StoreRef
	Documents []model.Document
	Elements  []model.Element
	Counters  []CounterDelta
	Claim     *ClaimRef
	// Refs are documents the write copies data from without writing them: the planner puts the
	// joining member's User document here, so a native chat.join can embed the member's name
	// without a read. An adapter that stores no copies ignores it, and it is never stored.
	Refs []model.Document

	// started is the logical time the runner sent the write, for the audit. Adapters ignore it.
	started int64
}

// Reply is what an operation returns, normalised.
type Reply struct {
	Documents []model.Document
	Elements  []model.Element
	// Totals is the answer of ledger.totals_by_category: one row per category, sorted by category.
	Totals     []CategoryTotal
	RoundTrips int
	// Commits is the number of transactions committed, for SQL; 0 elsewhere.
	Commits int
	// Retries is how many times the adapter repeated work it had already sent (a deadlock or
	// a stale-routing retry). A retry is allowed only if it is counted here: the runner sums it
	// per operation into the record and labels a record that has any.
	Retries int
}

// CategoryTotal is the sum of amount and the number of expenses of one category.
type CategoryTotal struct {
	Category     string
	Total, Count int64
}

// StoreKind is the type of a store field.
type StoreKind int

// The four store types that appear in the four layouts.
const (
	FIFO StoreKind = iota + 1
	LIFO
	Hash
	BTree
)

// Keyed reports whether elements of the store have a key and are replaced by a put.
func (k StoreKind) Keyed() bool { return k == Hash || k == BTree }

var schemaKinds = map[model.Layout]map[string]StoreKind{}

func init() {
	words := map[string]StoreKind{"fifoStore": FIFO, "lifoStore": LIFO, "hashStore": Hash, "btreeStore": BTree}
	for _, layout := range []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent} {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			panic(fmt.Sprintf("model rejects layout %s: %v", layout, err)) // invariant: the layouts are compiled in
		}
		kinds := map[string]StoreKind{}
		for _, bucket := range gen.Schema() {
			for _, field := range bucket.Fields {
				if kind, ok := words[field.Type]; ok {
					kinds[bucket.Name+"."+field.Name] = kind
				}
			}
		}
		schemaKinds[layout] = kinds
	}
}

// KindOf returns the type of a store of a layout, from the model's schema.
func KindOf(layout model.Layout, bucket, store string) (StoreKind, error) {
	kind, ok := schemaKinds[layout][bucket+"."+store]
	if !ok {
		return 0, fmt.Errorf("layout %s has no store %s.%s", layout, bucket, store)
	}
	return kind, nil
}

var adapters = map[string]func(context.Context, Config) (Engine, error){}

// register is called from each engine file's init().
func register(name string, open func(context.Context, Config) (Engine, error)) {
	if _, dup := adapters[name]; dup {
		panic("bigasstest: engine " + name + " registered twice") // init-time invariant
	}
	adapters[name] = open
}

// openEngine opens the named engine, or says which engines have an adapter.
func openEngine(ctx context.Context, cfg Config) (Engine, error) {
	open, ok := adapters[cfg.Engine]
	if !ok {
		names := make([]string, 0, len(adapters))
		for name := range adapters {
			names = append(names, name)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("engine %q has no adapter. Registered engines: [%s]", cfg.Engine, strings.Join(names, " "))
	}
	engine, err := open(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening engine %s: %w", cfg.Engine, err)
	}
	return engine, nil
}
