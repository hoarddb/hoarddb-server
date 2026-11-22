package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// readLimits are the spec's page sizes of the reads; 0 reads every element. A read keeps its spec
// limit and is asked only of owners seeded with at least that many elements (planPage), so a
// page of an ordered store never holds a written element and its cost does not grow with write
// volume. Where fewer than a quarter of the owners qualify the limit falls back, and the record says so.
var readLimits = map[ReadKind]int{
	ReadSocialFeed: 10, ReadSocialInbox: 20, ReadSocialFollowers: 0, ReadSocialFollowScan: 0,
	ReadChatRoomPage: 50, ReadChatUserRooms: 20, ReadLedgerPage: 50, ReadLedgerInvoices: 20, ReadLedgerAll: 0, ReadLedgerTotals: 0, ReadAgentMemories: 100, ReadAgentSession: 20,
}

// readBucket is the bucket whose documents a read starts from.
var readBucket = map[ReadKind]string{
	ReadSocialFeed: "User", ReadSocialInbox: "User", ReadSocialFollowers: "User", ReadSocialFollowScan: "User",
	ReadChatRoomPage: "Room", ReadChatUserRooms: "User", ReadLedgerPage: "Account", ReadLedgerInvoices: "Account", ReadLedgerAll: "Account", ReadLedgerTotals: "Account", ReadAgentMemories: "User", ReadAgentSession: "Session",
}

// readStores names the store each capped read pages through. The follow scan and the audit's
// full expense read are not capped: the first is a record-only leg, the second must see everything.
var readStores = map[ReadKind][2]string{
	ReadSocialFeed: {"User", "Feed"}, ReadSocialInbox: {"User", "Inbox"}, ReadSocialFollowers: {"User", "Followers"},
	ReadChatRoomPage: {"Room", "Messages"}, ReadChatUserRooms: {"User", "Rooms"},
	ReadLedgerPage: {"Account", "Expenses"}, ReadLedgerInvoices: {"Account", "Invoices"},
	ReadAgentMemories: {"User", "Memories"}, ReadAgentSession: {"Session", "Messages"},
}

// offsetEligible reports whether a read pages an ordered store (fifo, lifo or btree), the only
// reads that can start in the middle: a hash store has no order to take an offset in, and the
// record-only reads are not capped pages.
func offsetEligible(kind ReadKind) bool {
	store, ok := readStores[kind]
	if !ok {
		return false
	}
	storeKind, err := KindOf(layoutOf(kind), store[0], store[1])
	return err == nil && storeKind != Hash
}

// setOffset makes op start offset elements into the store it pages. It refuses an offset that
// is not eligible or that would end past the owner's seeded elements: such a page could hold
// written elements and its content could not be checked exactly.
func (w *world) setOffset(op *ReadOp, offset int) error {
	if offset == 0 {
		op.Offset, op.AfterKey = 0, ""
		return nil
	}
	if !offsetEligible(op.Kind) {
		return fmt.Errorf("read %s cannot take an offset", op.Kind)
	}
	store := readStores[op.Kind]
	seeded := w.sets[op.Layout].elements(StoreRef{store[0], op.Entity, store[1]})
	if offset < 0 || op.Limit < 1 || offset+op.Limit > len(seeded) {
		return fmt.Errorf("read %s of %s at offset %d with limit %d runs past the %d seeded elements", op.Kind, op.Entity, offset, op.Limit, len(seeded))
	}
	op.Offset, op.AfterKey = offset, ""
	if kind, _ := KindOf(op.Layout, store[0], store[1]); kind == BTree {
		op.AfterKey = seeded[offset-1].Key
	}
	return nil
}

// membersKey names the room page's member read in the elements-per-read figures.
const membersKey = "chat.room_page.members"

// feedFanout is the most followers a post is pushed to.
const feedFanout = 8

// world is the four seeded data sets a run knows the expected content of.
type world struct {
	seed uint64
	sets map[model.Layout]*Dataset
	gen  *contentGen
	// pages is the plan of every paged read; memberLimit is the member page of the room page, 0
	// for every member as the spec says.
	pages       map[ReadKind]*pagePlan
	memberLimit int
	// access says how an entity is picked. zipf holds one table per layout and bucket in
	// accessZipf mode; both are set before the run and only read after.
	access accessConfig
	zipf   map[model.Layout]map[string]*zipfTable
	// zipfReads is the Zipf table of each paged read over its eligible owners.
	zipfReads map[ReadKind]*zipfTable
}

// configureAccess sets how entities are picked and builds the Zipf tables. The rank order of
// the entities of a bucket is its seeding order, which is the model's own rank index (u0, u1, ...
// a0, a1, ...): rank 0 is the first document of the bucket and the hottest, on every engine.
func (w *world) configureAccess(cfg accessConfig) error {
	switch cfg.Mode {
	case accessUniform, accessZipf:
	default:
		return fmt.Errorf("-access %q is not a mode. Use uniform or zipf", cfg.Mode)
	}
	if cfg.PageDepth < 0 || cfg.PageDepth > 1 {
		return fmt.Errorf("-page-depth %v is not a share. Use a number from 0 to 1", cfg.PageDepth)
	}
	w.access, w.zipf, w.zipfReads = cfg, nil, nil
	if cfg.Mode != accessZipf {
		return nil
	}
	w.zipf = map[model.Layout]map[string]*zipfTable{}
	for layout, ds := range w.sets {
		w.zipf[layout] = map[string]*zipfTable{}
		for bucket, ids := range ds.idList {
			table, err := newZipfTable(len(ids), cfg.ZipfS)
			if err != nil {
				return err
			}
			w.zipf[layout][bucket] = table
		}
	}
	w.zipfReads = map[ReadKind]*zipfTable{}
	for kind, plan := range w.pages {
		table, err := newZipfTable(len(plan.Eligible), cfg.ZipfS)
		if err != nil {
			return err
		}
		w.zipfReads[kind] = table
	}
	return nil
}

// entityAtRank is the entity of a bucket at a rank, counting from 0 in seeding order.
func (w *world) entityAtRank(layout model.Layout, bucket string, rank int) string {
	return w.sets[layout].ids(bucket)[rank]
}

// pickEntity draws one seeded document of a bucket with one draw from rng: uniformly, or by
// the Zipf law over the bucket's rank order. Every entity pick of a request, read or write,
// goes through it, so both modes consume the same number of values.
func (w *world) pickEntity(rng *rand.Rand, layout model.Layout, bucket string) string {
	ids := w.sets[layout].ids(bucket)
	if w.access.Mode == accessZipf {
		return ids[w.zipf[layout][bucket].draw(rng.Float64())]
	}
	return ids[rng.IntN(len(ids))]
}

// minEligibleShare is the share of a bucket's owners that must be eligible for a paged read.
// Below it the page falls back to the largest limit that this share of owners supports.
const minEligibleShare = 0.25

// pagePlan is how one paged read is made: the spec limit, the limit in use, and the owners the
// read may be asked of. It is a function of the model only, so it is identical on every engine.
type pagePlan struct {
	// SpecLimit is the page the spec asks for (0: every element). Limit is the page in use.
	SpecLimit, Limit int
	// Owners is how many documents the read's bucket has; Eligible are the ones seeded with at
	// least Limit elements of the store, in seeding order.
	Owners   int
	Eligible []string
	// Fallback is set when Limit is below SpecLimit because fewer than minEligibleShare of the
	// owners hold SpecLimit elements.
	Fallback bool
}

// planPage works out the page of one paged read. A read keeps its spec limit and is asked only of
// owners seeded with at least that many elements; when fewer than a quarter of the owners
// qualify, the limit drops to the largest one a quarter of them hold. A read of every element
// (spec limit 0) goes to every owner.
func planPage(kind ReadKind, ds *Dataset) (*pagePlan, error) {
	store := readStores[kind]
	owners := ds.ids(store[0])
	plan := &pagePlan{SpecLimit: readLimits[kind], Owners: len(owners)}
	if plan.SpecLimit == 0 {
		plan.Eligible = owners
		return plan, nil
	}
	counts := make([]int, len(owners))
	for i, id := range owners {
		counts[i] = len(ds.elements(StoreRef{store[0], id, store[1]}))
	}
	sorted := append([]int(nil), counts...)
	sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
	quarter := int(math.Ceil(minEligibleShare*float64(len(owners)))) - 1
	plan.Limit = min(plan.SpecLimit, max(1, sorted[max(quarter, 0)]))
	plan.Fallback = plan.Limit < plan.SpecLimit
	for i, id := range owners {
		if counts[i] >= plan.Limit {
			plan.Eligible = append(plan.Eligible, id)
		}
	}
	if len(plan.Eligible) == 0 {
		return nil, fmt.Errorf("no %s owner was seeded with %s.%s elements: raise -scale", store[0], store[0], store[1])
	}
	return plan, nil
}

// PageInfo is one paged read's plan as the record carries it.
type PageInfo struct {
	SpecLimit      int     `json:"spec_limit"`
	Limit          int     `json:"limit"`
	Owners         int     `json:"owners"`
	EligibleOwners int     `json:"eligible_owners"`
	EligibleShare  float64 `json:"eligible_share"`
	Fallback       bool    `json:"fallback"`
}

// pageInfo is the plan of every paged read, for the record.
func (w *world) pageInfo() map[string]PageInfo {
	out := map[string]PageInfo{}
	for kind, plan := range w.pages {
		info := PageInfo{SpecLimit: plan.SpecLimit, Limit: plan.Limit, Owners: plan.Owners, EligibleOwners: len(plan.Eligible), Fallback: plan.Fallback}
		if plan.Owners > 0 {
			info.EligibleShare = float64(len(plan.Eligible)) / float64(plan.Owners)
		}
		out[string(kind)] = info
	}
	return out
}

// readLimit is the page size of a read in this run.
func (w *world) readLimit(kind ReadKind) int {
	if plan := w.pages[kind]; plan != nil {
		return plan.Limit
	}
	return readLimits[kind]
}

// readOwners are the documents a read may start from: the eligible owners of a paged read, or
// every document of the bucket.
func (w *world) readOwners(kind ReadKind) []string {
	if plan := w.pages[kind]; plan != nil {
		return plan.Eligible
	}
	return w.sets[layoutOf(kind)].ids(readBucket[kind])
}

// pickReadEntity draws the owner of a read with one draw from rng: uniformly among the eligible
// owners, or by the Zipf law over their rank order (the order of the eligible list, which is the
// seeding order).
func (w *world) pickReadEntity(rng *rand.Rand, kind ReadKind) string {
	plan := w.pages[kind]
	if plan == nil {
		return w.pickEntity(rng, layoutOf(kind), readBucket[kind])
	}
	if w.access.Mode == accessZipf {
		return plan.Eligible[w.zipfReads[kind].draw(rng.Float64())]
	}
	return plan.Eligible[rng.IntN(len(plan.Eligible))]
}

// readOp builds a read with this run's page sizes.
func (w *world) readOp(layout model.Layout, kind ReadKind, entity string) *ReadOp {
	op := &ReadOp{Layout: layout, Kind: kind, Entity: entity, Limit: w.readLimit(kind)}
	if kind == ReadChatRoomPage {
		op.MemberLimit = w.memberLimit
	}
	return op
}

// elementsPerRead is the page size of every capped read, for the record.
func (w *world) elementsPerRead() map[string]int {
	out := map[string]int{}
	for kind := range readStores {
		out[string(kind)] = w.readLimit(kind)
	}
	out[membersKey] = w.memberLimit
	return out
}

// newWorld generates the four layouts at one seed and scale.
func newWorld(seed uint64, scale int) (*world, error) {
	return newWorldWith(seed, scale, model.Options{})
}

// newWorldWith is newWorld with generator options: the ledger depth the data was seeded with.
func newWorldWith(seed uint64, scale int, opts model.Options) (*world, error) {
	w := &world{seed: seed, sets: map[model.Layout]*Dataset{}, gen: newContentGen(seed), access: accessConfig{Mode: accessUniform, ZipfS: 1.0}}
	for _, layout := range benchLayouts {
		ds, err := newDatasetWith(layout, seed, scale, opts)
		if err != nil {
			return nil, err
		}
		w.sets[layout] = ds
	}
	for layout, buckets := range map[model.Layout][]string{model.Social: {"User", "Post", "Group"}, model.Chat: {"Room", "User"}, model.Ledger: {"Account"}, model.Agent: {"User", "Run", "Session"}} {
		for _, bucket := range buckets {
			if len(w.sets[layout].ids(bucket)) == 0 {
				return nil, fmt.Errorf("layout %s has no %s documents at scale %d. Raise -scale", layout, bucket, scale)
			}
		}
	}
	w.pages = map[ReadKind]*pagePlan{}
	for kind := range readStores {
		plan, err := planPage(kind, w.sets[layoutOf(kind)])
		if err != nil {
			return nil, err
		}
		w.pages[kind] = plan
	}
	return w, nil
}

// layoutOf is the layout a read kind belongs to, from its "<layout>.<operation>" name.
func layoutOf(kind ReadKind) model.Layout {
	name, _, _ := strings.Cut(string(kind), ".")
	return model.Layout(name)
}

// request is one thing a virtual user does: exactly one of Read and Write is set.
type request struct {
	Layout model.Layout
	Kind   string
	Read   *ReadOp
	Write  *WriteOp
	// PlanErr is set when the model did not hold what the planner needed; the request fails
	// instead of crashing the virtual user.
	PlanErr error
}

// fieldString reads a string field of a seeded document.
func fieldString(fields []model.Field, name string) (string, error) {
	for _, f := range fields {
		if f.Name == name {
			if s, ok := f.Value.(string); ok {
				return s, nil
			}
			return "", fmt.Errorf("field %s is a %T, want a string", name, f.Value)
		}
	}
	return "", fmt.Errorf("no field %s", name)
}

// planner makes the requests of one virtual user. Its random stream is seeded from the run's
// -rand-seed and the user's index, and nothing it draws depends on what an engine replied,
// so every engine is offered the same request sequence.
type planner struct {
	w          *world
	mix        *Mix
	repetition int
	vuser      int
	rng        *rand.Rand
	writes     int64
}

func newPlanner(w *world, mix *Mix, repetition, vuser int, randSeed uint64) *planner {
	return &planner{w: w, mix: mix, repetition: repetition, vuser: vuser, rng: rand.New(rand.NewPCG(randSeed, uint64(vuser)+1))}
}

// next draws the next request.
func (p *planner) next() request {
	entry := p.mix.draw(p.rng)
	if !entry.Write {
		kind := ReadKind(entry.Kind)
		op := p.w.readOp(entry.Layout, kind, p.w.pickReadEntity(p.rng, kind))
		if err := p.drawOffset(op); err != nil {
			return request{Layout: entry.Layout, Kind: entry.Kind, PlanErr: fmt.Errorf("planning %s: %w", entry.Kind, err)}
		}
		return request{Layout: entry.Layout, Kind: entry.Kind, Read: op}
	}
	p.writes++
	id := fmt.Sprintf("w%d.%d.%d", p.repetition, p.vuser, p.writes)
	ordinal := int64(p.vuser)<<24 | p.writes
	op, err := p.planWrite(entry.Layout, WriteKind(entry.Kind), id, ordinal)
	if err != nil {
		return request{Layout: entry.Layout, Kind: entry.Kind, PlanErr: fmt.Errorf("planning %s: %w", entry.Kind, err)}
	}
	return request{Layout: entry.Layout, Kind: entry.Kind, Write: op}
}

// drawOffset gives a paged read of an ordered store a random offset with probability
// -page-depth, inside the owner's seeded elements (1 to seeded-limit, so the page still ends in
// them). It always makes its two draws for such a read, used or not, so that the stream of
// operations and entities is the same at any -page-depth.
func (p *planner) drawOffset(op *ReadOp) error {
	if !offsetEligible(op.Kind) {
		return nil
	}
	take, where := p.rng.Float64(), p.rng.Float64()
	store := readStores[op.Kind]
	room := len(p.w.sets[op.Layout].elements(StoreRef{store[0], op.Entity, store[1]})) - op.Limit
	if take >= p.w.access.PageDepth || op.Limit < 1 || room < 1 {
		return nil
	}
	return p.w.setOffset(op, 1+int(where*float64(room)))
}

// pick draws one seeded document of a bucket, see world.pickEntity.
func (p *planner) pick(layout model.Layout, bucket string) string {
	return p.w.pickEntity(p.rng, layout, bucket)
}

// pickOther draws a seeded document of a bucket other than not.
func (p *planner) pickOther(layout model.Layout, bucket, not string) string {
	for {
		if id := p.pick(layout, bucket); id != not {
			return id
		}
	}
}

// keys lists the keys of a seeded keyed store.
func (p *planner) keys(layout model.Layout, ref StoreRef) []string {
	var out []string
	for _, e := range p.w.sets[layout].elements(ref) {
		out = append(out, e.Key)
	}
	return out
}

func (p *planner) planWrite(layout model.Layout, kind WriteKind, id string, ordinal int64) (*WriteOp, error) {
	op := &WriteOp{Layout: layout, Kind: kind, ID: id, Ordinal: ordinal}
	g, at := p.w.gen, writeAt(ordinal)
	ds := p.w.sets[layout]
	field := func(name string, value any) model.Field { return model.Field{Name: name, Value: value} }
	push := func(bucket, owner, store, key string, body ...model.Field) {
		op.Elements = append(op.Elements, model.Element{Bucket: bucket, Owner: owner, Store: store, Key: key, Seq: SeqBase + ordinal, Body: body})
	}
	counter := func(bucket, docID, name string, delta int64, element int) {
		op.Counters = append(op.Counters, CounterDelta{Bucket: bucket, ID: docID, Field: name, Delta: delta, Element: element})
	}
	switch kind {
	case WriteSocialPost:
		author := p.pick(layout, "User")
		postID := "p" + id
		op.PreRead = []StoreRef{{"User", author, "Followers"}}
		op.Documents = []model.Document{{Bucket: "Post", ID: postID, Fields: []model.Field{field("Author", author), field("Text", g.sentence(id, "text", 14)), field("At", at)}}}
		push("User", author, "Feed", "", field("post", postID), field("at", at))
		for i, follower := range p.keys(layout, StoreRef{"User", author, "Followers"}) {
			if i == feedFanout {
				break
			}
			push("User", follower, "Inbox", "", field("post", postID), field("author", author), field("at", at))
		}
	case WriteSocialComment:
		post := p.pick(layout, "Post")
		postDoc, ok := ds.doc("Post", post)
		if !ok {
			return nil, fmt.Errorf("post %s is not in the model", post)
		}
		postAuthor, err := fieldString(postDoc.Fields, "Author")
		if err != nil {
			return nil, fmt.Errorf("post %s: %w", post, err)
		}
		commenter := p.pick(layout, "User")
		push("Post", post, "Comments", "", field("author", commenter), field("text", g.sentence(id, "text", 6)), field("at", at))
		push("User", postAuthor, "Notifications", "", field("kind", "comment"), field("actor", commenter), field("post", post), field("at", at))
		counter("Post", post, "comments_n", 1, 0)
	case WriteSocialLike:
		user, post := p.pick(layout, "User"), p.pick(layout, "Post")
		push("Post", post, "Likes", user, field("user", user), field("at", at))
		push("User", user, "Likes", post, field("post", post), field("at", at))
		counter("Post", post, "likes_n", 1, 0)
	case WriteSocialFollow:
		from := p.pick(layout, "User")
		to := p.pickOther(layout, "User", from)
		push("User", from, "Following", to, field("user", to), field("at", at))
		push("User", to, "Followers", from, field("user", from), field("at", at))
		op.Documents = []model.Document{{Bucket: "Follow", ID: from + ">" + to, Fields: []model.Field{field("From", from), field("To", to), field("At", at)}}}
		counter("User", from, "following_n", 1, 0)
		counter("User", to, "followers_n", 1, 1)
	case WriteSocialGroupPost:
		group := p.pick(layout, "Group")
		members := p.keys(layout, StoreRef{"Group", group, "Members"})
		if len(members) == 0 {
			return nil, fmt.Errorf("group %s was seeded with no members", group)
		}
		op.PreRead = []StoreRef{{"Group", group, "Members"}}
		push("Group", group, "Wall", "", field("author", members[p.rng.IntN(len(members))]), field("text", g.sentence(id, "text", 8)), field("at", at))
	case WriteSocialSettings:
		user := p.pick(layout, "User")
		options := map[string][]string{"theme": {"light", "dark"}, "lang": {"en", "de", "fr"}, "digest": {"daily", "weekly", "off"}}
		name := []string{"theme", "lang", "digest"}[p.rng.IntN(3)]
		choices := options[name]
		push("User", user, "Settings", name, field("value", choices[g.rng(id, "setting").IntN(len(choices))]))
	case WriteChatMessage:
		room := p.pick(layout, "Room")
		members := p.keys(layout, StoreRef{"Room", room, "Members"})
		if len(members) == 0 {
			return nil, fmt.Errorf("room %s was seeded with no members", room)
		}
		textRng := g.rng(id, "length")
		push("Room", room, "Messages", "", field("author", members[p.rng.IntN(len(members))]), field("text", g.sentence(id, "text", 3+textRng.IntN(12))), field("at", at))
		counter("Room", room, "message_n", 1, 0)
	case WriteChatJoin:
		room, user := p.pick(layout, "Room"), p.pick(layout, "User")
		member, ok := ds.doc("User", user)
		if !ok {
			return nil, fmt.Errorf("user %s is not in the model", user)
		}
		op.Refs = []model.Document{member}
		push("Room", room, "Members", user, field("user", user), field("since", at))
		push("User", user, "Rooms", room, field("room", room), field("since", at))
		op.Documents = []model.Document{{Bucket: "Member", ID: room + "/" + user, Fields: []model.Field{field("Room", room), field("User", user), field("Since", at)}}}
		counter("Room", room, "member_n", 1, 0)
	case WriteLedgerExpense:
		account := p.pick(layout, "Account")
		accountDoc, ok := ds.doc("Account", account)
		if !ok {
			return nil, fmt.Errorf("account %s is not in the model", account)
		}
		currency, err := fieldString(accountDoc.Fields, "Currency")
		if err != nil {
			return nil, fmt.Errorf("account %s: %w", account, err)
		}
		categories := []string{"travel", "meals", "software", "office", "hardware", "fees"}
		rng := g.rng(id, "expense")
		amount := int64(100 + rng.IntN(50000))
		push("Account", account, "Expenses", fmt.Sprintf("%010d-%s", 4_000_000_000+ordinal, id),
			field("amount", amount), field("category", categories[rng.IntN(len(categories))]), field("currency", currency))
		counter("Account", account, "expense_n", 1, 0)
		counter("Account", account, "expense_total", amount, 0)
	case WriteLedgerInvoice:
		account := p.pick(layout, "Account")
		rng := g.rng(id, "invoice")
		amount := int64(1000 + rng.IntN(500000))
		number := "INV-" + id
		push("Account", account, "Invoices", number, field("number", number), field("amount", amount), field("customer", g.sentence(id, "customer", 2)), field("due", at+30*86400))
		counter("Account", account, "invoice_n", 1, 0)
		counter("Account", account, "invoice_total", amount, 0)
	case WriteAgentMessage:
		session := p.pick(layout, "Session")
		length := g.rng(id, "length")
		push("Session", session, "Messages", "", field("role", []string{"user", "assistant"}[ordinal%2]), field("text", g.sentence(id, "text", 5+length.IntN(20))), field("at", at))
		counter("Session", session, "message_n", 1, 0)
	case WriteAgentTask:
		run := p.pick(layout, "Run")
		op.Claim = &ClaimRef{Run: run}
		push("Run", run, "Tasks", "", field("task", "t"+id), field("payload", g.sentence(id, "payload", 8)))
	case WriteAgentScratch:
		run := p.pick(layout, "Run")
		push("Run", run, "Scratch", fmt.Sprintf("k%d", p.rng.IntN(6)), field("value", g.sentence(id, "value", 4)))
	case WriteAgentMemory:
		user := p.pick(layout, "User")
		push("User", user, "Memories", "m"+id, field("text", g.sentence(id, "text", 12)), field("vec", g.vector(id)))
		counter("User", user, "memory_n", 1, 0)
	}
	return op, nil
}
