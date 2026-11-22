// Package machine holds the entry ring's pure state machines: a value with an
// Apply method that returns a decision. A machine does no I/O, starts no
// goroutine, reads no clock and takes no lock; everything it knows arrives in
// the event, so a table of events reproduces any run exactly.
package machine

import "slices"

// State words of a declaration (conv 19, 21).
const (
	// StateActive is a bucket that accepts reads and writes.
	StateActive = "active"
	// StateDropping is a bucket whose drop has begun; it is a refusal and is never cached.
	StateDropping = "dropping"
)

// SystemBucketID is the id of the _buckets system bucket in every database. Nothing is
// ever minted into it and it needs no reverse-index document (conv 19).
const SystemBucketID uint64 = 0xFFFFFFFFFFFFFFFF

// FieldEntry is one declared field: its id, its canonical type word (a typed-fields-spec
// 3.1 scalar word or a conv 17 store word) and its name.
type FieldEntry struct {
	ID         uint16
	Type, Name string
}

// RetiredID is a field id that an alter retired, with the epoch of that alter, which is
// what the lazy sweep uses to delete its keys.
type RetiredID struct {
	ID    uint16
	Epoch uint64
}

// FieldTable is the decoded packed field table of conv 19. A table is a value: the
// machines copy it by assignment and never edit its slices in place.
type FieldTable struct {
	NextFieldID uint16
	Entries     []FieldEntry
	Retired     []RetiredID
}

// Pending is an alter's table (field 8): inert until the database's published
// schema_version reaches Version.
type Pending struct {
	Version int64
	Table   FieldTable
	// Expire is the bucket's expiry declaration as it reads once Version is published. Nil is
	// "this alter does not touch expiry": the declaration's own table stays in force. A non-nil
	// table, even an empty one (`drop expire`), replaces it. Carried here, not in field 11
	// directly, so `set expire at` takes effect at the bump on every node and on none before.
	Expire *ExpireTable
}

// MaxIndexComponents is the component cap of an ordered index (ordered-index spec Q1).
const MaxIndexComponents = 4

// MaxOrderedIndexes is the largest ordered index id: ids are 1 to 127, and the top bit of
// a stored id marks an ordered entry (ordered-index spec §2.2).
const MaxOrderedIndexes = 127

// Lifecycle states of an ordered index, the values of IndexEntry.State (ordered-index
// spec §2.2: 0 ready, 1 building, 2 conflicted).
const (
	IndexStateReady uint8 = iota
	IndexStateBuilding
	IndexStateConflicted
)

// IndexComponent is one field of an ordered index and its direction.
type IndexComponent struct {
	FieldID uint16
	Desc    bool
}

// IndexEntry is one ordered index of a bucket (ordered-index spec §2.2).
type IndexEntry struct {
	ID         uint8 // 1 to MaxOrderedIndexes, never reused
	Name       string
	Components []IndexComponent // 1 to MaxIndexComponents
	State      uint8            // IndexStateReady, IndexStateBuilding or IndexStateConflicted
}

// IndexTable is field 10: the bucket's ordered indexes and the next id to mint.
type IndexTable struct {
	NextIndexID uint8
	Entries     []IndexEntry
}

// Declaration is one declaration document, decoded.
type Declaration struct {
	BucketID uint64
	Version  int64       // field 3: the database schema_version at which field 5 became visible
	State    string      // field 4: StateActive or StateDropping
	Fields   FieldTable  // field 5: the table in force when no pending table is published
	Pending  *Pending    // field 8: an alter's table, inert until the published version reaches it
	Indexes  IndexTable  // field 10: ordered composite indexes; zero value when none
	Expire   ExpireTable // field 11: the bucket's expiry declaration; the zero value is no expiry
	// Aggregates is field 12: the maintained aggregates of the bucket's store fields; the
	// zero value is none (aggregate spec 2.2).
	Aggregates AggregateTable
	// References are the fields declared 'of' another bucket (field 13), in field-id order;
	// none when no field refers to anything.
	References []Reference
}

// Reference says that the field FieldID holds ids of the bucket BucketID (fetch-through).
// It is stored beside the field table, not in it, so a field table keeps its layout.
type Reference struct {
	FieldID  uint16
	BucketID uint64
}

// ReferenceOf returns the bucket the field fieldID is declared of.
func (d Declaration) ReferenceOf(fieldID uint16) (uint64, bool) {
	for _, reference := range d.References {
		if reference.FieldID == fieldID {
			return reference.BucketID, true
		}
	}
	return 0, false
}

// Kinds of a maintained aggregate (aggregate spec 2.1), the values of AggregateEntry.Kind.
const (
	AggSum     uint8 = 1 // sum of one element field
	AggSumBy   uint8 = 2 // sum of one element field, per value of another
	AggCountBy uint8 = 3 // element count per value of one element field
)

// States of an aggregate, the values of AggregateEntry.State. Only ready is minted until
// the backfill of an aggregate added to a live store is built.
const (
	AggStateReady    uint8 = 0
	AggStateBuilding uint8 = 1
)

const (
	// MaxAggregates is how many aggregates one store field may declare (spec Q1).
	MaxAggregates = 8
	// MaxAggregatePath is the deepest dotted element path an aggregate names.
	MaxAggregatePath = 3
)

// AggregateEntry is one aggregate of a store field. Field is empty for AggCountBy and By
// is empty for AggSum.
type AggregateEntry struct {
	ID    uint8 // 1 to 255 within the store field, never reused
	Kind  uint8
	State uint8
	Field string
	By    string
}

// AggregateSet is the aggregates of one store field and the next id to mint.
type AggregateSet struct {
	FieldID uint16
	NextID  uint8 // greater than every Entries id
	Entries []AggregateEntry
}

// AggregateTable is declaration field 12, the sets in ascending FieldID order.
type AggregateTable struct {
	Sets []AggregateSet
}

// Set returns the aggregates of one store field and whether it declares any.
func (t AggregateTable) Set(fieldID uint16) (AggregateSet, bool) {
	for _, set := range t.Sets {
		if set.FieldID == fieldID {
			return set, true
		}
	}
	return AggregateSet{}, false
}

// ExpireTable is declaration field 11: the bucket's expiry declaration. The zero value is
// "no expiry" and is not written.
type ExpireTable struct {
	BucketField uint16        // the datetime field id; 0 when only stores expire
	Stores      []ExpireStore // fifoStore and lifoStore fields, in field-id order
}

// ExpireStore is one expiring queue field and the BSON key path of the datetime inside
// an element.
type ExpireStore struct {
	FieldID uint16
	Path    string // 1 to 255 bytes, at most 3 dot-separated keys
}

// SystemDeclaration returns the compiled-in declaration of the _buckets system bucket
// (conv 19): all eight fields, ids 1 to 8, Version 0 and StateActive. Each call returns
// a fresh value, so a caller cannot change what the next caller sees.
func SystemDeclaration() Declaration {
	return Declaration{
		BucketID: SystemBucketID,
		Version:  0,
		State:    StateActive,
		Fields: FieldTable{
			NextFieldID: 9,
			Entries: []FieldEntry{
				{ID: 1, Type: "any", Name: "_id_"},
				{ID: 2, Type: "binary", Name: "bucket_id"},
				{ID: 3, Type: "int", Name: "version"},
				{ID: 4, Type: "string", Name: "state"},
				{ID: 5, Type: "binary", Name: "fields"},
				{ID: 6, Type: "binary", Name: "swept"},
				{ID: 7, Type: "string", Name: "name"},
				{ID: 8, Type: "binary", Name: "pending"},
			},
		},
	}
}

// EffectiveTable is the one implementation of the effective-table rule (conv 19, 20):
// field 8 is in force if and only if pending.Version <= published, and then its version
// is the effective version.
func EffectiveTable(d Declaration, published int64) (FieldTable, int64) {
	if d.Pending != nil && d.Pending.Version <= published {
		return d.Pending.Table, d.Pending.Version
	}
	return d.Fields, d.Version
}

// Effective returns d as it reads at the published version: Fields and Version are the
// effective table and version, and Pending is nil because there is nothing left to resolve.
func Effective(d Declaration, published int64) Declaration {
	d.Expire = EffectiveExpire(d, published)
	d.Fields, d.Version = EffectiveTable(d, published)
	d.Pending = nil
	return PruneAggregates(d)
}

// PruneAggregates returns d without the aggregate sets of fields its table no longer declares,
// which is what an alter that dropped a store field leaves behind once its table is in force. The
// slice is a copy: d and the result never share storage.
func PruneAggregates(d Declaration) Declaration {
	if len(d.Aggregates.Sets) == 0 {
		return d
	}
	live := make(map[uint16]bool, len(d.Fields.Entries))
	for _, entry := range d.Fields.Entries {
		live[entry.ID] = true
	}
	kept := make([]AggregateSet, 0, len(d.Aggregates.Sets))
	for _, set := range d.Aggregates.Sets {
		if live[set.FieldID] {
			kept = append(kept, set)
		}
	}
	if len(kept) == len(d.Aggregates.Sets) {
		return d
	}
	d.Aggregates = AggregateTable{Sets: kept}
	return d
}

// EffectiveExpire is the effective-table rule for the expiry declaration: the pending section
// is in force if and only if its version is published, as for the field table.
func EffectiveExpire(d Declaration, published int64) ExpireTable {
	if d.Pending != nil && d.Pending.Version <= published && d.Pending.Expire != nil {
		return *d.Pending.Expire
	}
	return d.Expire
}

// Fold brings a published pending table into field 5: Version := pending.Version,
// Fields := pending.Table, Pending deleted. It is a write-time operation (an alter
// installing its own pending table, a drop's marker) and is never called on a read.
func Fold(d Declaration) Declaration {
	if d.Pending == nil {
		return d
	}
	d.Version = d.Pending.Version
	d.Fields = d.Pending.Table
	if d.Pending.Expire != nil {
		d.Expire = *d.Pending.Expire
	}
	d.Pending = nil
	d.References = PruneReferences(d.References, d.Fields)
	return PruneAggregates(d)
}

// PruneReferences returns references without the rows of fields table no longer declares: a
// dropped field's reference row goes with the fold that makes the drop effective. It returns
// references itself when nothing is pruned.
func PruneReferences(references []Reference, table FieldTable) []Reference {
	kept := make([]Reference, 0, len(references))
	for _, reference := range references {
		for _, entry := range table.Entries {
			if entry.ID == reference.FieldID {
				kept = append(kept, reference)
				break
			}
		}
	}
	if len(kept) == len(references) {
		return references
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// Entry is one cached declaration.
type Entry struct {
	DBID uint64
	Name string // an operation arrives with a name, so a name is the key (conv 20)
	// Seen is schema_version_seen: the PUBLISHED schema_version when the fetch was issued
	// (the brief calls this field IssuedAt). Lookup compares it with the published version.
	Seen int64
	Decl Declaration
	Size int64
}

// CacheConfig carries the bounds: cluster.decl_cache_entries, cluster.decl_cache_bytes
// and the 4 MiB per-declaration limit.
type CacheConfig struct {
	MaxEntries             int
	MaxBytes, MaxDeclBytes int64
}

// CacheState is the declaration cache. Entries is in LRU order, oldest first.
type CacheState struct {
	Entries []Entry
	flights map[cacheKey]int64
}

type cacheKey struct {
	dbID uint64
	name string
}

// InFlight reports the published version at which a fetch for (dbID, name) was issued and
// has not yet completed, failed or been dropped with its database. The caller uses it to
// collapse a burst of misses into one fetch (conv 20).
func (c *CacheState) InFlight(dbID uint64, name string) (int64, bool) {
	at, ok := c.flights[cacheKey{dbID, name}]
	return at, ok
}

// CacheEvent is an input of CacheState.Apply.
type CacheEvent interface{ isCacheEvent() }

// Lookup asks for a cached declaration at the published version Published.
type Lookup struct {
	DBID      uint64
	Name      string
	Published int64
}

// FetchIssued records a fetch for the single flight; At is the published version at issue.
type FetchIssued struct {
	DBID uint64
	Name string
	At   int64
}

// FetchFailed ends a fetch that found no declaration or failed. Nothing is cached.
type FetchFailed struct {
	DBID uint64
	Name string
}

// Fetched delivers a fetched declaration. Entry.Seen is ignored: the tag is IssuedAt.
type Fetched struct {
	Entry               Entry
	IssuedAt, Published int64
}

// Bumped reports that the published version of a database moved to To.
type Bumped struct {
	DBID uint64
	To   int64
}

// Dropped reports that a database was dropped.
type Dropped struct {
	DBID uint64
}

func (Lookup) isCacheEvent()      {}
func (FetchIssued) isCacheEvent() {}
func (FetchFailed) isCacheEvent() {}
func (Fetched) isCacheEvent()     {}
func (Bumped) isCacheEvent()      {}
func (Dropped) isCacheEvent()     {}

// CacheDecision is the answer to one event.
type CacheDecision struct {
	Entry   Entry
	Hit     bool
	Fetch   bool
	Store   bool
	Evict   []int
	Counted bool
}

// Apply applies one event and returns its decision.
func (c *CacheState) Apply(ev CacheEvent, cfg CacheConfig) CacheDecision {
	switch ev := ev.(type) {
	case Lookup:
		return c.lookup(ev)
	case FetchIssued:
		if c.flights == nil {
			c.flights = map[cacheKey]int64{}
		}
		c.flights[cacheKey{ev.DBID, ev.Name}] = ev.At
	case FetchFailed:
		delete(c.flights, cacheKey{ev.DBID, ev.Name})
	case Fetched:
		return c.fetched(ev, cfg)
	case Dropped:
		return c.dropped(ev)
	case Bumped:
		// Nothing to do, on purpose: every entry carries the version it was fetched at and
		// Lookup compares it lazily, so a bump is O(1) per database and never O(buckets).
	}
	return CacheDecision{}
}

func (c *CacheState) find(dbID uint64, name string) int {
	return slices.IndexFunc(c.Entries, func(e Entry) bool { return e.DBID == dbID && e.Name == name })
}

// lookup serves an entry only when its tag equals the published version. An entry with any
// other tag is dropped on the spot and refetched; an absent one is never cached as absent.
func (c *CacheState) lookup(ev Lookup) CacheDecision {
	i := c.find(ev.DBID, ev.Name)
	if i < 0 {
		return CacheDecision{Fetch: true}
	}
	hit := c.Entries[i]
	c.Entries = slices.Delete(c.Entries, i, i+1)
	if hit.Seen != ev.Published {
		return CacheDecision{Fetch: true}
	}
	c.Entries = append(c.Entries, hit)
	return CacheDecision{Entry: hit, Hit: true}
}

// fetched stores a completed fetch unless one of the never-cached rules applies. A
// declaration over MaxBytes could never fit whatever else is evicted, so it is counted and
// refetched on each use exactly like one over MaxDeclBytes.
func (c *CacheState) fetched(ev Fetched, cfg CacheConfig) CacheDecision {
	delete(c.flights, cacheKey{ev.Entry.DBID, ev.Entry.Name})
	switch {
	case ev.IssuedAt != ev.Published:
		return CacheDecision{}
	case ev.Entry.Decl.State == StateDropping:
		return CacheDecision{}
	case ev.Entry.Size > cfg.MaxDeclBytes || ev.Entry.Size > cfg.MaxBytes:
		return CacheDecision{Counted: true}
	case cfg.MaxEntries < 1:
		return CacheDecision{}
	}

	if i := c.find(ev.Entry.DBID, ev.Entry.Name); i >= 0 {
		c.Entries = slices.Delete(c.Entries, i, i+1)
	}
	stored := ev.Entry
	stored.Seen = ev.IssuedAt
	c.Entries = append(c.Entries, stored)

	// The new entry fits alone (checked above), so evicting from the front always stops
	// before it.
	var held int64
	for _, e := range c.Entries {
		held += e.Size
	}
	var evict []int
	for len(c.Entries) > cfg.MaxEntries || held > cfg.MaxBytes {
		held -= c.Entries[0].Size
		c.Entries = slices.Delete(c.Entries, 0, 1)
		evict = append(evict, 0)
	}
	return CacheDecision{Entry: stored, Store: true, Evict: evict}
}

// dropped removes every entry and flight of a dropped database. Each Evict index is valid
// at the moment of its removal, so they are applied in order.
func (c *CacheState) dropped(ev Dropped) CacheDecision {
	var evict []int
	for i := 0; i < len(c.Entries); {
		if c.Entries[i].DBID != ev.DBID {
			i++
			continue
		}
		c.Entries = slices.Delete(c.Entries, i, i+1)
		evict = append(evict, i)
	}
	for key := range c.flights {
		if key.dbID == ev.DBID {
			delete(c.flights, key)
		}
	}
	return CacheDecision{Evict: evict}
}
