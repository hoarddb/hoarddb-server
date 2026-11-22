package server

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"reflect"
	"sync"
	"sync/atomic"
)

// This file holds catalogTable, the pointer-free registry of one database's
// buckets (the catalog-gc-spec record section 5.2). bucketTable wraps it,
// and a bucket is its record: no struct, engine or name string per bucket
// lives on the heap between operations.
//
// Why it looks the way it does: the Go collector traces every pointer-bearing
// object on every cycle, so the registry holds records with no
// pointers in them, in chunks that never move, found by name through an index
// of integers. A record's address is therefore stable for the life of the
// process, which is what lets the record carry the bucket's one mutex.
//
// Locking: the table has no lock of its own. It lives inside an inMemoryDB and
// is guarded by that database's mu, exactly as the map it replaces is: find,
// name, residentBytes and scanStep need it held for reading, every other
// method for writing. A record's own mu is a different lock, taken after the
// table's (lock order data.mu, db.mu, bucket.mu).

// Sizes and limits. See the spec for the measurements behind them.
const (
	// maxCatalogNameBytes is the longest name the table stores: nameLen is a
	// uint16. CG-2 refuses names far below this before they reach the table.
	maxCatalogNameBytes = 1<<16 - 1

	firstRecordChunk    = 8    // records in chunk 0; chunk n holds firstRecordChunk<<n until the cap
	maxRecordChunk      = 4096 // records per chunk once the doubling stops
	doublingRecordChunk = 10   // chunks 0..9 double from 8 to 4096; every later chunk is maxRecordChunk
	// doublingRecordSpan is how many records chunks 0..9 hold together.
	doublingRecordSpan = firstRecordChunk * (1<<doublingRecordChunk - 1)

	// A name reference is arena chunk number << 16 | offset. A name is at most
	// 65,535 bytes, so even a chunk made just for one fits 16 offset bits.
	//
	// Arena chunks stop doubling at 16 KiB, not the spec's 1 MiB: a claim that
	// opens a 1 MiB chunk on top of a record chunk breaks the bound the spec
	// itself sets (one record chunk plus 64 KB per claim, TestCatalogTableClaimAllocatesBounded).
	// 16 KiB x 65,536 chunks is 1 GiB of names, 4x the 255-byte limit at 10^6.
	arenaOffsetBits  = 16
	maxArenaChunks   = 1 << (32 - arenaOffsetBits)
	maxArenaChunkLen = 16 << 10
	firstArenaChunk  = 128 // bytes in the first arena chunk; each new one doubles up to maxArenaChunkLen

	// singleIndexLimit is the live-record count above which the one index map
	// becomes 256 shards, so that growing it is a per-shard split and a shrink
	// can rebuild one shard at a time.
	singleIndexLimit = 4096
	indexShardCount  = 256
	// shardRebuildFloor keeps a shard that peaked at a handful of entries from
	// being rebuilt over and over.
	shardRebuildFloor = 64

	// indexEntryBytes and indexShardOverheadBytes are the table's estimate of
	// what its integer maps cost, kept as counters because residentBytes may
	// not ask the runtime. A Go map entry here is a uint64 key and a uint32
	// value plus control bytes and load-factor slack.
	indexEntryBytes         = 24
	indexShardOverheadBytes = 64
)

// recordSize is the resident size of one bucketRec, from the type, not a
// literal, so a field added later changes the counters with it.
var recordSize = int64(reflect.TypeFor[bucketRec]().Size())

// Record states, held in bucketRec.state. A record is linked while its name
// finds it, unlinked between the first and third phase of a drop, and free
// once its slot is back on a free list.
const (
	recFree uint8 = iota
	recLinked
	recUnlinked
)

// errCatalogNameTooLong is returned by claim for a name the record cannot
// describe. Nothing in the product reaches it: the create is refused first.
var errCatalogNameTooLong = errors.New("name is longer than the catalog table can hold")

// errCatalogRecordState is returned by unlink and free when the record is not
// in the state the call needs. Freeing twice would put one slot on the free
// list twice and hand it to two buckets, so the table refuses instead.
var errCatalogRecordState = errors.New("catalog record is not in the state this call needs")

// errCatalogRecordNotInChain is returned by chainRemove when the record is
// linked but the index no longer reaches it. A drop finds the record through
// that chain before it unlinks it, so a miss is the index contradicting
// itself; taking the record out of a chain it is not in would hide that.
var errCatalogRecordNotInChain = errors.New("catalog record is not in its index chain")

// bucketRec is one bucket's identity: everything the registry must know
// without building a handle, with no pointer in it, so a chunk of them is one
// allocation the collector does not scan. The record owns the bucket's only
// mutex and never moves; a holder that kept a pointer across a drop checks
// closed and epoch under mu before trusting it.
//
// Never copy a bucketRec (go vet's copylocks says so): index a chunk and take
// the address.
type bucketRec struct {
	mu sync.RWMutex
	// id is 0 until the create commits, then written once, last, after the
	// fields it publishes (query-pipelining spec I-1).
	id         atomic.Uint64
	generation atomic.Int64
	// term and lineageFlags are the lineage cache of server/lineage.go.
	term         atomic.Uint64
	epoch        atomic.Uint32 // +1 each time this slot is claimed for a name
	lineageFlags atomic.Uint32
	// replicationFactor is the factor the bucket declared; 0 means the node default.
	replicationFactor atomic.Int32
	// bits holds the record's flags (recCreating, recHasDecl,
	// recFailedSeqsWarned, recLargeCommitsWarned, recSpread), each readable with no lock.
	// The claim sets the word whole; the flags' own writers are named beside
	// them.
	bits atomic.Uint32
	// typesID is the bucket's typed-field declaration in the server's
	// interned set (fieldTypeSet), 0 for none. Buckets that declare the same
	// fields share one id, so a typed-only bucket costs these four bytes and
	// no declaration of its own. Stored before id is, like slot (I-1).
	typesID atomic.Uint32
	// nameRef locates the name in the arena; next chains records that share an
	// index entry while linked and is the free-list link while free (record
	// index + 1, 0 for none); self is this record's own index.
	nameRef uint32
	next    uint32
	self    uint32
	nameLen uint16
	slot    uint16

	bucketType uint8
	closed     bool
	state      uint8
}

// The flags in bucketRec.bits.
const (
	// recCreating is set while a create (not a pull) holds the uncommitted
	// record, and cleared by a create that fails; a drop that names no
	// generation leaves such a record for its create (BUG-0255's rule).
	// Written under the table's write lock at the claim, and by the create
	// under mu.
	recCreating uint32 = 1 << iota
	// recHasDecl says the server's declaration table holds this bucket's
	// declaration. Set under mu after the entry is put, so a reader that sees
	// it finds the entry.
	recHasDecl
	// recFailedSeqsWarned is set, under mu, once this start has warned that
	// the bucket's failed_seqs is long.
	recFailedSeqsWarned
	// recLargeCommitsWarned is set, under mu, once this start has warned that
	// the bucket's large_commits is long.
	recLargeCommitsWarned
	// recSpread says a hash bucket is recorded placement spread; clear is
	// whole, and is every other type's state. Set where the replication
	// factor is set: the create after its catalog commit (createBucketLocked),
	// the attach from the entry (attachBucketLocked) and a pulled copy
	// (materializeRange). It costs no byte beyond the word it lives in.
	recSpread
	// recPart says the record is a part of a spread collection: its name holds
	// the part separator. Set where the record is claimed, from the name, so
	// the counts a client sees (the listings and hoarddb_node_buckets) leave
	// parts out. It costs no byte beyond the word it lives in.
	recPart
)

func (rec *bucketRec) flag(bit uint32) bool { return rec.bits.Load()&bit != 0 }

func (rec *bucketRec) setFlag(bit uint32, on bool) {
	if on {
		rec.bits.Or(bit)
		return
	}
	rec.bits.And(^bit)
}

// isCreating reports recCreating.
func (rec *bucketRec) isCreating() bool { return rec.flag(recCreating) }

// setCreating sets or clears recCreating.
func (rec *bucketRec) setCreating(creating bool) { rec.setFlag(recCreating, creating) }

// isPart reports recPart.
func (rec *bucketRec) isPart() bool { return rec.flag(recPart) }

// isSpread reports recSpread.
func (rec *bucketRec) isSpread() bool { return rec.flag(recSpread) }

// hasDecl reports recHasDecl.
func (rec *bucketRec) hasDecl() bool { return rec.flag(recHasDecl) }

// recordChunk is one allocation of records. Its slice is made once and never
// grown or copied, which is what keeps every record's address stable.
type recordChunk struct {
	recs []bucketRec
	base uint32 // index of recs[0]
	// bump is how many records have ever been handed out: recs[bump:] are
	// untouched. freeHead heads the intrusive list, through next, of freed
	// records of this chunk only, so releasing the chunk never walks a list.
	bump     int
	freeHead uint32
	live     int // records claimed and not yet freed
	// prevSpace and nextSpace link the chunks that still have a record to give.
	prevSpace, nextSpace int32
	inSpace              bool
}

func (c *recordChunk) hasSpace() bool { return c.freeHead != 0 || c.bump < len(c.recs) }

// arenaChunk is a byte slab holding names. No name crosses a chunk boundary.
type arenaChunk struct {
	buf  []byte
	used int
	live int // bytes of names still referenced
}

// indexShard maps a name hash to the first record of the chain that shares it.
type indexShard struct {
	entries map[uint64]uint32
	peak    int
}

// catalogTable is the registry of one database's buckets. See the file comment.
type catalogTable struct {
	hasher func(name string) uint64

	chunks    []*recordChunk
	spaceHead int32 // first chunk with a record to give, -1 for none
	spareLive int32 // the one empty chunk kept to avoid thrash, -1 for none

	arena      []*arenaChunk
	arenaTail  int // chunk receiving new names, -1 for none
	arenaHint  int // size of the next arena chunk
	arenaCount int // resident arena chunks

	single  indexShard
	shards  *[indexShardCount]*indexShard
	sharded bool
	linked  int // records whose name finds them
	parts   int // of linked, the records that are parts (recPart)

	rebuildCursor int
	compactCursor int
	compacting    bool
	compactWanted bool
	indexEntries  int64
	shardCount    int64
	recordBytes   int64
	arenaBytes    int64
}

// newCatalogTable returns an empty table whose hash is seeded randomly per
// table, so a name chosen to collide in one database's index does not collide
// in another's and an attacker cannot aim a flood of names at one chain.
func newCatalogTable() *catalogTable {
	seed := maphash.MakeSeed()
	return newCatalogTableWithHasher(func(name string) uint64 { return maphash.String(seed, name) })
}

// newCatalogTableWithHasher builds a table around hasher. Tests inject one
// that collides on purpose; the product calls newCatalogTable.
func newCatalogTableWithHasher(hasher func(name string) uint64) *catalogTable {
	return &catalogTable{hasher: hasher, spaceHead: -1, spareLive: -1, arenaTail: -1, arenaHint: firstArenaChunk}
}

// chunkSize returns the record count of chunk number c.
func chunkSize(c int) int {
	if c < doublingRecordChunk {
		return firstRecordChunk << c
	}
	return maxRecordChunk
}

// chunkBase returns the index of the first record of chunk number c.
func chunkBase(c int) uint32 {
	if c < doublingRecordChunk {
		return uint32(firstRecordChunk * (1<<c - 1))
	}
	return uint32(doublingRecordSpan + (c-doublingRecordChunk)*maxRecordChunk)
}

// chunkOf returns the chunk number and the offset in it of record index idx.
func chunkOf(idx uint32) (chunk, offset int) {
	if idx < doublingRecordSpan {
		chunk = bits.Len32(idx/firstRecordChunk+1) - 1
		return chunk, int(idx - chunkBase(chunk))
	}
	over := int(idx - doublingRecordSpan)
	return doublingRecordChunk + over/maxRecordChunk, over % maxRecordChunk
}

func (t *catalogTable) recAt(idx uint32) *bucketRec {
	chunk, offset := chunkOf(idx)
	return &t.chunks[chunk].recs[offset]
}

// find returns the live record named name, or nil. Every hit is compared with
// the stored name, so a hash collision is a longer chain, never another
// bucket. The caller holds the table's lock for reading.
func (t *catalogTable) find(name string) *bucketRec {
	return t.findHashed(t.hasher(name), name)
}

func (t *catalogTable) findHashed(hash uint64, name string) *bucketRec {
	for ref := t.indexHead(hash); ref != 0; {
		rec := t.recAt(ref - 1)
		if t.nameIs(rec, name) {
			return rec
		}
		ref = rec.next
	}
	return nil
}

// nameIs compares without allocating: converting the arena bytes to a string
// inside a comparison does not copy them.
func (t *catalogTable) nameIs(rec *bucketRec, name string) bool {
	if int(rec.nameLen) != len(name) {
		return false
	}
	return string(t.nameBytes(rec)) == name
}

func (t *catalogTable) nameBytes(rec *bucketRec) []byte {
	slab := t.arena[rec.nameRef>>arenaOffsetBits]
	offset := int(rec.nameRef & (1<<arenaOffsetBits - 1))
	return slab.buf[offset : offset+int(rec.nameLen)]
}

// name returns a copy of the record's name. The caller holds the table's lock
// for reading and the record is not free.
func (t *catalogTable) name(rec *bucketRec) string {
	return string(t.nameBytes(rec))
}

// claim returns the record named name, giving the name a slot if it has none.
// A name already linked returns its record, and a record no create has
// committed (id 0) is restamped with generation and creating, for
// inMemoryDB.claimBucket. A new record bumps the slot's
// epoch, which is what tells a stale holder its pointer now names another
// bucket. The caller holds the table's lock for writing.
func (t *catalogTable) claim(name string, bucketType uint8, generation int64, creating bool) (*bucketRec, error) {
	if len(name) > maxCatalogNameBytes {
		return nil, fmt.Errorf("claiming a record for a name of %d bytes (the limit is %d): %w", len(name), maxCatalogNameBytes, errCatalogNameTooLong)
	}
	hash := t.hasher(name)
	if existing := t.findHashed(hash, name); existing != nil {
		if existing.id.Load() == 0 {
			existing.generation.Store(generation)
			existing.setCreating(creating)
		}
		return existing, nil
	}
	nameRef, err := t.storeName(name)
	if err != nil {
		return nil, fmt.Errorf("claiming a record for %d-byte name: %w", len(name), err)
	}
	rec := t.takeRecord()
	t.resetRecord(rec, nameRef, len(name), bucketType, generation, creating)
	if hasPartSeparator(name) {
		rec.setFlag(recPart, true)
		t.parts++
	}
	t.linkRecord(hash, rec)
	return rec, nil
}

// resetRecord rewrites a record for a new name field by field, under its
// mutex, because a stale holder may be about to take that mutex and copying the
// struct would copy the lock.
func (t *catalogTable) resetRecord(rec *bucketRec, nameRef uint32, nameLen int, bucketType uint8, generation int64, creating bool) {
	rec.mu.Lock()
	rec.id.Store(0)
	rec.generation.Store(generation)
	rec.term.Store(0)
	rec.lineageFlags.Store(0)
	rec.replicationFactor.Store(0)
	rec.bits.Store(0)
	rec.setCreating(creating)
	rec.typesID.Store(0)
	rec.epoch.Add(1)
	rec.nameRef = nameRef
	rec.nameLen = uint16(nameLen)
	rec.slot = 0
	rec.bucketType = bucketType
	rec.closed = false
	rec.state = recLinked
	rec.mu.Unlock()
}

// unlink is phase one of a drop: the name stops finding the record, and
// nothing else changes. The slot stays allocated so that a create arriving
// before the drop has closed the record cannot recycle it (spec section 5.3).
// An error means the record's chain does not reach it, and nothing was changed:
// the record stays linked rather than being dropped behind the index's back.
// The caller holds the table's lock for writing.
func (t *catalogTable) unlink(rec *bucketRec) error {
	if rec.state != recLinked {
		return fmt.Errorf("unlinking record %d, which is not linked: %w", rec.self, errCatalogRecordState)
	}
	hash := t.hasher(t.name(rec))
	if err := t.chainRemove(hash, rec); err != nil {
		return err
	}
	rec.state = recUnlinked
	t.linked--
	if rec.flag(recPart) {
		t.parts--
	}
	return nil
}

// free is phase three of a drop, after the record is closed: the slot goes on
// its chunk's free list and the name's bytes are given back. The caller holds
// the table's lock for writing and has unlinked the record.
func (t *catalogTable) free(rec *bucketRec) error {
	if rec.state != recUnlinked {
		return fmt.Errorf("freeing record %d, which is not unlinked: %w", rec.self, errCatalogRecordState)
	}
	rec.id.Store(0)
	rec.state = recFree
	t.releaseName(rec)
	rec.nameRef, rec.nameLen = 0, 0

	chunkNo, _ := chunkOf(rec.self)
	chunk := t.chunks[chunkNo]
	rec.next = chunk.freeHead
	chunk.freeHead = rec.self + 1
	chunk.live--
	if !chunk.inSpace {
		t.pushSpace(int32(chunkNo))
	}
	if chunk.live == 0 {
		t.retireEmptyChunk(chunkNo)
	}
	return nil
}

// takeRecord hands out a free slot, from a chunk's free list first, then from
// the untouched part of a chunk, and allocates a chunk only when none has room.
func (t *catalogTable) takeRecord() *bucketRec {
	if t.spaceHead < 0 {
		t.growChunks()
	}
	chunkNo := int(t.spaceHead)
	chunk := t.chunks[chunkNo]
	var rec *bucketRec
	if chunk.freeHead != 0 {
		rec = t.recAt(chunk.freeHead - 1)
		chunk.freeHead = rec.next
		rec.next = 0
	} else {
		rec = &chunk.recs[chunk.bump]
		rec.self = chunk.base + uint32(chunk.bump)
		chunk.bump++
	}
	chunk.live++
	if !chunk.hasSpace() {
		t.removeSpace(int32(chunkNo))
	}
	if t.spareLive == int32(chunkNo) {
		t.spareLive = -1
	}
	return rec
}

// growChunks allocates one chunk, in the lowest chunk number that is free so
// a released small chunk is reused before a new large one is made.
func (t *catalogTable) growChunks() {
	chunkNo := len(t.chunks)
	for candidate, existing := range t.chunks {
		if existing == nil {
			chunkNo = candidate
			break
		}
	}
	if chunkNo == len(t.chunks) {
		t.chunks = append(t.chunks, nil)
	}
	t.chunks[chunkNo] = &recordChunk{
		recs:      make([]bucketRec, chunkSize(chunkNo)),
		base:      chunkBase(chunkNo),
		prevSpace: -1,
		nextSpace: -1,
	}
	t.recordBytes += int64(chunkSize(chunkNo)) * recordSize
	t.pushSpace(int32(chunkNo))
}

func (t *catalogTable) pushSpace(chunkNo int32) {
	chunk := t.chunks[chunkNo]
	chunk.prevSpace = -1
	chunk.nextSpace = t.spaceHead
	if t.spaceHead >= 0 {
		t.chunks[t.spaceHead].prevSpace = chunkNo
	}
	t.spaceHead = chunkNo
	chunk.inSpace = true
}

func (t *catalogTable) removeSpace(chunkNo int32) {
	chunk := t.chunks[chunkNo]
	if chunk.prevSpace >= 0 {
		t.chunks[chunk.prevSpace].nextSpace = chunk.nextSpace
	} else {
		t.spaceHead = chunk.nextSpace
	}
	if chunk.nextSpace >= 0 {
		t.chunks[chunk.nextSpace].prevSpace = chunk.prevSpace
	}
	chunk.prevSpace, chunk.nextSpace, chunk.inSpace = -1, -1, false
}

// retireEmptyChunk keeps one empty chunk as a spare and releases any further
// one. Releasing sets the chunk's pointer to nil; a stale holder's interior
// pointer keeps the memory alive for as long as it holds it, which is safe
// because every holder checks closed and epoch before it reads anything.
func (t *catalogTable) retireEmptyChunk(chunkNo int) {
	if t.spareLive < 0 || t.spareLive == int32(chunkNo) {
		t.spareLive = int32(chunkNo)
		return
	}
	t.removeSpace(int32(chunkNo))
	t.recordBytes -= int64(len(t.chunks[chunkNo].recs)) * recordSize
	t.chunks[chunkNo] = nil
	for len(t.chunks) > 0 && t.chunks[len(t.chunks)-1] == nil {
		t.chunks = t.chunks[:len(t.chunks)-1]
	}
}

// storeName copies name into the arena's tail chunk, opening a new chunk when
// it does not fit, and returns the reference to it.
func (t *catalogTable) storeName(name string) (uint32, error) {
	if t.arenaTail < 0 || t.arena[t.arenaTail].used+len(name) > len(t.arena[t.arenaTail].buf) {
		if err := t.growArena(len(name)); err != nil {
			return 0, err
		}
	}
	slab := t.arena[t.arenaTail]
	offset := slab.used
	copy(slab.buf[offset:], name)
	slab.used += len(name)
	slab.live += len(name)
	return uint32(t.arenaTail)<<arenaOffsetBits | uint32(offset), nil
}

func (t *catalogTable) growArena(need int) error {
	slabNo := len(t.arena)
	for candidate, existing := range t.arena {
		if existing == nil {
			slabNo = candidate
			break
		}
	}
	if slabNo >= maxArenaChunks {
		return fmt.Errorf("the name arena holds %d chunks and has no room for another", maxArenaChunks)
	}
	if slabNo == len(t.arena) {
		t.arena = append(t.arena, nil)
	}
	size := max(t.arenaHint, need)
	t.arenaHint = min(t.arenaHint*2, maxArenaChunkLen)
	t.arena[slabNo] = &arenaChunk{buf: make([]byte, size)}
	t.arenaBytes += int64(size)
	t.arenaCount++
	t.arenaTail = slabNo
	return nil
}

// releaseName gives a record's name bytes back. A chunk with no live name is
// released; one with few is left for compaction (maintain).
func (t *catalogTable) releaseName(rec *bucketRec) {
	slabNo := int(rec.nameRef >> arenaOffsetBits)
	slab := t.arena[slabNo]
	slab.live -= int(rec.nameLen)
	t.compactWanted = true
	if slab.live == 0 {
		t.dropArenaChunk(slabNo)
	}
}

func (t *catalogTable) dropArenaChunk(slabNo int) {
	t.arenaBytes -= int64(len(t.arena[slabNo].buf))
	t.arena[slabNo] = nil
	t.arenaCount--
	if t.arenaTail == slabNo {
		t.arenaTail = -1
	}
	if t.arenaCount == 0 {
		t.arena = nil
		t.arenaHint = firstArenaChunk
	}
}

// linkRecord puts rec at the head of its hash chain, and moves to shards once
// the table is big enough for them to pay.
func (t *catalogTable) linkRecord(hash uint64, rec *bucketRec) {
	rec.next = t.indexHead(hash)
	t.indexSet(hash, rec.self+1)
	t.linked++
	if !t.sharded && t.linked > singleIndexLimit {
		t.splitIndex()
	}
}

// chainRemove takes rec out of its hash chain, and returns an error, changing
// nothing, when the chain does not reach it: the record and the index disagree
// about which names are linked, and the caller must not finish a drop of a
// record the index cannot name.
func (t *catalogTable) chainRemove(hash uint64, rec *bucketRec) error {
	head := t.indexHead(hash)
	if head == rec.self+1 {
		if rec.next == 0 {
			t.indexDelete(hash)
		} else {
			t.indexSet(hash, rec.next)
		}
		rec.next = 0
		return nil
	}
	for ref := head; ref != 0; {
		prev := t.recAt(ref - 1)
		if prev.next == rec.self+1 {
			prev.next = rec.next
			rec.next = 0
			return nil
		}
		ref = prev.next
	}
	return fmt.Errorf("removing record %d (name %q) from the chain under hash %#x: %w", rec.self, t.name(rec), hash, errCatalogRecordNotInChain)
}

// shardFor returns the index shard for hash, creating it when create is true.
// Below singleIndexLimit there is one shard and the hash is not consulted.
func (t *catalogTable) shardFor(hash uint64, create bool) *indexShard {
	if !t.sharded {
		return &t.single
	}
	shardNo := hash >> 56
	shard := t.shards[shardNo]
	if shard == nil && create {
		shard = &indexShard{}
		t.shards[shardNo] = shard
		t.shardCount++
	}
	return shard
}

func (t *catalogTable) indexHead(hash uint64) uint32 {
	shard := t.shardFor(hash, false)
	if shard == nil {
		return 0
	}
	return shard.entries[hash]
}

func (t *catalogTable) indexSet(hash uint64, head uint32) {
	shard := t.shardFor(hash, true)
	if shard.entries == nil {
		shard.entries = make(map[uint64]uint32)
	}
	if _, present := shard.entries[hash]; !present {
		t.indexEntries++
	}
	shard.entries[hash] = head
	shard.peak = max(shard.peak, len(shard.entries))
}

func (t *catalogTable) indexDelete(hash uint64) {
	shard := t.shardFor(hash, false)
	if _, present := shard.entries[hash]; present {
		delete(shard.entries, hash)
		t.indexEntries--
	}
}

// splitIndex moves the one index map into shards. It runs once per table, at
// singleIndexLimit entries.
func (t *catalogTable) splitIndex() {
	old := t.single.entries
	t.single = indexShard{}
	t.shards = new([indexShardCount]*indexShard)
	t.sharded = true
	t.indexEntries = 0
	for hash, head := range old {
		t.indexSet(hash, head)
	}
}

// maintain does one bounded unit of shrink work and reports how much it did,
// whether more is pending, and an error when the unit could not be finished. A
// unit is one index shard rebuilt (about 4,000 entries at 10^6 buckets)
// or one record chunk walked for arena compaction (at most maxRecordChunk
// records), so the table's lock is never held for longer than that. Drops drive
// it; there is no goroutine. The caller holds the table's lock for writing, and
// decides whether to retry after an error.
func (t *catalogTable) maintain() (work int, more bool, err error) {
	if shard := t.nextShardToRebuild(); shard != nil {
		work = len(shard.entries)
		shard.entries = t.copyEntries(shard.entries)
		shard.peak = len(shard.entries)
		return work, t.nextShardToRebuild() != nil || t.compacting || (t.compactWanted && t.hasSparseArena()), nil
	}
	if !t.compacting {
		if !t.compactWanted || !t.hasSparseArena() {
			return 0, false, nil
		}
		t.compacting, t.compactWanted, t.compactCursor = true, false, 0
	}
	work, err = t.compactOneChunk()
	if err != nil {
		// Leave the pass at the chunk that could not move its names, so a
		// later step, once the arena has room, tries it again instead of
		// skipping it for the rest of this pass.
		t.compactCursor--
		return work, false, err
	}
	return work, true, nil
}

func (t *catalogTable) copyEntries(old map[uint64]uint32) map[uint64]uint32 {
	rebuilt := make(map[uint64]uint32, len(old))
	for hash, head := range old {
		rebuilt[hash] = head
	}
	return rebuilt
}

// nextShardToRebuild returns a shard whose live entries fell under a quarter
// of its peak, or nil.
func (t *catalogTable) nextShardToRebuild() *indexShard {
	if !t.sharded {
		return nil
	}
	for step := 0; step < indexShardCount; step++ {
		shardNo := (t.rebuildCursor + step) % indexShardCount
		shard := t.shards[shardNo]
		if shard != nil && shard.peak >= shardRebuildFloor && len(shard.entries)*4 < shard.peak {
			t.rebuildCursor = shardNo
			return shard
		}
	}
	return nil
}

// isSparse reports whether an arena chunk is worth emptying: it is not the
// chunk receiving new names and under a quarter of what it ever took is live.
func (t *catalogTable) isSparse(slabNo int) bool {
	slab := t.arena[slabNo]
	return slab != nil && slabNo != t.arenaTail && slab.live*4 < slab.used
}

func (t *catalogTable) hasSparseArena() bool {
	for slabNo := range t.arena {
		if t.isSparse(slabNo) {
			return true
		}
	}
	return false
}

// compactOneChunk walks one record chunk and moves every name that sits in a
// sparse arena chunk to the arena's tail, then reports how many records it
// looked at and the first name it could not move. When the walk has covered
// every chunk the pass ends.
func (t *catalogTable) compactOneChunk() (int, error) {
	for t.compactCursor < len(t.chunks) && t.chunks[t.compactCursor] == nil {
		t.compactCursor++
	}
	if t.compactCursor >= len(t.chunks) {
		t.compacting = false
		return 0, nil
	}
	chunk := t.chunks[t.compactCursor]
	t.compactCursor++
	for offset := 0; offset < chunk.bump; offset++ {
		rec := &chunk.recs[offset]
		if rec.state == recFree || !t.isSparse(int(rec.nameRef>>arenaOffsetBits)) {
			continue
		}
		if err := t.moveName(rec); err != nil {
			return chunk.bump, err
		}
	}
	return chunk.bump, nil
}

// moveName copies a record's name to the arena's tail. The record's nameRef
// is written under the table's write lock, which every reader of it holds for
// reading. A name that cannot move because the arena is full stays where it is
// and the error is returned: compaction asked for a move it did not get.
func (t *catalogTable) moveName(rec *bucketRec) error {
	oldSlab := int(rec.nameRef >> arenaOffsetBits)
	name := t.name(rec)
	newRef, err := t.storeName(name)
	if err != nil {
		return fmt.Errorf("compacting the name %q of record %d: %w", name, rec.self, err)
	}
	rec.nameRef = newRef
	t.arena[oldSlab].live -= int(rec.nameLen)
	if t.arena[oldSlab].live == 0 {
		t.dropArenaChunk(oldSlab)
	}
	return nil
}

// residentBytes returns what the table holds, from counters kept as it grows
// and shrinks, never from the runtime. The index figure is an estimate (see
// indexEntryBytes); the records and the arena are exact.
func (t *catalogTable) residentBytes() int64 {
	pointers := int64(cap(t.chunks)+cap(t.arena)) * 8
	index := t.indexEntries*indexEntryBytes + t.shardCount*indexShardOverheadBytes
	return int64(reflect.TypeFor[catalogTable]().Size()) + pointers + t.recordBytes + t.arenaBytes + index
}

// forEachLinked calls visit with every linked record and its name, in chunk
// order, under the table's lock the caller holds for the whole walk. It is
// for the callers that must decide over all of a database's buckets in one
// hold (a database drop's unlink); every other walk is a scan.
func (t *catalogTable) forEachLinked(visit func(name string, rec *bucketRec)) {
	for _, chunk := range t.chunks {
		if chunk == nil {
			continue
		}
		for offset := 0; offset < chunk.bump; offset++ {
			if rec := &chunk.recs[offset]; rec.state == recLinked {
				visit(t.name(rec), rec)
			}
		}
	}
}

// linkedCount is the number of records a name finds, uncommitted claims
// included. The caller holds the table's lock for reading.
func (t *catalogTable) linkedCount() int { return t.linked }

// linkedNonPartCount is linkedCount without the parts of spread collections:
// the buckets a client counts. The caller holds the table's lock for reading.
func (t *catalogTable) linkedNonPartCount() int { return t.linked - t.parts }

// recordsStep returns the linked records of chunk number cursor with the
// epoch each was claimed under, and the cursor to pass next, or done when
// cursor is past the last chunk. It is scanStep for a caller that must lock
// each record afterwards, outside the table's lock, and so must be able to
// tell a record recycled meanwhile.
func (t *catalogTable) recordsStep(cursor int) (held []heldRecord, next int, done bool) {
	for cursor < len(t.chunks) && t.chunks[cursor] == nil {
		cursor++
	}
	if cursor >= len(t.chunks) {
		return nil, cursor, true
	}
	chunk := t.chunks[cursor]
	for offset := 0; offset < chunk.bump; offset++ {
		if rec := &chunk.recs[offset]; rec.state == recLinked {
			held = append(held, heldRecord{rec: rec, epoch: rec.epoch.Load()})
		}
	}
	return held, cursor + 1, false
}

// scanStep returns the names of the linked records in chunk number cursor and
// the cursor to pass next, or done when cursor is past the last chunk. A walk
// is one call per chunk, each under the table's read lock, with the callback
// outside it (spec section 5.4, the scan contract). Records never move, so a
// name present for the whole walk is returned exactly once.
func (t *catalogTable) scanStep(cursor int) (names []string, next int, done bool) {
	for cursor < len(t.chunks) && t.chunks[cursor] == nil {
		cursor++
	}
	if cursor >= len(t.chunks) {
		return nil, cursor, true
	}
	chunk := t.chunks[cursor]
	for offset := 0; offset < chunk.bump; offset++ {
		if rec := &chunk.recs[offset]; rec.state == recLinked {
			names = append(names, t.name(rec))
		}
	}
	return names, cursor + 1, false
}

// scan calls visit with the names of each record chunk in turn, outside lock.
func (t *catalogTable) scan(lock *sync.RWMutex, visit func(names []string)) {
	for cursor := 0; ; {
		lock.RLock()
		names, next, done := t.scanStep(cursor)
		lock.RUnlock()
		if done {
			return
		}
		cursor = next
		if len(names) > 0 {
			visit(names)
		}
	}
}
