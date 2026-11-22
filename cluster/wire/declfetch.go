package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// systemBucketName is the _buckets system bucket, whose declaration is compiled in (conv 19).
const systemBucketName = "_buckets"

// entryOverheadBytes is what a cached entry costs beyond its stored document: the Entry and
// Declaration headers, the key string header and the slice headers of the decoded table.
// It is a bound, not a measurement, so the byte limit errs toward evicting early.
const entryOverheadBytes = 256

// ErrBucketNotFound is a declaration fetch that found no such bucket. It is never cached.
var ErrBucketNotFound = errors.New("no such bucket")

// ErrDeclUnavailable is a declaration fetch that could not be answered: the slot has no live
// owner, the read failed, or the document did not decode. Entries already cached keep serving.
var ErrDeclUnavailable = errors.New("declaration unavailable")

// DeclarationRead names one keyed read of a declaration or reverse-index document.
type DeclarationRead struct {
	DBID   uint64
	DBName string
	Slot   uint16
	// EncodedID is the EncodeComponent of the document's _id_.
	EncodedID []byte
}

// DeclarationReader is the keyed read of conv 20: one seek at the slot's owner, assigning
// no position. The node that owns the slot reads its own Badger through the same method.
type DeclarationReader interface {
	// ReadDeclaration returns the stored field values (field id to bsonType and value) of the
	// declaration document, or found false when there is none.
	ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (stored map[uint16][]byte, found bool, err error)
	// ReadBucketName returns the name a bucket id belongs to, from its reverse-index document.
	ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (name string, found bool, err error)
}

// FetcherConfig wires a Fetcher to the node.
type FetcherConfig struct {
	Topology Topology
	Reader   DeclarationReader
	// DatabaseName is the name a database id placed under; the placement key uses it.
	DatabaseName func(dbID uint64) (string, bool)
	// Published is this node's published schema_version of a database, read at the call.
	Published func(dbID uint64) int64
	Cache     machine.CacheConfig
}

// DeclCacheStats are the cache's aggregate counters. Every field is a plain number: there is
// no per-bucket label and no per-bucket series (rule 11).
type DeclCacheStats struct {
	// Entries and Bytes are what the cache holds now; both are bounded by the config.
	Entries int
	Bytes   int64
	// Hits served without a round trip; Fetches are keyed reads issued; Collapsed are misses
	// that joined a fetch already in flight; Discarded are fetches that raced a bump;
	// NotCached are fetches the cache refused to keep (dropping, over the size limit).
	Hits, Fetches, Collapsed, Discarded, NotCached uint64
}

// flightKey is the single-flight key: the cache's own key.
type flightKey struct {
	dbID uint64
	name string
}

// flight is one fetch in progress. entry and err are written before done closes.
type flight struct {
	at        int64
	done      chan struct{}
	entry     machine.Entry
	err       error
	forgotten bool
}

// Fetcher is the declaration cache with its single-flight fill (conv 20). The cache is the
// machine's CacheState, so the bounds, LRU order and never-cached rules are the machine's;
// the Fetcher adds the keyed read, the single flight and the lock. Its cost is bounded by
// cluster.decl_cache_entries and cluster.decl_cache_bytes per node and is zero per bucket
// that is not cached: nothing here scales with the bucket count.
type Fetcher struct {
	cfg     FetcherConfig
	mu      sync.Mutex
	cache   machine.CacheState
	flights map[flightKey]*flight
	stats   DeclCacheStats
}

// NewFetcher returns an empty Fetcher.
func NewFetcher(cfg FetcherConfig) *Fetcher {
	return &Fetcher{cfg: cfg, flights: map[flightKey]*flight{}}
}

// Lookup returns the declaration of (dbID, name) for a caller whose published schema_version
// is published. A cache hit costs no round trip. A miss is one keyed read at the declaration
// slot's owner, shared by every concurrent miss for the same name. The entry is tagged with
// published, the version the fetch was issued at; when the published version has moved by the
// time the read returns, the entry is handed back (tagged with the old version, which the
// fence will see as stale) but not stored. A dropping declaration is returned and not stored;
// an absent one is ErrBucketNotFound and a failed read is ErrDeclUnavailable.
//
// The returned entry is a read-only view. A caller that will rewrite the declaration must use
// Read, which keeps field 6.
func (f *Fetcher) Lookup(ctx context.Context, dbID uint64, name string, published int64) (machine.Entry, error) {
	if name == systemBucketName {
		return machine.Entry{DBID: dbID, Name: name, Seen: published, Decl: machine.SystemDeclaration()}, nil
	}
	key := flightKey{dbID, name}
	for {
		f.mu.Lock()
		decision := f.cache.Apply(machine.Lookup{DBID: dbID, Name: name, Published: published}, f.cfg.Cache)
		if decision.Hit {
			f.stats.Hits++
			f.mu.Unlock()
			return decision.Entry, nil
		}
		if running, ok := f.flights[key]; ok {
			f.stats.Collapsed++
			f.mu.Unlock()
			entry, done, err := f.join(ctx, running, published)
			if done {
				return entry, err
			}
			continue
		}
		mine := &flight{at: published, done: make(chan struct{})}
		f.flights[key] = mine
		f.cache.Apply(machine.FetchIssued{DBID: dbID, Name: name, At: published}, f.cfg.Cache)
		f.mu.Unlock()

		entry, err := f.fetch(ctx, dbID, name)
		entry.Seen = published
		f.finish(key, mine, entry, err)
		return mine.entry, mine.err
	}
}

// join waits for a fetch someone else leads. done is false when the caller must look again:
// the flight was issued at another version, or its leader gave up for its own reasons.
func (f *Fetcher) join(ctx context.Context, running *flight, published int64) (entry machine.Entry, done bool, err error) {
	select {
	case <-running.done:
	case <-ctx.Done():
		return machine.Entry{}, true, fmt.Errorf("waiting for the declaration: %w", ctx.Err())
	}
	switch {
	case running.err == nil && running.at == published:
		return running.entry, true, nil
	case running.err == nil:
		return machine.Entry{}, false, nil
	case errors.Is(running.err, context.Canceled) && ctx.Err() == nil:
		return machine.Entry{}, false, nil
	}
	return machine.Entry{}, true, running.err
}

// finish ends flight mine: it stores the entry under the machine's rules, publishes the
// result to the waiters and counts what the cache refused.
func (f *Fetcher) finish(key flightKey, mine *flight, entry machine.Entry, err error) {
	f.mu.Lock()
	delete(f.flights, key)
	switch {
	case err != nil:
		f.cache.Apply(machine.FetchFailed{DBID: key.dbID, Name: key.name}, f.cfg.Cache)
		mine.err = err
	case mine.forgotten:
		mine.entry = entry
	default:
		current := f.cfg.Published(key.dbID)
		decision := f.cache.Apply(machine.Fetched{Entry: entry, IssuedAt: mine.at, Published: current}, f.cfg.Cache)
		switch {
		case decision.Store:
		case mine.at != current:
			f.stats.Discarded++
		default:
			f.stats.NotCached++
		}
		mine.entry = entry
	}
	f.mu.Unlock()
	close(mine.done)
}

// Read fetches a declaration straight from its owner, never from the cache, for a caller that
// will rewrite it. It decodes with the swept bitmap (field 6) so a read-modify-write of a
// dropping declaration keeps its drop progress; swept is nil when the document has none.
func (f *Fetcher) Read(ctx context.Context, dbID uint64, name string) (decl machine.Declaration, swept []byte, found bool, err error) {
	stored, found, err := f.readStored(ctx, dbID, name)
	if err != nil || !found {
		return machine.Declaration{}, nil, false, err
	}
	decl, swept, err = store.DecodeDeclarationWithSwept(stored)
	if err != nil {
		return machine.Declaration{}, nil, false, fmt.Errorf("%w: declaration of bucket %q: %w", ErrDeclUnavailable, name, err)
	}
	return decl, swept, true, nil
}

// fetch is the keyed read of one declaration, decoded and sized.
func (f *Fetcher) fetch(ctx context.Context, dbID uint64, name string) (machine.Entry, error) {
	stored, found, err := f.readStored(ctx, dbID, name)
	if err != nil {
		return machine.Entry{}, err
	}
	if !found {
		return machine.Entry{}, fmt.Errorf("%w: bucket %q does not exist in database %d. Run 'show buckets' to list what exists, or 'create bucket %s in <db> { <Field> string }' to create it",
			ErrBucketNotFound, name, dbID, name)
	}
	decl, err := store.DecodeDeclaration(stored)
	if err != nil {
		return machine.Entry{}, fmt.Errorf("%w: declaration of bucket %q: %w", ErrDeclUnavailable, name, err)
	}
	size := int64(len(name)) + entryOverheadBytes
	for _, raw := range stored {
		size += int64(len(raw))
	}
	return machine.Entry{DBID: dbID, Name: name, Decl: decl, Size: size}, nil
}

// readStored routes the keyed read to the owner of the declaration's slot.
func (f *Fetcher) readStored(ctx context.Context, dbID uint64, name string) (map[uint16][]byte, bool, error) {
	encID, err := store.EncodeComponent(name)
	if err != nil {
		return nil, false, fmt.Errorf("encoding bucket name %q: %w", name, err)
	}
	owner, read, err := f.route(dbID, encID)
	if err != nil {
		return nil, false, err
	}
	f.countFetch()
	stored, found, err := f.cfg.Reader.ReadDeclaration(ctx, owner, read)
	if err != nil {
		return nil, false, fmt.Errorf("%w: reading bucket %q from %s: %w", ErrDeclUnavailable, name, owner.ID, err)
	}
	return stored, found, nil
}

// countFetch counts a keyed read at the moment it is issued, after routing succeeded.
func (f *Fetcher) countFetch() {
	f.mu.Lock()
	f.stats.Fetches++
	f.mu.Unlock()
}

// readName is the keyed read of the reverse-index document that maps a bucket id to its name.
func (f *Fetcher) readName(ctx context.Context, dbID, bucketID uint64) (string, bool, error) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], bucketID)
	encID, err := store.EncodeComponent(raw[:])
	if err != nil {
		return "", false, fmt.Errorf("encoding bucket id %d: %w", bucketID, err)
	}
	owner, read, err := f.route(dbID, encID)
	if err != nil {
		return "", false, err
	}
	name, found, err := f.cfg.Reader.ReadBucketName(ctx, owner, read)
	if err != nil {
		return "", false, fmt.Errorf("%w: reading the name of bucket id %d from %s: %w", ErrDeclUnavailable, bucketID, owner.ID, err)
	}
	return name, found, nil
}

// reverseStateReader is a DeclarationReader that can also read a reverse-index document's state.
type reverseStateReader interface {
	ReadReverseState(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error)
}

// readReverse returns the state word of a bucket id's reverse-index document from its owner.
func (f *Fetcher) readReverse(ctx context.Context, dbID, bucketID uint64) (string, bool, error) {
	reader, ok := f.cfg.Reader.(reverseStateReader)
	if !ok {
		return "", false, fmt.Errorf("%w: this declaration reader cannot read a reverse-index document's state", ErrDeclUnavailable)
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], bucketID)
	encID, err := store.EncodeComponent(raw[:])
	if err != nil {
		return "", false, fmt.Errorf("encoding bucket id %d: %w", bucketID, err)
	}
	owner, read, err := f.route(dbID, encID)
	if err != nil {
		return "", false, err
	}
	state, found, err := reader.ReadReverseState(ctx, owner, read)
	if err != nil {
		return "", false, fmt.Errorf("%w: reading the reverse-index document of bucket id %d from %s: %w", ErrDeclUnavailable, bucketID, owner.ID, err)
	}
	return state, found, nil
}

// route finds the owner of the _buckets document encID of database dbID.
func (f *Fetcher) route(dbID uint64, encID []byte) (cluster.Member, DeclarationRead, error) {
	read, err := f.placement(dbID, encID)
	if err != nil {
		return cluster.Member{}, DeclarationRead{}, err
	}
	row, ok := f.cfg.Topology.Database(dbID)
	if !ok {
		return cluster.Member{}, DeclarationRead{}, fmt.Errorf("%w: database %q has no committed row", ErrDeclUnavailable, read.DBName)
	}
	owner, ok := f.cfg.Topology.Table().Owner(read.Slot, row.RF)
	if !ok {
		return cluster.Member{}, DeclarationRead{}, fmt.Errorf("%w: no live owner for slot %d of database %q. Run 'nodes' to see which node is down, then bring it back or run 'replace-node <node> --with <node>@<host:port>'",
			ErrDeclUnavailable, read.Slot, read.DBName)
	}
	return owner, read, nil
}

// placement is the keyed read of the _buckets document encID of database dbID, without its owner.
func (f *Fetcher) placement(dbID uint64, encID []byte) (DeclarationRead, error) {
	dbName, ok := f.cfg.DatabaseName(dbID)
	if !ok {
		return DeclarationRead{}, fmt.Errorf("%w: database id %d is not in the committed state", ErrDeclUnavailable, dbID)
	}
	slot := storage.SlotOf(storage.PlacementKey(dbName, systemBucketName, encID))
	return DeclarationRead{DBID: dbID, DBName: dbName, Slot: slot, EncodedID: encID}, nil
}

// peekByID returns the cached entry that declares bucketID, stale or not, without touching
// the LRU order or dropping it. A scan of at most MaxEntries small structs.
func (f *Fetcher) peekByID(dbID, bucketID uint64) (machine.Entry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, entry := range f.cache.Entries {
		if entry.DBID == dbID && entry.Decl.BucketID == bucketID {
			return entry, true
		}
	}
	return machine.Entry{}, false
}

// Stats returns the aggregate counters and what the cache holds now.
func (f *Fetcher) Stats() DeclCacheStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := f.stats
	snapshot.Entries = len(f.cache.Entries)
	for _, entry := range f.cache.Entries {
		snapshot.Bytes += entry.Size
	}
	return snapshot
}

// Forget drops every entry of a dropped database and keeps its in-flight fetches from
// storing anything (conv 20). It is one pass over the bounded cache, off the write path.
func (f *Fetcher) Forget(dbID uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cache.Apply(machine.Dropped{DBID: dbID}, f.cfg.Cache)
	for key, running := range f.flights {
		if key.dbID == dbID {
			running.forgotten = true
		}
	}
}
