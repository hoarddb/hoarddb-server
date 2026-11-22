package wire

import (
	"container/list"
	"sync"
)

// bucketIDCacheSize is how many bucket ids a node remembers. One cache serves the whole
// node, not each bucket (rule 11): 512 entries cost about 150 bytes of map and list each
// plus the two names, so roughly 100 KiB with ordinary names and under 1.2 MiB if every
// name is the 1021-byte maximum.
const bucketIDCacheSize = 512

// bucketIDCache remembers which database and bucket a bucket id was last found in, so a
// keyed request does not ask every database's reverse index for a bucket it has just served.
// An entry is a hint, never an answer: the caller checks it against the bucket's current
// declaration before using it and drops it when it no longer holds (a dropped or recreated
// bucket has another id, a dropped database has no row), so nothing invalidates it from
// outside and it can be at most one declaration read behind.
type bucketIDCache struct {
	mu      sync.Mutex
	limit   int
	order   *list.List // front = most recently used
	entries map[uint64]*list.Element
}

type bucketIDEntry struct {
	id       uint64
	db, name string
}

func newBucketIDCache(limit int) *bucketIDCache {
	return &bucketIDCache{limit: limit, order: list.New(), entries: make(map[uint64]*list.Element, limit)}
}

// get returns the database and bucket name remembered for id and marks them recently used.
func (c *bucketIDCache) get(id uint64) (db, name string, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, found := c.entries[id]
	if !found {
		return "", "", false
	}
	c.order.MoveToFront(element)
	entry := element.Value.(*bucketIDEntry)
	return entry.db, entry.name, true
}

// put remembers that id is bucket name of database db, evicting the least recently used
// entry when the cache is full.
func (c *bucketIDCache) put(id uint64, db, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, found := c.entries[id]; found {
		element.Value = &bucketIDEntry{id: id, db: db, name: name}
		c.order.MoveToFront(element)
		return
	}
	c.entries[id] = c.order.PushFront(&bucketIDEntry{id: id, db: db, name: name})
	if c.order.Len() > c.limit {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*bucketIDEntry).id)
	}
}

// forget drops id.
func (c *bucketIDCache) forget(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, found := c.entries[id]; found {
		c.order.Remove(element)
		delete(c.entries, id)
	}
}

func (c *bucketIDCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// reset forgets everything.
func (c *bucketIDCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.order.Init()
	clear(c.entries)
}
