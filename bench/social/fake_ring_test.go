package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringModel is an in-memory ring node: documents with scalar fields and store fields, the
// ordering rules of the six store types the layout uses, and a log of the requests it served.
// It is the fake ring API of the unit tests; the live test runs the same operations against a
// real node.
type ringModel struct {
	mu     sync.Mutex
	docs   map[string]*modelDoc // "<bucket>/<id>"
	log    []string
	leases map[[16]byte]modelLease
	leased atomic.Uint64
	ids    atomic.Uint64
	// applied holds the request ids of the pushes already applied, as the owner's dedupe window does.
	applied map[[16]byte]bool
	// batchSends logs every RING_BATCH frame the model received, in order.
	batchSends []batchSend
	// batchApplied holds (request id, operation index) of the batch operations already applied, as the dedupe key does.
	batchApplied map[batchKey]bool
	// batchFault, when set, answers operation op of frame send (both counted from 0) with an error and applies nothing.
	batchFault func(send, op int) error
	// batchFailWhole, when set, fails frame send as a whole (the connection broke) after applying its operations, so the outcome is unknown.
	batchFailWhole func(send int) error
}

type batchSend struct {
	id  [16]byte
	ops int
}

type batchKey struct {
	id    [16]byte
	index int
}

type modelDoc struct {
	scalars bson.Raw
	stores  map[string]*modelStore
}

// modelStore keeps elements in insertion order with their keys; kind says how it reads.
type modelStore struct {
	kind     string // fifo, lifo, hash or btree
	keys     []string
	elements []bson.Raw
}

type modelLease struct {
	store    *modelStore
	elements []bson.Raw
}

func newRingModel() *ringModel {
	return &ringModel{docs: map[string]*modelDoc{}, leases: map[[16]byte]modelLease{}}
}

// storeKinds maps the layout's store fields to their kinds.
var storeKinds = map[string]string{
	ringload.UserPosts: "btree", ringload.UserFeed: "lifo", ringload.UserFollowing: "hash", ringload.UserFollowers: "hash",
	ringload.UserInbox: "fifo", ringload.PostLikes: "hash", ringload.PostComments: "fifo", ringload.OutboxJobs: "fifo",
}

func notFound(what string) error {
	return fmt.Errorf("model: no %s: %w", what, ringclient.ErrNotFound)
}

// modelConn is one connection to the model; it counts its own requests.
type modelConn struct {
	model *ringModel
	sent  atomic.Int64
}

func (m *ringModel) connect() *modelConn { return &modelConn{model: m} }

func (c *modelConn) requests() int64                         { return c.sent.Load() }
func (c *modelConn) close()                                  {}
func (c *modelConn) version(context.Context) (string, error) { return "model", nil }

// record logs one request in the form "<verb> <bucket>/<id>[/<field>]".
func (c *modelConn) record(verb string, parts ...string) {
	c.sent.Add(1)
	c.model.log = append(c.model.log, verb+" "+strings.Join(parts, "/"))
}

// requestLog returns the requests served so far, in order.
func (m *ringModel) requestLog() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.log)
}

func (m *ringModel) clearLog() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = nil
}

func (m *ringModel) doc(bucket, id string) *modelDoc { return m.docs[bucket+"/"+id] }

func marshalElement(element any) (bson.Raw, error) {
	switch typed := element.(type) {
	case bson.Raw:
		return typed, nil
	case []byte:
		return typed, nil
	}
	raw, err := bson.Marshal(element)
	return raw, err
}

func (c *modelConn) get(_ context.Context, bucket, id string) (bson.Raw, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("get", bucket, id)
	doc := c.model.doc(bucket, id)
	if doc == nil {
		return nil, notFound("document " + bucket + "/" + id)
	}
	return doc.scalars, nil
}

// getFields is get restricted to the named scalars, as the server's get_fields is: a name that is
// not a declared scalar of the bucket is refused, like the server's FIELD_TYPE error.
func (c *modelConn) getFields(_ context.Context, bucket, id string, fields []string) (bson.Raw, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("get-fields", bucket, id)
	if err := checkScalars(bucket, fields); err != nil {
		return nil, err
	}
	doc := c.model.doc(bucket, id)
	if doc == nil {
		return nil, notFound("document " + bucket + "/" + id)
	}
	return projectScalars(doc.scalars, fields)
}

// getMany answers as the server's RING_GET_MANY does: one answer per id in order, a missing
// document as Found rather than an error, and the same refusal of an undeclared field.
func (c *modelConn) getMany(_ context.Context, bucket string, ids []string, fields []string) ([]ringclient.GetAnswer, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	if err := checkScalars(bucket, fields); err != nil {
		return nil, err
	}
	// One record per call, not per id: the point of the opcode is that a page of one bucket is
	// one request on the wire, and the tests count requests.
	c.record("get-many", bucket, strings.Join(ids, ","))
	answers := make([]ringclient.GetAnswer, len(ids))
	for index, id := range ids {
		doc := c.model.doc(bucket, id)
		if doc == nil {
			continue
		}
		projected, err := projectScalars(doc.scalars, fields)
		if err != nil {
			answers[index].Err = err
			continue
		}
		answers[index] = ringclient.GetAnswer{Doc: projected, Found: true}
	}
	return answers, nil
}

// checkScalars refuses a field name that is not a declared scalar of bucket, as the server does.
func checkScalars(bucket string, fields []string) error {
	for _, name := range fields {
		if !slices.Contains(ringload.ScalarFields(bucket), name) {
			return &ringclient.Error{Code: cluster.CodeFieldType, Message: "field " + name + " is not a scalar of " + bucket}
		}
	}
	return nil
}

// projectScalars keeps the named scalars of a document, in the fields' order, and returns the whole
// document when fields is nil, as the wire's get-many does.
func projectScalars(scalars bson.Raw, fields []string) (bson.Raw, error) {
	if fields == nil {
		return scalars, nil
	}
	var whole map[string]any
	if err := bson.Unmarshal(scalars, &whole); err != nil {
		return nil, err
	}
	kept := bson.D{}
	for _, name := range fields {
		if value, ok := whole[name]; ok {
			kept = append(kept, bson.E{Key: name, Value: value})
		}
	}
	return bson.Marshal(kept)
}

func (c *modelConn) write(verb, bucket, id string, doc ringclient.Doc, mustBeNew bool) error {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record(verb, bucket, id)
	raw, err := bson.Marshal(map[string]any(doc))
	if err != nil {
		return err
	}
	existing := c.model.doc(bucket, id)
	switch {
	case existing != nil && mustBeNew:
		return &ringclient.Error{Code: cluster.CodeDocumentExists, Message: "document exists"}
	case existing != nil:
		existing.scalars = raw
	default:
		c.model.docs[bucket+"/"+id] = &modelDoc{scalars: raw, stores: map[string]*modelStore{}}
	}
	return nil
}

func (c *modelConn) insert(_ context.Context, bucket, id string, doc ringclient.Doc) error {
	return c.write("insert", bucket, id, doc, true)
}

func (c *modelConn) upsert(_ context.Context, bucket, id string, doc ringclient.Doc) error {
	return c.write("upsert", bucket, id, doc, false)
}

// storeOf finds a store field of an existing document, creating the empty store on first use.
func (c *modelConn) storeOf(ref storeRef) (*modelStore, error) {
	doc := c.model.doc(ref.bucket, ref.id)
	if doc == nil {
		return nil, &ringclient.Error{Code: cluster.CodeNotFound, Message: "no document " + ref.bucket + "/" + ref.id}
	}
	kind, declared := storeKinds[ref.field]
	if !declared {
		return nil, &ringclient.Error{Code: cluster.CodeFieldType, Message: "no store field " + ref.field}
	}
	if doc.stores[ref.field] == nil {
		doc.stores[ref.field] = &modelStore{kind: kind}
	}
	return doc.stores[ref.field], nil
}

func (c *modelConn) push(_ context.Context, ref storeRef, element any) error {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("push", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return err
	}
	raw, err := marshalElement(element)
	if err != nil {
		return err
	}
	store.keys, store.elements = append(store.keys, ""), append(store.elements, raw)
	return nil
}

func (c *modelConn) nextRequestID() [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], c.model.ids.Add(1))
	return id
}

// pushWithID applies the push once per request id: a repeat is answered, not applied again.
func (c *modelConn) pushWithID(ctx context.Context, ref storeRef, element any, id [16]byte) error {
	c.model.mu.Lock()
	seen := c.model.applied[id]
	c.model.mu.Unlock()
	if seen {
		return nil
	}
	if err := c.push(ctx, ref, element); err != nil {
		return err
	}
	c.model.mu.Lock()
	if c.model.applied == nil {
		c.model.applied = map[[16]byte]bool{}
	}
	c.model.applied[id] = true
	c.model.mu.Unlock()
	return nil
}

func (c *modelConn) put(_ context.Context, ref storeRef, key string, element any) error {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("put", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return err
	}
	raw, err := marshalElement(element)
	if err != nil {
		return err
	}
	if at := slices.Index(store.keys, key); at >= 0 {
		store.elements[at] = raw
		return nil
	}
	store.keys, store.elements = append(store.keys, key), append(store.elements, raw)
	return nil
}

func (c *modelConn) take(_ context.Context, ref storeRef, key string) error {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("take", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return err
	}
	at := slices.Index(store.keys, key)
	if at < 0 {
		return notFound("element " + key)
	}
	store.keys, store.elements = slices.Delete(store.keys, at, at+1), slices.Delete(store.elements, at, at+1)
	return nil
}

func (c *modelConn) storeGet(_ context.Context, ref storeRef, key string) (bson.Raw, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("get-key", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return nil, err
	}
	at := slices.Index(store.keys, key)
	if at < 0 {
		return nil, notFound("element " + key)
	}
	return store.elements[at], nil
}

// listOrder is the order a list reads in on the real node: a btree by key and a hash by key,
// and a fifo and a lifo both oldest first.
func (s *modelStore) listOrder() []bson.Raw {
	order := make([]int, len(s.elements))
	for index := range order {
		order[index] = index
	}
	switch s.kind {
	case "btree", "hash":
		slices.SortFunc(order, func(a, b int) int { return strings.Compare(s.keys[a], s.keys[b]) })
	}
	ordered := make([]bson.Raw, len(order))
	for index, at := range order {
		ordered[index] = s.elements[at]
	}
	return ordered
}

// popOrder is the order a pop would take: a lifo newest first, the rest as listed.
func (s *modelStore) popOrder() []bson.Raw {
	ordered := s.listOrder()
	if s.kind == "lifo" {
		slices.Reverse(ordered)
	}
	return ordered
}

func (c *modelConn) peek(_ context.Context, ref storeRef, count int) ([]bson.Raw, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("peek", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return nil, err
	}
	ordered := store.popOrder()
	return ordered[:min(count, len(ordered))], nil
}

func (c *modelConn) list(_ context.Context, ref storeRef, limit int, after []byte) (ringclient.ElementPage, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("list", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return ringclient.ElementPage{}, err
	}
	ordered := store.listOrder()
	start := 0
	if len(after) > 0 {
		start, _ = strconv.Atoi(string(after))
	}
	end := min(start+limit, len(ordered))
	page := ringclient.ElementPage{Elements: ordered[start:end], More: end < len(ordered)}
	if page.More {
		page.Cursor = []byte(strconv.Itoa(end))
	}
	return page, nil
}

func (c *modelConn) count(_ context.Context, ref storeRef) (int64, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("count", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return 0, err
	}
	return int64(len(store.elements)), nil
}

func (c *modelConn) claim(_ context.Context, ref storeRef, count int) (ringclient.Lease, error) {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("claim", ref.bucket, ref.id, ref.field)
	store, err := c.storeOf(ref)
	if err != nil {
		return ringclient.Lease{}, err
	}
	ordered := store.popOrder()
	held := slices.Clone(ordered[:min(count, len(ordered))])
	var id [16]byte
	id[0], id[15] = 1, byte(c.model.leased.Add(1))
	c.model.leases[id] = modelLease{store: store, elements: held}
	return ringclient.Lease{ID: id, Elements: held}, nil
}

func (c *modelConn) ack(_ context.Context, ref storeRef, lease [16]byte) error {
	c.model.mu.Lock()
	defer c.model.mu.Unlock()
	c.record("ack", ref.bucket, ref.id, ref.field)
	held, live := c.model.leases[lease]
	if !live {
		return &ringclient.Error{Code: cluster.CodeLeaseLapsed, Message: "lease gone"}
	}
	delete(c.model.leases, lease)
	for _, element := range held.elements {
		if at := slices.IndexFunc(held.store.elements, func(other bson.Raw) bool { return slices.Equal(other, element) }); at >= 0 {
			held.store.keys, held.store.elements = slices.Delete(held.store.keys, at, at+1), slices.Delete(held.store.elements, at, at+1)
		}
	}
	return nil
}

var _ ringAPI = (*modelConn)(nil)

// ringModelEngine is a ring engine whose connections reach the model; opened counts the
// connections handed out and live those not yet closed.
func ringModelEngine(cfg *config, model *ringModel) (*ringEngine, *atomic.Int64) {
	var opened atomic.Int64
	return &ringEngine{cfg: cfg, dial: func(context.Context) (ringAPI, error) {
		opened.Add(1)
		return model.connect(), nil
	}}, &opened
}

// sendBatch is one RING_BATCH frame: each operation is applied, refused or deduped on its own,
// and the dedupe key is (request id, index), so resending a frame under its id applies the rest.
func (c *modelConn) sendBatch(ctx context.Context, stores []storeRef, element any, id [16]byte) ([]error, error) {
	m := c.model
	m.mu.Lock()
	send := len(m.batchSends)
	m.batchSends = append(m.batchSends, batchSend{id: id, ops: len(stores)})
	m.log = append(m.log, fmt.Sprintf("batch %d", len(stores)))
	m.mu.Unlock()
	c.sent.Add(1)
	answers := make([]error, len(stores))
	for index, store := range stores {
		if m.batchFault != nil {
			if refusal := m.batchFault(send, index); refusal != nil {
				answers[index] = refusal
				continue
			}
		}
		key := batchKey{id: id, index: index}
		m.mu.Lock()
		seen := m.batchApplied[key]
		m.mu.Unlock()
		if seen {
			continue
		}
		if err := c.push(ctx, store, element); err != nil {
			answers[index] = err
			continue
		}
		m.mu.Lock()
		if m.batchApplied == nil {
			m.batchApplied = map[batchKey]bool{}
		}
		m.batchApplied[key] = true
		m.mu.Unlock()
	}
	if m.batchFailWhole != nil {
		if err := m.batchFailWhole(send); err != nil {
			return nil, err
		}
	}
	return answers, nil
}

// sends returns the frames received so far.
func (m *ringModel) sends() []batchSend {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.batchSends)
}
