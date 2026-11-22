package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// fakeServer is an in-memory stand-in for a hoardDB server, enough of the
// field-store and queue verbs for the social operations. It checks what the
// real server checks that the operations could get wrong: a field verb needs
// its parent document (and, for an L3 store, its L2 element), `after` is
// exclusive, a heap orders by the request's Priority, and a verb the field's
// engine lacks is refused.
type fakeServer struct {
	mutex      sync.Mutex
	documents  map[string]bson.M // bucket|key -> document
	stores     map[string]*fakeStore
	engines    map[string]string // field name -> engine word, as declared
	queues     map[string]*fakeQueue
	nextID     int
	connection atomic.Int64 // sessions opened, for tests that count them
	// callDelay is how long a counted call stays open, in nanoseconds.
	callDelay atomic.Int64
	// reverseFeedPeeks makes every peek of a Feed answer oldest first, a wrong
	// answer the read checks must catch.
	reverseFeedPeeks bool
	// expectBucket, when set, names the one bucket the document of a key lives
	// in; a request for the key in any other bucket is refused as a missing
	// bucket would be, so a client that routes a user to the wrong shard fails.
	expectBucket func(key string) string
	// nodesOf, when set, answers a describe: the replica set of a bucket, owner
	// first. Nil answers a standalone server, which names no nodes.
	nodesOf func(bucket string) []string
	// created is every create-bucket request, in order.
	created []server.CreateBucketRequest
	// touched is every field verb that reached a store, in order.
	touched []fakeTouch
}

// fakeTouch is one field verb as the fake saw it.
type fakeTouch struct {
	op          transport.Opcode
	bucket, key string
	field       string
}

// checkBucket refuses a request that addresses a user in the wrong bucket.
func (s *fakeServer) checkBucket(op transport.Opcode, bucket, key string) error {
	if s.expectBucket == nil {
		return nil
	}
	if want := s.expectBucket(key); bucket != want {
		return refusal(op, "Bucket %q not found in this database; user %q lives in %q", bucket, key, want)
	}
	return nil
}

// fakeStore is one store instance: a field of one document, or of one L2 element.
type fakeStore struct {
	entries []fakeEntry
	nextPos int64
}

type fakeEntry struct {
	key      string
	position int64
	priority int64
	document bson.M
	content  []byte
}

type fakeQueue struct {
	entries []fakeQueueEntry
	leases  map[string][]string
	nextID  int
}

type fakeQueueEntry struct {
	id       string
	document bson.M
	leased   bool
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		documents: map[string]bson.M{}, stores: map[string]*fakeStore{}, queues: map[string]*fakeQueue{},
		engines: map[string]string{
			"Posts": "btree", "Likes": "hash", "Comments": "fifo", "Feed": "lifo",
			"Following": "hash", "Followers": "hash", "Inbox": "fifo", "Media": "blob",
		},
	}
}

// fakeRequest is one request the fake saw, for tests that assert what was sent.
type fakeRequest struct {
	op      transport.Opcode
	request interface{}
}

// fakeWire is one connection to a fakeServer.
type fakeWire struct {
	server *fakeServer
	sent   int64
	// log records every request in order.
	log []fakeRequest
	// inFlight is shared by every connection of one test: it is the number of
	// calls executing at this instant, and maxInFlight the peak.
	inFlight, maxInFlight *atomic.Int64
}

func (s *fakeServer) dial(inFlight, maxInFlight *atomic.Int64) *fakeWire {
	s.connection.Add(1)
	return &fakeWire{server: s, inFlight: inFlight, maxInFlight: maxInFlight}
}

func (w *fakeWire) requests() int64 { return w.sent }

func (w *fakeWire) call(op transport.Opcode, request, response interface{}) error {
	w.sent++
	w.log = append(w.log, fakeRequest{op: op, request: request})
	if w.inFlight != nil {
		current := w.inFlight.Add(1)
		defer w.inFlight.Add(-1)
		for peak := w.maxInFlight.Load(); current > peak && !w.maxInFlight.CompareAndSwap(peak, current); peak = w.maxInFlight.Load() {
		}
		// A test that watches for overlap sets callDelay so a goroutine started
		// alongside this call reaches its own call while this one is counted;
		// without it the window is microseconds wide and overlapping requests slip
		// through unseen whenever the race detector's slowdown is absent.
		time.Sleep(time.Duration(w.server.callDelay.Load()))
	}
	w.server.mutex.Lock()
	defer w.server.mutex.Unlock()
	return w.server.handle(op, request, response)
}

func storeAddress(bucket, key string, path []interface{}, field string) string {
	parts := []string{bucket, key}
	for _, step := range path {
		parts = append(parts, fmt.Sprint(step))
	}
	return strings.Join(append(parts, field), "|")
}

func refusal(op transport.Opcode, format string, args ...interface{}) error {
	return &serverError{op: op, code: transport.ErrInvalidArgument, message: fmt.Sprintf(format, args...)}
}

func (s *fakeServer) handle(op transport.Opcode, request, response interface{}) error {
	switch typed := request.(type) {
	case struct{}: // a health check carries no request body
		response.(*server.HealthResponse).Version = "v1.0-fake"
	case server.CreateDatabaseRequest:
		response.(*server.CreateDatabaseResponse).Success = true
	case server.DeleteDatabaseRequest:
		response.(*server.DeleteDatabaseResponse).Success = true
	case server.BucketStatsRequest:
		response.(*server.BucketStatsResponse).Found = true
		if s.nodesOf != nil {
			response.(*server.BucketStatsResponse).Nodes = s.nodesOf(typed.Bucket)
		}
	case server.CreateBucketRequest:
		s.created = append(s.created, typed)
		if typed.Type == server.BucketTypeFIFO {
			s.queues[typed.Database+"|"+typed.Name] = &fakeQueue{leases: map[string][]string{}}
		}
		response.(*server.CreateBucketResponse).Success = true
	case server.InsertRequest:
		if err := s.checkBucket(op, typed.Bucket, typed.Key); err != nil {
			return err
		}
		s.documents[typed.Bucket+"|"+typed.Key] = bson.M(typed.Data)
		response.(*server.InsertResponse).Success = true
	case server.FindRequest:
		return s.find(typed, response.(*server.FindResponse))
	case server.UpdateRequest:
		return s.update(op, typed, response.(*server.UpdateResponse))
	case server.FieldRequest:
		return s.fieldVerb(op, typed, response.(*server.FieldResponse))
	case server.FieldPeekRequest:
		return s.fieldPeek(op, typed, response.(*server.FieldResponse))
	case server.FieldPopRequest:
		return s.fieldPop(op, typed, response.(*server.FieldResponse))
	case server.FieldListRequest:
		return s.fieldList(op, typed, response.(*server.FieldListResponse))
	case server.PushRequest:
		return s.queuePush(op, typed, response.(*server.PushResponse))
	case server.ClaimRequest:
		return s.queueClaim(op, typed, response.(*server.ClaimResponse))
	case server.LeaseRequest:
		return s.queueAck(op, typed, response.(*server.LeaseResponse))
	case server.BatchRequest:
		return s.batch(typed, response.(*server.BatchResponse))
	default:
		return refusal(op, "the fake server does not implement %T", request)
	}
	return nil
}

func (s *fakeServer) find(request server.FindRequest, response *server.FindResponse) error {
	if err := s.checkBucket(transport.OpGet, request.Bucket, request.Key); err != nil {
		return err
	}
	document, found := s.documents[request.Bucket+"|"+request.Key]
	response.Found = found
	if found {
		raw, err := bson.Marshal(document)
		if err != nil {
			return fmt.Errorf("fake server encoding a document: %w", err)
		}
		response.Data = raw
	}
	return nil
}

func (s *fakeServer) update(op transport.Opcode, request server.UpdateRequest, response *server.UpdateResponse) error {
	if err := s.checkBucket(op, request.Bucket, request.Key); err != nil {
		return err
	}
	document, found := s.documents[request.Bucket+"|"+request.Key]
	if !found {
		return refusal(op, "no document %q in %q", request.Key, request.Bucket)
	}
	for field, delta := range request.Inc {
		current, _ := document[field].(int64)
		document[field] = current + delta
	}
	response.Success = true
	return nil
}

// resolve finds the store a field request addresses, checking the parent.
func (s *fakeServer) resolve(op transport.Opcode, request server.FieldRequest, verb string) (*fakeStore, string, error) {
	if err := s.checkBucket(op, request.Bucket, request.Key); err != nil {
		return nil, "", err
	}
	s.touched = append(s.touched, fakeTouch{op: op, bucket: request.Bucket, key: request.Key, field: request.Field})
	engine, declared := s.engines[request.Field]
	if !declared {
		return nil, "", refusal(op, "Bucket %q declares no store field named %q", request.Bucket, request.Field)
	}
	ordered := engine == "fifo" || engine == "lifo" || engine == "heap"
	switch verb {
	case "push", "pop", "peek":
		if !ordered {
			return nil, "", refusal(op, "%q is not an operation on %q, which is a %sStore", verb, request.Field, engine)
		}
	case "put", "get", "delete":
		if ordered {
			return nil, "", refusal(op, "%q is not an operation on %q, which is a %sStore", verb, request.Field, engine)
		}
	}
	if _, found := s.documents[request.Bucket+"|"+request.Key]; !found {
		return nil, "", refusal(op, "No document with key %q in bucket %q, so there is no %q store", request.Key, request.Bucket, request.Field)
	}
	if len(request.Path) > 0 {
		ancestor := s.stores[storeAddress(request.Bucket, request.Key, nil, fmt.Sprint(request.Path[0]))]
		if ancestor == nil || ancestor.find(fmt.Sprint(request.Path[1])) < 0 {
			return nil, "", refusal(op, "document %q has no %v element %v", request.Key, request.Path[0], request.Path[1])
		}
	}
	address := storeAddress(request.Bucket, request.Key, request.Path, request.Field)
	instance := s.stores[address]
	if instance == nil {
		instance = &fakeStore{}
		s.stores[address] = instance
	}
	return instance, engine, nil
}

func (f *fakeStore) find(key string) int {
	return slices.IndexFunc(f.entries, func(entry fakeEntry) bool { return entry.key == key })
}

func (s *fakeServer) element(entry fakeEntry) (server.FieldListEntry, error) {
	document := bson.M{"_id_": entry.key}
	for field, value := range entry.document {
		document[field] = value
	}
	raw, err := bson.Marshal(document)
	if err != nil {
		return server.FieldListEntry{}, fmt.Errorf("fake server encoding an element: %w", err)
	}
	position, priority := entry.position, entry.priority
	return server.FieldListEntry{Element: raw, ElementKey: entry.key, Position: &position, Priority: &priority}, nil
}

func (s *fakeServer) fieldVerb(op transport.Opcode, request server.FieldRequest, response *server.FieldResponse) error {
	verbs := map[transport.Opcode]string{
		transport.OpFieldPush: "push", transport.OpFieldPut: "put", transport.OpFieldGet: "get",
		transport.OpFieldDelete: "delete", transport.OpFieldLen: "len",
	}
	instance, _, err := s.resolve(op, request, verbs[op])
	if err != nil {
		return err
	}
	response.Success = true
	switch op {
	case transport.OpFieldPush:
		s.nextID++
		instance.entries = append(instance.entries, fakeEntry{key: fmt.Sprintf("e%d", s.nextID), position: instance.nextPos, priority: request.Priority, document: bson.M(request.Data)})
		instance.nextPos++
	case transport.OpFieldPut:
		entry := fakeEntry{key: request.Element, document: bson.M(request.Data), content: request.Content}
		if index := instance.find(request.Element); index >= 0 {
			instance.entries[index] = entry
		} else {
			instance.entries = append(instance.entries, entry)
		}
	case transport.OpFieldGet:
		index := instance.find(request.Element)
		if index < 0 {
			return nil
		}
		listed, err := s.element(instance.entries[index])
		if err != nil {
			return err
		}
		response.Found, response.Element, response.Content = true, listed.Element, instance.entries[index].content
	case transport.OpFieldDelete:
		if index := instance.find(request.Element); index >= 0 {
			instance.entries = slices.Delete(instance.entries, index, index+1)
			response.Found = true
		}
	case transport.OpFieldLen:
		response.Len = int64(len(instance.entries))
	}
	return nil
}

// popOrder lists the entries in the order a pop of the field would take them.
func popOrder(engine string, entries []fakeEntry) []fakeEntry {
	ordered := slices.Clone(entries)
	switch engine {
	case "lifo":
		slices.Reverse(ordered)
	case "heap":
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].priority > ordered[j].priority })
	}
	return ordered
}

func (s *fakeServer) fieldPeek(op transport.Opcode, request server.FieldPeekRequest, response *server.FieldResponse) error {
	instance, engine, err := s.resolve(op, request.FieldRequest, "peek")
	if err != nil {
		return err
	}
	response.Success = true
	for _, entry := range popOrder(engine, instance.entries)[:min(max(request.Limit, 1), len(instance.entries))] {
		listed, err := s.element(entry)
		if err != nil {
			return err
		}
		response.Items = append(response.Items, listed)
	}
	if s.reverseFeedPeeks && request.Field == "Feed" {
		slices.Reverse(response.Items)
	}
	response.Found = len(response.Items) > 0
	return nil
}

func (s *fakeServer) fieldPop(op transport.Opcode, request server.FieldPopRequest, response *server.FieldResponse) error {
	instance, engine, err := s.resolve(op, request.FieldRequest, "pop")
	if err != nil {
		return err
	}
	response.Success = true
	taken := popOrder(engine, instance.entries)[:min(max(request.Limit, 1), len(instance.entries))]
	for _, entry := range taken {
		listed, err := s.element(entry)
		if err != nil {
			return err
		}
		response.Items = append(response.Items, listed)
		instance.entries = slices.DeleteFunc(instance.entries, func(other fakeEntry) bool { return other.key == entry.key && other.position == entry.position })
	}
	response.Found = len(taken) > 0
	return nil
}

func (s *fakeServer) fieldList(op transport.Opcode, request server.FieldListRequest, response *server.FieldListResponse) error {
	instance, engine, err := s.resolve(op, request.FieldRequest, "list")
	if err != nil {
		return err
	}
	entries := slices.Clone(instance.entries)
	if engine == "btree" {
		sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	}
	if request.After.Type == bson.TypeString {
		after := request.After.StringValue()
		entries = slices.DeleteFunc(entries, func(entry fakeEntry) bool { return entry.key <= after })
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 100
	}
	response.Success = true
	response.HasMore = len(entries) > limit
	for _, entry := range entries[:min(limit, len(entries))] {
		listed, err := s.element(entry)
		if err != nil {
			return err
		}
		response.Entries = append(response.Entries, listed)
	}
	return nil
}

func (s *fakeServer) queuePush(op transport.Opcode, request server.PushRequest, response *server.PushResponse) error {
	queue := s.queues[request.Database+"|"+request.Bucket]
	if queue == nil {
		return refusal(op, "no queue %q", request.Bucket)
	}
	queue.nextID++
	queue.entries = append(queue.entries, fakeQueueEntry{id: fmt.Sprintf("q%d", queue.nextID), document: bson.M(request.Data)})
	response.Success = true
	return nil
}

func (s *fakeServer) queueClaim(op transport.Opcode, request server.ClaimRequest, response *server.ClaimResponse) error {
	queue := s.queues[request.Database+"|"+request.Bucket]
	if queue == nil {
		return refusal(op, "no queue %q", request.Bucket)
	}
	leaseID := fmt.Sprintf("lease-%d", len(queue.leases)+1)
	for index := range queue.entries {
		if queue.entries[index].leased || len(response.ItemIDs) == request.Limit {
			continue
		}
		queue.entries[index].leased = true
		response.ItemIDs = append(response.ItemIDs, queue.entries[index].id)
	}
	if len(response.ItemIDs) > 0 {
		response.Found, response.LeaseID = true, leaseID
		queue.leases[leaseID] = response.ItemIDs
	}
	return nil
}

func (s *fakeServer) queueAck(op transport.Opcode, request server.LeaseRequest, response *server.LeaseResponse) error {
	queue := s.queues[request.Database+"|"+request.Bucket]
	if queue == nil {
		return refusal(op, "no queue %q", request.Bucket)
	}
	for _, id := range request.ItemIDs {
		queue.entries = slices.DeleteFunc(queue.entries, func(entry fakeQueueEntry) bool { return entry.id == id })
		response.Acked++
	}
	response.Success = true
	return nil
}

// batch answers a BATCH as the server does, for the verbs the operations
// batch: each item is decoded and run in order, a refused item answers ERROR,
// and after an item of a bucket that is not a success the later items of
// that bucket are not run. It refuses an item that names a write concern, as
// the server does.
func (s *fakeServer) batch(request server.BatchRequest, response *server.BatchResponse) error {
	stopped := map[string]bool{}
	for _, item := range request.Items {
		op := transport.Opcode(item.Opcode)
		if _, err := item.Payload.LookupErr("writeconcern"); err == nil {
			return refusal(transport.OpBatch, "an item names a write concern; a batch takes one, on the batch")
		}
		bucket, _ := item.Payload.Lookup("bucket").StringValueOK()
		if stopped[bucket] {
			response.Answers = append(response.Answers, fakeErrorAnswer(transport.ErrUnavailable, "Not run: an earlier item for the same bucket was not applied"))
			continue
		}
		typed, reply, err := fakeBatchItem(op, item.Payload)
		if err == nil {
			err = s.handle(op, typed, reply)
		}
		if err != nil {
			stopped[bucket] = true
			response.Answers = append(response.Answers, fakeErrorAnswer(transport.ErrInvalidArgument, err.Error()))
			continue
		}
		payload, marshalErr := bson.Marshal(reply)
		if marshalErr != nil {
			return fmt.Errorf("fake server encoding a batch answer: %w", marshalErr)
		}
		if success, isBool := bson.Raw(payload).Lookup("success").BooleanOK(); isBool && !success {
			stopped[bucket] = true
		}
		response.Answers = append(response.Answers, server.BatchAnswer{Opcode: int32(op), Payload: payload})
	}
	return nil
}

// fakeBatchItem decodes one batch item into the request type the lone verb
// sends, and the reply it answers with.
func fakeBatchItem(op transport.Opcode, payload bson.Raw) (interface{}, interface{}, error) {
	switch op {
	case transport.OpFieldPush, transport.OpFieldPut, transport.OpFieldDelete, transport.OpFieldGet, transport.OpFieldLen:
		var request server.FieldRequest
		return request, &server.FieldResponse{}, bson.Unmarshal(payload, &request)
	case transport.OpFieldPeek:
		var request server.FieldPeekRequest
		err := bson.Unmarshal(payload, &request)
		return request, &server.FieldResponse{}, err
	case transport.OpUpdate:
		var request server.UpdateRequest
		err := bson.Unmarshal(payload, &request)
		return request, &server.UpdateResponse{}, err
	}
	return nil, nil, refusal(transport.OpBatch, "a batch does not carry %s", op)
}

func fakeErrorAnswer(code transport.ErrorCode, message string) server.BatchAnswer {
	payload, err := bson.Marshal(server.ErrorResponse{Code: int(code), Message: message, WriteOutcome: "not_applied"})
	if err != nil {
		panic(err) // test fake: an ErrorResponse always encodes
	}
	return server.BatchAnswer{Opcode: int32(transport.OpError), Payload: payload}
}
