package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// hoardLabel is the engine label every hoardDB leg record and table carries.
	hoardLabel      = "hoardDB (hu)"
	hoardLayout     = "hu"
	hoardBucketStem = "User"
	hoardOpTimeout  = 30 * time.Second
	ownerScanLimit  = 1000 // an owner holds -owner-fanout records; the cap only bounds a bad flag
)

// hoardCaller is the one wire operation the store needs; *wireConn is the real
// one and a test passes an in-memory server.
type hoardCaller interface {
	call(op transport.Opcode, request, response interface{}) error
	close()
}

// hoardStore keeps each owner's records in that owner's own hash bucket,
// User_<owner> (the hu layout of bench/social): a record is a document keyed by
// its decimal id, so a point read addresses the bucket directly from the key
// and the owner read is a scan of that one bucket, with no secondary index.
type hoardStore struct {
	database string
	fanout   int64
	durable  string
	slots    chan hoardCaller // nil means "dial on first use"
	dial     func() (hoardCaller, error)
}

// newHoardStore connects to the node at addr. durable is the server's
// durability settings as the launching script states them; the client cannot
// read them back, so it records the words it was given.
func newHoardStore(addr, password, database, durable string, fanout int64, pool int) *hoardStore {
	store := &hoardStore{database: database, fanout: fanout, durable: durable, slots: make(chan hoardCaller, pool)}
	store.dial = func() (hoardCaller, error) { return dialWire(addr, password, hoardOpTimeout) }
	for range pool {
		store.slots <- nil
	}
	return store
}

func (h *hoardStore) name() string   { return hoardLabel }
func (h *hoardStore) layout() string { return hoardLayout }
func (h *hoardStore) durability() string {
	if h.durable == "" {
		return "not stated (run through scripts/bench/competitor-scale-sweep.sh to record the server's settings)"
	}
	return h.durable
}

func (h *hoardStore) close() {
	for range cap(h.slots) {
		if conn := <-h.slots; conn != nil {
			conn.close()
		}
	}
}

// acquire takes a connection from the pool, dialing when its slot is empty.
func (h *hoardStore) acquire() (hoardCaller, error) {
	conn := <-h.slots
	if conn != nil {
		return conn, nil
	}
	conn, err := h.dial()
	if err != nil {
		h.slots <- nil
		return nil, err
	}
	return conn, nil
}

// release returns the connection; one that can no longer be trusted is closed
// and its slot is left to be re-dialed.
func (h *hoardStore) release(conn hoardCaller, err error) {
	if isDesynced(err) {
		conn.close()
		h.slots <- nil
		return
	}
	h.slots <- conn
}

func (h *hoardStore) call(op transport.Opcode, request, response interface{}) error {
	conn, err := h.acquire()
	if err != nil {
		return err
	}
	err = conn.call(op, request, response)
	h.release(conn, err)
	return err
}

func (h *hoardStore) bucketOfOwner(owner int64) string {
	return hoardBucketStem + "_" + strconv.FormatInt(owner, 10)
}
func (h *hoardStore) bucketOfID(id int64) string { return h.bucketOfOwner(id / h.fanout) }

func hoardDoc(rec record) map[string]interface{} {
	return map[string]interface{}{"id": rec.ID, "owner": rec.Owner, "seq": rec.Seq, "body": rec.Body}
}

// absent reports a refusal that means the bucket does not exist.
func absent(err error) bool {
	var refusal *serverError
	return errors.As(err, &refusal) && refusal.code == transport.ErrNotFound
}

// prepare creates the database when missing; the buckets are made as owners appear.
func (h *hoardStore) prepare(context.Context) error {
	var created server.CreateDatabaseResponse
	if err := h.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: h.database}, &created); err != nil {
		var refusal *serverError
		if errors.As(err, &refusal) && refusal.code == transport.ErrAlreadyExists {
			return nil
		}
		return fmt.Errorf("creating database %q: %w", h.database, err)
	}
	if !created.Success && !strings.Contains(created.Message, "already exists") {
		return fmt.Errorf("creating database %q: %s", h.database, created.Message)
	}
	return nil
}

func (h *hoardStore) createBucket(bucket string) error {
	var created server.CreateBucketResponse
	err := h.call(transport.OpCreateBucket, server.CreateBucketRequest{Database: h.database, Name: bucket, Type: server.BucketTypeHash}, &created)
	if err != nil {
		var refusal *serverError
		if errors.As(err, &refusal) && refusal.code == transport.ErrAlreadyExists {
			return nil
		}
		return fmt.Errorf("creating bucket %q: %w", bucket, err)
	}
	if !created.Success && !strings.Contains(created.Message, "already exists") {
		return fmt.Errorf("creating bucket %q: %s", bucket, created.Message)
	}
	return nil
}

// scanBucket reads up to limit documents of a bucket; found is false when the
// bucket does not exist.
func (h *hoardStore) scanBucket(bucket string, limit int) (docs []bson.Raw, found bool, err error) {
	var page server.ScanResponse
	if err := h.call(transport.OpScan, server.ScanRequest{Database: h.database, Bucket: bucket, Limit: limit}, &page); err != nil {
		if absent(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("reading bucket %q: %w", bucket, err)
	}
	return page.Documents, page.Found, nil
}

func (h *hoardStore) ownerExists(owner int64) (bool, error) {
	_, found, err := h.scanBucket(h.bucketOfOwner(owner), 1)
	return found, err
}

// nextKey finds the highest owner bucket by doubling then bisecting (owners
// are created in key order, so the buckets that exist are contiguous from 0),
// then the highest id inside it. A hole at the very frontier left by a failed
// insert is the one case this misjudges; it costs a few error counts, not data.
func (h *hoardStore) nextKey(context.Context) (int64, error) {
	if ok, err := h.ownerExists(0); err != nil || !ok {
		return 0, err
	}
	low, high := int64(0), int64(1)
	for {
		ok, err := h.ownerExists(high)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		low, high = high, high*2
	}
	for high-low > 1 {
		mid := (low + high) / 2
		ok, err := h.ownerExists(mid)
		if err != nil {
			return 0, err
		}
		if ok {
			low = mid
		} else {
			high = mid
		}
	}
	docs, _, err := h.scanBucket(h.bucketOfOwner(low), ownerScanLimit)
	if err != nil {
		return 0, err
	}
	top := low*h.fanout - 1
	for _, raw := range docs {
		var row struct {
			ID int64 `bson:"id"`
		}
		if err := bson.Unmarshal(raw, &row); err != nil {
			return 0, fmt.Errorf("decoding a record of owner %d: %w", low, err)
		}
		top = max(top, row.ID)
	}
	return top + 1, nil
}

// loadRange creates the owner buckets the keys fall in and writes each owner's
// records with one batch insert per bucket (the bulk verb the wire has, used
// for the load between points only; the measured insert is a single put).
func (h *hoardStore) loadRange(ctx context.Context, from, to int64, bodyBytes int) error {
	firstOwner, lastOwner := from/h.fanout, (to-1)/h.fanout
	return parallelBatches(ctx, firstOwner, lastOwner+1, func(ownerFrom, ownerTo int64) error {
		for owner := ownerFrom; owner < ownerTo; owner++ {
			bucket := h.bucketOfOwner(owner)
			if err := h.createBucket(bucket); err != nil {
				return err
			}
			var docs []map[string]interface{}
			for id := max(owner*h.fanout, from); id < min((owner+1)*h.fanout, to); id++ {
				doc := hoardDoc(newRecord(id, bodyBytes))
				doc["owner"] = owner
				doc["_id_"] = strconv.FormatInt(id, 10)
				docs = append(docs, doc)
			}
			var stored server.BatchInsertResponse
			if err := h.call(transport.OpBatchPut, server.BatchInsertRequest{Database: h.database, Bucket: bucket, Data: docs}, &stored); err != nil {
				return fmt.Errorf("loading owner %d: %w", owner, err)
			}
			if !stored.Success {
				return fmt.Errorf("loading owner %d: %s", owner, stored.Message)
			}
		}
		return nil
	})
}

func (h *hoardStore) get(_ context.Context, id int64) error {
	var found server.FindResponse
	err := h.call(transport.OpGet, server.FindRequest{Database: h.database, Bucket: h.bucketOfID(id), Key: strconv.FormatInt(id, 10)}, &found)
	if err != nil {
		return fmt.Errorf("reading key %d: %w", id, err)
	}
	if !found.Found {
		return fmt.Errorf("reading key %d: %s", id, found.Message)
	}
	return nil
}

func (h *hoardStore) update(_ context.Context, id, version int64, bodyBytes int) error {
	var updated server.UpdateResponse
	request := server.UpdateRequest{
		Database: h.database, Bucket: h.bucketOfID(id), Key: strconv.FormatInt(id, 10),
		Set: map[string]interface{}{"seq": version, "body": bodyFor(id, version, bodyBytes)},
	}
	if err := h.call(transport.OpUpdate, request, &updated); err != nil {
		return fmt.Errorf("updating key %d: %w", id, err)
	}
	if !updated.Success {
		return fmt.Errorf("updating key %d: %s", id, updated.Message)
	}
	return nil
}

// insert puts one new record. The first insert for a new owner finds no bucket
// and creates it, then retries once; that creation is part of the measured
// insert, because in this layout a new owner is a new bucket.
func (h *hoardStore) insert(_ context.Context, id int64, bodyBytes int) error {
	request := server.InsertRequest{Database: h.database, Bucket: h.bucketOfID(id), Key: strconv.FormatInt(id, 10), Data: hoardDoc(newRecord(id, bodyBytes))}
	request.Data["owner"] = id / h.fanout
	for attempt := range 2 {
		var stored server.InsertResponse
		err := h.call(transport.OpPut, request, &stored)
		switch {
		case err == nil && stored.Success:
			return nil
		case err == nil:
			return fmt.Errorf("inserting key %d: %s", id, stored.Message)
		case absent(err) && attempt == 0:
			if createErr := h.createBucket(request.Bucket); createErr != nil {
				return createErr
			}
		default:
			return fmt.Errorf("inserting key %d: %w", id, err)
		}
	}
	return fmt.Errorf("inserting key %d: the bucket was created but the put was still refused", id)
}

// byOwner reads one owner's whole bucket: the layout's own equivalent of an
// index lookup, since the owner is the bucket.
func (h *hoardStore) byOwner(_ context.Context, owner int64) (int, error) {
	docs, found, err := h.scanBucket(h.bucketOfOwner(owner), ownerScanLimit)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("reading owner %d: no bucket %s", owner, h.bucketOfOwner(owner))
	}
	return len(docs), nil
}
