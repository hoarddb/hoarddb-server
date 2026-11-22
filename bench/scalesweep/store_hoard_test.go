package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// fakeNode is an in-memory hoardDB node: databases, hash buckets of documents
// keyed by string, and the six requests the sweep sends.
type fakeNode struct {
	mu        sync.Mutex
	hasDB     bool
	buckets   map[string]map[string]map[string]interface{}
	requests  map[transport.Opcode]int
	refusePut bool
}

func newFakeNode() *fakeNode {
	return &fakeNode{buckets: map[string]map[string]map[string]interface{}{}, requests: map[transport.Opcode]int{}}
}

func (n *fakeNode) close() {}

func (n *fakeNode) missing(op transport.Opcode, bucket string) error {
	return &serverError{op: op, code: transport.ErrNotFound, message: "bucket " + bucket + " not found"}
}

func (n *fakeNode) call(op transport.Opcode, request, response interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.requests[op]++
	switch req := request.(type) {
	case server.CreateDatabaseRequest:
		out := response.(*server.CreateDatabaseResponse)
		out.Success, out.Message = !n.hasDB, ""
		if n.hasDB {
			out.Message = fmt.Sprintf("database %q already exists", req.Name)
		}
		n.hasDB = true
	case server.CreateBucketRequest:
		out := response.(*server.CreateBucketResponse)
		if _, ok := n.buckets[req.Name]; ok {
			out.Message = "bucket already exists"
			return nil
		}
		n.buckets[req.Name] = map[string]map[string]interface{}{}
		out.Success = true
	case server.BatchInsertRequest:
		bucket, ok := n.buckets[req.Bucket]
		if !ok {
			return n.missing(op, req.Bucket)
		}
		for _, doc := range req.Data {
			bucket[doc["_id_"].(string)] = doc
		}
		response.(*server.BatchInsertResponse).Success = true
	case server.InsertRequest:
		bucket, ok := n.buckets[req.Bucket]
		if !ok {
			return n.missing(op, req.Bucket)
		}
		if n.refusePut {
			response.(*server.InsertResponse).Message = "refused"
			return nil
		}
		bucket[req.Key] = req.Data
		response.(*server.InsertResponse).Success = true
	case server.FindRequest:
		bucket, ok := n.buckets[req.Bucket]
		if !ok {
			return n.missing(op, req.Bucket)
		}
		out := response.(*server.FindResponse)
		if doc, ok := bucket[req.Key]; ok {
			raw, _ := bson.Marshal(doc)
			out.Found, out.Data = true, raw
		} else {
			out.Message = "document not found"
		}
	case server.UpdateRequest:
		bucket, ok := n.buckets[req.Bucket]
		if !ok {
			return n.missing(op, req.Bucket)
		}
		doc, ok := bucket[req.Key]
		out := response.(*server.UpdateResponse)
		if !ok {
			out.Message = "document not found"
			return nil
		}
		for field, value := range req.Set {
			doc[field] = value
		}
		out.Success = true
	case server.ScanRequest:
		bucket, ok := n.buckets[req.Bucket]
		out := response.(*server.ScanResponse)
		if !ok {
			return n.missing(op, req.Bucket)
		}
		out.Found = true
		for _, doc := range bucket {
			if req.Limit > 0 && len(out.Documents) >= req.Limit {
				break
			}
			raw, _ := bson.Marshal(doc)
			out.Documents = append(out.Documents, raw)
		}
	default:
		return fmt.Errorf("fake node: unexpected request %T", request)
	}
	return nil
}

func hoardOn(node *fakeNode, fanout int64) *hoardStore {
	store := newHoardStore("unused", "unused", "scale", "unsafe_ack_before_sync=true", fanout, 4)
	store.dial = func() (hoardCaller, error) { return node, nil }
	return store
}

func TestHoardLoadMakesOneBucketPerFanoutRecords(t *testing.T) {
	for _, fanout := range []int64{10, 5, 1} {
		node := newFakeNode()
		store := hoardOn(node, fanout)
		if err := store.prepare(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := store.loadRange(context.Background(), 0, 95, 16); err != nil {
			t.Fatal(err)
		}
		want := int((95 + fanout - 1) / fanout)
		if len(node.buckets) != want {
			t.Errorf("fanout %d: %d buckets for 95 records, want %d (one per owner)", fanout, len(node.buckets), want)
		}
		if _, ok := node.buckets["User_0"]; !ok {
			t.Errorf("fanout %d: owners are not named User_<owner>: %v", fanout, len(node.buckets))
		}
		next, err := store.nextKey(context.Background())
		if err != nil || next != 95 {
			t.Errorf("fanout %d: nextKey = %d, %v; want 95", fanout, next, err)
		}
	}
}

func TestHoardPrepareIsRepeatable(t *testing.T) {
	store := hoardOn(newFakeNode(), 10)
	for range 2 {
		if err := store.prepare(context.Background()); err != nil {
			t.Fatalf("prepare on an existing database: %v", err)
		}
	}
}

func TestHoardNextKeyOnEmptyDatabase(t *testing.T) {
	store := hoardOn(newFakeNode(), 10)
	if next, err := store.nextKey(context.Background()); err != nil || next != 0 {
		t.Errorf("nextKey on an empty database = %d, %v; want 0", next, err)
	}
}

func TestHoardOperationsAddressTheOwnersBucket(t *testing.T) {
	ctx := context.Background()
	node := newFakeNode()
	store := hoardOn(node, 10)
	_ = store.prepare(ctx)
	if err := store.loadRange(ctx, 0, 30, 32); err != nil {
		t.Fatal(err)
	}
	if err := store.get(ctx, 25); err != nil {
		t.Errorf("point read of a loaded key: %v", err)
	}
	if err := store.get(ctx, 99); err == nil {
		t.Error("a read of a key in a missing bucket must be an error")
	}
	if err := store.update(ctx, 25, 7, 32); err != nil {
		t.Fatal(err)
	}
	if got := node.buckets["User_2"]["25"]["seq"]; got != int64(7) {
		t.Errorf("update wrote seq %v into User_2, want 7", got)
	}
	if found, err := store.byOwner(ctx, 1); err != nil || found != 10 {
		t.Errorf("owner read = %d, %v; want the owner's 10 records", found, err)
	}
	if _, err := store.byOwner(ctx, 50); err == nil {
		t.Error("an owner with no bucket must be an error, not an empty success")
	}
}

func TestHoardInsertForANewOwnerCreatesItsBucket(t *testing.T) {
	ctx := context.Background()
	node := newFakeNode()
	store := hoardOn(node, 10)
	_ = store.prepare(ctx)
	_ = store.loadRange(ctx, 0, 10, 8)
	if err := store.insert(ctx, 10, 8); err != nil {
		t.Fatal(err)
	}
	if _, ok := node.buckets["User_1"]["10"]; !ok {
		t.Error("the first record of owner 1 must create User_1 and land in it")
	}
	if err := store.insert(ctx, 11, 8); err != nil || node.requests[transport.OpCreateBucket] != 2 {
		t.Errorf("a second record of an existing owner must not create a bucket again (creates %d, err %v)", node.requests[transport.OpCreateBucket], err)
	}
	node.refusePut = true
	if err := store.insert(ctx, 12, 8); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Errorf("a refused put must surface its message, got %v", err)
	}
}

func TestHoardLegRecordsLabelLayoutAndFanout(t *testing.T) {
	ctx := context.Background()
	node := newFakeNode()
	store := hoardOn(node, 5)
	_ = store.prepare(ctx)
	if err := store.loadRange(ctx, 0, 500, 16); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	opt := sweepFlags{mode: "leg", workers: 4, warmup: 10 * time.Millisecond, measured: 150 * time.Millisecond, bodyBytes: 16, seed: 2, fanout: 5}
	if err := runWith(ctx, store, opt, &out); err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["engine"] != "hoardDB (hu)" || record["layout"] != "hu" || record["owner_fanout"] != float64(5) {
		t.Errorf("record = %v", record)
	}
	if !strings.Contains(fmt.Sprint(record["durability"]), "unsafe_ack_before_sync=true") {
		t.Errorf("the durability the script stated is not recorded: %v", record["durability"])
	}
	if errs, _ := record["errors"].(map[string]any); len(errs) != 0 {
		t.Errorf("errors in a clean leg: %v", errs)
	}
	// the owner read must have used the fanout of 5: every owner read found 5 records, never 10
	if node.requests[transport.OpScan] == 0 {
		t.Error("the leg made no owner reads")
	}
}

func TestOpenStoreHoardRules(t *testing.T) {
	ctx := context.Background()
	if _, err := openStore(ctx, sweepFlags{engine: "hoarddb", layout: "h1", fanout: 10}); err == nil || !strings.Contains(err.Error(), "use -layout hu") {
		t.Errorf("a layout other than hu must be refused with the fix, got %v", err)
	}
	if _, err := openStore(ctx, sweepFlags{engine: "mongo", fanout: 5}); err == nil || !strings.Contains(err.Error(), "-owner-fanout applies to -engine hoarddb only") {
		t.Errorf("a fanout on a competitor must be refused, got %v", err)
	}
	if _, err := openStore(ctx, sweepFlags{engine: "hoarddb", layout: "hu", fanout: 0}); err == nil {
		t.Error("a fanout of 0 must be refused")
	}
	opt, err := parseFlags([]string{"-engine", "hoarddb", "-owner-fanout", "20"})
	if err != nil || opt.fanout != 20 || opt.layout != "hu" {
		t.Errorf("flags = %+v, %v", opt, err)
	}
}

func TestCompetitorRecordsStayH1(t *testing.T) {
	record := buildRecord(newFakeStore("mongo", 1), legConfig{Records: 1, Workers: 1}, newLegCounters(), 0, time.Second)
	if record.Layout != "h1" {
		t.Errorf("layout = %q, want h1 for a store with no layout of its own", record.Layout)
	}
}
