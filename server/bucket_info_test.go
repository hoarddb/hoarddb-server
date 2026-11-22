package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestBucketInfoOpcodeRegistered pins BUCKET_INFO in every table an opcode
// needs: its code and name, the read classification that keeps it out of the
// replication log, the permission it takes (the same as BUCKET_STATS), and
// the bucket and database the cluster routes it by.
func TestBucketInfoOpcodeRegistered(t *testing.T) {
	op := transport.OpBucketInfo
	if byte(op) != 0xD9 {
		t.Errorf("OpBucketInfo = 0x%02X, want 0xD9", byte(op))
	}
	if got := op.String(); got != "BUCKET_INFO" {
		t.Errorf("OpBucketInfo.String() = %q, want BUCKET_INFO: a log line must not carry a bare integer", got)
	}
	if !op.IsReadOnly() {
		t.Error("OpBucketInfo is not read-only: a read routed as a write is WAL-appended and shipped to every holder")
	}
	if replicationRequired(op) {
		t.Error("replicationRequired(OpBucketInfo) is true")
	}
	if got, want := opRules[op], opRules[transport.OpBucketStats]; got != want {
		t.Errorf("opRules[BUCKET_INFO] = %+v, want BUCKET_STATS's %+v: the same read permission", got, want)
	}
	payload, err := bson.Marshal(BucketInfoRequest{Database: "app", Bucket: "Post"})
	if err != nil {
		t.Fatalf("encoding a request: %v", err)
	}
	if bucket, database := decodeFrameMeta(payload).scope(op); bucket != "Post" || database != "app" {
		t.Errorf("BUCKET_INFO is scoped to (%q, %q), want (Post, app): the cluster places it by the bucket", bucket, database)
	}
}

// TestBucketInfoAnswersWhatListBucketsSaysOfThatBucket: the one-bucket call
// returns the same BucketInfo the listing carries for it, so the two cannot
// drift.
func TestBucketInfoAnswersWhatListBucketsSaysOfThatBucket(t *testing.T) {
	s := newPostServer(t)
	fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"text": "hi"}}))

	answer, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: "Post"})
	if err != nil {
		t.Fatalf("BucketInfo: %v", err)
	}
	if !answer.Found || answer.Database != "app" {
		t.Errorf("answer = %+v, want found in app", answer)
	}
	listed, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil || len(listed.Buckets) != 1 {
		t.Fatalf("ListBuckets = %+v, %v; want the one bucket", listed, err)
	}
	if !reflect.DeepEqual(answer.Bucket, listed.Buckets[0]) {
		t.Errorf("BucketInfo says %+v, ListBuckets says %+v", answer.Bucket, listed.Buckets[0])
	}
	info := answer.Bucket
	if info.Name != "Post" || info.Type != BucketTypeHash || info.Entries != 2 {
		t.Errorf("info = %+v, want Post, hash, 2 entries", info)
	}
	if want := []string{"Comments", "Ranked", "Tags"}; !reflect.DeepEqual(info.Stores, want) {
		t.Errorf("Stores = %v, want the declared names %v", info.Stores, want)
	}
	if info.ReplicationFactor != 1 || !info.Inherited {
		t.Errorf("replication = %d inherited %v, want the node's factor 1, inherited", info.ReplicationFactor, info.Inherited)
	}
}

// TestBucketInfoMissingAnswersAsBucketStatsDoes: a missing bucket and a
// missing database get BUCKET_STATS's refusal word for word, so a caller
// handles one error.
func TestBucketInfoMissingAnswersAsBucketStatsDoes(t *testing.T) {
	s := newPostServer(t)
	for _, req := range []BucketStatsRequest{
		{Database: "app", Bucket: "Ghost"},
		{Database: "nowhere", Bucket: "Post"},
		{Database: "", Bucket: "Post"},
		{Database: "app", Bucket: ""},
	} {
		_, statsErr := s.BucketStats(req)
		_, infoErr := s.BucketInfo(BucketInfoRequest(req))
		if statsErr == nil || infoErr == nil {
			t.Fatalf("%+v: BucketStats error %v, BucketInfo error %v; want both to refuse", req, statsErr, infoErr)
		}
		if statsErr.Error() != infoErr.Error() {
			t.Errorf("%+v: BucketStats says %q, BucketInfo says %q", req, statsErr, infoErr)
		}
		if errors.Is(statsErr, ErrBucketNotFound) != errors.Is(infoErr, ErrBucketNotFound) {
			t.Errorf("%+v: the two refusals disagree on ErrBucketNotFound", req)
		}
	}
}

// TestBucketInfoOnTheWire: the opcode answers a found bucket under its own
// opcode with the wire names the vectors pin, and a missing one with the same
// error frame BUCKET_STATS gives, on every node of a cluster (the request is
// routed to the bucket's owner like BUCKET_STATS).
func TestBucketInfoOnTheWire(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Post", Type: BucketTypeHash, Options: storesOption(StoreDeclaration{Name: "Tags", Engine: "hashStore"})})

	for index := range tc.nodes {
		found := tc.frame(index, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: "Post"})
		if found.Opcode != transport.OpBucketInfo {
			t.Fatalf("node %d answered %s: %s", index+1, found.Opcode, found.Payload)
		}
		raw := bson.Raw(found.Payload)
		if ok, _ := raw.Lookup("found").BooleanOK(); !ok {
			t.Errorf("node %d: found is not true: %s", index+1, raw)
		}
		info, ok := raw.Lookup("bucket").DocumentOK()
		if !ok {
			t.Fatalf("node %d: no bucket document under \"bucket\": %s", index+1, raw)
		}
		if name, _ := info.Lookup("name").StringValueOK(); name != "Post" {
			t.Errorf("node %d: bucket.name = %q", index+1, name)
		}
		stores, _ := info.Lookup("stores").ArrayOK()
		if values, _ := stores.Values(); len(values) != 1 || values[0].StringValue() != "Tags" {
			t.Errorf("node %d: bucket.stores = %v, want [Tags]", index+1, stores)
		}
		if _, err := info.LookupErr("replication_factor"); err != nil {
			t.Errorf("node %d: no replication_factor under that name: %s", index+1, info)
		}

		missing := tc.frame(index, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: "Ghost"})
		statsMissing := tc.frame(index, transport.OpBucketStats, BucketStatsRequest{Database: testDatabase, Bucket: "Ghost"})
		if missing.Opcode != transport.OpError || statsMissing.Opcode != transport.OpError {
			t.Fatalf("node %d: a missing bucket answered %s, BUCKET_STATS answered %s; want both ERROR", index+1, missing.Opcode, statsMissing.Opcode)
		}
		if !reflect.DeepEqual(errorCodeAndMessage(t, missing), errorCodeAndMessage(t, statsMissing)) {
			t.Errorf("node %d: a missing bucket answered %v, BUCKET_STATS answered %v", index+1, errorCodeAndMessage(t, missing), errorCodeAndMessage(t, statsMissing))
		}
	}
}

func errorCodeAndMessage(t *testing.T, frame *transport.Frame) [2]string {
	t.Helper()
	return [2]string{fmt.Sprint(decodeErrorCode(t, frame.Payload)), decodeErrorMessage(t, frame.Payload)}
}

// TestBucketInfoNeedsTheReadGrantBucketStatsNeeds: a user without a read
// grant on the database is refused with the same message BUCKET_STATS gives,
// naming the grant.
func TestBucketInfoNeedsTheReadGrantBucketStatsNeeds(t *testing.T) {
	s := userTestServer(t)
	handler := NewHandler(s)
	reader := &transport.Session{}
	reader.SetIdentity(transport.Identity{Username: "reader", Roles: []string{"reader"}, Databases: []string{"app"}, Grants: []transport.ScopedGrant{{Role: "reader", DB: "app"}}})
	ctx := transport.WithSession(context.Background(), reader)

	infoAnswer, err := handler(ctx, transport.NewFrame(transport.OpBucketInfo, mustMarshalBSON(t, &BucketInfoRequest{Database: "other", Bucket: "b"})))
	if err != nil {
		t.Fatalf("handler returned a Go error: %v", err)
	}
	statsAnswer, err := handler(ctx, transport.NewFrame(transport.OpBucketStats, mustMarshalBSON(t, &BucketStatsRequest{Database: "other", Bucket: "b"})))
	if err != nil {
		t.Fatalf("handler returned a Go error: %v", err)
	}
	if infoAnswer.Opcode != transport.OpError || decodeErrorCode(t, infoAnswer.Payload) != int(transport.ErrPermissionDenied) {
		t.Fatalf("BUCKET_INFO outside the grant answered %s %s, want PERMISSION_DENIED", infoAnswer.Opcode, infoAnswer.Payload)
	}
	if got, want := decodeErrorMessage(t, infoAnswer.Payload), decodeErrorMessage(t, statsAnswer.Payload); got != want {
		t.Errorf("BUCKET_INFO refusal %q differs from BUCKET_STATS's %q", got, want)
	}
	if message := decodeErrorMessage(t, infoAnswer.Payload); !strings.Contains(message, "grant role reader@other to reader") {
		t.Errorf("the refusal %q does not name the fix", message)
	}
}

// fillStoreField puts count elements into one document's hash store field.
func fillStoreField(t *testing.T, s *Server, bucket, field string, count int) {
	t.Helper()
	for element := 0; element < count; element++ {
		fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{
			Database: "app", Bucket: bucket, Key: "d1", Field: field,
			Element: fmt.Sprintf("e%06d", element), Data: map[string]interface{}{"n": int32(element)},
		}))
	}
}

// storeCallsOf runs call with the node's store wrapped in a counting store and
// returns what it asked of it. The wrapper is removed when call returns.
//
// s.kv is a plain field, and two of the server's goroutines read it on their
// own schedule: the reclaimer on every lap, the capacity sampler's store-size
// probe on every sample (BUG-0519). Both are quiesced for the window, so the
// swap is ordered after their last read and before their next, and none of
// their store calls is counted as call's: the reclaimer is parked at the top
// of a lap, and the sampler waits on its sampleMu, which each sample holds
// from start to finish. The third, the TTL sweeper, reads the store only
// through a bucket with a TTL index, and the cost proofs declare none.
func storeCallsOf(t *testing.T, s *Server, call func() error) (snapshots, pointReads, iteratorSteps int64) {
	t.Helper()
	unpark := parkReclaimer(t, s)
	defer unpark()
	s.capacity.sampleMu.Lock()
	defer s.capacity.sampleMu.Unlock()
	var counts storeCallCounts
	plain := s.kv
	s.kv = countingKV{KV: plain, counts: &counts}
	defer func() { s.kv = plain }()
	if err := call(); err != nil {
		t.Fatalf("the counted call failed: %v", err)
	}
	return counts.snapshots.Load(), counts.pointReads.Load(), counts.iteratorSteps.Load()
}

// parkReclaimer holds s's reclaimer at the top of its next lap, wakes it so
// that lap starts at once, and returns when it is held: a lap in flight
// finishes first. The handshake orders everything the reclaimer did before
// the caller's next step, and unpark orders the caller's steps before
// anything it does after. Only one server's reclaimer may run while it is
// held: the hook is the package's.
func parkReclaimer(t *testing.T, s *Server) (unpark func()) {
	t.Helper()
	parked := make(chan struct{}, 1)
	gate := make(chan struct{})
	hold := func() {
		select {
		case parked <- struct{}{}:
		default:
		}
		<-gate
	}
	previous := reclaimLapHook.Swap(&hold)
	var once sync.Once
	unpark = func() {
		once.Do(func() {
			reclaimLapHook.Store(previous)
			close(gate)
		})
	}
	s.wakeReclaimer()
	select {
	case <-parked:
	case <-time.After(30 * time.Second):
		unpark()
		t.Fatal("the reclaimer did not reach the top of a lap within 30s of being woken; it is stuck in a lap or has stopped")
	}
	return unpark
}

// TestStoreCallsOfIgnoresAReclaimerLapInsideTheWindow is BUG-0519's proof:
// the counted window of storeCallsOf is entered while a reclaimer lap is in
// flight, held at the lap's top for lapDelay by a hook that does not
// synchronise with this goroutine, and a capacity sample is started inside the
// window as the sampler's ticker would. The counted call must make the same
// store calls as with neither, and under -race no read of the server's store
// field may be concurrent with the fixture's swap of it. Guard: swap s.kv with
// the reclaimer and the sampler running — the race detector reports the swap
// against nextNodeRecord and storeSize, and the lap's snapshot and iterator
// steps land in the count.
func TestStoreCallsOfIgnoresAReclaimerLapInsideTheWindow(t *testing.T) {
	s := newTestServer(t)
	if resp := createWithStores(t, s, "app", "Small", BucketTypeHash, StoreDeclaration{Name: "Tags", Engine: "hashStore"}); !resp.Success {
		t.Fatalf("creating Small: %s", resp.Message)
	}
	bucketInfo := func() error {
		_, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: "Small"})
		return err
	}
	quietSnapshots, quietReads, quietSteps := storeCallsOf(t, s, bucketInfo)

	const lapDelay = 100 * time.Millisecond
	lapStarted := make(chan struct{}, 1)
	delayLap := func() {
		select {
		case lapStarted <- struct{}{}:
		default:
		}
		time.Sleep(lapDelay)
	}
	reclaimLapHook.Store(&delayLap)
	t.Cleanup(func() { reclaimLapHook.Store(nil) })
	s.wakeReclaimer()
	select {
	case <-lapStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("setup: the reclaimer did not start a lap within 30s of being woken")
	}

	sampled := make(chan struct{})
	snapshots, reads, steps := storeCallsOf(t, s, func() error {
		go func() {
			defer close(sampled)
			s.capacity.sampleNow()
		}()
		err := bucketInfo()
		// Long enough for the held lap to wake and walk its records inside
		// the window, unless the fixture waited it out.
		time.Sleep(5 * lapDelay)
		return err
	})
	<-sampled
	quiet, inFlight := [3]int64{quietSnapshots, quietReads, quietSteps}, [3]int64{snapshots, reads, steps}
	if inFlight != quiet {
		t.Errorf("BUCKET_INFO counted %v store calls [snapshots, point reads, iterator steps] with a reclaimer lap in flight, %v without: "+
			"the count includes the reclaimer's own calls", inFlight, quiet)
	}
}

// TestBucketInfoCostIsIndependentOfStoreElements is the cost proof: BUCKET_INFO
// reads the same keys of the node's store whether the bucket's store field
// holds 1,000 elements or 100,000, and takes the same time order. The control
// is BUCKET_STATS, which walks the field's elements: the counter must see that
// walk grow, or it would prove nothing.
func TestBucketInfoCostIsIndependentOfStoreElements(t *testing.T) {
	const small, large = 1_000, 100_000
	if testing.Short() {
		t.Skip("fills a store field with 100,000 elements")
	}
	s := newTestServer(t)
	for _, bucket := range []string{"Small", "Large"} {
		resp := createWithStores(t, s, "app", bucket, BucketTypeHash, StoreDeclaration{Name: "Tags", Engine: "hashStore"})
		if !resp.Success {
			t.Fatalf("creating %s: %s", bucket, resp.Message)
		}
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: "d1", Data: map[string]interface{}{"title": "t"}}); err != nil || !resp.Success {
			t.Fatalf("inserting into %s: %+v, %v", bucket, resp, err)
		}
	}
	fillStoreField(t, s, "Small", "Tags", small)
	fillStoreField(t, s, "Large", "Tags", large)

	infoCalls := func(bucket string) [3]int64 {
		snapshots, reads, steps := storeCallsOf(t, s, func() error {
			_, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: bucket})
			return err
		})
		return [3]int64{snapshots, reads, steps}
	}
	statsCalls := func(bucket string) [3]int64 {
		snapshots, reads, steps := storeCallsOf(t, s, func() error {
			_, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: bucket})
			return err
		})
		return [3]int64{snapshots, reads, steps}
	}

	infoSmall, infoLarge := infoCalls("Small"), infoCalls("Large")
	if want := [3]int64{1, 1, 0}; infoSmall != want || infoLarge != want {
		t.Errorf("BUCKET_INFO store calls [snapshots, point reads, iterator steps] = %v at %d elements and %v at %d, want %v at both", infoSmall, small, infoLarge, large, want)
	}
	for _, bucket := range []string{"Small", "Large"} {
		answer, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: bucket})
		if err != nil {
			t.Fatalf("BucketInfo %s: %v", bucket, err)
		}
		if got, want := storeFieldPairs(t, answer), [][2]string{{"Tags", "hash"}}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: store_fields = %v, want %v whatever the field holds", bucket, got, want)
		}
	}
	if infoSmall != infoLarge {
		t.Errorf("BUCKET_INFO store calls [snapshots, point reads, iterator steps]: %d elements %v, %d elements %v; want equal", small, infoSmall, large, infoLarge)
	}
	statsSmall, statsLarge := statsCalls("Small"), statsCalls("Large")
	if statsLarge[2] < statsSmall[2]+int64(large-small)/2 {
		t.Errorf("control: BUCKET_STATS iterator steps %d (%d elements) vs %d (%d elements); the counter does not see a walk grow", statsSmall[2], small, statsLarge[2], large)
	}
	if infoLarge[2] >= statsLarge[2]/100 {
		t.Errorf("BUCKET_INFO took %d iterator steps against BUCKET_STATS's %d on the large bucket", infoLarge[2], statsLarge[2])
	}
	t.Logf("store calls [snapshots, point reads, iterator steps]: BUCKET_INFO %v then %v, BUCKET_STATS %v then %v (%d then %d elements)", infoSmall, infoLarge, statsSmall, statsLarge, small, large)
}

// storeFieldPairs reads the reply's "store_fields" as it travels (the BSON the
// handler sends), as [name, engine] pairs in order, nil when the key is absent.
func storeFieldPairs(t *testing.T, answer *BucketInfoResponse) [][2]string {
	t.Helper()
	encoded, err := bson.Marshal(answer)
	if err != nil {
		t.Fatalf("encoding the reply: %v", err)
	}
	fields, err := bson.Raw(encoded).LookupErr("store_fields")
	if err != nil {
		return nil
	}
	elements, ok := fields.ArrayOK()
	if !ok {
		t.Fatalf("store_fields is not an array: %s", fields)
	}
	values, err := elements.Values()
	if err != nil {
		t.Fatalf("reading store_fields: %v", err)
	}
	pairs := make([][2]string, 0, len(values))
	for _, value := range values {
		entry := value.Document()
		name, _ := entry.Lookup("name").StringValueOK()
		engine, _ := entry.Lookup("engine").StringValueOK()
		pairs = append(pairs, [2]string{name, engine})
	}
	return pairs
}

// TestBucketInfoNamesEachDeclaredStoreFieldsEngine: store_fields carries one
// {name, engine} per top-level declared store field, in declaration order,
// engine spelled as BUCKET_STATS spells it, for all six engines. A nested
// store is left out exactly as stores[] leaves it out, and stores[] still
// answers the names.
func TestBucketInfoNamesEachDeclaredStoreFieldsEngine(t *testing.T) {
	s := newTestServer(t)
	resp := createWithStores(t, s, "app", "User", BucketTypeHash,
		StoreDeclaration{Name: "Purchases", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Flags", Engine: "fifoStore"}}},
		StoreDeclaration{Name: "Photos", Engine: "lifoStore"},
		StoreDeclaration{Name: "Friends", Engine: "hashStore"},
		StoreDeclaration{Name: "Recipes", Engine: "btreeStore"},
		StoreDeclaration{Name: "Books", Engine: "heapStore"},
		StoreDeclaration{Name: "Data", Engine: "blobStore"})
	if !resp.Success {
		t.Fatalf("creating User: %s", resp.Message)
	}
	answer, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: "User"})
	if err != nil {
		t.Fatalf("BucketInfo: %v", err)
	}
	want := [][2]string{{"Purchases", "fifo"}, {"Photos", "lifo"}, {"Friends", "hash"}, {"Recipes", "btree"}, {"Books", "heap"}, {"Data", "blob"}}
	if got := storeFieldPairs(t, answer); !reflect.DeepEqual(got, want) {
		t.Errorf("store_fields = %v, want %v (declaration order, no nested Flags)", got, want)
	}
	if wantNames := []string{"Purchases", "Photos", "Friends", "Recipes", "Books", "Data"}; !reflect.DeepEqual(answer.Bucket.Stores, wantNames) {
		t.Errorf("bucket.stores = %v, want the unchanged names %v", answer.Bucket.Stores, wantNames)
	}
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "User"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	var statsPairs [][2]string
	for _, statStore := range stats.Stores {
		if statStore.Parent == "" {
			statsPairs = append(statsPairs, [2]string{statStore.Name, statStore.Engine})
		}
	}
	if !reflect.DeepEqual(statsPairs, want) {
		t.Errorf("BUCKET_STATS spells the engines %v, store_fields %v: they must agree", statsPairs, want)
	}
}

// TestBucketInfoWithoutStoreFieldsSendsNoStoreFields: a bucket that declares
// none answers no store_fields key at all, so an older reader sees the reply
// it always saw.
func TestBucketInfoWithoutStoreFieldsSendsNoStoreFields(t *testing.T) {
	s := newTestServer(t)
	if resp := createWithStores(t, s, "app", "Plain", BucketTypeHash); !resp.Success {
		t.Fatalf("creating Plain: %s", resp.Message)
	}
	answer, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: "Plain"})
	if err != nil {
		t.Fatalf("BucketInfo: %v", err)
	}
	encoded, err := bson.Marshal(answer)
	if err != nil {
		t.Fatalf("encoding the reply: %v", err)
	}
	if _, err := bson.Raw(encoded).LookupErr("store_fields"); err == nil {
		t.Errorf("a bucket with no store fields sent store_fields: %s", bson.Raw(encoded))
	}
}

// TestBucketInfoStoreFieldsOnTheWire: the opcode's reply carries store_fields
// under that name, with name and engine members, and the bucket row inside
// "bucket" gains nothing, so ListBuckets rows keep their shape.
func TestBucketInfoStoreFieldsOnTheWire(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Post", Type: BucketTypeHash, Options: storesOption(StoreDeclaration{Name: "Tags", Engine: "hashStore"}, StoreDeclaration{Name: "Wall", Engine: "lifoStore"})})

	found := tc.frame(0, transport.OpBucketInfo, BucketInfoRequest{Database: testDatabase, Bucket: "Post"})
	if found.Opcode != transport.OpBucketInfo {
		t.Fatalf("answered %s: %s", found.Opcode, found.Payload)
	}
	raw := bson.Raw(found.Payload)
	fields, err := raw.LookupErr("store_fields")
	if err != nil {
		t.Fatalf("no store_fields in the reply: %s", raw)
	}
	values, _ := fields.Array().Values()
	if len(values) != 2 {
		t.Fatalf("store_fields = %s, want two entries", fields)
	}
	first := values[0].Document()
	if name, _ := first.Lookup("name").StringValueOK(); name != "Tags" {
		t.Errorf("store_fields[0].name = %q, want Tags", name)
	}
	if engine, _ := first.Lookup("engine").StringValueOK(); engine != "hash" {
		t.Errorf("store_fields[0].engine = %q, want hash", engine)
	}
	if _, err := raw.Lookup("bucket").Document().LookupErr("store_fields"); err == nil {
		t.Errorf("store_fields leaked into the bucket row, which ListBuckets shares: %s", raw)
	}
}
