package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The result budget's tests: query-pipelining spec §8 T-4, T-4b and T-4c, and
// the build doc's TestBudgetChargesEqualEncodedSize and
// TestQueryWalkObservesContext. T-6's accountant cases are in
// query_retention_test.go.

// testBudget and testWalk are the default budget and a never-cancelled walk,
// for tests that call a query's inner walk directly.
func testBudget(method string) *resultBudget {
	return (&Server{}).newResultBudget(method, resultTooLargeError{})
}

func testWalk(method string) *queryWalk {
	return newQueryWalk(context.Background(), method, "app", "test")
}

// setQueryCeiling sets the node's result ceiling on a test configuration.
func setQueryCeiling(cfg *config.Config, ceiling int) {
	cfg.Query.MaxResultBytes = ceiling
}

// budgetReport is what queryBudgetHook saw for the last query of one method.
type budgetReport struct {
	retained  int64
	highWater int64
	reported  bool
}

// recordBudgets installs queryBudgetHook for the test and returns a function
// that yields, and clears, the last report for a method.
func recordBudgets(t *testing.T) func(method string) budgetReport {
	t.Helper()
	var mu sync.Mutex
	reports := map[string]budgetReport{}
	hook := func(method string, retained, highWater int64) {
		mu.Lock()
		reports[method] = budgetReport{retained: retained, highWater: highWater, reported: true}
		mu.Unlock()
	}
	queryBudgetHook.Store(&hook)
	t.Cleanup(func() { queryBudgetHook.Store(nil) })
	return func(method string) budgetReport {
		mu.Lock()
		defer mu.Unlock()
		report := reports[method]
		delete(reports, method)
		return report
	}
}

// seedMiBDocuments writes count documents of about 1 MiB each into a hash
// bucket, so a page of them outgrows both the 16 MiB result ceiling and the
// 32 MiB frame limit.
func seedMiBDocuments(t *testing.T, s *Server, database, bucket string, count int) {
	t.Helper()
	padding := strings.Repeat("m", 1<<20)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("big%03d", i)
		resp, err := s.Insert(InsertRequest{Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"pad": padding}})
		if err != nil || !resp.Success {
			t.Fatalf("seeding %s: %+v, %v", key, resp, err)
		}
	}
}

// pipelinedClient is a raw TLS connection that writes several frames before
// reading any answer, and matches the answers by request ID.
type pipelinedClient struct {
	t    *testing.T
	conn *tls.Conn
	br   *bufio.Reader
}

func dialPipelined(t *testing.T, addr string) *pipelinedClient {
	t.Helper()
	plain := dialTCPServer(t, addr)
	plain.login()
	return &pipelinedClient{t: t, conn: plain.conn, br: plain.br}
}

// exchange writes every frame, then reads one answer per frame, and returns
// the answers keyed by request ID.
func (c *pipelinedClient) exchange(frames ...*transport.Frame) map[[transport.RequestIDLen]byte]*transport.Frame {
	c.t.Helper()
	if err := c.conn.SetDeadline(time.Now().Add(120 * time.Second)); err != nil {
		c.t.Fatalf("setting the connection deadline: %v", err)
	}
	for _, frame := range frames {
		if _, err := c.conn.Write(frame.Encode()); err != nil {
			c.t.Fatalf("writing %s: %v", frame.Opcode, err)
		}
	}
	answers := make(map[[transport.RequestIDLen]byte]*transport.Frame, len(frames))
	for range frames {
		answer, err := transport.DecodeFrame(c.br)
		if err != nil {
			c.t.Fatalf("reading an answer after %d of %d: %v — the connection did not survive (S-3)", len(answers), len(frames), err)
		}
		answers[answer.RequestID] = answer
	}
	return answers
}

func requestFrame(t *testing.T, op transport.Opcode, request interface{}) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("marshalling %s: %v", op, err)
	}
	return transport.NewFrame(op, payload)
}

// decodeErrorFrame decodes an OpError answer, failing if it is not one.
func decodeErrorFrame(t *testing.T, answer *transport.Frame, what string) ErrorResponse {
	t.Helper()
	if answer == nil {
		t.Fatalf("%s: no answer", what)
	}
	if answer.Opcode != transport.OpError {
		t.Fatalf("%s: answered %s, want OpError", what, answer.Opcode)
	}
	var refused ErrorResponse
	if err := bson.Unmarshal(answer.Payload, &refused); err != nil {
		t.Fatalf("%s: decoding the error: %v", what, err)
	}
	return refused
}

// assertFoundGet checks a GET answer found its document.
func assertFoundGet(t *testing.T, answer *transport.Frame, what string) {
	t.Helper()
	if answer == nil || answer.Opcode != transport.OpGetResponse {
		t.Fatalf("%s: answered %v, want a GET response", what, answer)
	}
	var found FindResponse
	if err := bson.Unmarshal(answer.Payload, &found); err != nil || !found.Found {
		t.Fatalf("%s: %+v, %v", what, found, err)
	}
}

// TestOversizedResultIsNamedErrorAndConnectionSurvives is T-4: over TCP at
// W = 4, a SCAN whose page outgrows the ceiling is refused with
// RESULT_TOO_LARGE and W-3's fields, the GET pipelined behind it is answered,
// and so is a third request on the same connection. The frame guard never
// fires: the budget refused first.
func TestOversizedResultIsNamedErrorAndConnectionSurvives(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { cfg.TCP.PipelineFrames = 4 })
	createTestBucket(t, qs.core, "app", "big", BucketTypeHash)
	seedMiBDocuments(t, qs.core, "app", "big", 40)
	insertScanDoc(t, qs.core, "app", "big", "small", map[string]interface{}{"n": int32(1)})

	c := dialPipelined(t, addr)
	guardedBefore := transport.OversizedResponses()
	scan := requestFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "big", Limit: 41})
	get := requestFrame(t, transport.OpGet, FindRequest{Database: "app", Bucket: "big", Key: "small"})
	answers := c.exchange(scan, get)

	refused := decodeErrorFrame(t, answers[scan.RequestID], "the oversized SCAN")
	if refused.Code != int(transport.ErrResultTooLarge) {
		t.Fatalf("the oversized SCAN answered code %d (%q), want RESULT_TOO_LARGE", refused.Code, refused.Message)
	}
	if refused.MaxResultBytes != 16<<20 {
		t.Errorf("max_result_bytes = %d, want the 16 MiB ceiling: the refusal did not come from the result budget", refused.MaxResultBytes)
	}
	if refused.RetainedBytes <= 0 || refused.RetainedBytes > 16<<20 {
		t.Errorf("retained_bytes = %d, want the bytes the query held when it stopped, at most the ceiling", refused.RetainedBytes)
	}
	if !strings.Contains(refused.Message, "HOARDDB_QUERY_MAX_RESULT_BYTES") {
		t.Errorf("the refusal does not name the setting that raises the ceiling: %q", refused.Message)
	}
	assertFoundGet(t, answers[get.RequestID], "the GET pipelined behind the SCAN")

	third := requestFrame(t, transport.OpGet, FindRequest{Database: "app", Bucket: "big", Key: "small"})
	assertFoundGet(t, c.exchange(third)[third.RequestID], "a third request on the same connection")

	if fired := transport.OversizedResponses() - guardedBefore; fired != 0 {
		t.Errorf("the frame guard replaced %d response(s): the page reached the writer before the budget stopped it", fired)
	}
}

// minimalDocumentCount is T-4b's bucket size. The spec says "about 1.6
// million" 17-byte documents; this is 1.4 million because only a count
// between about 1.29 and 1.48 million can see the test's guard broken: the
// broken budget (document bytes only) must admit the page, N × 17 ≤ 24 MiB,
// while the page it admits must outgrow the frame, N × (17 + ~9 element
// header) > 32 MiB. At 1.6 million even the broken budget refuses, and the
// test could not tell the two apart.
const minimalDocumentCount = 1_400_000

// TestMinimalDocumentsAtTheClampStayUnderFrameLimit is T-4b: at the 24 MiB
// clamp, a page of the smallest document a write path can store — where the
// per-element header is the largest share of the frame — is either answered
// in one legal frame or refused by the budget. The frame guard never fires,
// because the budget charges each element its header as well as its bytes.
func TestMinimalDocumentsAtTheClampStayUnderFrameLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("seeds 1.4 million documents")
	}
	qs, addr := newTCPEnabledServer(t, func(cfg *config.Config) { setQueryCeiling(cfg, 24<<20) })
	createTestBucket(t, qs.core, "app", "tiny", BucketTypeFIFO)

	// {_id_: "a"} is the smallest document every write path stores (spec
	// §3.2). A queue is keyed by position, not by _id_, so one bucket holds
	// any number of them; a hash or btree bucket could not.
	minimal, err := bson.Marshal(map[string]interface{}{"_id_": "a"})
	if err != nil || len(minimal) != 17 {
		t.Fatalf("the minimal document encodes to %d bytes (%v), want the spec's 17", len(minimal), err)
	}
	bucket, err := qs.core.resolveBucket("app", "tiny")
	if err != nil {
		t.Fatalf("resolving app.tiny: %v", err)
	}
	const batchSize = 50_000
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = minimal
	}
	for pushed := 0; pushed < minimalDocumentCount; pushed += batchSize {
		if err := bucket.queueStore.PushBatch(applyInfoFrom(context.Background(), qs.core), batch); err != nil {
			t.Fatalf("seeding at %d: %v", pushed, err)
		}
	}

	c := dialPipelined(t, addr)
	guardedBefore := transport.OversizedResponses()
	scan := requestFrame(t, transport.OpScan, ScanRequest{Database: "app", Bucket: "tiny", Limit: 2_000_000})
	answer := c.exchange(scan)[scan.RequestID]
	switch answer.Opcode {
	case transport.OpScan:
		if body := 1 + transport.RequestIDLen + len(answer.Payload); body > transport.MaxFrameLength {
			t.Errorf("the SCAN answered a %d-byte frame body, over MaxFrameLength", body)
		}
	case transport.OpError:
		refused := decodeErrorFrame(t, answer, "the minimal-document SCAN")
		if refused.Code != int(transport.ErrResultTooLarge) || refused.MaxResultBytes != 24<<20 {
			t.Errorf("the SCAN was refused with code %d, max_result_bytes %d (%q); want the budget's RESULT_TOO_LARGE at 24 MiB",
				refused.Code, refused.MaxResultBytes, refused.Message)
		}
	default:
		t.Fatalf("the SCAN answered %s", answer.Opcode)
	}
	if fired := transport.OversizedResponses() - guardedBefore; fired != 0 {
		t.Errorf("the frame guard fired %d time(s): per-element overhead took the page past 32 MiB inside the ceiling (R-3)", fired)
	}
}

// TestForwardedOversizedResultReachesClientAs0x0E is T-4c: a SCAN sent to the
// node that does not own the bucket is forwarded, refused by the owner's
// budget, and the refusal reaches the client as RESULT_TOO_LARGE with W-3's
// fields. A GET afterwards is answered.
func TestForwardedOversizedResultReachesClientAs0x0E(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	const database = "app"
	const coordinator = 0
	bucket := bucketNameOwnedBy(t, tc.nodes[coordinator].server.core.cluster, database, tc.addrs[1])
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	tc.ok(coordinator, transport.OpCreateBucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
	padding := strings.Repeat("m", 1<<20)
	for i := 0; i < 40; i++ {
		tc.ok(coordinator, transport.OpPut, InsertRequest{Database: database, Bucket: bucket, Key: fmt.Sprintf("big%03d", i), Data: map[string]interface{}{"pad": padding}})
	}
	tc.ok(coordinator, transport.OpPut, InsertRequest{Database: database, Bucket: bucket, Key: "small", Data: map[string]interface{}{"n": int32(1)}})

	answer := tc.frame(coordinator, transport.OpScan, ScanRequest{Database: database, Bucket: bucket, Limit: 41})
	refused := decodeErrorFrame(t, answer, "the forwarded oversized SCAN")
	if refused.Code != int(transport.ErrResultTooLarge) {
		t.Fatalf("the forwarded SCAN answered code %d (%q), want RESULT_TOO_LARGE", refused.Code, refused.Message)
	}
	if refused.MaxResultBytes != 16<<20 || refused.RetainedBytes <= 0 {
		t.Errorf("the forwarded refusal carries max_result_bytes %d and retained_bytes %d; want the owner's 16 MiB ceiling and what it held",
			refused.MaxResultBytes, refused.RetainedBytes)
	}
	assertFoundGet(t, tc.frame(coordinator, transport.OpGet, FindRequest{Database: database, Bucket: bucket, Key: "small"}),
		"a GET after the refusal")
}

// encodedElementBytes is the byte length of the elements of one array field of
// a response as the wire encodes it: the array document less its four-byte
// length and its terminator. It is what the result budget charges for.
func encodedElementBytes(t *testing.T, response interface{}, field string) int64 {
	t.Helper()
	encoded, err := bson.Marshal(response)
	if err != nil {
		t.Fatalf("marshalling the response: %v", err)
	}
	array, err := bson.Raw(encoded).LookupErr(field)
	if err != nil {
		t.Fatalf("the response has no %q field: %v", field, err)
	}
	if array.Type != bson.TypeArray {
		// An empty result marshals as null.
		return 0
	}
	return int64(len(array.Value) - 5)
}

// createTestBucketWithOptions is createTestBucket with create options, such
// as an index declaration.
func createTestBucketWithOptions(t testing.TB, s *Server, db, bucket string, btype BucketType, options map[string]interface{}) {
	t.Helper()
	if _, err := s.CreateDatabase(db); err != nil {
		t.Fatalf("CreateDatabase(%q): %v", db, err)
	}
	created, err := s.CreateBucket(CreateBucketRequest{Database: db, Name: bucket, Type: btype, Options: options})
	if err != nil || !created.Success {
		t.Fatalf("CreateBucket(%q) = %+v, %v", bucket, created, err)
	}
}

// TestBudgetChargesEqualEncodedSize proves R-3 by derivation: for random pages
// of every response type, what the budget still holds when the query returns
// is exactly the byte length of the element array the response marshals to,
// plus the sort keys or distinct keys that shape holds beside its elements.
// So a ceiling C bounds the elements of the frame by C, whatever the
// documents look like.
func TestBudgetChargesEqualEncodedSize(t *testing.T) {
	s := newTestServer(t)
	rng := rand.New(rand.NewSource(3))
	createTestBucketWithOptions(t, s, "app", "mixed", BucketTypeHash, map[string]interface{}{"indices": []string{"tag"}})
	createTestBucket(t, s, "app", "ordered", BucketTypeBTree)
	for i := 0; i < 400; i++ {
		key := fmt.Sprintf("k%04d", i)
		document := map[string]interface{}{
			"tag":  "t",
			"when": float64(rng.Intn(1000)),
			"pad":  strings.Repeat("p", rng.Intn(300)),
			"kind": fmt.Sprintf("kind-%d", rng.Intn(40)),
		}
		if rng.Intn(3) == 0 {
			document["extra"] = []interface{}{int32(i), "x"}
		}
		insertScanDoc(t, s, "app", "mixed", key, document)
		insertScanDoc(t, s, "app", "ordered", key, document)
	}
	lastReport := recordBudgets(t)

	type chargedQuery struct {
		name   string
		method string
		field  string
		run    func(skip, limit int) (response interface{}, extra int64, err error)
	}
	queries := []chargedQuery{
		{"find()", "SCAN", "documents", func(skip, limit int) (interface{}, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "mixed", Skip: skip, Limit: limit})
			return resp, 0, err
		}},
		{"find(filter)", "SCAN", "documents", func(skip, limit int) (interface{}, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "mixed", Filter: map[string]interface{}{"tag": "t"}, Skip: skip, Limit: limit})
			return resp, 0, err
		}},
		{"find(filter, projection)", "SCAN", "documents", func(skip, limit int) (interface{}, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "ordered", Filter: map[string]interface{}{"tag": "t"},
				Projection: map[string]int{"kind": 1, "extra": 1}, Skip: skip, Limit: limit})
			return resp, 0, err
		}},
		{"find(sort) at skip 0", "SCAN", "documents", func(_, limit int) (interface{}, int64, error) {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "mixed", Sort: map[string]int{"when": 1}, Limit: limit})
			if err != nil {
				return nil, 0, err
			}
			// Every document holds a float64 "when": each kept key is its 8
			// value bytes plus 16, and at skip 0 the keys kept are the page's.
			return resp, int64(len(resp.Documents)) * (8 + 16), nil
		}},
		{"search", "SEARCH", "results", func(_, limit int) (interface{}, int64, error) {
			resp, err := s.Search(SearchRequest{Database: "app", Bucket: "mixed", Filter: map[string]interface{}{"tag": "t"}, Limit: limit})
			return resp, 0, err
		}},
		{"btree range", "BTREE_RANGE", "entries", func(_, limit int) (interface{}, int64, error) {
			resp, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "ordered", Low: "k0100", Limit: limit})
			return resp, 0, err
		}},
		{"distinct", "DISTINCT", "values", func(int, int) (interface{}, int64, error) {
			resp, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "ordered", Field: "kind"})
			if err != nil {
				return nil, 0, err
			}
			// A distinct value is also charged its seen-set key.
			var keys int64
			for _, value := range resp.Values {
				keys += int64(len(valueToDistinctKey(value)))
			}
			return resp, keys, nil
		}},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			for trial := 0; trial < 12; trial++ {
				skip, limit := rng.Intn(120), 1+rng.Intn(150)
				response, extra, err := query.run(skip, limit)
				if err != nil {
					t.Fatalf("skip %d limit %d: %v", skip, limit, err)
				}
				report := lastReport(query.method)
				if !report.reported {
					t.Fatalf("skip %d limit %d: the query returned without reporting a result budget, so nothing bounds what it holds (B-1)", skip, limit)
				}
				if want := encodedElementBytes(t, response, query.field) + extra; report.retained != want {
					t.Fatalf("skip %d limit %d: the budget holds %d bytes for elements that encode to %d: it does not count "+
						"what the frame carries, so the ceiling does not bound the frame (R-3)", skip, limit, report.retained, want)
				}
			}
		})
	}
}

// TestQueryWalkObservesContext: every query executor, given a context that is
// already cancelled, stops within one check interval and returns an error
// that matches context.Canceled.
func TestQueryWalkObservesContext(t *testing.T) {
	s := newTestServer(t)
	createTestBucketWithOptions(t, s, "app", "walked", BucketTypeHash, map[string]interface{}{"indices": []string{"tag"}})
	createTestBucket(t, s, "app", "ordered", BucketTypeBTree)
	const docCount = 1000
	for start := 0; start < docCount; start += 500 {
		batch := make([]map[string]interface{}, 0, 500)
		for i := start; i < start+500; i++ {
			batch = append(batch, map[string]interface{}{"_id_": fmt.Sprintf("k%04d", i), "tag": "t", "n": int32(i)})
		}
		for _, bucket := range []string{"walked", "ordered"} {
			if resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: bucket, Data: batch}); err != nil || !resp.Success {
				t.Fatalf("seeding %s: %+v, %v", bucket, resp, err)
			}
		}
	}

	var visitsMu sync.Mutex
	visits := map[string]int{}
	countVisit := func(method string) {
		visitsMu.Lock()
		visits[method]++
		visitsMu.Unlock()
	}
	queryVisitHook.Store(&countVisit)
	t.Cleanup(func() { queryVisitHook.Store(nil) })

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	executors := []struct {
		name   string
		method string
		run    func() error
	}{
		{"find()", "SCAN", func() error {
			_, err := s.ScanCtx(cancelled, ScanRequest{Database: "app", Bucket: "walked", Limit: docCount})
			return err
		}},
		{"find(filter)", "SCAN", func() error {
			_, err := s.ScanCtx(cancelled, ScanRequest{Database: "app", Bucket: "walked", Filter: map[string]interface{}{"tag": "t"}, Limit: docCount})
			return err
		}},
		{"find(sort)", "SCAN", func() error {
			_, err := s.ScanCtx(cancelled, ScanRequest{Database: "app", Bucket: "ordered", Sort: map[string]int{"n": 1}})
			return err
		}},
		{"count(filter)", "COUNT", func() error {
			_, err := s.CountCtx(cancelled, CountRequest{Database: "app", Bucket: "walked", Filter: map[string]interface{}{"tag": "t"}})
			return err
		}},
		{"distinct", "DISTINCT", func() error {
			_, err := s.DistinctCtx(cancelled, DistinctRequest{Database: "app", Bucket: "ordered", Field: "n"})
			return err
		}},
		{"btree range", "BTREE_RANGE", func() error {
			_, err := s.RangeQueryCtx(cancelled, BTreeRangeRequest{Database: "app", Bucket: "ordered"})
			return err
		}},
		{"search", "SEARCH", func() error {
			_, err := s.SearchCtx(cancelled, SearchRequest{Database: "app", Bucket: "walked", Filter: map[string]interface{}{"tag": "t"}})
			return err
		}},
	}
	for _, executor := range executors {
		t.Run(executor.name, func(t *testing.T) {
			visitsMu.Lock()
			visits[executor.method] = 0
			visitsMu.Unlock()
			err := executor.run()
			visitsMu.Lock()
			visited := visits[executor.method]
			visitsMu.Unlock()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s under a cancelled context returned %v, want an error matching context.Canceled", executor.name, err)
			}
			if visited == 0 || visited > queryCheckInterval {
				t.Errorf("%s visited %d entries under a cancelled context; want between 1 and %d", executor.name, visited, queryCheckInterval)
			}
		})
	}
}

// TestChargeFirstCountsTheDocumentAndNeverRefuses: a peek's first entry is
// exempt from the ceiling, as a pop's entry is, but it still counts, so the
// refusal that follows says "after 1 entries" and not "after 0".
func TestChargeFirstCountsTheDocumentAndNeverRefuses(t *testing.T) {
	budget := (&Server{}).newResultBudget(queryMethodPeek, resultTooLargeError{shape: resultShapePeek})
	oversized := budget.ceiling + 4<<20
	budget.chargeFirst(oversized)
	if budget.documents != 1 || budget.retained != oversized {
		t.Fatalf("after the first charge documents = %d, retained = %d; want 1, %d", budget.documents, budget.retained, oversized)
	}
	err := budget.chargeDocument(1)
	var refused *resultTooLargeError
	if !errors.As(err, &refused) {
		t.Fatalf("the second charge = %v, want a result-too-large refusal", err)
	}
	if refused.documents != 1 {
		t.Fatalf("the refusal counts %d documents, want the exempt first entry's 1", refused.documents)
	}
}
