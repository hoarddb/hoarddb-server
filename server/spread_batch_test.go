package server

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 5 of the spread MVP (document placement spread MVP note, rev
// 2.1, section 5 row 5): a BATCH addressed to a spread collection. A BATCH
// carries seven verbs (batchCarried): a store field's push, put, delete, get,
// len and peek, and the update of one key. Every one names a document, so
// every item is routed by its id; the note's "key-less items fanned out" has
// no item to apply to (nothing a BATCH carries scans), and a test below keeps
// that so.

// batchTarget is a collection a differential test sends the same BATCH to:
// whole or spread, named by a pair of equal-length names so an answer's text
// can be compared after the second name is replaced by the first.
type batchTarget struct {
	name string
	// send sends a BATCH of items and returns its answers, or the whole-batch
	// refusal when the batch was refused.
	send func(items []BatchItem) ([]BatchAnswer, *ErrorResponse)
	// read is the document key as the collection holds it, with each store
	// field's length, rendered for comparison; "" when it is absent.
	read func(key string) string
}

// differentialItems builds the same items for a bucket name.
type differentialItems func(t *testing.T, bucket string) []BatchItem

// docKeyItem is an item over one document of bucket.
func docKeyItem(t testing.TB, op transport.Opcode, bucket, key, field, element string, data map[string]interface{}) BatchItem {
	return batchItemOf(t, op, FieldRequest{Database: "app", Bucket: bucket, Key: key, Field: field, Element: element, Data: data})
}

func setItem(t testing.TB, bucket, key string, set map[string]interface{}) BatchItem {
	return batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: bucket, Key: key, Set: set})
}

func incrementItem(t testing.TB, bucket, key, field string) BatchItem {
	return batchItemOf(t, transport.OpUpdate, UpdateRequest{Database: "app", Bucket: bucket, Key: key, Inc: map[string]int64{field: 1}})
}

// answerText renders an answer with minted ids blanked and the spread
// bucket's name shown as the whole one's.
func batchAnswerText(t *testing.T, answer BatchAnswer, from, to string) string {
	t.Helper()
	text := strings.ReplaceAll(normaliseAnswer(t, answer.Payload), from, to)
	return fmt.Sprintf("%s %s", transport.Opcode(answer.Opcode), text)
}

// requireSameBatch sends the items built for each target and requires the
// same whole-batch refusal or the same answers in the same order, and the same
// documents afterwards for every key.
func requireSameBatch(t *testing.T, whole, spread batchTarget, build differentialItems, keys []string) {
	t.Helper()
	wholeAnswers, wholeRefusal := whole.send(build(t, whole.name))
	spreadAnswers, spreadRefusal := spread.send(build(t, spread.name))
	if (wholeRefusal == nil) != (spreadRefusal == nil) {
		t.Fatalf("whole refused=%v (%+v), spread refused=%v (%+v)", wholeRefusal != nil, wholeRefusal, spreadRefusal != nil, spreadRefusal)
	}
	if wholeRefusal != nil {
		got := strings.ReplaceAll(spreadRefusal.Message, spread.name, whole.name)
		if got != wholeRefusal.Message || spreadRefusal.Code != wholeRefusal.Code || spreadRefusal.WriteOutcome != wholeRefusal.WriteOutcome {
			t.Errorf("the batch refusals differ:\n whole:  %d %q (%s)\n spread: %d %q (%s)", wholeRefusal.Code, wholeRefusal.Message, wholeRefusal.WriteOutcome,
				spreadRefusal.Code, got, spreadRefusal.WriteOutcome)
		}
		if strings.Contains(spreadRefusal.Message, unitSeparator) {
			t.Errorf("the refusal shows a part name: %q", spreadRefusal.Message)
		}
		return
	}
	if len(wholeAnswers) != len(spreadAnswers) {
		t.Fatalf("%d answers on the whole bucket, %d on the spread one", len(wholeAnswers), len(spreadAnswers))
	}
	for i := range wholeAnswers {
		want := batchAnswerText(t, wholeAnswers[i], whole.name, whole.name)
		got := batchAnswerText(t, spreadAnswers[i], spread.name, whole.name)
		if got != want {
			t.Errorf("item %d answered differently:\n whole:  %s\n spread: %s", i+1, want, got)
		}
		if strings.Contains(string(spreadAnswers[i].Payload), unitSeparator) {
			t.Errorf("item %d's answer shows a part name: %q", i+1, spreadAnswers[i].Payload)
		}
	}
	for _, key := range keys {
		if got, want := spread.read(key), whole.read(key); got != want {
			t.Errorf("document %q differs afterwards:\n whole:  %s\n spread: %s", key, want, got)
		}
	}
}

// standaloneTargets is one zero-config node with a whole and a spread
// collection of one declaration: typed Name and Age, a fifo Feed and a hash
// Posts, with a document under every key.
func standaloneTargets(t *testing.T, keys []string) (whole, spread batchTarget, n *spreadNode) {
	t.Helper()
	n = newSpreadNode(t, nil)
	declaration := map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Feed", "engine": "fifoStore"},
			map[string]interface{}{"name": "Posts", "engine": "hashStore"},
		},
		"fields": []interface{}{
			map[string]interface{}{"name": "Name", "type": "string"},
			map[string]interface{}{"name": "Age", "type": "int"},
		},
	}
	// newSpreadNode made app.User; the pair under test is its own.
	for name, placement := range map[string]string{"BatchW": "whole", "BatchS": "spread"} {
		options := map[string]interface{}{"placement": placement}
		for key, value := range declaration {
			options[key] = value
		}
		reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: options})
		if reply.Opcode == transport.OpError {
			t.Fatalf("creating %s: %s", name, refusalText(t, reply))
		}
		for _, key := range keys {
			reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: key, Data: map[string]interface{}{"Name": "n-" + key, "Age": int64(1)}})
			if reply.Opcode == transport.OpError {
				t.Fatalf("seeding %s/%s: %s", name, key, refusalText(t, reply))
			}
		}
	}
	target := func(name string) batchTarget {
		return batchTarget{
			name: name,
			send: func(items []BatchItem) ([]BatchAnswer, *ErrorResponse) {
				reply := n.sendRaw(transport.OpBatch, spreadMarshal(t, BatchRequest{Items: items}))
				return decodeBatchReply(t, reply)
			},
			read: func(key string) string { return readCollectionDocument(t, n, name, key) },
		}
	}
	return target("BatchW"), target("BatchS"), n
}

// decodeBatchReply is a BATCH reply's answers, or its refusal.
func decodeBatchReply(t testing.TB, reply *transport.Frame) ([]BatchAnswer, *ErrorResponse) {
	t.Helper()
	if reply.Opcode == transport.OpError {
		var refusal ErrorResponse
		if err := bson.Unmarshal(reply.Payload, &refusal); err != nil {
			t.Fatalf("decoding a batch refusal: %v", err)
		}
		return nil, &refusal
	}
	if reply.Opcode != transport.OpBatchOK {
		t.Fatalf("a batch answered %s %q", reply.Opcode, reply.Payload)
	}
	var decoded BatchResponse
	if err := bson.Unmarshal(reply.Payload, &decoded); err != nil {
		t.Fatalf("decoding BATCH_OK: %v", err)
	}
	return decoded.Answers, nil
}

// readCollectionDocument renders a document and its stores' lengths through
// the client door, so the same call reads a whole bucket and a spread one.
func readCollectionDocument(t *testing.T, n *spreadNode, bucket, key string) string {
	t.Helper()
	reply := n.send(transport.OpGet, FindRequest{Database: "app", Bucket: bucket, Key: key})
	found := findAnswer(t, reply)
	if !found.Found {
		return ""
	}
	var document bson.M
	if err := bson.Unmarshal(found.Data, &document); err != nil {
		t.Fatal(err)
	}
	blankMinted(document)
	text := fmt.Sprint(document)
	for _, field := range []string{"Feed", "Posts"} {
		length := n.send(transport.OpFieldLen, FieldRequest{Database: "app", Bucket: bucket, Key: key, Field: field})
		text += fmt.Sprintf(" %s=%s", field, normaliseAnswer(t, length.Payload))
	}
	return text
}

// spreadKeys returns count keys, covering as many parts as it can.
func spreadKeys(t testing.TB, count int) []string {
	t.Helper()
	keys := make([]string, 0, count)
	for i := 0; len(keys) < count; i++ {
		keys = append(keys, fmt.Sprintf("doc-%03d", i))
	}
	sort.Strings(keys)
	return keys
}

// differentialScenario is a named list of items for any bucket.
type differentialScenario struct {
	name  string
	build differentialItems
}

// differentialScenarios are the BATCHes a spread collection must answer as a
// whole bucket does, over keys that cover most parts.
func differentialScenarios(keys []string) []differentialScenario {
	sameKeyAsPart := keysSharingAPart(keys, 4)
	return []differentialScenario{
		{"mixed verbs in one batch", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldPush, b, keys[0], "Feed", "", map[string]interface{}{"n": 1}),
				docKeyItem(t, transport.OpFieldPut, b, keys[1], "Posts", "p-1", map[string]interface{}{"text": "hello"}),
				docKeyItem(t, transport.OpFieldGet, b, keys[1], "Posts", "p-1", nil),
				docKeyItem(t, transport.OpFieldLen, b, keys[0], "Feed", "", nil),
				batchItemOf(t, transport.OpFieldPeek, FieldPeekRequest{FieldRequest: FieldRequest{Database: "app", Bucket: b, Key: keys[0], Field: "Feed"}, Limit: 2}),
				incrementItem(t, b, keys[2], "Age"),
				setItem(t, b, keys[3], map[string]interface{}{"Name": "renamed"}),
				docKeyItem(t, transport.OpFieldDelete, b, keys[1], "Posts", "p-1", nil),
				docKeyItem(t, transport.OpFieldGet, b, keys[1], "Posts", "p-1", nil),
			}
		}},
		{"a failing item stops the later items of the collection", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldPush, b, keys[4], "Feed", "", map[string]interface{}{"n": 1}),
				setItem(t, b, "no-such-document", map[string]interface{}{"Name": "x"}),
				docKeyItem(t, transport.OpFieldPush, b, keys[5], "Feed", "", map[string]interface{}{"n": 2}),
				docKeyItem(t, transport.OpFieldLen, b, keys[4], "Feed", "", nil),
			}
		}},
		{"a field of a document that does not exist", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldPush, b, "ghost", "Feed", "", map[string]interface{}{"n": 1}),
				docKeyItem(t, transport.OpFieldLen, b, "ghost", "Feed", "", nil),
			}
		}},
		{"reads of a missing document and a missing element", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldGet, b, "ghost", "Posts", "p", nil),
				docKeyItem(t, transport.OpFieldGet, b, keys[6], "Posts", "absent", nil),
				docKeyItem(t, transport.OpFieldLen, b, keys[6], "Feed", "", nil),
			}
		}},
		{"a typed field violation", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				setItem(t, b, keys[7], map[string]interface{}{"Age": "not a number"}),
				incrementItem(t, b, keys[7], "Age"),
			}
		}},
		{"an unknown store field", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldPush, b, keys[8], "Nope", "", map[string]interface{}{"n": 1}),
			}
		}},
		{"a repeated id in one batch applies in order", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				incrementItem(t, b, keys[9], "Age"),
				incrementItem(t, b, keys[9], "Age"),
				incrementItem(t, b, keys[9], "Age"),
				docKeyItem(t, transport.OpFieldPush, b, keys[9], "Feed", "", map[string]interface{}{"n": 1}),
				docKeyItem(t, transport.OpFieldPush, b, keys[9], "Feed", "", map[string]interface{}{"n": 2}),
				docKeyItem(t, transport.OpFieldLen, b, keys[9], "Feed", "", nil),
			}
		}},
		{"ids that hash to one part", func(t *testing.T, b string) []BatchItem {
			items := make([]BatchItem, 0, len(sameKeyAsPart))
			for _, key := range sameKeyAsPart {
				items = append(items, docKeyItem(t, transport.OpFieldPush, b, key, "Feed", "", map[string]interface{}{"n": 1}))
			}
			return items
		}},
		{"ids spread across every part", func(t *testing.T, b string) []BatchItem {
			items := make([]BatchItem, 0, len(keys))
			for _, key := range keys {
				items = append(items, incrementItem(t, b, key, "Age"))
			}
			return items
		}},
		{"a one-item batch", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{docKeyItem(t, transport.OpFieldLen, b, keys[10], "Feed", "", nil)}
		}},
		{"an empty batch", func(t *testing.T, b string) []BatchItem { return nil }},
		{"scans are not carried", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{
				docKeyItem(t, transport.OpFieldPush, b, keys[11], "Feed", "", map[string]interface{}{"n": 1}),
				batchItemOf(t, transport.OpCount, CountRequest{Database: "app", Bucket: b}),
			}
		}},
		{"a pop is not carried", func(t *testing.T, b string) []BatchItem {
			return []BatchItem{docKeyItem(t, transport.OpFieldPop, b, keys[12], "Feed", "", nil)}
		}},
	}
}

// keysSharingAPart returns count keys of one part, drawn from a larger list
// that covers every part.
func keysSharingAPart(keys []string, count int) []string {
	byPart := make(map[int][]string)
	for _, key := range keys {
		encoded, err := store.EncodeComponent(key)
		if err != nil {
			panic(err)
		}
		p, err := store.PartOf(encoded, defaultSpreadParts)
		if err != nil {
			panic(err)
		}
		byPart[p] = append(byPart[p], key)
	}
	for _, sharing := range byPart {
		if len(sharing) >= count {
			return sharing[:count]
		}
	}
	return nil
}

// TestSpreadBatchMatchesAWholeBucket is the differential test: the same
// BATCH, mixed verbs, failing items included, gives a spread collection the
// answers a whole bucket gives, in request order, and leaves the same
// documents.
func TestSpreadBatchMatchesAWholeBucket(t *testing.T) {
	keys := spreadKeys(t, 300)
	whole, spread, _ := standaloneTargets(t, keys)
	parts := make(map[int]bool)
	for _, key := range keys {
		parts[spreadPartOf(t, key)] = true
	}
	if len(parts) < 100 {
		t.Fatalf("the keys cover %d parts, want a spread of them", len(parts))
	}
	scenarios := differentialScenarios(keys)
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			requireSameBatch(t, whole, spread, scenario.build, keys[:14])
		})
	}
}

// TestSpreadBatchAtTheItemAndByteBounds repeats the batch bounds of a whole
// bucket on a spread one: 1,000 items run, 1,001 are refused whole, and a
// batch over the byte bound is refused whole, each with the whole bucket's
// words and code.
func TestSpreadBatchAtTheItemAndByteBounds(t *testing.T) {
	keys := spreadKeys(t, 64)
	whole, spread, _ := standaloneTargets(t, keys)
	many := func(count int) differentialItems {
		return func(t *testing.T, b string) []BatchItem {
			items := make([]BatchItem, count)
			for i := range items {
				items[i] = docKeyItem(t, transport.OpFieldLen, b, keys[i%len(keys)], "Feed", "", nil)
			}
			return items
		}
	}
	t.Run("at the item bound", func(t *testing.T) { requireSameBatch(t, whole, spread, many(batchMaxItems), keys) })
	t.Run("over the item bound", func(t *testing.T) { requireSameBatch(t, whole, spread, many(batchMaxItems+1), keys) })
	t.Run("over the byte bound", func(t *testing.T) {
		big := strings.Repeat("x", 6<<20)
		requireSameBatch(t, whole, spread, func(t *testing.T, b string) []BatchItem {
			items := make([]BatchItem, 3)
			for i := range items {
				items[i] = docKeyItem(t, transport.OpFieldPush, b, keys[i], "Feed", "", map[string]interface{}{"blob": big})
			}
			return items
		}, keys[:3])
	})
}

// TestSpreadBatchBeforeAnyPartIsBorn is the spread collection's own case: no
// part exists, so every item meets the born-set check, and the answers are
// the ones the same items give a whole bucket with no such documents.
func TestSpreadBatchBeforeAnyPartIsBorn(t *testing.T) {
	whole, spread, _ := standaloneTargets(t, nil)
	requireSameBatch(t, whole, spread, func(t *testing.T, b string) []BatchItem {
		return []BatchItem{setItem(t, b, "a", map[string]interface{}{"Name": "x"})}
	}, []string{"a"})
	requireSameBatch(t, whole, spread, func(t *testing.T, b string) []BatchItem {
		return []BatchItem{docKeyItem(t, transport.OpFieldLen, b, "a", "Feed", "", nil), docKeyItem(t, transport.OpFieldGet, b, "a", "Posts", "p", nil)}
	}, []string{"a"})
	requireSameBatch(t, whole, spread, func(t *testing.T, b string) []BatchItem {
		return []BatchItem{
			docKeyItem(t, transport.OpFieldPush, b, "a", "Feed", "", map[string]interface{}{"n": 1}),
			docKeyItem(t, transport.OpFieldLen, b, "a", "Feed", "", nil),
			setItem(t, b, "a", map[string]interface{}{"Name": "x"}),
		}
	}, []string{"a"})
}

// clusterTargets is a whole and a spread collection of one declaration on a
// cluster, with a document under every key, driven through node index.
func clusterTargets(t *testing.T, sc *spreadCluster, keys []string, index int) (whole, spread batchTarget) {
	t.Helper()
	target := func(name string) batchTarget {
		return batchTarget{
			name: name,
			send: func(items []BatchItem) ([]BatchAnswer, *ErrorResponse) {
				reply := sc.frame(index, transport.OpBatch, BatchRequest{Items: items})
				return decodeBatchReply(t, reply)
			},
			read: func(key string) string { return readClusterDocument(t, sc, index, name, key) },
		}
	}
	return target("BatchW"), target("BatchS")
}

// seedClusterTargets creates BatchW (whole) and BatchS (spread) with the
// differential declaration and a document under every key.
func seedClusterTargets(t *testing.T, sc *spreadCluster, keys []string) {
	t.Helper()
	for name, placement := range map[string]string{"BatchW": "whole", "BatchS": "spread"} {
		reply := sc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Options: map[string]interface{}{
			"placement": placement,
			"stores": []interface{}{
				map[string]interface{}{"name": "Feed", "engine": "fifoStore"},
				map[string]interface{}{"name": "Posts", "engine": "hashStore"},
			},
			"fields": []interface{}{
				map[string]interface{}{"name": "Name", "type": "string"},
				map[string]interface{}{"name": "Age", "type": "int"},
			},
		}})
		if !strings.Contains(string(reply.Payload), placement) {
			t.Fatalf("creating %s %s answered %q", name, placement, reply.Payload)
		}
		if placement == "spread" {
			sc.waitHeldBy(name, sc.templateSet(name))
		}
		for _, key := range keys {
			reply := sc.frame(0, transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: key, WriteConcern: "all",
				Data: map[string]interface{}{"Name": "n-" + key, "Age": int64(1)}})
			if reply.Opcode == transport.OpError {
				t.Fatalf("seeding %s/%s: %s", name, key, refusalText(t, reply))
			}
		}
	}
}

// readClusterDocument is readCollectionDocument through a cluster node.
func readClusterDocument(t *testing.T, sc *spreadCluster, index int, bucket, key string) string {
	t.Helper()
	found := findAnswer(t, sc.frame(index, transport.OpGet, FindRequest{Database: "app", Bucket: bucket, Key: key}))
	if !found.Found {
		return ""
	}
	var document bson.M
	if err := bson.Unmarshal(found.Data, &document); err != nil {
		t.Fatal(err)
	}
	blankMinted(document)
	text := fmt.Sprint(document)
	for _, field := range []string{"Feed", "Posts"} {
		length := sc.frame(index, transport.OpFieldLen, FieldRequest{Database: "app", Bucket: bucket, Key: key, Field: field})
		text += fmt.Sprintf(" %s=%s", field, normaliseAnswer(t, length.Payload))
	}
	return text
}

// TestSpreadBatchMatchesAWholeBucketAtEveryDoor sends the differential
// BATCHes to the template's owner (the router), to a replica of the
// template and to a node outside its set, at replication factor 2: the
// answers and the documents match a whole bucket's whichever door the client
// used (section 2.1). Each scenario runs in a fresh cluster so one door's
// writes never colour another's.
func TestSpreadBatchMatchesAWholeBucketAtEveryDoor(t *testing.T) {
	keys := spreadKeys(t, 120)
	doors := map[string]func(sc *spreadCluster) int{
		"owner":    func(sc *spreadCluster) int { return sc.templateSet("BatchS")[0] },
		"replica":  func(sc *spreadCluster) int { return sc.templateSet("BatchS")[1] },
		"outsider": func(sc *spreadCluster) int { return sc.nonHolder("BatchS") },
	}
	for door, pick := range doors {
		t.Run(door, func(t *testing.T) {
			sc := newSpreadCluster(t, 3, 2, "User", nil)
			seedClusterTargets(t, sc, keys)
			index := pick(sc)
			if index < 0 {
				t.Skip("every node holds the template")
			}
			whole, spread := clusterTargets(t, sc, keys, index)
			for _, scenario := range differentialScenarios(keys) {
				t.Run(scenario.name, func(t *testing.T) {
					requireSameBatch(t, whole, spread, scenario.build, keys[:14])
				})
			}
		})
	}
}
