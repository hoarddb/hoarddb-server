package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// newPostServer is a server with bucket app.Post declaring Comments (fifo),
// Ranked (heap) and Tags (hash), and documents p1 and p2.
func newPostServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	seedPost(t, s)
	return s
}

func seedPost(t *testing.T, s *Server) {
	t.Helper()
	resp := createWithStores(t, s, "app", "Post", BucketTypeHash,
		StoreDeclaration{Name: "Comments", Engine: "fifoStore"},
		StoreDeclaration{Name: "Ranked", Engine: "heapStore"},
		StoreDeclaration{Name: "Tags", Engine: "hashStore"})
	if !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	for _, key := range []string{"p1", "p2"} {
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: key, Data: map[string]interface{}{"title": key}}); err != nil || !resp.Success {
			t.Fatalf("inserting %s: %+v, %v", key, resp, err)
		}
	}
}

func fieldReq(key, field string) FieldRequest {
	return FieldRequest{Database: "app", Bucket: "Post", Key: key, Field: field}
}

// fieldOK fails the test unless a field write succeeded:
// fieldOK(t)(s.FieldPushCtx(...)).
func fieldOK(t *testing.T) func(*FieldResponse, error) *FieldResponse {
	return func(resp *FieldResponse, err error) *FieldResponse {
		t.Helper()
		if err != nil || !resp.Success {
			t.Fatalf("field write: %+v, %v", resp, err)
		}
		return resp
	}
}

func fieldLen(t *testing.T, s *Server, key, field string) int64 {
	t.Helper()
	resp, err := s.FieldLen(fieldReq(key, field))
	if err != nil {
		t.Fatalf("FieldLen(%s, %s): %v", key, field, err)
	}
	return resp.Len
}

// TestFieldOpcodesRegistered pins the eight verbs in every table a data verb
// needs, and the two routing facts the write path reads off the payload.
func TestFieldOpcodesRegistered(t *testing.T) {
	cases := []struct {
		op       transport.Opcode
		name     string
		readOnly bool
		rule     OpRule
	}{
		{transport.OpFieldPush, "FIELD_PUSH", false, OpRule{ActionWrite, ScopeDatabase}},
		{transport.OpFieldPop, "FIELD_POP", false, OpRule{ActionWrite, ScopeDatabase}},
		{transport.OpFieldPeek, "FIELD_PEEK", true, OpRule{ActionRead, ScopeDatabase}},
		{transport.OpFieldPut, "FIELD_PUT", false, OpRule{ActionWrite, ScopeDatabase}},
		{transport.OpFieldGet, "FIELD_GET", true, OpRule{ActionRead, ScopeDatabase}},
		{transport.OpFieldDelete, "FIELD_DELETE", false, OpRule{ActionWrite, ScopeDatabase}},
		{transport.OpFieldLen, "FIELD_LEN", true, OpRule{ActionRead, ScopeDatabase}},
		{transport.OpFieldList, "FIELD_LIST", true, OpRule{ActionRead, ScopeDatabase}},
	}
	payload, err := bson.Marshal(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Element: "e9"})
	if err != nil {
		t.Fatalf("encoding a field request: %v", err)
	}
	wantKey := bson.Raw(payload).Lookup("key").Value
	for i, c := range cases {
		if byte(c.op) != 0xD0+byte(i) {
			t.Errorf("%s = 0x%02X, want 0x%02X", c.name, byte(c.op), 0xD0+i)
		}
		if got := c.op.String(); got != c.name {
			t.Errorf("0x%02X.String() = %q, want %q", byte(c.op), got, c.name)
		}
		if got := c.op.IsReadOnly(); got != c.readOnly {
			t.Errorf("%s.IsReadOnly() = %v, want %v: a read routed as a write is WAL-appended and shipped on every holder", c.name, got, c.readOnly)
		}
		if got, ok := opRules[c.op]; !ok || got != c.rule {
			t.Errorf("opRules[%s] = %+v (present %v), want %+v", c.name, got, ok, c.rule)
		}
		if got := replicationRequired(c.op); got != !c.readOnly {
			t.Errorf("replicationRequired(%s) = %v, want %v", c.name, got, !c.readOnly)
		}
		meta := decodeFrameMeta(payload)
		if bucket, database := meta.scope(c.op); bucket != "Post" || database != "app" {
			t.Errorf("%s is scoped to (%q, %q), want (Post, app): a field store has no ring identity of its own", c.name, bucket, database)
		}
		if got := meta.key(); !bytes.Equal(got, wantKey) {
			t.Errorf("%s logs WAL key %x, want the parent document's key %x (F-2)", c.name, got, wantKey)
		}
	}
}

// TestFieldStoreAddressingRefusals is spec T-18: every addressing mistake is
// refused with spec §10.3's words and a code, never answered as an empty
// store.
func TestFieldStoreAddressingRefusals(t *testing.T) {
	s := newPostServer(t)
	ctx := context.Background()
	type verb struct {
		name string
		run  func(FieldRequest) error
	}
	withElement := func(req FieldRequest) FieldRequest { req.Element = "k"; return req }
	verbs := []verb{
		{"push", func(r FieldRequest) error { _, err := s.FieldPushCtx(ctx, r); return err }},
		{"pop", func(r FieldRequest) error { _, err := s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: r}); return err }},
		{"put", func(r FieldRequest) error { _, err := s.FieldPutCtx(ctx, withElement(r)); return err }},
		{"delete", func(r FieldRequest) error { _, err := s.FieldDeleteCtx(ctx, withElement(r)); return err }},
		{"peek", func(r FieldRequest) error { _, err := s.FieldPeek(FieldPeekRequest{FieldRequest: r}); return err }},
		{"get", func(r FieldRequest) error { _, err := s.FieldGet(withElement(r)); return err }},
		{"len", func(r FieldRequest) error { _, err := s.FieldLen(r); return err }},
		{"list", func(r FieldRequest) error { _, err := s.FieldList(FieldListRequest{FieldRequest: r}); return err }},
	}
	fieldFor := map[string]string{"push": "Comments", "pop": "Comments", "peek": "Comments", "put": "Tags", "get": "Tags", "delete": "Tags", "len": "Tags", "list": "Comments"}
	for _, v := range verbs {
		refusals := []struct {
			name     string
			req      FieldRequest
			sentinel error
			code     transport.ErrorCode
			says     []string
		}{
			{"undeclared field", fieldReq("p1", "Comment"), ErrFieldNotDeclared, transport.ErrNotFound,
				[]string{`declares no store field named "Comment"`, "It declares: Comments (fifo), Ranked (heap), Tags (hash).", "describe bucket Post in app"}},
			{"missing parent", fieldReq("nope", fieldFor[v.name]), ErrFieldParentNotFound, transport.ErrNotFound,
				[]string{`No document with key "nope" in bucket "Post"`, "the document has to exist first", `db.Post.find({_id_: "nope"})`}},
			{"empty bucket", FieldRequest{Database: "app", Key: "p1", Field: fieldFor[v.name]}, nil, 0, nil},
			{"empty key", fieldReq("", fieldFor[v.name]), ErrFieldAddressIncomplete, transport.ErrInvalidArgument, []string{"no key"}},
			{"empty field", fieldReq("p1", ""), ErrFieldAddressIncomplete, transport.ErrInvalidArgument, []string{"no field"}},
		}
		for _, r := range refusals {
			err := v.run(r.req)
			if err == nil {
				t.Errorf("%s with %s: answered as if the store were empty; want a refusal", v.name, r.name)
				continue
			}
			if r.sentinel == nil {
				continue // the bucket name is resolved first: its refusal is the bucket's own
			}
			if !errors.Is(err, r.sentinel) || codeFor(err) != r.code {
				t.Errorf("%s with %s: %v (code %v), want %v with code %v", v.name, r.name, err, codeFor(err), r.sentinel, r.code)
			}
			for _, part := range r.says {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("%s with %s: %q does not say %q", v.name, r.name, err, part)
				}
			}
		}
	}
	wrong := []struct {
		name string
		run  func() error
	}{
		{"pop on a hash field", func() error {
			_, err := s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: fieldReq("p1", "Tags")})
			return err
		}},
		{"put on a fifo field", func() error { _, err := s.FieldPutCtx(ctx, withElement(fieldReq("p1", "Comments"))); return err }},
		{"get on a heap field", func() error { _, err := s.FieldGet(withElement(fieldReq("p1", "Ranked"))); return err }},
	}
	for _, w := range wrong {
		err := w.run()
		if !errors.Is(err, ErrFieldWrongVerb) || codeFor(err) != transport.ErrInvalidArgument {
			t.Errorf("%s: %v, want ErrFieldWrongVerb with InvalidArgument", w.name, err)
		} else if !strings.Contains(err.Error(), "Declared engines on \"Post\": Comments (fifo), Ranked (heap), Tags (hash)") {
			t.Errorf("%s: %q does not list the declared engines", w.name, err)
		}
	}
}

func TestFieldVerbsEndToEnd(t *testing.T) {
	s := newPostServer(t)
	ctx := context.Background()
	for n := range 3 {
		fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": n}}))
	}
	fieldOK(t)(s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p2", Field: "Comments", Data: map[string]interface{}{"n": 100}}))
	peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: fieldReq("p1", "Comments")})
	if err != nil || !peeked.Found || len(peeked.Items) != 1 || peeked.Items[0].Element.Lookup("n").Int32() != 0 {
		t.Fatalf("peek on p1's Comments = %+v, %v; want the oldest, n 0", peeked, err)
	}
	popped := fieldOK(t)(s.FieldPopCtx(ctx, FieldPopRequest{FieldRequest: fieldReq("p1", "Comments")}))
	if !popped.Found || popped.Items[0].Element.Lookup("n").Int32() != 0 || popped.Items[0].Position == nil || *popped.Items[0].Position != 0 {
		t.Fatalf("pop on p1's Comments = %+v; want n 0 at position 0", popped)
	}
	if got := fieldLen(t, s, "p1", "Comments"); got != 2 {
		t.Fatalf("p1's Comments len = %d, want 2", got)
	}
	if got := fieldLen(t, s, "p2", "Comments"); got != 1 {
		t.Fatalf("p2's Comments len = %d, want 1", got)
	}
	put := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "go", Data: map[string]interface{}{"weight": 3}}
	fieldOK(t)(s.FieldPutCtx(ctx, put))
	got, err := s.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "go"})
	if err != nil || !got.Found || got.Element.Lookup("weight").Int32() != 3 {
		t.Fatalf("get of Tags[go] = %+v, %v", got, err)
	}
	page, err := s.FieldList(FieldListRequest{FieldRequest: fieldReq("p1", "Comments"), Limit: 1})
	if err != nil || len(page.Entries) != 1 || !page.HasMore || page.Entries[0].Position == nil || *page.Entries[0].Position != 1 {
		t.Fatalf("the first page of p1's Comments = %+v, %v; want position 1, has_more", page, err)
	}
	rest, err := s.FieldList(FieldListRequest{FieldRequest: fieldReq("p1", "Comments"), After: bson.RawValue{Type: bson.TypeBinary, Value: binaryValue(page.Next)}})
	if err != nil || len(rest.Entries) != 1 || rest.HasMore || rest.Entries[0].Position == nil || *rest.Entries[0].Position != 2 {
		t.Fatalf("the page after %x = %+v, %v; want position 2 and no more", page.Next, rest, err)
	}
	deleted := fieldOK(t)(s.FieldDeleteCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "go"}))
	if !deleted.Found || fieldLen(t, s, "p1", "Tags") != 0 {
		t.Fatalf("deleting Tags[go] = %+v; want found and len 0", deleted)
	}
}

// binaryValue is a BSON binary value's bytes: int32 length, subtype 0, data.
func binaryValue(data []byte) []byte {
	encoded := make([]byte, 0, 5+len(data))
	encoded = append(encoded, byte(len(data)), byte(len(data)>>8), byte(len(data)>>16), byte(len(data)>>24), 0x00)
	return append(encoded, data...)
}

// fieldKeysOf counts the keys under a document's field-store owner.
func fieldKeysOf(t *testing.T, s *Server, key string) int {
	t.Helper()
	b := servedBucket(t, s, "app", "Post")
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), key)
	if err != nil {
		t.Fatalf("owner of %s: %v", key, err)
	}
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	upper := append(append([]byte(nil), owner[:len(owner)-1]...), owner[len(owner)-1]+1)
	it := snap.NewIterator(storage.IterOptions{Lower: owner, Upper: upper})
	defer it.Close()
	n := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		n++
	}
	return n
}

func appliedSeqOf(t *testing.T, s *Server) uint64 {
	t.Helper()
	b := servedBucket(t, s, "app", "Post")
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	st, err := store.LoadBucketState(snap, b.slot, b.databaseID, b.id.Load())
	if err != nil {
		t.Fatalf("loading Post's state: %v", err)
	}
	return st.AppliedSeq
}

// TestDocumentAndFieldWriteAreOneTransaction is spec T-06.
func TestDocumentAndFieldWriteAreOneTransaction(t *testing.T) {
	s := newPostServer(t)
	fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p2", Field: "Comments", Data: map[string]interface{}{"n": 1}}))
	seqBefore := appliedSeqOf(t, s)

	fail := func(storage.KV, uint16, uint64, uint64) error { return errors.New("injected commit failure") }
	store.ApplyFaultHook.Store(&fail)
	t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })

	ctx := withApplySeq(context.Background(), seqBefore+10)
	resp, err := s.FieldPushCtx(ctx, FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": 1}})
	if err != nil || resp.Success {
		t.Fatalf("a push whose commit fails = %+v, %v; want Success false", resp, err)
	}
	deleted, err := s.DeleteCtx(withApplySeq(context.Background(), seqBefore+11), DeleteRequest{Database: "app", Bucket: "Post", Key: "p2"})
	if err != nil || deleted.Success {
		t.Fatalf("a delete whose commit fails = %+v, %v; want Success false", deleted, err)
	}
	store.ApplyFaultHook.Store(nil)

	if got := fieldKeysOf(t, s, "p1"); got != 0 {
		t.Fatalf("the failed push left %d keys under p1's stores: an element or a state without the other", got)
	}
	if got := fieldKeysOf(t, s, "p2"); got != 2 {
		t.Fatalf("the failed delete left %d of p2's 2 field-store keys", got)
	}
	if doc, _ := s.Find(FindRequest{Database: "app", Bucket: "Post", Key: "p2"}); doc == nil || !doc.Found {
		t.Fatalf("the failed delete removed p2")
	}
	if got := appliedSeqOf(t, s); got != seqBefore {
		t.Fatalf("applied_seq moved from %d to %d across two failed commits", seqBefore, got)
	}
}

// TestFieldPushIDsMintedOnce, the standalone half: the element's _id_ is the
// one the coordinator minted.
func TestFieldPushIDsMintedOnce(t *testing.T) {
	s := newPostServer(t)
	payload, err := bson.Marshal(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Ranked", Priority: 5, Data: map[string]interface{}{"n": 1}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	rewritten, err := resolveDocumentIDs(transport.OpFieldPush, payload)
	if err != nil || rewritten == nil {
		t.Fatalf("resolveDocumentIDs(FIELD_PUSH) = %v, %v; want the payload with a minted _id_", rewritten, err)
	}
	minted := bson.Raw(rewritten).Lookup("data", "_id_").StringValue()
	answer, err := s.dispatch(context.Background(), transport.NewFrame(transport.OpFieldPush, rewritten))
	if err != nil || answer.Opcode != transport.OpFieldPush {
		t.Fatalf("dispatching the push: %v, %v", answer, err)
	}
	peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: fieldReq("p1", "Ranked")})
	if err != nil || len(peeked.Items) != 1 {
		t.Fatalf("peeking the heap field: %+v, %v", peeked, err)
	}
	if item := peeked.Items[0]; item.EntryID != minted || item.Element.Lookup("_id_").StringValue() != minted {
		t.Fatalf("the heap element is %+v; want entry id and _id_ %q, the coordinator's", item, minted)
	}
}

func TestDeleteDocumentRefusedWhenCascadeTooLarge(t *testing.T) {
	s := newPostServer(t)
	b := servedBucket(t, s, "app", "Post")
	owner, err := store.FieldOwnerPrefix(b.slot, b.databaseID, b.id.Load(), "p1")
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	ref := store.FieldRef{Owner: owner, FieldID: 1, Engine: store.FieldFIFO}
	element, _ := bson.Marshal(map[string]interface{}{"n": 1})
	// A delete is charged its key alone, so the cascade needs more elements
	// than the bound has bytes per key; they are pushed thousands to a batch.
	total := int(s.kv.MaxBatchBytes()/40) + 1
	for pushed := 0; pushed < total; {
		_, err := store.Apply(store.ApplyRequest{KV: s.kv, Slot: b.slot, DatabaseID: b.databaseID, ID: b.id.Load(), Owed: b.hashStore.Carry(),
			Op: func(snap storage.KVSnapshot, st *store.BucketState, batch storage.KVBatch) error {
				// The state is read from the snapshot, so one batch holds one push.
				_, err := store.FieldPush(store.FieldWrite{Snap: snap, State: st, Batch: batch}, ref, element)
				return err
			}})
		if err != nil {
			t.Fatalf("pushing element %d: %v", pushed, err)
		}
		pushed++
		if pushed%5000 == 0 {
			t.Logf("%d of %d elements", pushed, total)
		}
	}
	resp, err := s.DeleteCtx(context.Background(), DeleteRequest{Database: "app", Bucket: "Post", Key: "p1"})
	if err != nil || resp.Success {
		t.Fatalf("deleting p1 = %+v, %v; want refused", resp, err)
	}
	want := `Document "p1" in bucket "Post" was not deleted: its store fields hold too many elements to delete in one step (Comments: ` +
		groupThousands(total) + `). Empty them first — db.Post["p1"].Comments.pop() until len() is 0 — then delete the document.`
	if resp.Message != want {
		t.Fatalf("the refusal reads\n%s\nwant S-3's\n%s", resp.Message, want)
	}
	if doc, _ := s.Find(FindRequest{Database: "app", Bucket: "Post", Key: "p1"}); doc == nil || !doc.Found {
		t.Fatalf("the refused delete removed p1")
	}
}

// markerEdgeSlack is how many bytes at each end of the marker the plaintext
// control does not rely on.
const markerEdgeSlack = 4

// markerScanWindow is the length of the marker fragment the sealed side looks
// for at every offset: long enough that a chance match in ciphertext is
// impossible (2^-64 per offset), short enough that no compression or framing
// of the marker can hide every window.
const markerScanWindow = 8

// anyMarkerWindow returns the offset of the first markerScanWindow-byte window
// of marker found in contents, or -1. On a sealed store the assertion is that
// no window appears, which is strictly stronger than the whole marker's absence.
func anyMarkerWindow(contents, marker []byte) int {
	for offset := 0; offset+markerScanWindow <= len(marker); offset++ {
		if bytes.Contains(contents, marker[offset:offset+markerScanWindow]) {
			return offset
		}
	}
	return -1
}

// TestFieldStoreElementsSealedWithTheNodeStore is spec T-20, recast (RD-10):
// no identity string is owed; what is true and testable is that elements
// are inside the sealed node store. The plaintext half is what lets it fail.
func TestFieldStoreElementsSealedWithTheNodeStore(t *testing.T) {
	for _, mode := range []string{"on", "off"} {
		t.Run(mode, func(t *testing.T) {
			dataDir := newClusteredTestDataDir(t)
			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = dataDir
			cfg.Metrics.Enabled = false
			cfg.Audit.Enabled = false
			if mode == "on" {
				cfg = sealedServiceConfig(dataDir, makeKeyFile(t, t.TempDir(), 0x22), "on")
			} else {
				cfg.Encryption.Mode, cfg.Encryption.Keystore = "off", "off"
			}
			srv, err := NewService(cfg, quietLogger())
			if err != nil {
				t.Fatalf("starting a mode=%s node: %v", mode, err)
			}
			seedPost(t, srv.core)
			marker := make([]byte, 32)
			if _, err := rand.Read(marker); err != nil {
				t.Fatalf("drawing the marker: %v", err)
			}
			// The plaintext store compresses its blocks, and a
			// compressor can replace the marker's first or last byte with a
			// back-reference to a neighbouring record byte, so the whole marker
			// is not always contiguous on disk (about 1 run in 100). The
			// control therefore looks for the interior; four bytes of slack per
			// edge means a loss would need a four-byte match. A false hit on
			// 24 random bytes has probability 2^-192 per offset.
			markerInterior := marker[markerEdgeSlack : len(marker)-markerEdgeSlack]
			fieldOK(t)(srv.core.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"marker": marker}}))
			if err := srv.Stop(); err != nil {
				t.Fatalf("stopping the node: %v", err)
			}
			found := ""
			err = filepath.WalkDir(filepath.Join(dataDir, nodeStoreDirName), func(path string, entry fs.DirEntry, err error) error {
				if err != nil || !entry.Type().IsRegular() {
					return err
				}
				contents, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if mode == "off" && bytes.Contains(contents, markerInterior) {
					found = path
				}
				if mode == "on" {
					if window := anyMarkerWindow(contents, marker); window >= 0 {
						found = fmt.Sprintf("%s (marker bytes %d..%d)", path, window, window+markerScanWindow)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("scanning the store: %v", err)
			}
			if mode == "on" && found != "" {
				t.Fatalf("an element's bytes are in plaintext in %s on a node whose mode is on", found)
			}
			if mode == "off" && found == "" {
				t.Fatalf("an element's bytes are nowhere under store/ on a plaintext node: the scan cannot see what it looks for")
			}
		})
	}
}
