package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// GAP-35: a blob has an explicit size cap, enforced where a client's write
// enters the node and nowhere a replica or a replay applies it.

const blobLimitPointerAdvice = `For larger files, keep the file on your own storage and store a pointer document — see "Large file stores" in the docs.`

// blobLimitServer is a standalone node with a blob bucket "app.files" and the
// given cap (0 leaves the configured default).
func blobLimitServer(t *testing.T, capBytes int) (*Server, transport.RequestHandler) {
	t.Helper()
	s := newTestServer(t)
	if capBytes != 0 {
		s.config.Storage.MaxBlobBytes = capBytes
	}
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "files", Type: BucketTypeBlob})
	return s, handler
}

func blobWriteFrame(t *testing.T, handler transport.RequestHandler, key string, size int) *transport.Frame {
	t.Helper()
	frame, err := handler(context.Background(), transport.NewFrame(transport.OpBlobWrite, mustMarshalBSON(t, BlobWriteRequest{
		Database: "app", Bucket: "files", Key: key, Data: make([]byte, size),
	})))
	if err != nil {
		t.Fatalf("blob write of %d bytes: handler error %v", size, err)
	}
	return frame
}

// requireBlobRefusal decodes an error frame and checks the code and the exact
// text.
func requireBlobRefusal(t *testing.T, frame *transport.Frame, wantMessage string) {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("blob write answered %s, want an error frame", frame.Opcode)
	}
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if refusal.Code != int(transport.ErrInvalidArgument) {
		t.Errorf("refusal code = %d, want %d (INVALID_ARGUMENT)", refusal.Code, transport.ErrInvalidArgument)
	}
	if refusal.Message != wantMessage {
		t.Errorf("refusal text:\n got: %s\nwant: %s", refusal.Message, wantMessage)
	}
}

func TestBlobAtTheCapIsAcceptedAndOneByteOverIsRefused(t *testing.T) {
	const capBytes = 1 << 20
	_, handler := blobLimitServer(t, capBytes)

	if frame := blobWriteFrame(t, handler, "at-cap", capBytes); frame.Opcode == transport.OpError {
		t.Fatalf("a blob of exactly the cap was refused: %s", frame.Payload)
	}

	over := blobWriteFrame(t, handler, "over-cap", capBytes+1)
	requireBlobRefusal(t, over, fmt.Sprintf(
		`Blob "over-cap" is %d bytes; this server's limit is %d bytes (storage.max_blob_bytes, maximum 31.9 MiB). %s`,
		capBytes+1, capBytes, blobLimitPointerAdvice))
}

func TestBlobLimitDefaultsTo16MiBOnAServerBuiltWithNoCapSet(t *testing.T) {
	_, handler := blobLimitServer(t, 0)

	if frame := blobWriteFrame(t, handler, "sixteen", 16<<20); frame.Opcode == transport.OpError {
		t.Fatalf("a 16 MiB blob was refused on a default node: %s", frame.Payload)
	}
	over := blobWriteFrame(t, handler, "videos/intro.mp4", 20<<20)
	requireBlobRefusal(t, over, `Blob "videos/intro.mp4" is 20.0 MiB; this server's limit is 16 MiB (storage.max_blob_bytes, maximum 31.9 MiB). `+blobLimitPointerAdvice)
}

func TestRaisedBlobCapTakesEffect(t *testing.T) {
	s, handler := blobLimitServer(t, 0)

	if frame := blobWriteFrame(t, handler, "twenty", 20<<20); frame.Opcode != transport.OpError {
		t.Fatal("a 20 MiB blob was accepted at the default cap")
	}
	s.config.Storage.MaxBlobBytes = config.MaxBlobBytesCeiling
	if frame := blobWriteFrame(t, handler, "twenty", 20<<20); frame.Opcode == transport.OpError {
		t.Fatalf("a 20 MiB blob was refused although the cap was raised to the ceiling: %.200s", frame.Payload)
	}
}

// The refusal comes before the payload is hashed or written: nothing reaches
// the blob directory and nothing is readable.
func TestOversizedBlobWritesNothing(t *testing.T) {
	const capBytes = 128 << 10 // above InlineThreshold, so an accepted blob is a file
	s, handler := blobLimitServer(t, capBytes)

	requireBlobRefusalCode(t, blobWriteFrame(t, handler, "too-big", capBytes+1))

	var files []string
	walkErr := filepath.WalkDir(s.blobRoot(), func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking the blob directory: %v", walkErr)
	}
	if len(files) != 0 {
		t.Errorf("a refused blob left files behind: %v", files)
	}
	read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "files", Key: "too-big"})
	if err != nil {
		t.Fatalf("BlobRead: %v", err)
	}
	if read.Found {
		t.Error("a refused blob can be read back")
	}
}

func requireBlobRefusalCode(t *testing.T, frame *transport.Frame) {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("blob write answered %s, want an error frame", frame.Opcode)
	}
}

// requireBlobBucketPutRefusal fails unless frame is the refusal of a PUT into
// the blob bucket named bucket (build doc BD-7): INVALID_ARGUMENT, naming the
// verb that writes a blob bucket, and never the advice to use a blob bucket
// or the document limit.
func requireBlobBucketPutRefusal(t *testing.T, frame *transport.Frame, bucket string) {
	t.Helper()
	message := wireRefusal(t, frame)
	want := fmt.Sprintf(`a blob bucket is written with put: db.%s.put("key", ...)`, bucket)
	if !strings.Contains(message, want) {
		t.Errorf("a PUT into blob bucket %q got %.300q; want the blob-bucket refusal saying %q", bucket, message, want)
	}
	for _, wrong := range []string{"belongs in a blob bucket", "storage.max_document_bytes"} {
		if strings.Contains(message, wrong) {
			t.Errorf("a PUT into blob bucket %q was told %q: %.300q", bucket, wrong, message)
		}
	}
}

// A blob bucket is written with put (BLOB_WRITE), which replaces; a document
// PUT into one is refused whatever its size (document-placement spec §6.3,
// build doc BD-7), a small one by the handler and one over the cap at the
// door, and nothing is stored. The cap itself is BLOB_WRITE's
// (TestBlobAtTheCapIsAcceptedAndOneByteOverIsRefused).
func TestDocumentPutIntoABlobBucketIsRefused(t *testing.T) {
	const capBytes = 1 << 20
	s, handler := blobLimitServer(t, capBytes)

	for key, size := range map[string]int{"small": 64, "over-cap": capBytes + 1} {
		frame, err := handler(context.Background(), transport.NewFrame(transport.OpPut, mustMarshalBSON(t, InsertRequest{
			Database: "app", Bucket: "files", Key: key,
			Data: map[string]interface{}{"body": strings.Repeat("x", size)},
		})))
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		requireBlobBucketPutRefusal(t, frame, "files")
		if read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "files", Key: key}); err != nil || read.Found {
			t.Errorf("the refused put of %s is readable: %+v, %v", key, read, err)
		}
	}
}

// A write whose replication frame cannot fit is refused at the door, not
// accepted by the owner and then lost on the way to the replica.
func TestBlobWhoseKeyMakesItsReplicationFrameTooLargeIsRefused(t *testing.T) {
	_, handler := blobLimitServer(t, 0)

	frame := blobWriteFrame(t, handler, strings.Repeat("k", 17<<20), 1<<10)
	requireBlobRefusalCode(t, frame)
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if !strings.Contains(refusal.Message, "replicate") || !strings.Contains(refusal.Message, "key") {
		t.Errorf("a %d-byte refusal does not say the key is what cannot be replicated: %.200q", len(refusal.Message), refusal.Message)
	}
}

// The replica applies what its owner accepted, whatever its own cap says:
// the cap is enforced where a client's write enters, not where an entry is
// applied.
func TestReplicaAppliesABlobItsOwnerAcceptedWhateverItsOwnCap(t *testing.T) {
	tc := newTestClusterWith(t, 2, 2, func(index int, cfg *config.Config) {
		if index == 1 {
			cfg.Storage.MaxBlobBytes = 100 << 10
		}
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeBlob})

	const size = 2 << 20
	tc.ok(0, transport.OpBlobWrite, BlobWriteRequest{
		Database: testDatabase, Bucket: testBucket, Key: "big", Data: make([]byte, size),
	})

	replicaCopy := tc.blobReadFromNode(1, "big")
	if !replicaCopy.Found || replicaCopy.Size != size {
		t.Fatalf("the replica (cap 100 KiB) holds found=%v size=%d of the owner's %d-byte blob: %q",
			replicaCopy.Found, replicaCopy.Size, size, replicaCopy.Message)
	}

	refused := tc.frame(1, transport.OpBlobWrite, BlobWriteRequest{
		Database: testDatabase, Bucket: testBucket, Key: "via-replica", Data: make([]byte, size),
	})
	if refused.Opcode != transport.OpError {
		t.Fatalf("the node with the 100 KiB cap accepted a 2 MiB client write")
	}
}

// The ceiling is honest: a blob of exactly config.MaxBlobBytesCeiling, with a
// key at the top of what the door allows, is written, replicated and read
// back through the cluster.
func TestBlobAtTheCeilingReplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("moves two 32 MiB frames")
	}
	tc := newTestClusterWith(t, 2, 2, func(_ int, cfg *config.Config) {
		cfg.Storage.MaxBlobBytes = config.MaxBlobBytesCeiling
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeBlob})

	key := strings.Repeat("k", 16<<10)
	tc.ok(0, transport.OpBlobWrite, BlobWriteRequest{
		Database: testDatabase, Bucket: testBucket, Key: key, Data: make([]byte, config.MaxBlobBytesCeiling),
	})
	for node := 0; node < 2; node++ {
		copyOnNode := tc.blobReadFromNode(node, key)
		if !copyOnNode.Found || copyOnNode.Size != config.MaxBlobBytesCeiling {
			t.Fatalf("node %d holds found=%v size=%d of a %d-byte blob: %q",
				node+1, copyOnNode.Found, copyOnNode.Size, config.MaxBlobBytesCeiling, copyOnNode.Message)
		}
	}
}

func TestBlobSizeRefusalTextIsTheDocumentedShape(t *testing.T) {
	got := blobSizeRefusalText("videos/intro.mp4", 5<<30, 16<<20)
	want := `Blob "videos/intro.mp4" is 5.0 GiB; this server's limit is 16 MiB (storage.max_blob_bytes, maximum 31.9 MiB). ` + blobLimitPointerAdvice
	if got != want {
		t.Errorf("refusal text:\n got: %s\nwant: %s", got, want)
	}
}

// The ceiling reserves config.BlobFrameReserve for the envelope and the
// replication wrapper. This measures the wrapper against the constant the
// door uses for it.
func TestBlobReplicationOverheadFitsItsReserve(t *testing.T) {
	const (
		payloadBytes = 1 << 20
		keyBytes     = 200
		bucketBytes  = 64
	)
	key, bucket := strings.Repeat("k", keyBytes), strings.Repeat("b", bucketBytes)
	push := ReplPushRequest{
		Token:    strings.Repeat("t", 256),
		FromNode: strings.Repeat("n", 256),
		Entries: []ReplEntry{{
			Seq: 1<<63 - 1, Op: int32(transport.OpBlobWrite), Bucket: bucket,
			Key: []byte(key), Payload: make([]byte, payloadBytes), OwnerOutcome: 1,
		}},
		WriteConcern: 3, ReturnResponse: true, Epoch: 1<<63 - 1,
		// A stream push names the sender's incarnation instead of asking for
		// responses; both are counted, so the bound covers either.
		Stream: &ReplStream{Incarnation: make([]byte, 16)},
	}
	body := 1 + transport.RequestIDLen + len(mustMarshalBSON(t, push))
	fixed := body - payloadBytes - keyBytes - bucketBytes
	if fixed > blobReplicationFixedBytes {
		t.Errorf("the replication wrapper adds %d bytes beyond payload, key and bucket name; blobReplicationFixedBytes is %d",
			fixed, blobReplicationFixedBytes)
	}
	if config.BlobFrameReserve < blobReplicationFixedBytes+2*(16<<10) {
		t.Errorf("BlobFrameReserve %d leaves no room for a 16 KiB key sent twice beside %d bytes of fixed overhead",
			config.BlobFrameReserve, blobReplicationFixedBytes)
	}
}

// Server.BlobWrite is the embedded entry point; it is a client write too.
func TestServerBlobWriteMethodIsCapped(t *testing.T) {
	s, _ := blobLimitServer(t, 1<<20)

	_, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "files", Key: "big", Data: make([]byte, 1<<20+1)})

	if !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("BlobWrite of cap+1 bytes = %v, want ErrBlobTooLarge", err)
	}
}

// A blobStore field's element is a blob: its content is held to the same cap.
func TestBlobFieldElementContentIsCapped(t *testing.T) {
	const capBytes = 100 << 10
	s, _ := newBlobFieldServer(t)
	s.config.Storage.MaxBlobBytes = capBytes
	handler := NewHandler(s)

	put := func(size int) *transport.Frame {
		req := fieldReq("p1", "Photos")
		req.Element = "big.jpg"
		req.Content = make([]byte, size)
		return sendFrame(t, handler, transport.OpFieldPut, req)
	}
	if frame := put(capBytes); frame.Opcode == transport.OpError {
		t.Fatalf("content of exactly the cap was refused: %.300s", frame.Payload)
	}
	if frame := put(capBytes + 1); frame.Opcode != transport.OpError {
		t.Fatal("content one byte over the cap was accepted")
	}
}

// A blob bucket's write is held to the blob cap, not the document limit
// (BUG-0225): with the cap raised above the document limit, a BLOB_WRITE the
// cap allows is accepted, and a hash-bucket put of the same size is still held
// to the document limit. A document PUT into the blob bucket is refused as a
// blob-bucket PUT at any size (BD-7), never with the advice that a large value
// belongs in a blob bucket.
func TestBlobBucketPutIsHeldToTheBlobCapNotTheDocumentLimit(t *testing.T) {
	const blobBucketAdvice = "belongs in a blob bucket"
	put := func(t *testing.T, handler transport.RequestHandler, bucket, key string, size int) *transport.Frame {
		t.Helper()
		return sendFrame(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: bucket, Key: key, Data: docOfEncodedSize(size)})
	}
	blobWrite := func(t *testing.T, handler transport.RequestHandler, key string, size int) *transport.Frame {
		t.Helper()
		return sendFrame(t, handler, transport.OpBlobWrite, BlobWriteRequest{Database: "app", Bucket: "files", Key: key, Data: make([]byte, size)})
	}
	requireBlobCapRefusal := func(t *testing.T, frame *transport.Frame) {
		t.Helper()
		message := wireRefusal(t, frame)
		if !strings.Contains(message, `Blob "`) || !strings.Contains(message, "storage.max_blob_bytes") {
			t.Errorf("a blob write over the blob cap got %.300q, want the blob refusal", message)
		}
		if strings.Contains(message, blobBucketAdvice) {
			t.Errorf("a blob write was told %q: %.300q", blobBucketAdvice, message)
		}
	}

	t.Run("blob cap above the document limit", func(t *testing.T) {
		s, handler := blobLimitServer(t, config.MaxBlobBytesCeiling)
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
		const between = 26 << 20
		if between <= s.maxDocumentBytes() || between > s.maxBlobBytes() {
			t.Fatalf("%d bytes is not between the document limit %d and the blob cap %d", between, s.maxDocumentBytes(), s.maxBlobBytes())
		}

		if frame := blobWrite(t, handler, "doc", between); frame.Opcode == transport.OpError {
			t.Errorf("a 26 MiB blob write into a blob bucket whose cap is %d bytes was refused: %.300s", s.maxBlobBytes(), wireRefusal(t, frame))
		} else if read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "files", Key: "doc"}); err != nil || !read.Found {
			t.Errorf("the accepted blob write cannot be read back: %+v, %v", read, err)
		}
		requireBlobCapRefusal(t, blobWrite(t, handler, "over-cap", config.MaxBlobBytesCeiling+1))
		requireBlobBucketPutRefusal(t, put(t, handler, "files", "put-between", between), "files")
		requireBlobBucketPutRefusal(t, put(t, handler, "files", "put-small", 64), "files")

		message := wireRefusal(t, put(t, handler, "docs", "doc", between))
		if !strings.Contains(message, `Document "doc" is 26.0 MiB`) || !strings.Contains(message, "storage.max_document_bytes") {
			t.Errorf("a 26 MiB put into a hash bucket got %.300q, want the document-limit refusal", message)
		}
	})

	t.Run("blob cap below the document limit", func(t *testing.T) {
		const capBytes, documentLimit = 1 << 20, 2 << 20
		s, handler := blobLimitServer(t, capBytes)
		s.config.Storage.MaxDocumentBytes = documentLimit
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

		if frame := blobWrite(t, handler, "at-cap", capBytes); frame.Opcode == transport.OpError {
			t.Errorf("a blob write of exactly the blob cap was refused: %.300s", frame.Payload)
		}
		requireBlobCapRefusal(t, blobWrite(t, handler, "between", capBytes+(512<<10)))
		requireBlobBucketPutRefusal(t, put(t, handler, "files", "put-between", capBytes+(512<<10)), "files")
		requireBlobBucketPutRefusal(t, put(t, handler, "files", "put-over-both", documentLimit+(512<<10)), "files")

		if frame := put(t, handler, "docs", "between", capBytes+(512<<10)); frame.Opcode == transport.OpError {
			t.Errorf("a hash-bucket put under the document limit was refused for the blob cap: %.300s", frame.Payload)
		}
		message := wireRefusal(t, put(t, handler, "docs", "over-both", documentLimit+(512<<10)))
		if !strings.Contains(message, "storage.max_document_bytes") || !strings.Contains(message, blobBucketAdvice) {
			t.Errorf("a hash-bucket put over the document limit got %.300q, want the document-limit refusal and its advice", message)
		}
	})
}

// nodeThatHasNotLearned returns the index of a node that neither owns nor
// holds database/bucket yet. With a replication factor of one the bucket is
// placed on its owner alone, so every other node is a candidate: the node it
// returns has not learned the bucket's type, which is the entering node
// BUG-0582 is about.
func nodeThatHasNotLearned(t *testing.T, tc *testCluster, database, bucket string) int {
	t.Helper()
	for i := range tc.nodes {
		if _, held := tc.nodes[i].server.core.peekBucket(database, bucket); !held {
			return i
		}
	}
	t.Fatalf("every node holds bucket %q; the unlearned window is closed", bucket)
	return -1
}

// TestBlobBucketPutOnANodeThatHasNotLearnedTheBucketIsHeldToTheBlobCap is
// BUG-0582's acceptance test. The blob cap is raised to its ceiling (above the
// 24 MiB document limit) and a 26 MiB put enters a node that does not hold the
// bucket (RF=1, so it is not a replica and has not learned its type). Before
// the fix the entering node's door fell through to the document limit and
// refused it with the "belongs in a blob bucket" advice. The door still lets
// it through to the owner, which knows the type; since step 5 (BD-7) the owner
// refuses a document PUT into a blob bucket whatever its size, naming put, and
// stores nothing. It is never told the document limit or that advice.
func TestBlobBucketPutOnANodeThatHasNotLearnedTheBucketIsHeldToTheBlobCap(t *testing.T) {
	const between = 26 << 20
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) {
		cfg.Storage.MaxBlobBytes = config.MaxBlobBytesCeiling
	})
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeBlob})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", testBucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	entering := nodeThatHasNotLearned(t, tc, testDatabase, testBucket)

	requireBlobBucketPutRefusal(t, tc.frame(entering, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "doc",
		Data: docOfEncodedSize(between),
	}), testBucket)
	if read := tc.blobReadFromNode(ownerIndex, "doc"); read.Found {
		t.Fatalf("the owner stored a blob-bucket put it refused: %q", read.Message)
	}
}

// TestBlobBucketPutOverTheCapOnANodeThatHasNotLearnedTheBucketIsRefusedByTheOwner
// is BUG-0582's default-limits half: a 20 MiB put is over the 16 MiB blob cap
// but under the 24 MiB document limit, so the entering node's door, which
// cannot know the type, must let it through and the owner must refuse it and
// must not store it. Before the fix the owner stored it above its own cap;
// since step 5 (BD-7) the owner refuses any document PUT into a blob bucket,
// as INVALID_ARGUMENT naming put.
func TestBlobBucketPutOverTheCapOnANodeThatHasNotLearnedTheBucketIsRefusedByTheOwner(t *testing.T) {
	const overCap = 20 << 20
	tc := newTestClusterWith(t, 2, 1, nil)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeBlob})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolving the owner of %q: %v", testBucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	entering := nodeThatHasNotLearned(t, tc, testDatabase, testBucket)

	requireBlobBucketPutRefusal(t, tc.frame(entering, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: "doc",
		Data: docOfEncodedSize(overCap),
	}), testBucket)
	if read := tc.blobReadFromNode(ownerIndex, "doc"); read.Found {
		t.Fatalf("the owner stored a put it refused: %q", read.Message)
	}
}

// TestHashBucketPutOnANodeThatHasNotLearnedTheBucketIsHeldToTheDocumentLimit
// is BUG-0582's other-type control: a 26 MiB put into a hash bucket the
// entering node has not learned, with the blob cap raised above the document
// limit, is measured by the owner against the document limit and refused with
// the document advice. The bucket's type decides the limit, not the verb.
func TestHashBucketPutOnANodeThatHasNotLearnedTheBucketIsHeldToTheDocumentLimit(t *testing.T) {
	const between = 26 << 20
	tc := newTestClusterWith(t, 2, 1, func(_ int, cfg *config.Config) {
		cfg.Storage.MaxBlobBytes = config.MaxBlobBytesCeiling
	})
	defer tc.stop()
	const hashBucket = "papers"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hashBucket, Type: BucketTypeHash})

	entering := nodeThatHasNotLearned(t, tc, testDatabase, hashBucket)

	frame := tc.frame(entering, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: hashBucket, Key: "doc",
		Data: docOfEncodedSize(between),
	})
	if frame.Opcode != transport.OpError {
		t.Fatalf("a %d-byte put into a hash bucket was accepted: %s", between, frame.Payload)
	}
	message := wireRefusal(t, frame)
	if !strings.Contains(message, "storage.max_document_bytes") || !strings.Contains(message, "belongs in a blob bucket") {
		t.Errorf("a hash-bucket put over the document limit got %.300q, want the document-limit refusal and its advice", message)
	}
}

// TestBlobBucketPutWhileItsCreateIsInFlightIsHeldToTheBlobCap is the
// deterministic half of BUG-0582 (the live not-learned half is
// TestBlobBucketPutOnANodeThatHasNotLearnedTheBucketIsHeldToTheBlobCap):
// peekBucket reports a bucket whose create has not committed as held with id
// 0 and no type, so the door cannot tell a blob bucket from any other. With
// the cap raised to its ceiling the door must apply the ceiling (the cap),
// not the document limit, and must never tell the writer that a large value
// belongs in a blob bucket. Before the fix a 26 MiB put was refused with the
// document limit and that advice.
func TestBlobBucketPutWhileItsCreateIsInFlightIsHeldToTheBlobCap(t *testing.T) {
	s := newTestServer(t)
	s.config.Storage.MaxBlobBytes = config.MaxBlobBytesCeiling
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	db := s.serverDataAccessor().getDB("app")
	claimForTest(t, db, "files", BucketTypeBlob)

	put := func(size int) error {
		_, err := s.refuseOversizedWrite(transport.NewFrame(transport.OpPut, mustMarshalBSON(t, InsertRequest{
			Database: "app", Bucket: "files", Key: "doc", Data: docOfEncodedSize(size),
		})))
		return err
	}
	const between = 26 << 20
	if err := put(between); err != nil {
		t.Fatalf("a %d-byte put into a blob bucket whose create is in flight was refused: %v", between, err)
	}
	if err := put(config.MaxBlobBytesCeiling + 1); !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("a put over the raised blob cap into a bucket whose create is in flight = %v, want ErrBlobTooLarge", err)
	}
}
