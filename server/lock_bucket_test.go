package server

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// writerPreambleSites are the writers of catalog-gc spec §5.4 rows 2 to 10,
// and updateMultiple, which UpdateCtx's multi-document path hands its locked
// bucket. The value says whether the function is a writer's entry, which must
// call lockBucket, rather than a helper handed the bucket already locked.
var writerPreambleSites = map[string]bool{
	"service.go:Server.InsertCtx":                     true,
	"service.go:Server.BatchInsertCtx":                true,
	"service.go:Server.PushCtx":                       true,
	"service.go:Server.PopCtx":                        true,
	"service.go:Server.DeleteCtx":                     true,
	"manage.go:Server.UpdateCtx":                      true,
	"manage.go:Server.updateMultiple":                 false,
	"manage.go:Server.BlobWriteCtx":                   true,
	"fieldstore.go:Server.writeFieldLocked":           true,
	"fieldstore_decl.go:Server.DeclareStoreFieldsCtx": true,
}

// preambleCall says what a call inside a writer is, for the preamble test:
// "lockBucket", a bucket lock taken by hand, a resolve, or nothing of note.
func preambleCall(call *ast.CallExpr) string {
	selector, isSelector := call.Fun.(*ast.SelectorExpr)
	if !isSelector {
		return ""
	}
	switch selector.Sel.Name {
	case "lockBucket", "resolveBucket":
		return selector.Sel.Name
	case "Lock", "RLock":
		if lock, isField := selector.X.(*ast.SelectorExpr); isField && lock.Sel.Name == "mu" {
			return "mu." + selector.Sel.Name
		}
	}
	return ""
}

// TestLockBucketIsTheOnlyWriterPreamble is CG-12's structural proof: every
// writer takes its bucket through lockBucket, and none resolves the bucket or
// locks it by hand. Nine copies of the preamble are nine places to forget the
// closed check; CG-13 changes what the check is, in one place.
func TestLockBucketIsTheOnlyWriterPreamble(t *testing.T) {
	fileSet := token.NewFileSet()
	files := make(map[string]bool)
	for site := range writerPreambleSites {
		for i := range site {
			if site[i] == ':' {
				files[site[:i]] = true
				break
			}
		}
	}
	found := make(map[string]bool)
	var offenders []string
	for path := range files {
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		for _, decl := range parsed.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc {
				continue
			}
			site := path + ":" + enclosingFunctionName(parsed, fn.Pos())
			isEntry, listed := writerPreambleSites[site]
			if !listed {
				continue
			}
			found[site] = true
			callsLockBucket := false
			ast.Inspect(fn, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch what := preambleCall(call); what {
				case "lockBucket":
					callsLockBucket = true
				case "":
				default:
					offenders = append(offenders, site+" calls "+what+" ("+fileSet.Position(call.Pos()).String()+")")
				}
				return true
			})
			if isEntry && !callsLockBucket {
				offenders = append(offenders, site+" does not take its bucket through lockBucket")
			}
		}
	}
	for site := range writerPreambleSites {
		if !found[site] {
			offenders = append(offenders, site+" was not found: update writerPreambleSites with its new name")
		}
	}
	sort.Strings(offenders)
	for _, offender := range offenders {
		t.Errorf("a writer has its own preamble: %s", offender)
	}
}

// droppedAfterResolveCase is one writer of the drop-after-resolve test.
type droppedAfterResolveCase struct {
	name       string
	bucketType BucketType
	// prepare puts what the write needs in the bucket before it is dropped.
	prepare func(t *testing.T, s *Server)
	write   func(s *Server) error
}

// writeOutcome folds a writer's reply into the error the test asserts on: a
// reply that says it wrote is an outcome as wrong as no error at all.
func writeOutcome(succeeded bool, err error) error {
	if err == nil && succeeded {
		return errors.New("the write reported success")
	}
	return err
}

func droppedAfterResolveCases() []droppedAfterResolveCase {
	ctx := context.Background()
	const database, bucket = "app", "held"
	insertDocument := func(t *testing.T, s *Server) {
		t.Helper()
		resp, err := s.Insert(InsertRequest{Database: database, Bucket: bucket, Key: "d1", Data: map[string]interface{}{"v": 1}})
		if err != nil || !resp.Success {
			t.Fatalf("seeding d1: %+v, %v", resp, err)
		}
	}
	return []droppedAfterResolveCase{
		{name: "InsertCtx", bucketType: BucketTypeHash, write: func(s *Server) error {
			resp, err := s.InsertCtx(ctx, InsertRequest{Database: database, Bucket: bucket, Key: "late", Data: map[string]interface{}{"v": 2}})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "BatchInsertCtx", bucketType: BucketTypeHash, write: func(s *Server) error {
			resp, err := s.BatchInsertCtx(ctx, BatchInsertRequest{Database: database, Bucket: bucket, Data: []map[string]interface{}{{"_id_": "late", "v": 2}}})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "PushCtx", bucketType: BucketTypeFIFO, write: func(s *Server) error {
			resp, err := s.PushCtx(ctx, PushRequest{Database: database, Bucket: bucket, Data: map[string]interface{}{"_id_": "late"}})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "PopCtx", bucketType: BucketTypeFIFO,
			prepare: func(t *testing.T, s *Server) {
				t.Helper()
				if resp, err := s.Push(PushRequest{Database: database, Bucket: bucket, Data: map[string]interface{}{"_id_": "first"}}); err != nil || !resp.Success {
					t.Fatalf("seeding the queue: %+v, %v", resp, err)
				}
			},
			write: func(s *Server) error {
				resp, err := s.PopCtx(ctx, PopRequest{Database: database, Bucket: bucket})
				return writeOutcome(resp != nil && resp.Found, err)
			}},
		{name: "DeleteCtx", bucketType: BucketTypeHash, prepare: insertDocument, write: func(s *Server) error {
			resp, err := s.DeleteCtx(ctx, DeleteRequest{Database: database, Bucket: bucket, Key: "d1"})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "UpdateCtx", bucketType: BucketTypeHash, prepare: insertDocument, write: func(s *Server) error {
			resp, err := s.UpdateCtx(ctx, UpdateRequest{Database: database, Bucket: bucket, Key: "d1", Set: map[string]interface{}{"v": 3}})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "UpdateCtx multi", bucketType: BucketTypeHash, prepare: insertDocument, write: func(s *Server) error {
			resp, err := s.UpdateCtx(ctx, UpdateRequest{Database: database, Bucket: bucket, Multi: true, Set: map[string]interface{}{"v": 3}})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "BlobWriteCtx", bucketType: BucketTypeBlob, write: func(s *Server) error {
			resp, err := s.BlobWriteCtx(ctx, BlobWriteRequest{Database: database, Bucket: bucket, Key: "late", Data: []byte("payload")})
			return writeOutcome(resp != nil && resp.Success, err)
		}},
		{name: "field write", bucketType: BucketTypeHash,
			prepare: func(t *testing.T, s *Server) {
				t.Helper()
				if resp, err := s.DeclareStoreFieldsCtx(ctx, declareReq(database, bucket, StoreDeclaration{Name: "Tags", Engine: "hashStore"})); err != nil || !resp.Success {
					t.Fatalf("declaring Tags: %+v, %v", resp, err)
				}
				insertDocument(t, s)
			},
			write: func(s *Server) error {
				resp, err := s.FieldPutCtx(ctx, FieldRequest{Database: database, Bucket: bucket, Key: "d1", Field: "Tags", Element: "late", Data: map[string]interface{}{"v": 2}})
				return writeOutcome(resp != nil && resp.Success, err)
			}},
		{name: "DeclareStoreFieldsCtx", bucketType: BucketTypeHash, write: func(s *Server) error {
			resp, err := s.DeclareStoreFieldsCtx(ctx, declareReq(database, bucket, StoreDeclaration{Name: "Tags", Engine: "hashStore"}))
			return writeOutcome(resp != nil && resp.Success, err)
		}},
	}
}

// bucketRange is every key and value of bucket id's range, in key order.
func bucketRange(t *testing.T, kv storage.KV, slot uint16, databaseID, id uint64) [][2][]byte {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.Scope(slot, databaseID, id), Upper: storage.Scope(slot, databaseID, id+1)})
	defer it.Close()
	var pairs [][2][]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		stored, err := it.Value()
		if err != nil {
			t.Fatalf("reading key %x of bucket %d: %v", it.Key(), id, err)
		}
		pairs = append(pairs, [2][]byte{append([]byte(nil), it.Key()...), stored})
	}
	return pairs
}

// TestEachWriterAnswersNotFoundWhenTheBucketIsDroppedAfterResolve parks each
// writer between its resolve and its lock (afterResolveHook), drops the
// bucket, and lets the writer go: it must answer ErrBucketNotFound and leave
// the dropped bucket's range, and its catalog entry, exactly as the drop left
// them. A writer that wrote would be putting keys into a range the reclaimer
// owns, or bringing back the catalog entry the drop deleted.
func TestEachWriterAnswersNotFoundWhenTheBucketIsDroppedAfterResolve(t *testing.T) {
	for _, tc := range droppedAfterResolveCases() {
		t.Run(tc.name, func(t *testing.T) {
			const database, bucket = "app", "held"
			s := newTestServer(t)
			createTestBucket(t, s, database, bucket, tc.bucketType)
			if tc.prepare != nil {
				tc.prepare(t, s)
			}
			held := lookupBucket(s, database, bucket)
			id, slot, databaseID := held.id.Load(), held.slot, held.databaseID

			resolved, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			park := func(_, name string) {
				if name != bucket {
					return
				}
				once.Do(func() {
					close(resolved)
					<-release
				})
			}
			afterResolveHook.Store(&park)
			// The reclaimer would start deleting the dropped range at once;
			// holding it makes the range the drop left the thing compared.
			holdReclaim := func(reclaimStep) error { return errors.New("the test holds the reclaimer") }
			reclaimStepHook.Store(&holdReclaim)
			t.Cleanup(func() {
				afterResolveHook.Store(nil)
				reclaimStepHook.Store(nil)
			})

			outcome := make(chan error, 1)
			go func() { outcome <- tc.write(s) }()
			waitFor(t, resolved, "the writer to resolve the bucket")

			if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket}); err != nil || !resp.Success {
				close(release)
				t.Fatalf("drop: %+v, %v", resp, err)
			}
			droppedRange := bucketRange(t, s.kv, slot, databaseID, id)
			close(release)

			if err := <-outcome; !errors.Is(err, ErrBucketNotFound) {
				t.Errorf("a %s that resolved before the drop and locked after it answered %v, want ErrBucketNotFound", tc.name, err)
			}
			afterRange := bucketRange(t, s.kv, slot, databaseID, id)
			if len(afterRange) != len(droppedRange) {
				t.Fatalf("the dropped bucket's range holds %d keys after the late %s, %d before it", len(afterRange), tc.name, len(droppedRange))
			}
			for i := range afterRange {
				if !bytes.Equal(afterRange[i][0], droppedRange[i][0]) || !bytes.Equal(afterRange[i][1], droppedRange[i][1]) {
					t.Fatalf("the late %s changed key %x of the dropped bucket's range", tc.name, afterRange[i][0])
				}
			}
			if _, found := readStoreKey(t, s.kv, catalogKey(database, bucket)); found {
				t.Errorf("the late %s brought back the catalog entry the drop deleted", tc.name)
			}
		})
	}
}

// writeRecovering runs write and turns a panic into a returned value, so a
// writer that dereferences a nil engine fails the test on an assertion
// instead of taking the test binary down.
func writeRecovering(write func() error) (panicked any, err error) {
	defer func() { panicked = recover() }()
	return nil, write()
}

// TestAWriteToAFailedCreateAnswersNotFound: a create that fails
// after its registry claim takes the claim back (BUG-0257). A writer to the
// name must answer not found, as a reader does (resolveForRead), not
// dereference an engine the create never built.
func TestAWriteToAFailedCreateAnswersNotFound(t *testing.T) {
	ctx := context.Background()
	const database, bucket = "app", "failed"
	s := newTestServer(t)
	createTestBucket(t, s, database, "existing", BucketTypeHash)
	refusedIndex := map[string]interface{}{"indices": []interface{}{map[string]interface{}{"field": 7}}}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeBTree, Options: refusedIndex}); err == nil && resp.Success {
		t.Fatalf("a create with a malformed index declaration was accepted: %+v", resp)
	}
	if left := lookupBucket(s, database, bucket); left != nil {
		t.Fatalf("precondition: the failed create left a registry entry under %q (BUG-0257)", bucket)
	}

	writers := map[string]func() error{
		"InsertCtx": func() error {
			_, err := s.InsertCtx(ctx, InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"v": 1}})
			return err
		},
		"BatchInsertCtx": func() error {
			_, err := s.BatchInsertCtx(ctx, BatchInsertRequest{Database: database, Bucket: bucket, Data: []map[string]interface{}{{"_id_": "k"}}})
			return err
		},
		"PushCtx": func() error {
			_, err := s.PushCtx(ctx, PushRequest{Database: database, Bucket: bucket, Data: map[string]interface{}{"_id_": "k"}})
			return err
		},
		"DeleteCtx": func() error {
			_, err := s.DeleteCtx(ctx, DeleteRequest{Database: database, Bucket: bucket, Key: "k"})
			return err
		},
		"UpdateCtx": func() error {
			_, err := s.UpdateCtx(ctx, UpdateRequest{Database: database, Bucket: bucket, Key: "k", Set: map[string]interface{}{"v": 2}})
			return err
		},
		"DeclareStoreFieldsCtx": func() error {
			_, err := s.DeclareStoreFieldsCtx(ctx, declareReq(database, bucket, StoreDeclaration{Name: "Tags", Engine: "hashStore"}))
			return err
		},
	}
	for name, write := range writers {
		panicked, err := writeRecovering(write)
		if panicked != nil {
			t.Errorf("%s to a bucket whose create failed panicked: %v", name, panicked)
			continue
		}
		if !errors.Is(err, ErrBucketNotFound) {
			t.Errorf("%s to a bucket whose create failed answered %v, want ErrBucketNotFound", name, err)
		}
	}
}
