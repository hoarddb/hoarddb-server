package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// standaloneWrite is one acknowledged write on a standalone node and the
// read that proves it survived a power loss. setup runs before the store is
// synced, so only write's own commits are at risk.
type standaloneWrite struct {
	name    string
	setup   func(t *testing.T, s *Server)
	write   func(t *testing.T, s *Server)
	survive func(t *testing.T, s *Server)
}

func createDurabilityBucket(t *testing.T, s *Server, bucket string, btype BucketType) {
	t.Helper()
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: bucket, Type: btype}); err != nil || !resp.Success {
		t.Fatalf("create %s: %+v, %v", bucket, resp, err)
	}
}

// foundDocument fails unless key is stored in bucket, and returns it decoded.
func foundDocument(t *testing.T, s *Server, bucket, key string) bson.M {
	t.Helper()
	found, err := s.Find(FindRequest{Database: "app", Bucket: bucket, Key: key})
	if err != nil || !found.Found {
		t.Fatalf("after a power loss %s/%s is gone (%+v, %v): the node acknowledged a write it had not synced", bucket, key, found, err)
	}
	var doc bson.M
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decoding %s/%s: %v", bucket, key, err)
	}
	return doc
}

func standaloneWrites() []standaloneWrite {
	return []standaloneWrite{
		{
			name:  "insert",
			setup: func(t *testing.T, s *Server) { createDurabilityBucket(t, s, "users", BucketTypeHash) },
			write: func(t *testing.T, s *Server) {
				mustInsert(t, s, "app", "users", "ada", map[string]interface{}{"name": "Ada"})
			},
			survive: func(t *testing.T, s *Server) { foundDocument(t, s, "users", "ada") },
		},
		{
			name:  "batch insert",
			setup: func(t *testing.T, s *Server) { createDurabilityBucket(t, s, "users", BucketTypeBTree) },
			write: func(t *testing.T, s *Server) {
				resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "users", Data: []map[string]interface{}{
					{"_id_": "a"}, {"_id_": "b"}, {"_id_": "c"},
				}})
				if err != nil || !resp.Success {
					t.Fatalf("batch insert: %+v, %v", resp, err)
				}
			},
			survive: func(t *testing.T, s *Server) {
				for _, key := range []string{"a", "b", "c"} {
					foundDocument(t, s, "users", key)
				}
			},
		},
		{
			name: "update",
			setup: func(t *testing.T, s *Server) {
				createDurabilityBucket(t, s, "users", BucketTypeHash)
				mustInsert(t, s, "app", "users", "ada", map[string]interface{}{"visits": int32(1)})
			},
			write: func(t *testing.T, s *Server) {
				resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "users", Key: "ada", Set: map[string]interface{}{"visits": int32(2)}})
				if err != nil || !resp.Success {
					t.Fatalf("update: %+v, %v", resp, err)
				}
			},
			survive: func(t *testing.T, s *Server) {
				if visits := foundDocument(t, s, "users", "ada")["visits"]; visits != int32(2) {
					t.Fatalf("after a power loss visits = %v, want the acknowledged update's 2", visits)
				}
			},
		},
		{
			name: "delete",
			setup: func(t *testing.T, s *Server) {
				createDurabilityBucket(t, s, "users", BucketTypeHash)
				mustInsert(t, s, "app", "users", "ada", map[string]interface{}{"name": "Ada"})
			},
			write: func(t *testing.T, s *Server) {
				if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "users", Key: "ada"}); err != nil || !resp.Success {
					t.Fatalf("delete: %+v, %v", resp, err)
				}
			},
			survive: func(t *testing.T, s *Server) {
				if found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "ada"}); err != nil || found.Found {
					t.Fatalf("after a power loss the acknowledged delete of ada is undone: %+v, %v", found, err)
				}
			},
		},
		{
			name: "push and pop",
			setup: func(t *testing.T, s *Server) {
				createDurabilityBucket(t, s, "jobs", BucketTypeFIFO)
				for _, id := range []string{"first", "second"} {
					if resp, err := s.Push(PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"_id_": id}}); err != nil || !resp.Success {
						t.Fatalf("push %s: %+v, %v", id, resp, err)
					}
				}
			},
			write: func(t *testing.T, s *Server) {
				if resp, err := s.Pop(PopRequest{Database: "app", Bucket: "jobs"}); err != nil || !resp.Found {
					t.Fatalf("pop: %+v, %v", resp, err)
				}
				if resp, err := s.Push(PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"_id_": "third"}}); err != nil || !resp.Success {
					t.Fatalf("push third: %+v, %v", resp, err)
				}
			},
			survive: func(t *testing.T, s *Server) {
				var order []string
				for {
					resp, err := s.Pop(PopRequest{Database: "app", Bucket: "jobs"})
					if err != nil {
						t.Fatalf("pop after the power loss: %v", err)
					}
					if !resp.Found {
						break
					}
					order = append(order, bson.Raw(resp.Items[0]).Lookup("_id_").StringValue())
				}
				if len(order) != 2 || order[0] != "second" || order[1] != "third" {
					t.Fatalf("after a power loss the queue holds %v, want [second third]: an acknowledged pop or push was not synced", order)
				}
			},
		},
		{
			name: "store field push",
			setup: func(t *testing.T, s *Server) {
				if resp := createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Comments", Engine: "fifoStore"}); !resp.Success {
					t.Fatalf("creating Post: %s", resp.Message)
				}
				mustInsert(t, s, "app", "Post", "p1", map[string]interface{}{"title": "p1"})
			},
			write: func(t *testing.T, s *Server) {
				fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{
					Database: "app", Bucket: "Post", Key: "p1", Field: "Comments", Data: map[string]interface{}{"text": "hi"},
				}))
			},
			survive: func(t *testing.T, s *Server) {
				if n := fieldLen(t, s, "p1", "Comments"); n != 1 {
					t.Fatalf("after a power loss p1.Comments holds %d elements, want the acknowledged push's 1", n)
				}
			},
		},
		{
			// The path a driver takes: a PUT frame through the handler.
			name:  "insert from a client frame",
			setup: func(t *testing.T, s *Server) { createDurabilityBucket(t, s, "users", BucketTypeHash) },
			write: func(t *testing.T, s *Server) {
				handlerSend(t, NewHandler(s), transport.OpPut, InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: map[string]interface{}{"name": "Ada"}})
			},
			survive: func(t *testing.T, s *Server) { foundDocument(t, s, "users", "ada") },
		},
		{
			name:  "create database",
			setup: func(t *testing.T, s *Server) {},
			write: func(t *testing.T, s *Server) {
				if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
					t.Fatalf("create database: %+v, %v", resp, err)
				}
			},
			survive: func(t *testing.T, s *Server) {
				if s.serverDataAccessor().getDB("app") == nil {
					t.Fatal("after a power loss the acknowledged create of database app is gone")
				}
			},
		},
		{
			name:  "create bucket",
			setup: func(t *testing.T, s *Server) {},
			write: func(t *testing.T, s *Server) { createDurabilityBucket(t, s, "users", BucketTypeHash) },
			survive: func(t *testing.T, s *Server) {
				if _, err := s.resolveBucket("app", "users"); err != nil {
					t.Fatalf("after a power loss the acknowledged create of app.users is gone: %v", err)
				}
			},
		},
	}
}

// TestStandaloneAcknowledgedWriteSurvivesAPowerLoss: a standalone
// node has no write-ahead log, so the store itself must be synced before a
// write is acknowledged (owner ruling 43). On the fault store a power loss
// drops every batch committed without a sync, the model the drop and
// retention tests use.
func TestStandaloneAcknowledgedWriteSurvivesAPowerLoss(t *testing.T) {
	for _, tc := range standaloneWrites() {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			kv := kvfault.New()
			s := newKVFaultServer(dataDir, kv)
			tc.setup(t, s)
			if err := kv.Sync(); err != nil {
				t.Fatalf("sync after setup: %v", err)
			}
			tc.write(t, s)

			kv.PowerLoss()
			restarted := newKVFaultServer(dataDir, kv)
			if err := restarted.LoadCatalog(); err != nil {
				t.Fatalf("LoadCatalog after the power loss: %v", err)
			}
			tc.survive(t, restarted)
		})
	}
}
