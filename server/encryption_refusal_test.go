package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// newRefusalTestServer builds a server whose encryption mode is the one under
// test. A mode that encrypts gets a keystore and a node master; mode=off gets
// neither, because that is what keystore=off actually looks like and the
// plaintext path is half of what these tests are about.
func newRefusalTestServer(t *testing.T, mode string) *Server {
	t.Helper()

	dataDir := t.TempDir()
	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataDir:            dataDir,
			BadgerBlockCacheMB: 64,
			BadgerIndexCacheMB: 64,
		},
		Encryption: config.EncryptionConfig{Mode: mode},
	}

	s := &Server{
		config:      cfg,
		logger:      discardLogger(),
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          sharedTestNodeStore(dataDir),
		authReqSeen: make(map[string]time.Time),
		knownNodes:  make(map[string]*NodeInfo),
	}

	if mode == "off" {
		cfg.Encryption.Keystore = "off"
		return s
	}

	cfg.Encryption.Keystore = "keyfile"
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(i)
	}
	s.keystore = &Keystore{kek: kek, kid: sealed.Kid(kek), mode: mode}
	nmBytes := make([]byte, 32)
	for i := range nmBytes {
		nmBytes[i] = byte(i + 1)
	}
	s.nodeMaster = &NodeMaster{bytes: nmBytes, kid: sealed.Kid(nmBytes)}
	return s
}

// TestModeOnCreatesQueueBuckets: a fifo or lifo bucket lives on the node's
// store, which the mode seals, so neither mode refuses one any more (build doc
// Step 12 item 6). It used to be refused, because its segment store was not
// sealed.
func TestModeOnCreatesQueueBuckets(t *testing.T) {
	for _, mode := range []string{"on", "require"} {
		for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO} {
			t.Run(mode+"/"+string(bucketType), func(t *testing.T) {
				s := newRefusalTestServer(t, mode)

				resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
					Database:  "appdb",
					Name:      "events",
					Type:      bucketType,
					Encrypted: true,
				})
				if err != nil {
					t.Fatalf("createBucketLocked returned a transport error: %v", err)
				}
				if !resp.Success {
					t.Fatalf("mode=%s refused a %s bucket, which the node's store seals: %s", mode, bucketType, resp.Message)
				}
				if strings.Contains(resp.Message, "Stage 2") {
					t.Errorf("a queue bucket's create names the Stage 2 gap it no longer has: %s", resp.Message)
				}
				if bucket := lookupBucket(s, "appdb", "events"); bucket == nil || bucket.queueStore == nil {
					t.Error("the created bucket has no queue store attached")
				}
			})
		}
	}
}

// TestModeOnCreatesBlobBuckets replaces the Stage 2 refusal: a blob bucket's
// large payloads are sealed files (at-rest encryption spec §4.2.1), so a mode
// that encrypts creates one, and the blob store it gets holds the node's key
// source.
func TestModeOnCreatesBlobBuckets(t *testing.T) {
	for _, mode := range []string{"on", "require"} {
		t.Run(mode, func(t *testing.T) {
			s := newRefusalTestServer(t, mode)

			resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
				Database:  "appdb",
				Name:      "uploads",
				Type:      BucketTypeBlob,
				Encrypted: true,
			})
			if err != nil {
				t.Fatalf("createBucketLocked returned a transport error: %v", err)
			}
			if !resp.Success {
				t.Fatalf("mode=%s refused a blob bucket, whose payloads the node seals: %s", mode, resp.Message)
			}
			if strings.Contains(resp.Message, "Stage 2") {
				t.Errorf("a blob bucket's create names the Stage 2 gap it no longer has: %s", resp.Message)
			}
			if bucket := lookupBucket(s, "appdb", "uploads"); bucket == nil || bucket.blobStore == nil {
				t.Error("the created bucket has no blob store attached")
			}
		})
	}
}

// TestModeOffCreatesQueueAndBlobBucketsUnchanged is what stops the refusal
// leaking into the plaintext path. With encryption off there is nothing to
// claim and nothing to refuse: all three store types build as they always have.
func TestModeOffCreatesQueueAndBlobBucketsUnchanged(t *testing.T) {
	cases := []struct {
		bucketType BucketType
		name       string
	}{
		{BucketTypeFIFO, "queue_fifo"},
		{BucketTypeLIFO, "queue_lifo"},
		{BucketTypeBlob, "uploads"},
	}

	for _, tc := range cases {
		t.Run(string(tc.bucketType), func(t *testing.T) {
			s := newRefusalTestServer(t, "off")

			resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
				Database: "appdb",
				Name:     tc.name,
				Type:     tc.bucketType,
			})
			if err != nil {
				t.Fatalf("createBucketLocked: %v", err)
			}
			if !resp.Success {
				t.Fatalf("mode=off refused a %s bucket: %s", tc.bucketType, resp.Message)
			}
			if strings.Contains(resp.Message, "Stage 2") {
				t.Errorf("the Stage 2 refusal leaked into the plaintext path: %s", resp.Message)
			}

			bucket := lookupBucket(s, "appdb", tc.name)
			if bucket == nil {
				t.Fatalf("bucket %q was reported created but is not registered", tc.name)
			}
			switch tc.bucketType {
			case BucketTypeFIFO, BucketTypeLIFO:
				if bucket.queueStore == nil {
					t.Error("no queue store was attached")
				}
			case BucketTypeBlob:
				if bucket.blobStore == nil {
					t.Error("no blob store was opened")
				}
			}
		})
	}
}

// lookupBucket returns the in-memory bucket, or nil when the database or the
// bucket was never registered. A refusal may happen before either exists.
func lookupBucket(s *Server, database, bucket string) *inMemoryBucket {
	handle, err := s.resolveBucket(database, bucket)
	if err != nil {
		return nil
	}
	return handle
}
