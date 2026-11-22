package harness_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// harnessAtRest hands a node fixed keys of its own, as the server's ringAtRest does once the
// node's keys exist, and counts nothing: the asks are pinned by cluster/wire's tests.
type harnessAtRest struct {
	storeKey []byte
	logKeys  storage.WALKeySource
}

func (k harnessAtRest) StoreKey(string) ([]byte, error)              { return k.storeKey, nil }
func (k harnessAtRest) LogKeys(string) (storage.WALKeySource, error) { return k.logKeys, nil }

// perNodeAtRest gives every node id keys of its own, fixed for the life of the cluster, as the
// server's ringAtRest does once a node's keys exist: no two nodes share a key, and a node that
// restarts gets the keys it was first opened with.
func perNodeAtRest() func(id string) wire.AtRestKeys {
	var mu sync.Mutex
	keys := map[string]harnessAtRest{}
	return func(id string) wire.AtRestKeys {
		mu.Lock()
		defer mu.Unlock()
		if k, ok := keys[id]; ok {
			return k
		}
		seed := byte(len(keys) + 1)
		k := harnessAtRest{storeKey: bytes.Repeat([]byte{0x40 + seed}, 32), logKeys: harnessLogKeys{key: bytes.Repeat([]byte{0x70 + seed}, 32)}}
		keys[id] = k
		return k
	}
}

// harnessLogKeys is a one-epoch log key source.
type harnessLogKeys struct{ key []byte }

func (f harnessLogKeys) CurrentEpoch() uint64 { return 1 }

func (f harnessLogKeys) FileKey(epoch uint64, _ int) ([]byte, error) {
	if epoch != 1 {
		return nil, errors.New("no such epoch")
	}
	return f.key, nil
}

// TestARingClusterWithEncryptionOnWritesAndRestartsSealed: [GUARD three ring nodes at RF 2, each
// sealing its store and log under keys of its own, take writes, and two of them restart on their
// sealed directories: every document reads back from its owner, and every node's store and log are
// sealed; red when the node opens its store or log without the keys AtRest hands it]. The keys are
// fixed per node, as the server's loaded keys are; the server's own key creation and refusals are
// pinned in server/. A joiner seeded with encryption on is not covered here: on this host the
// harness's join (TestAMemberWhoseStoreWasMovedAsideIsRefusedAndCommittedStateIsKept) does not
// finish its seeding in five minutes on next itself, with or without encryption; it is owed to the
// VM run (integration review, CHANGELOG cluster sentence).
func TestARingClusterWithEncryptionOnWritesAndRestartsSealed(t *testing.T) {
	const (
		database = "enc"
		bucket   = "Box"
		docs     = 6
	)
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second, AtRest: perNodeAtRest()})
	bug0834Retry(t, "creating the database", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, database, 2)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	bug0834Retry(t, "creating the bucket", func(ctx context.Context) error {
		_, err := c.Node(c.AwaitLeader(30*time.Second)).CreateBucket(ctx, database, bucket, wire.Field{Name: "name", Type: "string"})
		return err
	})
	ids := make([]string, 0, docs)
	for i := range docs {
		id := fmt.Sprintf("doc-%d", i)
		owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), database, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		bug0834Retry(t, "inserting "+id, func(ctx context.Context) error {
			_, err := c.Node(owner).Insert(ctx, database, bucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
			return err
		})
		ids = append(ids, id)
	}

	for _, restarted := range c.IDs()[:2] {
		c.Kill(restarted)
		c.Restart(restarted)
		c.AwaitConverged(60 * time.Second)
	}
	for _, id := range ids {
		owner, _, err := c.Node(c.IDs()[2]).Owner(proofCtx(t), database, bucket, id)
		if err != nil {
			t.Fatal(err)
		}
		bug0834Retry(t, "reading "+id, func(ctx context.Context) error {
			_, err := c.Node(owner).Get(ctx, database, bucket, id)
			return err
		})
	}
	for _, id := range c.IDs() {
		requireSealedRing(t, c.DataDir(id))
	}
}

// TestAJoinerSeededIntoAnEncryptedClusterSealsWhatItCopies: [GUARD a fourth node, opened with
// keys of its own, joins a three-node cluster whose nodes each seal their ring and which holds a
// database at RF 2, pulls the pairs it will hold from the other members' sealed stores, is
// activated, and is restarted on its own sealed directory: it holds documents byte for byte equal
// to their owners' copies before and after the restart, and its store and log are sealed; red when
// the joiner opens its store or log without the keys AtRest hands it]. Records travel decoded over
// the pinned connection and the joiner seals what it copies under its own key (ring encryption at
// rest spec §5.1, §5.2; the CHANGELOG's joiner sentence). The cluster rule (every node the same
// setting) is the operator's; nothing here checks it.
func TestAJoinerSeededIntoAnEncryptedClusterSealsWhatItCopies(t *testing.T) {
	c, ids := t3Cluster(t, 2, 40, harness.Options{AtRest: perNodeAtRest()})
	added := c.AddNode()
	c.ActivateWhileSeedingMoves(added, quorumSeedingStall, quorumSeedingBudget)
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	if held := t3Compare(t, c, added, ids); held == 0 {
		t.Fatalf("SCENARIO NOT REACHED: the joiner %s holds none of the %d documents", added, len(ids))
	}

	c.Kill(added)
	c.Restart(added)
	c.AwaitConverged(60 * time.Second)
	c.AwaitLeases(30 * time.Second)
	// A restarted node holds each pair it serves until it has proved its copies stopped and caught
	// up (its refusal says "this node restarted and is proving..."): wait for that, then compare.
	for _, id := range ids {
		bug0834Retry(t, "reading "+id+" through the restarted joiner", func(ctx context.Context) error {
			_, err := c.Node(added).Get(ctx, proofDB, proofBucket, id)
			return err
		})
	}
	if held := t3Compare(t, c, added, ids); held == 0 {
		t.Fatalf("the joiner %s holds none of the %d documents after its restart on its sealed directory", added, len(ids))
	}
	requireSealedRing(t, c.DataDir(added))
}

// requireSealedRing fails unless the ring directory dir holds a store whose key registry refuses an
// empty key and a log whose segments are sealed.
func requireSealedRing(t *testing.T, dir string) {
	t.Helper()
	store := filepath.Join(dir, "badger")
	if _, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{Dir: store, ReadOnly: true}); !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		t.Errorf("%s's key registry opens with an empty key (%v): the store is not sealed", store, err)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal", "wal_*.log"))
	if err != nil || len(segments) == 0 {
		t.Errorf("no log segment under %s (%v)", filepath.Join(dir, "wal"), err)
		return
	}
	for _, segment := range segments {
		header := make([]byte, 24)
		file, err := os.Open(segment)
		if err != nil {
			t.Error(err)
			continue
		}
		_, readErr := file.ReadAt(header, 0)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			t.Errorf("reading %s: %v %v", segment, readErr, closeErr)
			continue
		}
		if !storage.IsSealedWALSegmentHeader(header) {
			t.Errorf("%s is not sealed", segment)
		}
	}
}
