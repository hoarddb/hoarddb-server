package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6 of the replication stream, step 6.1: the join under load
// (spec §19.2 item 1, §19.12 probe 1).

// loadWrite is one write a join-window writer made and what it was told.
type loadWrite struct {
	bucket  string
	key     string
	queue   bool
	concern string
	// acknowledged is a success answer; anything else — an error frame, a
	// response that says it failed, a transport failure — is not.
	acknowledged bool
}

// joinWindowWriter writes through one node, alternating a put to a hash bucket
// and a push to a FIFO bucket, each carrying a key no other write uses, 2 ms
// apart, until stop is closed.
func joinWindowWriter(client *transport.DriverClient, name, concern string, hashBuckets, queueBuckets []string, stop <-chan struct{}) []loadWrite {
	var writes []loadWrite
	for n := 0; ; n++ {
		select {
		case <-stop:
			return writes
		case <-time.After(2 * time.Millisecond):
		}
		key := fmt.Sprintf("%s-%d", name, n)
		write := loadWrite{key: key, concern: concern, queue: n%2 == 1}
		var op transport.Opcode
		var request any
		if write.queue {
			write.bucket = queueBuckets[(n/2)%len(queueBuckets)]
			op, request = transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: write.bucket,
				Data: map[string]interface{}{"k": key}, WriteConcern: concern}
		} else {
			write.bucket = hashBuckets[(n/2)%len(hashBuckets)]
			op, request = transport.OpPut, InsertRequest{Database: testDatabase, Bucket: write.bucket, Key: key,
				Data: map[string]interface{}{"k": key}, WriteConcern: concern}
		}
		write.acknowledged = sendAndReadSuccess(client, op, request)
		writes = append(writes, write)
	}
}

// sendAndReadSuccess sends one write and reports whether it was answered as a
// success.
func sendAndReadSuccess(client *transport.DriverClient, op transport.Opcode, request any) bool {
	payload, err := bson.Marshal(request)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := client.SendRequest(ctx, op, payload)
	if err != nil || frame.Opcode == transport.OpError {
		return false
	}
	var answer struct {
		Success bool `bson:"success"`
	}
	return bson.Unmarshal(frame.Payload, &answer) == nil && answer.Success
}

// queueKeys drains a FIFO bucket on one node's own store and counts each
// element's key.
func queueKeys(t *testing.T, s *Server, bucket string) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	for {
		popped, err := s.Pop(PopRequest{Database: testDatabase, Bucket: bucket})
		if err != nil {
			t.Fatalf("draining %s on its holder: %v", bucket, err)
		}
		if !popped.Found {
			return counts
		}
		var element struct {
			K string `bson:"k"`
		}
		if err := bson.Unmarshal(popped.Items[0], &element); err != nil {
			t.Fatalf("decoding an element of %s: %v", bucket, err)
		}
		counts[element.K]++
	}
}

// waitForEveryNode polls until settled holds for every server, or fails the
// test naming what was not.
func waitForEveryNode(t *testing.T, servers []*Server, wait time.Duration, what string, settled func(*Server) string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		var behind []string
		for _, s := range servers {
			if reason := settled(s); reason != "" {
				behind = append(behind, reason)
			}
		}
		if len(behind) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s, %s: %s", wait, what, strings.Join(behind, "; "))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestWritesDuringAJoinAreOnEveryHolderExactlyOnce is T-97, spec
// §19.12 probe 1 as a test: three nodes at factor 3, six hash and six FIFO
// buckets, two writers per node — one at `all`, one at the default concern —
// a fourth node joined 300 ms in, with no manual reconcile, six seconds of
// writes. Once every node is on the same epoch, every bucket's owner has taken
// it over, every set member follows its owner's term and every stream has
// caught up: every write answered with a success, at any concern, is on every
// member of its bucket's final set exactly once; no other write is on any
// holder twice.
//
// Widened by replication dispatch 10b, step 10b.8 (spec §20.16): the strict
// leg was an `all` write answered copies_confirmed equal to its factor (A-6.1,
// D-2), because a ceded owner stranded what it had not delivered. From 10b a
// ceded owner finishes delivering its term and a new owner takes the bucket
// over from its most complete copy, so a write acknowledged at any concern is
// on every holder that follows the owner.
func TestWritesDuringAJoinAreOnEveryHolderExactlyOnce(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	var hashBuckets, queueBuckets []string
	for i := 0; i < 6; i++ {
		hashBuckets = append(hashBuckets, fmt.Sprintf("h%02d", i))
		queueBuckets = append(queueBuckets, fmt.Sprintf("q%02d", i))
	}
	for _, bucket := range hashBuckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	}
	for _, bucket := range queueBuckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	}
	everyBucket := append(append([]string(nil), hashBuckets...), queueBuckets...)
	for _, bucket := range everyBucket {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	ownerBefore := make(map[string]string, len(everyBucket))
	for _, bucket := range everyBucket {
		owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
		if err != nil {
			t.Fatalf("owner of %s: %v", bucket, err)
		}
		ownerBefore[bucket] = owner.ID
	}

	type writer struct {
		client  *transport.DriverClient
		name    string
		concern string
	}
	var writers []writer
	for index := range tc.nodes {
		for _, concern := range []string{"all", ""} {
			label := concern
			if label == "" {
				label = "default"
			}
			writers = append(writers, writer{client: tc.dial(index), name: fmt.Sprintf("n%d-%s", index+1, label), concern: concern})
		}
	}
	defer func() {
		for _, w := range writers {
			_ = w.client.Close()
		}
	}()

	stop := make(chan struct{})
	results := make([][]loadWrite, len(writers))
	var wg sync.WaitGroup
	for i, w := range writers {
		wg.Add(1)
		go func(i int, w writer) {
			defer wg.Done()
			results[i] = joinWindowWriter(w.client, w.name, w.concern, hashBuckets, queueBuckets, stop)
		}(i, w)
	}

	time.Sleep(300 * time.Millisecond)
	ports := reserveNodePorts(t)
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelJoin()
	if err := tc.nodes[0].server.core.cluster.proposeJoin(joinCtx, ports.ringNode()); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	defer func() { _ = joiner.client.Close() }()

	time.Sleep(6 * time.Second)
	close(stop)
	wg.Wait()

	servers := []*Server{joiner.server.core}
	byID := map[string]*Server{joiner.addr: joiner.server.core}
	for i, node := range tc.nodes {
		servers = append(servers, node.server.core)
		byID[tc.addrs[i]] = node.server.core
	}
	finalEpoch := servers[1].cluster.Epoch()
	waitForEveryNode(t, servers, 60*time.Second, "every node is on the same membership epoch with four members", func(s *Server) string {
		if epoch, members := s.cluster.epochMembers(); epoch < 2 || len(members) != 4 {
			return fmt.Sprintf("%s is at epoch %d with %d members", s.cluster.self.ID, epoch, len(members))
		}
		return ""
	})
	finalEpoch = max(finalEpoch, servers[1].cluster.Epoch())
	finalSets := make(map[string][]string, len(everyBucket))
	for _, bucket := range everyBucket {
		set, err := servers[1].cluster.Replicas(placementOf(testDatabase, bucket))
		if err != nil {
			t.Fatalf("replica set of %s: %v", bucket, err)
		}
		finalSets[bucket] = nodeIDs(set)
	}
	waitForEveryNode(t, servers, 90*time.Second, "every member of each bucket's final set holds it", func(s *Server) string {
		var missing []string
		for _, bucket := range everyBucket {
			for _, id := range finalSets[bucket] {
				if id == s.cluster.self.ID && !s.bucketStoreOpen(testDatabase, bucket) {
					missing = append(missing, bucket)
				}
			}
		}
		if len(missing) > 0 {
			return fmt.Sprintf("%s does not hold %v", s.cluster.self.ID, missing)
		}
		return ""
	})
	// The hand-off settles: every owner has taken its buckets over and every
	// member follows its owner's term, so each stream delivers the new term.
	waitForEveryNode(t, servers[:1], 90*time.Second, "the hand-off settles", func(*Server) string {
		return handOffUnsettled(byID, testDatabase, everyBucket)
	})
	settleCtx, cancelSettle := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelSettle()
	for _, s := range servers {
		for _, member := range s.cluster.otherMembers() {
			if err := catchUpPeer(settleCtx, s.cluster.peerFor(member)); err != nil {
				t.Fatalf("the stream from %s to %s did not catch up: %v", s.cluster.self.ID, member.ID, err)
			}
		}
	}

	// What each holder holds of each bucket, read from its own store.
	queueHeld := make(map[string]map[string]map[string]int) // bucket -> holder -> key -> count
	for _, bucket := range queueBuckets {
		queueHeld[bucket] = make(map[string]map[string]int)
		for id, s := range byID {
			if s.bucketStoreOpen(testDatabase, bucket) {
				queueHeld[bucket][id] = queueKeys(t, s, bucket)
			}
		}
	}

	type shortfall struct{ missing, twice, erroredTwice, acknowledged, errored int }
	report := make(map[string]map[string]*shortfall) // bucket -> holder
	note := func(bucket, holder string) *shortfall {
		if report[bucket] == nil {
			report[bucket] = make(map[string]*shortfall)
		}
		if report[bucket][holder] == nil {
			report[bucket][holder] = &shortfall{}
		}
		return report[bucket][holder]
	}
	failed := false
	for _, writes := range results {
		for _, write := range writes {
			for _, id := range finalSets[write.bucket] {
				s := byID[id]
				held := 0
				if write.queue {
					held = queueHeld[write.bucket][id][write.key]
				} else if found, err := s.Find(FindRequest{Database: testDatabase, Bucket: write.bucket, Key: write.key}); err == nil && found.Found {
					held = 1
				}
				line := note(write.bucket, id)
				if write.acknowledged {
					line.acknowledged++
				} else {
					line.errored++
				}
				switch {
				case write.acknowledged && held == 0:
					line.missing++
					failed = true
				case held > 1 && write.acknowledged:
					line.twice++
					failed = true
				case held > 1:
					line.erroredTwice++
					failed = true
				}
			}
		}
	}
	acknowledged := map[string]int{}
	for _, writes := range results {
		for _, write := range writes {
			if write.acknowledged {
				acknowledged[write.concern]++
			}
		}
	}
	if acknowledged["all"] == 0 || acknowledged[""] == 0 {
		t.Errorf("acknowledged writes by concern %v: the strict leg needs writes at all and at the default concern", acknowledged)
	}
	if !failed {
		return
	}
	buckets := make([]string, 0, len(report))
	for bucket := range report {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	for _, bucket := range buckets {
		for holder, line := range report[bucket] {
			if line.missing+line.twice+line.erroredTwice == 0 {
				continue
			}
			finalOwner := finalSets[bucket][0]
			if holder == joiner.addr {
				holder += " (the joiner)"
			}
			t.Errorf("%s (owner %s before the join, %s at epoch %d): holder %s is missing %d acknowledged writes, holds %d acknowledged writes twice and %d errored writes twice (of %d acknowledged and %d errored writes to the bucket)",
				bucket, ownerBefore[bucket], finalOwner, finalEpoch, holder, line.missing, line.twice, line.erroredTwice, line.acknowledged, line.errored)
		}
	}
}
