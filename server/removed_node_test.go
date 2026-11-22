package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6, step 6.5b: a node that was removed stops accepting writes (A-6.3;
// review F-5).

// TestARemovedNodeRefusesWrites: a node removed by the others while a client
// writes to it at `one`. From its first refused exchange it refuses every
// write, saying it was removed and which member to connect to; every write it
// answered with a success and no member holds is counted by its stranded
// counter; and it does not rewrite membership.json with a member list that
// excludes it. Guard: no fence — the removed node keeps answering writes with
// a success, and they are on no member.
func TestARemovedNodeRefusesWrites(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const removedIndex = 2
	removed := tc.nodes[removedIndex].server.core.cluster
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := ""
	for i := 0; i < 1000 && bucket == ""; i++ {
		name := fmt.Sprintf("removed%03d", i)
		if owner, err := removed.Owner(placementOf(testDatabase, name)); err == nil && owner.ID == removed.self.ID {
			bucket = name
		}
	}
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	epochBefore := removed.Epoch()

	type answered struct {
		key       string
		succeeded bool
	}
	var (
		mu      sync.Mutex
		answers []answered
	)
	stop := make(chan struct{})
	client := tc.dial(removedIndex)
	defer func() { _ = client.Close() }()
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			key := fmt.Sprintf("w-%04d", n)
			request := InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": n}, WriteConcern: "one"}
			succeeded := sendAndReadSuccess(client, transport.OpPut, request)
			mu.Lock()
			answers = append(answers, answered{key: key, succeeded: succeeded})
			mu.Unlock()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := tc.nodes[0].server.core.cluster.proposeRemoval(ctx, removed.self.ID); err != nil {
		close(stop)
		writer.Wait()
		t.Fatalf("removing node %d: %v", removedIndex+1, err)
	}
	time.Sleep(3 * time.Second)
	close(stop)
	writer.Wait()

	// Once a write was refused, none after it succeeded.
	firstRefusal := -1
	for i, answer := range answers {
		if !answer.succeeded && firstRefusal < 0 {
			firstRefusal = i
		}
		if answer.succeeded && firstRefusal >= 0 {
			t.Errorf("write %s was answered with a success after write %s had been refused", answer.key, answers[firstRefusal].key)
			break
		}
	}
	if firstRefusal < 0 {
		t.Fatalf("the removed node answered all %d writes with a success, three seconds after its removal", len(answers))
	}

	// What a client is told now.
	frame := tc.frame(removedIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": -1}, WriteConcern: "one"})
	if frame.Opcode != transport.OpError {
		t.Errorf("a write to the removed node after its removal was answered %s", frame.Opcode)
	} else if text := string(frame.Payload); !strings.Contains(text, "removed from the cluster") || !strings.Contains(text, "connect to") {
		t.Errorf("the removed node's refusal does not say it was removed and where to connect: %s", text)
	}

	// Every success that no member holds is counted as stranded.
	lost := 0
	succeeded := 0
	for _, answer := range answers {
		if !answer.succeeded {
			continue
		}
		succeeded++
		heldByAMember := false
		for index := 0; index < removedIndex; index++ {
			if holdsKeyLocally(tc.nodes[index], bucket, answer.key) {
				heldByAMember = true
			}
		}
		if !heldByAMember {
			lost++
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for int(removed.stranded.total.Load()) < lost && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := int(removed.stranded.total.Load()); got < lost || got > succeeded {
		t.Errorf("the removed node counts %d stranded entries; %d of its %d successes are on no member", got, lost, succeeded)
	}
	t.Logf("%d writes, %d succeeded, %d of those on no member, %d counted stranded", len(answers), succeeded, lost, removed.stranded.total.Load())

	if epoch, members, found, err := loadMembership(removed.membershipPath()); err != nil || !found || epoch != epochBefore || !nodeListed(members, removed.self.ID) {
		t.Errorf("the removed node's membership.json was rewritten: epoch %d (was %d), members %v, found %v, %v", epoch, epochBefore, memberIDsOf(members), found, err)
	}
}
