package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 9, step 9.6: the question (T-136).

// TestANodeAnswersWhatItsCopyIsUnderTheBucketsFence is T-136: a write held
// on the owner between its log append and its apply; another node asks the
// owner what its copy of the bucket is. The answer does not come while the
// write is held, and once it is released the answer's position is the held
// write's sequence. Guard: answer without the fence — the answer then comes
// at once, at the write before it.
func TestANodeAnswersWhatItsCopyIsUnderTheBucketsFence(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	askerIndex := (ownerIndex + 1) % len(tc.nodes)
	for i := 0; i < 3; i++ {
		putValue(t, tc, ownerIndex, fmt.Sprintf("k%d", i), "v", "all")
	}

	// The owner applies first; its apply of the held write is the one held.
	var applies atomic.Int32
	reached := make(chan uint64, 1)
	release := make(chan struct{})
	setBeforeLocalApplyHook(t, func(_ string, entry ReplEntry) {
		var put InsertRequest
		if entry.Bucket != testBucket || bson.Unmarshal(entry.Payload, &put) != nil || put.Key != "held" {
			return
		}
		if applies.Add(1) == 1 {
			reached <- entry.Seq
			<-release
		}
	})
	written := make(chan *transport.Frame, 1)
	go func() { written <- putValue(t, tc, ownerIndex, "held", "v", "one") }()
	var heldSeq uint64
	select {
	case heldSeq = <-reached:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the held write never reached the owner's apply")
	}

	asker := clusterOf(tc, askerIndex)
	owner := mustNode(t, asker, tc.addrs[ownerIndex])
	type answer struct {
		copies []ReplCopy
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		copies, err := asker.askCopies(ctx, owner, []ReplBucketRef{{Database: testDatabase, Bucket: testBucket}})
		answered <- answer{copies: copies, err: err}
	}()
	select {
	case early := <-answered:
		close(release)
		t.Fatalf("the owner answered while a write of the bucket was between its log append and its apply: %+v, %v", early.copies, early.err)
	case <-time.After(time.Second):
	}
	close(release)
	<-written

	var got answer
	select {
	case got = <-answered:
	case <-time.After(30 * time.Second):
		t.Fatal("the owner never answered once the write was released")
	}
	if got.err != nil {
		t.Fatalf("asking the owner: %v", got.err)
	}
	if len(got.copies) != 1 {
		t.Fatalf("the owner answered %d copies for one bucket asked", len(got.copies))
	}
	copied := got.copies[0]
	if !copied.Holds || copied.Through != heldSeq || copied.Term != InitialEpoch || !copied.Opened {
		t.Errorf("the owner answered %+v; want holds, through %d (the held write's sequence), term %d, opened", copied, heldSeq, InitialEpoch)
	}
}

// TestAnUpgradedCopyCountsAsOpenedOnItsUnmovedOwner is SA-9.4: a copy with no
// lineage — a bucket from before this build, until its first write — counts
// as opened on the ring's owner, unless it is marked moved.
func TestAnUpgradedCopyCountsAsOpenedOnItsUnmovedOwner(t *testing.T) {
	s, _, c, _ := newReplicaOnFaultStore(t)
	key := placementOf("app", "held")
	st := lineageOf(t, s, "app", "held")
	if st.Term != 0 || st.Through != 0 || st.Opened {
		t.Fatalf("test setup: a bucket created on a node that coordinates for itself has a lineage: %+v", st)
	}
	if !c.copyIsOpened(key, st, placementMarks{}) {
		t.Error("the ring owner's copy of a bucket with no lineage and no mark does not count as opened")
	}
	if c.copyIsOpened(key, st, placementMarks{Moved: 2}) {
		t.Error("a copy with no lineage that is marked moved counts as opened")
	}
}

// TestAQuestionAcrossEpochsIsRefused: a question is answered only between two
// nodes on one membership epoch, because the marks and the ownership in its
// answer are about one ring (step 9.6). Asked on another epoch, the node
// refuses it as it refuses a push that changes state, and answers no copy.
// Guard: no epoch check for a question.
func TestAQuestionAcrossEpochsIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	asked := clusterOf(tc, 0)
	for _, epoch := range []uint64{asked.Epoch() + 1, asked.Epoch() - 1} {
		push, err := bson.Marshal(ReplPushRequest{Token: asked.localToken(), FromNode: tc.addrs[1], Epoch: epoch,
			Handoff: &ReplHandoff{Ask: []ReplBucketRef{{Database: testDatabase, Bucket: testBucket}}}})
		if err != nil {
			t.Fatalf("encoding the question: %v", err)
		}
		frame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), asked.s)
		if err != nil {
			t.Fatalf("asking: %v", err)
		}
		var answer ReplPushResponse
		if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
			t.Fatalf("decoding the answer: %v", err)
		}
		if answer.Success || answer.Refusal != refusalEpoch || len(answer.Copies) != 0 {
			t.Errorf("a question on epoch %d to a node on epoch %d was answered success=%v refusal=%q with %d copies; want an epoch refusal and no copy",
				epoch, asked.Epoch(), answer.Success, answer.Refusal, len(answer.Copies))
		}
	}
}
