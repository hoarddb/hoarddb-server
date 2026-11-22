package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.8, the review's §6 item 8: schema changes sent during a join, with
// the join freeze on and off, lose nothing.

// schemaAnswer is one request of the test retried until it is answered.
type schemaAnswer struct {
	label string
	// acknowledged is a success, or "already exists" for a create of a name
	// that exists.
	acknowledged bool
	// refusals is how many answers before the last were refused or failed.
	refusals int
	last     string
}

// sendUntilAnswered sends request through client until it is answered as a
// success or "already exists", or deadline passes: the join freeze refuses a
// schema change with retry, and an owner taking a bucket over refuses a write
// or a drop with retry.
func sendUntilAnswered(client *transport.DriverClient, label string, op transport.Opcode, request any, deadline time.Time) schemaAnswer {
	answer := schemaAnswer{label: label}
	payload, err := bson.Marshal(request)
	if err != nil {
		answer.last = fmt.Sprintf("encoding: %v", err)
		return answer
	}
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		frame, err := client.SendRequest(ctx, op, payload)
		cancel()
		switch {
		case err != nil:
			answer.last = err.Error()
		case frame.Opcode == transport.OpError:
			answer.last = string(frame.Payload)
			if op != transport.OpDeleteBucket && strings.Contains(answer.last, "already exists") {
				answer.acknowledged = true
				return answer
			}
		default:
			var reply struct {
				Success bool   `bson:"success"`
				Message string `bson:"message"`
			}
			if bson.Unmarshal(frame.Payload, &reply) == nil && reply.Success {
				answer.acknowledged = true
				answer.last = reply.Message
				return answer
			}
			answer.last = string(frame.Payload)
		}
		answer.refusals++
		time.Sleep(100 * time.Millisecond)
	}
	return answer
}

// TestSchemaChangesDuringAJoinLoseNothing: three nodes at factor 3, buckets of
// twenty documents each, some owned by the joiner after the join and some by
// members; a fourth node joins, and through the joiner and through a member
// at once come `create database`, `create bucket` of existing names, `drop
// bucket`, and puts to the kept buckets, each retried until it is answered.
// With the join freeze on, the schema changes are refused with retry until the
// freeze lifts; with it off (the second line), they meet the take-over
// directly. Afterwards, the hand-off settled: every holder of a kept bucket
// counts its twenty documents plus every put answered a success, each once;
// an acknowledged drop left its bucket on no node; and the created database
// is on every node.
func TestSchemaChangesDuringAJoinLoseNothing(t *testing.T) {
	t.Run("with the join freeze on", func(t *testing.T) {
		schemaChangesDuringAJoin(t, true)
	})
	t.Run("with the join freeze off", func(t *testing.T) {
		switchJoinFreezeOff(t)
		schemaChangesDuringAJoin(t, false)
	})
}

func schemaChangesDuringAJoin(t *testing.T, freezeOn bool) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ports := reserveNodePorts(t)
	joined := ringWith(t, 3, append(append([]string(nil), tc.addrs...), ports.ringAddr)...)
	toTheJoiner := func(name string) bool { return ownerOn(joined, name) == ports.ringAddr }
	toAMember := func(name string) bool { return ownerOn(joined, name) != ports.ringAddr }
	kept := []string{bucketNamed(t, "keptj", toTheJoiner), bucketNamed(t, "keptm", toAMember)}
	doomed := []string{bucketNamed(t, "doomj", toTheJoiner), bucketNamed(t, "doomm", toAMember)}
	const before = 20
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range append(append([]string(nil), kept...), doomed...) {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		for i := 0; i < before; i++ {
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("before%02d", i),
				Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		}
	}

	via := clusterOf(tc, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	tc.bringMembershipTo(t, via)
	if err := joiner.server.core.cluster.reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing the joiner to the join: %v", err)
	}

	// Through the joiner and through a member, at once: every schema change
	// of the review's list, and five puts to each kept bucket.
	deadline := time.Now().Add(120 * time.Second)
	type request struct {
		label   string
		op      transport.Opcode
		payload any
		schema  bool
	}
	routes := map[string][]request{}
	for index, route := range []string{"joiner", "member"} {
		routes[route] = []request{
			{label: route + ": create database " + testDatabase + "2", op: transport.OpCreateDB, payload: CreateDatabaseRequest{Name: testDatabase + "2"}, schema: true},
			{label: route + ": create bucket " + kept[index], op: transport.OpCreateBucket, payload: CreateBucketRequest{Database: testDatabase, Name: kept[index], Type: BucketTypeHash}, schema: true},
			{label: route + ": drop bucket " + doomed[index], op: transport.OpDeleteBucket, payload: DeleteBucketRequest{Database: testDatabase, Name: doomed[index]}, schema: true},
		}
		for _, bucket := range kept {
			for i := 0; i < 5; i++ {
				routes[route] = append(routes[route], request{label: fmt.Sprintf("%s: put %s/during-%s-%d", route, bucket, route, i), op: transport.OpPut,
					payload: InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("during-%s-%d", route, i), Data: map[string]interface{}{"n": i}}})
			}
		}
	}
	clients := map[string]*transport.DriverClient{"joiner": joiner.client, "member": tc.dial(1)}
	defer func() { _ = clients["member"].Close() }()
	answers := map[string][]schemaAnswer{}
	var answersMu sync.Mutex
	var wg sync.WaitGroup
	for route, requests := range routes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, sent := range requests {
				answer := sendUntilAnswered(clients[route], sent.label, sent.op, sent.payload, deadline)
				answersMu.Lock()
				answers[route] = append(answers[route], answer)
				answersMu.Unlock()
			}
		}()
	}
	wg.Wait()
	refusedSchema := 0
	acknowledgedPuts := map[string]int{}
	droppedAcknowledged := map[string]bool{}
	for route, requests := range routes {
		for index, sent := range requests {
			answer := answers[route][index]
			if !answer.acknowledged {
				t.Errorf("%s was never answered within the deadline; the last answer: %s", sent.label, answer.last)
				continue
			}
			if sent.schema {
				refusedSchema += answer.refusals
			}
			switch request := sent.payload.(type) {
			case InsertRequest:
				acknowledgedPuts[request.Bucket]++
			case DeleteBucketRequest:
				droppedAcknowledged[request.Name] = true
			}
		}
	}
	if freezeOn && refusedSchema == 0 {
		t.Error("setup: no schema change was refused during the join; the freeze was not met")
	}
	t.Logf("schema changes refused before they were answered: %d; puts answered by bucket: %v", refusedSchema, acknowledgedPuts)

	// The hand-off settles, and every stream catches up.
	byID := map[string]*Server{joiner.addr: joiner.server.core}
	for index, node := range tc.nodes {
		byID[tc.addrs[index]] = node.server.core
	}
	if !holdsWithin(90*time.Second, func() bool { return handOffUnsettled(byID, testDatabase, kept) == "" }) {
		t.Fatalf("the hand-off did not settle within 90 s: %s", handOffUnsettled(byID, testDatabase, kept))
	}
	catchUpCtx, cancelCatchUp := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelCatchUp()
	for _, s := range byID {
		for _, member := range s.cluster.otherMembers() {
			if err := catchUpPeer(catchUpCtx, s.cluster.peerFor(member)); err != nil {
				t.Fatalf("the stream from %s to %s did not catch up: %v", s.cluster.self.ID, member.ID, err)
			}
		}
	}

	for _, bucket := range kept {
		set, err := via.Replicas(placementOf(testDatabase, bucket))
		if err != nil {
			t.Fatalf("placing %s: %v", bucket, err)
		}
		want := before + acknowledgedPuts[bucket]
		for _, member := range set {
			count, err := byID[member.ID].Count(CountRequest{Database: testDatabase, Bucket: bucket})
			if err != nil || count.Count != int64(want) {
				t.Errorf("%s holds %v documents of %s (%v); the acknowledged history is %d", member.ID, count, bucket, err, want)
			}
		}
	}
	for _, bucket := range doomed {
		if !droppedAcknowledged[bucket] {
			continue
		}
		for id, s := range byID {
			if s.bucketStoreOpen(testDatabase, bucket) {
				t.Errorf("%s still holds %s, whose drop was acknowledged", id, bucket)
			}
		}
	}
	for id, s := range byID {
		listed, err := s.ListDatabases(ListDatabasesRequest{})
		if err != nil {
			t.Fatalf("listing databases on %s: %v", id, err)
		}
		found := false
		for _, database := range listed.Databases {
			found = found || database.Name == testDatabase+"2"
		}
		if !found {
			t.Errorf("%s does not list database %s2, whose create was acknowledged", id, testDatabase)
		}
	}
}
