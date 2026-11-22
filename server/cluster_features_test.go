package server

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// pathUpdateCluster starts three nodes at factor 1, creates a bucket node 1
// owns holding document "a", and stops node 3 after letting edit change what
// the live nodes believe it advertises. It returns the cluster, the bucket and
// the stopped member's id.
func pathUpdateCluster(t *testing.T, edit func(c *Cluster, stale string)) (*testCluster, string, string) {
	t.Helper()
	tc := newTestCluster(t, 3, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "paths")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash,
		// Whole: the multi-updates below are the E-19 refusal under test, and a spread bucket refuses them first (step 12c).
		Options: map[string]interface{}{"placement": "whole"}})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "a",
		Data: map[string]interface{}{"Address": map[string]interface{}{"City": "Tirana"}, "Visits": int32(1)}})
	stale := stopThirdNodeAndEditWhatIsKnown(t, tc, edit)
	return tc, bucket, stale
}

// TestPathUpdateIsRefusedWhileAMemberLacksTypedFields is OC-1's path half: an
// older member reads $set of "Address.City" as a literal top-level key, so the
// owner refuses a path-keyed update, before logging it, while any member that
// has gossiped does not advertise typed_fields (E-19). An update with no path
// is unaffected.
func TestPathUpdateIsRefusedWhileAMemberLacksTypedFields(t *testing.T) {
	tc, bucket, stale := pathUpdateCluster(t, func(c *Cluster, stale string) {
		c.memberFeatures[stale] = map[string]bool{featureBucketRF: true, featureMemtableMB: true}
	})
	defer tc.stop()

	refused := tc.frame(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "a",
		Set: map[string]interface{}{"Address.City": "Paris"}})
	if refused.Opcode != transport.OpError {
		t.Fatalf("a path update while %s lacks typed_fields was answered %s; want E-19", stale, refused.Opcode)
	}
	var answer constraintRefusalFrame
	if err := bson.Unmarshal(refused.Payload, &answer); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	if transport.ErrorCode(answer.Code) != transport.ErrUnavailable || answer.WriteOutcome != outcomeNotApplied {
		t.Errorf("the refusal = code %#x outcome %q; want UNAVAILABLE and not_applied", answer.Code, answer.WriteOutcome)
	}
	for _, part := range []string{bucket, "a path update", `member "` + stale + `"`, "Nothing was changed.", "Upgrade the cluster"} {
		if !strings.Contains(answer.Message, part) {
			t.Errorf("the refusal %q does not say %q", answer.Message, part)
		}
	}
	document := documentOf(t, tc.nodes[0].server.core, testDatabase, bucket, "a")
	if city := document["Address"].(map[string]interface{})["City"]; city != "Tirana" {
		t.Errorf("the refused update changed Address.City to %v", city)
	}

	tc.ok(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "a", Inc: map[string]int64{"Visits": 1}})

	// A multi-update's filter is re-evaluated by every copy, so an older member
	// would read a path in it literally and change different documents: a path
	// in the filter, at the top or inside $and/$or, is refused the same way.
	for _, filter := range []map[string]interface{}{
		{"Address.City": "Tirana"},
		{"$or": []interface{}{map[string]interface{}{"Visits": int32(9)}, map[string]interface{}{"Address.City": "Tirana"}}},
	} {
		refused := tc.frame(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Multi: true,
			Filter: filter, Inc: map[string]int64{"Visits": 1}})
		if refused.Opcode != transport.OpError || !strings.Contains(string(refused.Payload), "a path update") {
			t.Errorf("a multi-update filtered on %v while %s lacks typed_fields was answered %s; want E-19", filter, stale, refused.Opcode)
		}
	}
	tc.ok(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Multi: true,
		Filter: map[string]interface{}{"Visits": map[string]interface{}{"$gte": int32(1)}}, Inc: map[string]int64{"Visits": 1}})
	if visits := documentOf(t, tc.nodes[0].server.core, testDatabase, bucket, "a")["Visits"]; visits != int64(3) {
		t.Errorf("Visits = %v, want 3: the refused multi-updates changed it, or the plain one did not", visits)
	}
}

// TestUnheardMemberDoesNotBlockTypedFields: a member this node has not heard
// from is not evidence that it lacks the feature, exactly as for bucket_rf.
func TestUnheardMemberDoesNotBlockTypedFields(t *testing.T) {
	tc, bucket, _ := pathUpdateCluster(t, func(c *Cluster, stale string) { delete(c.memberFeatures, stale) })
	defer tc.stop()
	tc.ok(0, transport.OpUpdate, UpdateRequest{Database: testDatabase, Bucket: bucket, Key: "a",
		Set: map[string]interface{}{"Address.City": "Paris"}})
	document := documentOf(t, tc.nodes[0].server.core, testDatabase, bucket, "a")
	if city := document["Address"].(map[string]interface{})["City"]; city != "Paris" {
		t.Errorf("Address.City = %v after a path update with every heard member current, want Paris", city)
	}
}

// featureNoBuildAdvertises is a member feature no build advertises, so every
// member that has gossiped lacks it.
const featureNoBuildAdvertises = "bug_0576_no_build_advertises_this"

// withGatedOpcode gives op a writeOpcodeFeatures entry naming feature for the
// rest of the test.
func withGatedOpcode(t *testing.T, op transport.Opcode, feature string) {
	t.Helper()
	gatedOpcodeHook.Store(&gatedOpcode{op: op, feature: feature})
	t.Cleanup(func() { gatedOpcodeHook.Store(nil) })
}

// TestEveryWriteOpcodeDeclaresAMemberFeatureOrThatEveryBuildHasIt is
// BUG-0576's drift test. An owner refuses a forwarded write while a member
// lacks the feature its writeOpcodeFeatures entry names, so every write
// opcode must have an entry: a feature string, or everyBuildHasIt for one
// every build runs with the same meaning. A write opcode added with no entry
// would reach an older member that runs it differently, and an entry for an
// opcode dispatch does not run, or that is not a write, is a table that has
// rotted. The write set is every opcode dispatch runs that authz.go rules a
// write (ActionWrite, ActionDBAdmin) or that the write path replicates
// (replicationRequired). Every value of the opcode byte is asked.
func TestEveryWriteOpcodeDeclaresAMemberFeatureOrThatEveryBuildHasIt(t *testing.T) {
	writes := 0
	for value := 0; value <= 0xFF; value++ {
		op := transport.Opcode(value)
		rule, ruled := opRules[op]
		ruledAWrite := ruled && (rule.Action == ActionWrite || rule.Action == ActionDBAdmin)
		write := implementsOpcode(op) && (ruledAWrite || replicationRequired(op))
		_, declared := writeOpcodeFeatures[op]
		switch {
		case write && !declared:
			t.Errorf("write opcode 0x%02X (%s) has no writeOpcodeFeatures entry: declare the member feature it needs, "+
				"or everyBuildHasIt if every build that can join the cluster runs it with the same meaning", value, op)
		case declared && !implementsOpcode(op):
			t.Errorf("writeOpcodeFeatures has an entry for opcode 0x%02X (%s), which dispatch does not run", value, op)
		case declared && !write:
			t.Errorf("writeOpcodeFeatures has an entry for opcode 0x%02X (%s), which is not a write", value, op)
		}
		if write {
			writes++
		}
	}
	if writes == 0 {
		t.Fatal("the write set is empty")
	}
}

// TestDeleteDatabaseStaysEveryBuildHasItWhileRelogDropIsExempt pins the one
// write that stays outside the member-feature gate:
// generation.go:Cluster.relogDrop logs a DELETE_DB without asking
// memberFeaturePrecheck, because refusing it would turn learning the dropped
// generations into a failure and block convergence. The exemption is safe only
// while DELETE_DB's row is everyBuildHasIt, so gating DELETE_DB first needs
// relogDrop to be gated or the exemption re-argued.
func TestDeleteDatabaseStaysEveryBuildHasItWhileRelogDropIsExempt(t *testing.T) {
	if feature := writeOpcodeFeatures[transport.OpDeleteDB]; feature != everyBuildHasIt {
		t.Fatalf("writeOpcodeFeatures[DELETE_DB] = %q, not everyBuildHasIt: relogDrop logs DELETE_DB without asking "+
			"memberFeaturePrecheck, so gating DELETE_DB first needs relogDrop to be gated or the exemption re-argued", feature)
	}
}

// TestAnOwnerRefusesAGatedOpcodeWhileAMemberLacksItsFeature is BUG-0576's
// reproduction. An older member runs an opcode it has the number of with the
// meaning it had when it was built; a newer owner that logs and ships a write
// whose meaning has changed leaves the copies holding different data. The
// owner must refuse it before its log while a member that gossiped does not
// advertise the feature, and must append, apply and ship it once every member
// does.
func TestAnOwnerRefusesAGatedOpcodeWhileAMemberLacksItsFeature(t *testing.T) {
	tc, owner, coordinator := forwardedOpcodeCluster(t)
	ownerCore := tc.nodes[owner].server.core
	ownerCore.cluster.recordMemberFeatures(tc.addrs[coordinator], clusterFeatures())

	t.Run("a member lacks the feature", func(t *testing.T) {
		withGatedOpcode(t, transport.OpPut, featureNoBuildAdvertises)
		logged := ownerCore.wal.LastSequence()

		resp := forwardToOwner(t, tc, owner, coordinator, transport.OpPut,
			InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "gated", Data: map[string]interface{}{"key": "gated"}})

		if now := ownerCore.wal.LastSequence(); now != logged {
			t.Errorf("the owner's log went from %d to %d for a PUT gated on %q, which a member lacks: "+
				"the entry is logged and shipped to a member that would carry it out differently", logged, now, featureNoBuildAdvertises)
		}
		if queuedPast(ownerCore.cluster, logged+1) {
			t.Errorf("the owner queued an entry past sequence %d for a replica while a member lacks the feature", logged)
		}
		if found, _ := tc.readFromNode(owner, "gated"); found {
			t.Error("the owner applied a PUT gated on a feature a member lacks")
		}
		if len(resp.Responses) != 1 || transport.Opcode(resp.Responses[0].Opcode) != transport.OpError {
			t.Fatalf("the owner answered success=%t error=%q with %d responses; want one ERROR refusing the PUT",
				resp.Success, resp.Error, len(resp.Responses))
		}
		var answer constraintRefusalFrame
		if err := bson.Unmarshal(resp.Responses[0].Payload, &answer); err != nil {
			t.Fatalf("decoding the refusal: %v", err)
		}
		if transport.ErrorCode(answer.Code) != transport.ErrUnavailable || answer.WriteOutcome != outcomeNotApplied {
			t.Errorf("the refusal = code %#x outcome %q; want UNAVAILABLE and %q", answer.Code, answer.WriteOutcome, outcomeNotApplied)
		}
		namesAMember := false
		for index, addr := range tc.addrs {
			if index != owner && strings.Contains(answer.Message, `member "`+addr+`"`) {
				namesAMember = true
			}
		}
		if !namesAMember {
			t.Errorf("the refusal %q does not name the member that lacks the feature", answer.Message)
		}
		for _, part := range []string{"PUT", "Nothing was changed.", "Upgrade the cluster"} {
			if !strings.Contains(answer.Message, part) {
				t.Errorf("the refusal %q does not say %q", answer.Message, part)
			}
		}
	})

	t.Run("every member advertises the feature", func(t *testing.T) {
		withGatedOpcode(t, transport.OpPut, featureBucketRF)
		logged := ownerCore.wal.LastSequence()

		resp := forwardToOwner(t, tc, owner, coordinator, transport.OpPut,
			InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "advertised", Data: map[string]interface{}{"key": "advertised"}})

		if !resp.Success || len(resp.Responses) != 1 || transport.Opcode(resp.Responses[0].Opcode) == transport.OpError {
			t.Fatalf("the owner refused a PUT gated on %q, which every member advertises: success=%t error=%q responses=%d",
				featureBucketRF, resp.Success, resp.Error, len(resp.Responses))
		}
		if now := ownerCore.wal.LastSequence(); now != logged+1 {
			t.Errorf("the owner's log went from %d to %d for a gated PUT every member can run; want one entry appended", logged, now)
		}
		deadline := time.Now().Add(10 * time.Second)
		for index := range tc.nodes {
			for {
				if found, _ := tc.readFromNode(index, "advertised"); found {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("node %d (%s) does not hold the gated PUT 10s after its owner applied it: it was not shipped",
						index+1, tc.addrs[index])
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})
}

// wellFormedLeaseID has the 40 hexadecimal characters of a lease id, so a
// lease verb's request passes its shape check and reaches the member gate.
const wellFormedLeaseID = "0123456789abcdef0123456789abcdef01234567"

// memberFeatureRefusalText is what every member-feature refusal of
// memberFeaturePrecheck says, and nothing else does.
const memberFeatureRefusalText = "needs every cluster member to run a hoardDB that supports"

// TestAWriteEveryBuildHasPassesAMemberThatAdvertisesNothing is BUG-0576's
// green path, sent from a client through the real write path so a path that
// skips the gate is noticed. For every write opcode in writeOpcodeFeatures:
// gated on a feature no member advertises, it is refused before any live
// node logs it; marked everyBuildHasIt, as it is, it is not refused for a
// feature while a member has gossiped an empty list; a row that names a real
// feature (queue_claim) is held to the gated half only, since a member that
// advertises nothing is rightly refused it. ALTER_BUCKET is left out: its
// entry names bucket_rf_alter, and alterPrecheck refuses it first, with the
// alter's own words (bucket_rf_alter_refusal_test.go). DEAD_RESTORE is left
// out too: a client's frame is refused before routing whatever its role
// (TestClientDeadRestoreIsRefusedEvenForRoot), so the gate is only reachable
// from a restore, which TestDeadRestoreIsGatedByQueueClaim covers.
func TestAWriteEveryBuildHasPassesAMemberThatAdvertisesNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "doomed"})
	waitForFeatureConvergence(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "gate")
	doomed := bucketOwnedBy(t, tc, 0, "doomed")
	created := bucketOwnedBy(t, tc, 0, "created")
	for _, name := range []string{bucket, doomed} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
	stopThirdNodeAndEditWhatIsKnown(t, tc, func(c *Cluster, stale string) { c.memberFeatures[stale] = map[string]bool{} })

	requests := map[transport.Opcode]any{
		transport.OpCreateDB:           CreateDatabaseRequest{Name: "fresh"},
		transport.OpDeleteDB:           DeleteDatabaseRequest{Name: "doomed"},
		transport.OpCreateBucket:       CreateBucketRequest{Database: testDatabase, Name: created, Type: BucketTypeHash},
		transport.OpDeleteBucket:       DeleteBucketRequest{Database: testDatabase, Name: doomed},
		transport.OpDeclareStoreFields: DeclareStoreFieldsRequest{Database: testDatabase, Name: bucket},
		transport.OpBatchPut:           BatchInsertRequest{Database: testDatabase, Bucket: bucket, Data: []map[string]interface{}{{"key": "k"}}},
		transport.OpBatchUpsert:        BatchInsertRequest{Database: testDatabase, Bucket: bucket, Data: []map[string]interface{}{{"key": "k"}}},
		transport.OpBlobWrite:          BlobWriteRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: []byte("blob")},
		// The lease and dead verbs validate their request before the member
		// gate, so each needs a well-formed one to reach it.
		transport.OpQueueClaim:   ClaimRequest{Database: testDatabase, Bucket: bucket},
		transport.OpQueueAck:     LeaseRequest{Database: testDatabase, Bucket: bucket, LeaseID: wellFormedLeaseID},
		transport.OpQueueRelease: LeaseRequest{Database: testDatabase, Bucket: bucket, LeaseID: wellFormedLeaseID},
		transport.OpQueueExtend:  LeaseRequest{Database: testDatabase, Bucket: bucket, LeaseID: wellFormedLeaseID, LeaseMs: 30000},
		transport.OpDeadRequeue:  DeadMoveRequest{Database: testDatabase, Bucket: bucket, All: true},
		transport.OpDeadDiscard:  DeadMoveRequest{Database: testDatabase, Bucket: bucket, All: true},
	}
	requestFor := func(op transport.Opcode) any {
		if request, ok := requests[op]; ok {
			return request
		}
		return bson.M{"database": testDatabase, "bucket": bucket, "key": "k", "field": "f", "data": bson.M{"n": 1}}
	}
	opcodes := slices.Sorted(maps.Keys(writeOpcodeFeatures))
	opcodes = slices.DeleteFunc(opcodes, func(op transport.Opcode) bool { return op == transport.OpAlterBucket || op == transport.OpDeadRestore })
	lastSequences := func() [2]uint64 {
		return [2]uint64{tc.nodes[0].server.core.wal.LastSequence(), tc.nodes[1].server.core.wal.LastSequence()}
	}

	for _, op := range opcodes {
		t.Run(op.String()+" gated", func(t *testing.T) {
			withGatedOpcode(t, op, featureNoBuildAdvertises)
			before := lastSequences()
			answer := tc.frame(0, op, requestFor(op))
			if answer.Opcode != transport.OpError || !strings.Contains(string(answer.Payload), memberFeatureRefusalText) {
				t.Errorf("%s gated on a feature no member advertises was answered %s %q; want the member-feature refusal",
					op, answer.Opcode, answer.Payload)
			}
			time.Sleep(100 * time.Millisecond)
			if after := lastSequences(); after != before {
				t.Errorf("the live nodes' logs went from %v to %v for a refused %s", before, after, op)
			}
		})
	}
	// A row naming a real feature is refused by a member that advertises
	// nothing, as it should be; only the everyBuildHasIt rows must pass.
	everyBuildOpcodes := slices.DeleteFunc(slices.Clone(opcodes), func(op transport.Opcode) bool {
		return writeOpcodeFeatures[op] != everyBuildHasIt
	})
	for _, op := range everyBuildOpcodes {
		t.Run(op.String()+" every build has it", func(t *testing.T) {
			answer := tc.frame(0, op, requestFor(op))
			if strings.Contains(string(answer.Payload), memberFeatureRefusalText) {
				t.Errorf("%s is marked everyBuildHasIt, but it was refused for a feature while a member advertises nothing: %q",
					op, answer.Payload)
			}
		})
	}
}

// TestADatabaseWriteIsRefusedWhileAMemberLacksItsFeature is BUG-0576's
// database-level case. CREATE_DB and DELETE_DB name no bucket, so they are
// broadcast to every member and never reach applyAsOwner; the gate must
// still refuse them, from a client through the real write path, before any
// node logs them.
func TestADatabaseWriteIsRefusedWhileAMemberLacksItsFeature(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	lastSequences := func() []uint64 {
		sequences := make([]uint64, len(tc.nodes))
		for index, node := range tc.nodes {
			sequences[index] = node.server.core.wal.LastSequence()
		}
		return sequences
	}

	for _, refused := range []struct {
		op      transport.Opcode
		request any
	}{
		{transport.OpCreateDB, CreateDatabaseRequest{Name: "gated"}},
		{transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase}},
	} {
		t.Run(refused.op.String()+" while a member lacks the feature", func(t *testing.T) {
			withGatedOpcode(t, refused.op, featureNoBuildAdvertises)
			// Earlier DDL is still shipping until every log stands where node 1's does.
			pollUntil(t, "every node's log to hold node 1's entries", func() bool {
				sequences := lastSequences()
				return slices.Min(sequences) == slices.Max(sequences)
			})
			before := lastSequences()

			answer := tc.frame(0, refused.op, refused.request)

			// The broadcast ships asynchronously; give an entry that was logged
			// time to reach the members' logs before they are compared.
			time.Sleep(500 * time.Millisecond)
			if after := lastSequences(); !slices.Equal(before, after) {
				t.Errorf("the nodes' logs went from %v to %v for a %s gated on %q, which every member lacks",
					before, after, refused.op, featureNoBuildAdvertises)
			}
			if answer.Opcode != transport.OpError {
				t.Fatalf("a %s gated on a feature every member lacks was answered %s %q; want a refusal",
					refused.op, answer.Opcode, answer.Payload)
			}
			var refusal constraintRefusalFrame
			if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
				t.Fatalf("decoding the refusal: %v", err)
			}
			if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable || refusal.WriteOutcome != outcomeNotApplied {
				t.Errorf("the refusal = code %#x outcome %q; want UNAVAILABLE and %q", refusal.Code, refusal.WriteOutcome, outcomeNotApplied)
			}
			if !strings.Contains(refusal.Message, `member "`+tc.addrs[1]+`"`) && !strings.Contains(refusal.Message, `member "`+tc.addrs[2]+`"`) {
				t.Errorf("the refusal %q does not name the member that lacks the feature", refusal.Message)
			}
			for _, part := range []string{refused.op.String(), "Nothing was changed.", "Upgrade the cluster"} {
				if !strings.Contains(refusal.Message, part) {
					t.Errorf("the refusal %q does not say %q", refusal.Message, part)
				}
			}
		})
	}

	t.Run("CREATE_DB while every member advertises the feature", func(t *testing.T) {
		withGatedOpcode(t, transport.OpCreateDB, featureBucketRF)
		before := tc.nodes[0].server.core.wal.LastSequence()
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "advertised"})
		if after := tc.nodes[0].server.core.wal.LastSequence(); after <= before {
			t.Errorf("node 1's log stayed at %d for a CREATE_DB every member can run; want it appended", before)
		}
	})
}
