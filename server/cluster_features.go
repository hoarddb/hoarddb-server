package server

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// featureBucketRF is advertised by a build that honours a per-bucket
// replication factor: it reads CreateBucketRequest.ReplicationFactor, stores it,
// and places the bucket at it.
const featureBucketRF = "bucket_rf"

// featureMemtableMB is advertised by a build that puts its applied memtable
// size on gossip (GossipRequest.AppliedMemtableMB), so a zero from it is a
// zero and an omission from an older build is not mistaken for one.
const featureMemtableMB = "memtable_mb"

// featureTypedFields is advertised by a build that reads an update's keys as
// paths (typed-fields spec §4.9, OC-1). An older member reads $set of
// "Address.City" as a literal top-level key, so a path-keyed update is refused
// while a member that has gossiped does not advertise it (E-19).
const featureTypedFields = "typed_fields"

// featurePackedVersions is advertised by a build that packs a write's version
// from its term and sequence (packVersion) and answers a versioned read with
// the copy's lineage. A consistent get is refused while a member does not
// advertise it: a member that stamps the bare sequence writes versions every
// packed one outranks.
const featurePackedVersions = "packed_versions"

// featureBucketRFAlter is advertised by a build that changes a bucket's
// replication factor (OpAlterBucket) on both its sides: it logs the alter as an
// owner and applies it as a member. An older member would fail the alter's
// dispatch and stall its stream, so an alter is refused while a member that
// has gossiped does not advertise it (bucket-replication-rev8-spec §11.2). It is
// advertised from the build that gives the alter its authorization row: one
// that advertised it without the member side would pass that check in a mixed
// cluster.
const featureBucketRFAlter = "bucket_rf_alter"

// featureQueueClaim is advertised by a build that runs the lease verbs (claim,
// ack, release, extend) and the dead-letter writes. An older member would fail
// the dispatch of an opcode it does not know and stall its stream, so each is
// refused while a member that has gossiped does not advertise it
// (claim-ack-spec 10.2).
const featureQueueClaim = "queue_claim"

// featureBatch is advertised by a build that runs a BATCH request: to drivers
// at AUTH, and to members on gossip, where it says the member runs a
// REPL_PUSH with Batch set as one sub-batch of independent entries
// (batch-request spec §5.2 step 7). A coordinator sends an owner that does not
// advertise it one forward per item instead.
const featureBatch = "batch"

// clusterFeatures is what this build advertises on every gossip exchange.
//
// The list exists so an owner can REFUSE an operation a member would carry out
// differently, which is the opposite of the usual additive-field contract. An
// older member decoding a create with ReplicationFactor set drops the field,
// creates the bucket, and places it at its own node factor, so two nodes
// disagree about which nodes hold it and nothing in the protocol notices. There
// is no version number to compare, so an advertisement that is missing from a
// member that did gossip is the signal.
func clusterFeatures() []string {
	return []string{featureBucketRF, featureMemtableMB, featureTypedFields, featurePackedVersions, featureBucketRFAlter, featureInstanceNonce, featureQueueClaim, featureBatch}
}

// serverCapabilities is what this server tells a driver at AUTH. `auth_token`
// says it can resume by token; the rest name read-path and durability features
// a driver may rely on. `bucket_rf` says it honours
// CreateBucketRequest.ReplicationFactor, so a driver that intends to set one
// learns by name rather than sending a field an older server drops in silence.
// `query_pipelining` promises (query-pipelining spec §3.7, W-1) that queries in
// flight on one connection run independently, that each request gets one
// answer while the connection lives, that an oversized result leaves the
// connection up, that a query is abandoned when its client goes away, and
// that a connection with a request in flight is not closed for idleness.
func serverCapabilities() []string {
	return []string{"pipelining", "get_fast", "write_concern", "consistent_get", "auth_token", featureBucketRF, "query_pipelining", featureBatch}
}

// containsString reports whether a list holds an exact string.
func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// recordMemberFeatures stores what a member advertised on a gossip exchange it
// took part in. An empty list is recorded as an empty set rather than dropped:
// "this member gossiped and told us nothing" is evidence about that member, and
// "we have not heard from it" is not, and they must not be the same state.
func (c *Cluster) recordMemberFeatures(nodeID string, features []string) {
	if nodeID == "" {
		return
	}
	advertised := make(map[string]bool, len(features))
	for _, feature := range features {
		advertised[feature] = true
	}
	c.featuresMu.Lock()
	defer c.featuresMu.Unlock()
	c.memberFeatures[nodeID] = advertised
}

// memberWithout names a member that has answered gossip without advertising
// feature, or the empty string. A member this node has not heard from — one
// that just joined, one that is down since before this node started — is not
// evidence that it lacks the feature and does not block: the member list is in
// a deterministic order, so a retry is told about the same node. It is
// O(members), never O(buckets).
func (c *Cluster) memberWithout(feature string) string {
	c.featuresMu.RLock()
	defer c.featuresMu.RUnlock()
	for _, member := range c.Members() {
		if member.ID == c.self.ID {
			continue
		}
		if advertised, heard := c.memberFeatures[member.ID]; heard && !advertised[feature] {
			return member.ID
		}
	}
	return ""
}

// memberAdvertises reports whether member has gossiped feature to this node.
// A member not heard from does not.
func (c *Cluster) memberAdvertises(member, feature string) bool {
	c.featuresMu.RLock()
	defer c.featuresMu.RUnlock()
	return c.memberFeatures[member][feature]
}

// typedFieldsPrecheck is the owner's ask, before the append (OC-1, D-1): an
// update whose keys hold a path, and a create that declares typed fields or a
// mode, are refused while a member lacks typed_fields, so nothing is logged,
// applied or shipped for them (E-19). The request is decoded only when a
// member lacks the feature.
func (c *Cluster) typedFieldsPrecheck(entry ReplEntry) (ReplResponse, bool) {
	switch transport.Opcode(entry.Op) {
	case transport.OpCreateBucket:
		return c.typedCreatePrecheck(entry)
	case transport.OpUpdate:
	default:
		return ReplResponse{}, false
	}
	member := c.memberWithout(featureTypedFields)
	if member == "" {
		return ReplResponse{}, false
	}
	var req UpdateRequest
	if err := bson.Unmarshal(entry.Payload, &req); err != nil || !updateNamesAPath(req) {
		return ReplResponse{}, false
	}
	refusal := &sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf(
		"Bucket %q (database %q): a path update needs every cluster member to run a hoardDB that supports it, "+
			"and member %q does not. Nothing was changed.\n"+
			"Upgrade the cluster: stop every node once replication has settled, start the new version on all of them, then retry.",
		req.Bucket, req.Database, member)}
	frame := errorFrame(&transport.Frame{Opcode: transport.OpUpdate}, withOutcome(outcomeNotApplied, refusal))
	return ReplResponse{Opcode: int32(frame.Opcode), Payload: frame.Payload}, true
}

// typedCreatePrecheck is typedFieldsPrecheck for a create: an older member
// would decode the create, drop the declaration and create the bucket
// undeclared, then store every write the owner refuses. It is answered in-band,
// Success false, as bucketRFPrecheck answers a create the cluster cannot
// honour.
func (c *Cluster) typedCreatePrecheck(entry ReplEntry) (ReplResponse, bool) {
	member := c.memberWithout(featureTypedFields)
	if member == "" {
		return ReplResponse{}, false
	}
	var req CreateBucketRequest
	if err := bson.Unmarshal(entry.Payload, &req); err != nil || !createDeclaresTypedFields(req) {
		return ReplResponse{}, false
	}
	payload, err := refuseCreate(&sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf(
		"Bucket %q (database %q): typed fields need every cluster member to run a hoardDB that supports them, "+
			"and member %q does not. Nothing was changed.\n"+
			"Upgrade the cluster: stop every node once replication has settled, start the new version on all of them, then retry.",
		req.Name, req.Database, member)})
	if err != nil {
		return ReplResponse{}, false
	}
	return ReplResponse{Opcode: int32(transport.OpCreateBucket), Payload: payload}, true
}

// createDeclaresTypedFields reports whether a create carries a typed-field
// declaration or a mode (spec §8.3; TF-10 adds a store entry's types).
func createDeclaresTypedFields(req CreateBucketRequest) bool {
	_, fields := req.Options["fields"]
	_, mode := req.Options["field_types"]
	return fields || mode
}

// everyBuildHasIt is the writeOpcodeFeatures entry of a write opcode that every
// build which can join the cluster runs with the same meaning, so no member can
// carry it out differently and it needs no advertisement.
const everyBuildHasIt = ""

// writeOpcodeFeatures says, for every write opcode this build runs, which
// member feature a write of it needs (BUG-0576). The node that would log the
// write first refuses it, before its log, while a member that has gossiped
// does not advertise that feature (memberFeaturePrecheck).
//
// The rule: a write opcode added, or whose meaning changes, after v1.0 names a
// new feature here, and clusterFeatures advertises it from the build that
// introduces it. everyBuildHasIt is an honest entry only for an opcode
// present, with its meaning, at v1.0. The table must exist in v1.0 for this to
// work: a missing advertisement can mean "older build" only if v1.0 already
// advertises the features it has. TestEveryWriteOpcodeDeclaresAMemberFeatureOrThatEveryBuildHasIt
// fails for a write opcode with no entry.
var writeOpcodeFeatures = map[transport.Opcode]string{
	transport.OpPut:                everyBuildHasIt,
	transport.OpDelete:             everyBuildHasIt,
	transport.OpBatchPut:           everyBuildHasIt,
	transport.OpUpsert:             everyBuildHasIt,
	transport.OpBatchUpsert:        everyBuildHasIt,
	transport.OpFIFOPush:           everyBuildHasIt,
	transport.OpFIFOPop:            everyBuildHasIt,
	transport.OpLIFOPush:           everyBuildHasIt,
	transport.OpLIFOPop:            everyBuildHasIt,
	transport.OpBTreeInsert:        everyBuildHasIt,
	transport.OpHeapPush:           everyBuildHasIt,
	transport.OpHeapPop:            everyBuildHasIt,
	transport.OpBlobWrite:          everyBuildHasIt,
	transport.OpCreateDB:           everyBuildHasIt,
	transport.OpCreateBucket:       everyBuildHasIt,
	transport.OpDeleteDB:           everyBuildHasIt,
	transport.OpDeleteBucket:       everyBuildHasIt,
	transport.OpUpdate:             everyBuildHasIt,
	transport.OpDeclareStoreFields: everyBuildHasIt,
	transport.OpFieldPush:          everyBuildHasIt,
	transport.OpFieldPop:           everyBuildHasIt,
	transport.OpFieldPut:           everyBuildHasIt,
	transport.OpFieldDelete:        everyBuildHasIt,
	// alterPrecheck refuses an alter first, with the factor in its message;
	// the entry records that a build without the member side exists today.
	transport.OpAlterBucket: featureBucketRFAlter,
	// The lease verbs are new after v1.0's table, so each names the feature
	// that says a member runs them.
	transport.OpQueueClaim:   featureQueueClaim,
	transport.OpQueueAck:     featureQueueClaim,
	transport.OpQueueRelease: featureQueueClaim,
	transport.OpQueueExtend:  featureQueueClaim,
	// DEAD_LIST is a read and has no row: the drift test fails for an entry on
	// something that is not a write.
	transport.OpDeadRequeue: featureQueueClaim,
	transport.OpDeadDiscard: featureQueueClaim,
	transport.OpDeadRestore: featureQueueClaim,
}

// gatedOpcode is a test seam: it gives op an entry in writeOpcodeFeatures that
// names feature, so a gate can be exercised before any build differs from
// another.
type gatedOpcode struct {
	op      transport.Opcode
	feature string
}

// gatedOpcodeHook, when set, overrides one opcode's writeOpcodeFeatures entry.
// It is process-global: every in-process node reads it. Nil in production.
var gatedOpcodeHook atomic.Pointer[gatedOpcode]

// memberFeatureOf returns the feature a write of op needs. everyBuildHasIt,
// which an opcode with no entry also gets, needs nothing: the drift test, not
// this lookup, is what keeps every write opcode declared.
func memberFeatureOf(op transport.Opcode) string {
	if gated := gatedOpcodeHook.Load(); gated != nil && gated.op == op {
		return gated.feature
	}
	return writeOpcodeFeatures[op]
}

// memberFeaturePrecheck is asked, before the append, for any write whose
// writeOpcodeFeatures entry names a feature: while a member that has gossiped
// lacks it, the write is refused and nothing is logged, applied or shipped
// (BUG-0576). A bucket's owner asks it in applyAsOwner; the node a client's
// database DDL reaches asks it in handleClientFrame, because that DDL is
// broadcast and never reaches applyAsOwner. An opcode every build has costs one map lookup;
// memberWithout runs only for a gated one.
func (c *Cluster) memberFeaturePrecheck(entry ReplEntry) (ReplResponse, bool) {
	op := transport.Opcode(entry.Op)
	feature := memberFeatureOf(op)
	if feature == everyBuildHasIt {
		return ReplResponse{}, false
	}
	member := c.memberWithout(feature)
	if member == "" {
		return ReplResponse{}, false
	}
	bucket, database := decodeFrameMeta(entry.Payload).scope(op)
	target := fmt.Sprintf("Database %q", database)
	if bucket != "" {
		target = fmt.Sprintf("Bucket %q (database %q)", bucket, database)
	}
	refusal := &sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf(
		"%s: %s needs every cluster member to run a hoardDB that supports %q, "+
			"and member %q does not. Nothing was changed.\n"+
			"Upgrade the cluster: stop every node once replication has settled, start the new version on all of them, then retry.",
		target, op, feature, member)}
	frame := errorFrame(&transport.Frame{Opcode: op}, withOutcome(outcomeNotApplied, refusal))
	return ReplResponse{Opcode: int32(frame.Opcode), Payload: frame.Payload}, true
}

// errMemberLacksFeature is under E-19: a cluster member that does not
// advertise a feature the request needs. Upgrading the member is the fix, so
// it reaches the wire as UNAVAILABLE.
var errMemberLacksFeature = errors.New("a cluster member lacks a feature")

// updateNamesAPath reports whether any operator key of req, or any key of its
// filter, holds a path. A multi-update's filter is re-evaluated by every copy,
// so an older member would read a path in it literally and change different
// documents.
func updateNamesAPath(req UpdateRequest) bool {
	isPath := func(key string) bool { return strings.ContainsAny(key, ".[") }
	if filterNamesAPath(req.Filter, isPath) {
		return true
	}
	for _, keys := range [][]string{
		slices.Collect(maps.Keys(req.Set)), req.Unset, slices.Collect(maps.Keys(req.Inc)),
		slices.Collect(maps.Keys(req.Push)), slices.Collect(maps.Keys(req.AddToSet)),
		slices.Collect(maps.Keys(req.Mul)), slices.Collect(maps.Keys(req.Min)),
		slices.Collect(maps.Keys(req.Max)), slices.Collect(maps.Keys(req.Rename)), slices.Collect(maps.Values(req.Rename)),
	} {
		if slices.ContainsFunc(keys, isPath) {
			return true
		}
	}
	return false
}

// filterNamesAPath reports whether a filter's field keys, at the top or inside
// its $and and $or clauses, hold a path.
func filterNamesAPath(filter map[string]interface{}, isPath func(string) bool) bool {
	for key, condition := range filter {
		if key != "$and" && key != "$or" {
			if isPath(key) {
				return true
			}
			continue
		}
		clauses, _ := asList(condition)
		for _, clause := range clauses {
			if nested, ok := clause.(map[string]interface{}); ok && filterNamesAPath(nested, isPath) {
				return true
			}
		}
	}
	return false
}
