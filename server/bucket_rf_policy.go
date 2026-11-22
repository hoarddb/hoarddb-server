package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The refusal of a name over the length limit (name_limit.go) is asked from
// here too, as createShapeRefusal: like the factor refusals it must be settled
// before the create is logged, and this is the one place both the owner and a
// node that does not route already ask. The file keeps its name so that
// write_path.go, which calls bucketRFPrecheck, is not edited; a rename to
// createPrecheck belongs after the join-freeze and create-beside-copy changes merge
// (catalog GC build SA-5).
//
// Every refusal about a per-bucket replication factor lives in this file, and
// every one is a question about the cluster's shape at the moment a create is
// accepted. So each is asked exactly once, before the create is logged: by the
// bucket's owner in Cluster.applyAsOwner, beside the unique-index precheck whose
// property it shares ("nothing is appended, applied or shipped for it"), and by
// Server.fencedDispatch for a node that does not route. Not by the create
// handler: a handler runs on a replica applying the owner's entry and on a WAL
// replay, both after the create committed, and a refusal there is either
// acknowledged as if it were an apply (a replica) or turned into a bucket the
// client was told was refused (a replay that has to be let through). Review F-3.

// errCreateInputRefused is the cause of a create refused for input its caller
// can fix: a name the server will not take, or a replication factor it cannot
// place. codeFor answers it INVALID_ARGUMENT (BUG-0665).
var errCreateInputRefused = errors.New("create input refused")

// errRFAboveMemberCount refuses a factor larger than the cluster can place.
// Ring.Replicas clamps silently, so without it a bucket reports a factor it
// does not hold.
func errRFAboveMemberCount(bucket string, factor, members int) error {
	return &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf("bucket %q asks for replication_factor %d, but this cluster has %d members, so it cannot place more than %d copies. "+
		"Add members first ('join' on the new node, then 'nodes' to confirm there are %d), or create the bucket with replication_factor %d",
		bucket, factor, members, members, factor, members)}
}

// errRFStandalone refuses a factor above one on a server that has no cluster. A
// server with no seed nodes and a factor of 1 has no write-ahead log and no
// cluster and cannot acquire either without a restart; storing the factor for
// later would leave a catalog a later reader will believe.
func errRFStandalone(bucket string, factor int) error {
	return &sentencedError{cause: errCreateInputRefused, message: fmt.Sprintf("bucket %q asks for replication_factor %d, but this server is running standalone: it has no write-ahead log and no cluster to place a second copy on, "+
		"so it keeps one copy of every bucket and cannot grant a factor above one. Replication needs a cluster of nodes; the replication page of the user docs describes how one is formed. "+
		"To create the bucket here anyway, leave replication_factor out",
		bucket, factor)}
}

// errRFPeerCannotHonourIt refuses a factor a member of this cluster would carry
// out differently: it would create the bucket without the field and place it
// from its own node factor, so two nodes would disagree about which nodes hold
// it.
func errRFPeerCannotHonourIt(bucket string, factor int, member string) error {
	return &sentencedError{cause: errMemberLacksFeature, message: fmt.Sprintf("bucket %q asks for replication_factor %d, but member %s has answered this node without advertising per-bucket replication factors, "+
		"so it would create the bucket with its own node-wide factor and the two nodes would disagree about which of them hold it. "+
		"Upgrade %s (wait for 'nodes' to show it active), or create the bucket without a replication_factor so every node follows the same default",
		bucket, factor, member, member)}
}

// factorShape is why this server cannot keep a number of copies of a bucket:
// the shape predicate a create and a factor alter share, apart from the words
// each refuses with (bucket-replication-rev8-spec §4.3, B F-4). The zero value
// is "it can".
type factorShape struct {
	// standalone: the server has no cluster to place a second copy on.
	standalone bool
	// members is the cluster's member count when the factor is above it.
	members int
	// lacking is a member that gossiped without the feature asked about.
	lacking string
}

// factorShapeOf is the shape predicate for factor copies: standalone, then
// above the member count, then, when feature is not empty, a member that
// gossiped without it. One copy is what every server can do.
func (s *Server) factorShapeOf(factor int, feature string) factorShape {
	if factor <= 1 {
		// A negative factor is malformed and createBucketLocked refuses it at
		// every depth.
		return factorShape{}
	}
	if s.cluster == nil {
		return factorShape{standalone: true}
	}
	if members := len(s.cluster.Members()); factor > members {
		return factorShape{members: members}
	}
	if feature != "" {
		if member := s.cluster.memberWithout(feature); member != "" {
			return factorShape{lacking: member}
		}
	}
	return factorShape{}
}

// checkBucketRF is the shape policy for a create: nil when the request declares
// no factor or the cluster can honour it.
func (s *Server) checkBucketRF(req CreateBucketRequest) error {
	shape := s.factorShapeOf(req.ReplicationFactor, featureBucketRF)
	switch {
	case shape.standalone:
		return errRFStandalone(req.Name, req.ReplicationFactor)
	case shape.members > 0:
		return errRFAboveMemberCount(req.Name, req.ReplicationFactor, shape.members)
	case shape.lacking != "":
		return errRFPeerCannotHonourIt(req.Name, req.ReplicationFactor, shape.lacking)
	}
	return nil
}

// createShapeRefusal is every refusal a create gets before it is logged: a name
// over the length limit, then a replication factor the cluster cannot honour.
func (s *Server) createShapeRefusal(req CreateBucketRequest) error {
	if err := checkNamePair(req.Database, req.Name); err != nil {
		return err
	}
	if err := createNameRefusal(req); err != nil {
		return err
	}
	return s.checkBucketRF(req)
}

// refuseCreate is the response a refused create is answered with: the same shape
// createBucketLocked's own refusals have, so a driver reads them alike.
func refuseCreate(reason error) ([]byte, error) {
	payload, err := bson.Marshal(CreateBucketResponse{Success: false, Message: reason.Error()})
	if err != nil {
		return nil, fmt.Errorf("encoding the refusal of a create: %w", err)
	}
	return payload, nil
}

// databaseCreateRefusal is createShapeRefusal for a database create, asked by
// the coordinator before the broadcast is logged: a name databaseNameRefusal
// refuses, if logged, is refused at apply by every member. It returns nil for
// any other frame, for a payload the handler reads and refuses in its own
// words, and for a name every member accepts.
func (s *Server) databaseCreateRefusal(frame *transport.Frame) *transport.Frame {
	if frame.Opcode != transport.OpCreateDB {
		return nil
	}
	var req CreateDatabaseRequest
	if err := bson.Unmarshal(frame.Payload, &req); err != nil {
		return nil
	}
	if refusal := s.databaseNameRefusal(req.Name); refusal != nil {
		return respondFrame(frame, transport.OpCreateDB, refusal)
	}
	if err := clientNameRefusal(frame.Opcode, frame.Payload); err != nil {
		return respondFrame(frame, transport.OpCreateDB, &CreateDatabaseResponse{Success: false, Message: err.Error()})
	}
	return nil
}

// declareNamePrecheck is the owner's ask, before the append, for a declare of
// store fields: a field name that holds a control character is refused, and
// nothing is logged, applied or shipped for it.
func (c *Cluster) declareNamePrecheck(entry ReplEntry) (ReplResponse, bool) {
	if transport.Opcode(entry.Op) != transport.OpDeclareStoreFields {
		return ReplResponse{}, false
	}
	refusal := clientNameRefusal(transport.OpDeclareStoreFields, entry.Payload)
	if refusal == nil {
		return ReplResponse{}, false
	}
	frame := errorFrame(&transport.Frame{Opcode: transport.OpDeclareStoreFields}, withOutcome(outcomeNotApplied, refusal))
	return ReplResponse{Opcode: int32(frame.Opcode), Payload: frame.Payload}, true
}

// bucketRFPrecheck is the owner's ask, before the append: the name length and the
// replication factor. A refused create is neither logged, applied nor shipped.
func (c *Cluster) bucketRFPrecheck(entry ReplEntry) (ReplResponse, bool) {
	if transport.Opcode(entry.Op) != transport.OpCreateBucket {
		return ReplResponse{}, false
	}
	var req CreateBucketRequest
	if err := bson.Unmarshal(entry.Payload, &req); err != nil {
		return ReplResponse{}, false
	}
	reason := c.s.createShapeRefusal(req)
	if reason == nil {
		return ReplResponse{}, false
	}
	payload, err := refuseCreate(reason)
	if err != nil {
		return ReplResponse{}, false
	}
	return ReplResponse{Opcode: int32(transport.OpCreateBucket), Payload: payload}, true
}

// warnUnhonourableFactors says, once, at the end of LoadCatalog, that a
// standalone server holds buckets whose declared factor it cannot honour: how
// many, and at most ten names, because a warning per bucket is a line per bucket
// at 10^6 buckets.
func (s *Server) warnUnhonourableFactors(entries []catalogEntry) {
	if s.cluster != nil {
		return
	}
	const named = 10
	count := 0
	names := make([]string, 0, named)
	for _, entry := range entries {
		if entry.ReplicationFactor <= 1 {
			continue
		}
		count++
		if len(names) < named {
			names = append(names, entry.database+"."+entry.bucket)
		}
	}
	if count == 0 {
		return
	}
	s.logger.Warn("this server is standalone and cannot keep the replication factor these buckets declare: it holds one copy of each. "+
		"The declared factors are kept and take effect if this data directory is served by a cluster (the replication page of the user docs)",
		"buckets", count, "first", strings.Join(names, ", "))
}
