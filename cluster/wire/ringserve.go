package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the single-node binding of entry-ring-client-wire.md: the dispatcher that
// takes an authenticated RING_KEYED, RING_LIST, RING_COUNT, RING_ADMIN or RING_TOPOLOGY frame and
// answers it from a Node. It lives in package wire, beside Node, because the dispatch
// needs Node's resolved buckets, and wire already imports transport (the server package
// dispatches the legacy opcodes with a switch of its own; the ring has no database name
// on its requests and so cannot pass that package's per-database gate, see build SPEC-GAP
// U66-2). It starts no goroutine: the transport runs each frame on its own, with its own
// exit path, and a handler returns when its frame is answered.

// ringServer answers ring frames from one node, for the sessions its authorizer admits.
type ringServer struct {
	node  *Node
	authz RingAuthorizer
	// adminMu serializes create_database: Node.CreateDatabase reads the next database id
	// before it proposes, so two concurrent creates would both read the same id.
	adminMu sync.Mutex
	// facts are what describe_node reports that only the server process knows.
	facts RingNodeFacts
	// membership is the node-side behaviour of the membership commands; NoMembership unless WithMembership says otherwise.
	membership MembershipOps
	// users is the node-side behaviour of the user commands; NoUserAdmin unless WithUserAdmin says otherwise.
	users UserAdmin
	// resultBudget is the bytes of answers one RING_BATCH reply keeps whole; see chargeBatchAnswers.
	resultBudget int64
	// elementCeiling is the most bytes one pop or take answer can add to a reply. The dedupe
	// window caps a stored answer at maxResultBytes (popFit trims a pop {n} to it), so that cap
	// plus the reply's own members is the whole bound; raising the cap raises this with it.
	elementCeiling int64
	// expandFetch and expandOwner replace the far read and the owner lookup of an expand; nil
	// means the node's own. Tests set them to count reads per owner or to fail one.
	expandFetch expandFetcher
	expandOwner func(ref bucketRef, slot uint16) string
	// expandBytes lowers the byte ceiling of an expanded page; 0 means the page ceiling.
	expandBytes int64
	// bucketLookup replaces Node.bucketByID in admission; nil means the node's own. Tests set it
	// to count the lookups of a frame.
	bucketLookup func(ctx context.Context, bucketID uint64, admit func(database, bucket string) error) (bucketRef, error)
	// manyFetch and manyOwner replace the read and the owner lookup of a get-many key; nil means
	// the node's own (Node.readDocumentFrom and the owner table). manyHeld, when set, is told the
	// bytes of answers a get-many frame holds each time a read lands. Tests set them.
	manyFetch expandFetcher
	manyOwner func(ref bucketRef, slot uint16) string
	manyHeld  func(bytes int64)
	// batchExecute replaces execute for the operations of a RING_BATCH; nil means execute. Tests
	// set it to answer one operation as a node with a multi-member cluster can (an outcome the
	// owner cannot vouch for), which a one-member test node never does.
	batchExecute func(ctx context.Context, ref bucketRef, requestID [16]byte, req RingKeyedRequest, pos ringBatchPos) (RingReply, error)
}

// ringReplyEnvelopeBytes is the room a reply's own members (outcome, epoch, lease, position and
// the answer's framing) take beside the elements it carries.
const ringReplyEnvelopeBytes = 4096

// NewRingHandler returns the transport.RequestHandler that serves the ring opcodes on
// node and hands every other frame, AUTH included, to next. The transport has already
// held an unauthenticated connection to AUTH and PING, so a ring frame reaches this
// handler only on an authenticated session. authz decides what that session may do
// (see RingAuthorizer); there is no default, so a nil authz is an error, not allow-all.
func NewRingHandler(node *Node, next transport.RequestHandler, authz RingAuthorizer, options ...RingOption) (transport.RequestHandler, error) {
	if authz == nil {
		return nil, errNoRingAuthorizer
	}
	server := &ringServer{node: node, authz: authz, facts: defaultNodeFacts(), membership: NoMembership{}, users: NoUserAdmin{}, resultBudget: maxRingBatchBudget, elementCeiling: maxResultBytes + ringReplyEnvelopeBytes}
	for _, option := range options {
		option(server)
	}
	return func(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
		switch frame.Opcode {
		case transport.OpRingKeyed:
			return server.keyed(ctx, frame)
		case transport.OpRingBatch:
			return server.batch(ctx, frame)
		case transport.OpRingGetMany:
			return server.getMany(ctx, frame)
		case transport.OpRingList:
			return server.list(ctx, frame)
		case transport.OpRingCount:
			return server.count(ctx, frame)
		case transport.OpRingAdmin:
			return server.admin(ctx, frame)
		case transport.OpRingTopology:
			return server.topology(ctx, frame)
		}
		return next(ctx, frame)
	}, nil
}

// ringDetailed is an error that carries the structured detail of its code.
type ringDetailed struct {
	err    *cluster.Error
	detail cluster.Detail
}

func (e *ringDetailed) Error() string { return e.err.Error() }
func (e *ringDetailed) Unwrap() error { return e.err }

// ringFailure turns err into the OpError frame answering frame: the BSON payload of
// entry-ring-client-wire.md section 3, with the detail the code defines. An error that is
// not a ring refusal is UNAVAILABLE with its text, because the caller cannot know
// whether a write took effect and a retry with the same request id is safe. It returns an
// error only when even the short fallback text cannot be encoded, which the transport
// answers as an internal error.
func ringFailure(frame *transport.Frame, err error) (*transport.Frame, error) {
	refusal := ringRefusalOf(err)
	payload, encodeErr := EncodeRingError(refusal)
	if encodeErr != nil {
		// The text or the detail did not fit the wire form: keep the code, say so.
		payload, encodeErr = EncodeRingError(ringShortRefusal(refusal.Code))
	}
	if encodeErr != nil {
		return nil, fmt.Errorf("encoding the error frame for opcode %s: %w", frame.Opcode, encodeErr)
	}
	return transport.NewFrameWithID(transport.OpError, frame.RequestID, payload), nil
}

// ringRefusalOf is the RingError an error is answered with: the ring refusal it is, with
// the detail its code defines, or UNAVAILABLE with its text.
func ringRefusalOf(err error) RingError {
	refusal := RingError{Code: cluster.CodeUnavailable, Message: err.Error()}
	var coded *cluster.Error
	if errors.As(err, &coded) && ringCodeRegistered(coded.Code) && coded.Message != "" {
		refusal = RingError{Code: coded.Code, Message: coded.Message}
	}
	var detailed *ringDetailed
	if errors.As(err, &detailed) && detailed.err.Code == refusal.Code {
		detail := detailed.detail
		refusal.Detail = &detail
	}
	return refusal
}

// ringShortRefusal is the answer for a refusal whose full text or detail does not fit the wire form.
func ringShortRefusal(code cluster.Code) RingError {
	return RingError{Code: code, Message: fmt.Sprintf(
		"%s: the full message does not fit an error frame. Retry the request; a retry with the same request id is safe", code)}
}

func (s *ringServer) keyed(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingKeyed(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	reply, err := s.serve(ctx, frame.RequestID, req)
	if err != nil {
		return ringFailure(frame, err)
	}
	payload, err := EncodeRingReply(reply)
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// ringBatchPos is where a keyed request sits in its client batch. The zero value is a lone
// request, which the owner keys as index 0 of a batch of 0, the same dedupe entry as index 0
// of a batch of one.
type ringBatchPos struct {
	index uint8
	size  int
}

// serve runs one decoded keyed request: it resolves the bucket by id, checks the client's
// epoch and calls the Node method of the class.
func (s *ringServer) serve(ctx context.Context, requestID [16]byte, req RingKeyedRequest) (RingReply, error) {
	ref, err := s.admit(ctx, req)
	if err != nil {
		return RingReply{}, err
	}
	return s.execute(ctx, ref, requestID, req, ringBatchPos{})
}

// admit is everything a keyed request must pass before it touches the bucket: the class-only
// authorization, the bucket by id with the per-bucket authorization, and the client's epoch.
// A batch runs it once per operation and never once per frame, so a frame cannot widen a grant.
func (s *ringServer) admit(ctx context.Context, req RingKeyedRequest) (bucketRef, error) {
	return s.admitIn(ctx, req, nil)
}

// admitMemo is what one frame already learned about the bucket ids its operations name.
type admitMemo struct {
	found map[uint64]bucketRef
	// refused holds the refusal of an id the lookup did not resolve (no bucket has it, it was
	// dropped, or a database could not be asked), so the frame pays that lookup once per id. It is
	// nil when refusals are not remembered. A refusal is per class: the lookup asks about the
	// operation's class, so a bucket refused for get_fields may still be read by get.
	refused map[admitRefusalKey]error
}

// admitRefusalKey names a remembered refusal: the bucket id the lookup did not resolve for one
// class of operation.
type admitRefusalKey struct {
	bucketID uint64
	class    byte
}

// newAdmitMemo returns an empty memo; refusals says whether it remembers refused ids too.
func newAdmitMemo(refusals bool) *admitMemo {
	memo := &admitMemo{found: map[uint64]bucketRef{}}
	if refusals {
		memo.refused = map[admitRefusalKey]error{}
	}
	return memo
}

// admitIn is admit with a memo of the buckets this frame already resolved. A memo hit skips only
// the lookup of the bucket by id: the class-only ask, the ask about the bucket's database and
// bucket, and the epoch check are made again for every operation. A remembered refusal is the
// refusal the lookup gave, which names neither a database nor a bucket.
func (s *ringServer) admitIn(ctx context.Context, req RingKeyedRequest, memo *admitMemo) (bucketRef, error) {
	class, bucketID, encID := req.Op.Class, req.Op.BucketID, req.Op.EncID
	if req.Read != nil {
		class, bucketID, encID = req.Read.Class, req.Read.BucketID, req.Read.EncID
	}
	// A session that holds this access nowhere learns nothing: it is refused before any
	// lookup, and the class refusals below are for sessions that may use the bucket.
	if err := s.authz.Authorize(ctx, RingAccess{Class: class}); err != nil {
		return bucketRef{}, ringDenied()
	}
	var ref bucketRef
	known := false
	if memo != nil {
		if refusal, refused := memo.refused[admitRefusalKey{bucketID, class}]; refused {
			return bucketRef{}, refusal
		}
		ref, known = memo.found[bucketID]
	}
	if known {
		if err := s.authz.Authorize(ctx, RingAccess{Database: ref.db, Bucket: ref.name, Class: class}); err != nil {
			return bucketRef{}, ringDenied()
		}
	} else {
		lookup := s.node.bucketByID
		if s.bucketLookup != nil {
			lookup = s.bucketLookup
		}
		var err error
		ref, err = lookup(ctx, bucketID, func(database, bucket string) error {
			return s.authz.Authorize(ctx, RingAccess{Database: database, Bucket: bucket, Class: class})
		})
		if err != nil {
			if memo != nil && memo.refused != nil {
				memo.refused[admitRefusalKey{bucketID, class}] = err
			}
			return bucketRef{}, err
		}
		if memo != nil {
			memo.found[bucketID] = ref
		}
	}
	if err := s.node.checkClientEpoch(req.Prologue.Epoch, ref, encID); err != nil {
		return bucketRef{}, err
	}
	return ref, nil
}

// execute runs an admitted request against its bucket and stamps the reply with the node's view.
func (s *ringServer) execute(ctx context.Context, ref bucketRef, requestID [16]byte, req RingKeyedRequest, pos ringBatchPos) (RingReply, error) {
	var reply RingReply
	var err error
	if req.Read != nil {
		reply, err = s.read(ctx, ref, *req.Read)
	} else {
		reply, err = s.write(ctx, ref, requestID, req, pos)
	}
	if err != nil {
		return RingReply{}, err
	}
	if reply.WriteOutcome != "" {
		reply.ServedBy = s.node.cfg.NodeID
	}
	epoch, stateSeq := int64(s.node.topology.Committed()), s.node.topology.seq.Load()
	reply.Epoch, reply.StateSeq = &epoch, &stateSeq
	return reply, nil
}

// bucketByID finds the bucket a RING_KEYED header names. The request carries a bucket id
// and no database (build SPEC-GAP U66-1). A bucket the node served lately is remembered
// (bucketIDCache) and the remembered place is trusted only after the bucket's current
// declaration still has this id; otherwise each database's reverse index is asked until one
// has the id. A database that cannot answer is skipped, because an existing bucket must not
// become unreadable for the sake of an unrelated database; when no database has the id the
// refusal is UNAVAILABLE, since then the node cannot say the bucket does not exist, and its
// text names no database and no node (the cause is logged).
// A bucket id is 8 random bytes, so the first match is taken.
//
// admit is asked about the database and bucket the id names before anything else reads
// that bucket, and a refusal from it, like an id no database has, is ringDenied: the one
// answer that does not say which of the two it was (build SPEC-GAP U66-2). Nothing in a
// refusal returned before admit passes names a database or a bucket.
func (n *Node) bucketByID(ctx context.Context, bucketID uint64, admit func(database, bucket string) error) (bucketRef, error) {
	if db, name, ok := n.bucketIDs.get(bucketID); ok {
		if err := admit(db, name); err != nil {
			return bucketRef{}, ringDenied()
		}
		if ref, err := n.bucket(ctx, db, name); err == nil && ref.entry.Decl.BucketID == bucketID {
			return ref, nil
		}
		n.bucketIDs.forget(bucketID)
	}
	unanswered := 0
	var lastErr error
	for dbID, row := range n.topology.view.Load().byID {
		name, ok, err := n.fetcher.readName(ctx, dbID, bucketID)
		if err != nil {
			unanswered, lastErr = unanswered+1, err
			continue
		}
		if !ok {
			continue
		}
		if err := admit(row.name, name); err != nil {
			return bucketRef{}, ringDenied()
		}
		ref, err := n.bucket(ctx, row.name, name)
		if err != nil {
			return bucketRef{}, err
		}
		if ref.entry.Decl.BucketID != bucketID {
			// A reverse-index document that outlived its bucket: the name now belongs to a
			// bucket with another id, and a request for the old id must not reach it.
			continue
		}
		n.bucketIDs.put(bucketID, row.name, name)
		return ref, nil
	}
	if lastErr != nil {
		// The cause names a node and a database, which the session may not hold: it is
		// logged here and the reply is one fixed text.
		slog.Warn("a ring request named a bucket id no reachable database has, and a database could not be asked",
			"bucket_id", bucketID, "databases_unanswered", unanswered, "error", lastErr)
		return bucketRef{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"the bucket for id %d was not found, but a database could not be asked and it may be in that one. Nothing was done. Retry in a moment; a retry with the same request id is safe",
			bucketID)}
	}
	return bucketRef{}, ringDenied()
}

// ringDenied is the PERMISSION_DENIED answer for a bucket id the session may not use. It
// is the answer for a bucket that exists in a database the session has no grant on and
// for an id no database has, in one text that names neither a database nor a bucket, so a
// session cannot learn what exists by asking (client-wire section 3).
func ringDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot use that bucket: no bucket has the id " +
		"the request carried on this node, or the user holds no role on the bucket's database that allows this operation. " +
		"Nothing was done. Check the bucket's id with 'describe bucket <bucket> in <database>', and ask an administrator to run " +
		"'grant role <reader or writer>@<database> to <user>' if the user should have access"}
}

// checkClientEpoch refuses a request whose client epoch is neither 0 (route it) nor the
// node's committed epoch, with the STALE_EPOCH detail a driver refreshes its table from
// (conventions section 7).
func (n *Node) checkClientEpoch(clientEpoch uint64, ref bucketRef, encID []byte) error {
	committed := n.topology.Committed()
	if clientEpoch == 0 || clientEpoch == committed {
		return nil
	}
	if clientEpoch > committed {
		// A client that knows a later epoch than this node does is evidence the cluster moved
		// (conventions section 7): this node stops serving until it catches up.
		markBehind(n.lease, clientEpoch, true)
	}
	slot := ref.slot(encID)
	owner := n.topology.Self()
	ownerAddr := ""
	if row, ok := n.topology.Database(ref.dbID); ok {
		if member, found := n.topology.Table().Owner(slot, row.RF); found {
			owner, ownerAddr = member.ID, member.DriverAddr()
		}
	}
	return &ringDetailed{
		err: &cluster.Error{Code: cluster.CodeStaleEpoch, Message: cluster.CodeStaleEpoch.Text(cluster.CondStaleEpoch, map[string]string{
			"slot": fmt.Sprint(slot), "db": ref.db, "owner": owner, "e": fmt.Sprint(committed), "e_seen": fmt.Sprint(clientEpoch),
			"bucket": ref.name,
		})},
		detail: cluster.Detail{Epoch: committed, Owner: owner, OwnerAddr: ownerAddr},
	}
}

func ringTrue() *bool  { found := true; return &found }
func ringFalse() *bool { found := false; return &found }

// servedMember is the reply's served_from for a plain get: said when the call asked for local,
// and when a copy answered because cluster.stale_reads allowed it, so the developer can tell a
// lagging answer; an ordinary owner get says nothing and its reply is unchanged.
func servedMember(asked bool, doc Document) string {
	if asked || doc.Served == servedLocal {
		return doc.Served
	}
	return ""
}

// read answers a get, a peek, a store list, count or get, or a lease_get. A document that is not there is found=false, not an
// error: the table of entry-ring-client-wire.md section 2 lists get under found, and a
// driver turns the miss into its typed not-found.
func (s *ringServer) read(ctx context.Context, ref bucketRef, read RingRead) (RingReply, error) {
	switch read.Class {
	case RingClassGet, RingClassGetFields:
		var project []uint16
		if read.Class == RingClassGetFields {
			var err error
			if project, err = ref.projection(read.Fields); err != nil {
				return RingReply{}, err
			}
		}
		if err := checkExpandNearFields(ref, read.Fields, read.Options.Expand); err != nil {
			return RingReply{}, err
		}
		doc, err := s.node.readDocumentFrom(ctx, ref, read.EncID, project, read.Options.Local)
		var coded *cluster.Error
		if errors.As(err, &coded) && coded.Code == cluster.CodeNotFound {
			return RingReply{Found: ringFalse()}, nil
		}
		if err != nil {
			return RingReply{}, fmt.Errorf("reading from %s.%s: %w", ref.db, ref.name, err)
		}
		if len(read.Options.Expand) > 0 {
			return s.expandGet(ctx, ref, read.Options, doc.Body, doc.Served)
		}
		return RingReply{Found: ringTrue(), Elements: [][]byte{doc.Body}, Served: servedMember(read.Options.Local, doc)}, nil
	case RingClassPeek:
		if err := checkPeekCount(int(read.Limit), ref.db, ref.name, fmt.Sprintf("field %d", read.FieldID)); err != nil {
			return RingReply{}, err
		}
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "peek")
		if err != nil {
			return RingReply{}, err
		}
		peeked, err := s.node.peekField(ctx, call, "", int(read.Limit))
		if err != nil {
			return RingReply{}, err
		}
		found := len(peeked.Elements) > 0
		return RingReply{Found: &found, Elements: peeked.Elements, Keys: peeked.Keys, Priorities: peeked.Priorities}, nil
	case RingClassStoreList:
		if err := refuseLocalWithoutExpand(read.Options, "a store list"); err != nil {
			return RingReply{}, err
		}
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "list")
		if err != nil {
			return RingReply{}, err
		}
		if len(read.Options.Expand) > 0 && call.engine != store.FieldHash && call.engine != store.FieldBTree {
			return RingReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondExpandStoreType, map[string]string{"type": call.engineType()})}
		}
		page, err := s.node.listFieldOf(ctx, call, "", read.After, int(read.Limit))
		if err != nil {
			return RingReply{}, err
		}
		if len(read.Options.Expand) > 0 {
			return s.expandStoreList(ctx, ref, call, read.Options, page)
		}
		found := len(page.Elements) > 0
		return RingReply{Found: &found, Elements: page.Elements, Keys: page.Keys, Priorities: page.Priorities, More: page.More, Cursor: page.Cursor}, nil
	case RingClassNearest:
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "nearest")
		if err != nil {
			return RingReply{}, err
		}
		result, err := s.node.nearestFieldOf(ctx, call, "", NearestRequest{Metric: read.Nearest.Metric, K: int(read.Nearest.K), VectorField: read.Nearest.Field, Vector: read.Nearest.Vector})
		if err != nil {
			return RingReply{}, err
		}
		found, scanned, skipped := len(result.Elements) > 0, int32(result.Scanned), int32(result.Skipped)
		return RingReply{Found: &found, Elements: result.Elements, Keys: result.Keys, Scores: result.Scores, Scanned: &scanned, Skipped: &skipped}, nil
	case RingClassAggregate:
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "aggregate")
		if err != nil {
			return RingReply{}, err
		}
		answer, err := s.node.aggregateOfRead(ctx, call, *read.Aggregate, int(read.Limit), read.After)
		if err != nil {
			return RingReply{}, err
		}
		found := true
		return RingReply{Found: &found, Aggregate: answer.Document, More: answer.More, Cursor: answer.Cursor}, nil
	case RingClassStoreGet:
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "get")
		if err != nil {
			return RingReply{}, err
		}
		got, err := s.node.getFieldOf(ctx, call, "", read.Key)
		if err != nil {
			return RingReply{}, err
		}
		if !got.Found {
			return RingReply{Found: ringFalse()}, nil
		}
		return RingReply{Found: ringTrue(), Elements: [][]byte{got.Element}}, nil
	case RingClassLeaseGet:
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "lease_get")
		if err != nil {
			return RingReply{}, err
		}
		held, err := s.node.leaseElementsOf(ctx, call, "", read.Lease)
		if err != nil {
			return RingReply{}, err
		}
		if !held.Found {
			return RingReply{Found: ringFalse()}, nil
		}
		return RingReply{Found: ringTrue(), Elements: held.Elements, Lease: read.Lease[:]}, nil
	case RingClassStoreCount:
		call, err := s.fieldOf(ref, read.EncID, read.FieldID, "count")
		if err != nil {
			return RingReply{}, err
		}
		total, err := s.node.countFieldOf(ctx, call, "")
		if err != nil {
			return RingReply{}, err
		}
		return RingReply{Count: &total}, nil
	}
	return RingReply{}, ringClassRefusal(read.Class)
}

// fieldOf resolves a store field by id and checks that its store type takes verb.
func (s *ringServer) fieldOf(ref bucketRef, encID []byte, fieldID uint16, verb string) (fieldCall, error) {
	entry, ok := entryByID(ref.table, fieldID)
	if !ok || !isStoreType(entry.Type) {
		return fieldCall{}, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
			"field": fmt.Sprintf("#%d", fieldID), "db": ref.db, "bucket": ref.name, "declared": declaredScalars(ref.table),
		})}
	}
	return storeCall{ref: ref, encID: encID, fieldID: fieldID}.takes(verb)
}

// ringVerbNames maps a wire store verb to the name verbStoreTypes and takesVerb use.
var ringVerbNames = map[byte]string{
	cluster.VerbPush: "push", cluster.VerbPop: "pop", cluster.VerbTake: "take", cluster.VerbPut: "put", cluster.VerbInc: "inc",
}

// write runs one write class. The op sent to the writer is built here from the parts of the
// client's request that are the client's (the id, the fields, the lease and the arg) over
// the owner's own header: the declaration version and schema version are the node's, and
// now, nonce and deadline are stamped by the writer, never taken from the wire.
func (s *ringServer) write(ctx context.Context, ref bucketRef, requestID [16]byte, req RingKeyedRequest, pos ringBatchPos) (RingReply, error) {
	opts, err := newWriteOptions([]WriteOption{WithRequestID(requestID)})
	if err != nil {
		return RingReply{}, err
	}
	opts.opIndex, opts.batchSize = pos.index, pos.size
	opts.deadline = time.Duration(req.Prologue.DeadlineMs) * time.Millisecond
	if err := checkDeclaredVersion(ref, req.Op); err != nil {
		return RingReply{}, err
	}
	op := ref.template()
	op.Class, op.EncID = req.Op.Class, req.Op.EncID
	switch req.Op.Class {
	case cluster.OpInsert, cluster.OpUpsert:
		return s.writeDocument(ctx, ref, op, req.Op, opts)
	case cluster.OpDelete:
		if err := ringCheckGuard(ref, req.Op); err != nil {
			return RingReply{}, err
		}
		op.Guard, op.Return = req.Op.Guard, req.Op.Return
		answer, err := s.node.send(ctx, ref, op.EncID, op, opts)
		return ringGuardedReply(ref, req.Op, answer, err)
	case cluster.OpStoreVerb:
		return s.writeVerb(ctx, ref, req.Op, opts)
	case cluster.OpClaim:
		return s.writeClaim(ctx, ref, req.Op, opts)
	case cluster.OpAck:
		call, err := s.fieldOf(ref, op.EncID, req.Op.Fields[0].FieldID, "ack")
		if err != nil {
			return RingReply{}, err
		}
		op.LeaseID, op.Fields = req.Op.LeaseID, []cluster.FieldOp{{FieldID: call.fieldID}}
		answer, err := s.node.send(ctx, ref, op.EncID, op, opts)
		return ringWriteReply(answer, err)
	case cluster.OpRelease, cluster.OpExtend:
		return s.writeLeaseChange(ctx, ref, op, req.Op, opts)
	case cluster.OpUpdate:
		return s.writeUpdate(ctx, ref, op, req.Op, opts)
	}
	return RingReply{}, ringClassRefusal(req.Op.Class)
}

// checkDeclaredVersion is the client's half of the schema fence. A write carries the
// declaration version its client built the field ids from; when the bucket's table in force has
// moved on (an alter published since the client described the bucket), the ids may mean other
// things now, so the write is refused SCHEMA_NOT_CURRENT before any position is taken and the
// client learns the header again and sends it once more (ringclient keyed). The owner's own
// fence, which sees only the node's header, cannot tell, because this handler stamps that
// header itself.
func checkDeclaredVersion(ref bucketRef, sent cluster.Op) error {
	if sent.DeclVersion == ref.version {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeSchemaNotCurrent, Message: cluster.CodeSchemaNotCurrent.Text(cluster.CondClientDeclarationStale, map[string]string{
		"db": ref.db, "bucket": ref.name, "d": strconv.FormatInt(sent.DeclVersion, 10), "s": strconv.FormatInt(ref.version, 10),
	})}
}

// ringWriteReply is the reply of a write with no result of its own.
func ringWriteReply(answer WriteAnswer, err error) (RingReply, error) {
	if err != nil {
		return RingReply{}, err
	}
	copies := int32(answer.CopiesConfirmed)
	outcome := answer.Outcome.String()
	if outcome == "" {
		outcome = outcomeApplied
	}
	return RingReply{WriteOutcome: outcome, CopiesConfirmed: &copies, Position: &answer.Pos, Deduped: answer.Deduped}, nil
}

func (s *ringServer) writeDocument(ctx context.Context, ref bucketRef, op cluster.Op, sent cluster.Op, opts writeOptions) (RingReply, error) {
	if err := ringCheckDocument(ref, sent, s.referenceNamer(ctx, ref)); err != nil {
		return RingReply{}, err
	}
	op.Fields = sent.Fields
	op.EncID, op.IDValue = sent.EncID, sent.IDValue
	id := bson.RawValue{Type: bsontype.Type(sent.IDValue[0]), Value: sent.IDValue[1:]}
	if sent.Class == cluster.OpUpsert {
		op.Class = cluster.OpUpsert
		answer, err := s.node.send(ctx, ref, op.EncID, op, opts)
		reply, err := ringWriteReply(answer, err)
		reply.ID = id
		return reply, err
	}
	inserted, err := s.node.coord.Insert(ctx, InsertRequest{
		Database: ref.db, Bucket: ref.name, DBID: ref.dbID, Template: op, RequestID: opts.requestID, OpIndex: opts.opIndex, BatchSize: opts.batchSize, Deadline: opts.deadline,
	})
	if err != nil {
		return RingReply{}, ringNameDuplicate(ref, id, err)
	}
	if inserted.Held {
		// The owner ordered nothing: answering applied here acknowledged a write that was never made (BUG-0918).
		return RingReply{}, heldInsertRefusal(ref.db, ref.name, inserted.HoldCause)
	}
	if inserted.Forward {
		return RingReply{}, fmt.Errorf("inserting into %s.%s: %w", ref.db, ref.name, ErrNoPeers)
	}
	// The owner's outcome, as every other class sends it: a degraded or unvouched insert is not "applied" (BUG-0919).
	reply, err := ringWriteReply(WriteAnswer{Outcome: inserted.Outcome, CopiesConfirmed: inserted.CopiesConfirmed, Pos: inserted.Pos, Deduped: inserted.Deduped}, nil)
	reply.ID = id
	return reply, err
}

// ringNameDuplicate rewrites the owner's duplicate-_id_ refusal with the names this handler
// resolved. The owner holds ids, so its text still carries the placeholders; any other error
// passes through unchanged.
func ringNameDuplicate(ref bucketRef, id bson.RawValue, err error) error {
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeDocumentExists {
		return err
	}
	text, readErr := storedString(append([]byte{byte(id.Type)}, id.Value...))
	if readErr != nil {
		return err
	}
	return &cluster.Error{Code: cluster.CodeDocumentExists, Message: cluster.CodeDocumentExists.Text(cluster.CondDocumentExists, map[string]string{
		"id": text, "db": ref.db, "bucket": ref.name,
	})}
}

// ringCheckDocument refuses an insert or upsert whose id, or whose fields, this build
// would store wrongly: a non-string _id_, an encoding that is not the _id_'s own, a field
// the bucket does not declare as a scalar, or a value that is not a BSON value.
func ringCheckDocument(ref bucketRef, op cluster.Op, targets referenceNamer) error {
	id, err := storedString(op.IDValue)
	if err != nil {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"the _id_ of a document in %s.%s is not a string; this build takes a string _id_. Nothing was written. Send _id_ as a string", ref.db, ref.name)}
	}
	if encoded, err := store.EncodeComponent(id); err != nil || string(encoded) != string(op.EncID) {
		return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
			"the id encoding of a document in %s.%s is not the encoding of its _id_ %q. Nothing was written. Encode the key from the same _id_ you send", ref.db, ref.name, id)}
	}
	for _, field := range op.Fields {
		entry, ok := entryByID(ref.table, field.FieldID)
		if !ok || isStoreType(entry.Type) {
			return &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": fmt.Sprintf("#%d", field.FieldID), "db": ref.db, "bucket": ref.name, "declared": declaredScalars(ref.table),
			})}
		}
		if err := ringCheckTypedValue(field.Value); err != nil {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"the value of field %s in %s.%s is not a BSON value: %v. Nothing was written. Send the type byte then the BSON bytes", entry.Name, ref.db, ref.name, err)}
		}
		if err := ringCheckReferenceValue(ref, entry, field.Value, targets); err != nil {
			return err
		}
	}
	return nil
}

// referenceNamer names the bucket a reference field is declared of, for a refusal text.
type referenceNamer func(fieldID uint16) string

// ringCheckReferenceValue is W-F1: a field declared 'of' a bucket holds that bucket's _id_,
// which is text, so its value is a string or null (no reference). Whether the document exists
// is not asked: that would be a cross-owner read inside a write.
func ringCheckReferenceValue(ref bucketRef, entry machine.FieldEntry, typed []byte, targets referenceNamer) error {
	_, declared := ref.entry.Decl.ReferenceOf(entry.ID)
	if !declared || typed[0] == byte(bsontype.String) || typed[0] == byte(bsontype.Null) {
		return nil
	}
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: cluster.CodeInvalidArgument.Text(cluster.CondRefIDType, map[string]string{
		"field": entry.Name, "db": ref.db, "bucket": ref.name, "target": targets(entry.ID),
	})}
}

// ringCheckTypedValue validates a type byte then BSON bytes by wrapping them in a
// one-element document and running the library's bounded validator, which refuses a
// length that lies or a nesting past the ceiling; RawValue.Validate panics on both.
func ringCheckTypedValue(typed []byte) error {
	if len(typed) == 0 {
		return errors.New("it has no type byte")
	}
	doc := make([]byte, 0, len(typed)+7)
	doc = binary.LittleEndian.AppendUint32(doc, uint32(len(typed)+7))
	doc = append(doc, typed[0], 'v', 0)
	doc = append(doc, typed[1:]...)
	doc = append(doc, 0)
	_, err := bson.Validate(doc, bson.Limits{Depth: bson.MaxNestingCeiling})
	return err
}

func (s *ringServer) writeVerb(ctx context.Context, ref bucketRef, sent cluster.Op, opts writeOptions) (RingReply, error) {
	field := sent.Fields[0]
	call, err := s.fieldOf(ref, sent.EncID, field.FieldID, ringVerbNames[field.Verb])
	if err != nil {
		return RingReply{}, err
	}
	answer, err := s.node.sendVerb(ctx, call, field.Verb, field.Value, opts)
	reply, err := ringWriteReply(answer, err)
	if err == nil && field.Verb == cluster.VerbInc && len(answer.Result) > 0 {
		// An inc that asked for the element back (r) recorded it as its result.
		found := true
		reply.Found, reply.Elements = &found, [][]byte{answer.Result}
		return reply, nil
	}
	if err != nil || (field.Verb != cluster.VerbPop && field.Verb != cluster.VerbTake) {
		return reply, err
	}
	found := len(answer.Result) > 0
	reply.Found = &found
	switch {
	case !found:
	case field.Verb == cluster.VerbPop && len(field.Value) > 0:
		// A pop {n} recorded all its elements in one result (build SPEC-GAP U85-3).
		reply.Elements, err = unpackPopped(answer.Result)
		if err != nil {
			return RingReply{}, fmt.Errorf("reading the elements popped from %s.%s: %w", ref.db, ref.name, err)
		}
	default:
		reply.Elements = [][]byte{answer.Result}
	}
	return reply, nil
}

func (s *ringServer) writeClaim(ctx context.Context, ref bucketRef, sent cluster.Op, opts writeOptions) (RingReply, error) {
	field := sent.Fields[0]
	if err := checkClaimCount(int(sent.Count), ref.db, ref.name, fmt.Sprintf("field %d", field.FieldID)); err != nil {
		return RingReply{}, err
	}
	call, err := s.fieldOf(ref, sent.EncID, field.FieldID, "claim")
	if err != nil {
		return RingReply{}, err
	}
	lease, answer, err := s.node.claimField(ctx, call, int(sent.Count), opts)
	reply, err := ringWriteReply(answer, err)
	if err != nil {
		return RingReply{}, err
	}
	reply.Elements = lease.Elements
	if lease.ID != ([16]byte{}) {
		reply.Lease = lease.ID[:]
	}
	return reply, nil
}

// writeLeaseChange runs a release or an extend. The reply has no member for the state the
// lease was left in (build SPEC-GAP U67-2), so found says whether the lease was live: false
// is an expired or unknown lease, which is an answer and not an error.
func (s *ringServer) writeLeaseChange(ctx context.Context, ref bucketRef, op cluster.Op, sent cluster.Op, opts writeOptions) (RingReply, error) {
	verb := leaseClassName(sent.Class)
	call, err := s.fieldOf(ref, op.EncID, sent.Fields[0].FieldID, verb)
	if err != nil {
		return RingReply{}, err
	}
	op.LeaseID, op.Until, op.Fields = sent.LeaseID, sent.Until, []cluster.FieldOp{{FieldID: call.fieldID}}
	change, answer, err := s.node.leaseChange(ctx, call, op, opts)
	reply, err := ringWriteReply(answer, err)
	if err != nil {
		return RingReply{}, err
	}
	live := change.State == store.LeaseReleased || change.State == store.LeaseExtended
	reply.Found = &live
	return reply, err
}

// writeUpdate runs an update. The ring names fields by id, so the checks that can name a
// field by its declared name are made here, before the owner's own: a store field or an
// undeclared id is FIELD_TYPE, and an operation this build does not take is refused by name.
func (s *ringServer) writeUpdate(ctx context.Context, ref bucketRef, op cluster.Op, sent cluster.Op, opts writeOptions) (RingReply, error) {
	for _, field := range sent.Fields {
		entry, ok := entryByID(ref.table, field.FieldID)
		if !ok || isStoreType(entry.Type) {
			name := fmt.Sprintf("#%d", field.FieldID)
			if ok {
				name = entry.Name
			}
			return RingReply{}, &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": name, "db": ref.db, "bucket": ref.name, "declared": declaredScalars(ref.table),
			})}
		}
	}
	if reason := updateOpsRefusal(sent.Fields, ref.fieldWord); reason != "" {
		return RingReply{}, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf("update of %s.%s: %s", ref.db, ref.name, reason)}
	}
	if err := ref.renameRefusal(sent.Fields); err != nil {
		return RingReply{}, err
	}
	if err := ringCheckGuard(ref, sent); err != nil {
		return RingReply{}, err
	}
	if err := ref.referenceRefusal(sent.Fields, s.referenceNamer(ctx, ref)); err != nil {
		return RingReply{}, err
	}
	op.Fields = sent.Fields
	op.Guard, op.Return = sent.Guard, sent.Return
	answer, err := s.node.send(ctx, ref, op.EncID, op, opts)
	return ringGuardedReply(ref, sent, answer, err)
}

// ringCheckGuard is the static check of an update's or delete's guard, made where the
// declaration's names are known: a term on a store field or an undeclared id is FIELD_TYPE,
// and a value that is not a BSON value is refused before the owner reads anything.
func ringCheckGuard(ref bucketRef, sent cluster.Op) error {
	for _, term := range sent.Guard {
		entry, ok := entryByID(ref.table, term.FieldID)
		if !ok || isStoreType(entry.Type) {
			name := fmt.Sprintf("#%d", term.FieldID)
			if ok {
				name = entry.Name
			}
			return &cluster.Error{Code: cluster.CodeFieldType, Message: cluster.CodeFieldType.Text(cluster.CondFieldNotDeclared, map[string]string{
				"field": name, "db": ref.db, "bucket": ref.name, "declared": declaredScalars(ref.table),
			}) + " A guard compares scalar fields; to act on a store, claim or ack its elements"}
		}
		if err := ringCheckTypedValue(term.Value); err != nil {
			return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"the guard on field %s in %s.%s is not a BSON value: %v. Nothing was written. Send the type byte then the BSON bytes", entry.Name, ref.db, ref.name, err)}
		}
	}
	return nil
}

// ringGuardedReply is ringWriteReply for an update or delete that may carry a guard or a
// return: a guard that did not hold is a successful reply with no position and the current
// document, and an applied write or a deduped retry carries the document it was asked for.
func ringGuardedReply(ref bucketRef, sent cluster.Op, answer WriteAnswer, err error) (RingReply, error) {
	if err != nil {
		return RingReply{}, namedNotFound(ref, sent, err)
	}
	if answer.NotApplied {
		document, err := ringScalarsBSON(ref, answer.Result)
		if err != nil {
			return RingReply{}, err
		}
		field := fmt.Sprintf("#%d", answer.GuardField)
		if entry, ok := entryByID(ref.table, answer.GuardField); ok {
			field = entry.Name
		}
		return RingReply{WriteOutcome: outcomeNotApplied, Reason: reasonGuard, GuardField: field, Document: document}, nil
	}
	reply, err := ringWriteReply(answer, nil)
	if err != nil || sent.Return == cluster.ReturnNone {
		return reply, err
	}
	reply.DocumentTruncated = answer.DocumentTruncated
	scalars, _ := splitGuardResult(answer.Result)
	if len(scalars) == 0 {
		return reply, nil
	}
	if reply.Document, err = ringScalarsBSON(ref, scalars); err != nil {
		return RingReply{}, err
	}
	return reply, nil
}

// ringScalarsBSON builds the BSON document of an encoded scalar set: _id_ first from the
// header, then the declared scalars by name in field-id order. A key of a retired id or a
// store marker is left out, as a get leaves it out of its scalars.
func ringScalarsBSON(ref bucketRef, encoded []byte) ([]byte, error) {
	scalars, err := DecodeScalarSet(encoded)
	if err != nil {
		return nil, fmt.Errorf("reading the document of a %s.%s write: %w", ref.db, ref.name, err)
	}
	var elements []byte
	if header := scalars[headerScalarID]; len(header) > 0 {
		elements = appendElement(elements, "_id_", header)
	}
	ids := make([]uint16, 0, len(scalars))
	for id := range scalars {
		if id >= 2 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		entry, live := entryWithID(ref.table, id)
		if !live || isStoreType(entry.Type) || len(scalars[id]) == 0 {
			continue
		}
		elements = appendElement(elements, entry.Name, scalars[id])
	}
	out := binary.LittleEndian.AppendUint32(make([]byte, 0, len(elements)+5), uint32(len(elements)+5))
	return append(append(out, elements...), 0x00), nil
}

// namedNotFound rewrites the owner's NOT_FOUND of an update or delete, which can only name
// ids, in the words the developer used: their database, bucket and document id, and a fix
// command they can paste. Any other error comes back as it was.
func namedNotFound(ref bucketRef, sent cluster.Op, err error) error {
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeNotFound || (sent.Class != cluster.OpUpdate && sent.Class != cluster.OpDelete) {
		return err
	}
	id, decodeErr := store.DecodeStringID(sent.EncID)
	if decodeErr != nil {
		return err
	}
	where := fmt.Sprintf("document %q does not exist in bucket %q of database %q", id, ref.name, ref.db)
	if sent.Class == cluster.OpDelete {
		return &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf(
			"%s, so there is nothing to delete under the guard. Nothing was changed. Check the id with db.%s.list({limit: 10}), or delete without 'if'", where, ref.name)}
	}
	return &cluster.Error{Code: cluster.CodeNotFound, Message: fmt.Sprintf(
		"%s, so it cannot be updated. Nothing was changed. Insert it with db.%s.insert({_id_: %q, …}), or write it with db.%s.upsert({_id_: %q, …})", where, ref.name, id, ref.name, id)}
}
