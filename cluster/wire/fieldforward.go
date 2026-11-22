package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// FieldOp names which store read a forwarded field read is.
type FieldOp byte

// The store reads a node forwards to the owner of the document (BUG-0766).
const (
	FieldOpList FieldOp = iota + 1
	FieldOpCount
	FieldOpGet
	FieldOpPeek
	FieldOpLease
	FieldOpNearest
	// FieldOpAggregate forwards an aggregate read; the owner resolves the aggregate from its own
	// declaration.
	FieldOpAggregate
)

// FieldForward is a store read sent to the owner of the document's pair. It carries only what
// the owner needs to run the same read again: the owner reads its own copy and its own
// declaration, so no element, scalar or declaration travels with the request.
type FieldForward struct {
	// Req names the document; its Hop is already counted.
	Req ReadRequest
	Op  FieldOp
	// FieldID and Engine are the field as the sending node resolved it; the owner refuses the
	// read when its own declaration says otherwise.
	FieldID uint16
	Engine  store.FieldEngine
	// After and Limit are a list's cursor and page size.
	After []byte
	Limit int
	// Key is a get's element key or heap entry id.
	Key string
	// Count is how many elements a peek returns.
	Count int
	// Lease is the lease a lease_get reads.
	Lease [16]byte
	// Metric, K, VectorField and Vector are a nearest's query: how to score, how many to keep,
	// the element field that holds the vector, and the query vector (4 to 16384 bytes).
	Metric      store.NearestMetric
	K           int
	VectorField string
	Vector      []byte
	// Aggregate is an aggregate read's kind and paths; Limit and After page a by form.
	Aggregate RingAggregateRead
}

// FieldAnswer is the owner's answer to a FieldForward; only the member of its Op is set.
type FieldAnswer struct {
	// Page answers a list and a peek.
	Page FieldElements
	// Total answers a count.
	Total int64
	// Element and Found answer a get.
	Element []byte
	Found   bool
	// Lease answers a lease_get.
	Lease LeaseRead
	// Nearest answers a nearest.
	Nearest NearestAnswer
	// Aggregate answers an aggregate read.
	Aggregate AggregateAnswer
}

// forwardedReplyBytes is the most element bytes the owner puts in the reply of a forwarded
// store read. A peer link carries at most 16 MiB a message, half the client frame, so the
// owner answers a peer with less than it answers a client; a list then pages sooner.
const forwardedReplyBytes = 8 << 20

// replyBytes is the most element bytes one reply to req carries: the client frame's bound
// for a client, the peer link's for a read another node forwarded.
func (r *Reader) replyBytes(req ReadRequest) int {
	if req.FromNode {
		return forwardedReplyBytes
	}
	return peekReplyBytes
}

// checkForwardedField refuses a read a peer sent when the owner's declaration does not hold the
// field the peer resolved: a field dropped or re-declared under the same id would otherwise
// be read as another store. A local read resolved its field from this node's own declaration
// a moment before; the peer's was resolved from a cache this node cannot see.
func (r *Reader) checkForwardedField(req ReadRequest, field fieldTarget, admitted ReadVerdict) error {
	if !req.FromNode {
		return nil
	}
	entry, found := entryByID(admitted.Table, field.FieldID)
	if engine, isStore := engineOfType(entry.Type); found && isStore && engine == field.Engine {
		return nil
	}
	return r.cfg.Gate.refusal(cluster.CodeSchemaNotCurrent, req)
}

// checkNearestQuery refuses a forwarded nearest query outside the bounds the ring edge holds
// every nearest to, so neither end of a peer link accepts what the other would not send. A vector
// with a NaN or infinite value is the owner's store to refuse; the length is all the codec sees.
func checkNearestQuery(metric store.NearestMetric, k int, vectorField string, vectorBytes int) error {
	switch {
	case metric < store.NearestCosine || metric > store.NearestL2:
		return peerRefusal("a forwarded nearest names metric %d, which is not 1 (cosine), 2 (dot) or 3 (l2)", metric)
	case k < 1 || k > ringMaxPeek:
		return peerRefusal("a forwarded nearest asks for k %d, which must be 1 to %d", k, ringMaxPeek)
	case len(vectorField) < 1 || len(vectorField) > ringMaxNearestName || !utf8.ValidString(vectorField):
		return peerRefusal("a forwarded nearest names its vector field in %d bytes of valid UTF-8, which must be 1 to %d", len(vectorField), ringMaxNearestName)
	case vectorBytes < 4 || vectorBytes > ringMaxNearestVector || vectorBytes%4 != 0:
		return peerRefusal("a forwarded nearest vector is %d bytes, which must be a multiple of 4 from 4 to %d", vectorBytes, ringMaxNearestVector)
	}
	return nil
}

// ServeFieldForward answers a store read a peer forwarded: the same Reader entry the node's
// own clients use, so the owner admits, parks and confirms it as any other read.
func (r *Reader) ServeFieldForward(ctx context.Context, forwarded FieldForward) (FieldAnswer, error) {
	read := FieldRead{Request: forwarded.Req, FieldID: forwarded.FieldID, Engine: forwarded.Engine, After: forwarded.After, Limit: forwarded.Limit}
	switch forwarded.Op {
	case FieldOpList:
		page, err := r.ListField(ctx, read)
		return FieldAnswer{Page: page}, err
	case FieldOpCount:
		total, err := r.CountField(ctx, read)
		return FieldAnswer{Total: total}, err
	case FieldOpGet:
		element, found, err := r.GetField(ctx, read, forwarded.Key)
		return FieldAnswer{Element: element, Found: found}, err
	case FieldOpPeek:
		page, err := r.Peek(ctx, PeekField{Request: forwarded.Req, FieldID: forwarded.FieldID, Engine: forwarded.Engine, Count: forwarded.Count})
		return FieldAnswer{Page: page}, err
	case FieldOpLease:
		lease, err := r.LeaseElements(ctx, LeaseField{Request: forwarded.Req, FieldID: forwarded.FieldID, Engine: forwarded.Engine, Lease: forwarded.Lease})
		return FieldAnswer{Lease: lease}, err
	case FieldOpAggregate:
		answer, err := r.AggregateOf(ctx, read, forwarded.Aggregate, forwarded.Limit, forwarded.After)
		return FieldAnswer{Aggregate: answer}, err
	case FieldOpNearest:
		// MaxElements is left zero: the owner's own ceiling applies, never the sender's.
		hits, err := r.Nearest(ctx, read, store.NearestRequest{Metric: forwarded.Metric, K: forwarded.K, Field: forwarded.VectorField, Vector: forwarded.Vector})
		return FieldAnswer{Nearest: hits}, err
	}
	return FieldAnswer{}, peerRefusal("a forwarded store read names operation %d, which is not list, count, get, peek, lease_get, nearest or aggregate", forwarded.Op)
}

// ForwardFieldRead implements ReadForwarder.
func (l *peerLinks) ForwardFieldRead(ctx context.Context, owner string, forwarded FieldForward) (FieldAnswer, error) {
	req := forwarded.Req
	body, err := EncodeForwardReadBody(ForwardReadBody{Req: req, Field: &forwarded})
	if err != nil {
		return FieldAnswer{}, err
	}
	reply, err := l.call(ctx, owner, peer.LaneForward, l.envelope(transport.KindForwardRead, l.hello().Epoch, req.Slot, req.DBID), body, transport.KindForwardReadReply)
	if err != nil {
		return FieldAnswer{}, unreachableOwner(ctx, cluster.CondForwardReadUnreachable, owner, fmt.Errorf("forwarding the read of %s to %s: %w", req.idText(), owner, err))
	}
	answer, err := DecodeFieldAnswer(forwarded.Op, reply.Body)
	return answer, clientFacing(err, owner)
}

// serveForwardField is serveForwardRead for a store read.
func (n *Node) serveForwardField(ctx context.Context, env transport.Envelope, forwarded FieldForward) (*transport.Message, error) {
	forwarded.Req.Slot, forwarded.Req.DBID, forwarded.Req.FromNode, forwarded.Req.FrameEpoch = env.Slot, env.DBID, true, env.Epoch
	// A peer's read may be the first touch of the pair since a restart: it opens the restart
	// hold like a local one (BUG-0743).
	n.own.Acquire(env.Slot, env.DBID)
	answer, err := n.reader.ServeFieldForward(ctx, forwarded)
	var refusal *cluster.Error
	if errors.As(err, &refusal) {
		return n.reply(transport.KindForwardReadReply, env, EncodeForwardRefusal(refusal)), nil
	}
	if err != nil {
		return nil, err
	}
	raw, err := EncodeFieldAnswer(forwarded.Op, answer)
	if err != nil {
		return nil, err
	}
	return n.reply(transport.KindForwardReadReply, env, raw), nil
}

// The tail of a store read, after the keyed header of FORWARD_READ sub-kind 3:
// [op:1][field:2][engine:1] and then by op
//
//	list   [limit:4][afterLen:2][after]
//	count  nothing
//	get    [keyLen:2][key]
//	peek   [count:4]
//	lease  [lease:16]
//	nearest [metric:1][k:4][fieldLen:1][field][vectorLen:2][vector]
//	aggregate [kind:1][fieldLen:1][field][byLen:1][by][limit:4][afterLen:2][after]
func appendFieldForward(body []byte, forwarded FieldForward) ([]byte, error) {
	if forwarded.Limit < 0 || forwarded.Limit > 1<<31-1 || forwarded.Count < 0 || forwarded.Count > 1<<31-1 {
		return nil, peerRefusal("a forwarded store read asks for %d/%d elements, which does not fit the forward", forwarded.Limit, forwarded.Count)
	}
	if len(forwarded.After) > 1<<16-1 || len(forwarded.Key) > 1<<16-1 {
		return nil, peerRefusal("a forwarded store read carries a %d-byte cursor and a %d-byte key, which does not fit the forward", len(forwarded.After), len(forwarded.Key))
	}
	body = append(body, byte(forwarded.Op))
	body = binary.BigEndian.AppendUint16(body, forwarded.FieldID)
	body = append(body, byte(forwarded.Engine))
	switch forwarded.Op {
	case FieldOpList:
		body = binary.BigEndian.AppendUint32(body, uint32(forwarded.Limit))
		body = binary.BigEndian.AppendUint16(body, uint16(len(forwarded.After)))
		body = append(body, forwarded.After...)
	case FieldOpCount:
	case FieldOpGet:
		body = binary.BigEndian.AppendUint16(body, uint16(len(forwarded.Key)))
		body = append(body, forwarded.Key...)
	case FieldOpPeek:
		body = binary.BigEndian.AppendUint32(body, uint32(forwarded.Count))
	case FieldOpLease:
		body = append(body, forwarded.Lease[:]...)
	case FieldOpNearest:
		if err := checkNearestQuery(forwarded.Metric, forwarded.K, forwarded.VectorField, len(forwarded.Vector)); err != nil {
			return nil, err
		}
		body = append(body, byte(forwarded.Metric))
		body = binary.BigEndian.AppendUint32(body, uint32(forwarded.K))
		body = append(body, byte(len(forwarded.VectorField)))
		body = append(body, forwarded.VectorField...)
		body = binary.BigEndian.AppendUint16(body, uint16(len(forwarded.Vector)))
		body = append(body, forwarded.Vector...)
	case FieldOpAggregate:
		aggregate := forwarded.Aggregate
		if len(aggregate.Field) > 255 || len(aggregate.By) > 255 {
			return nil, peerRefusal("a forwarded aggregate read names a path over 255 bytes")
		}
		body = append(body, aggregate.Kind, byte(len(aggregate.Field)))
		body = append(body, aggregate.Field...)
		body = append(append(body, byte(len(aggregate.By))), aggregate.By...)
		body = binary.BigEndian.AppendUint32(body, uint32(forwarded.Limit))
		body = binary.BigEndian.AppendUint16(body, uint16(len(forwarded.After)))
		body = append(body, forwarded.After...)
	default:
		return nil, peerRefusal("a forwarded store read names operation %d, which is not list, count, get, peek, lease_get, nearest or aggregate", forwarded.Op)
	}
	return body, nil
}

// fieldForward reads the tail of a store read; req is the keyed header already read.
func (r *peerReader) fieldForward(req ReadRequest) (FieldForward, error) {
	forwarded := FieldForward{Req: req}
	op, err := r.u8()
	if err != nil {
		return FieldForward{}, err
	}
	forwarded.Op = FieldOp(op)
	if forwarded.FieldID, err = r.u16(); err != nil {
		return FieldForward{}, err
	}
	engine, err := r.u8()
	if err != nil {
		return FieldForward{}, err
	}
	forwarded.Engine = store.FieldEngine(engine)
	switch forwarded.Op {
	case FieldOpList:
		limit, err := r.u32()
		if err != nil {
			return FieldForward{}, err
		}
		if limit > 1<<31-1 {
			return FieldForward{}, peerRefusal("FORWARD_READ list limit %d does not fit", limit)
		}
		forwarded.Limit = int(limit)
		afterLen, err := r.u16()
		if err != nil {
			return FieldForward{}, err
		}
		after, err := r.take(int(afterLen))
		if err != nil {
			return FieldForward{}, err
		}
		if len(after) > 0 {
			forwarded.After = append([]byte(nil), after...)
		}
	case FieldOpCount:
	case FieldOpGet:
		keyLen, err := r.u16()
		if err != nil {
			return FieldForward{}, err
		}
		key, err := r.take(int(keyLen))
		if err != nil {
			return FieldForward{}, err
		}
		forwarded.Key = string(key)
	case FieldOpPeek:
		count, err := r.u32()
		if err != nil {
			return FieldForward{}, err
		}
		if count > 1<<31-1 {
			return FieldForward{}, peerRefusal("FORWARD_READ peek count %d does not fit", count)
		}
		forwarded.Count = int(count)
	case FieldOpLease:
		lease, err := r.take(16)
		if err != nil {
			return FieldForward{}, err
		}
		copy(forwarded.Lease[:], lease)
	case FieldOpNearest:
		metric, err := r.u8()
		if err != nil {
			return FieldForward{}, err
		}
		k, err := r.u32()
		if err != nil {
			return FieldForward{}, err
		}
		nameLen, err := r.u8()
		if err != nil {
			return FieldForward{}, err
		}
		name, err := r.take(int(nameLen))
		if err != nil {
			return FieldForward{}, err
		}
		vectorLen, err := r.u16()
		if err != nil {
			return FieldForward{}, err
		}
		vector, err := r.take(int(vectorLen))
		if err != nil {
			return FieldForward{}, err
		}
		if err := checkNearestQuery(store.NearestMetric(metric), int(min(k, 1<<31-1)), string(name), len(vector)); err != nil {
			return FieldForward{}, err
		}
		forwarded.Metric, forwarded.K, forwarded.VectorField, forwarded.Vector = store.NearestMetric(metric), int(k), string(name), append([]byte(nil), vector...)
	case FieldOpAggregate:
		kind, err := r.u8()
		if err != nil {
			return FieldForward{}, err
		}
		fieldLen, err := r.u8()
		if err != nil {
			return FieldForward{}, err
		}
		field, err := r.take(int(fieldLen))
		if err != nil {
			return FieldForward{}, err
		}
		byLen, err := r.u8()
		if err != nil {
			return FieldForward{}, err
		}
		by, err := r.take(int(byLen))
		if err != nil {
			return FieldForward{}, err
		}
		limit, err := r.u32()
		if err != nil {
			return FieldForward{}, err
		}
		if limit > aggregateMaxLimit {
			return FieldForward{}, peerRefusal("FORWARD_READ aggregate limit %d is over %d", limit, aggregateMaxLimit)
		}
		afterLen, err := r.u16()
		if err != nil {
			return FieldForward{}, err
		}
		after, err := r.take(int(afterLen))
		if err != nil {
			return FieldForward{}, err
		}
		forwarded.Aggregate = RingAggregateRead{Kind: kind, Field: string(field), By: string(by)}
		forwarded.Limit = int(limit)
		if len(after) > 0 {
			forwarded.After = append([]byte(nil), after...)
		}
		if err := ringCheckAggregate(RingRead{Class: RingClassAggregate, Aggregate: &forwarded.Aggregate}); err != nil {
			return FieldForward{}, peerRefusal("FORWARD_READ aggregate read is malformed: %v", err)
		}
	default:
		return FieldForward{}, peerRefusal("FORWARD_READ store read operation %d is not list, count, get, peek, lease_get, nearest or aggregate", op)
	}
	return forwarded, nil
}

// EncodeFieldAnswer is a FORWARD_READ_REPLY carrying the answer to a store read, by op:
//
//	list, peek [0][more:1][elements]{[len:4][element]} [keys]{[len:2][key]} [priorities]{[i64]} [nextLen:2][next]
//	count      [0][total:8]
//	get        [0][found:1][len:4][element]
//	lease      [0][found:1][until:8][elements]{[len:4][element]}
//	nearest    [0] the list page, then [scores]{[f64]} [scanned:8][skipped:8]
//	aggregate  [0][more:1][docLen:4][document][cursorLen:2][cursor]
//
// where each group is led by a [count:4]. An owner's refusal is EncodeForwardRefusal.
func EncodeFieldAnswer(op FieldOp, answer FieldAnswer) ([]byte, error) {
	body := []byte{forwardAnswered}
	switch op {
	case FieldOpList, FieldOpPeek:
		return appendPage(body, answer.Page)
	case FieldOpNearest:
		hits := answer.Nearest
		if len(hits.Scores) != len(hits.Elements) {
			return nil, peerRefusal("a nearest answer holds %d scores for %d elements", len(hits.Scores), len(hits.Elements))
		}
		body, err := appendPage(body, hits.FieldElements)
		if err != nil {
			return nil, err
		}
		body = binary.BigEndian.AppendUint32(body, uint32(len(hits.Scores)))
		for _, score := range hits.Scores {
			body = binary.BigEndian.AppendUint64(body, math.Float64bits(score))
		}
		if hits.Scanned < 0 || hits.Skipped < 0 {
			return nil, peerRefusal("a nearest answer scanned %d and skipped %d elements", hits.Scanned, hits.Skipped)
		}
		body = binary.BigEndian.AppendUint64(body, uint64(hits.Scanned))
		return binary.BigEndian.AppendUint64(body, uint64(hits.Skipped)), nil
	case FieldOpCount:
		body = binary.BigEndian.AppendUint64(body, uint64(answer.Total))
	case FieldOpGet:
		body = append(body, boolByte(answer.Found))
		body = binary.BigEndian.AppendUint32(body, uint32(len(answer.Element)))
		body = append(body, answer.Element...)
	case FieldOpLease:
		body = append(body, boolByte(answer.Lease.Found))
		body = binary.BigEndian.AppendUint64(body, uint64(answer.Lease.Until))
		body = appendByteGroup(body, answer.Lease.Elements)
	case FieldOpAggregate:
		if len(answer.Aggregate.Cursor) > 1<<16-1 {
			return nil, peerRefusal("an aggregate cursor of %d bytes does not fit the reply", len(answer.Aggregate.Cursor))
		}
		body = append(body, boolByte(answer.Aggregate.More))
		body = binary.BigEndian.AppendUint32(body, uint32(len(answer.Aggregate.Document)))
		body = append(body, answer.Aggregate.Document...)
		body = binary.BigEndian.AppendUint16(body, uint16(len(answer.Aggregate.Cursor)))
		body = append(body, answer.Aggregate.Cursor...)
	default:
		return nil, peerRefusal("a store read answer for operation %d, which is not list, count, get, peek, lease_get, nearest or aggregate", op)
	}
	return body, nil
}

// appendPage writes the page of a list, a peek and a nearest after the status byte:
// [more:1][elements] [keys]{[len:2][key]} [priorities]{[i64]} [nextLen:2][next].
func appendPage(body []byte, page FieldElements) ([]byte, error) {
	if len(page.Next) > 1<<16-1 {
		return nil, peerRefusal("a page cursor of %d bytes does not fit the reply", len(page.Next))
	}
	body = append(body, boolByte(page.More))
	body = appendByteGroup(body, page.Elements)
	body = binary.BigEndian.AppendUint32(body, uint32(len(page.Keys)))
	for _, key := range page.Keys {
		if len(key) > 1<<16-1 {
			return nil, peerRefusal("an element key of %d bytes does not fit the reply", len(key))
		}
		body = binary.BigEndian.AppendUint16(body, uint16(len(key)))
		body = append(body, key...)
	}
	body = binary.BigEndian.AppendUint32(body, uint32(len(page.Priorities)))
	for _, priority := range page.Priorities {
		body = binary.BigEndian.AppendUint64(body, uint64(priority))
	}
	body = binary.BigEndian.AppendUint16(body, uint16(len(page.Next)))
	return append(body, page.Next...), nil
}

func appendByteGroup(body []byte, group [][]byte) []byte {
	body = binary.BigEndian.AppendUint32(body, uint32(len(group)))
	for _, member := range group {
		body = binary.BigEndian.AppendUint32(body, uint32(len(member)))
		body = append(body, member...)
	}
	return body
}

// byteGroup reads a group written by appendByteGroup. A count larger than the bytes that
// remain is refused before anything is allocated for it.
func (r *peerReader) byteGroup() ([][]byte, error) {
	count, err := r.u32()
	if err != nil {
		return nil, err
	}
	if int64(count)*4 > int64(len(r.rest)) {
		return nil, peerRefusal("%s says %d elements and %d bytes remain", r.what, count, len(r.rest))
	}
	var group [][]byte
	for range count {
		length, err := r.u32()
		if err != nil {
			return nil, err
		}
		member, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
		if err != nil {
			return nil, err
		}
		group = append(group, append([]byte(nil), member...))
	}
	return group, nil
}

func (r *peerReader) flag() (bool, error) {
	value, err := r.u8()
	if err != nil {
		return false, err
	}
	if value > 1 {
		return false, peerRefusal("%s flag %d is not 0 or 1", r.what, value)
	}
	return value == 1, nil
}

// page reads the page appendPage wrote. Every count is checked against the bytes that remain
// before anything is allocated for it.
func (r *peerReader) page() (FieldElements, error) {
	var page FieldElements
	var err error
	if page.More, err = r.flag(); err != nil {
		return FieldElements{}, err
	}
	if page.Elements, err = r.byteGroup(); err != nil {
		return FieldElements{}, err
	}
	keys, err := r.u32()
	if err != nil {
		return FieldElements{}, err
	}
	if int64(keys)*2 > int64(len(r.rest)) {
		return FieldElements{}, peerRefusal("FORWARD_READ_REPLY says %d keys and %d bytes remain", keys, len(r.rest))
	}
	for range keys {
		length, err := r.u16()
		if err != nil {
			return FieldElements{}, err
		}
		key, err := r.take(int(length))
		if err != nil {
			return FieldElements{}, err
		}
		page.Keys = append(page.Keys, string(key))
	}
	priorities, err := r.u32()
	if err != nil {
		return FieldElements{}, err
	}
	if int64(priorities)*8 > int64(len(r.rest)) {
		return FieldElements{}, peerRefusal("FORWARD_READ_REPLY says %d priorities and %d bytes remain", priorities, len(r.rest))
	}
	for range priorities {
		priority, err := r.u64()
		if err != nil {
			return FieldElements{}, err
		}
		page.Priorities = append(page.Priorities, int64(priority))
	}
	nextLen, err := r.u16()
	if err != nil {
		return FieldElements{}, err
	}
	next, err := r.take(int(nextLen))
	if err != nil {
		return FieldElements{}, err
	}
	if len(next) > 0 {
		page.Next = append([]byte(nil), next...)
	}
	return page, nil
}

// DecodeFieldAnswer reads the FORWARD_READ_REPLY to a store read of op: the owner's answer,
// or its refusal as a *cluster.Error.
func DecodeFieldAnswer(op FieldOp, body []byte) (FieldAnswer, error) {
	r := &peerReader{rest: body, what: "FORWARD_READ_REPLY"}
	status, err := r.u8()
	if err != nil {
		return FieldAnswer{}, err
	}
	if status == forwardRefused {
		return FieldAnswer{}, decodeForwardRefusal(r)
	}
	if status != forwardAnswered {
		return FieldAnswer{}, peerRefusal("FORWARD_READ_REPLY status %d is not 0 answered or 1 refused", status)
	}
	var answer FieldAnswer
	switch op {
	case FieldOpList, FieldOpPeek:
		if answer.Page, err = r.page(); err != nil {
			return FieldAnswer{}, err
		}
	case FieldOpNearest:
		if answer.Nearest.FieldElements, err = r.page(); err != nil {
			return FieldAnswer{}, err
		}
		scores, err := r.u32()
		if err != nil {
			return FieldAnswer{}, err
		}
		if int64(scores)*8 > int64(len(r.rest)) {
			return FieldAnswer{}, peerRefusal("FORWARD_READ_REPLY says %d scores and %d bytes remain", scores, len(r.rest))
		}
		if int(scores) != len(answer.Nearest.Elements) {
			return FieldAnswer{}, peerRefusal("FORWARD_READ_REPLY says %d scores for %d elements", scores, len(answer.Nearest.Elements))
		}
		for range scores {
			bits, err := r.u64()
			if err != nil {
				return FieldAnswer{}, err
			}
			answer.Nearest.Scores = append(answer.Nearest.Scores, math.Float64frombits(bits))
		}
		scanned, err := r.u64()
		if err != nil {
			return FieldAnswer{}, err
		}
		skipped, err := r.u64()
		if err != nil {
			return FieldAnswer{}, err
		}
		if scanned > math.MaxInt64 || skipped > math.MaxInt64 {
			return FieldAnswer{}, peerRefusal("FORWARD_READ_REPLY says %d scanned and %d skipped elements, which do not fit", scanned, skipped)
		}
		answer.Nearest.Scanned, answer.Nearest.Skipped = int(scanned), int(skipped)
	case FieldOpAggregate:
		if answer.Aggregate.More, err = r.flag(); err != nil {
			return FieldAnswer{}, err
		}
		docLen, err := r.u32()
		if err != nil {
			return FieldAnswer{}, err
		}
		if int64(docLen) > int64(len(r.rest)) {
			return FieldAnswer{}, peerRefusal("FORWARD_READ_REPLY says a %d-byte aggregate document and %d bytes remain", docLen, len(r.rest))
		}
		document, err := r.take(int(docLen))
		if err != nil {
			return FieldAnswer{}, err
		}
		answer.Aggregate.Document = append([]byte(nil), document...)
		cursorLen, err := r.u16()
		if err != nil {
			return FieldAnswer{}, err
		}
		cursor, err := r.take(int(cursorLen))
		if err != nil {
			return FieldAnswer{}, err
		}
		if len(cursor) > 0 {
			answer.Aggregate.Cursor = append([]byte(nil), cursor...)
		}
	case FieldOpCount:
		total, err := r.u64()
		if err != nil {
			return FieldAnswer{}, err
		}
		answer.Total = int64(total)
	case FieldOpGet:
		if answer.Found, err = r.flag(); err != nil {
			return FieldAnswer{}, err
		}
		length, err := r.u32()
		if err != nil {
			return FieldAnswer{}, err
		}
		element, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
		if err != nil {
			return FieldAnswer{}, err
		}
		if len(element) > 0 {
			answer.Element = append([]byte(nil), element...)
		}
	case FieldOpLease:
		if answer.Lease.Found, err = r.flag(); err != nil {
			return FieldAnswer{}, err
		}
		until, err := r.u64()
		if err != nil {
			return FieldAnswer{}, err
		}
		answer.Lease.Until = int64(until)
		if answer.Lease.Elements, err = r.byteGroup(); err != nil {
			return FieldAnswer{}, err
		}
	default:
		return FieldAnswer{}, peerRefusal("a store read answer for operation %d, which is not list, count, get, peek, lease_get, nearest or aggregate", op)
	}
	return answer, r.done()
}
