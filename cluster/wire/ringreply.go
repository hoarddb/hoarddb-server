package wire

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

const (
	// ringLeaseLen is the lease id of a claim. The note's section 2 says 24 bytes,
	// conventions section 9.1 and cluster.Op say 16 ([deadline:8][nonce:8]); the
	// conventions are the authority (build SPEC-GAP U65-6).
	ringLeaseLen = 16
	// ringMaxNodeText bounds served_by and the node and address strings of an
	// error detail.
	ringMaxNodeText = 256
	// ringReplyDepth is the nesting an id value may use; the rest of the reply is flat.
	ringReplyDepth = 4
)

// The write outcomes of conventions section 17.
const (
	outcomeApplied         = "applied"
	outcomeAppliedDegraded = "applied_degraded"
	// outcomeNotApplied is the owner refusing the write because its guard did not hold:
	// nothing was written and no position was taken.
	outcomeNotApplied = "not_applied"
	outcomeUnknown    = "unknown"
)

// reasonGuard is the only reason a write was not applied today: a declared guard on a field
// did not hold.
const reasonGuard = "guard"

// RingReply is the RING_REPLY document, the success reply to every ring request
// (entry-ring-client-wire.md section 2). An absent member is omitted from the
// document: nil for a pointer, empty for a string, slice or RawValue, false for
// Deduped. A present zero (CopiesConfirmed 0, Found false) is written.
type RingReply struct {
	// WriteOutcome is applied, applied_degraded or unknown; empty on a read.
	WriteOutcome string
	// CopiesConfirmed is how many copies confirmed.
	CopiesConfirmed *int32
	// ServedBy is the node id that ordered the write.
	ServedBy string
	// Position is the applied record's position.
	Position *cluster.Position
	// ID is the _id_ value the insert used.
	ID bson.RawValue
	// Found is whether a pop, take, get or peek found an element or document.
	Found *bool
	// Elements are the popped, taken, peeked or claimed elements.
	Elements [][]byte
	// Keys are the element keys (hash, btree) or entry ids (heap) of Elements, in the same
	// order; absent for a fifo, lifo or blob store (build SPEC-GAP U85-2).
	Keys []string
	// Priorities are the priorities of Elements, heap only, in the same order as Keys.
	Priorities []int64
	// Scores are the scores of Elements of a nearest, best first, one per element; absent on every other reply.
	Scores []float64
	// Scanned and Skipped are a nearest's counts of elements scored and elements without the vector field;
	// both are present on a nearest reply (a present zero is written) and absent on every other.
	Scanned, Skipped *int32
	// Entries is a list page: the records of queries Formats, concatenated, each
	// [slot:2 BE][uvarint idlen][encID][uvarint bodylen][body]. Read it with
	// cluster.DecodeEntries.
	Entries []byte
	// Lease is the 16-byte lease id of a claim.
	Lease []byte
	// Count is a store count's number of elements; a present zero is written.
	Count *int64
	// More is true when another page of a store list exists.
	More bool
	// Cursor is the store list resume point, at most ringMaxAfter bytes.
	Cursor []byte
	// Deduped is true when the answer came from the dedupe window.
	Deduped bool
	// Reason is why a write was not applied; "guard" today.
	Reason string
	// GuardField is the declared name of the first guarded field that differed.
	GuardField string
	// Document is a BSON document of the document's scalar fields, _id_ first: the document
	// as applied or as it was when a return was asked, or the current document when a guard
	// refused the write.
	Document []byte
	// DocumentTruncated is a deduped retry whose recorded document was too large to repeat;
	// the caller runs a get.
	DocumentTruncated bool
	// Epoch and StateSeq are the node's view, so a driver can see it is stale.
	Epoch, StateSeq *int64
	// Outcome is the whole batch's outcome on a RING_BATCH reply: ok when every operation
	// was answered with a reply, partial when any was answered with an error. Empty on the
	// reply to every other request.
	Outcome string
	// Answers are the RING_BATCH answers, exactly one per operation, in request order. Empty
	// on the reply to every other request.
	Answers []RingAnswer
	// Expanded are the far documents an expanded read returned, one per expansion element,
	// each a whole BSON document.
	Expanded [][]byte
	// ExpandFiltered is how many expanded far documents a where excluded; nil when no
	// expansion carried a where.
	ExpandFiltered *int32
	// Served is which copy answered: "local", "owner", or empty for a read that expanded
	// nothing.
	Served string
	// Aggregate is the BSON document an aggregate read answers with: {sum}, {values: [{by, sum, n}]},
	// {values: [{by, n}]} or {aggregates: [...]}. Absent on every other reply.
	Aggregate []byte
}

// RingAnswer is one operation's answer inside a RING_BATCH reply: exactly one of Reply, the
// reply the operation would have had as a lone RING_KEYED, or Error, the OpError payload it
// would have had.
type RingAnswer struct {
	// Reply is the operation's reply; nil when the operation was refused.
	Reply *RingReply
	// Error is the operation's refusal; nil when it was answered.
	Error *RingError
}

const (
	// ringBatchOK and ringBatchPartial are the batch outcomes. The words are the legacy BATCH's
	// (batch-request-spec.md section 4.1) so one client reads both.
	ringBatchOK      = "ok"
	ringBatchPartial = "partial"
)

// The wire document. Field order is the note's table and is the canonical byte layout.
type ringReplyDoc struct {
	WriteOutcome      string        `bson:"write_outcome,omitempty"`
	CopiesConfirmed   *int32        `bson:"copies_confirmed,omitempty"`
	ServedBy          string        `bson:"served_by,omitempty"`
	Position          string        `bson:"position,omitempty"`
	ID                bson.RawValue `bson:"id,omitempty"`
	Found             *bool         `bson:"found,omitempty"`
	Elements          [][]byte      `bson:"elements,omitempty"`
	Keys              []string      `bson:"keys,omitempty"`
	Priorities        []int64       `bson:"priorities,omitempty"`
	Scores            []float64     `bson:"scores,omitempty"`
	Scanned           *int32        `bson:"scanned,omitempty"`
	Skipped           *int32        `bson:"skipped,omitempty"`
	Entries           []byte        `bson:"entries,omitempty"`
	Lease             []byte        `bson:"lease,omitempty"`
	Count             *int64        `bson:"count,omitempty"`
	More              bool          `bson:"more,omitempty"`
	Cursor            []byte        `bson:"cursor,omitempty"`
	Deduped           bool          `bson:"deduped,omitempty"`
	Reason            string        `bson:"reason,omitempty"`
	GuardField        string        `bson:"guard_field,omitempty"`
	Document          []byte        `bson:"document,omitempty"`
	DocumentTruncated bool          `bson:"document_truncated,omitempty"`
	Epoch             *int64        `bson:"epoch,omitempty"`
	StateSeq          *int64        `bson:"state_seq,omitempty"`
	Outcome           string        `bson:"outcome,omitempty"`
	Answers           []ringAnswer  `bson:"answers,omitempty"`
	Expanded          [][]byte      `bson:"expanded,omitempty"`
	ExpandFiltered    *int32        `bson:"expand_filtered,omitempty"`
	Served            string        `bson:"served_from,omitempty"`
	Aggregate         []byte        `bson:"aggregate,omitempty"`
}

// ringAnswer is the wire form of a RingAnswer: the EncodeRingReply or EncodeRingError bytes,
// unchanged, so a client decodes a batch with the two decoders it already has.
type ringAnswer struct {
	Reply []byte `bson:"reply,omitempty"`
	Error []byte `bson:"error,omitempty"`
}

// checkRingBatchReply holds the batch members to their one form: an outcome and between 1 and
// 255 answers go together, the outcome says whether any answer is an error, an answer is a
// reply or an error and never both, and an answer is a lone reply (no batch inside a batch).
// Only epoch and state_seq may sit beside them.
func checkRingBatchReply(reply RingReply) error {
	if reply.Outcome == "" && len(reply.Answers) == 0 {
		return nil
	}
	if len(reply.Answers) == 0 || len(reply.Answers) > maxBatchOps {
		return ringRefusal("a batch reply carries 1 to %d answers and this one has %d", maxBatchOps, len(reply.Answers))
	}
	refused := false
	for index, answer := range reply.Answers {
		switch {
		case (answer.Reply == nil) == (answer.Error == nil):
			return ringRefusal("answer %d of a batch reply is a reply or an error, exactly one", index)
		case answer.Reply != nil && (answer.Reply.Outcome != "" || len(answer.Reply.Answers) != 0):
			return ringRefusal("answer %d of a batch reply is itself a batch reply; a batch does not nest", index)
		}
		refused = refused || answer.Error != nil
	}
	want := ringBatchOK
	if refused {
		want = ringBatchPartial
	}
	if reply.Outcome != want {
		return ringRefusal("a batch reply with %d answers, %d of them errors, has outcome %q, want %q", len(reply.Answers), ringCountErrors(reply.Answers), reply.Outcome, want)
	}
	bare := RingReply{Outcome: reply.Outcome, Answers: reply.Answers, Epoch: reply.Epoch, StateSeq: reply.StateSeq}
	if !reflect.DeepEqual(bare, reply) {
		return ringRefusal("a batch reply carries answers, epoch and state_seq and no member of a lone reply")
	}
	return nil
}

func ringCountErrors(answers []RingAnswer) int {
	count := 0
	for _, answer := range answers {
		if answer.Error != nil {
			count++
		}
	}
	return count
}

func checkRingReply(reply RingReply) error {
	if err := checkRingBatchReply(reply); err != nil {
		return err
	}
	switch reply.WriteOutcome {
	case "", outcomeApplied, outcomeAppliedDegraded, outcomeNotApplied, outcomeUnknown:
	default:
		return ringRefusal("write_outcome %q is not one of %s, %s, %s or %s (conventions section 17)", reply.WriteOutcome, outcomeApplied, outcomeAppliedDegraded, outcomeNotApplied, outcomeUnknown)
	}
	if err := checkRingReplyGuard(reply); err != nil {
		return err
	}
	if reply.CopiesConfirmed != nil && *reply.CopiesConfirmed < 0 {
		return ringRefusal("copies_confirmed is %d and a count of copies is 0 or more", *reply.CopiesConfirmed)
	}
	if len(reply.ServedBy) > ringMaxNodeText {
		return ringRefusal("served_by is %d bytes and a node id is at most %d", len(reply.ServedBy), ringMaxNodeText)
	}
	if reply.Position != nil && reply.Position.Epoch > uint64(1)<<63-1 {
		return ringRefusal("position %s has an epoch above the BSON int64 range", reply.Position)
	}
	if !reply.ID.IsZero() && len(reply.ID.Value) == 0 {
		return ringRefusal("id has type 0x%02x and no value bytes", byte(reply.ID.Type))
	}
	if len(reply.Lease) != 0 && len(reply.Lease) != ringLeaseLen {
		return ringRefusal("lease is %d bytes and a lease id is %d: [deadline:8][nonce:8]", len(reply.Lease), ringLeaseLen)
	}
	if reply.Count != nil && *reply.Count < 0 {
		return ringRefusal("count is %d and a count of elements is 0 or more", *reply.Count)
	}
	if err := checkReplyKeys(reply); err != nil {
		return err
	}
	if err := checkNearestCounts(reply); err != nil {
		return err
	}
	if len(reply.Entries) > 0 {
		if _, err := cluster.DecodeEntries(reply.Entries); err != nil {
			return ringRefusal("entries are not a list page: %v", err)
		}
	}
	if len(reply.Cursor) > ringMaxAfter {
		return ringRefusal("cursor is %d bytes and a store list resume point is at most %d", len(reply.Cursor), ringMaxAfter)
	}
	if reply.Epoch != nil && *reply.Epoch < 0 {
		return ringRefusal("epoch is %d and an epoch counts from 0", *reply.Epoch)
	}
	if reply.StateSeq != nil && *reply.StateSeq < 0 {
		return ringRefusal("state_seq is %d and a state sequence counts from 0", *reply.StateSeq)
	}
	if err := checkExpandedReply(reply); err != nil {
		return err
	}
	return nil
}

// checkExpandedReply holds the expansion members to their one form: every expanded item and the
// aggregate are a whole BSON document, the filtered count is 0 or more, and served names one of the two copies
// that can answer an expanded read.
func checkExpandedReply(reply RingReply) error {
	for index, doc := range reply.Expanded {
		if _, err := bson.Validate(doc, bson.Limits{Depth: bson.MaxNestingCeiling, Size: maxSubScanBody}); err != nil {
			return ringRefusal("expanded[%d] is not a valid BSON document: %v", index, err)
		}
	}
	if len(reply.Aggregate) > 0 {
		if _, err := bson.Validate(reply.Aggregate, bson.Limits{Depth: bson.MaxNestingCeiling, Size: maxSubScanBody}); err != nil {
			return ringRefusal("aggregate is not a valid BSON document: %v", err)
		}
	}
	if reply.ExpandFiltered != nil && *reply.ExpandFiltered < 0 {
		return ringRefusal("expand_filtered is %d and a count of filtered documents is 0 or more", *reply.ExpandFiltered)
	}
	switch reply.Served {
	case "", "local", "owner":
		return nil
	}
	return ringRefusal("served_from is %q and an expanded read is served by the local copy or the owner; send \"local\", \"owner\" or leave it out", reply.Served)
}

func ringReplyDocOf(reply RingReply) ringReplyDoc {
	doc := ringReplyDoc{
		WriteOutcome:      reply.WriteOutcome,
		CopiesConfirmed:   reply.CopiesConfirmed,
		ServedBy:          reply.ServedBy,
		ID:                reply.ID,
		Found:             reply.Found,
		Elements:          reply.Elements,
		Keys:              reply.Keys,
		Priorities:        reply.Priorities,
		Scores:            reply.Scores,
		Scanned:           reply.Scanned,
		Skipped:           reply.Skipped,
		Entries:           reply.Entries,
		Lease:             reply.Lease,
		Count:             reply.Count,
		More:              reply.More,
		Cursor:            reply.Cursor,
		Deduped:           reply.Deduped,
		Reason:            reply.Reason,
		GuardField:        reply.GuardField,
		Document:          reply.Document,
		DocumentTruncated: reply.DocumentTruncated,
		Epoch:             reply.Epoch,
		StateSeq:          reply.StateSeq,
		Outcome:           reply.Outcome,
		Expanded:          reply.Expanded,
		ExpandFiltered:    reply.ExpandFiltered,
		Served:            reply.Served,
		Aggregate:         reply.Aggregate,
	}
	if reply.Position != nil {
		doc.Position = reply.Position.String()
	}
	return doc
}

// ringReplyDocWithAnswers is ringReplyDocOf and also encodes the batch answers, which can fail.
func ringReplyDocWithAnswers(reply RingReply) (ringReplyDoc, error) {
	doc := ringReplyDocOf(reply)
	for index, answer := range reply.Answers {
		var wire ringAnswer
		var err error
		if answer.Reply != nil {
			wire.Reply, err = EncodeRingReply(*answer.Reply)
		} else {
			wire.Error, err = EncodeRingError(*answer.Error)
		}
		if err != nil {
			return ringReplyDoc{}, fmt.Errorf("answer %d of the batch reply: %w", index, err)
		}
		doc.Answers = append(doc.Answers, wire)
	}
	return doc, nil
}

// EncodeRingReply returns the RING_REPLY document for reply. It refuses a reply
// DecodeRingReply would not accept, with INVALID_ARGUMENT.
func EncodeRingReply(reply RingReply) ([]byte, error) {
	if err := checkRingReply(reply); err != nil {
		return nil, err
	}
	doc, err := ringReplyDocWithAnswers(reply)
	if err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding a ring reply: %w", err)
	}
	// A frame's length counts its opcode and request id as well as the payload.
	if room := maxSubScanBody - 1 - transport.RequestIDLen; len(raw) > room {
		return nil, ringRefusal("a ring reply would be %d bytes and a frame holds at most %d. Ask for fewer elements", len(raw), room)
	}
	return raw, nil
}

// DecodeRingReply reads a RING_REPLY document written by EncodeRingReply. It never
// panics: the document is validated (frame ceiling, depth) before it is unpacked, and it is accepted only if it re-encodes to the same bytes (no
// reordered, duplicate, extra or widened member, no explicit false or empty
// value where absence is the form).
func DecodeRingReply(raw []byte) (RingReply, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringReplyDepth, Size: maxSubScanBody}); err != nil {
		return RingReply{}, ringRefusal("a ring reply is not a valid BSON document: %v", err)
	}
	var doc ringReplyDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingReply{}, ringRefusal("a ring reply is not a valid BSON document of its kind: %v", err)
	}
	reply := RingReply{
		WriteOutcome:      doc.WriteOutcome,
		CopiesConfirmed:   doc.CopiesConfirmed,
		ServedBy:          doc.ServedBy,
		ID:                doc.ID,
		Found:             doc.Found,
		Elements:          doc.Elements,
		Keys:              doc.Keys,
		Priorities:        doc.Priorities,
		Scores:            doc.Scores,
		Scanned:           doc.Scanned,
		Skipped:           doc.Skipped,
		Entries:           doc.Entries,
		Lease:             doc.Lease,
		Count:             doc.Count,
		More:              doc.More,
		Cursor:            doc.Cursor,
		Deduped:           doc.Deduped,
		Reason:            doc.Reason,
		GuardField:        doc.GuardField,
		Document:          doc.Document,
		DocumentTruncated: doc.DocumentTruncated,
		Epoch:             doc.Epoch,
		StateSeq:          doc.StateSeq,
		Outcome:           doc.Outcome,
		Expanded:          doc.Expanded,
		ExpandFiltered:    doc.ExpandFiltered,
		Served:            doc.Served,
		Aggregate:         doc.Aggregate,
	}
	for index, wire := range doc.Answers {
		answer, err := decodeRingAnswer(wire)
		if err != nil {
			return RingReply{}, fmt.Errorf("answer %d of the batch reply: %w", index, err)
		}
		reply.Answers = append(reply.Answers, answer)
	}
	if doc.Position != "" {
		position, err := cluster.ParsePosition(doc.Position)
		if err != nil || position.String() != doc.Position {
			return RingReply{}, ringRefusal("position %q is not epoch:seq in plain decimal, for example 41:98213", doc.Position)
		}
		reply.Position = &position
	}
	if err := checkRingReply(reply); err != nil {
		return RingReply{}, err
	}
	againDoc, err := ringReplyDocWithAnswers(reply)
	if err != nil {
		return RingReply{}, ringRefusal("a ring reply holds a value that cannot be read back: %v", err)
	}
	again, err := bson.Marshal(againDoc)
	if err != nil {
		return RingReply{}, ringRefusal("a ring reply holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingReply{}, ringRefusal("a ring reply does not have the layout of entry-ring-client-wire.md section 2 (members in order, none duplicated or extra, the stated types, absent rather than empty or false)")
	}
	return reply, nil
}

// decodeRingAnswer reads one batch answer with the two decoders a lone reply and a lone
// error already have.
func decodeRingAnswer(wire ringAnswer) (RingAnswer, error) {
	switch {
	case len(wire.Reply) != 0 && len(wire.Error) == 0:
		reply, err := DecodeRingReply(wire.Reply)
		return RingAnswer{Reply: &reply}, err
	case len(wire.Error) != 0 && len(wire.Reply) == 0:
		refusal, err := DecodeRingError(wire.Error)
		return RingAnswer{Error: &refusal}, err
	}
	return RingAnswer{}, ringRefusal("an answer is a reply or an error, exactly one")
}

// checkRingReplyGuard holds the guard members to their one form: a not_applied reply says why
// and has neither a position nor copies, reason goes only with not_applied, guard_field only
// with a reason, a document only on a reply that has a document or refused one, and
// document_truncated only on a deduped retry that had no document to repeat.
func checkRingReplyGuard(reply RingReply) error {
	if reply.WriteOutcome == outcomeNotApplied {
		if reply.Reason == "" {
			return ringRefusal("write_outcome not_applied carries no reason; send reason %q with it, because a guard that did not hold is a successful reply with no position", reasonGuard)
		}
		if reply.Position != nil || reply.CopiesConfirmed != nil {
			return ringRefusal("write_outcome not_applied carries a position or copies_confirmed; send neither, because the owner refused the write and took no position")
		}
	} else if reply.Reason != "" {
		return ringRefusal("reason %q is set on a reply with write_outcome %q; send reason only with write_outcome not_applied, where the reply returns the refused write's document instead of a position", reply.Reason, reply.WriteOutcome)
	}
	if reply.GuardField != "" {
		if reply.Reason == "" {
			return ringRefusal("guard_field %q is set with no reason; send guard_field only with reason, naming the first guarded field that differed", reply.GuardField)
		}
		if len(reply.GuardField) > ringMaxNodeText {
			return ringRefusal("guard_field is %d bytes and a field name is at most %d; send the field's declared name", len(reply.GuardField), ringMaxNodeText)
		}
	}
	if len(reply.Document) > 0 {
		switch reply.WriteOutcome {
		case outcomeApplied, outcomeAppliedDegraded, outcomeNotApplied, outcomeUnknown:
		default:
			return ringRefusal("document is set on a reply with write_outcome %q; send a document only on an applied, applied_degraded, not_applied or unknown reply", reply.WriteOutcome)
		}
		if _, err := bson.Validate(reply.Document, bson.Limits{Depth: ringReplyDepth, Size: maxSubScanBody}); err != nil {
			return ringRefusal("document is not a valid BSON document: %v; send the document's scalar fields with _id_ first", err)
		}
	}
	if reply.DocumentTruncated {
		if !reply.Deduped {
			return ringRefusal("document_truncated is set without deduped; send document_truncated only on a deduped retry whose recorded document was too large to repeat, and the caller runs a get")
		}
		if len(reply.Document) > 0 {
			return ringRefusal("document_truncated is set with a document; send document_truncated and no document, and the caller runs a get")
		}
	}
	return nil
}

// checkReplyKeys holds the keys and priorities members to their one form: parallel to
// elements, keys of 1 to 1024 bytes, priorities only with keys (build SPEC-GAP U85-2).
func checkReplyKeys(reply RingReply) error {
	if len(reply.Keys) > 0 && len(reply.Keys) != len(reply.Elements) {
		return ringRefusal("keys has %d entries and elements has %d; keys is parallel to elements, one key per element", len(reply.Keys), len(reply.Elements))
	}
	for i, key := range reply.Keys {
		if len(key) < 1 || len(key) > maxVerbKeyBytes {
			return ringRefusal("keys[%d] is %d bytes and an element key or entry id is 1 to %d", i, len(key), maxVerbKeyBytes)
		}
	}
	if len(reply.Priorities) > 0 && len(reply.Priorities) != len(reply.Keys) {
		return ringRefusal("priorities has %d entries and keys has %d; priorities is parallel to keys and only a heapStore sends it", len(reply.Priorities), len(reply.Keys))
	}
	return nil
}

// checkNearestCounts holds the nearest members to their one form: scores parallel to elements,
// and scanned and skipped sent together as counts of 0 or more. Scores need the counts, so a
// reply with scores and no counts is refused rather than read as some other reply.
func checkNearestCounts(reply RingReply) error {
	if len(reply.Scores) > 0 && len(reply.Scores) != len(reply.Elements) {
		return ringRefusal("scores has %d entries and elements has %d; scores is parallel to elements, one score per element", len(reply.Scores), len(reply.Elements))
	}
	if (reply.Scanned == nil) != (reply.Skipped == nil) {
		return ringRefusal("scanned and skipped go together: a nearest reply sends both or neither")
	}
	if reply.Scanned != nil && *reply.Scanned < 0 {
		return ringRefusal("scanned is %d and a count of elements is 0 or more", *reply.Scanned)
	}
	if reply.Skipped != nil && *reply.Skipped < 0 {
		return ringRefusal("skipped is %d and a count of elements is 0 or more", *reply.Skipped)
	}
	if len(reply.Scores) > 0 && reply.Scanned == nil {
		return ringRefusal("scores without scanned and skipped are not a nearest reply")
	}
	if reply.Scanned != nil && len(reply.Scores) != len(reply.Elements) {
		return ringRefusal("scores has %d entries and elements has %d; a nearest reply sends one score per element", len(reply.Scores), len(reply.Elements))
	}
	return nil
}
