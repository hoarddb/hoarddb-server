package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The document nesting bound.
//
// A document a client sends may nest at most maxDocumentDepth levels: the
// document itself is level 1, and each embedded document or array inside it
// adds one. It is the drivers' bound (driver specification, section 7), and the
// BSON corpus (clients/testdata/bson-vectors/v1.json) fixes the count:
// document/nested-depth-100 and array/nested-depth-100 are accepted,
// reject/depth-101-* refused.
//
// The bound is checked on the raw bytes, before any decode, by one iterative
// pass, the library's Validate at the site's limit (validateAt):
//
//   - every frame NewHandler serves, first, before the node-to-node handlers,
//     the authorization gate, cluster routing, the write path and the
//     handlers (validateRequest);
//   - every entry of a peer's replicated push, and every entry WAL replay
//     applies, since neither passes NewHandler;
//   - every record of a restore chunk, before it is decoded (restoreChunk),
//     and every document a restore decodes from a dump (decodeRestoreDocument);
//   - the result of an update that can move a value deeper than its request
//     carries it ($rename, a path of several segments, a push), since that
//     result is stored and every later read decodes it (updateGuard);
//   - every answer a node reads back from a member it asked
//     (decodePeerAnswer), the answers a member relays inside a replication
//     push's answer, a compact read's document among them
//     (refuseDeepRelayedAnswers), the document of a versioned get, and every
//     record of a migration stream with every value of it a read decodes
//     later — the state document, blob metadata, documents, queue and heap
//     entries, store-field elements and delivery records — before any of the
//     copy is written (readMigrateStream, storedDocumentOf): a member's answer
//     is held to the check a client's request gets.
//
// Each of these boundaries makes its bytes a bson.Document there (build step
// 7b). The server's own depth scan, which these checks once were, is gone
// (build step 9): Validate words every refusal, and updateMayDeepen asks it
// whether an update request is past a depth instead of measuring one.
//
// A stored document is not measured again when it is read, indexed or
// repaired: every way a document is stored, a copy pulled from another member
// included, passes one of the checks above, so none is deeper than the bound.

// maxDocumentDepth is how many levels a document may nest, the root included.
const maxDocumentDepth = 100

// ErrDocumentTooDeep is the sentinel under every refusal of a document nested
// past maxDocumentDepth. It reaches the wire as INVALID_ARGUMENT.
var ErrDocumentTooDeep = errors.New("document nested too deep")

// ErrMalformedPayload is the sentinel under the refusal of bytes a client
// sent that are not well-formed BSON. It reaches the wire as INVALID_ARGUMENT:
// the request is the client's to correct. errMalformedBSON is not mapped
// itself, because it is also wrapped when the server measures bytes it encoded.
var ErrMalformedPayload = errors.New("payload is not well-formed BSON")

// errMalformedBSON is under every refusal of bytes Validate could not read as
// BSON (malformedBSONError).
var errMalformedBSON = errors.New("malformed BSON")

// depthAdvice names the ways out of a document nested too deep (AGENTS.md
// rule 3).
const depthAdvice = `Flatten the deep part, or keep it under its own key, in a store field or as a blob — see "Limits" in the docs.`

// requestDepthLimit is the depth a request payload of op may reach: its
// documents may nest maxDocumentDepth levels, and the request's own envelope,
// and the list a batch or a declaration carries its documents in, are not
// part of any document.
func requestDepthLimit(op transport.Opcode) int {
	switch op {
	case transport.OpBatchPut, transport.OpBatchUpsert, transport.OpDeclareStoreFields:
		return maxDocumentDepth + 2
	case transport.OpBatch:
		// The envelope, its items array and the item document sit above an
		// item's payload, whose own root is the +1 every request has. Each
		// item's payload is then measured at its own limit.
		return maxDocumentDepth + 4
	}
	return maxDocumentDepth + 1
}

// depthRecordKind names a record that carries a client's document, for
// recordDepthLimit.
type depthRecordKind int

const (
	// recordStored is a value as the store keeps it: a document, a queue or
	// heap entry (a heap's priority lives in its key), a field element.
	recordStored depthRecordKind = iota
	// recordDumped is the record a dump writes for a bucket's document.
	recordDumped
	// recordClaimed is a claimed delivery record's entry: the stored entry itself.
	recordClaimed
	// recordDead is a dead entry's record.
	recordDead
)

// recordDepthLimit is the one table of how deep a record that wraps a client's
// document may nest: the document (maxDocumentDepth) plus the levels its record
// wraps it in. The pull of a migration (storedDocumentOf) and the restore of a
// dump (restoreRecordDepthLimit) both read it, so the two cannot drift; a row
// changed here turns the tests of both sides red.
//
//   - recordStored, recordClaimed: the document itself, 0 levels.
//   - recordDumped: a heap's dump record is {_id_, priority, value} and a
//     blob's holds the caller's metadata under one key, 1 level; the others are
//     the document itself.
//   - recordDead: holds the entry in data, 1 level; a heap's entry is the
//     {_id_, priority, value} document, a second.
func recordDepthLimit(kind depthRecordKind, storeType BucketType) int {
	switch kind {
	case recordDumped:
		if storeType == BucketTypeHeap || storeType == BucketTypeBlob {
			return maxDocumentDepth + 1
		}
	case recordDead:
		if storeType == BucketTypeHeap {
			return maxDocumentDepth + 2
		}
		return maxDocumentDepth + 1
	}
	return maxDocumentDepth
}

// peerAnswerDepthLimit is the depth an answer from another member may reach.
// Its documents nest maxDocumentDepth levels, and the deepest an answer
// carries one is three levels down, in a list of entries each holding its
// document (a range's entries, a dead list's items, a field list's elements),
// so a document a client may store crosses between members.
const peerAnswerDepthLimit = maxDocumentDepth + 3

// decodePeerAnswer decodes into answer the bytes a member sent this node, once
// they are validated as a Document at peerAnswerDepthLimit; bytes Validate
// refuses are not read. Each answer is validated once, here.
func decodePeerAnswer(payload []byte, answer any) error {
	document, err := validateAt(payload, peerAnswerDepthLimit)
	if err != nil {
		return fmt.Errorf("the member's answer was not read: %w", err)
	}
	return document.Decode(answer)
}

// refuseDeepPeerAnswer validates every answer a member relays inside a
// replication push's answer: the lone forward's and a sub-batch's.
func refuseDeepPeerAnswer(resp *ReplPushResponse) error {
	if err := refuseDeepRelayedAnswers(resp.Responses); err != nil {
		return err
	}
	// A sub-batch's answers are relayed answers too, each validated as a lone
	// forward's is (batch-request spec §5.3).
	for _, answer := range resp.BatchAnswers {
		if err := refuseDeepRelayedAnswers(answer.Responses); err != nil {
			return err
		}
	}
	return nil
}

// refuseDeepRelayedAnswers validates the answers a member relays inside a
// replication push's answer, each carried as a binary value the validation of
// the push's answer does not descend into. The answers travel on as the
// response's payloads, so only the refusal is kept. GET_FAST's answer has a layout of its
// own: a status byte, then the stored document it found, or nothing for a
// miss, which is held to the bound of a stored document.
func refuseDeepRelayedAnswers(relayed []ReplResponse) error {
	for _, answer := range relayed {
		document, limit := answer.Payload, peerAnswerDepthLimit
		if transport.Opcode(answer.Opcode) == transport.OpGetFastResponse && len(document) > 0 {
			document, limit = document[1:], maxDocumentDepth
		}
		if len(document) == 0 {
			continue
		}
		if _, err := validateAt(document, limit); err != nil {
			return fmt.Errorf("the member's %s answer was not read: %w", transport.Opcode(answer.Opcode), err)
		}
	}
	return nil
}

// validateAt makes raw, bytes from outside this node's own store, a
// bson.Document checked at depthLimit (spec 5.2). Limits.Size stays 0: the
// size check is on a stored document's own bytes (checkDocument), and a
// payload may legitimately be larger. A refusal is ErrDocumentTooDeep for a
// level past depthLimit, and otherwise Validate's own error, which names the
// byte it stopped at, under errMalformedBSON.
func validateAt(raw []byte, depthLimit int) (bson.Document, error) {
	document, err := bson.Validate(raw, bson.Limits{Depth: depthLimit})
	if err == nil {
		return document, nil
	}
	if errors.Is(err, bson.ErrNestingTooDeep) {
		return bson.Document{}, ErrDocumentTooDeep
	}
	return bson.Document{}, malformedBSONError{validation: err}
}

// malformedBSONError is Validate's refusal of bytes that are not BSON, worded
// as Validate words it and matching errMalformedBSON as well.
type malformedBSONError struct {
	validation error
}

func (refusal malformedBSONError) Error() string { return refusal.validation.Error() }

// Unwrap gives errors.Is both the server's sentinel and Validate's error.
func (refusal malformedBSONError) Unwrap() []error {
	return []error{errMalformedBSON, refusal.validation}
}

// validateRequest is the boundary check of one request payload, made before
// anything decodes it into Go values: the payload as a Document, a refusal for
// a document nested past the bound, or a refusal for a payload that is not
// BSON. A payload of a compact layout of its own (GET_FAST), and an empty one,
// hold no document: they are answered with the zero Document and no error.
func validateRequest(op transport.Opcode, payload []byte) (bson.Document, error) {
	if len(payload) == 0 || hasWireLayoutPayload(op) {
		return bson.Document{}, nil
	}
	document, err := validateAt(payload, requestDepthLimit(op))
	if err != nil {
		return bson.Document{}, refusalForValidation(err, func() string { return requestSubject(op) })
	}
	return document, nil
}

// requestSubject names what a request's documents are, for its refusal. It is
// worked out from the opcode alone: the payload is what Validate refused, so
// nothing reads it to name the bucket it was sent to.
func requestSubject(op transport.Opcode) string {
	switch op {
	case transport.OpScan, transport.OpSearch, transport.OpCount, transport.OpDistinct:
		return "The filter"
	case transport.OpUpdate:
		return "The update"
	case transport.OpCreateBucket, transport.OpDeclareStoreFields, transport.OpAlterBucket:
		return "The bucket declaration"
	case transport.OpBlobWrite:
		return "The blob's metadata"
	case transport.OpGossip, transport.OpReplPush, transport.OpReplAck, transport.OpMigrateStart, transport.OpMigrateData,
		transport.OpMigrateDone, transport.OpClusterPropose, transport.OpClusterProbe, transport.OpClusterProbeReply,
		transport.OpVersionedGet, transport.OpMembershipChange,
		transport.OpAuthConfigPush, transport.OpAuthConfigAck,
		transport.OpStreamOpen, transport.OpStreamData, transport.OpStreamEnd, transport.OpStreamAck:
		// The node-to-node opcodes, as transport's isNodeToNodeOpcode lists
		// them: their payload is a request between members, not a document.
		return "The node-to-node request"
	}
	return "The document"
}

// refusalForValidation turns validateAt's error into the refusal a client
// reads. subject names what was validated, and is worked out only for a
// refusal.
func refusalForValidation(validateErr error, subject func() string) error {
	switch {
	case validateErr == nil:
		return nil
	case errors.Is(validateErr, ErrDocumentTooDeep):
		return &sentencedError{cause: ErrDocumentTooDeep, message: depthRefusalText(subject(), "nests")}
	}
	return &sentencedError{cause: errors.Join(ErrMalformedPayload, validateErr), message: fmt.Sprintf(
		"%s is not well-formed BSON (%v). Send it with a hoardDB driver, or encode it as one BSON document.", subject(), validateErr)}
}

// depthRefusalText is the refusal for a document nested past the bound: what
// nests too deep, the bound and how it is counted, and the ways out. verb is
// "nests" for what was sent and "would nest" for an update's result.
func depthRefusalText(subject, verb string) string {
	return fmt.Sprintf("%s %s more than %d levels deep; a document may nest at most %d levels (the document itself is level 1, "+
		"and each embedded document or array inside it adds one). %s", subject, verb, maxDocumentDepth, maxDocumentDepth, depthAdvice)
}

// updateMayDeepen reports whether applying req can leave a document deeper
// than the bound when the stored one is within it. A $rename can move a stored
// subtree below a longer path. Any other operator places its value at its
// path, so the result is no deeper than the longest path, plus the array a
// push or add-to-set adds, plus the deepest value, which is the request's own
// depth less the request and its operator map: segments+1+(requestDepth-2)
// past maxDocumentDepth. Validate answers that without measuring the depth:
// the request nests past maxDocumentDepth+1-segments levels. A request
// Validate refuses for any other reason has every result measured too, as
// one that would not encode (encoded nil) does.
func updateMayDeepen(req UpdateRequest, encoded []byte) bool {
	if len(req.Rename) > 0 || encoded == nil {
		return true
	}
	segments := 0
	for _, operator := range []map[string]interface{}{req.Set, req.Push, req.AddToSet, req.Min, req.Max} {
		for path := range operator {
			segments = max(segments, pathSegmentBound(path))
		}
	}
	for path := range req.Inc {
		segments = max(segments, pathSegmentBound(path))
	}
	for path := range req.Mul {
		segments = max(segments, pathSegmentBound(path))
	}
	if segments+1 > maxDocumentDepth {
		return true
	}
	_, err := bson.Validate(encoded, bson.Limits{Depth: maxDocumentDepth + 1 - segments})
	return err != nil
}

// pathSegmentBound is at least the number of segments in an update path: one,
// plus one per dot and per opening bracket.
func pathSegmentBound(path string) int {
	return 1 + strings.Count(path, ".") + strings.Count(path, "[")
}

// refuseDeepUpdateResult measures an update's result, for an update that can
// deepen a document (updateMayDeepen): the encoded result is validated at the
// stored document's bound. A result past the library's encoding ceiling
// (bson.MaxNestingCeiling) cannot be encoded to be measured, and is past the
// bound all the more. No client update reaches it today: a path has at most
// 100 segments and the subtree it moves at most 99 levels, 199 in all.
func refuseDeepUpdateResult(key string, updated map[string]interface{}) error {
	encoded, err := bson.Marshal(updated)
	if errors.Is(err, bson.ErrNestingTooDeep) {
		return deepUpdateRefusal(key)
	}
	if err != nil {
		return fmt.Errorf("measuring the depth of updated document %q: %w", key, err)
	}
	_, validateErr := validateAt(encoded, maxDocumentDepth)
	if errors.Is(validateErr, ErrDocumentTooDeep) {
		return deepUpdateRefusal(key)
	}
	if validateErr != nil {
		return fmt.Errorf("measuring the depth of updated document %q: %w", key, validateErr)
	}
	return nil
}

// deepUpdateRefusal is the refusal of an update whose result would nest past
// the bound.
func deepUpdateRefusal(key string) error {
	return &sentencedError{cause: ErrDocumentTooDeep, message: depthRefusalText(keyLabel("Document", "The document", key), "would nest") +
		" The document was not changed."}
}
