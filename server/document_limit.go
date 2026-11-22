package server

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The per-document size limit (GAP-36).
//
// It follows the blob cap's placement rule (blob_limit.go): a client's write
// is measured where it enters, before cluster routing and the log, and a
// replica or a replay never measures anything, so a replica whose own limit is
// lower still stores what its owner accepted. A document put, push, batch or
// field-store element is measured from its payload as received. An update is
// the one write whose size is not known at the door, because it is the result
// of applying operators to a stored document; the entering node therefore
// stamps its limit into the update's payload (stampUpdateLimit), and every
// applier, owner or replica or replay, holds the update to that same number
// and reaches the same answer on the same document.
//
// "A document's bytes" is its encoding without its _id_ element. The server
// adds _id_ when a client sends none, so counting it would make a document
// accepted at exactly the limit unable to take a no-op update.

// ErrDocumentTooLarge is the sentinel under every document refused for its
// size. It reaches the wire as INVALID_ARGUMENT, as ErrBlobTooLarge does.
var ErrDocumentTooLarge = errors.New("document too large")

// documentAdvice names the two ways out of a document that is too large (AGENTS.md
// rule 3): a list that grows belongs in a nested store field, whose elements are
// stored separately, and a large value belongs in a blob bucket or, for a file,
// on the developer's own storage behind a pointer document.
const documentAdvice = `A list that grows belongs in a nested store field; a large value belongs in a blob bucket, and a large file on your own storage behind a pointer document — see "Large file stores" in the docs.`

// maxDocumentBytes is this node's document limit: the configured one, or the
// default when the server was built without config.Load.
func (s *Server) maxDocumentBytes() int {
	if s.config != nil && s.config.Storage.MaxDocumentBytes > 0 {
		return s.config.Storage.MaxDocumentBytes
	}
	return config.DefaultMaxDocumentBytes
}

// refuseOversizedWrite is the door check for one client frame: the blob cap
// and the document limit. It stamps an update with the limit it will be held
// to. It returns nil for every opcode that stores neither, and for a payload it
// cannot read, which the handler then refuses with its own decode error. Each
// check reads the payload's fields the way its handler decodes them
// (size_check_fields.go). typeLimitDeferred reports that the put's
// bucket-dependent limit could not be applied here and must be applied by the
// owner; NewHandler marks the request with it (BUG-0582).
func (s *Server) refuseOversizedWrite(frame *transport.Frame) (deferred bool, err error) {
	switch frame.Opcode {
	case transport.OpBlobWrite:
		return false, s.refuseBlobWriteFrame(frame.Payload)
	case transport.OpPut, transport.OpUpsert, transport.OpBTreeInsert:
		return s.refuseInsertFrame(frame.Opcode, frame.Payload)
	case transport.OpFIFOPush, transport.OpLIFOPush, transport.OpHeapPush:
		return false, s.refusePushFrame(frame.Payload)
	case transport.OpFieldPush, transport.OpFieldPut:
		return false, s.refuseFieldElementFrame(frame.Payload)
	case transport.OpBatchPut, transport.OpBatchUpsert:
		return false, s.refuseBatchFrame(frame.Payload)
	case transport.OpUpdate:
		frame.Payload = s.stampUpdateLimit(frame.Payload)
	}
	return false, nil
}

// documentShape is what the door needs to know about one document write.
type documentShape struct {
	label        string
	bucket       string
	key          string
	document     bson.Raw
	payloadBytes int
}

// refuseInsertFrame measures a put's or a B+tree insert's document against
// the document limit. A large put into a blob bucket this node holds is
// refused as a blob-bucket put instead (build doc BD-7), and a put whose
// bucket this node cannot resolve is not held to the document limit; it
// reports the deferred type limit for the owner to apply (BUG-0582).
func (s *Server) refuseInsertFrame(opcode transport.Opcode, payload []byte) (deferred bool, err error) {
	var insert insertFields
	insert.read(payload)
	document, isDocument := insert.data.document()
	if !isDocument {
		return false, nil
	}
	if opcode == transport.OpPut {
		resolved, typeDeferred, err := s.refuseBlobBucketPut(insert, document, len(payload))
		if resolved || err != nil {
			return typeDeferred, err
		}
	}
	return false, s.refuseDocument(insert.bucket.text, insert.key.text, document, len(payload))
}

// refusePushFrame measures a FIFO, LIFO or heap push's document.
func (s *Server) refusePushFrame(payload []byte) error {
	var push pushFields
	push.read(payload)
	document, isDocument := push.data.document()
	if !isDocument {
		return nil
	}
	return s.refuseDocument(push.bucket.text, "", document, len(payload))
}

// refuseDocument measures one document a write carries under key, or under
// the key it names inside itself when key is empty.
func (s *Server) refuseDocument(bucket, key string, document bson.Raw, payloadBytes int) error {
	if key == "" {
		// A restored document, and a client's that names its key inside itself.
		key = lastStringValue(document, "_id_")
	}
	return s.checkDocument(documentShape{
		label: keyLabel("Document", "The document", key), bucket: bucket, key: key,
		document: document, payloadBytes: payloadBytes,
	})
}

// refuseFieldElementFrame measures a store field's element document, and the
// content of a blobStore field's element against the blob cap: it is a blob.
func (s *Server) refuseFieldElementFrame(payload []byte) error {
	var fields fieldElementFields
	fields.read(payload)
	bucket, element := fields.bucket.text, fields.element.text
	if fields.content.isSet {
		if err := s.checkBlobWrite(blobWriteShape{bucket: bucket, key: element, dataBytes: fields.content.length, payloadBytes: len(payload)}); err != nil {
			return err
		}
	}
	document, isDocument := fields.data.document()
	if !isDocument {
		return nil
	}
	return s.checkDocument(documentShape{
		label: keyLabel("Document", "The document", element), bucket: bucket, key: element,
		document: document, payloadBytes: len(payload),
	})
}

// refuseBatchFrame measures each document of a batch and names the first one
// over the limit, by position and by key.
func (s *Server) refuseBatchFrame(payload []byte) error {
	var batch batchFields
	batch.read(payload)
	if batch.data.array == nil {
		return nil
	}
	for position, element := range readableElements(batch.data.array) {
		document, isDocument := element.Value().DocumentOK()
		if !isDocument {
			continue
		}
		key := lastStringValue(document, "_id_")
		label := fmt.Sprintf("Document %d of the batch", position+1)
		if key != "" {
			label = keyLabel("Document", "", key) + fmt.Sprintf(" (document %d of the batch)", position+1)
		}
		if err := s.checkDocument(documentShape{label: label, document: bson.Raw(document), payloadBytes: 0}); err != nil {
			return err
		}
	}
	return checkReplicationFits("The batch", "", batch.bucket.text, len(payload), len(payload))
}

// checkDocument applies the limit to one document and the frame arithmetic to
// the write that carries it.
func (s *Server) checkDocument(write documentShape) error {
	limit := s.maxDocumentBytes()
	if len(write.document) > limit {
		if own := documentBytes(write.document); own > limit {
			return &sizeRefusal{sentinel: ErrDocumentTooLarge, message: documentSizeRefusalText(write.label, "is", int64(own), int64(limit))}
		}
	}
	if write.payloadBytes == 0 {
		return nil
	}
	return checkReplicationFits(write.label, write.key, write.bucket, write.payloadBytes, write.payloadBytes-len(write.document))
}

// documentBytes is a document's own encoded size: everything but its _id_.
func documentBytes(document []byte) int {
	idValue, err := bson.Raw(document).LookupErr("_id_")
	if err != nil {
		return len(document)
	}
	const elementHeader = 1 + len("_id_") + 1 // type byte, name, terminator
	return len(document) - elementHeader - len(idValue.Value)
}

// documentSizeRefusalText is the refusal for a document over the limit: what
// was sent, what the limit is, where it is set, and the two ways out. verb is
// "is" for a write and "would be" for an update's result. A size that rounds to
// the limit's own text is given in bytes, so the two numbers cannot read as
// equal.
func documentSizeRefusalText(label, verb string, size, limit int64) string {
	sizeText, limitText := formatBytesIEC(size), formatBlobLimit(limit)
	if sizeText == formatBytesIEC(limit) {
		sizeText, limitText = fmt.Sprintf("%d bytes", size), fmt.Sprintf("%d bytes", limit)
	}
	suffix := ""
	if verb == "would be" {
		suffix = " after this update"
	}
	text := fmt.Sprintf(`%s %s %s%s; this server's limit is %s per document (storage.max_document_bytes, maximum %s).`,
		label, verb, sizeText, suffix, limitText, formatBlobLimit(config.MaxDocumentBytesCeiling))
	if verb == "would be" {
		text += " The document was not changed."
	}
	return text + " " + documentAdvice
}

// stampUpdateLimit appends this node's document limit to an update's payload
// as the field the update is applied under, replacing any the client sent (the
// last duplicate wins on decode). A payload that is not one whole BSON document
// is returned as it is for the handler to refuse.
func (s *Server) stampUpdateLimit(payload []byte) []byte {
	if len(payload) < 5 || int(binary.LittleEndian.Uint32(payload)) != len(payload) {
		return payload
	}
	stamped := append([]byte(nil), payload[:len(payload)-1]...)
	stamped = bsoncore.AppendInt64Element(stamped, "max_document_bytes", int64(s.maxDocumentBytes()))
	stamped = append(stamped, 0)
	binary.LittleEndian.PutUint32(stamped, uint32(len(stamped)))
	return stamped
}

// updateGuard holds the documents an update produces to the limit the update
// was stamped with, and to the nesting bound.
type updateGuard struct {
	limit  int
	growth int
	// deepens reports an update whose result can nest deeper than its request
	// (updateMayDeepen): only its results are measured for depth. The answer
	// depends on the request and the stored document alone, so every applier,
	// owner or replica or replay, reaches the same one.
	deepens bool
}

// newUpdateGuard prepares the guard for one update. The stamped limit is the
// entering node's. An update with none is held to nothing: every client update
// is stamped at the door (refuseOversizedWrite), so one without a stamp is a
// log entry from before the limit existed or one an older owner shipped, and
// its owner already acknowledged it at whatever size it produced. Refusing it
// on replay would drop that update, and on a replica leave the copies apart.
//
// growth bounds how many bytes the operators can add to any document: the
// request's own encoded size. A document that stays under the limit even
// after that much is not re-encoded to be measured, which keeps the check off
// the common update.
func newUpdateGuard(req UpdateRequest) *updateGuard {
	encoded, err := bson.Marshal(req)
	if err != nil {
		encoded = nil
	}
	guard := &updateGuard{deepens: updateMayDeepen(req, encoded)}
	limit := req.MaxDocumentBytes
	if limit <= 0 {
		return guard
	}
	guard.limit, guard.growth = limit, limit
	if encoded != nil {
		guard.growth = len(encoded)
	}
	return guard
}

// check refuses a document whose update result is over the limit and larger
// than the stored document, or nested past the bound. stored is the document's
// encoding before the update. The limit bounds what enters: a document left
// over a lowered limit can still be trimmed in steps (cluster-settings spec
// §9.6, T-23), so a result no larger than what is stored is accepted.
func (g *updateGuard) check(key string, stored []byte, updated map[string]interface{}) error {
	if g.deepens {
		if err := refuseDeepUpdateResult(key, updated); err != nil {
			return err
		}
	}
	if g.limit == 0 || len(stored)+g.growth <= g.limit {
		return nil
	}
	encoded, err := bson.Marshal(updated)
	if err != nil {
		return fmt.Errorf("measuring updated document %q: %w", key, err)
	}
	if own := documentBytes(encoded); own > g.limit && own > documentBytes(stored) {
		return &sizeRefusal{sentinel: ErrDocumentTooLarge, message: documentSizeRefusalText(keyLabel("Document", "The document", key), "would be", int64(own), int64(g.limit))}
	}
	return nil
}
