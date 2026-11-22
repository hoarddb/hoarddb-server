package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The blob size cap (GAP-35).
//
// Where it is enforced matters more than how. A blob write reaches storage by
// three routes: a client's frame, a replica applying the owner's entry, and a
// restart replaying the write-ahead log. The cap belongs to a client's write
// only. The owner has already accepted the write when it ships it, so a replica
// whose own storage.max_blob_bytes is lower must still apply it, and a replay
// must reproduce what was accepted whatever the cap is now. A blob write
// (OpBlobWrite) is checked in NewHandler, ahead of cluster routing, the WAL
// append and every hash or file write, and not in BlobWriteCtx, which all three
// routes share. A document put into a blob bucket (OpPut) is refused: a blob
// bucket is written with put (BLOB_WRITE), and the handler refuses the PUT
// (build doc BD-7). The door refuses a large one before it is logged when the
// entering node holds the bucket (Server.refuseBlobBucketPut); a node that has
// not learned the bucket, or sees it with its create in flight, cannot resolve
// its type, so the door applies only the ceiling common to every type there
// and marks the put as deferred, and the bucket's owner, which knows the type,
// judges the put when it applies it (Cluster.refuseOwnerPutSize, called from
// Cluster.applyAsOwner, the owner's client-write path only). The owner measures only a deferred put, so read
// repair, restore, replica applies and replays are never measured. A replica
// applying the owner's live entry and a replay reach
// Server.dispatch through Cluster.applyLocal without passing applyAsOwner.
// Migration and restore never call the cap either: migration moves payload
// files under Badger keys (server/migrate.go:writeMigratedPayloads) and a dump
// carries no blob content to restore.

// ErrBlobTooLarge is the sentinel under every blob refused for its size or for
// the size of the frame that would carry it to a replica. It reaches the wire
// as INVALID_ARGUMENT: the request is at fault, and no retry can succeed.
var ErrBlobTooLarge = errors.New("blob too large")

// ErrBlobEmpty is the sentinel under the refusal of a BLOB_WRITE with no data.
// It reaches the wire as INVALID_ARGUMENT: the request is at fault, and the
// message already names the command that stores a document instead.
var ErrBlobEmpty = errors.New("blob has no data")

// blobReplicationFixedBytes is the room a blob's replication frame needs
// beyond the write's own payload and one more copy of its key and bucket name:
// the frame's opcode and request id, the cluster token, the sender's node id,
// and the entry's sequence, opcode and flags. Measured with 256-byte token and
// node-id values (TestBlobReplicationOverheadFitsItsReserve); a real token is
// far shorter.
const blobReplicationFixedBytes = 4 << 10

// blobWriteShape is what the door needs to know about one blob write.
type blobWriteShape struct {
	bucket       string
	key          string
	dataBytes    int
	payloadBytes int
}

// sizeRefusal is a write refused for its size, with the text a developer
// reads. Its sentinel is ErrBlobTooLarge or ErrDocumentTooLarge; the frame
// arithmetic refusal, which has neither limit's name, leaves it nil and
// matches both.
type sizeRefusal struct {
	sentinel error
	message  string
}

func (e *sizeRefusal) Error() string { return e.message }

// Unwrap makes the refusal match its sentinel with errors.Is.
func (e *sizeRefusal) Unwrap() error {
	if e.sentinel == nil {
		return ErrBlobTooLarge
	}
	return e.sentinel
}

// maxBlobBytes is this node's blob cap: the configured one, or the default
// when the server was built without config.Load.
func (s *Server) maxBlobBytes() int {
	if s.config != nil && s.config.Storage.MaxBlobBytes > 0 {
		return s.config.Storage.MaxBlobBytes
	}
	return config.DefaultMaxBlobBytes
}

// refuseBlobWriteFrame reads only the fields it needs off the payload, without
// copying the blob into a request struct just to count it.
func (s *Server) refuseBlobWriteFrame(payload []byte) error {
	var write blobWriteFields
	write.read(payload)
	if !write.data.isSet {
		return nil
	}
	return s.checkBlobWrite(blobWriteShape{bucket: write.bucket.text, key: write.key.text, dataBytes: write.data.length, payloadBytes: len(payload)})
}

// refuseBlobBucketPut resolves, at the door, a document put too large for
// the smaller of the blob cap and the document limit whose bucket may be a
// blob bucket. resolved reports that the put was judged here, so the document
// limit is not applied after it. The bucket is looked up only for such a
// document, so the ordinary put pays for one length read and no catalog read
// (rule 11; build doc BD-7). A held blob bucket refuses the put outright: a
// blob bucket is written with put (BLOB_WRITE), and a small document PUT into
// one is refused by its handler, logged and consumed like any constraint
// refusal. A held bucket of another type is left to the document limit.
//
// A bucket this node has not learned, or sees with its create still in flight
// (id 0), has no type to read, so only what holds for every type is applied:
// the frame arithmetic and the higher of the two limits. The owner, which
// knows the type, judges the put when it applies it
// (Cluster.refuseOwnerPutSize); the put must not fall through to the document
// limit here, which would refuse a blob-bucket put with the advice to use a
// blob bucket (BUG-0582). typeDeferred reports that the type-dependent limit
// was deferred: it is carried on the entry so the owner measures exactly the
// puts this door could not. A put the door did resolve — held, with an id — is
// not deferred and is not measured twice.
func (s *Server) refuseBlobBucketPut(insert insertFields, document bson.Raw, payloadBytes int) (resolved bool, typeDeferred bool, err error) {
	if len(document) <= min(s.maxBlobBytes(), s.maxDocumentBytes()) {
		return false, false, nil
	}
	facts, held := s.peekBucket(insert.database.text, insert.bucket.text)
	if held && facts.id != 0 {
		// A blob bucket is written with put (BLOB_WRITE), never with a
		// document PUT, so the door refuses it as the handler would, before
		// it is logged (build doc BD-7).
		return facts.bucketType == BucketTypeBlob, false, refuseInsertOnBlob(insert.bucket.text, facts.bucketType)
	}
	return true, true, s.checkPutCeiling(insert, document, payloadBytes)
}

// checkPutCeiling bounds a put at a node that cannot resolve its bucket's type
// to the higher of the blob cap and the document limit, and to the frame
// arithmetic every write shares. A document over both limits is over either
// type's real limit too, so the refusal needs no bucket type; it names the two
// limits rather than guessing one, and never advises a value into a blob bucket
// it cannot know the writer is already targeting.
func (s *Server) checkPutCeiling(insert insertFields, document bson.Raw, payloadBytes int) error {
	ceiling := max(s.maxBlobBytes(), s.maxDocumentBytes())
	label := keyLabel("Document", "The document", insert.key.text)
	if len(document) > ceiling {
		return &sizeRefusal{
			sentinel: ErrBlobTooLarge,
			message: fmt.Sprintf(
				`%s is %s; this node has not learned bucket %q, so the higher of its limits applies here, %s (the blob cap and the document limit). For larger files, keep the file on your own storage and store a pointer document — see "Large file stores" in the docs.`,
				label, formatBytesIEC(int64(len(document))), insert.bucket.text, formatBlobLimit(int64(ceiling))),
		}
	}
	return checkReplicationFits(label, insert.key.text, insert.bucket.text, payloadBytes, payloadBytes-len(document))
}

// checkBlobWrite applies both limits: the configured cap on the blob's bytes,
// and the frame arithmetic that the cap's ceiling assumes.
//
// The second limit exists because config.BlobFrameReserve is a reserve, not a
// bound on the key and metadata around the bytes. A write the owner accepts
// is sent again to every replica inside a wrapper that carries the key a
// second time; one that cannot fit a frame would be applied by the owner and
// then never reach its replicas. Refusing it at the door is what keeps "a
// replica never refuses what its owner accepted" true.
func (s *Server) checkBlobWrite(write blobWriteShape) error {
	if limit := s.maxBlobBytes(); write.dataBytes > limit {
		return &sizeRefusal{sentinel: ErrBlobTooLarge, message: blobSizeRefusalText(write.key, int64(write.dataBytes), int64(limit))}
	}
	if err := checkReplicationFits(blobLabel(write.key), write.key, write.bucket, write.payloadBytes, write.payloadBytes-write.dataBytes); err != nil {
		return err
	}
	return nil
}

// checkReplicationFits refuses a write whose copy for a replica would not fit
// one frame: the payload again, the key a second time, the bucket name and the
// wrapper's fixed fields. aroundBytes is what the message reports as the part
// that is not the write's own bytes.
func checkReplicationFits(label, key, bucket string, payloadBytes, aroundBytes int) error {
	if payloadBytes+len(key)+len(bucket)+blobReplicationFixedBytes <= transport.MaxFrameLength {
		return nil
	}
	return &sizeRefusal{message: fmt.Sprintf(
		"%s carries %s of key and metadata around its bytes, which is too much to replicate: the write and its copy for a replica, which repeats the key, must each fit one %d MiB frame. Shorten the key or the metadata.",
		label, formatBytesIEC(int64(aroundBytes)), transport.MaxFrameLength>>20)}
}

// blobSizeRefusalText is the refusal for a blob over the cap: what was sent,
// what the limit is, where it is set, and what to do instead (AGENTS.md
// rule 3). A size that rounds to the limit's own text is given in bytes, so
// the two numbers cannot read as equal.
func blobSizeRefusalText(key string, size, limit int64) string {
	sizeText, limitText := formatBytesIEC(size), formatBlobLimit(limit)
	if sizeText == formatBytesIEC(limit) {
		sizeText, limitText = fmt.Sprintf("%d bytes", size), fmt.Sprintf("%d bytes", limit)
	}
	return fmt.Sprintf(
		`%s is %s; this server's limit is %s (storage.max_blob_bytes, maximum %s). For larger files, keep the file on your own storage and store a pointer document — see "Large file stores" in the docs.`,
		blobLabel(key), sizeText, limitText, formatMiB(config.MaxBlobBytesCeiling))
}

// formatBlobLimit prints a cap the way an operator wrote it: 16 MiB, not
// 16.0 MiB, when it is a whole number of them.
func formatBlobLimit(limit int64) string {
	switch {
	case limit%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", limit>>20)
	case limit%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", limit>>10)
	}
	return formatBytesIEC(limit)
}

// blobLabel names the blob in a refusal. A key is echoed cut to 64 bytes: it
// is caller-supplied and, on the replication refusal, may be megabytes long.
func blobLabel(key string) string { return keyLabel("Blob", "The blob", key) }

// keyLabel names a write's subject: "Blob \"k\"", or the fallback when the
// write has no key yet.
func keyLabel(noun, keyless, key string) string {
	if key == "" {
		return keyless
	}
	const shown = 64
	if len(key) > shown {
		key = strings.ToValidUTF8(key[:shown], "") + "…"
	}
	return noun + " " + fmt.Sprintf("%q", key)
}
