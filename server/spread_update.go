package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/transport"
)

// scansBucket reports whether an UPDATE frame asks to change documents by
// what they hold rather than by key: it is multi, or it carries a non-empty
// filter and no key. A keyed update with a filter is a guard on one document
// and routes to its part.
func (m frameMeta) scansBucket() bool {
	if m.Multi {
		return true
	}
	hasFilter := m.Filter.Type == bsontype.EmbeddedDocument && len(m.Filter.Value) > emptyDocumentBytes
	return hasFilter && !m.namesKey()
}

// namesKey reports whether an UPDATE frame names a document: its key is not
// absent or the empty string a request struct writes for no key.
func (m frameMeta) namesKey() bool {
	if text, isText := m.Key.StringValueOK(); isText {
		return text != ""
	}
	return m.Key.Type != 0
}

// emptyDocumentBytes is the size of the BSON document {}.
const emptyDocumentBytes = 5

// filteredUpdateRefusal judges a filtered UPDATE of bucket against what this
// node knows of the bucket. judged is false when the node holds no copy, so
// it cannot tell whether the bucket is spread; the refusal is then the
// owner's (Cluster.refuseOwnerFilteredUpdate). The refusal is nil for a bucket
// recorded whole.
func (s *Server) filteredUpdateRefusal(database, bucket string) (refusal error, judged bool) {
	facts, held := s.peekBucket(database, bucket)
	if !held {
		return nil, false
	}
	if !facts.spread {
		return nil, true
	}
	return filteredUpdateOnSpreadError(database, bucket), true
}

// filteredUpdateOnSpreadError is the refusal, in the words of document
// placement spec §6.7 with the fix as commands that run (project rule 3).
func filteredUpdateOnSpreadError(database, bucket string) error {
	return &sentencedError{cause: ErrFilteredUpdateOnSpread, message: fmt.Sprintf(
		"Bucket %q (database %q) is declared spread (placement: spread), and an update matching a filter is refused on a spread bucket. "+
			"Update by _id_: db.%s.update(\"ada\", {$set: {City: \"London\"}}). "+
			"To change many documents, find them and update each id. "+
			"To update by filter, re-create the bucket on one node: create bucket %s in %s { type: hash, placement: whole }",
		bucket, database, bucket, bucket, database)}
}

// refuseOwnerFilteredUpdate is the owner's half of the filtered-update
// refusal, for the entry whose entering node held no copy of the bucket. It
// runs only for a marked entry (entry.FilterDeferred) and only as the owner
// applies a client's write (Cluster.applyAsOwner): a replica applying the
// owner's entry and a WAL replay never set the mark and never pass here.
func (c *Cluster) refuseOwnerFilteredUpdate(entry ReplEntry) error {
	if !entry.FilterDeferred {
		return nil
	}
	_, database := decodeFrameMeta(entry.Payload).scope(transport.OpUpdate)
	refusal, _ := c.s.filteredUpdateRefusal(database, entry.Bucket)
	return refusal
}
