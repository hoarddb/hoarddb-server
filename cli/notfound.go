package cli

import "strings"

// A Found:false reply carries three different meanings and no code (spec
// gap24-one-shot-spec.md 6.4): the thing asked about is absent, the store
// failed, or the bucket's type does not have the verb. The server says which
// only in the message, so the CLI sorts the message against an allow-list of
// the server's absence wording. Anything that is not on the list is a refusal,
// so a failure can never again pass as a clean miss.
//
// This is string matching across the server boundary. It is acceptable only
// because TestFoundFalseClassification drives a real server into every literal:
// if the server renames one, that test fails here rather than silently turning
// misses into errors. The real fix is a reason field on those responses, which
// is a wire change and outside GAP-24.

// foundKind is the verdict on a Found:false reply.
type foundKind int

const (
	// foundAbsent: the key, the queue head or the blob is not there. The
	// statement succeeded and the answer is "nothing".
	foundAbsent foundKind = iota
	// foundRefused: the reply names a failure or a verb the bucket does not
	// have. The statement did not do what was asked.
	foundRefused
	// foundEmpty: the reply has no message at all. A server from before
	// the reason was sent sends a zero-byte frame for every Found:false, and a future
	// encoding regression would look the same, so an empty reply is never a
	// clean miss.
	foundEmpty
)

// emptyFoundFalseMessage is what a script is told for foundEmpty.
const emptyFoundFalseMessage = "the server answered found:false with no reason; upgrade the server"

// absentMessages are the server's whole-message absence answers
// (server/service.go:Server.Find, Pop and Peek). The blob form embeds the key
// and the location, so it is matched by shape in foundFalse.
var absentMessages = []string{"document not found", "queue is empty", "heap is empty"}

// The server's answers to a delete of a key that is not stored
// (server/service.go:deleteNotFound), on a hash or btree bucket and on a blob
// bucket. The bucket-delete golden drives a real server into the first.
const (
	deleteAbsentMessage     = "document not found"
	deleteAbsentBlobMessage = "blob not found"
)

// foundFalse sorts the message of a Found:false reply on a bucket verb. The
// field-store verbs do not use it: their absent form is Success:true with no
// message, which is legitimate there.
func foundFalse(message string) foundKind {
	if message == "" {
		return foundEmpty
	}
	for _, absent := range absentMessages {
		if message == absent {
			return foundAbsent
		}
	}
	// The opening quote is the key's: it keeps "blob read failed: not found in
	// cache" from passing as an absence.
	if strings.HasPrefix(message, `blob "`) && strings.Contains(message, " not found in ") {
		return foundAbsent
	}
	return foundRefused
}

// missError is the error for a Found:false reply, or nil when the reply says
// the thing asked about is simply absent and the caller should print its own
// "nothing here" line.
func missError(message string) error {
	switch foundFalse(message) {
	case foundAbsent:
		return nil
	case foundEmpty:
		return &RefusedError{Message: emptyFoundFalseMessage}
	default:
		return &RefusedError{Message: message}
	}
}
