package server

import (
	"fmt"
	"strings"
)

// storeTypeVerbs is, per store type, the driver verbs it answers, in the order
// of the handbook's matrix (clients/handbook/operations.md section 1). Every
// refusal of a verb the type does not answer ends with the type's row, so the
// message that says "not this" also says what works. TestStoreTypeVerbsMatchTheMatrix
// holds it to the matrix test's table.
var storeTypeVerbs = map[BucketType][]string{
	BucketTypeFIFO: {"put", "insert", "insert many", "push", "pop", "peek", "claim", "ack", "release", "extend",
		"dead", "requeue", "discard", "find", "distinct", "count", "search", "stats"},
	BucketTypeLIFO: {"put", "insert", "insert many", "push", "pop", "peek", "claim", "ack", "release", "extend",
		"dead", "requeue", "discard", "find", "distinct", "count", "search", "stats"},
	BucketTypeHeap: {"put", "insert", "insert many", "push", "pop", "peek", "claim", "ack", "release", "extend",
		"dead", "requeue", "discard", "find", "distinct", "count", "search", "stats"},
	BucketTypeHash: {"put", "insert", "insert many", "get", "exists", "delete", "update", "push", "find", "distinct",
		"count", "search", "stats", "consistent get"},
	BucketTypeBTree: {"put", "insert", "insert many", "get", "exists", "delete", "update", "push", "range", "first", "last",
		"find", "distinct", "count", "search", "stats", "consistent get"},
	BucketTypeBlob: {"get", "exists", "delete", "put blob", "get blob", "count", "stats"},
}

// wrongTypeSentence is the one way a verb a bucket's store type does not answer
// is refused (BUG-0469): an ERROR frame, INVALID_ARGUMENT, under
// ErrWrongBucketType, whose message is the verb-specific sentence (why, and the
// verb that does the job) followed by the verbs the type takes. Handlers return
// it instead of an in-band found:false or success:false, so a driver checks one
// place.
func wrongTypeSentence(sentence string, got BucketType) error {
	return &sentencedError{
		message: sentence + " " + typeTakesSentence(got),
		cause:   ErrWrongBucketType,
	}
}

// typeTakesSentence names the verbs a store type answers.
func typeTakesSentence(got BucketType) string {
	return fmt.Sprintf("A %s bucket takes: %s.", got, strings.Join(storeTypeVerbs[got], ", "))
}
