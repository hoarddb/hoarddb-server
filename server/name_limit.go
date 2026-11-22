package server

import (
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"strconv"
	"unicode/utf8"
)

// maxNameBytes is the longest database name, the longest bucket name, and the
// longest typed field name (at every level of a declaration), in UTF-8 bytes:
// the limit developers already know from file names.
//
// A name is a piece of every key that records the bucket, and the store refuses
// a key over 65,000 bytes (third_party/badger/txn.go:Txn.modify). The catalog
// key is 12 bytes plus both names (server/nodestore.go:catalogKey); the
// placement mark and the replication snapshot position add about 18 bytes of
// framing and, for the position, the source node's whole name
// (server/replication_positions.go:seededKey). Two names of 255 leave about
// 64,000 bytes of room under the store's limit for all of them, so no smaller
// budget is forced; TestEveryNameBearingKeyFitsAtTheNameLimit holds the claim.
const maxNameBytes = namepolicy.MaxBytes

// echoedNameBytes is how much of a refused name the refusal repeats. The
// refusal travels in an error frame, so it must not grow with the name.
const echoedNameBytes = 64

// nameTooLongError is a database name or bucket name over maxNameBytes.
type nameTooLongError struct {
	kind     string // "database" or "bucket"
	database string
	name     string
}

func (e *nameTooLongError) Error() string {
	if e.kind == "database" {
		return fmt.Sprintf("database name %s is too long: it is %s bytes and the limit is %d. Use a shorter database name.",
			echoName(e.name), groupThousands(len(e.name)), maxNameBytes)
	}
	return fmt.Sprintf("bucket name %s in database %s is too long: it is %s bytes and the limit is %d. "+
		"Use a shorter bucket name. To keep a long identifier, store it as a field of a document and name the bucket by a short id.",
		echoName(e.name), echoName(e.database), groupThousands(len(e.name)), maxNameBytes)
}

// echoName quotes at most echoedNameBytes of name, cut on a rune boundary and
// marked with an ellipsis when cut.
func echoName(name string) string {
	return strconv.Quote(boundedIdentifier(name))
}

// boundedIdentifier is at most echoedNameBytes of a client-supplied
// identifier, cut on a rune boundary and marked with an ellipsis when cut. It
// is what a log line or an audit entry repeats of a name the client sent
// before any account vouched for it, so a login attempt cannot make the logs
// grow with what it sends.
func boundedIdentifier(name string) string {
	if len(name) <= echoedNameBytes {
		return name
	}
	cut := echoedNameBytes
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return name[:cut] + "…"
}

// maxLoggedCapabilities is how many of an AUTH request's capability strings
// the connect log line repeats. Drivers send a handful.
const maxLoggedCapabilities = 16

// boundedCapabilities is what the connect log line repeats of a client's
// capability list: the first maxLoggedCapabilities, each bounded, and a count
// of the rest.
func boundedCapabilities(capabilities []string) []string {
	shown := capabilities[:min(len(capabilities), maxLoggedCapabilities)]
	bounded := make([]string, 0, len(shown)+1)
	for _, capability := range shown {
		bounded = append(bounded, boundedIdentifier(capability))
	}
	if rest := len(capabilities) - len(shown); rest > 0 {
		bounded = append(bounded, fmt.Sprintf("… and %d more", rest))
	}
	return bounded
}

// checkDatabaseNameLength refuses a database name over the limit.
func checkDatabaseNameLength(database string) error {
	if len(database) > maxNameBytes {
		return &nameTooLongError{kind: "database", name: database}
	}
	return nil
}

// checkNamePair refuses a database name or a bucket name over the limit, the
// database first. The error is a *nameTooLongError.
func checkNamePair(database, bucket string) error {
	if err := checkDatabaseNameLength(database); err != nil {
		return err
	}
	if len(bucket) > maxNameBytes {
		return &nameTooLongError{kind: "bucket", database: database, name: bucket}
	}
	return nil
}

// invalidBucketNameMessage is the text for a name that validateBucketName
// refused: the limit's own sentence as it stands, and the path-safety refusals
// under the prefix they have always had.
func invalidBucketNameMessage(err error) string {
	var tooLong *nameTooLongError
	if errors.As(err, &tooLong) {
		return tooLong.Error()
	}
	return fmt.Sprintf("invalid bucket name: %v", err)
}
