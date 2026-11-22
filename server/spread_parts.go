package server

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The part scheme of a spread collection (document placement spread MVP note,
// section 2). A spread collection is a template bucket plus up to
// defaultSpreadParts part buckets, each an ordinary bucket named
// collection ‖ 0x1F ‖ hex4(part). The separator is a control character, which
// no client may put in a bucket name, so a part name can never collide with a
// name a client chose.
const (
	// partSeparator ends the collection's name in a part's name.
	partSeparator = '\x1f'
	// partSuffixBytes is the separator plus four hex digits.
	partSuffixBytes = 5
	// defaultSpreadParts is K for a template made by this build. It is
	// written into the template's catalog entry at create (parts) and never
	// changes after; a caller that needs K reads it from the template from the template's entry, never from
	// this constant, so a later build can vary it per collection.
	defaultSpreadParts = 256
)

// errPreSpreadDataDirectory refuses to start on a data directory whose hash
// buckets were made before spread collections (B6). Before v1.0 nothing is
// retrofitted: the owner's rule is to start with a new data directory.
var errPreSpreadDataDirectory = errors.New("this data directory holds buckets from before spread collections; start with a new data directory")

// partName is the name of part p of collection.
func partName(collection string, p int) string {
	const hexDigits = "0123456789abcdef"
	suffix := [partSuffixBytes]byte{partSeparator, hexDigits[p>>12&15], hexDigits[p>>8&15], hexDigits[p>>4&15], hexDigits[p&15]}
	return collection + string(suffix[:])
}

// partNameParts reads a part name back: its collection and part number. It is
// strict: exactly one separator, followed by four lower-case hex digits and
// nothing else, after a collection name that has none.
func partNameParts(name string) (collection string, p int, ok bool) {
	cut := len(name) - partSuffixBytes
	if cut < 1 || name[cut] != partSeparator {
		return "", 0, false
	}
	collection = name[:cut]
	if hasPartSeparator(collection) {
		return "", 0, false
	}
	digits := name[cut+1:]
	for i := 0; i < len(digits); i++ {
		if c := digits[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", 0, false
		}
	}
	number, err := strconv.ParseUint(digits, 16, 16)
	if err != nil {
		return "", 0, false
	}
	return collection, int(number), true
}

// hasPartSeparator reports whether name holds the part separator: it is a
// part's name, or a name no client may use.
func hasPartSeparator(name string) bool { return strings.IndexByte(name, partSeparator) >= 0 }

// displayBucket is the one function through which a bucket name reaches a
// client, a log line or a metric label: a part displays as its collection and
// any other name as itself. Every place that could print a part name goes
// through it, so the separator is never seen outside the server.
func displayBucket(name string) string {
	if cut := strings.IndexByte(name, partSeparator); cut >= 0 {
		return name[:cut]
	}
	return name
}

// isPreSpreadEntry reports whether entry was written by a build before spread
// collections: a hash bucket whose name is not a part's and that has no
// placement (before step 12a), or is spread with no parts (12a alone). A whole
// hash bucket, a spread template that records its parts, a part and a bucket
// of any other type never match.
func isPreSpreadEntry(entry catalogEntry) bool {
	if entry.Type != BucketTypeHash || hasPartSeparator(entry.bucket) {
		return false
	}
	return entry.Placement == "" || (entry.Placement == placementSpread && entry.Parts == 0)
}

// spreadPartsFor is the parts a bucket made with placement records: K for a
// spread template and none for anything else. A request never carries it.
func spreadPartsFor(placement string) int32 {
	if placement == placementSpread {
		return defaultSpreadParts
	}
	return 0
}

// partFieldsOf reads the catalog fields of a part, for a bucket that moves: the
// part number and the template incarnation it belongs to. A bucket that is not
// a part has none and costs no read.
func (s *Server) partFieldsOf(database, bucket string) (part int32, partOf uint64, err error) {
	if !hasPartSeparator(bucket) {
		return 0, 0, nil
	}
	entry, found, err := s.readCatalogEntry(database, bucket)
	if err != nil {
		return 0, 0, fmt.Errorf("reading the catalog entry of part %q: %w", displayBucket(bucket), err)
	}
	if !found {
		return 0, 0, nil
	}
	return entry.Part, entry.PartOf, nil
}

// refuseClientPartName is B7's door: nil unless the client frame names a bucket
// that holds the part separator, then NOT FOUND of the displayed name. The
// common frame has no separator byte anywhere, so one vectorised scan of the
// payload settles it and nothing is decoded; only a frame that holds the byte
// (a binary value, say) is read for its bucket. A create that names a bucket
// is refused for the same reason, so a client cannot make a part, and the
// per-item check of a BATCH is the batch's (step 2).
func refuseClientPartName(frame *transport.Frame) error {
	if bytes.IndexByte(frame.Payload, partSeparator) < 0 {
		return nil
	}
	bucket, database, err := frameScope(frame)
	if err != nil || !hasPartSeparator(bucket) {
		return nil
	}
	return bucketNotFoundErr(database, bucket)
}
