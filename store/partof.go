package store

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

// ErrBadPartCount is a part count K that is not a power of two between 2 and
// 65,536. The part is the top log2(K) bits of a hash, so any other K would
// give parts of unequal size.
var ErrBadPartCount = errors.New("the part count must be a power of two between 2 and 65536")

// maxPartCount keeps a part number within the four hex digits of a part name.
const maxPartCount = 1 << 16

// PartOf is the part of a spread collection that holds the document whose
// encoded id is encoded: the top log2(k) bits of xxhash64(encoded). The input
// is EncodeComponent's output, not the raw id, so an id of another type reaches
// the same function with its own tag and a change of id type never re-parts a
// string (document placement spread MVP note, B11). k is the collection's
// parts, read from its template by the caller. The result is computed on
// every operation and never stored.
func PartOf(encoded []byte, k int) (int, error) {
	if k < 2 || k > maxPartCount || k&(k-1) != 0 {
		return 0, fmt.Errorf("part count %d: %w", k, ErrBadPartCount)
	}
	return int(xxhash.Sum64(encoded) >> (64 - bits.TrailingZeros(uint(k)))), nil
}
