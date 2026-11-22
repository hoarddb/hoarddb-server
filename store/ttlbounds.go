package store

import (
	"bytes"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The TTL scan bounds (GAP-14 spec §6.2.1, build doc Step 15). A TTL index's
// entries hold whatever its field holds: dates, numbers, and — once a missing
// field indexes as null (Step 3) — nulls, booleans, strings and binaries. A
// document expires only by a date or a number of Unix seconds; everything else
// has no expiry. So the sweep walks two brackets, each type-tagged at both
// ends, and never one walk from the head of the index: that walk would reach
// every null, boolean and number below the date tag and delete them on the
// first tick — the S1 (§4 F-6).

// Bracket is one type's walk of a TTL index: the entries of type Tag at or
// below Upper, an EncodeComponent output carrying the same tag. It is
// consumed only through SecondaryIndex.ScanBracket.
type Bracket struct {
	Tag   byte
	Upper []byte
}

// TTLBounds returns the two brackets a sweep at now walks for an index whose
// documents expire expireAfter after the date they hold: dates at or before
// now - expireAfter, in milliseconds, and numbers at or before it in Unix
// seconds — canonicalised, so every numeric spelling of that second compares
// as the number it is. Always exactly two.
func TTLBounds(now time.Time, expireAfter time.Duration) []Bracket {
	cutoff := now.Add(-expireAfter)
	return []Bracket{
		{Tag: tagDateTime, Upper: encodeDateTime(int64(primitive.NewDateTimeFromTime(cutoff)))},
		{Tag: tagNumber, Upper: encodedSeconds(cutoff.Unix())},
	}
}

// encodedSeconds is the number key of a whole number of seconds. An int64 is
// always a finite number, which EncodeComponent always encodes.
func encodedSeconds(seconds int64) []byte {
	encoded, _, _ := encodeNumber(seconds)
	return encoded
}

// ExpiryCondition is the bound a sweep computed once, on the owner, from its
// clock, carried in the delete so no node reads its own: a document expires
// when Field holds a date at or before DateBefore (Unix milliseconds) or a
// number at or before SecondsBefore (Unix seconds).
type ExpiryCondition struct {
	Field         string
	DateBefore    int64
	SecondsBefore int64
}

// Expired applies the two brackets of TTLBounds to one value: a date or a
// number past the bound expires; anything else has no expiry.
func (c ExpiryCondition) Expired(value interface{}) bool {
	encoded, err := EncodeComponent(value)
	if err != nil {
		return false
	}
	switch encoded[0] {
	case tagDateTime:
		return bytes.Compare(encoded, encodeDateTime(c.DateBefore)) <= 0
	case tagNumber:
		return bytes.Compare(encoded, encodedSeconds(c.SecondsBefore)) <= 0
	}
	return false
}
