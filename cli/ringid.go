package cli

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/store"
)

// ringMaxEncodedID is the largest document id the ring takes, in the bytes of its
// store.EncodeComponent encoding: cluster/wire/delete_reclaim.go:maxEncodedID, which is
// unexported and which the CLI may not edit, so the value is copied here and a live test
// (TestIngestIDRefusesAnOverlongRenderingAndNamesTheDocument) is what notices a drift.
const ringMaxEncodedID = 1024

// errIngestNoID is the refusal of a document that has no _id member.
var errIngestNoID = errors.New("the document has no _id")

// ingestIDTooLongError is the refusal of an id whose rendering the ring cannot take.
type ingestIDTooLongError struct {
	// Rendered is the length of the rendered text, Encoded the length of its encoding.
	Rendered, Encoded int
}

// Error names the sizes and the limit.
func (e *ingestIDTooLongError) Error() string {
	return fmt.Sprintf("the _id renders to a text of %d bytes, which encodes to %d and a document id encodes to at most %d", e.Rendered, e.Encoded, ringMaxEncodedID)
}

// ingestID renders the _id of one document as the string the ring keeps as _id_ (build plan
// 4.4). An ObjectId and a plain string are bare; every other type, and a string that could be
// mistaken for one of them, carries a "~", a type letter and a colon.
func ingestID(doc bson.Raw) (string, error) {
	value, err := doc.LookupErr("_id")
	if err != nil {
		return "", errIngestNoID
	}
	return renderID(value)
}

// renderID renders one BSON value the way ingestID renders an _id, so that an element key
// taken from a value uses the same scheme.
func renderID(value bson.RawValue) (string, error) {
	rendered, err := renderIDText(value)
	if err != nil {
		return "", err
	}
	encoded, err := store.EncodeComponent(rendered)
	if err != nil {
		return "", fmt.Errorf("encoding the rendered _id %q: %w", truncateForMessage(rendered, 40), err)
	}
	if len(encoded) > ringMaxEncodedID {
		return "", &ingestIDTooLongError{Rendered: len(rendered), Encoded: len(encoded)}
	}
	return rendered, nil
}

// renderIDText is the table of build plan 4.4 and nothing else.
func renderIDText(value bson.RawValue) (string, error) {
	switch value.Type {
	case bsontype.ObjectID:
		objectID, ok := value.ObjectIDOK()
		if !ok {
			return "", errMalformedID(value)
		}
		return hex.EncodeToString(objectID[:]), nil
	case bsontype.String:
		text, ok := value.StringValueOK()
		if !ok {
			return "", errMalformedID(value)
		}
		if strings.HasPrefix(text, "~") || isLowercaseHex24(text) {
			return "~s:" + text, nil
		}
		return text, nil
	case bsontype.Int32:
		number, ok := value.Int32OK()
		if !ok {
			return "", errMalformedID(value)
		}
		return "~i:" + strconv.FormatInt(int64(number), 10), nil
	case bsontype.Int64:
		number, ok := value.Int64OK()
		if !ok {
			return "", errMalformedID(value)
		}
		return "~i:" + strconv.FormatInt(number, 10), nil
	case bsontype.Double:
		number, ok := value.DoubleOK()
		if !ok {
			return "", errMalformedID(value)
		}
		return renderDoubleID(number), nil
	case bsontype.Boolean:
		flag, ok := value.BooleanOK()
		if !ok {
			return "", errMalformedID(value)
		}
		return "~b:" + strconv.FormatBool(flag), nil
	case bsontype.DateTime:
		millis, ok := value.DateTimeOK()
		if !ok {
			return "", errMalformedID(value)
		}
		return "~t:" + strconv.FormatInt(millis, 10), nil
	case bsontype.Null:
		return "~z:", nil
	case bsontype.Binary:
		subtype, data, ok := value.BinaryOK()
		if !ok {
			return "", errMalformedID(value)
		}
		return fmt.Sprintf("~y:%02x:%s", subtype, hex.EncodeToString(data)), nil
	}
	return fmt.Sprintf("~x:%02x:%s", byte(value.Type), hex.EncodeToString(value.Value)), nil
}

// renderDoubleID renders a double: an integral value inside the int64 range is the integer
// of the same value (the one designed group of integers and doubles), anything else is "~d:"
// and the shortest text that parses back to the same double. The upper bound is strict: the
// float64 of math.MaxInt64 is 2^63, which int64 cannot hold, and converting it would give the
// int64 minimum and collide with the real one.
func renderDoubleID(number float64) string {
	switch {
	case math.IsNaN(number):
		return "~d:NaN"
	case math.IsInf(number, 1):
		return "~d:+Inf"
	case math.IsInf(number, -1):
		return "~d:-Inf"
	case number == math.Trunc(number) && number >= -(1<<63) && number < 1<<63:
		return "~i:" + strconv.FormatInt(int64(number), 10)
	}
	return "~d:" + strconv.FormatFloat(number, 'g', -1, 64)
}

func errMalformedID(value bson.RawValue) error {
	return fmt.Errorf("the _id is a %s whose bytes are not a valid %s", value.Type, value.Type)
}

// isLowercaseHex24 reports whether text is exactly the 24 lowercase hex digits an ObjectId
// renders to, which is why such a string is escaped: the two must stay two documents.
func isLowercaseHex24(text string) bool {
	if len(text) != 24 {
		return false
	}
	for _, character := range text {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// ingestIDRefusalText is the sentence for a document whose _id cannot be used, naming the
// document by its position in the file and the change that fixes it (rule 3).
func ingestIDRefusalText(err error, ordinal int64, file string) string {
	var tooLong *ingestIDTooLongError
	switch {
	case errors.Is(err, errIngestNoID):
		return fmt.Sprintf("Document %s of %s has no _id. hoarddb will not invent one, because running the file again would then add the document again. Give every document an _id.", formatCount(ordinal), file)
	case errors.As(err, &tooLong):
		return fmt.Sprintf("Document %s of %s has an _id that renders to a text of %s bytes, which encodes to %s bytes, and a document id encodes to at most %s bytes (a text of up to %s bytes fits). Change the _id in the file.",
			formatCount(ordinal), file, formatCount(int64(tooLong.Rendered)), formatCount(int64(tooLong.Encoded)), formatCount(ringMaxEncodedID), formatCount(ringMaxEncodedID-3))
	}
	return fmt.Sprintf("Document %s of %s has an _id that cannot be used: %v. Change the _id in the file.", formatCount(ordinal), file, err)
}
