package server

import (
	"bytes"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// A client sends now() as a marker value: the BSON DBPointer whose namespace
// is nowMarkerNamespace and whose id is all zero bytes. A write's markers are
// replaced with one instant on the node the write enters, before it is logged
// (resolveDocumentIDs; resolveNowAtTheDoor on a node that logs nothing), so the
// owner, every replica and every replay apply the same bytes. An applier never
// reads this clock.
//
// The marker is a deprecated type with a reserved name because no value a user
// stores has either by accident. The earlier sentinel, the sub-document
// {"$$now": true}, is plain JSON, and a user who stored it got a date back
// (BUG-0151); that sub-document is now stored as written.

// nowMarkerNamespace is the marker's reserved namespace.
const nowMarkerNamespace = "$$hoardDB.now"

// nowSentinelKey is the marker's namespace as bytes. A payload whose bytes do
// not hold it has no now() in it, so the common write pays one scan of its
// bytes and no walk of its documents.
var nowSentinelKey = []byte(nowMarkerNamespace)

// nowMarker is the value a client sends for now().
var nowMarker = primitive.DBPointer{DB: nowMarkerNamespace, Pointer: primitive.NilObjectID}

// NowMarker returns the value a client sends where the user wrote now(): the
// server replaces it, and only it, with the server's time of the write. The
// CLI builds it after it has refused every deprecated type in what the user
// typed, so typed text cannot produce it.
func NowMarker() primitive.DBPointer { return nowMarker }

// nowClockHook, when set, is the clock now() reads instead of time.Now. It is
// a test seam: a clock that moves on every reading makes two readings of one
// request visible as two instants, which is how a test tells "stamped once"
// from "stamped by each copy" without depending on millisecond timing.
var nowClockHook atomic.Pointer[func() time.Time]

// nowInstant is one reading of the clock now() is substituted from.
func nowInstant() primitive.DateTime {
	clock := time.Now
	if hook := nowClockHook.Load(); hook != nil {
		clock = *hook
	}
	return primitive.NewDateTimeFromTime(clock().UTC())
}

// nowStamp fixes one request's now() calls to one instant. The clock is read
// once, and only for a payload that holds the sentinel's key, so every
// sentinel in a request — a batch's documents, an update's operators — is the
// same date, which is what a client expects of one request.
type nowStamp struct {
	present  bool
	read     bool
	at       primitive.DateTime
	replaced bool
}

// newNowStamp is the stamp for one request's payload.
func newNowStamp(payload []byte) *nowStamp {
	return &nowStamp{present: bytes.Contains(payload, nowSentinelKey)}
}

// fields replaces the sentinels among a document's fields and returns the
// document, a copy when anything was replaced. The document itself is never
// taken for a marker: a whole document is not a date.
func (n *nowStamp) fields(document map[string]interface{}) map[string]interface{} {
	if !n.present {
		return document
	}
	if !n.read {
		n.at, n.read = nowInstant(), true
	}
	stamped, replaced := replaceNowInFields(document, n.at)
	n.replaced = n.replaced || replaced
	return stamped
}

// replaceNowSentinels returns value with every marker in it replaced by at,
// and whether it replaced any. It copies only along the path to a marker:
// value is never modified, and a value without one is returned as it is,
// with nothing allocated.
func replaceNowSentinels(value interface{}, at primitive.DateTime) (interface{}, bool) {
	switch typed := value.(type) {
	case primitive.DBPointer:
		if typed == nowMarker {
			return at, true
		}
	case map[string]interface{}:
		return replaceNowInFields(typed, at)
	case []interface{}:
		return replaceNowInElements(typed, at)
	case primitive.A:
		// A document off the wire holds its arrays as primitive.A, not
		// []interface{}; the result keeps the type.
		return replaceNowInElements(typed, at)
	case primitive.D:
		// bson.D is primitive.D. Only an in-process caller holds one; the wire
		// decodes a document into a map.
		var stamped primitive.D
		for position, element := range typed {
			next, replaced := replaceNowSentinels(element.Value, at)
			if !replaced {
				continue
			}
			if stamped == nil {
				stamped = slices.Clone(typed)
			}
			stamped[position].Value = next
		}
		if stamped == nil {
			return typed, false
		}
		return stamped, true
	}
	return value, false
}

// replaceNowInFields is replaceNowSentinels over a document's fields.
func replaceNowInFields(document map[string]interface{}, at primitive.DateTime) (map[string]interface{}, bool) {
	var stamped map[string]interface{}
	for name, field := range document {
		next, replaced := replaceNowSentinels(field, at)
		if !replaced {
			continue
		}
		if stamped == nil {
			stamped = maps.Clone(document)
		}
		stamped[name] = next
	}
	if stamped == nil {
		return document, false
	}
	return stamped, true
}

// replaceNowInElements is replaceNowSentinels over an array's elements.
func replaceNowInElements[Array ~[]interface{}](elements Array, at primitive.DateTime) (Array, bool) {
	var stamped Array
	for position, element := range elements {
		next, replaced := replaceNowSentinels(element, at)
		if !replaced {
			continue
		}
		if stamped == nil {
			stamped = slices.Clone(elements)
		}
		stamped[position] = next
	}
	if stamped == nil {
		return elements, false
	}
	return stamped, true
}
