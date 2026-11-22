package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The normalisations every capture goes through. Each is a sentence in the
// file's "normalisations" list, so a reviewer can re-capture by hand.
const (
	normaliseVary = "Values that differ from run to run (each listed in its vector's response.vary, a path with [] " +
		"for every array element) are replaced inside the BSON by a fixed value of the same BSON type: a string by " +
		"\"normalised\", an int32 or int64 by 1, a double by 1.0, a datetime by 2026-01-01T00:00:00Z, a binary by the " +
		"bytes of \"normalised\" with its subtype kept, an array by an empty array and a document by an empty document; " +
		"the payload is then re-encoded. A driver checks a varying field's presence and BSON type, not its value."
	normaliseStored = "Document field order: the server holds a stored document, and every document a request type " +
		"declares as a map, as a Go map and encodes it in an unspecified order that differs from run to run. So the " +
		"fields of every nested document in a request, and of every stored document in an answer (under data, doc, " +
		"documents, results, value, element, metadata and values, and the document after a GET_FAST answer's " +
		"status byte), are sorted by name. A driver compares documents " +
		"field by field, never by their bytes."
	normaliseAddresses = "Loopback ports the run happened to get are rewritten to fixed ones inside every string " +
		"outside a stored document (the single node's driver port to 7433 and ring port to 4433; node1, node2 and " +
		"node3 of the three-node cluster to driver ports 7433, 7434, 7435 and ring ports 4433, 4435, 4437; the " +
		"not-yet-added node to 7436 and 4439), and the run's temporary directory to /data."
	normaliseRequestIDs = "Request IDs are fixed: the 12 bytes of \"vector-id-v1\", then the exchange's number, " +
		"big-endian, in the last 4 bytes. Database names are fixed (v001, v002, ...), one per exchange."
	normaliseIdentities = "Server identities are fixed ed25519 keys made from fixed seeds, so fingerprints are " +
		"the same in every run and are not normalised."
	normaliseMapError = "An ERROR answer written by the transport layer (its only fields are code and message) is " +
		"encoded from a Go map, so its two fields arrive in either order; they are recorded sorted (code, then message)."
	normaliseDropped = "Fields that describe the capturing host rather than the protocol (health's capacity, wal and " +
		"memtable) are removed from the captured answer and listed in additive_fields: a driver must accept them " +
		"and may not require them."
)

// varyNames are the field names whose value differs run to run wherever they
// appear in an answer outside a stored document.
var varyNames = []string{
	"token", "token_expires_at", "token_deadline", "server_version",
	"uptime", "version", "seq", "bucket_id", "blobid", "blob_id",
	"joinedat", "entry_id", "lease_id", "lease_deadline",
}

// storedDocumentNames are the answer fields that carry a stored document (or
// an array of them), whose field order the server does not keep.
var storedDocumentNames = []string{
	"data", "doc", "documents", "results", "value", "element", "metadata", "values", "items",
}

// normaliseRules is how one answer is normalised.
type normaliseRules struct {
	// vary are field names normalised wherever they appear outside a stored
	// document; varyPaths are exact paths ("data._id_", "ids[]"), normalised
	// even inside one.
	vary      []string
	varyPaths []string
	// drop are top-level fields removed from the answer.
	drop     []string
	replacer *strings.Replacer
	// sortTop sorts the top-level fields (normaliseMapError).
	sortTop bool
}

// normalised is an answer after its normalisation.
type normalised struct {
	payload []byte
	vary    []string
	dropped []string
}

// fixed values, one per BSON type a varying field has been seen with.
var (
	fixedString   = "normalised"
	fixedDateTime = int64(1767225600000) // 2026-01-01T00:00:00Z
)

// rewriter walks one payload.
type rewriter struct {
	rules   normaliseRules
	request bool
	vary    []string
	dropped []string
}

// canonicalRequest re-encodes a request the server's encoder produced with
// every nested document's fields sorted, keeping the top level (a struct, in
// field order) as it is. See normaliseStored.
func canonicalRequest(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}
	walk := &rewriter{request: true}
	return walk.document(payload, "", false, false)
}

// normaliseAnswer applies rules to a captured answer.
func normaliseAnswer(payload []byte, rules normaliseRules) (normalised, error) {
	if len(payload) == 0 {
		return normalised{payload: payload}, nil
	}
	walk := &rewriter{rules: rules}
	rewritten, err := walk.document(payload, "", rules.sortTop, false)
	if err != nil {
		return normalised{}, err
	}
	slices.Sort(walk.vary)
	return normalised{payload: rewritten, vary: slices.Compact(walk.vary), dropped: walk.dropped}, nil
}

// document rebuilds one document at path. sorted sorts its fields by name;
// stored says it is (inside) a stored document.
func (w *rewriter) document(raw []byte, path string, sorted, stored bool) ([]byte, error) {
	elements, err := bsoncore.Document(raw).Elements()
	if err != nil {
		return nil, fmt.Errorf("reading the document at %q: %w", displayPath(path), err)
	}
	if sorted {
		sort.SliceStable(elements, func(left, right int) bool { return elements[left].Key() < elements[right].Key() })
	}
	index, rebuilt := bsoncore.AppendDocumentStart(nil)
	for _, element := range elements {
		key := element.Key()
		value := element.Value()
		childPath := joinPath(path, key)
		if path == "" && slices.Contains(w.rules.drop, key) {
			w.dropped = append(w.dropped, key)
			continue
		}
		if slices.Contains(w.rules.varyPaths, childPath) || !stored && slices.Contains(w.rules.vary, key) {
			rebuilt = appendFixed(rebuilt, key, value)
			w.vary = append(w.vary, childPath)
			continue
		}
		childStored := stored || !w.request && slices.Contains(storedDocumentNames, key)
		childSorted := w.request || childStored
		rebuilt, err = w.value(rebuilt, key, value, childPath, childSorted, childStored)
		if err != nil {
			return nil, err
		}
	}
	rebuilt, err = bsoncore.AppendDocumentEnd(rebuilt, index)
	if err != nil {
		return nil, fmt.Errorf("closing the document at %q: %w", displayPath(path), err)
	}
	return rebuilt, nil
}

// value appends one element, recursing into documents and arrays and
// rewriting strings outside stored documents.
func (w *rewriter) value(dst []byte, key string, value bsoncore.Value, path string, sorted, stored bool) ([]byte, error) {
	switch value.Type {
	case bson.TypeEmbeddedDocument:
		inner, err := w.document(value.Data, path, sorted, stored)
		if err != nil {
			return nil, err
		}
		return bsoncore.AppendDocumentElement(dst, key, inner), nil
	case bson.TypeArray:
		inner, err := w.array(value.Data, path, sorted, stored)
		if err != nil {
			return nil, err
		}
		return bsoncore.AppendArrayElement(dst, key, inner), nil
	case bson.TypeString:
		if w.rules.replacer != nil && !stored {
			return bsoncore.AppendStringElement(dst, key, w.rules.replacer.Replace(value.StringValue())), nil
		}
	}
	return append(bsoncore.AppendHeader(dst, value.Type, key), value.Data...), nil
}

// array rebuilds an array in its order; its element documents are rewritten
// as the array's own fields would be.
func (w *rewriter) array(raw []byte, path string, sorted, stored bool) ([]byte, error) {
	values, err := bsoncore.Document(raw).Values()
	if err != nil {
		return nil, fmt.Errorf("reading the array at %q: %w", displayPath(path), err)
	}
	index, rebuilt := bsoncore.AppendArrayStart(nil)
	for position, element := range values {
		elementPath := path + "[]"
		if slices.Contains(w.rules.varyPaths, elementPath) {
			rebuilt = appendFixed(rebuilt, fmt.Sprint(position), element)
			w.vary = append(w.vary, elementPath)
			continue
		}
		rebuilt, err = w.value(rebuilt, fmt.Sprint(position), element, elementPath, sorted, stored)
		if err != nil {
			return nil, err
		}
	}
	rebuilt, err = bsoncore.AppendArrayEnd(rebuilt, index)
	if err != nil {
		return nil, fmt.Errorf("closing the array at %q: %w", displayPath(path), err)
	}
	return rebuilt, nil
}

// appendFixed appends key with the fixed value of value's BSON type.
func appendFixed(dst []byte, key string, value bsoncore.Value) []byte {
	switch value.Type {
	case bson.TypeString:
		return bsoncore.AppendStringElement(dst, key, fixedString)
	case bson.TypeInt32:
		return bsoncore.AppendInt32Element(dst, key, 1)
	case bson.TypeInt64:
		return bsoncore.AppendInt64Element(dst, key, 1)
	case bson.TypeDouble:
		return bsoncore.AppendDoubleElement(dst, key, 1.0)
	case bson.TypeDateTime:
		return bsoncore.AppendDateTimeElement(dst, key, fixedDateTime)
	case bson.TypeBinary:
		subtype, _ := value.Binary()
		return bsoncore.AppendBinaryElement(dst, key, subtype, []byte(fixedString))
	case bson.TypeObjectID:
		return bsoncore.AppendObjectIDElement(dst, key, primitive.ObjectID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	case bson.TypeArray:
		return bsoncore.AppendArrayElement(dst, key, bsoncore.BuildArray(nil))
	case bson.TypeEmbeddedDocument:
		return bsoncore.AppendDocumentElement(dst, key, bsoncore.BuildDocument(nil))
	default:
		return append(bsoncore.AppendHeader(dst, value.Type, key), value.Data...)
	}
}

// normaliseGetFast normalises a GET_FAST answer: the status byte, then the
// stored document, whose fields are sorted (normaliseStored).
func normaliseGetFast(payload []byte) ([]byte, error) {
	if len(payload) <= 1 {
		return payload, nil
	}
	walk := &rewriter{}
	document, err := walk.document(payload[1:], "", true, true)
	if err != nil {
		return nil, fmt.Errorf("the GET_FAST answer's document: %w", err)
	}
	return append([]byte{payload[0]}, document...), nil
}

// mapEncodedError says whether an ERROR payload is the transport's map-encoded
// {code, message} (transport.WriteErrorResponse).
func mapEncodedError(payload []byte) bool {
	elements, err := bsoncore.Document(payload).Elements()
	if err != nil || len(elements) != 2 {
		return false
	}
	names := []string{elements[0].Key(), elements[1].Key()}
	slices.Sort(names)
	return names[0] == "code" && names[1] == "message"
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func displayPath(path string) string {
	if path == "" {
		return "(top level)"
	}
	return path
}
