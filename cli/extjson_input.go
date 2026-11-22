package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// This file is the data half of the CLI's document reader (typed-fields spec
// 7.2): relaxed Extended JSON, the rule `hoarddb ingest` already follows, so a
// number or a wrapper means the same thing whichever text input carried it.

// supportedWrappers is the order the usage errors list the wrappers in, and the
// shape each one is checked against when a document is refused.
var supportedWrappers = []struct{ name, shape string }{
	{"$numberInt", `{"$numberInt": "42"}`},
	{"$numberLong", `{"$numberLong": "3000000000"}`},
	{"$numberDouble", `{"$numberDouble": "1.5"}`},
	{"$numberDecimal", `{"$numberDecimal": "12.34"}`},
	{"$binary", `{"$binary": {"base64": "aGk=", "subType": "00"}}`},
	{"$uuid", `{"$uuid": "d9c3b3a0-1c3a-4f5e-8a3a-0123456789ab"} (8-4-4-4-12 hex digits)`},
	{"$oid", `{"$oid": "507f1f77bcf86cd799439011"} (24 hex digits)`},
	{"$regularExpression", `{"$regularExpression": {"pattern": "^a", "options": "i"}}`},
	{"$timestamp", `{"$timestamp": {"t": 1700000000, "i": 1}} ("t" and "i" are whole numbers)`},
	{"$date", `{"$date": "2026-09-19T16:47:43Z"} (or ISODate("2026-09-19T16:47:43Z"))`},
}

// decodeDataDocument turns strict JSON (already rewritten from the relaxed
// spelling) into a document whose values are BSON types. original is the text
// the developer typed, quoted back in the errors.
func decodeDataDocument(original, strict string) (map[string]interface{}, error) {
	tree, err := checkedJSONTree(original, strict)
	if err != nil {
		return nil, err
	}
	var document map[string]interface{}
	if err := bson.UnmarshalExtJSON([]byte(strict), false, &document); err != nil {
		if wrapperErr := malformedWrapper(tree); wrapperErr != nil {
			return nil, wrapperErr
		}
		return nil, usagef("could not read document %s: %v", original, err)
	}
	if err := refuseDeprecatedTypes(document); err != nil {
		return nil, err
	}
	markNowCalls(document)
	return document, nil
}

// checkedJSONTree decodes strict with encoding/json and UseNumber, for the two
// checks the BSON reader cannot make: text after the document, which it
// ignores, and an integer above int64, which it turns into a double (so the
// server would later say the value was "written with a decimal point").
func checkedJSONTree(original, strict string) (map[string]interface{}, error) {
	decoder := json.NewDecoder(strings.NewReader(strict))
	decoder.UseNumber()
	var tree map[string]interface{}
	if err := decoder.Decode(&tree); err != nil {
		return nil, usagef(
			"could not read document %s: %v\n       expected JSON like {\"name\": \"ada\"}, or relaxed form {name: \"ada\"}",
			original, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, usagef("could not read document %s: there is text after the closing brace\n       pass one document per argument", original)
	}
	if err := refuseHugeIntegers(tree); err != nil {
		return nil, err
	}
	return tree, nil
}

// refuseHugeIntegers walks a UseNumber tree for an integer literal that does
// not fit int64.
func refuseHugeIntegers(node interface{}) error {
	switch typed := node.(type) {
	case json.Number:
		text := typed.String()
		if strings.ContainsAny(text, ".eE") {
			return nil
		}
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return usagef("the integer %s is outside the range of int64 (-9223372036854775808 to 9223372036854775807)\n       write it with a decimal point to store a double, or as {\"$numberDecimal\": \"%s\"} to keep every digit", text, text)
		}
	case map[string]interface{}:
		for _, member := range typed {
			if err := refuseHugeIntegers(member); err != nil {
				return err
			}
		}
	case []interface{}:
		for _, element := range typed {
			if err := refuseHugeIntegers(element); err != nil {
				return err
			}
		}
	}
	return nil
}

// malformedWrapper finds the innermost wrapper in tree that the BSON reader
// refuses on its own and says which wrapper it is and what shape it takes. It
// returns nil when no single wrapper is the cause.
func malformedWrapper(node interface{}) error {
	switch typed := node.(type) {
	case map[string]interface{}:
		for _, member := range typed {
			if err := malformedWrapper(member); err != nil {
				return err
			}
		}
		return refusedWrapper(typed)
	case []interface{}:
		for _, element := range typed {
			if err := malformedWrapper(element); err != nil {
				return err
			}
		}
	}
	return nil
}

// refusedWrapper checks one object that is shaped like a wrapper, a single
// supported "$..." key, by decoding it alone.
func refusedWrapper(object map[string]interface{}) error {
	if len(object) != 1 {
		return nil
	}
	var key string
	var member interface{}
	for key, member = range object {
	}
	shape := ""
	for _, wrapper := range supportedWrappers {
		if wrapper.name == key {
			shape = wrapper.shape
		}
	}
	if shape == "" {
		return nil
	}
	encoded, err := json.Marshal(map[string]interface{}{"v": object})
	if err != nil {
		return fmt.Errorf("re-encoding %s: %w", key, err)
	}
	var decoded map[string]interface{}
	if bson.UnmarshalExtJSON(encoded, false, &decoded) == nil {
		return nil
	}
	if text, isText := member.(string); isText && key == "$date" && strings.Contains(text, " ") {
		return usagef("named time zones are not supported: use an offset instead, e.g. +02:00 instead of Europe/Tirane")
	}
	return usagef("%s is malformed: it takes the form %s", key, shape)
}

// refuseDeprecatedTypes walks a decoded document for the BSON types the
// deprecated Extended JSON wrappers revive. The reader accepts them, so
// without this a typed document could carry a type no driver should write.
func refuseDeprecatedTypes(node interface{}) error {
	switch typed := node.(type) {
	case primitive.Symbol:
		return deprecatedWrapper("$symbol")
	case primitive.JavaScript, primitive.CodeWithScope:
		return deprecatedWrapper("$code")
	case primitive.DBPointer:
		return deprecatedWrapper("$dbPointer")
	case primitive.Undefined:
		return deprecatedWrapper("$undefined")
	case primitive.MinKey:
		return deprecatedWrapper("$minKey")
	case primitive.MaxKey:
		return deprecatedWrapper("$maxKey")
	case map[string]interface{}:
		for _, member := range typed {
			if err := refuseDeprecatedTypes(member); err != nil {
				return err
			}
		}
	case primitive.A:
		for _, element := range typed {
			if err := refuseDeprecatedTypes(element); err != nil {
				return err
			}
		}
	case primitive.D:
		for _, member := range typed {
			if err := refuseDeprecatedTypes(member.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func deprecatedWrapper(name string) error {
	names := make([]string, len(supportedWrappers))
	for index, wrapper := range supportedWrappers {
		names[index] = wrapper.name
	}
	return usagef("%s is a deprecated BSON type and hoardDB does not store it\n       the supported wrappers are %s", name, strings.Join(names, ", "))
}

// strictDocumentText trims arg and returns it as strict JSON: unchanged when
// it already is, rewritten from the relaxed spelling when it is not.
func strictDocumentText(arg string) (string, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", &UsageError{Message: `empty document — pass JSON, e.g. {"name": "ada"}`}
	}
	if json.Valid([]byte(arg)) {
		return arg, nil
	}
	return normalizeDocument(arg)
}

// decodeOptionDocument is the option reader: plain encoding/json, so every
// number is a float64 and every list a []interface{}.
func decodeOptionDocument(original, strict string) (map[string]interface{}, error) {
	var document map[string]interface{}
	if err := json.Unmarshal([]byte(strict), &document); err != nil {
		return nil, usagef(
			"could not read document %s: %v\n       expected JSON like {\"name\": \"ada\"}, or relaxed form {name: \"ada\"}",
			original, err)
	}
	if holdsNowCall(document) {
		return nil, usagef("could not read document %s: now() is a value in a document you store or match, not in an options document\n"+
			"       write it in the document of an insert, update or find", original)
	}
	return document, nil
}

// holdsNowCall reports whether a decoded options document holds the
// placeholder normalizeDocument writes for a now() call.
func holdsNowCall(node interface{}) bool {
	switch typed := node.(type) {
	case map[string]interface{}:
		if _, isCall := typed[nowCallKey]; isCall {
			return true
		}
		for _, member := range typed {
			if holdsNowCall(member) {
				return true
			}
		}
	case []interface{}:
		for _, element := range typed {
			if holdsNowCall(element) {
				return true
			}
		}
	}
	return false
}
