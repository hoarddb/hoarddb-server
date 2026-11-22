package cli

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// readLimitOption reads the page size from a parsed options document and
// refuses what it cannot honour. A missing limit is 0, which the server reads
// as its default page. Anything else must be a whole number of at least 1; a
// string, fraction, zero or negative number is a usage error, because the
// request carries 0 for "default" and so a dropped value would be
// indistinguishable from the default page (BUG-0458). Keys outside allowed
// are refused too, so {limt: 3} is not silently ignored. The upper bound is
// the server's to enforce, so its number and message have one source.
func readLimitOption(options map[string]interface{}, allowed map[string]bool) (int, error) {
	for key := range options {
		if !allowed[key] {
			return 0, usagef("unknown option %q. This call accepts: %s. Example: {limit: 3}", key, joinSortedKeys(allowed))
		}
	}
	raw, present := options["limit"]
	if !present {
		return 0, nil
	}
	var number float64
	switch typed := raw.(type) {
	case float64:
		number = typed
	case int:
		number = float64(typed)
	case int64:
		number = float64(typed)
	default:
		return 0, usagef(`"limit" must be a whole number of at least 1, got %s. Example: {limit: 3}`, describeOptionValue(raw))
	}
	if number != math.Trunc(number) {
		return 0, usagef(`"limit" must be a whole number of at least 1, got the fraction %v. Example: {limit: 3}`, number)
	}
	if number < 1 {
		return 0, usagef(`"limit" must be a whole number of at least 1, got %v. Omit it for the server's default page, or write {limit: 3}`, number)
	}
	return int(number), nil
}

// readSkipOption reads the number of documents a find skips. A missing skip is
// 0, from the start. Anything else must be a whole number of at least 0: a
// string, fraction or negative value is a usage error rather than a silent
// start from the beginning (BUG-0478).
func readSkipOption(options map[string]interface{}) (int, error) {
	raw, present := options["skip"]
	if !present {
		return 0, nil
	}
	number, ok := optionNumber(raw)
	if !ok {
		return 0, usagef(`"skip" must be a whole number of at least 0, got %s. Example: {skip: 20}`, describeOptionValue(raw))
	}
	if number != math.Trunc(number) {
		return 0, usagef(`"skip" must be a whole number of at least 0, got the fraction %v. Example: {skip: 20}`, number)
	}
	if number < 0 {
		return 0, usagef(`"skip" must be a whole number of at least 0, got %v. Example: {skip: 20}`, number)
	}
	return int(number), nil
}

// readSortOption reads a find's sort: a map of field names to 1 (ascending) or
// -1 (descending). An absent sort is nil, which means no sort (BUG-0478).
func readSortOption(options map[string]interface{}) (map[string]int, error) {
	return readFieldMap(options, "sort", "{sort: {Age: 1}}", "1 (ascending) or -1 (descending)", 1, -1)
}

// readProjectionOption reads a find's projection: a map of field names to 1
// (include) or 0 (exclude). An absent projection is nil, which means whole
// documents (BUG-0478).
func readProjectionOption(options map[string]interface{}) (map[string]int, error) {
	return readFieldMap(options, "projection", "{projection: {Name: 1}}", "1 (include) or 0 (exclude)", 1, 0)
}

// readFieldMap reads an option that maps field names to one of the values
// allowed. Any other shape, or a value outside allowed, is a usage error.
func readFieldMap(options map[string]interface{}, key, example, want string, allowed ...int) (map[string]int, error) {
	raw, present := options[key]
	if !present {
		return nil, nil
	}
	fields, ok := raw.(map[string]interface{})
	if !ok {
		return nil, usagef("%q must map field names to %s, got %s. Example: %s", key, want, describeOptionValue(raw), example)
	}
	result := make(map[string]int, len(fields))
	for field, value := range fields {
		number, ok := optionNumber(value)
		if !ok || number != math.Trunc(number) || !containsInt(allowed, int(number)) {
			return nil, usagef("%q must map field names to %s, got %s for %q. Example: %s", key, want, describeOptionValue(value), field, example)
		}
		result[field] = int(number)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// optionNumber reads a number an options document holds, as JSON (float64) or
// as built in process (int, int64).
func optionNumber(raw interface{}) (float64, bool) {
	switch typed := raw.(type) {
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	}
	return 0, false
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// describeOptionValue names the type and value an option was given as.
func describeOptionValue(raw interface{}) string {
	switch typed := raw.(type) {
	case string:
		return fmt.Sprintf("a string (%q)", typed)
	case bool:
		return fmt.Sprintf("a boolean (%v)", typed)
	case nil:
		return "null"
	case map[string]interface{}:
		return "a document"
	case []interface{}:
		return "an array"
	default:
		return fmt.Sprintf("%T (%v)", raw, raw)
	}
}

func joinSortedKeys(keys map[string]bool) string {
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
