package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringMaxDeclaredFields is the most fields a bucket holds (entry-ring-conventions.md section 3).
const ringMaxDeclaredFields = 999

// tooManyFieldsError is the refusal of a file with more distinct top-level field names than a
// bucket can declare. It is raised before anything is created.
type tooManyFieldsError struct {
	Count int
	// AtLeast says Count stopped growing at the scan's bound, so the real count is higher.
	AtLeast bool
}

// Error states the facts without the file's name, which only the caller has.
func (e *tooManyFieldsError) Error() string {
	return fmt.Sprintf("the file has %s distinct field names and a bucket holds at most %d", e.countText(), ringMaxDeclaredFields)
}

func (e *tooManyFieldsError) countText() string {
	if e.AtLeast {
		return "more than " + formatCount(int64(e.Count))
	}
	return formatCount(int64(e.Count))
}

// fileText is the refusal with the file named and the fix said (build plan 4.3).
func (e *tooManyFieldsError) fileText(file string) string {
	return fmt.Sprintf("%s has %s distinct field names and a bucket holds at most %d. Nothing was created. Drop the fields you do not need from the file, or split it.",
		file, e.countText(), ringMaxDeclaredFields)
}

// ringWordForSingle maps the one word a field showed (after dropping null) to the ring's
// word. A word the table does not name is any: the ring has no word for it.
var ringWordForSingle = map[string]string{
	"string": "string", "int": "int", "double": "double", "128-bit decimal": "decimal",
	"bool": "bool", "datetime": "datetime", "objectid": "objectid", "binary": "binary",
	"object": "object", "array": "array", "regex": "regex", "timestamp": "bsontimestamp",
}

// inferDeclaration turns what pass 1 saw into the declaration of a new bucket (build plan 4.3):
// plain type words only, in the order the fields were first seen. The second result lists the
// fields whose several types forced the word any, so a dry run can say so.
func inferDeclaration(stats map[string]*planFieldStats, order []string) ([]ringclient.FieldDecl, []string, error) {
	if len(order) > ringMaxDeclaredFields {
		return nil, nil, &tooManyFieldsError{Count: len(order)}
	}
	fields := make([]ringclient.FieldDecl, 0, len(order))
	var notes []string
	for _, name := range order {
		word, mixed := ringWordFor(stats[name])
		fields = append(fields, ringclient.FieldDecl{Name: name, Type: word})
		if len(mixed) > 0 {
			notes = append(notes, fmt.Sprintf("Field %q holds %s values, so it is declared any.", name, joinWords(mixed)))
		}
	}
	return fields, notes, nil
}

// ringWordFor picks the declared word for one field. mixed names the words that no row of the
// table joins, which is empty unless the answer is any because of a mix.
func ringWordFor(stats *planFieldStats) (word string, mixed []string) {
	seen := make([]string, 0, len(stats.types))
	for typeWord := range stats.types {
		if typeWord != "null" {
			seen = append(seen, typeWord)
		}
	}
	sort.Strings(seen)
	switch {
	case len(seen) == 0:
		return "any", nil
	case len(seen) == 1:
		if ringWord, named := ringWordForSingle[seen[0]]; named {
			return ringWord, nil
		}
		return "any", nil
	case allNumeric(seen):
		return "number", nil
	}
	return "any", seen
}

// allNumeric reports whether every word is one of the three numeric ones, which one number
// field can hold together.
func allNumeric(words []string) bool {
	for _, word := range words {
		if word != "int" && word != "double" && word != "128-bit decimal" {
			return false
		}
	}
	return true
}

// joinWords renders "a", "a and b" or "a, b and c".
func joinWords(words []string) string {
	if len(words) <= 2 {
		return strings.Join(words, " and ")
	}
	return strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}
