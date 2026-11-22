package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// traceOpWords are the operations a trace line may name, in the order a
// refusal lists them.
var traceOpWords = []string{"get", "list", "range", "insert", "update", "delete", "take", "put", "join", "aggregate", "search"}

// TraceLine is one operation of the access trace (spec section 4.2) with the
// section 17.2 additions. Ordinal is the 1-based line number in the trace file,
// so a refusal and a plan's evidence point at the line the author sees.
type TraceLine struct {
	Ordinal    int               `json:"-"`
	Op         string            `json:"op" doc:"what the application does: get, list, range, insert, update, delete, take, put, join, aggregate or search"`
	Coll       string            `json:"coll" doc:"a kind name from the model"`
	By         []string          `json:"by,omitempty" doc:"fields the operation selects on; _id is a read by key"`
	Sort       []string          `json:"sort,omitempty" doc:"field:asc or field:desc"`
	SortFrom   string            `json:"sort_from,omitempty" doc:"who chooses the sort: client, fixed or setting"`
	Order      string            `json:"order,omitempty" doc:"for take: oldest, newest or priority:<field>"`
	Key        string            `json:"key,omitempty" doc:"for range: the field whose range is read"`
	Limit      int               `json:"limit,omitempty" doc:"the usual page size"`
	BytesP50   int64             `json:"bytes_p50,omitempty" doc:"for put: the usual size of the bytes"`
	Weight     int               `json:"weight,omitempty" doc:"how often, relative to a line with no weight; 0 means 1"`
	Surface    string            `json:"surface,omitempty" doc:"primary (default) or admin; admin lines earn at most a partial"`
	Filters    []TraceFilter     `json:"filters,omitempty" doc:"filters a list composes at run time"`
	Inc        []string          `json:"inc,omitempty" doc:"fields an update increments by hand"`
	AtomicWith string            `json:"atomic_with,omitempty" doc:"a group name: writes one transaction performs together"`
	Guard      map[string]string `json:"guard,omitempty" doc:"an update that runs only if a field holds a value"`
	Join       *TraceJoin        `json:"join,omitempty" doc:"a join edge: to, on and kind"`
	Dynamic    bool              `json:"dynamic,omitempty" doc:"the dimension or segment is chosen at run time"`
	Substring  bool              `json:"substring,omitempty" doc:"a substring, regex or full-text match; fails rule 1 unless a search line delegates it"`
	Visibility string            `json:"visibility,omitempty" doc:"owner, membership, container, object_scoped or object_global"`
}

// TraceFilter is one filter field of a list; Optional means composed at run time.
type TraceFilter struct {
	Field    string `json:"field" doc:"the filter field"`
	Optional bool   `json:"optional,omitempty" doc:"true when the filter is composed at run time"`
}

// TraceJoin is a join edge classified by the extractor (spec section 15).
// Kind is one of pk, nonkey, visibility, aggregate or sort.
type TraceJoin struct {
	To   string `json:"to" doc:"the kind joined to"`
	On   string `json:"on" doc:"the field joined on"`
	Kind string `json:"kind" doc:"pk, nonkey, visibility, aggregate or sort"`
}

// LoadTrace reads a JSON Lines trace against the model's kind names. A line
// with an unknown op, an unknown coll or an unknown field is refused with its
// line number, the value and the accepted values, so the author can repair the
// line without reading this code. Blank lines are skipped but still counted.
func LoadTrace(src io.Reader, kinds []string) ([]TraceLine, error) {
	sortedKinds := slices.Sorted(slices.Values(kinds))
	scanner := bufio.NewScanner(src)
	var lines []TraceLine
	ordinal := 0
	for scanner.Scan() {
		ordinal++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		line, err := parseTraceLine(text, ordinal, sortedKinds)
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("trace: read after line %d: %w", ordinal, err)
	}
	return lines, nil
}

// parseTraceLine decodes one non-blank line and checks its op and coll.
func parseTraceLine(text string, ordinal int, kinds []string) (TraceLine, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	var line TraceLine
	if err := decoder.Decode(&line); err != nil {
		return TraceLine{}, traceDecodeError(ordinal, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return TraceLine{}, fmt.Errorf("trace line %d: the line holds more than one JSON object. Fix it so each line is one object", ordinal)
	}
	line.Ordinal = ordinal
	if !slices.Contains(traceOpWords, line.Op) {
		return TraceLine{}, fmt.Errorf("trace line %d: op %q is not one of %s. Fix the op, or delete the line",
			ordinal, line.Op, strings.Join(traceOpWords, ", "))
	}
	if !slices.Contains(kinds, line.Coll) {
		return TraceLine{}, fmt.Errorf("trace line %d: coll %q is not a kind in this model (kinds: %s). Fix the name, or add the kind to the model",
			ordinal, line.Coll, strings.Join(kinds, ", "))
	}
	return line, nil
}

// traceDecodeError names the nearest known key for an unknown field, as
// LoadPlanModel does for the model file; other decode errors keep their wrapping.
func traceDecodeError(ordinal int, err error) error {
	quoted, unknown := strings.CutPrefix(err.Error(), "json: unknown field ")
	if !unknown {
		return fmt.Errorf("trace line %d: %w", ordinal, err)
	}
	name, unquoteErr := strconv.Unquote(quoted)
	if unquoteErr != nil {
		return fmt.Errorf("trace line %d: %w", ordinal, err)
	}
	keys := traceJSONKeys()
	best, bestDistance := "", 3
	for _, key := range keys {
		if distance := ringEditDistance(name, key); distance < bestDistance {
			best, bestDistance = key, distance
		}
	}
	if best != "" {
		return fmt.Errorf("trace line %d: unknown field %q. Did you mean %q? Fix the name, or delete the field", ordinal, name, best)
	}
	return fmt.Errorf("trace line %d: unknown field %q. The known fields are %s. Fix the name, or delete the field",
		ordinal, name, strings.Join(keys, ", "))
}

// traceJSONKeys lists the JSON keys of a trace line and of its nested filter
// and join, sorted, so a refusal prints the same text every run.
func traceJSONKeys() []string {
	var keys []string
	for _, typ := range []reflect.Type{reflect.TypeFor[TraceLine](), reflect.TypeFor[TraceFilter](), reflect.TypeFor[TraceJoin]()} {
		for index := range typ.NumField() {
			name, _, _ := strings.Cut(typ.Field(index).Tag.Get("json"), ",")
			if name != "" && name != "-" {
				keys = append(keys, name)
			}
		}
	}
	slices.Sort(keys)
	return keys
}
