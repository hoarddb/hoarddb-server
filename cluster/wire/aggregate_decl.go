package wire

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// AggregateDecl is one aggregate a create_bucket declares on a store field: Store names the
// field, Kind is "sum", "sum by" or "count by" (the grammar's words), Field is the summed
// element path (empty for count by) and By the grouping path (empty for sum).
type AggregateDecl struct {
	Store, Kind, Field, By string
}

// aggregateKindWords maps the grammar's kind words to the declaration's kind bytes.
var aggregateKindWords = map[string]uint8{"sum": machine.AggSum, "sum by": machine.AggSumBy, "count by": machine.AggCountBy}

// aggregateKindWord is the inverse of aggregateKindWords.
func aggregateKindWord(kind uint8) string {
	for word, kindByte := range aggregateKindWords {
		if kindByte == kind {
			return word
		}
	}
	return fmt.Sprintf("kind %d", kind)
}

// aggregateStoreWords are the store words that hold elements with fields; a blobStore
// element has none.
var aggregateStoreWords = "fifoStore, lifoStore, hashStore, btreeStore or heapStore"

// aggregateText renders an aggregate as the create grammar writes it.
func aggregateText(entry machine.AggregateEntry) string {
	switch entry.Kind {
	case machine.AggSum:
		return "sum " + entry.Field
	case machine.AggSumBy:
		return "sum " + entry.Field + " by " + entry.By
	default:
		return "count by " + entry.By
	}
}

// aggregateRefusal is the INVALID_ARGUMENT every aggregate check returns: the condition
// first, nothing was created, then the create statement that declares it correctly.
func aggregateRefusal(condition, dbName, bucket string, format string, args ...any) error {
	return refuse(cluster.CodeInvalidArgument, "%s: %s. Nothing was created in database %q for bucket %q. The form is 'create bucket Property in app { Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Category) }', with up to 8 aggregates per store field",
		condition, fmt.Sprintf(format, args...), dbName, bucket)
}

// buildAggregates validates the declared aggregates against the field table and mints their
// ids from 1 per store field, in the order written. Every refusal here repeats the CLI's, so a
// wire client that skips the grammar is held to the same rules.
func buildAggregates(dbName, bucket string, table machine.FieldTable, decls []AggregateDecl) (machine.AggregateTable, error) {
	if len(decls) == 0 {
		return machine.AggregateTable{}, nil
	}
	types := make(map[string]machine.FieldEntry, len(table.Entries))
	for _, entry := range table.Entries {
		types[entry.Name] = entry
	}
	sets := make(map[uint16]*machine.AggregateSet)
	for _, decl := range decls {
		entry, ok := types[decl.Store]
		switch {
		case !ok:
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_STORE_NOT_FOUND", dbName, bucket, "bucket %q declares no field %q to keep an aggregate on", bucket, decl.Store)
		case entry.Type == "blobStore":
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_ON_BLOB", dbName, bucket, "field %q is a blobStore and a blob element has no fields to sum or count by", decl.Store)
		case !strings.HasSuffix(entry.Type, "Store"):
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_ON_SCALAR", dbName, bucket, "field %q is %s, and an aggregate is a clause of a store field: %s", decl.Store, entry.Type, aggregateStoreWords)
		}
		kind, ok := aggregateKindWords[decl.Kind]
		if ok && ((kind != machine.AggCountBy && decl.Field == "") || (kind != machine.AggSum && decl.By == "") ||
			(kind == machine.AggSum && decl.By != "") || (kind == machine.AggCountBy && decl.Field != "")) {
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_PATH", dbName, bucket, "%q on %q needs the element field and by it names: sum <Field>, sum <Field> by <By>, count by <By>", decl.Kind, decl.Store)
		}
		if !ok {
			if decl.Kind == "count" {
				return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_PLAIN_COUNT", dbName, bucket, "every store already counts its elements: db.%s[\"<id>\"].%s.count()", bucket, decl.Store)
			}
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_KIND", dbName, bucket, "aggregate kind %q is not sum, sum by or count by", decl.Kind)
		}
		for _, path := range []string{decl.Field, decl.By} {
			if err := checkAggregatePath(path); err != nil {
				return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_PATH", dbName, bucket, "path %q on %q: %v", path, decl.Store, err)
			}
		}
		set := sets[entry.ID]
		if set == nil {
			set = &machine.AggregateSet{FieldID: entry.ID, NextID: 1}
			sets[entry.ID] = set
		}
		next := machine.AggregateEntry{ID: set.NextID, Kind: kind, Field: decl.Field, By: decl.By}
		for _, held := range set.Entries {
			if held.Kind == next.Kind && held.Field == next.Field && held.By == next.By {
				return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_DUPLICATE", dbName, bucket, "%q declares %q twice", decl.Store, aggregateText(next))
			}
		}
		if len(set.Entries) == machine.MaxAggregates {
			return machine.AggregateTable{}, aggregateRefusal("AGGREGATE_LIMIT", dbName, bucket, "field %q declares more than the limit of at most %d aggregates", decl.Store, machine.MaxAggregates)
		}
		set.Entries = append(set.Entries, next)
		set.NextID++
	}
	out := machine.AggregateTable{Sets: make([]machine.AggregateSet, 0, len(sets))}
	for _, set := range sets {
		out.Sets = append(out.Sets, *set)
	}
	sort.Slice(out.Sets, func(i, j int) bool { return out.Sets[i].FieldID < out.Sets[j].FieldID })
	return out, nil
}

// replyAggregates lists a bucket's aggregates for describe bucket: by field id, then id.
func replyAggregates(table machine.FieldTable, aggregates machine.AggregateTable) []RingAdminReplyAggregate {
	if len(aggregates.Sets) == 0 {
		return nil
	}
	names := make(map[uint16]string, len(table.Entries))
	for _, entry := range table.Entries {
		names[entry.ID] = entry.Name
	}
	var out []RingAdminReplyAggregate
	for _, set := range aggregates.Sets {
		if _, declared := names[set.FieldID]; !declared {
			continue // the store was dropped; its set goes at the next fold
		}
		for _, entry := range set.Entries {
			state := "ready"
			if entry.State == machine.AggStateBuilding {
				state = "building"
			}
			out = append(out, RingAdminReplyAggregate{Store: names[set.FieldID], ID: entry.ID, Kind: aggregateKindWord(entry.Kind), Field: entry.Field, By: entry.By, State: state})
		}
	}
	return out
}

// checkAggregatePath refuses an element path the declaration cannot hold: empty segments,
// more than machine.MaxAggregatePath keys, over 255 bytes, or characters the grammar's word
// does not allow. An empty path is fine here; the kind decides which paths are required.
func checkAggregatePath(path string) error {
	if path == "" {
		return nil
	}
	keys := strings.Split(path, ".")
	if len(keys) > machine.MaxAggregatePath || len(path) > 255 {
		return fmt.Errorf("a path is at most %d keys and 255 bytes", machine.MaxAggregatePath)
	}
	for _, key := range keys {
		if key == "" || strings.IndexFunc(key, func(r rune) bool {
			return r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
		}) >= 0 {
			return fmt.Errorf("each key is letters, digits and _")
		}
	}
	return nil
}
