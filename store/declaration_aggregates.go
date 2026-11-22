package store

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// declFieldAggregates is the bucket's maintained aggregates (aggregate spec 2.2). Absent
// means none, so a declaration written before aggregates existed decodes unchanged.
const declFieldAggregates = machine.SystemOptionalAggregates

// aggregateTableFormat is the first byte of the field 12 table.
const aggregateTableFormat byte = 1

// EncodeAggregateTable packs field 12: [v:1=1][nsets:1] then per store field
// [field_id:2 BE][next_agg_id:1][n:1] and per aggregate
// [agg_id:1][kind:1][state:1][fLen:1][field][bLen:1][by]. The caller has checked the table
// with checkAggregateTable; DecodeAggregateTable checks the bytes.
func EncodeAggregateTable(table machine.AggregateTable) []byte {
	out := []byte{aggregateTableFormat, byte(len(table.Sets))}
	for _, set := range table.Sets {
		out = binary.BigEndian.AppendUint16(out, set.FieldID)
		out = append(out, set.NextID, byte(len(set.Entries)))
		for _, entry := range set.Entries {
			out = append(out, entry.ID, entry.Kind, entry.State, byte(len(entry.Field)))
			out = append(out, entry.Field...)
			out = append(out, byte(len(entry.By)))
			out = append(out, entry.By...)
		}
	}
	return out
}

// DecodeAggregateTable reads a table EncodeAggregateTable wrote. It never panics and
// refuses a format other than 1, a set with no entries or more than MaxAggregates, a kind
// outside 1 to 3, a state above 1, a sum with a by, a by form without one, a count with a
// field, an empty or over-deep path, a duplicate (kind, field, by), a zero or repeated id,
// an id at or above next_agg_id, sets out of field-id order, and trailing bytes.
func DecodeAggregateTable(encoded []byte) (machine.AggregateTable, error) {
	var table machine.AggregateTable
	if len(encoded) < 2 {
		return table, malformed("an aggregate table is at least 2 bytes, got %d", len(encoded))
	}
	if encoded[0] != aggregateTableFormat {
		return table, malformed("aggregate table format %d, want %d", encoded[0], aggregateTableFormat)
	}
	count := int(encoded[1])
	rest := encoded[2:]
	for range count {
		if len(rest) < 4 {
			return machine.AggregateTable{}, malformed("aggregate table ends inside a set header")
		}
		set := machine.AggregateSet{FieldID: binary.BigEndian.Uint16(rest[0:2]), NextID: rest[2]}
		entries := int(rest[3])
		rest = rest[4:]
		for range entries {
			entry, tail, err := decodeAggregateEntry(rest)
			if err != nil {
				return machine.AggregateTable{}, err
			}
			set.Entries = append(set.Entries, entry)
			rest = tail
		}
		table.Sets = append(table.Sets, set)
	}
	if len(rest) != 0 {
		return machine.AggregateTable{}, malformed("aggregate table has %d trailing bytes", len(rest))
	}
	if err := checkAggregateSets(table); err != nil {
		return machine.AggregateTable{}, err
	}
	return table, nil
}

func decodeAggregateEntry(rest []byte) (machine.AggregateEntry, []byte, error) {
	if len(rest) < 4 {
		return machine.AggregateEntry{}, nil, malformed("aggregate table ends inside an aggregate")
	}
	entry := machine.AggregateEntry{ID: rest[0], Kind: rest[1], State: rest[2]}
	fieldLen := int(rest[3])
	rest = rest[4:]
	if len(rest) < fieldLen+1 {
		return machine.AggregateEntry{}, nil, malformed("aggregate %d ends inside its field path", entry.ID)
	}
	entry.Field = string(rest[:fieldLen])
	byLen := int(rest[fieldLen])
	rest = rest[fieldLen+1:]
	if len(rest) < byLen {
		return machine.AggregateEntry{}, nil, malformed("aggregate %d ends inside its by path", entry.ID)
	}
	entry.By = string(rest[:byLen])
	return entry, rest[byLen:], nil
}

// checkAggregateSets holds every rule that needs no field table, so the encoder and the
// decoder share it.
func checkAggregateSets(table machine.AggregateTable) error {
	for i, set := range table.Sets {
		if i > 0 && set.FieldID <= table.Sets[i-1].FieldID {
			return malformed("aggregate sets are not in ascending field-id order at field %d", set.FieldID)
		}
		if len(set.Entries) == 0 || len(set.Entries) > machine.MaxAggregates {
			return malformed("field %d declares %d aggregates, want 1 to %d", set.FieldID, len(set.Entries), machine.MaxAggregates)
		}
		seen := make(map[machine.AggregateEntry]bool, len(set.Entries))
		ids := make(map[uint8]bool, len(set.Entries))
		for _, entry := range set.Entries {
			if err := checkAggregateEntry(set, entry); err != nil {
				return err
			}
			if ids[entry.ID] {
				return malformed("field %d repeats aggregate id %d", set.FieldID, entry.ID)
			}
			ids[entry.ID] = true
			key := entry
			key.ID, key.State = 0, 0
			if seen[key] {
				return malformed("field %d declares the same aggregate twice", set.FieldID)
			}
			seen[key] = true
		}
	}
	return nil
}

func checkAggregateEntry(set machine.AggregateSet, entry machine.AggregateEntry) error {
	if entry.ID == 0 || entry.ID >= set.NextID {
		return malformed("field %d aggregate id %d is outside 1 to next id %d", set.FieldID, entry.ID, set.NextID)
	}
	if entry.State > machine.AggStateBuilding {
		return malformed("field %d aggregate %d has state %d", set.FieldID, entry.ID, entry.State)
	}
	switch entry.Kind {
	case machine.AggSum:
		if entry.By != "" || entry.Field == "" {
			return malformed("field %d aggregate %d: a sum names one field and no by", set.FieldID, entry.ID)
		}
	case machine.AggSumBy:
		if entry.By == "" || entry.Field == "" {
			return malformed("field %d aggregate %d: a sum by names a field and a by", set.FieldID, entry.ID)
		}
	case machine.AggCountBy:
		if entry.By == "" || entry.Field != "" {
			return malformed("field %d aggregate %d: a count by names a by and no field", set.FieldID, entry.ID)
		}
	default:
		return malformed("field %d aggregate %d has kind %d", set.FieldID, entry.ID, entry.Kind)
	}
	for _, path := range []string{entry.Field, entry.By} {
		if path == "" {
			continue
		}
		keys := strings.Split(path, ".")
		if len(keys) > machine.MaxAggregatePath || len(path) > 255 || strings.Contains(path, "..") || path[0] == '.' || path[len(path)-1] == '.' {
			return malformed("field %d aggregate %d path %q is empty or deeper than %d keys", set.FieldID, entry.ID, path, machine.MaxAggregatePath)
		}
	}
	return nil
}

// checkAggregateTable is checkAggregateSets plus the schema rule: every set names a
// hash, btree, fifo, lifo or heap field of fields, or a field fields retired.
func checkAggregateTable(table machine.AggregateTable, fields FieldTable) error {
	if err := checkAggregateSets(table); err != nil {
		return err
	}
	types := make(map[uint16]string, len(fields.Entries))
	for _, entry := range fields.Entries {
		types[entry.ID] = entry.Type
	}
	retired := make(map[uint16]bool, len(fields.Retired))
	for _, id := range fields.Retired {
		retired[id.ID] = true
	}
	for _, set := range table.Sets {
		if _, declared := types[set.FieldID]; !declared && retired[set.FieldID] {
			continue // a store field an alter dropped: its set goes at the next fold
		}
		switch types[set.FieldID] {
		case "hashStore", "btreeStore", "fifoStore", "lifoStore", "heapStore":
		default:
			return malformed("aggregates are declared on field %d, which is not a hash, btree, fifo, lifo or heap store", set.FieldID)
		}
	}
	return nil
}

// decodeAggregates reads field 12 and checks it against the declaration's own fields.
func decodeAggregates(stored map[uint16][]byte, fields FieldTable) (machine.AggregateTable, error) {
	encoded, err := requireBinary(stored, declFieldAggregates)
	if err != nil {
		return machine.AggregateTable{}, err
	}
	table, err := DecodeAggregateTable(encoded)
	if err != nil {
		return machine.AggregateTable{}, fmt.Errorf("field 12: %w", err)
	}
	if err := checkAggregateTable(table, fields); err != nil {
		return machine.AggregateTable{}, fmt.Errorf("field 12: %w", err)
	}
	return table, nil
}
