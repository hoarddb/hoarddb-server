package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// byName finds a field or counter by name.
func byName(name string, lists ...[]model.Field) (any, bool) {
	for _, list := range lists {
		for _, field := range list {
			if field.Name == name {
				return field.Value, true
			}
		}
	}
	return nil, false
}

// rowValues returns the column values of one row of spec, in column order, from a document
// (element nil) or from an element of a store. It names the column when the model does not
// produce what the table asks for, because that is a bug in schema.go, not in the data.
func rowValues(spec table, doc model.Document, element *model.Element) ([]any, error) {
	values := make([]any, len(spec.Cols))
	for i, col := range spec.Cols {
		var found bool
		switch {
		case col.From == "":
			values[i], found = col.Fixed, true
		case element == nil && col.From == "_id":
			values[i], found = doc.ID, true
		case element == nil:
			values[i], found = byName(col.From, doc.Fields, doc.Counters)
		case col.From == "_owner":
			values[i], found = element.Owner, true
		case col.From == "_key":
			values[i], found = element.Key, true
		case col.From == "_key:playlist" || col.From == "_key:position":
			values[i], found = splitPlaylistKey(element.Key, col.From == "_key:position")
		case col.From == "_seq":
			values[i], found = element.Seq, true
		default:
			values[i], found = byName(col.From, element.Body)
		}
		if !found {
			return nil, fmt.Errorf("table %s: column %s takes %q, which the model does not produce for %s %s. Fix the From of that column in schema.go", spec.Name, col.Name, col.From, doc.Bucket, doc.ID)
		}
	}
	return values, nil
}

// splitPlaylistKey takes apart a "<playlist>/<position>" key at its last slash, and returns the
// position as a number when position is set. found is false when the key has no slash or the
// position is not a number.
func splitPlaylistKey(elementKey string, position bool) (any, bool) {
	slash := strings.LastIndex(elementKey, "/")
	if slash < 0 {
		return nil, false
	}
	if !position {
		return elementKey[:slash], true
	}
	n, err := strconv.ParseInt(elementKey[slash+1:], 10, 64)
	return n, err == nil
}

// tableRows is the rows of a batch of units, by table name.
type tableRows map[string][][]any

// sourceIndex maps a source ("Bucket" or "Bucket.Store") to its table; a source with no table
// (a paired store) is absent.
func sourceIndex(layout model.Layout) map[string]table {
	out := map[string]table{}
	for _, spec := range tables(layout) {
		out[spec.Source] = spec
	}
	return out
}

// rowsOf turns units into rows of their tables. A document is a row of its bucket's table;
// each element is a row of its store's table, unless the store is paired with another source.
func rowsOf(layout model.Layout, units []model.Unit) (tableRows, error) {
	sources := sourceIndex(layout)
	out := tableRows{}
	for _, unit := range units {
		if spec, ok := sources[unit.Doc.Bucket]; ok {
			row, err := rowValues(spec, unit.Doc, nil)
			if err != nil {
				return nil, err
			}
			out[spec.Name] = append(out[spec.Name], row)
		}
		for i := range unit.Elements {
			element := &unit.Elements[i]
			spec, ok := sources[element.StoreName()]
			if !ok {
				continue
			}
			row, err := rowValues(spec, unit.Doc, element)
			if err != nil {
				return nil, err
			}
			out[spec.Name] = append(out[spec.Name], row)
		}
	}
	return out, nil
}

// columnNames lists a table's column names.
func columnNames(spec table) []string {
	names := make([]string, len(spec.Cols))
	for i, col := range spec.Cols {
		names[i] = col.Name
	}
	return names
}
