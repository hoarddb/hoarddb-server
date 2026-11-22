package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// TestPostgresMapCoversTheModel: every store the model declares has a table, a keyed store is
// keyed and a fifo or lifo store is ordered by seq, and the columns of a shared table agree.
func TestPostgresMapCoversTheModel(t *testing.T) {
	for layout, kinds := range schemaKinds {
		m := pgMaps[layout]
		for name, kind := range kinds {
			s, ok := m.stores[name]
			if !ok {
				t.Errorf("%s: store %s has no PostgreSQL table", layout, name)
				continue
			}
			if kind.Keyed() == s.seqKeyed() {
				t.Errorf("%s: store %s is keyed=%v in the model but the map says seq-ordered=%v", layout, name, kind.Keyed(), s.seqKeyed())
			}
			if kind.Keyed() {
				if !slices.Contains(s.pk, s.ownerCol) || !slices.Contains(s.pk, s.keyCol) {
					t.Errorf("%s: store %s primary key %v lacks its owner and key columns", layout, name, s.pk)
				}
				if len(s.bodyCols()) == 0 {
					t.Errorf("%s: keyed store %s has no column to update on a repeated put", layout, name)
				}
			}
		}
		for name := range m.stores {
			if _, ok := kinds[name]; !ok {
				t.Errorf("%s: the map has store %s, which the model does not declare", layout, name)
			}
		}
		if _, err := m.expectedColumns(); err != nil {
			t.Errorf("%s: %v", layout, err)
		}
	}
}

// TestPostgresMapDocumentsMatchTheModel: every bucket of the model has a table and the fields
// of a document are the model's, in the model's order.
func TestPostgresMapDocumentsMatchTheModel(t *testing.T) {
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range gen.Schema() {
			d, ok := pgMaps[layout].docs[bucket.Name]
			if !ok {
				t.Errorf("%s: bucket %s has no PostgreSQL table", layout, bucket.Name)
				continue
			}
			var want []string
			for _, field := range bucket.Fields {
				if !field.IsStore() {
					want = append(want, field.Name)
				}
			}
			var got []string
			for _, f := range d.fields {
				got = append(got, f.field)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%s: bucket %s has fields %v in the model and %v in the map", layout, bucket.Name, want, got)
			}
		}
	}
}

// TestPostgresMapBodiesMatchSeededElements: the body fields of each store are the model's
// fields of a seeded element, in the model's order.
func TestPostgresMapBodiesMatchSeededElements(t *testing.T) {
	for _, layout := range benchLayouts {
		gen, err := model.New(layout, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for unit := range gen.Units() {
			for _, e := range unit.Elements {
				name := e.StoreName()
				if seen[name] {
					continue
				}
				seen[name] = true
				s := pgMaps[layout].stores[name]
				var want, got []string
				for _, f := range e.Body {
					want = append(want, f.Name)
				}
				for _, b := range s.body {
					got = append(got, b.field)
				}
				if !slices.Equal(got, want) {
					t.Errorf("%s: store %s body is %v in the model and %v in the map", layout, name, want, got)
				}
			}
		}
	}
}

func TestPostgresIdentQuotes(t *testing.T) {
	if got := pgIdent(`a"b`); got != `"a""b"` {
		t.Errorf("pgIdent = %s", got)
	}
	if got := pgPlaceholders(3, 3); got != "$3, $4, $5" {
		t.Errorf("pgPlaceholders = %s", got)
	}
	if got := pgIdents([]string{"at", "text"}); !strings.Contains(got, `"at", "text"`) {
		t.Errorf("pgIdents = %s", got)
	}
}
