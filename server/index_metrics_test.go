package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"
)

// Observability, aggregate only (GAP-14 build doc Step 20, S-10; rule 11): a
// 10^6 buckets must not be 10^6 series.

// TestNoMetricIsLabelledByABucketName (§4 F-14): no metric in either metric
// file takes a label that carries a user string — a bucket, a database, an
// index, a field, a name. Read from the source, so a metric added later is
// held to it without anyone remembering to add it here.
func TestNoMetricIsLabelledByABucketName(t *testing.T) {
	banned := []string{"bucket", "database", "index", "name", "field"}
	checked := 0
	for _, file := range []string{"metrics.go", "capacity_metrics.go"} {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(selector.Sel.Name, "New") || !strings.HasSuffix(selector.Sel.Name, "Vec") {
				return true
			}
			if ident, isIdent := call.Args[1].(*ast.Ident); isIdent && ident.Name == "nil" {
				return true
			}
			labels, ok := call.Args[1].(*ast.CompositeLit)
			if !ok {
				t.Errorf("%s: %s takes its labels from something other than a literal list; this test cannot check them", file, selector.Sel.Name)
				return true
			}
			for _, element := range labels.Elts {
				literal, ok := element.(*ast.BasicLit)
				if !ok {
					t.Errorf("%s: a label of %s is not a string literal", file, selector.Sel.Name)
					continue
				}
				label, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("unquoting %s: %v", literal.Value, err)
				}
				checked++
				for _, word := range banned {
					if strings.Contains(strings.ToLower(label), word) {
						t.Errorf("%s: a metric is labelled %q — a series per %s", file, label, word)
					}
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no labelled metric was found: the test is reading the wrong files")
	}
}

// gatheredGauge reads one labelled series of a gauge from the default
// registry, 0 when it is absent.
func gatheredGauge(t *testing.T, name, label, value string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == label && pair.GetValue() == value {
					return metric.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

// TestConflictedAndUnreadableIndexesAreVisibleWithoutALog (spec §11's first
// bullet): an operator who never reads the log still sees an index that is
// conflicted or unreadable — in the aggregate gauge, and in describe.
func TestConflictedAndUnreadableIndexesAreVisibleWithoutALog(t *testing.T) {
	dataDir := t.TempDir()
	first := seedConflictedUnique(t, dataDir)
	reopened := startCatalogServerAt(t, dataDir)
	writeCatalogIndices(t, reopened.core, "app", "users", []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true},
		{"id": int32(1), "name": "Name_1", "fields": bson.A{"Name"}, "version": int32(3)},
	})
	_ = first
	conflictedBefore := gatheredGauge(t, "hoarddb_indexes", "state", "conflicted")
	unreadableBefore := gatheredGauge(t, "hoarddb_indexes", "state", "unreadable")
	s := restartCatalog(t, reopened, dataDir).core

	if moved := gatheredGauge(t, "hoarddb_indexes", "state", "conflicted") - conflictedBefore; moved != 1 {
		t.Errorf("hoarddb_indexes{state=\"conflicted\"} moved by %v, want 1", moved)
	}
	if moved := gatheredGauge(t, "hoarddb_indexes", "state", "unreadable") - unreadableBefore; moved != 1 {
		t.Errorf("hoarddb_indexes{state=\"unreadable\"} moved by %v, want 1", moved)
	}
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	described := strings.Join(stats.Indexes, " | ")
	for _, want := range []string{"Email_1 (unique) — conflicted", "Name_1 — unreadable"} {
		if !strings.Contains(described, want) {
			t.Errorf("describe says %q, want it to include %q", described, want)
		}
	}
}
