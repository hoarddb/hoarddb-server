package wire

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

var updateOnDiskGolden = flag.Bool("update", false, "rewrite cluster/wire/testdata/ondisk-vocabulary.txt")
var createOnDiskGolden = flag.Bool("create", false, "with -update, create the golden when it does not exist")

const onDiskGolden = "testdata/ondisk-vocabulary.txt"

// onDiskLines is what a node writes to disk that the replicated state's goldens do not see: the
// ring FORMAT marker, the op-record flag bits and the declaration fields the decoder accepts.
// A change to any of them is a change to an on-disk format, which StateLevel counts (mixed-build
// spec 13.6), so it must come with a bump.
// constLines reads the integer constants of a Go source file whose names match, as "name value"
// lines in name order. The values are read from the source, not from a list kept here, so a new
// namespace byte or a changed format number changes the golden.
func constLines(t *testing.T, path string, match func(name string) bool) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	// Imports are not resolved: the constants scanned only use iota, literals and each other,
	// and an error about an unresolved import does not stop go/types evaluating them.
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	conf := types.Config{Error: func(error) {}}
	_, _ = conf.Check("scan", fset, []*ast.File{file}, info)
	literal := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec := spec.(*ast.ValueSpec)
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					continue
				}
				if lit, ok := valueSpec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.INT {
					literal[name.Name] = lit.Value
				}
			}
		}
	}
	var lines []string
	for ident, obj := range info.Defs {
		constObj, ok := obj.(*types.Const)
		if !ok || !match(ident.Name) || constObj.Val().Kind() != constant.Int {
			continue
		}
		if text, ok := literal[ident.Name]; ok {
			lines = append(lines, ident.Name+" "+text)
			continue
		}
		// An iota run or an expression: the evaluated value, so a reordered or added member moves it.
		number, _ := constant.Int64Val(constObj.Val())
		lines = append(lines, fmt.Sprintf("%s 0x%02x", ident.Name, number))
	}
	sort.Strings(lines)
	if len(lines) == 0 {
		t.Fatalf("%s has no constant the golden should record: the scan is broken", path)
	}
	return lines
}

func onDiskLines(t *testing.T) []string {
	t.Helper()
	lines := []string{
		fmt.Sprintf("level %d", cluster.StateLevel),
		fmt.Sprintf("ring_format %d", ringFormat),
		fmt.Sprintf("op_flags_known 0x%02x", cluster.KnownOpFlags()),
	}
	var accepted []string
	for id := 0; id <= 0xFF; id++ {
		_, _, err := store.DecodeDeclarationWithSwept(map[uint16][]byte{uint16(id): nil})
		if err == nil || !strings.Contains(err.Error(), "is not a declaration field") {
			accepted = append(accepted, fmt.Sprint(id))
		}
	}
	lines = append(lines, "declaration_fields "+strings.Join(accepted, " "))
	// Each is a format number or a key namespace a node writes to disk. Scratch proof: change
	// fieldTableFormat, dedupeEntryFormat or seedRecordVersion, or add an NS byte, and this is red.
	for _, line := range constLines(t, "../../store/declaration.go", func(name string) bool { return name == "fieldTableFormat" }) {
		lines = append(lines, "store "+line)
	}
	for _, line := range constLines(t, "dedupe_wire.go", func(name string) bool { return name == "dedupeEntryFormat" }) {
		lines = append(lines, "wire "+line)
	}
	for _, line := range constLines(t, "seedrecord.go", func(name string) bool { return name == "seedRecordVersion" }) {
		lines = append(lines, "wire "+line)
	}
	// Scratch proof for each: change the constant and this is red. They are the bytes a node
	// writes inside a record or a value, or sends in a reply, that no other golden pins.
	scans := []struct {
		label, path string
		match       func(name string) bool
	}{
		{"cluster", "../opcodec_guard.go", func(name string) bool { return strings.HasPrefix(name, "opTrailer") }},
		{"cluster", "../opcodec.go", func(name string) bool {
			return strings.HasPrefix(name, "opFlag") || strings.HasPrefix(name, "Op") || strings.HasPrefix(name, "Verb") || strings.HasPrefix(name, "Update")
		}},
		{"store", "../../store/declaration.go", func(name string) bool {
			return name == "pendingExpireMarker" || name == "indexTableFormat" || name == "referenceEntryBytes" || name == "maxReferences" || strings.HasPrefix(name, "declField")
		}},
		{"store", "../../store/declaration_aggregates.go", func(name string) bool { return name == "aggregateTableFormat" }},
		{"store", "../../store/aggregatekey.go", func(name string) bool { return name == "InnerAggregate" }},
		{"storage", "../../storage/expirykey.go", func(name string) bool {
			return strings.HasPrefix(name, "ExpiryKind") || name == "expiryBodyHead" || name == "expiryElementTail"
		}},
		{"wire", "peercodec.go", func(name string) bool { return strings.HasPrefix(name, "forward") }},
		{"wire", "fieldforward.go", func(name string) bool { return strings.HasPrefix(name, "FieldOp") }},
	}
	for _, scan := range scans {
		for _, line := range constLines(t, scan.path, scan.match) {
			lines = append(lines, scan.label+" "+line)
		}
	}
	for _, line := range constLines(t, "../../storage/keys.go", func(name string) bool { return strings.HasPrefix(name, "NS") }) {
		lines = append(lines, "storage "+line)
	}
	return lines
}

func TestStateLevelMovesWithTheOnDiskFormats(t *testing.T) {
	text := strings.Join(onDiskLines(t), "\n") + "\n"
	if *updateOnDiskGolden {
		old, err := os.ReadFile(onDiskGolden)
		if err != nil && !*createOnDiskGolden {
			t.Fatalf("%s does not exist (%v): -update does not create it unless -create is given, because a deleted golden would otherwise be regenerated at the same level", onDiskGolden, err)
		}
		if err == nil && bytes.HasPrefix(old, []byte(fmt.Sprintf("level %d\n", cluster.StateLevel))) && !bytes.Equal(old, []byte(text)) {
			t.Fatalf("%s was recorded at level %d, the build still declares it, and the contents changed: bump cluster.StateLevel and add its row to cluster/stateversions.go first, then regenerate", onDiskGolden, cluster.StateLevel)
		}
		if err := os.WriteFile(onDiskGolden, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(onDiskGolden)
	if err != nil {
		t.Fatalf("%s is missing (%v): regenerate it with 'go test ./cluster/wire -run TestStateLevelMovesWithTheOnDiskFormats -update'", onDiskGolden, err)
	}
	if string(want) != text {
		t.Fatalf("an on-disk format changed (ring FORMAT, an op-record flag or a declaration field): bump cluster.StateLevel, add its row to cluster/stateversions.go and regenerate %s with 'go test ./cluster/wire -run TestStateLevelMovesWithTheOnDiskFormats -update'\ngolden:\n%sbuild:\n%s", onDiskGolden, want, text)
	}
}

// TestOnDiskGoldenRecordsTheHiddenFormats keeps the scans above from being dropped: each name is a
// byte a node writes in a record, a value or a reply that no other golden pins.
func TestOnDiskGoldenRecordsTheHiddenFormats(t *testing.T) {
	text := strings.Join(onDiskLines(t), "\n")
	for _, name := range []string{
		"opTrailerGuard", "opTrailerReturn", "pendingExpireMarker", "aggregateTableFormat",
		"referenceEntryBytes", "indexTableFormat", "InnerAggregate", "ExpiryKindElement",
		"expiryBodyHead", "forwardFlagTruncated", "forwardReadField", "FieldOpAggregate",
	} {
		if !strings.Contains(text, " "+name+" ") {
			t.Errorf("the on-disk golden does not record %s: a change to it would pass the build without a state level bump", name)
		}
	}
}

// ringFormatAtLevel records the ring FORMAT number each StateLevel was released with. The level
// numbering restarted at 1 in the first release (1.1), which moved the format from the development
// builds' 8 to 9 without a level bump, so that the marker refuses their directories. Until the
// "not loaded while ClusterVersion() < Min" hold exists (cluster-version spec 4.4, step 3c), the only
// thing that stops a build from loading a directory written below its level is ensureRingFormat, so
// every StateLevel bump must come with a ringFormat bump. A row is added in the same commit as the
// bump; this table cannot be regenerated, which is the point.
var ringFormatAtLevel = map[uint8]int{
	1: 9,
}

// TestEveryStateLevelBumpBumpsTheRingFormat enforces that rule: the build's level has a row, the
// row names the build's ringFormat, and a higher level never reuses a lower level's format.
// Against: StateLevel bumped with ringFormat left alone (the row is missing, or names the old format).
func TestEveryStateLevelBumpBumpsTheRingFormat(t *testing.T) {
	format, ok := ringFormatAtLevel[cluster.StateLevel]
	if !ok {
		t.Fatalf("StateLevel is %d and ringFormatAtLevel has no row for it: add the row together with a ringFormat bump, so an older directory is refused by ensureRingFormat", cluster.StateLevel)
	}
	if format != ringFormat {
		t.Fatalf("StateLevel %d is recorded with ring format %d but ringFormat is %d: a level bump must bump the ring format (and a format bump the row)", cluster.StateLevel, format, ringFormat)
	}
	// The numbering restarted at 1 in the first release: a row above the build's level is a stale
	// row of the development levels before it, and would let a later bump to that level pass with
	// the old format.
	for level := range ringFormatAtLevel {
		if level > cluster.StateLevel {
			t.Fatalf("ringFormatAtLevel has a row for level %d, above StateLevel %d: drop the rows of the levels before the reset", level, cluster.StateLevel)
		}
	}
	for level, lowerFormat := range ringFormatAtLevel {
		for higher, higherFormat := range ringFormatAtLevel {
			if higher > level && higherFormat <= lowerFormat {
				t.Fatalf("level %d reuses ring format %d of level %d: every level bump must move the ring format up", higher, higherFormat, level)
			}
		}
	}
}
