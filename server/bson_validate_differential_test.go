package server

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	vendorbson "go.mongodb.org/mongo-driver/bson"

	"github.com/hoarddb/hoarddb-server/internal/bsonoracle"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The library's Validate is the check at every site that holds a client's
// bytes. Until build step 9 it was held to accept exactly what the server's
// depth scan accepted at the site's limit AND what the vendor's decode into
// bson.D accepts (bson-codec-spec 6.2, ruling O16); step 9 removed the scan.
// These tests keep the half that needs no scan, at every limit the sites use,
// over the corpora, the probe classes, depth ladders and a byte-mutation sweep:
// Validate accepts only what the vendor decodes, refuses what the vendor
// decodes only for depth or the old-binary rule the scan added, and counts
// levels exactly (the ladders).

// --- byte builders: written here, from bytes, with no codec ---

func le32(value int) []byte {
	var out [4]byte
	binary.LittleEndian.PutUint32(out[:], uint32(int32(value)))
	return out[:]
}

// docOf frames elements as a document: length, elements, terminator.
func docOf(elements ...[]byte) []byte {
	body := []byte{}
	for _, element := range elements {
		body = append(body, element...)
	}
	framed := append(le32(4+len(body)+1), body...)
	return append(framed, 0)
}

func elemOf(typeByte byte, name string, value ...byte) []byte {
	out := append([]byte{typeByte}, name...)
	out = append(out, 0)
	return append(out, value...)
}

func bsonString(text string) []byte {
	return append(append(le32(len(text)+1), text...), 0)
}

// nestedChain is a document nesting depth levels, the root level 1, joined by
// kind: "doc" (embedded documents), "array", or "scope" (code-with-scope
// scopes). The innermost level holds one int32.
func nestedChain(depth int, kind string) []byte {
	current := docOf(elemOf(0x10, "v", 1, 0, 0, 0))
	for level := depth - 1; level >= 1; level-- {
		switch kind {
		case "array":
			current = docOf(elemOf(0x04, "0", current...))
		case "scope":
			codeWithScope := append(le32(4+len(bsonString("x"))+len(current)), bsonString("x")...)
			codeWithScope = append(codeWithScope, current...)
			current = docOf(elemOf(0x0F, "s", codeWithScope...))
		default:
			current = docOf(elemOf(0x03, "a", current...))
		}
	}
	return current
}

// richDocument holds one value of every BSON type, so a byte mutation lands in
// every kind of value.
func richDocument() []byte {
	inner := docOf(elemOf(0x10, "i", 7, 0, 0, 0))
	scopeValue := append(le32(4+len(bsonString("f()"))+len(inner)), bsonString("f()")...)
	scopeValue = append(scopeValue, inner...)
	oldBinary := append(append(le32(6), 0x02), append(le32(2), 'a', 'b')...)
	return docOf(
		elemOf(0x01, "double", make([]byte, 8)...),
		elemOf(0x02, "string", bsonString("hello")...),
		elemOf(0x03, "doc", inner...),
		elemOf(0x04, "array", inner...),
		elemOf(0x05, "binary", append(append(le32(3), 0x00), 1, 2, 3)...),
		elemOf(0x05, "oldbinary", oldBinary...),
		elemOf(0x06, "undefined"),
		elemOf(0x07, "oid", make([]byte, 12)...),
		elemOf(0x08, "bool", 1),
		elemOf(0x09, "date", make([]byte, 8)...),
		elemOf(0x0A, "null"),
		elemOf(0x0B, "regex", 'a', 0, 'i', 0),
		elemOf(0x0C, "dbpointer", append(bsonString("ns"), make([]byte, 12)...)...),
		elemOf(0x0D, "code", bsonString("x")...),
		elemOf(0x0E, "symbol", bsonString("s")...),
		elemOf(0x0F, "cws", scopeValue...),
		elemOf(0x10, "int32", 1, 0, 0, 0),
		elemOf(0x11, "timestamp", make([]byte, 8)...),
		elemOf(0x12, "int64", make([]byte, 8)...),
		elemOf(0x13, "decimal", make([]byte, 16)...),
		elemOf(0x7F, "maxkey"),
		elemOf(0xFF, "minkey"),
	)
}

// --- the two answers ---

// vendorAccepts is the vendor's decode into an ordered document. It runs under
// recover: the vendor's validation panics on some declared lengths, and a
// panic is a refusal here.
func vendorAccepts(raw []byte) (accepted bool) {
	defer func() {
		if recover() != nil {
			accepted = false
		}
	}()
	var decoded vendorbson.D
	return vendorbson.Unmarshal(raw, &decoded) == nil
}

// siteAnswers is what the two readings say about raw at limit.
type siteAnswers struct {
	library, vendor bool
	libraryErr      error
}

func answersAt(raw []byte, limit int) siteAnswers {
	_, libraryErr := bson.Validate(raw, bson.Limits{Depth: limit})
	return siteAnswers{library: libraryErr == nil, vendor: vendorAccepts(raw), libraryErr: libraryErr}
}

// refusedForDepth is a refusal of a level past the limit.
func (answers siteAnswers) refusedForDepth() bool {
	return errors.Is(answers.libraryErr, bson.ErrNestingTooDeep)
}

// refusedByOldBinaryRule is the refusal the server's scan added to the
// vendor's reading: an old binary value whose two lengths disagree, which a
// decoder skipping the value never reads.
func (answers siteAnswers) refusedByOldBinaryRule() bool {
	var validationError *bson.ValidationError
	return errors.As(answers.libraryErr, &validationError) && strings.Contains(validationError.Reason, "old binary")
}

// agrees is the half of the validity oracle (spec 6.2) that remains: the
// library accepts only what the vendor decodes, and refuses what the vendor
// decodes only for depth or by the old-binary rule.
func (answers siteAnswers) agrees() bool {
	if answers.library {
		return answers.vendor
	}
	return !answers.vendor || answers.refusedForDepth() || answers.refusedByOldBinaryRule()
}

// --- the site table ---

type limitSite struct {
	name  string
	limit int
}

// limitSites lists every limit the server's sites check at (build doc step 7b
// table): the request limits, every row of recordDepthLimit, the restore
// element rows, the peer answer limit, and the bare bound, so a row changed in
// nesting_depth.go changes this table with it. Every row calls the site's own
// function; a literal row would cover a limit no site checks.
func limitSites() []limitSite {
	sites := []limitSite{
		{"maxDocumentDepth", maxDocumentDepth},
		{"requestDepthLimit(OpUpdate)", requestDepthLimit(transport.OpUpdate)},
		{"requestDepthLimit(OpBlobWrite)", requestDepthLimit(transport.OpBlobWrite)},
		{"requestDepthLimit(OpBatchPut)", requestDepthLimit(transport.OpBatchPut)},
		{"requestDepthLimit(OpDeclareStoreFields)", requestDepthLimit(transport.OpDeclareStoreFields)},
		{"peerAnswerDepthLimit", peerAnswerDepthLimit},
		{"storeElementRecordDepthLimit(FieldHash)", storeElementRecordDepthLimit(store.FieldHash)},
		{"storeElementRecordDepthLimit(FieldBlob)", storeElementRecordDepthLimit(store.FieldBlob)},
	}
	kinds := map[string]depthRecordKind{"stored": recordStored, "dumped": recordDumped, "claimed": recordClaimed, "dead": recordDead}
	types := []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob}
	for kindName, kind := range kinds {
		for _, storeType := range types {
			sites = append(sites, limitSite{
				fmt.Sprintf("recordDepthLimit(%s,%s)", kindName, storeType), recordDepthLimit(kind, storeType)})
		}
	}
	sort.SliceStable(sites, func(first, second int) bool { return sites[first].name < sites[second].name })
	return sites
}

// limitSiteGroups folds the site table by limit, for the report: the distinct
// limits and the sites that share each.
func limitSiteGroups(sites []limitSite) map[int][]string {
	groups := map[int][]string{}
	for _, site := range sites {
		groups[site.limit] = append(groups[site.limit], site.name)
	}
	return groups
}

// --- the corpora ---

type namedInput struct {
	name string
	raw  []byte
}

// vectorInputs is every vector of the BSON corpus that carries bytes. The
// published server tree has no clients/, so the vectors skip there.
func vectorInputs(t *testing.T) []namedInput {
	t.Helper()
	const vectorsPath = "../../clients/testdata/bson-vectors/v1.json"
	corpusFile, err := os.ReadFile(vectorsPath)
	if errors.Is(err, os.ErrNotExist) {
		if _, markerErr := os.Stat("../../scripts/ci.sh"); errors.Is(markerErr, os.ErrNotExist) {
			t.Skipf("published server tree: no %s", vectorsPath)
		}
	}
	if err != nil {
		t.Fatalf("reading the BSON vectors %s: %v", vectorsPath, err)
	}
	var corpus struct {
		Vectors []struct {
			Name string `json:"name"`
			Hex  string `json:"hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(corpusFile, &corpus); err != nil {
		t.Fatalf("parsing %s: %v", vectorsPath, err)
	}
	var inputs []namedInput
	for _, vector := range corpus.Vectors {
		if vector.Hex == "" && vector.Name != "reject/empty-input" {
			continue
		}
		raw, err := hex.DecodeString(vector.Hex)
		if err != nil {
			t.Fatalf("vector %q: hex does not decode: %v", vector.Name, err)
		}
		inputs = append(inputs, namedInput{"vector " + vector.Name, raw})
	}
	return inputs
}

func v1CorpusInputs(t *testing.T) []namedInput {
	t.Helper()
	records, err := bsonoracle.V1Corpus()
	if err != nil {
		t.Fatalf("loading the v1.0 corpus: %v", err)
	}
	inputs := make([]namedInput, 0, len(records))
	for position, record := range records {
		inputs = append(inputs, namedInput{fmt.Sprintf("v1 record %d (%s)", position, record.Class), record.Value})
	}
	return inputs
}

// probeInputs are the spec 6.2 probe classes, each at the root and one level
// down, and every refusal class of the depth scan Validate replaced.
func probeInputs() []namedInput {
	var inputs []namedInput
	add := func(name string, raw []byte) { inputs = append(inputs, namedInput{name, raw}) }
	wrapped := func(name string, element []byte) {
		add(name, docOf(element))
		add(name+" (nested)", docOf(elemOf(0x03, "a", docOf(element)...)))
	}
	wrapped("boolean byte 0x02", elemOf(0x08, "b", 2))
	wrapped("boolean byte 0xff", elemOf(0x08, "b", 0xff))
	wrapped("unterminated string", elemOf(0x02, "s", append(le32(3), 'a', 'b', 'c')...))
	wrapped("string length 0", elemOf(0x02, "s", le32(0)...))
	wrapped("string runs past the end", elemOf(0x02, "s", le32(1000)...))
	wrapped("negative string length", elemOf(0x02, "s", le32(-1)...))
	wrapped("code unterminated", elemOf(0x0D, "c", append(le32(2), 'a', 'b')...))
	wrapped("symbol unterminated", elemOf(0x0E, "c", append(le32(2), 'a', 'b')...))
	for _, typeByte := range []byte{0x00 + 0x14, 0x20, 0x41, 0x7E, 0x80, 0xFE} {
		wrapped(fmt.Sprintf("invalid type byte 0x%02x", typeByte), elemOf(typeByte, "t", 1, 2, 3, 4))
	}
	wrapped("binary too short for its length", elemOf(0x05, "b", append(le32(10), 0, 1, 2)...))
	wrapped("binary with no subtype", elemOf(0x05, "b", le32(0)...))
	wrapped("binary negative length", elemOf(0x05, "b", append(le32(-1), 0)...))
	wrapped("old binary inner length too big", elemOf(0x05, "b", append(append(le32(6), 0x02), append(le32(9), 'a', 'b')...)...))
	wrapped("old binary inner length too small", elemOf(0x05, "b", append(append(le32(6), 0x02), append(le32(1), 'a', 'b')...)...))
	wrapped("old binary too short for its inner length", elemOf(0x05, "b", append(append(le32(2), 0x02), 'a', 'b')...))
	wrapped("DBPointer unterminated string", elemOf(0x0C, "p", append(append(le32(2), 'a', 'b'), make([]byte, 12)...)...))
	wrapped("DBPointer short oid", elemOf(0x0C, "p", append(bsonString("ns"), make([]byte, 11)...)...))
	wrapped("regex unterminated", elemOf(0x0B, "r", 'a', 0, 'i'))
	wrapped("regex missing options", elemOf(0x0B, "r", 'a', 0))
	wrapped("int32 short", elemOf(0x10, "i", 1, 2))
	wrapped("decimal128 short", elemOf(0x13, "d", make([]byte, 15)...))
	wrapped("embedded length too big", elemOf(0x03, "d", append(le32(500), 0)...))
	wrapped("embedded length too small", elemOf(0x03, "d", le32(4)...))
	wrapped("embedded length negative", elemOf(0x03, "d", append(le32(-5), 0)...))
	wrapped("embedded missing terminator", elemOf(0x03, "d", append(le32(5), 1)...))
	wrapped("array length too big", elemOf(0x04, "d", append(le32(500), 0)...))

	inner := docOf(elemOf(0x10, "i", 1, 0, 0, 0))
	codeWithScope := func(total, codeLength, scopeLength int, scope []byte) []byte {
		out := append(le32(total), le32(codeLength)...)
		out = append(out, 'x', 0)
		scopeBytes := append([]byte{}, scope...)
		copy(scopeBytes, le32(scopeLength))
		return append(out, scopeBytes...)
	}
	goodTotal := 4 + 4 + 2 + len(inner)
	wrapped("code-with-scope good", elemOf(0x0F, "w", codeWithScope(goodTotal, 2, len(inner), inner)...))
	wrapped("code-with-scope total too big", elemOf(0x0F, "w", codeWithScope(goodTotal+1, 2, len(inner), inner)...))
	wrapped("code-with-scope total too small", elemOf(0x0F, "w", codeWithScope(goodTotal-1, 2, len(inner), inner)...))
	wrapped("code-with-scope code length 0", elemOf(0x0F, "w", codeWithScope(goodTotal, 0, len(inner), inner)...))
	wrapped("code-with-scope scope length too big", elemOf(0x0F, "w", codeWithScope(goodTotal, 2, len(inner)+1, inner)...))
	wrapped("code-with-scope scope length too small", elemOf(0x0F, "w", codeWithScope(goodTotal, 2, len(inner)-1, inner)...))
	wrapped("code-with-scope scope length negative", elemOf(0x0F, "w", codeWithScope(goodTotal, 2, -1, inner)...))

	// Bad lengths at each level of a four-level chain, and a missing terminator
	// at each level.
	chain := nestedChain(4, "doc")
	add("chain good", chain)
	for position := 0; position+4 <= len(chain); position++ {
		for _, delta := range []int{-1, 1} {
			mutated := append([]byte{}, chain...)
			binary.LittleEndian.PutUint32(mutated[position:], uint32(int32(binary.LittleEndian.Uint32(mutated[position:]))+int32(delta)))
			add(fmt.Sprintf("chain length bytes at %d %+d", position, delta), mutated)
		}
	}
	for terminator := len(chain) - 1; terminator >= len(chain)-4; terminator-- {
		mutated := append([]byte{}, chain...)
		mutated[terminator] = 1
		add(fmt.Sprintf("chain terminator %d not zero", terminator), mutated)
	}

	// Roots whose declared length cannot be a document.
	add("empty input", nil)
	add("three bytes", []byte{5, 0, 0})
	for _, declared := range []int{-1, 0, 1, 4, 5, 6, 1 << 30} {
		add(fmt.Sprintf("root length %d, five bytes", declared), append(le32(declared), 0))
		add(fmt.Sprintf("root length %d, four bytes", declared), le32(declared))
	}
	add("empty document", docOf())
	add("trailing byte", append(docOf(), 0))

	// Every truncation of a rich document, with and without a repaired prefix.
	rich := richDocument()
	for cut := 0; cut < len(rich); cut++ {
		add(fmt.Sprintf("rich truncated to %d", cut), rich[:cut])
		repaired := append([]byte{}, rich[:cut]...)
		if cut >= 4 {
			copy(repaired, le32(cut))
			add(fmt.Sprintf("rich truncated to %d, prefix repaired", cut), repaired)
		}
	}
	return inputs
}

// ladderInputs are depth ladders of documents, arrays and code-with-scope
// scopes, at 99 to 104 and around limit.
func ladderInputs(limit int) []namedInput {
	var inputs []namedInput
	depths := map[int]bool{}
	for depth := 99; depth <= 104; depth++ {
		depths[depth] = true
	}
	for depth := limit - 1; depth <= limit+1; depth++ {
		depths[depth] = true
	}
	for depth := range depths {
		for _, kind := range []string{"doc", "array", "scope"} {
			inputs = append(inputs, namedInput{fmt.Sprintf("%s ladder depth %d", kind, depth), nestedChain(depth, kind)})
		}
	}
	return inputs
}

// mutationInputs flips each byte of the rich document and of a three-level
// chain to values that matter to BSON framing: zero, one, two, a type byte
// that is not one, 0x7f, 0xff, and the neighbours of the original.
func mutationInputs() []namedInput {
	var inputs []namedInput
	for baseName, base := range map[string][]byte{"rich": richDocument(), "chain3": nestedChain(3, "scope")} {
		for position := range base {
			for _, replacement := range []byte{0, 1, 2, 0x14, 0x7F, 0xFF, base[position] + 1, base[position] - 1} {
				mutated := append([]byte{}, base...)
				mutated[position] = replacement
				inputs = append(inputs, namedInput{fmt.Sprintf("%s byte %d -> 0x%02x", baseName, position, replacement), mutated})
			}
		}
	}
	return inputs
}

// siteReport is one row of the per-site agreement table.
type siteReport struct {
	inputs, accepted, refusedByDepth, refusedByVendorToo, disagreements int
}

// TestValidateMatchesVendorDecodeAtEverySite holds the remaining half of the
// validity oracle at every site limit, and the ladders to the exact level
// count: a chain of at most limit levels is accepted, a deeper one refused
// for depth, as documents, arrays and code-with-scope scopes.
func TestValidateMatchesVendorDecodeAtEverySite(t *testing.T) {
	probes := probeInputs()
	vectors := vectorInputs(t)
	v1Corpus := v1CorpusInputs(t)
	mutations := mutationInputs()
	sites := limitSites()
	groups := limitSiteGroups(sites)

	var totals siteReport
	limits := make([]int, 0, len(groups))
	for limit := range groups {
		limits = append(limits, limit)
	}
	sort.Ints(limits)
	for _, limit := range limits {
		report := siteReport{}
		inputs := append(append(append(append(append([]namedInput{}, probes...), vectors...), v1Corpus...), ladderInputs(limit)...), mutations...)
		for _, input := range inputs {
			answers := answersAt(input.raw, limit)
			report.inputs++
			switch {
			case answers.library:
				report.accepted++
			case answers.refusedForDepth():
				report.refusedByDepth++
			case !answers.vendor:
				report.refusedByVendorToo++
			}
			if !answers.agrees() {
				report.disagreements++
				t.Errorf("limit %d, %s: library accepts=%v (%v), vendor decode accepts=%v; bytes %x",
					limit, input.name, answers.library, answers.libraryErr, answers.vendor, shorten(input.raw))
			}
		}
		for depth := limit - 1; depth <= limit+1; depth++ {
			for _, kind := range []string{"doc", "array", "scope"} {
				answers := answersAt(nestedChain(depth, kind), limit)
				if (depth <= limit) != answers.library || (depth > limit && !answers.refusedForDepth()) {
					t.Errorf("limit %d, %s chain of %d levels: Validate says %v", limit, kind, depth, answers.libraryErr)
				}
			}
		}
		if report.accepted == 0 || report.refusedByDepth == 0 || report.refusedByVendorToo == 0 {
			t.Errorf("limit %d: the corpus must hold accepted, depth-refused and vendor-refused inputs; got %+v", limit, report)
		}
		t.Logf("limit %3d sites %v: inputs %d, accepted %d, refused by depth %d, refused by the vendor decode too %d, disagreements %d",
			limit, groups[limit], report.inputs, report.accepted, report.refusedByDepth, report.refusedByVendorToo, report.disagreements)
		totals.inputs += report.inputs
		totals.disagreements += report.disagreements
	}
	t.Logf("%d limits, %d site rows, %d comparisons, %d disagreements", len(limits), len(sites), totals.inputs, totals.disagreements)
}

// TestDepthSiteTableCoversEveryLimit pins the function-derived rows to exactly
// the limits the differential runs at: a site that drifts to another limit, or
// a new fifth limit, turns this red.
func TestDepthSiteTableCoversEveryLimit(t *testing.T) {
	groups := limitSiteGroups(limitSites())
	want := []int{100, 101, 102, 103}
	for _, limit := range want {
		if len(groups[limit]) == 0 {
			t.Errorf("no site is checked at limit %d; the build doc's step 2a table says 100, 101, 102 and 103", limit)
		}
	}
	for limit, sites := range groups {
		if !slices.Contains(want, limit) {
			t.Errorf("sites %v are checked at limit %d, outside the differential's 100 to 103; a site that drifted to another limit, or a new limit, changes the corpus and the build doc's step 2a table with it", sites, limit)
		}
	}
}

// TestBlobMetaSiteLimitIsTheOneThisTestUses ties the 101 that
// internal/bsonoracle's TestV1CorpusPassesValidateAtItsSiteLimit applies to
// blob metadata records to the limit storedDocumentOf reads them at.
func TestBlobMetaSiteLimitIsTheOneThisTestUses(t *testing.T) {
	const blobMetaSiteDepthLimit = 101
	if got := requestDepthLimit(transport.OpBlobWrite); got != blobMetaSiteDepthLimit {
		t.Fatalf("requestDepthLimit(OpBlobWrite) is %d, and internal/bsonoracle/v1corpus_test.go validates blobmeta records at %d; change both together", got, blobMetaSiteDepthLimit)
	}
}

// shorten keeps a failure message readable for the long corpus records.
func shorten(raw []byte) []byte {
	const most = 96
	if len(raw) > most {
		return raw[:most]
	}
	return raw
}

func FuzzValidateMatchesVendorDecode(f *testing.F) {
	// The library's own regression seed (oracle/testdata/fuzz/
	// FuzzValidateDifferential/87639ac47bade5d1): a binary subtype 0x02 whose
	// inner length disagrees.
	f.Add([]byte("\x13\x00\x00\x00\x050\x000000\x02\x02\x00\x00\x0000\x00"))
	f.Add(richDocument())
	f.Add(nestedChain(100, "doc"))
	f.Add(nestedChain(101, "array"))
	f.Add(nestedChain(100, "scope"))
	f.Add(docOf())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		const fuzzLimit = 101
		answers := answersAt(raw, fuzzLimit)
		if !answers.agrees() {
			t.Fatalf("library accepts=%v (%v), vendor decode accepts=%v at limit %d", answers.library, answers.libraryErr, answers.vendor, fuzzLimit)
		}
	})
}
