package cli

import (
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

// planValueCap bounds what a field keeps: its distinct values, its sizes and
// its array lengths. A value past the cap is counted in the field's present
// count but not kept, so one run's memory grows with the fields, not with the
// documents. A distinct count that reached the cap is a lower bound.
const planValueCap = 4096

// planSource is one input file and the kind it reads as before grouping.
type planSource struct {
	path string
	kind string
}

// planFileInfo is what planEachDocument saw of one file.
type planFileInfo struct {
	// size is the file's length; read is the bytes its documents cover.
	size, read int64
	// cut is true when the sample stopped before the end of the file.
	cut bool
}

// planCollect turns the named files and directories into a model, reading at
// most sample documents per file (0 means all). It opens no socket. A
// directory is listed by resolveDirectory, so its refusals are ingest's, and
// every file is format-checked by detectFormat before any document is read.
// The returned notes say what was grouped or not read.
func planCollect(paths []string, sample int, maxBuckets int) (*PlanModel, []string, error) {
	var notes []string
	var sources []planSource
	for _, input := range paths {
		stat, err := os.Stat(input)
		if err != nil {
			return nil, nil, &ingestUsageError{Message: fmt.Sprintf("cannot open %s: %v. Check the path.", input, describeFSError(err))}
		}
		if stat.IsDir() {
			listed, err := resolveDirectory(directoryRequest{Input: input, MaxBuckets: maxBuckets})
			if err != nil {
				return nil, nil, err
			}
			notes = append(notes, listed.Notes...)
			for _, file := range listed.Files {
				sources = append(sources, planSource{path: file.Path, kind: file.Bucket})
			}
			continue
		}
		head, err := readHead(input)
		if err != nil {
			return nil, nil, err
		}
		if _, err := detectFormat(input, head, ""); err != nil {
			return nil, nil, err
		}
		sources = append(sources, planSource{path: input, kind: planKindName(input)})
	}
	if len(sources) > maxBuckets {
		return nil, nil, fmt.Errorf("plan: %d files is more than --max-buckets %d. Plan fewer files at a time, or raise --max-buckets", len(sources), maxBuckets)
	}

	counters := make([]*planFileCounter, len(sources))
	for index, source := range sources {
		counter := newPlanFileCounter(source.path)
		scan, err := planEachDocument(source.path, sample, counter.observe)
		if err != nil {
			return nil, nil, err
		}
		counter.size, counter.read, counter.cut = scan.size, scan.read, scan.cut
		counters[index] = counter
	}

	groups := planGroupFiles(sources, counters)
	kinds := make([]*planKind, 0, len(groups))
	for _, group := range groups {
		if len(group.members) > 1 {
			notes = append(notes, planGroupNote(group))
		}
		kinds = append(kinds, newPlanKind(group.name, group.members))
	}
	slices.SortFunc(kinds, func(a, b *planKind) int { return strings.Compare(a.name, b.name) })
	for index := 1; index < len(kinds); index++ {
		if kinds[index].name == kinds[index-1].name {
			return nil, nil, fmt.Errorf("plan: two inputs both read as kind %q. Rename one file, or plan them in separate runs", kinds[index].name)
		}
	}

	if err := planResolveReferences(kinds, sample); err != nil {
		return nil, nil, err
	}
	model := &PlanModel{Version: PlanModelVersion, Source: "dump", Kinds: make([]ModelKind, 0, len(kinds))}
	for _, kind := range kinds {
		model.Kinds = append(model.Kinds, kind.modelKind())
	}
	return model, notes, nil
}

// planKindName is the kind a single file becomes: its name without the
// extension, as directoryFile.Bucket is.
func planKindName(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// planFamilyStem strips a trailing run of digits and its separator, so
// customer-001 and customer-002 share the stem customer. It returns "" when
// the name has no digits to strip.
func planFamilyStem(kindName string) string {
	stem := strings.TrimRight(kindName, "0123456789")
	if stem == kindName {
		return ""
	}
	return strings.TrimRight(stem, "-_.")
}

// planOpenSample opens one file for streaming. The format is decided from the
// name and first bytes, as ingest decides it, and the closer must be called.
func planOpenSample(path string) (documentReader, func() error, error) {
	head, err := readHead(path)
	if err != nil {
		return nil, nil, err
	}
	format, err := detectFormat(path, head, "")
	if err != nil {
		return nil, nil, err
	}
	scan, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s: %w", path, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening %s: %w", path, err)
	}
	start := readerStart{name: filepath.Base(path), totalBytes: scan.Size()}
	if format == formatBSON {
		return newBSONReader(file, start), file.Close, nil
	}
	return newJSONLReader(file, start), file.Close, nil
}

// planEachDocument streams the first sample documents of path (all of them
// when sample is 0) into visit, one at a time, and reports what it covered.
// A corrupt document fails the file with the ingest message for it.
func planEachDocument(path string, sample int, visit func(bson.Raw) error) (planFileInfo, error) {
	reader, closeFile, err := planOpenSample(path)
	if err != nil {
		return planFileInfo{}, err
	}
	scan, err := planReadDocuments(path, reader, sample, visit)
	closeErr := closeFile()
	if err != nil {
		return planFileInfo{}, err
	}
	if closeErr != nil {
		return planFileInfo{}, fmt.Errorf("closing %s: %w", path, closeErr)
	}
	return scan, nil
}

// planReadDocuments runs the sample loop over an open reader.
func planReadDocuments(path string, reader documentReader, sample int, visit func(bson.Raw) error) (planFileInfo, error) {
	var scan planFileInfo
	if stat, err := os.Stat(path); err == nil {
		scan.size = stat.Size()
	} else {
		return planFileInfo{}, fmt.Errorf("reading %s: %w", path, err)
	}
	var documents int64
	for sample == 0 || documents < int64(sample) {
		document, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return scan, nil
		}
		if err != nil {
			return planFileInfo{}, planReadFailure(path, err)
		}
		if err := visit(document.Doc); err != nil {
			return planFileInfo{}, err
		}
		documents++
		scan.read = document.End
	}
	if _, err := reader.Next(); err == nil {
		scan.cut = true
	} else if !errors.Is(err, io.EOF) {
		return planFileInfo{}, planReadFailure(path, err)
	}
	return scan, nil
}

// planReadFailure names the file a reader failure belongs to. The ingest
// messages already name it or the document's position, so they are kept.
func planReadFailure(path string, err error) error {
	var corrupt *inputCorruptError
	if errors.As(err, &corrupt) {
		return corrupt
	}
	var failure *documentFailure
	if errors.As(err, &failure) {
		return fmt.Errorf("%s: document %d: %w", filepath.Base(path), failure.Ordinal, failure)
	}
	return fmt.Errorf("reading %s: %w", filepath.Base(path), err)
}

// planFileCounter holds what one file's sample says: per-field counters, the
// _id values, and which top-level scalars held one value in every document.
type planFileCounter struct {
	path       string
	docs       int64
	size, read int64
	cut        bool
	fields     map[string]*planFieldStats
	ids        map[string]struct{}
	// firstKey is the first value of each scalar field; varied marks a field
	// that later held another value or a non-scalar. Together with the field's
	// presence they say whether the field tells this file's documents apart.
	firstKey map[string]string
	varied   map[string]bool
}

// newPlanFileCounter returns an empty counter for one file.
func newPlanFileCounter(path string) *planFileCounter {
	return &planFileCounter{
		path:     path,
		fields:   map[string]*planFieldStats{},
		ids:      map[string]struct{}{},
		firstKey: map[string]string{},
		varied:   map[string]bool{},
	}
}

// observe folds one document into the counter.
func (c *planFileCounter) observe(doc bson.Raw) error {
	c.docs++
	elements, err := doc.Elements()
	if err != nil {
		return fmt.Errorf("%s: reading a document: %w", filepath.Base(c.path), err)
	}
	for _, element := range elements {
		name := element.Key()
		raw := element.Value()
		field := c.fields[name]
		if field == nil {
			field = newPlanFieldStats()
			c.fields[name] = field
		}
		if err := field.observe(raw); err != nil {
			return fmt.Errorf("%s: field %q: %w", filepath.Base(c.path), name, err)
		}
		c.noteConstant(name, raw)
		if name == "_id" {
			c.ids[planValueKey(raw)] = struct{}{}
		}
	}
	return nil
}

// noteConstant records whether a field keeps one scalar value across the file.
func (c *planFileCounter) noteConstant(name string, raw bson.RawValue) {
	if !planIsScalar(raw.Type) {
		c.varied[name] = true
		return
	}
	key := planValueKey(raw)
	if first, seen := c.firstKey[name]; !seen {
		c.firstKey[name] = key
	} else if first != key {
		c.varied[name] = true
	}
}

// signature names the fields and their dominant types, so two files of one
// shape have one signature and can be read as one kind.
func (c *planFileCounter) signature() string {
	var builder strings.Builder
	for _, name := range planSortedNames(c.fields) {
		builder.WriteString(name + ":" + planDominantType(c.fields[name].types) + ";")
	}
	return builder.String()
}

// estimatedDocuments is the document count of the file: the sample when it
// read the whole file, else the sample scaled by the file's size over the
// bytes it covered.
func (c *planFileCounter) estimatedDocuments() int64 {
	if !c.cut {
		return c.docs
	}
	return planScale(c.docs, c.size, c.read)
}

// planScale returns docs*size/read without overflow, saturating at the largest
// int64 when the quotient does not fit.
func planScale(docs, size, read int64) int64 {
	if read <= 0 || size <= 0 || docs <= 0 {
		return docs
	}
	high, low := bits.Mul64(uint64(docs), uint64(size))
	if high >= uint64(read) {
		return math.MaxInt64
	}
	quotient, _ := bits.Div64(high, low, uint64(read))
	if quotient > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(quotient)
}

// planGroup is the set of files that become one kind.
type planGroup struct {
	name    string
	stem    string
	members []*planFileCounter
}

// planGroupFiles puts files of one shape and one family name into one group
// (spec 6.5, T5). A file with no family, or a family of one, keeps its own name.
// Groups keep the order of their first file.
func planGroupFiles(sources []planSource, counters []*planFileCounter) []planGroup {
	var groups []planGroup
	position := map[string]int{}
	for index, counter := range counters {
		stem := planFamilyStem(sources[index].kind)
		key := fmt.Sprintf("file:%d", index)
		if stem != "" {
			key = "family:" + stem + "\x00" + counter.signature()
		}
		at, seen := position[key]
		if !seen {
			at = len(groups)
			position[key] = at
			groups = append(groups, planGroup{name: sources[index].kind, stem: stem})
		}
		groups[at].members = append(groups[at].members, counter)
	}
	for index := range groups {
		if len(groups[index].members) > 1 && groups[index].stem != "" {
			groups[index].name = groups[index].stem
		}
	}
	return groups
}

// planGroupNote says what a group of files became, and the field that tells
// its files apart when there is one.
func planGroupNote(group planGroup) string {
	field := planDiscriminator(group.members)
	if field == "" {
		return fmt.Sprintf("%d files of one shape are read as one kind %q, and no field tells them apart. Check that they are one collection before creating the bucket.",
			len(group.members), group.name)
	}
	return fmt.Sprintf("%d files of one shape are read as one kind %q: one kind with a discriminating field %q, which holds one value per file. A bucket per file is not proposed.",
		len(group.members), group.name, field)
}

// planDiscriminator returns the first field (by name) that holds one scalar
// value in every document of each file, with a different value in each file.
func planDiscriminator(members []*planFileCounter) string {
	for _, name := range planSortedNames(members[0].fields) {
		if planTellsFilesApart(name, members) {
			return name
		}
	}
	return ""
}

// planTellsFilesApart reports whether field name is constant within each file
// and different across them.
func planTellsFilesApart(name string, members []*planFileCounter) bool {
	seen := make(map[string]bool, len(members))
	for _, member := range members {
		field, present := member.fields[name]
		key, constant := member.firstKey[name]
		if !present || !constant || member.varied[name] || field.present != member.docs {
			return false
		}
		if seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

// planKind is one bucket the model describes: one or more grouped files, their
// merged field counters, and the reference targets resolved after the first pass.
type planKind struct {
	name      string
	members   []*planFileCounter
	docs      int64
	documents int64
	fields    map[string]*planFieldStats
	ids       map[string]struct{}
	refs      map[string]*ModelRef
}

// newPlanKind merges the counters of a group's files into one kind.
func newPlanKind(name string, members []*planFileCounter) *planKind {
	kind := &planKind{
		name:    name,
		members: members,
		fields:  map[string]*planFieldStats{},
		ids:     map[string]struct{}{},
		refs:    map[string]*ModelRef{},
	}
	for _, member := range members {
		kind.docs += member.docs
		kind.documents = planSaturatingAdd(kind.documents, member.estimatedDocuments())
		for _, fieldName := range planSortedNames(member.fields) {
			merged := kind.fields[fieldName]
			if merged == nil {
				merged = newPlanFieldStats()
				kind.fields[fieldName] = merged
			}
			merged.absorb(member.fields[fieldName])
		}
		for key := range member.ids {
			kind.ids[key] = struct{}{}
		}
	}
	return kind
}

// planSaturatingAdd adds two non-negative counts, saturating at the largest int64.
func planSaturatingAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// modelKind writes the kind in the model's shape, fields in name order.
func (k *planKind) modelKind() ModelKind {
	kind := ModelKind{Name: k.name, Documents: k.documents, Sampled: k.docs, Fields: make([]ModelField, 0, len(k.fields))}
	for _, name := range planSortedNames(k.fields) {
		field := k.fields[name].modelField(name, k.docs)
		field.Ref = k.refs[name]
		kind.Fields = append(kind.Fields, field)
	}
	return kind
}

// planRefTally counts, for one candidate field, the values that resolve to each target.
type planRefTally struct {
	nonNull int64
	hits    map[string]int64
}

// planResolveReferences is the second pass over the same sample. Every string
// or object-id field whose name ends in _id (or that is an object id) is
// resolved against the _id values of every kind, and the kind that resolves
// the most values is its ModelRef.To.
func planResolveReferences(kinds []*planKind, sample int) error {
	for _, kind := range kinds {
		candidates := planReferenceCandidates(kind)
		if len(candidates) == 0 {
			continue
		}
		tallies := map[string]*planRefTally{}
		for _, member := range kind.members {
			_, err := planEachDocument(member.path, sample, func(doc bson.Raw) error {
				elements, err := doc.Elements()
				if err != nil {
					return fmt.Errorf("%s: reading a document: %w", filepath.Base(member.path), err)
				}
				for _, element := range elements {
					name := element.Key()
					raw := element.Value()
					if !candidates[name] || (raw.Type != bsontype.String && raw.Type != bsontype.ObjectID) {
						continue
					}
					tally := tallies[name]
					if tally == nil {
						tally = &planRefTally{hits: map[string]int64{}}
						tallies[name] = tally
					}
					tally.nonNull++
					key := planValueKey(raw)
					for _, target := range kinds {
						if _, found := target.ids[key]; found {
							tally.hits[target.name]++
						}
					}
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("read %s for references: %w", member.path, err)
			}
		}
		for _, name := range planSortedNames(tallies) {
			tally := tallies[name]
			best, bestHits := "", int64(0)
			for _, target := range kinds {
				if hits := tally.hits[target.name]; hits > bestHits {
					best, bestHits = target.name, hits
				}
			}
			if best != "" {
				kind.refs[name] = &ModelRef{To: best, ResolvePermille: permille(bestHits, tally.nonNull)}
			}
		}
	}
	return nil
}

// planReferenceCandidates names the top-level fields that may hold another
// kind's _id: a string or object id whose name ends in _id, or an object id.
func planReferenceCandidates(kind *planKind) map[string]bool {
	candidates := map[string]bool{}
	for name, field := range kind.fields {
		if name == "_id" {
			continue
		}
		switch planDominantType(field.types) {
		case "objectid":
			candidates[name] = true
		case "string":
			if strings.HasSuffix(name, "_id") {
				candidates[name] = true
			}
		}
	}
	return candidates
}

// planFieldStats is what a field's values say: presence, the types seen, the
// distinct values kept, the sizes kept, and for an array its element facts.
type planFieldStats struct {
	present  int64
	types    map[string]int64
	distinct map[string]struct{}
	sizes    []int64
	array    *planArrayStats
}

// newPlanFieldStats returns empty counters for one field.
func newPlanFieldStats() *planFieldStats {
	return &planFieldStats{types: map[string]int64{}, distinct: map[string]struct{}{}}
}

// observe folds one value of the field into its counters.
func (f *planFieldStats) observe(raw bson.RawValue) error {
	f.present++
	f.types[planTypeWord(raw.Type)]++
	switch raw.Type {
	case bsontype.Array:
		if f.array == nil {
			f.array = newPlanArrayStats()
		}
		return f.array.observe(raw.Array())
	case bsontype.EmbeddedDocument:
		return nil
	}
	size := int64(len(raw.Value))
	if raw.Type == bsontype.Binary {
		if _, payload, ok := raw.BinaryOK(); ok {
			size = int64(len(payload))
		}
	}
	f.sizes = planAppendCapped(f.sizes, size)
	if len(f.distinct) < planValueCap {
		f.distinct[planValueKey(raw)] = struct{}{}
	}
	return nil
}

// absorb adds another counter of the same field into this one.
func (f *planFieldStats) absorb(other *planFieldStats) {
	f.present += other.present
	for word, count := range other.types {
		f.types[word] += count
	}
	for key := range other.distinct {
		if len(f.distinct) < planValueCap {
			f.distinct[key] = struct{}{}
		}
	}
	for _, size := range other.sizes {
		f.sizes = planAppendCapped(f.sizes, size)
	}
	if other.array != nil {
		if f.array == nil {
			f.array = newPlanArrayStats()
		}
		f.array.absorb(other.array)
	}
}

// modelField writes the field in the model's shape. docs is the sampled
// document count of its kind, which every presence fraction is taken over.
func (f *planFieldStats) modelField(name string, docs int64) ModelField {
	word := planDominantType(f.types)
	field := ModelField{Name: name, Type: word, PresentPermille: permille(f.present, docs)}
	switch word {
	case "object":
	case "array":
		if f.array != nil {
			field.Array = f.array.modelArray()
		}
	default:
		field.DistinctPermille = permille(int64(len(f.distinct)), f.present)
		sizes := slices.Clone(f.sizes)
		slices.Sort(sizes)
		field.BytesP50 = planPercentile(sizes, 500)
	}
	return field
}

// planArrayStats holds an array field's facts: its lengths, whether its
// elements are documents, the counters of the fields inside those documents,
// and the adjacent-pair and distinctness counts that name an order and a key.
type planArrayStats struct {
	lengths  []int64
	elements int64
	docs     int64
	fields   map[string]*planFieldStats
	pairs    map[string]int64
	monotone map[string]int64
	distinct map[string]int64
}

// newPlanArrayStats returns empty counters for one array field.
func newPlanArrayStats() *planArrayStats {
	return &planArrayStats{
		fields:   map[string]*planFieldStats{},
		pairs:    map[string]int64{},
		monotone: map[string]int64{},
		distinct: map[string]int64{},
	}
}

// observe folds one array value into the counters. Adjacent elements that both
// hold a field are compared in array order; distinct values are counted per
// array, because a key is a field unique within each array.
func (a *planArrayStats) observe(array bson.Raw) error {
	raws, err := array.Values()
	if err != nil {
		return fmt.Errorf("reading an array: %w", err)
	}
	a.lengths = planAppendCapped(a.lengths, int64(len(raws)))
	a.elements += int64(len(raws))
	previous := map[string]planOrderKey{}
	seen := map[string]map[string]struct{}{}
	for _, raw := range raws {
		if raw.Type != bsontype.EmbeddedDocument {
			continue
		}
		a.docs++
		elements, err := raw.Document().Elements()
		if err != nil {
			return fmt.Errorf("reading an array element: %w", err)
		}
		for _, element := range elements {
			name := element.Key()
			inner := element.Value()
			field := a.fields[name]
			if field == nil {
				field = newPlanFieldStats()
				a.fields[name] = field
			}
			if err := field.observe(inner); err != nil {
				return fmt.Errorf("field %s: %w", name, err)
			}
			if key, ordered := planOrderOf(inner); ordered {
				if last, had := previous[name]; had {
					a.pairs[name]++
					if planOrderLE(last, key) {
						a.monotone[name]++
					}
				}
				previous[name] = key
			}
			if planIsScalar(inner.Type) {
				if seen[name] == nil {
					seen[name] = map[string]struct{}{}
				}
				seen[name][planValueKey(inner)] = struct{}{}
			}
		}
	}
	for name, values := range seen {
		a.distinct[name] += int64(len(values))
	}
	return nil
}

// absorb adds another counter of the same array field into this one.
func (a *planArrayStats) absorb(other *planArrayStats) {
	for _, length := range other.lengths {
		a.lengths = planAppendCapped(a.lengths, length)
	}
	a.elements += other.elements
	a.docs += other.docs
	for name, field := range other.fields {
		merged := a.fields[name]
		if merged == nil {
			merged = newPlanFieldStats()
			a.fields[name] = merged
		}
		merged.absorb(field)
	}
	for name, count := range other.pairs {
		a.pairs[name] += count
	}
	for name, count := range other.monotone {
		a.monotone[name] += count
	}
	for name, count := range other.distinct {
		a.distinct[name] += count
	}
}

// modelArray writes the array's facts. Order and key are only named when the
// elements are documents; the order is a named time field, else the first
// time or object-id field present in every element.
func (a *planArrayStats) modelArray() *ModelArray {
	lengths := slices.Clone(a.lengths)
	slices.Sort(lengths)
	array := &ModelArray{
		LenP50:         int(planPercentile(lengths, 500)),
		LenP95:         int(planPercentile(lengths, 950)),
		ElemIsDocument: a.elements > 0 && a.docs == a.elements,
	}
	if len(lengths) > 0 {
		array.LenMax = int(lengths[len(lengths)-1])
	}
	if a.docs == 0 {
		return array
	}
	array.OrderField, array.MonotonePermille = a.order()
	array.KeyField, array.KeyDistinctPermille, array.KeyIsTime = a.key()
	return array
}

// order names the field the elements are ordered by and the share of its
// adjacent pairs that rise.
func (a *planArrayStats) order() (string, int) {
	names := planSortedNames(a.fields)
	choice := ""
	for _, name := range names {
		if planIsOrderName(name) {
			choice = name
			break
		}
	}
	if choice == "" {
		for _, name := range names {
			field := a.fields[name]
			if word := planDominantType(field.types); field.present == a.docs && (word == "datetime" || word == "objectid") {
				choice = name
				break
			}
		}
	}
	if choice == "" {
		return "", 0
	}
	return choice, permille(a.monotone[choice], a.pairs[choice])
}

// key names the field whose values are distinct within each array most often,
// among the scalar fields every element holds; it returns that share in per-mille.
func (a *planArrayStats) key() (string, int, bool) {
	best, bestPermille := "", -1
	for _, name := range planSortedNames(a.fields) {
		field := a.fields[name]
		word := planDominantType(field.types)
		if field.present != a.docs || word == "object" || word == "array" {
			continue
		}
		if share := permille(a.distinct[name], field.present); share > bestPermille {
			best, bestPermille = name, share
		}
	}
	if best == "" {
		return "", 0, false
	}
	return best, bestPermille, planDominantType(a.fields[best].types) == "datetime"
}

// planIsOrderName reports whether a field name says the elements are in time order.
func planIsOrderName(name string) bool {
	switch name {
	case "ts", "time", "at", "date", "_id":
		return true
	}
	return strings.HasPrefix(name, "created")
}

// planOrderKey is a value's place in an order: times and numbers compare as
// integers, doubles as reals, strings as text and object ids as their bytes.
// Values of different families never compare.
type planOrderKey struct {
	family  string
	integer int64
	real    float64
	text    string
}

// planOrderOf returns the order key of a value, or false when it has none.
func planOrderOf(raw bson.RawValue) (planOrderKey, bool) {
	switch raw.Type {
	case bsontype.DateTime:
		return planOrderKey{family: "time", integer: raw.DateTime()}, true
	case bsontype.Int32:
		return planOrderKey{family: "number", integer: int64(raw.Int32())}, true
	case bsontype.Int64:
		return planOrderKey{family: "number", integer: raw.Int64()}, true
	case bsontype.Double:
		return planOrderKey{family: "double", real: raw.Double()}, true
	case bsontype.String:
		return planOrderKey{family: "text", text: raw.StringValue()}, true
	case bsontype.ObjectID:
		return planOrderKey{family: "bytes", text: string(raw.Value)}, true
	}
	return planOrderKey{}, false
}

// planOrderLE reports whether next does not fall below prev.
func planOrderLE(prev, next planOrderKey) bool {
	if prev.family != next.family {
		return false
	}
	switch prev.family {
	case "double":
		return prev.real <= next.real
	case "text", "bytes":
		return prev.text <= next.text
	}
	return prev.integer <= next.integer
}

// planValueKey identifies a value by its type and bytes, so equal values of
// the same type have one key and the same text in two types does not collide.
func planValueKey(raw bson.RawValue) string {
	return string([]byte{byte(raw.Type)}) + string(raw.Value)
}

// planIsScalar reports whether a BSON type is a scalar (not a document or array).
func planIsScalar(word bsontype.Type) bool {
	switch word {
	case bsontype.String, bsontype.Int32, bsontype.Int64, bsontype.Double, bsontype.Boolean,
		bsontype.DateTime, bsontype.ObjectID:
		return true
	}
	return false
}

// planTypeWord names a BSON type with the words the model uses.
func planTypeWord(word bsontype.Type) string {
	switch word {
	case bsontype.String:
		return "string"
	case bsontype.Int32, bsontype.Int64:
		return "int"
	case bsontype.Double:
		return "double"
	case bsontype.Boolean:
		return "bool"
	case bsontype.DateTime:
		return "datetime"
	case bsontype.ObjectID:
		return "objectid"
	case bsontype.Binary:
		return "binary"
	case bsontype.EmbeddedDocument:
		return "object"
	case bsontype.Array:
		return "array"
	}
	return strings.ToLower(word.String())
}

// planDominantType returns the type word seen most often; a tie goes to the
// word first in name order, so the same counts always name the same type.
func planDominantType(types map[string]int64) string {
	best, bestCount := "", int64(0)
	for _, word := range planSortedNames(types) {
		if types[word] > bestCount {
			best, bestCount = word, types[word]
		}
	}
	return best
}

// planSortedNames lists a map's keys in order, so output never depends on map iteration.
func planSortedNames[V any](values map[string]V) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// planAppendCapped appends entry unless the slice already holds planValueCap values.
func planAppendCapped(values []int64, entry int64) []int64 {
	if len(values) >= planValueCap {
		return values
	}
	return append(values, entry)
}

// planPercentile returns the value at a per-mille rank of sorted values, or 0 when there are none.
func planPercentile(sorted []int64, rank int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)-1)*rank/1000]
}

// permille returns part as a per-mille share of whole, or 0 when whole is 0.
func permille(part, whole int64) int {
	if whole <= 0 {
		return 0
	}
	return int(part * 1000 / whole)
}
