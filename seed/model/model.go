// Package model generates the five example data sets (social, chat, ledger, agent memory, video)
// deterministically from a seed and a scale, so every run loads the same entities and the same
// bytes and the how-to pages' sample output stays true.
//
// The generator knows nothing about any engine. It yields Units: a document of a bucket
// together with the store elements that belong to it, in a fixed order. The seeders map a
// unit onto their own schema.
package model

import (
	"fmt"
	"hash/fnv"
	"iter"
	"math/rand/v2"
	"strings"
)

// Layout names a scenario layout.
type Layout string

// The five example layouts.
const (
	Social Layout = "social"
	Chat   Layout = "chat"
	Ledger Layout = "ledger"
	Agent  Layout = "agent"
	// Video is the fifth layout: the entities around a video library. The bytes of a video are
	// not stored; Path names where they live.
	Video Layout = "video"
)

// Layouts lists every layout in the order "all" seeds them.
var Layouts = []Layout{Social, Chat, Ledger, Agent, Video}

// ParseLayout turns a flag value into a Layout.
func ParseLayout(name string) (Layout, error) {
	for _, layout := range Layouts {
		if string(layout) == name {
			return layout, nil
		}
	}
	return "", fmt.Errorf("unknown layout %q. Use one of social, chat, ledger, agent or video (or all, which seeds each in a database of its own)", name)
}

// Field is one named scalar. Value is a string, an int64 or, for an embedding vector, a []byte.
type Field struct {
	Name  string
	Value any
}

// Document is one document of a bucket: the scalar fields the layout declares, and the
// denormalised counters a relational schema would keep beside them. hoardDB derives those from
// its stores, so seeding never writes Counters; they exist so a test can compare a loaded store
// with what the generator says it holds.
type Document struct {
	Bucket   string
	ID       string
	Fields   []Field
	Counters []Field
}

// Element is one element of a store field of a document. Key is empty for a fifoStore and
// lifoStore and is the element key for a hashStore and btreeStore. Seq counts the elements
// of the same store of the same owner from 0, in push order.
type Element struct {
	Bucket, Owner, Store string
	Key                  string
	Seq                  int64
	Body                 []Field
}

// StoreName is "Bucket.Store", the name a count is reported under.
func (e Element) StoreName() string { return e.Bucket + "." + e.Store }

// Unit is a document and the elements of its own stores. One worker writes a unit start to
// finish, so pushes onto a store keep their order.
type Unit struct {
	Doc      Document
	Elements []Element
}

// FieldDecl declares one field of a bucket with the type word the layouts document uses.
type FieldDecl struct{ Name, Type string }

// IsStore reports whether the field is a store.
func (f FieldDecl) IsStore() bool { return strings.HasSuffix(f.Type, "Store") }

// BucketDecl is one bucket as `create bucket` declares it.
type BucketDecl struct {
	Name   string
	Fields []FieldDecl
}

// Counts is what a generated data set holds: documents by bucket name and store elements
// by "Bucket.Store".
type Counts map[string]int64

// Generator is one layout at one seed and scale.
type Generator struct {
	layout Layout
	seed   uint64
	scale  int
	schema []BucketDecl
	units  []Unit
	counts Counts
	opts   Options
}

// Options are the parameters of a layout beyond its seed and scale. The zero value is the layout
// every engine is normally seeded with.
type Options struct {
	// LedgerExpenses is the expenses each ledger account holds; 0 is the default of 100. Another
	// layout ignores it.
	LedgerExpenses int
	// ElementsPerStore caps the elements each document keeps in each of its stores; 0 keeps them
	// all. It exists so engine tests can load every layout in seconds: the cap is applied before
	// the counts are taken, so Counts still says what is loaded, but a document's own counter
	// fields are not adjusted.
	ElementsPerStore int
}

// New generates a layout with the default options. scale multiplies every count; it must be 1 or
// more.
func New(layout Layout, seed uint64, scale int) (*Generator, error) {
	return NewWith(layout, seed, scale, Options{})
}

// NewWith generates a layout with options.
func NewWith(layout Layout, seed uint64, scale int, opts Options) (*Generator, error) {
	if scale < 1 {
		return nil, fmt.Errorf("scale %d: -scale must be 1 or more", scale)
	}
	if opts.ElementsPerStore < 0 {
		return nil, fmt.Errorf("elements per store %d: use 1 or more, or 0 to keep them all", opts.ElementsPerStore)
	}
	if opts.LedgerExpenses < 0 {
		return nil, fmt.Errorf("ledger expenses per account %d: use 1 or more, or 0 for the default of %d", opts.LedgerExpenses, expensesPerAccount)
	}
	g := &Generator{layout: layout, seed: seed, scale: scale, opts: opts}
	switch layout {
	case Social:
		g.schema, g.units = buildSocial(g)
	case Chat:
		g.schema, g.units = buildChat(g)
	case Ledger:
		g.schema, g.units = buildLedger(g)
	case Agent:
		g.schema, g.units = buildAgent(g)
	case Video:
		g.schema, g.units = buildVideo(g)
	default:
		return nil, fmt.Errorf("unknown layout %q", layout)
	}
	if opts.ElementsPerStore > 0 {
		g.capElements(opts.ElementsPerStore)
	}
	g.counts = Counts{}
	for _, decl := range g.schema {
		g.counts[decl.Name] = 0
		for _, field := range decl.Fields {
			if field.IsStore() {
				g.counts[decl.Name+"."+field.Name] = 0
			}
		}
	}
	for _, unit := range g.units {
		g.counts[unit.Doc.Bucket]++
		for _, element := range unit.Elements {
			g.counts[element.StoreName()]++
		}
	}
	return g, nil
}

// capElements keeps the first limit elements of each store of each document.
func (g *Generator) capElements(limit int) {
	for i := range g.units {
		kept := map[string]int{}
		elements := g.units[i].Elements[:0:0]
		for _, element := range g.units[i].Elements {
			if kept[element.Store] < limit {
				kept[element.Store]++
				elements = append(elements, element)
			}
		}
		g.units[i].Elements = elements
	}
}

// Layout is the layout this generator holds.
func (g *Generator) Layout() Layout { return g.layout }

// Seed is the seed it was built from.
func (g *Generator) Seed() uint64 { return g.seed }

// Scale is the multiplier it was built with.
func (g *Generator) Scale() int { return g.scale }

// Schema returns the buckets in creation order, as the layouts document declares them.
func (g *Generator) Schema() []BucketDecl { return g.schema }

// Counts returns the documents per bucket and the elements per store the data set holds.
// Every declared bucket and store is present, with 0 when nothing is seeded into it.
func (g *Generator) Counts() Counts {
	out := make(Counts, len(g.counts))
	for name, count := range g.counts {
		out[name] = count
	}
	return out
}

// Units yields every unit in a fixed order.
func (g *Generator) Units() iter.Seq[Unit] {
	return func(yield func(Unit) bool) {
		for _, unit := range g.units {
			if !yield(unit) {
				return
			}
		}
	}
}

// Documents yields the documents of one bucket in the fixed order.
func (g *Generator) Documents(bucket string) iter.Seq[Document] {
	return func(yield func(Document) bool) {
		for _, unit := range g.units {
			if unit.Doc.Bucket == bucket && !yield(unit.Doc) {
				return
			}
		}
	}
}

// Elements yields the elements of one store ("Bucket.Store") in the fixed order.
func (g *Generator) Elements(store string) iter.Seq[Element] {
	return func(yield func(Element) bool) {
		for _, unit := range g.units {
			for _, element := range unit.Elements {
				if element.StoreName() == store && !yield(element) {
					return
				}
			}
		}
	}
}

// stream returns the random stream of one purpose, so adding a draw to one entity never
// shifts the numbers of another.
func stream(seed uint64, domain string, index int) *rand.Rand {
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(domain))
	return rand.New(rand.NewPCG(seed, digest.Sum64()^(uint64(index)+1)*0x9e3779b97f4a7c15))
}

// epoch is the instant (seconds) every generated time counts from.
const epoch int64 = 1_700_000_000

// vocabulary is the word list of the seed, so text is the same bytes on every run.
type vocabulary []string

func newVocabulary(seed uint64) vocabulary {
	rng := stream(seed, "vocabulary", 0)
	words := make(vocabulary, 2048)
	for i := range words {
		letters := make([]byte, 2+rng.IntN(8))
		for j := range letters {
			letters[j] = byte('a' + rng.IntN(26))
		}
		words[i] = string(letters)
	}
	return words
}

// sentence returns count space-separated words.
func (v vocabulary) sentence(rng *rand.Rand, count int) string {
	words := make([]string, count)
	for i := range words {
		words[i] = v[rng.IntN(len(v))]
	}
	return strings.Join(words, " ")
}

// builder collects units and numbers store elements as they are added.
type builder struct {
	units []*Unit
	index map[string]*Unit
	seqs  map[string]int64
}

func newBuilder() *builder {
	return &builder{index: map[string]*Unit{}, seqs: map[string]int64{}}
}

// document adds a unit and returns it so elements can follow.
func (b *builder) document(bucket, id string, fields, counters []Field) *Unit {
	unit := &Unit{Doc: Document{Bucket: bucket, ID: id, Fields: fields, Counters: counters}}
	b.units = append(b.units, unit)
	b.index[bucket+"/"+id] = unit
	return unit
}

// element appends to a store of an existing document, numbering it after the elements already there.
func (b *builder) element(bucket, owner, store, key string, body ...Field) {
	unit := b.index[bucket+"/"+owner]
	name := bucket + "/" + owner + "/" + store
	unit.Elements = append(unit.Elements, Element{Bucket: bucket, Owner: owner, Store: store, Key: key, Seq: b.seqs[name], Body: body})
	b.seqs[name]++
}

// held is how many elements a store holds so far, for the denormalised counters.
func (b *builder) held(bucket, owner, store string) int64 {
	return b.seqs[bucket+"/"+owner+"/"+store]
}

func (b *builder) finish() []Unit {
	out := make([]Unit, len(b.units))
	for i, unit := range b.units {
		out[i] = *unit
	}
	return out
}

// setCounters sets the counters on a document after its elements are known.
func (b *builder) setCounters(bucket, id string, counters ...Field) {
	b.index[bucket+"/"+id].Doc.Counters = counters
}

func decl(fields ...string) []FieldDecl {
	out := make([]FieldDecl, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		out = append(out, FieldDecl{Name: fields[i], Type: fields[i+1]})
	}
	return out
}

// distinct draws count different values from 0 up to but not including n, none of them skip.
func distinct(rng *rand.Rand, n, count, skip int) []int {
	if count > n-1 {
		count = n - 1
	}
	taken := map[int]bool{skip: true}
	out := make([]int, 0, count)
	for len(out) < count {
		candidate := rng.IntN(n)
		if !taken[candidate] {
			taken[candidate] = true
			out = append(out, candidate)
		}
	}
	return out
}
