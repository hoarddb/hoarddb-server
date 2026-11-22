package main

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// benchLayouts are the four layouts a run serves at the same time, in seeding order.
var benchLayouts = []model.Layout{model.Social, model.Chat, model.Ledger, model.Agent}

// Dataset is what one layout holds right after it is seeded, read from the model: every
// document, and the elements of every store in order. The verifier, the audit and the
// planner all read it and never change it.
type Dataset struct {
	layout model.Layout
	seed   uint64
	scale  int
	counts model.Counts
	finger string
	// nativeFinger is the model's hash of the native variant's embedded copies, empty for a
	// layout that has none.
	nativeFinger string
	stores       map[StoreRef][]model.Element
	docs         map[string]model.Document
	idList       map[string][]string
}

// newDataset generates a layout and indexes it.
func newDataset(layout model.Layout, seed uint64, scale int) (*Dataset, error) {
	return newDatasetWith(layout, seed, scale, model.Options{})
}

// newDatasetWith is newDataset with generator options (a deep ledger).
func newDatasetWith(layout model.Layout, seed uint64, scale int, opts model.Options) (*Dataset, error) {
	gen, err := model.NewWith(layout, seed, scale, opts)
	if err != nil {
		return nil, fmt.Errorf("generating layout %s: %w", layout, err)
	}
	ds := &Dataset{layout: layout, seed: seed, scale: scale, counts: gen.Counts(),
		stores: map[StoreRef][]model.Element{}, docs: map[string]model.Document{}, idList: map[string][]string{}}
	digest := fnv.New64a()
	for unit := range gen.Units() {
		doc := unit.Doc
		ds.docs[doc.Bucket+"/"+doc.ID] = doc
		ds.idList[doc.Bucket] = append(ds.idList[doc.Bucket], doc.ID)
		// hash.Hash.Write never returns an error.
		_, _ = fmt.Fprintf(digest, "D%s/%s|%s|%s\n", doc.Bucket, doc.ID, canonBody(doc.Fields), canonBody(doc.Counters))
		for _, element := range unit.Elements {
			ref := StoreRef{element.Bucket, element.Owner, element.Store}
			ds.stores[ref] = append(ds.stores[ref], element)
			_, _ = fmt.Fprintf(digest, "E%s/%s/%s|%s|%d|%s\n", element.Bucket, element.Owner, element.Store, element.Key, element.Seq, canonBody(element.Body))
		}
	}
	ds.finger = fmt.Sprintf("%s-%016x", layout, digest.Sum64())
	ds.nativeFinger = gen.Native().Fingerprint()
	return ds, nil
}

// count is the seeded count of a bucket or of "Bucket.Store".
func (d *Dataset) count(name string) int64 { return d.counts[name] }

// ids lists the document ids of a bucket in seeding order.
func (d *Dataset) ids(bucket string) []string { return d.idList[bucket] }

// doc returns one seeded document.
func (d *Dataset) doc(bucket, id string) (model.Document, bool) {
	doc, ok := d.docs[bucket+"/"+id]
	return doc, ok
}

// elements returns the seeded elements of one store of one owner in order.
func (d *Dataset) elements(ref StoreRef) []model.Element { return d.stores[ref] }

// seededKey reports whether the store was seeded with an element under key.
func (d *Dataset) seededKey(ref StoreRef, key string) bool {
	for _, element := range d.stores[ref] {
		if element.Key == key {
			return true
		}
	}
	return false
}

// fingerprint hashes every document and element, so two engines can be shown to hold the
// same data set.
func (d *Dataset) fingerprint() string { return d.finger }

// canonBody renders fields so that two bodies are equal exactly when their names, order,
// types and values are. Integers of any width count as int64, which is what adapters return.
func canonBody(fields []model.Field) string {
	var out strings.Builder
	for _, field := range fields {
		switch value := field.Value.(type) {
		case string:
			fmt.Fprintf(&out, "%s=s:%q;", field.Name, value)
		case []byte:
			fmt.Fprintf(&out, "%s=b:%s;", field.Name, hex.EncodeToString(value))
		case int, int32, int64:
			fmt.Fprintf(&out, "%s=i:%d;", field.Name, toInt64(value))
		default:
			fmt.Fprintf(&out, "%s=?%T:%v;", field.Name, value, value)
		}
	}
	return out.String()
}

func toInt64(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

// canonElement is the identity of an element: its place, key and body.
func canonElement(e model.Element) string {
	return e.Bucket + "/" + e.Owner + "/" + e.Store + "|" + e.Key + "|" + canonBody(e.Body)
}
