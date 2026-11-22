package main

import (
	"strings"
	"testing"
)

// rankFakeStore is an in-memory keyed store whose faults are switchable, so
// the test can prove each leg's verification fails a wrong answer rather than
// timing it.
type rankFakeStore struct {
	docs        map[string]rankDoc
	corruptKey  string
	dropKey     string
	shortBands  bool
	reportFresh bool
}

func (f *rankFakeStore) upsert(doc rankDoc) (bool, error) {
	_, existed := f.docs[doc.Key]
	f.docs[doc.Key] = doc
	return f.reportFresh && !existed, nil
}

func (f *rankFakeStore) read(key string) (rankDoc, bool, error) {
	if key == f.dropKey {
		return rankDoc{}, false, nil
	}
	doc, found := f.docs[key]
	if key == f.corruptKey {
		doc.Seq++
	}
	return doc, found, nil
}

func (f *rankFakeStore) scan(low, high string) ([]rankDoc, error) {
	var band []rankDoc
	for i := 0; ; i++ {
		doc, found := f.docs[rankDocAt(i).Key]
		if !found {
			break
		}
		if doc.Key >= low && doc.Key <= high {
			band = append(band, doc)
		}
	}
	if f.shortBands {
		band = band[:len(band)-1]
	}
	return band, nil
}

type rankFakeEngine struct {
	store   *rankFakeStore
	docs    int
	preload bool
}

func (e *rankFakeEngine) leg() rankLeg       { return rankLeg{Engine: "fake", Docs: e.docs} }
func (e *rankFakeEngine) createdKnown() bool { return e.store.reportFresh }
func (e *rankFakeEngine) close() error       { return nil }

// open gives each store fresh state, as the real engines' drop does — or, for
// the stale-state case, a store already holding the workload.
func (e *rankFakeEngine) open(store string) (rankStore, error) {
	e.store.docs = map[string]rankDoc{}
	if e.preload {
		for i := 0; i < e.docs; i++ {
			e.store.docs[rankDocAt(i).Key] = rankDocAt(i)
		}
	}
	return e.store, nil
}

func TestRankLegVerification(t *testing.T) {
	cases := []struct {
		name      string
		store     *rankFakeStore
		preload   bool
		wantError string
	}{
		{name: "correct store passes", store: &rankFakeStore{reportFresh: true}},
		{name: "wrong document fails the point read", store: &rankFakeStore{corruptKey: rankDocAt(7).Key}, wantError: "mismatches=1"},
		{name: "missing document fails the point read", store: &rankFakeStore{dropKey: rankDocAt(3).Key}, wantError: "misses=1"},
		{name: "short band fails the range", store: &rankFakeStore{shortBands: true}, wantError: "bad_bands=200"},
		{name: "stale state fails the insert", store: &rankFakeStore{reportFresh: true}, preload: true, wantError: "created=0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runRankLeg(&rankFakeEngine{store: tc.store, docs: 150, preload: tc.preload})
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("a correct store failed verification: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("want a verification failure containing %q, got %v", tc.wantError, err)
			}
		})
	}
}
