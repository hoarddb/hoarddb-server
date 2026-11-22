package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func completeFacts() map[string]any {
	facts := map[string]any{}
	for _, key := range requiredFacts {
		facts[key] = "x"
	}
	return facts
}

func completeRecord() *Record {
	return &Record{Schema: recordSchema, Engine: "fake", Level: "L-A", VUsers: 4, Repetition: 1, Loop: "closed", Scale: 300,
		Settings: map[string]string{"engine_version": "1", "tls": "off"}, Facts: completeFacts(),
		Profile: "R", ReadShares: map[string]float64{"social": 0.9, "chat": 0.6, "ledger": 0.7, "agent": 0.5},
		Access: "uniform", PageDepth: 0.3, Capabilities: []string{}}
}

func TestRecordCarriesTheProfileAndRefusesWithoutIt(t *testing.T) {
	rec := completeRecord()
	rec.Profile = ""
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "profile") {
		t.Errorf("a record without a profile name must be refused, got %v", err)
	}
	rec = completeRecord()
	rec.ReadShares = nil
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "read_shares") {
		t.Errorf("a record without the per-layout read shares must be refused, got %v", err)
	}
}

func TestRecordRefusesMissingFacts(t *testing.T) {
	rec := completeRecord()
	delete(rec.Facts, "mount_options")
	delete(rec.Facts, "kernel")
	err := rec.validate()
	if err == nil || !strings.Contains(err.Error(), "mount_options") || !strings.Contains(err.Error(), "kernel") {
		t.Fatalf("a record without facts must be refused and name them, got %v", err)
	}
	rec = completeRecord()
	delete(rec.Settings, "tls")
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "tls") {
		t.Errorf("missing engine setting tls must be refused, got %v", err)
	}
	rec = completeRecord()
	rec.VUsers = 0
	if err := rec.validate(); err == nil {
		t.Error("a record without a virtual-user count must be refused")
	}
	if err := completeRecord().validate(); err != nil {
		t.Errorf("a complete record failed: %v", err)
	}
}

func TestRecordCarriesTheAccessModeAndRefusesWithoutIt(t *testing.T) {
	rec := completeRecord()
	rec.Access = ""
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "access") {
		t.Errorf("a record without an access mode must be refused, got %v", err)
	}
}

func TestRecordFileNamesKeepLegsApart(t *testing.T) {
	dir := t.TempDir()
	base := completeRecord()
	variants := []func(*Record){
		func(r *Record) {},
		func(r *Record) { r.Op = "social.follow_scan" },
		func(r *Record) { r.Loop, r.Rate = "open", 250 },
		func(r *Record) { r.Loop, r.Rate = "open", 500 },
		func(r *Record) { r.Scale = 10 },
		func(r *Record) { r.Profile = "S" },
		func(r *Record) { r.Access, r.ZipfS = "zipf", 1 },
		func(r *Record) { r.Access, r.ZipfS = "zipf", 1.2 },
		func(r *Record) { r.PageDepth = 0 },
	}
	seen := map[string]bool{}
	for _, mutate := range variants {
		rec := *base
		mutate(&rec)
		path, err := writeRecord(dir, &rec)
		if err != nil {
			t.Fatal(err)
		}
		if seen[path] {
			t.Errorf("%s was written twice: a leg overwrote another", path)
		}
		seen[path] = true
	}
}

func TestWriteRecordNamesTheFileByLevelAndRepetition(t *testing.T) {
	dir := t.TempDir()
	rec := completeRecord()
	path, err := writeRecord(dir, rec)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "fake-L-A-mix-pR-uniform-pd30-closed-s300-v4-r1.json" {
		t.Errorf("file %s", path)
	}
	content, _ := os.ReadFile(path)
	var back Record
	if err := json.Unmarshal(content, &back); err != nil || back.VUsers != 4 {
		t.Errorf("record did not round trip: %v %+v", err, back)
	}
	rec.Facts = nil
	if _, err := writeRecord(dir, rec); err == nil {
		t.Error("writeRecord must refuse an invalid record")
	}
}

func TestLoadFactsRefusesAMissingFile(t *testing.T) {
	if _, err := loadFacts(""); err == nil || !strings.Contains(err.Error(), "-facts") {
		t.Errorf("got %v", err)
	}
	path := filepath.Join(t.TempDir(), "f.json")
	writeFile(t, path, `{"kernel":"6.1"}`)
	facts, err := loadFacts(path)
	if err != nil || facts["kernel"] != "6.1" {
		t.Errorf("facts %v err %v", facts, err)
	}
}

func TestLabels(t *testing.T) {
	rec := completeRecord()
	rec.GrowthPct, rec.ClientCPUPct, rec.Warm = 12, 90, false
	rec.label()
	for _, want := range []string{"drift", "client-bound", "warm=false"} {
		found := false
		for _, l := range rec.Labels {
			found = found || l == want
		}
		if !found {
			t.Errorf("label %q missing from %v", want, rec.Labels)
		}
	}
	rec = completeRecord()
	rec.GrowthPct, rec.ClientCPUPct, rec.Warm = 9.9, 84, true
	rec.label()
	if len(rec.Labels) != 0 {
		t.Errorf("labels %v on a clean record", rec.Labels)
	}
}

func TestIdiomaticLegIsLabelledAndOnlyThatLeg(t *testing.T) {
	cases := []struct {
		batch, multiget, native string
		want                    bool
	}{
		{"on", "on", "off", true},
		{"on", "on", "", true},
		{"on", "off", "off", false},
		{"off", "on", "off", false},
		{"off", "off", "off", false},
		{"", "", "", false},
		{"on", "on", "on", false},
	}
	for _, c := range cases {
		rec := completeRecord()
		rec.Warm = true
		rec.Batch, rec.MultiGet, rec.Native = c.batch, c.multiget, c.native
		rec.label()
		if got := slices.Contains(rec.Labels, labelIdiomatic); got != c.want {
			t.Errorf("batch %q multiget %q native %q: labels %v, idiomatic %v want %v", c.batch, c.multiget, c.native, rec.Labels, got, c.want)
		}
	}
	withAggregates := completeRecord()
	withAggregates.Warm = true
	withAggregates.Batch, withAggregates.MultiGet, withAggregates.Aggregates = "on", "on", "on"
	withAggregates.label()
	if slices.Contains(withAggregates.Labels, labelIdiomatic) {
		t.Errorf("a leg that also declares aggregates is a different leg, not the idiomatic baseline: labels %v", withAggregates.Labels)
	}
	rec := completeRecord()
	rec.Batch, rec.MultiGet = "on", "on"
	if want := completeRecord().legName() + "-batched-multiget"; rec.legName() != want {
		t.Errorf("the idiomatic leg is named %q, want %q: the label must not move the file name", rec.legName(), want)
	}
}

func TestRecordAggregatesKeepsLegsApart(t *testing.T) {
	off, on := completeRecord(), completeRecord()
	off.Aggregates, on.Aggregates = "off", "on"
	if off.legName() != completeRecord().legName() {
		t.Error("an aggregates=off record keeps the leg name it always had")
	}
	if on.legName() != completeRecord().legName()+"-agg" {
		t.Errorf("an aggregates=on record is named %q, want the plain name plus -agg", on.legName())
	}
	if dir := t.TempDir(); recordPath(dir, off) == recordPath(dir, on) {
		t.Fatal("aggregates on and off records would overwrite each other")
	}
	encoded, err := json.Marshal(on)
	if err != nil || !strings.Contains(string(encoded), `"aggregates":"on"`) {
		t.Fatalf("the record must say aggregates on: %s, %v", encoded, err)
	}
	both := completeRecord()
	both.Batch, both.MultiGet, both.Aggregates = "on", "on", "on"
	if want := completeRecord().legName() + "-batched-multiget-agg"; both.legName() != want {
		t.Errorf("the leg is named %q, want %q", both.legName(), want)
	}
}

func TestRecordBatchKeepsLegsApart(t *testing.T) {
	dir := t.TempDir()
	off, on := completeRecord(), completeRecord()
	off.Batch, on.Batch = "off", "on"
	if off.legName() != completeRecord().legName() {
		t.Error("an unbatched record keeps the leg name it always had")
	}
	if off.legName() == on.legName() {
		t.Fatalf("batched and unbatched legs share the name %q", on.legName())
	}
	if recordPath(dir, off) == recordPath(dir, on) {
		t.Fatal("batched and unbatched records would overwrite each other")
	}
	encoded, err := json.Marshal(on)
	if err != nil || !strings.Contains(string(encoded), `"batch":"on"`) {
		t.Fatalf("the record must carry batch: %s %v", encoded, err)
	}
}
