package main

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// ranRecord is a ring record whose run drew every operation once and audited the maintained sums.
func ranRecord() *Record {
	rec := completeRecord()
	rec.Engine = "ring"
	rec.Reads, rec.Writes = 10, 10
	rec.Ops = map[string]OpStats{string(ReadSocialFeed): {Count: 5}, string(ReadChatRoomPage): {Count: 5}, string(WriteAgentTask): {Count: 3}}
	rec.MaintainedSumsAudited = 4
	return rec
}

func TestCapabilitiesFollowWhatRan(t *testing.T) {
	base := []string{capEntityBuckets, capStoreFields, capClaimAck}
	cases := []struct {
		name   string
		mutate func(*Record)
		want   []string
	}{
		{"ring, every switch off", func(r *Record) {}, base},
		{"ring batched", func(r *Record) { r.Batch = "on" }, append(slices.Clone(base), capBatchFrame)},
		{"ring batched but nothing was written", func(r *Record) {
			r.Batch, r.Writes = "on", 0
			r.Ops = map[string]OpStats{string(ReadSocialFeed): {Count: 5}}
		}, []string{capEntityBuckets, capStoreFields}},
		{"ring idiomatic", func(r *Record) { r.Batch, r.MultiGet = "on", "on" }, append(slices.Clone(base), capBatchFrame, capGetMany)},
		{"multiget on but no page was read", func(r *Record) {
			r.MultiGet = "on"
			r.Ops = map[string]OpStats{string(WriteAgentTask): {Count: 3}}
		}, base},
		{"multiget and native both on: the native path reads no get-many", func(r *Record) { r.MultiGet, r.Native = "on", "on" }, append(slices.Clone(base), capNativeLayout)},
		{"ring native", func(r *Record) { r.Native = "on" }, append(slices.Clone(base), capNativeLayout)},
		{"ring aggregates audited", func(r *Record) { r.Aggregates = "on" }, append(slices.Clone(base), capDeclaredAggregates)},
		{"ring aggregates, nothing audited", func(r *Record) { r.Aggregates, r.MaintainedSumsAudited = "on", 0 }, base},
		{"ring, no task ran", func(r *Record) { r.Ops = map[string]OpStats{string(ReadSocialFeed): {Count: 5}} }, []string{capEntityBuckets, capStoreFields}},
		{"ring, nothing ran", func(r *Record) { r.Reads, r.Writes, r.Ops = 0, 0, nil }, []string{}},
		{"ring everything (native reads no get-many)", func(r *Record) { r.Batch, r.MultiGet, r.Native, r.Aggregates = "on", "on", "on", "on" },
			append(slices.Clone(base), capBatchFrame, capNativeLayout, capDeclaredAggregates)},
		{"mongo", func(r *Record) { r.Engine = "mongo" }, []string{}},
		{"fake", func(r *Record) { r.Engine = "fake" }, []string{}},
	}
	for _, c := range cases {
		rec := ranRecord()
		c.mutate(rec)
		got := capabilitiesOf(rec)
		if got == nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: capabilities %v, want %v", c.name, got, c.want)
		}
	}
}

func TestEveryCapabilityIsDescribed(t *testing.T) {
	for _, name := range []string{capEntityBuckets, capStoreFields, capClaimAck, capBatchFrame, capGetMany, capNativeLayout, capDeclaredAggregates} {
		if strings.TrimSpace(capabilityDescriptions[name]) == "" {
			t.Errorf("capability %q has no description", name)
		}
	}
	for name := range capabilityDescriptions {
		if !strings.Contains(name, "-") && name != "claim-ack" {
			t.Errorf("capability name %q is not a hyphenated word", name)
		}
	}
}

// The record states its coverage: a record without the list is refused, and the list is written
// even when it is empty, so "none" and "not declared" are told apart.
func TestRecordRefusesNoCapabilitiesAndWritesAnEmptyList(t *testing.T) {
	rec := completeRecord()
	rec.Capabilities = nil
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "capabilities") {
		t.Errorf("a record with no capabilities list must be refused naming it, got %v", err)
	}
	rec = completeRecord()
	rec.Capabilities = []string{}
	if err := rec.validate(); err != nil {
		t.Errorf("an empty list is a declaration: %v", err)
	}
	encoded, err := json.Marshal(rec)
	if err != nil || !strings.Contains(string(encoded), `"capabilities":[]`) {
		t.Errorf("the empty list must be written, got %s, %v", encoded, err)
	}
	rec.Capabilities = []string{"no-such-thing"}
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "no-such-thing") {
		t.Errorf("an unknown capability must be refused naming it, got %v", err)
	}
}

func TestRunnerDeclaresTheCapabilitiesOfTheLeg(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.aggregates = true })
	p.EngineName = "ring"
	rec, err := runRepetition(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.MaintainedSumsAudited == 0 {
		t.Errorf("an aggregates run that writes expenses must record the sums it audited")
	}
	if !slices.Contains(rec.Capabilities, capDeclaredAggregates) || !slices.Contains(rec.Capabilities, capClaimAck) {
		t.Errorf("a ring record with aggregates on declares %v", rec.Capabilities)
	}
	p2, _ := fakeRun(t, nil)
	rec2, err := runRepetition(t.Context(), p2)
	if err != nil {
		t.Fatal(err)
	}
	if rec2.Capabilities == nil || len(rec2.Capabilities) != 0 {
		t.Errorf("a record of another engine declares an empty list, got %v", rec2.Capabilities)
	}
}

func TestPlanCheckPrintsEveryLayoutAndTheTotal(t *testing.T) {
	var out strings.Builder
	if err := planCheckMain(&out, 1); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"layout social", "layout chat", "layout ledger", "layout agent", "all layouts", "User.Feed", "Account.Expenses", "User.Memories"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the plan-check output lacks %q:\n%s", want, out.String())
		}
	}
}
