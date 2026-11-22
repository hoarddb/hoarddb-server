package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func recordAt(vusers, rep int, throughput, p50 float64, failed bool) *Record {
	rec := completeRecord()
	rec.VUsers, rec.Repetition, rec.Failed = vusers, rep, failed
	rec.Overall = Summary{Requests: 100, Throughput: throughput, latencySummary: latencySummary{P50: latencyMs(p50), P99: latencyMs(p50 * 4), P999: latencyMs(p50 * 9)}}
	rec.Layouts = map[string]Summary{"social": rec.Overall, "chat": rec.Overall}
	return rec
}

func TestSummaryShowsEveryLevelLayoutAndOverall(t *testing.T) {
	var recs []*Record
	for rep, tp := range []float64{100, 300, 200} {
		recs = append(recs, recordAt(1, rep+1, tp, 2, false), recordAt(16, rep+1, tp*5, 3, rep == 2))
	}
	text := summaryTable(recs)
	for _, want := range []string{"fake", "L-A", "vusers 1", "vusers 16", "overall", "social", "chat", "200.0", "1000.0", "2 of 3", "excluded: r3"} {
		if !strings.Contains(text, want) {
			t.Errorf("summary lacks %q:\n%s", want, text)
		}
	}
	if strings.Index(text, "vusers 1 ") > strings.Index(text, "vusers 16") {
		t.Errorf("levels must be in ascending order:\n%s", text)
	}
}

func TestSummaryKeepsLegsApartAndExcludesFailedRepetitions(t *testing.T) {
	mix1, mix2 := recordAt(4, 1, 100, 2, false), recordAt(4, 2, 300, 2, false)
	aborted := recordAt(4, 3, 9999, 1, true)
	aborted.Aborted, aborted.Labels = "disk_guard", []string{"warm=false", "drift"}
	scan := recordAt(4, 1, 777, 5, false)
	scan.Op = "social.follow_scan"
	open := recordAt(4, 1, 555, 5, false)
	open.Loop, open.Rate = "open", 250
	small := recordAt(4, 1, 321, 5, false)
	small.Scale = 10
	text := summaryTable([]*Record{mix1, mix2, aborted, scan, open, small})
	for _, want := range []string{"social.follow_scan", "open", "250", "s10", "disk_guard", "warm=false", "drift", "2 of 3"} {
		if !strings.Contains(text, want) {
			t.Errorf("summary lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "9999") {
		t.Errorf("an aborted repetition entered the figures:\n%s", text)
	}
	if !strings.Contains(text, "200.0") {
		t.Errorf("the median of the two good repetitions (200) is missing:\n%s", text)
	}
	for _, folded := range []string{"777", "555", "321"} {
		if !strings.Contains(text, folded) {
			t.Errorf("leg %s was folded into another median:\n%s", folded, text)
		}
	}
}

func TestLoadRecordsReportsSkippedFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := writeRecord(dir, completeRecord()); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, "junk.json"), "{not json")
	writeFile(t, filepath.Join(dir, "other.json"), `{"schema":"other/1"}`)
	records, skipped, err := loadRecords(dir)
	if err != nil || len(records) != 1 || len(skipped) != 2 {
		t.Errorf("records %d skipped %v err %v", len(records), skipped, err)
	}
}

func recordWithOps(counts map[string]int64) *Record {
	rec := recordAt(4, 1, 100, 2, false)
	rec.Overall.Under10Pct, rec.Overall.Under50Pct = 91.5, 99.25
	rec.Layouts["social"] = Summary{Requests: 50, Throughput: 50, latencySummary: latencySummary{P50: 1, Under10Pct: 88, Under50Pct: 97}}
	rec.Ops = map[string]OpStats{}
	for kind, n := range counts {
		rec.Ops[kind] = OpStats{Count: n, Thin: n < thinOpSamples, latencySummary: latencySummary{Count: n, P50: 1.5, P99: 7, Under10Pct: 75.5, Under50Pct: 96}}
	}
	return rec
}

func TestSummaryPrintsLatencyBudgetAndPerOperationSamples(t *testing.T) {
	var recs []*Record
	for rep, small := range []int64{400, 900, 1200} {
		rec := recordWithOps(map[string]int64{"social.feed": 50_000, "chat.user_rooms": small})
		rec.Repetition = rep + 1
		recs = append(recs, rec)
	}
	text := summaryTable(recs)
	for _, want := range []string{"<10ms", "<50ms", "91.5", "99.2", "88.0", "social.feed", "chat.user_rooms", "50000", "75.5", "96.0"} {
		if !strings.Contains(text, want) {
			t.Errorf("summary lacks %q:\n%s", want, text)
		}
	}
	var feedLine, roomsLine string
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.Contains(line, "social.feed"):
			feedLine = line
		case strings.Contains(line, "chat.user_rooms"):
			roomsLine = line
		}
	}
	if strings.Contains(feedLine, "thin") {
		t.Errorf("an operation with 50000 samples is not thin: %q", feedLine)
	}
	// The count shown and judged is the fewest over the repetitions: 400 here.
	if !strings.Contains(roomsLine, "400") || !strings.Contains(roomsLine, "thin") {
		t.Errorf("an operation with 400 samples in one repetition must show 400 and be marked thin: %q", roomsLine)
	}
}

func TestRecordMarksThinOperationsAndLabelsThem(t *testing.T) {
	rec := completeRecord()
	rec.Ops = map[string]OpStats{"a.thin": {Count: 999, Thin: true}, "a.ok": {Count: 1000}}
	rec.label()
	found := false
	for _, l := range rec.Labels {
		found = found || l == "thin-ops"
	}
	if !found {
		t.Errorf("a record with a thin operation must carry the thin-ops label, got %v", rec.Labels)
	}
	if thinOpSamples != 1000 {
		t.Errorf("an operation under about 1,000 samples is thin, the threshold is %d", thinOpSamples)
	}
}

func TestSummaryListsProfiledRecordsSeparately(t *testing.T) {
	good := recordAt(64, 1, 1000, 2, false)
	profiled := recordAt(64, 1, 4242, 2, false)
	profiled.Profiled = true
	text := summaryTable([]*Record{good, profiled})
	head, tail, found := strings.Cut(text, "profiled")
	if !found {
		t.Fatalf("the profiled record is not listed:\n%s", text)
	}
	if strings.Contains(head, "4242") || !strings.Contains(tail, "4242") {
		t.Errorf("the profiled record must appear only in its own section:\n%s", text)
	}
	if !strings.Contains(head, "1 of 1 repetitions ok") || !strings.Contains(head, "1000.0") {
		t.Errorf("the measured record is missing from the median section:\n%s", text)
	}
	only := summaryTable([]*Record{profiled})
	if strings.Contains(only, "repetitions ok") {
		t.Errorf("a profiled record formed a median group:\n%s", only)
	}
}

func TestProfiledRecordsHaveTheirOwnFileName(t *testing.T) {
	plain, profiled := completeRecord(), completeRecord()
	profiled.Profiled = true
	if plain.legName() == profiled.legName() {
		t.Fatal("a profiled record shares a leg name with a measured one")
	}
	encoded, err := json.Marshal(profiled)
	if err != nil || !strings.Contains(string(encoded), `"profiled":true`) {
		t.Fatalf("the record must say profiled: %s %v", encoded, err)
	}
}
