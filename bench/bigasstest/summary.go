package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// loadRecords reads every *.json record of a directory. Files that are not records of this
// schema are returned in skipped, so a summary can say what it left out.
func loadRecords(dir string) (records []*Record, skipped []string, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, nil, fmt.Errorf("listing %s: %w", dir, err)
	}
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("reading %s: %w", path, err)
		}
		var rec Record
		if err := json.Unmarshal(content, &rec); err != nil || rec.Schema != recordSchema {
			skipped = append(skipped, filepath.Base(path))
			continue
		}
		records = append(records, &rec)
	}
	return records, skipped, nil
}

func median(values []float64) float64 {
	var ok []float64
	for _, v := range values {
		if !math.IsNaN(v) {
			ok = append(ok, v)
		}
	}
	if len(ok) == 0 {
		return math.NaN()
	}
	sort.Float64s(ok)
	if len(ok)%2 == 1 {
		return ok[len(ok)/2]
	}
	return (ok[len(ok)/2-1] + ok[len(ok)/2]) / 2
}

func ms(v float64) string {
	if math.IsNaN(v) {
		return "n.a."
	}
	return fmt.Sprintf("%.2f", v)
}

func pct(v float64) string {
	if math.IsNaN(v) {
		return "n.a."
	}
	return fmt.Sprintf("%.1f", v)
}

// opRows prints one row per operation of the successful repetitions: its measured samples, the
// median latencies and budget shares, and "thin" when any repetition measured under
// thinOpSamples of it. The samples shown are the fewest over the repetitions, because the
// thinnest repetition is the one that limits what the figures can say.
func opRows(good []*Record) string {
	if len(good) == 0 {
		return ""
	}
	kinds := map[string]bool{}
	for _, rec := range good {
		for kind := range rec.Ops {
			kinds[kind] = true
		}
	}
	if len(kinds) == 0 {
		return ""
	}
	names := make([]string, 0, len(kinds))
	for kind := range kinds {
		names = append(names, kind)
	}
	sort.Strings(names)
	var out strings.Builder
	fmt.Fprintf(&out, "  operations (samples: fewest over the repetitions; thin: under %d)\n", thinOpSamples)
	fmt.Fprintf(&out, "    %-22s %9s         %8s %8s %7s %7s\n", "", "", "p50", "p99", "<10ms%", "<50ms%")
	for _, kind := range names {
		fewest, thin := int64(math.MaxInt64), false
		var p50, p99, u10, u50 []float64
		for _, rec := range good {
			stats := rec.Ops[kind]
			fewest = min(fewest, stats.Count)
			thin = thin || stats.Thin || stats.Count < thinOpSamples
			p50 = append(p50, float64(stats.P50))
			p99 = append(p99, float64(stats.P99))
			u10 = append(u10, float64(stats.Under10Pct))
			u50 = append(u50, float64(stats.Under50Pct))
		}
		mark := ""
		if thin {
			mark = "  thin"
		}
		fmt.Fprintf(&out, "    %-22s %9d samples %8s %8s %7s %7s%s\n", kind, fewest, ms(median(p50)), ms(median(p99)), pct(median(u10)), pct(median(u50)), mark)
	}
	return out.String()
}

// summaryTable prints throughput and p50, p99 and p99.9 per level, per layout and overall, as
// the median of the successful repetitions with the throughput range. A repetition that failed
// its checks or was aborted is listed but never enters a median, and every label is shown.
func summaryTable(records []*Record) string {
	type group struct {
		engine, level, leg string
		vusers             int
	}
	groups := map[group][]*Record{}
	var profiled []*Record
	for _, rec := range records {
		if rec.Profiled {
			profiled = append(profiled, rec)
			continue
		}
		key := group{rec.Engine, rec.Level, rec.legName(), rec.VUsers}
		groups[key] = append(groups[key], rec)
	}
	keys := make([]group, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.engine != b.engine {
			return a.engine < b.engine
		}
		if a.level != b.level {
			return a.level < b.level
		}
		if a.leg != b.leg {
			return a.leg < b.leg
		}
		return a.vusers < b.vusers
	})
	var out strings.Builder
	lastHeader := ""
	for _, key := range keys {
		reps := groups[key]
		sort.Slice(reps, func(i, j int) bool { return reps[i].Repetition < reps[j].Repetition })
		header := key.engine + " " + key.level + ", " + key.leg
		if reps[0].Native == "on" {
			header += " (hoardDB layout: native; not comparable with another engine's common layout)"
		}
		if header != lastHeader {
			fmt.Fprintf(&out, "\n%s (median of the successful repetitions; latency in ms)\n", header)
			fmt.Fprintf(&out, "%-24s %-5s %11s %22s %8s %8s %8s %7s %7s\n", "", "reps", "req/s", "range", "p50", "p99", "p99.9", "<10ms%", "<50ms%")
			lastHeader = header
		}
		var good []*Record
		var failures []string
		labels := map[string]bool{}
		for _, rec := range reps {
			for _, label := range rec.Labels {
				labels[label] = true
			}
			if rec.Failed || rec.Aborted != "" {
				why := "failed its checks"
				if rec.Aborted != "" {
					why = "aborted: " + rec.Aborted
					labels["aborted"] = true
				}
				failures = append(failures, fmt.Sprintf("r%d %s", rec.Repetition, why))
				continue
			}
			good = append(good, rec)
		}
		names := make([]string, 0, len(labels))
		for label := range labels {
			names = append(names, label)
		}
		sort.Strings(names)
		note := fmt.Sprintf("  %d of %d repetitions ok", len(good), len(reps))
		if len(names) > 0 {
			note += "  labels: " + strings.Join(names, ", ")
		}
		fmt.Fprintf(&out, "vusers %d%s\n", key.vusers, note)
		for _, failure := range failures {
			fmt.Fprintf(&out, "  excluded: %s\n", failure)
		}
		rows := []struct {
			name string
			pick func(*Record) Summary
		}{{"overall", func(r *Record) Summary { return r.Overall }}}
		for _, layout := range benchLayouts {
			name := string(layout)
			rows = append(rows, struct {
				name string
				pick func(*Record) Summary
			}{name, func(r *Record) Summary { return r.Layouts[name] }})
		}
		for _, row := range rows {
			var throughput, p50, p99, p999, u10, u50 []float64
			for _, rec := range good {
				s := row.pick(rec)
				if row.name != "overall" && s.Requests == 0 {
					continue
				}
				throughput = append(throughput, s.Throughput)
				p50 = append(p50, float64(s.P50))
				p99 = append(p99, float64(s.P99))
				p999 = append(p999, float64(s.P999))
				u10 = append(u10, float64(s.Under10Pct))
				u50 = append(u50, float64(s.Under50Pct))
			}
			if len(throughput) == 0 {
				continue
			}
			sort.Float64s(throughput)
			fmt.Fprintf(&out, "  %-22s %-5d %11.1f %22s %8s %8s %8s %7s %7s\n", row.name, len(throughput), median(throughput),
				fmt.Sprintf("%.1f - %.1f", throughput[0], throughput[len(throughput)-1]), ms(median(p50)), ms(median(p99)), ms(median(p999)),
				pct(median(u10)), pct(median(u50)))
		}
		out.WriteString(opRows(good))
	}
	if len(profiled) > 0 {
		sort.Slice(profiled, func(i, j int) bool {
			a, b := profiled[i], profiled[j]
			if a.legName() != b.legName() {
				return a.legName() < b.legName()
			}
			return a.VUsers < b.VUsers
		})
		fmt.Fprintf(&out, "\nprofiled records (a sampler was attached; never in a median above; latency in ms)\n")
		for _, rec := range profiled {
			fmt.Fprintf(&out, "  %s %s, %s, vusers %d r%d: %.1f req/s p50 %s p99 %s p99.9 %s  failed=%v labels=%v\n", rec.Engine, rec.Level, rec.legName(), rec.VUsers, rec.Repetition,
				rec.Overall.Throughput, ms(float64(rec.Overall.P50)), ms(float64(rec.Overall.P99)), ms(float64(rec.Overall.P999)), rec.Failed, rec.Labels)
		}
	}
	return out.String()
}
