// bench/rank_summary.go — folds the JSON lines of the rank legs into the
// markdown table the comparison report quotes: per scale, one row per
// (store, op), one column per engine, the median of the repetitions.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// rankEngineOrder fixes the column order; an engine not listed is appended.
var rankEngineOrder = []string{"hoardDB", "MongoDB", "MongoDB j:true", "MySQL no-fsync", "MySQL", "PostgreSQL no-fsync", "PostgreSQL", "CockroachDB no-fsync", "CockroachDB"}

type rankCellKey struct {
	docs      int
	engine    string
	store, op string
}

type rankCell struct {
	opsPerSec, p50, p99 []float64
	failed              int
}

func summarizeRank(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	cells := map[rankCellKey]*rankCell{}
	scales := map[int]bool{}
	engines := map[string]bool{}
	lines := bufio.NewScanner(file)
	for lineNo := 1; lines.Scan(); lineNo++ {
		var sample rankSample
		if err := json.Unmarshal(lines.Bytes(), &sample); err != nil {
			return fmt.Errorf("%s line %d: %w", path, lineNo, err)
		}
		key := rankCellKey{docs: sample.Docs, engine: sample.Engine, store: sample.Store, op: sample.Op}
		cell := cells[key]
		if cell == nil {
			cell = &rankCell{}
			cells[key] = cell
		}
		if !sample.Verified {
			cell.failed++
			continue
		}
		cell.opsPerSec = append(cell.opsPerSec, sample.OpsPerSec)
		cell.p50 = append(cell.p50, sample.P50Micros)
		cell.p99 = append(cell.p99, sample.P99Micros)
		scales[sample.Docs] = true
		engines[sample.Engine] = true
	}
	if err := lines.Err(); err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	columns := rankColumns(engines)
	sortedScales := make([]int, 0, len(scales))
	for docs := range scales {
		sortedScales = append(sortedScales, docs)
	}
	sort.Ints(sortedScales)

	rows := [][2]string{{"hash", "insert"}, {"hash", "point-read"}, {"btree", "insert"}, {"btree", "point-read"}, {"btree", "range"}}
	for _, docs := range sortedScales {
		fmt.Printf("\n#### %d docs per bucket — ops/s · p50 / p99 µs (median of repetitions)\n\n", docs)
		fmt.Printf("| store / op | %s |\n", strings.Join(columns, " | "))
		fmt.Printf("|---|%s\n", strings.Repeat("---|", len(columns)))
		for _, row := range rows {
			rendered := make([]string, 0, len(columns))
			for _, engine := range columns {
				rendered = append(rendered, rankRenderCell(cells[rankCellKey{docs: docs, engine: engine, store: row[0], op: row[1]}]))
			}
			fmt.Printf("| %s %s | %s |\n", row[0], row[1], strings.Join(rendered, " | "))
		}
	}
	return nil
}

func rankColumns(present map[string]bool) []string {
	var columns []string
	for _, engine := range rankEngineOrder {
		if present[engine] {
			columns = append(columns, engine)
			delete(present, engine)
		}
	}
	var others []string
	for engine := range present {
		others = append(others, engine)
	}
	sort.Strings(others)
	return append(columns, others...)
}

func rankRenderCell(cell *rankCell) string {
	if cell == nil {
		return "—"
	}
	if cell.failed > 0 {
		return fmt.Sprintf("**FAILED verification ×%d**", cell.failed)
	}
	return fmt.Sprintf("%s · %.0f / %.0f (n=%d)", rankThousands(rankMedian(cell.opsPerSec)), rankMedian(cell.p50), rankMedian(cell.p99), len(cell.p50))
}

func rankMedian(samples []float64) float64 {
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func rankThousands(opsPerSec float64) string {
	digits := fmt.Sprintf("%.0f", opsPerSec)
	var grouped strings.Builder
	for i, digit := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			grouped.WriteByte(',')
		}
		grouped.WriteRune(digit)
	}
	return grouped.String()
}
