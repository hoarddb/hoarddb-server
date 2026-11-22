// bench/types.go — Benchmark result types.
package main

import "time"

// BenchmarkResult holds timing results for a benchmark run.
type BenchmarkResult struct {
	Mode        string        `json:"mode"`
	Database    string        `json:"database"`
	InsertTime  time.Duration `json:"insert_time"`
	ReadTime    time.Duration `json:"read_time"`
	TotalTime   time.Duration `json:"total_time"`
	InsertOps   float64       `json:"insert_ops_sec"`
	ReadOps     float64       `json:"read_ops_sec"`
	TotalOps    float64       `json:"total_ops_sec"`
	RecordCount int           `json:"record_count"`
	ReadCount   int           `json:"read_count"`
}
