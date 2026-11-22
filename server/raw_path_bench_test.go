package server

import (
	"fmt"
	"strings"
	"testing"
)

// The query paths step 7a moves from a map decode per visited document to
// the resolver (bson-codec-spec 7.2, 9.2: informational, not a gate). Each
// walks a 2,000-document hash bucket whose documents carry ten fields, and
// returns or counts a tenth of them, so the cost per visited document is the
// one that changes.

const rawPathBenchDocuments = 2000

func seedRawPathBench(b *testing.B) *Server {
	b.Helper()
	s := newBenchServer(b)
	createTestBucket(b, s, "bench", "docs", BucketTypeHash)
	payload := strings.Repeat("abcdefghij", 10)
	seedDocuments(b, s, "bench", "docs", rawPathBenchDocuments, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"seq": int32(i), "tenth": int32(i % 10), "label": fmt.Sprintf("k%07d", i), "payload": payload,
			"score": float64(i) / 3, "active": i%2 == 0, "city": fmt.Sprintf("c%02d", i%50),
			"address": map[string]interface{}{"street": "Main", "zip": fmt.Sprintf("%05d", i%977)},
			"tags":    []interface{}{"a", "b", fmt.Sprintf("t%d", i%7)},
		}
	})
	return s
}

// BenchmarkRawPathFilteredScan is a filtered find: one equality on a
// top-level field, and a $or of a nested path and a range.
func BenchmarkRawPathFilteredScan(b *testing.B) {
	s := seedRawPathBench(b)
	for _, shape := range []struct {
		name   string
		filter map[string]interface{}
	}{
		{"equality", map[string]interface{}{"tenth": int32(3)}},
		{"or-nested-range", map[string]interface{}{"$or": []interface{}{
			map[string]interface{}{"address.zip": "00003"},
			map[string]interface{}{"seq": map[string]interface{}{"$gte": int32(1800)}},
		}}},
	} {
		b.Run(shape.name, func(b *testing.B) {
			request := ScanRequest{Database: "bench", Bucket: "docs", Filter: shape.filter, Limit: 1000}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := s.Scan(request); err != nil {
					b.Fatalf("Scan: %v", err)
				}
			}
		})
	}
}

// BenchmarkRawPathSortedScan is a filtered find sorted by a number, one page.
func BenchmarkRawPathSortedScan(b *testing.B) {
	s := seedRawPathBench(b)
	request := ScanRequest{Database: "bench", Bucket: "docs", Filter: map[string]interface{}{"tenth": int32(3)},
		Sort: map[string]int{"score": -1}, Limit: 20}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.Scan(request); err != nil {
			b.Fatalf("Scan: %v", err)
		}
	}
}

// BenchmarkRawPathSearchFallback is a search no index serves whose filter has
// a top-level $or: the residual fallback that needed the whole document.
func BenchmarkRawPathSearchFallback(b *testing.B) {
	s := seedRawPathBench(b)
	request := SearchRequest{Database: "bench", Bucket: "docs", Filter: map[string]interface{}{"$or": []interface{}{
		map[string]interface{}{"tenth": int32(3)}, map[string]interface{}{"city": "c07"},
	}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.Search(request); err != nil {
			b.Fatalf("Search: %v", err)
		}
	}
}

// BenchmarkRawPathCountAndDistinct are count and distinct with a filter.
func BenchmarkRawPathCountAndDistinct(b *testing.B) {
	s := seedRawPathBench(b)
	filter := map[string]interface{}{"tenth": int32(3)}
	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := s.Count(CountRequest{Database: "bench", Bucket: "docs", Filter: filter}); err != nil {
				b.Fatalf("Count: %v", err)
			}
		}
	})
	b.Run("distinct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := s.Distinct(DistinctRequest{Database: "bench", Bucket: "docs", Field: "city", Filter: filter}); err != nil {
				b.Fatalf("Distinct: %v", err)
			}
		}
	})
}
