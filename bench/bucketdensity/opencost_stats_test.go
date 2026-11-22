package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurationStats(t *testing.T) {
	tests := []struct {
		name       string
		observed   []time.Duration
		wantCount  int
		wantMinMs  float64
		wantMedian float64
		wantP95    float64
		wantMaxMs  float64
	}{
		{
			name:       "empty",
			observed:   nil,
			wantCount:  0,
			wantMinMs:  0,
			wantMedian: 0,
			wantP95:    0,
			wantMaxMs:  0,
		},
		{
			name:       "single observation is every percentile",
			observed:   []time.Duration{7 * time.Millisecond},
			wantCount:  1,
			wantMinMs:  7,
			wantMedian: 7,
			wantP95:    7,
			wantMaxMs:  7,
		},
		{
			name: "unsorted input, odd count",
			observed: []time.Duration{
				30 * time.Millisecond,
				10 * time.Millisecond,
				20 * time.Millisecond,
			},
			wantCount:  3,
			wantMinMs:  10,
			wantMedian: 20,
			wantP95:    30,
			wantMaxMs:  30,
		},
		{
			// 100 observations of 1..100 ms: the 95th percentile is the
			// nearest-rank element, index ceil(0.95*100)-1 = 94, i.e. 95 ms.
			name:       "nearest-rank p95 over a hundred observations",
			observed:   rampMillis(100),
			wantCount:  100,
			wantMinMs:  1,
			wantMedian: 50,
			wantP95:    95,
			wantMaxMs:  100,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := append([]time.Duration(nil), tc.observed...)
			summary := durationStats(tc.observed)

			if summary.Count != tc.wantCount {
				t.Errorf("Count = %d, want %d", summary.Count, tc.wantCount)
			}
			if summary.MinMillis != tc.wantMinMs {
				t.Errorf("MinMillis = %v, want %v", summary.MinMillis, tc.wantMinMs)
			}
			if summary.P50Millis != tc.wantMedian {
				t.Errorf("P50Millis = %v, want %v", summary.P50Millis, tc.wantMedian)
			}
			if summary.P95Millis != tc.wantP95 {
				t.Errorf("P95Millis = %v, want %v", summary.P95Millis, tc.wantP95)
			}
			if summary.MaxMillis != tc.wantMaxMs {
				t.Errorf("MaxMillis = %v, want %v", summary.MaxMillis, tc.wantMaxMs)
			}

			// A summary that reorders its caller's slice would silently
			// corrupt the per-bucket sequence the report prints alongside it.
			for i := range original {
				if tc.observed[i] != original[i] {
					t.Fatalf("durationStats reordered its input at %d: got %v, want %v",
						i, tc.observed[i], original[i])
				}
			}
		})
	}
}

func rampMillis(n int) []time.Duration {
	observed := make([]time.Duration, 0, n)
	// Descending, so a stats function that assumes sorted input fails here.
	for i := n; i >= 1; i-- {
		observed = append(observed, time.Duration(i)*time.Millisecond)
	}
	return observed
}

func TestEvictFileFromPageCacheReportsAMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nothing-here")
	err := evictFileFromPageCache(missing)
	if err == nil {
		t.Fatal("evicting a file that does not exist returned no error")
	}
	if got := err.Error(); !strings.Contains(got, missing) {
		t.Errorf("error %q does not name the path it failed on", got)
	}
}
