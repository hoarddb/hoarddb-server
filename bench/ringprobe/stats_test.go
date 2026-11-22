package ringprobe

import "testing"

func TestSummarize(t *testing.T) {
	tests := []struct {
		name       string
		samples    []float64
		wantMedian float64
		wantP90    float64
		wantMax    float64
	}{
		{
			name:       "single sample is every statistic",
			samples:    []float64{0.07},
			wantMedian: 0.07,
			wantP90:    0.07,
			wantMax:    0.07,
		},
		{
			name:       "unsorted input, odd count",
			samples:    []float64{0.30, 0.10, 0.20},
			wantMedian: 0.20,
			wantP90:    0.30,
			wantMax:    0.30,
		},
		{
			// Ten evenly spaced samples: nearest-rank p90 is the 9th of ten
			// (index 8), i.e. 0.09; the median is the average-free
			// nearest-rank pick at index 4, i.e. 0.05.
			name:       "ten evenly spaced samples",
			samples:    []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10},
			wantMedian: 0.05,
			wantP90:    0.09,
			wantMax:    0.10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := append([]float64(nil), tt.samples...)
			got := summarize(tt.samples)

			if got.count != len(tt.samples) {
				t.Errorf("count = %d, want %d", got.count, len(tt.samples))
			}
			if got.median != tt.wantMedian {
				t.Errorf("median = %v, want %v", got.median, tt.wantMedian)
			}
			if got.p90 != tt.wantP90 {
				t.Errorf("p90 = %v, want %v", got.p90, tt.wantP90)
			}
			if got.max != tt.wantMax {
				t.Errorf("max = %v, want %v", got.max, tt.wantMax)
			}
			for i, v := range input {
				if v != tt.samples[i] {
					t.Errorf("summarize mutated its input at index %d: got %v, want %v", i, tt.samples[i], v)
				}
			}
		})
	}
}

func TestSummarizeEmpty(t *testing.T) {
	got := summarize(nil)
	want := sampleStats{}
	if got != want {
		t.Errorf("summarize(nil) = %+v, want zero value %+v", got, want)
	}
}

func TestMinMax(t *testing.T) {
	tests := []struct {
		name    string
		samples []float64
		wantMin float64
		wantMax float64
	}{
		{name: "single value", samples: []float64{0.42}, wantMin: 0.42, wantMax: 0.42},
		{name: "min first", samples: []float64{0.1, 0.9, 0.5}, wantMin: 0.1, wantMax: 0.9},
		{name: "min last", samples: []float64{0.9, 0.5, 0.1}, wantMin: 0.1, wantMax: 0.9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max := minMax(tt.samples)
			if min != tt.wantMin || max != tt.wantMax {
				t.Errorf("minMax(%v) = (%v, %v), want (%v, %v)", tt.samples, min, max, tt.wantMin, tt.wantMax)
			}
		})
	}
}
