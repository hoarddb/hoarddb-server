package cli

import (
	"strings"
	"testing"
)

func TestIntoSplitsAtFirstDot(t *testing.T) {
	tests := []struct {
		into         string
		wantDatabase string
		wantBucket   string
		wantErr      string
	}{
		{"app.users", "app", "users", ""},
		{"app.orders.2024", "app", "orders.2024", ""},
		{"app", "", "", "--into app.users"},
		{"app.", "", "", "--into app.users"},
		{".users", "", "", "--into app.users"},
		{"", "", "", "--into app.users"},
		{"a/b.users", "", "", "path separator"},
	}
	for _, tc := range tests {
		database, bucket, err := splitInto(tc.into)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("splitInto(%q) error = %v, want it to contain %q", tc.into, err, tc.wantErr)
			}
			continue
		}
		if err != nil || database != tc.wantDatabase || bucket != tc.wantBucket {
			t.Errorf("splitInto(%q) = (%q, %q, %v), want (%q, %q)", tc.into, database, bucket, err, tc.wantDatabase, tc.wantBucket)
		}
	}
}
