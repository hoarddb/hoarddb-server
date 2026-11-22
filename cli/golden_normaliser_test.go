package cli

import "testing"

// TestNormaliserKeepsTheHostDiskState is BUG-0483's other half: the disk row's
// state is pinned by the server probe seam (PinDiskUsageForTesting), not read
// from the test host, so the normaliser must NOT hide the word. Only the ratio
// stays hidden; a change to the word itself must show up in the golden.
func TestNormaliserKeepsTheHostDiskState(t *testing.T) {
	cases := []struct {
		name string
		row  string
		want string
	}{
		{"ok stays", "  disk        0.01  threshold 0.85  ok", "  disk        <RATIO>  threshold 0.85  ok"},
		{"approaching stays", "  disk        0.82  threshold 0.85  approaching", "  disk        <RATIO>  threshold 0.85  approaching"},
		{"red stays", "  disk        0.93  threshold 0.85  red", "  disk        <RATIO>  threshold 0.85  red"},
		// The ratio is host state too; it stays hidden, one rule only.
		{"ratio stays hidden", "  fds         0.01  threshold 0.80  ok", "  fds         <RATIO>  threshold 0.80  ok"},
	}
	norm := &normaliser{dataDir: "/nonexistent-data", outDir: "/nonexistent-out", addr: "127.0.0.1:1"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := norm.apply(tc.row); got != tc.want {
				t.Errorf("normalised row:\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}
