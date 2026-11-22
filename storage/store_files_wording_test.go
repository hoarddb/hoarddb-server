package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGuardRefusalsDoNotOverstateTheRisk defends the refusal wording. A refusal
// must say the store cannot be opened safely, not that it would crash the
// server (Badger sometimes returns an error instead), and a manifest Badger
// cannot replay must say nothing was changed and name the remove-node route.
func TestGuardRefusalsDoNotOverstateTheRisk(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, dir string)
		want   []string
	}{
		{
			name: "table footer cut",
			damage: func(t *testing.T, dir string) {
				if err := os.Truncate(tableFiles(t, dir)[0], 3); err != nil {
					t.Fatalf("cutting the table: %v", err)
				}
			},
			want: []string{"cannot be opened safely", "nothing in it was changed"},
		},
		{
			name: "manifest Badger cannot replay",
			damage: func(t *testing.T, dir string) {
				garbage := bytes.Repeat([]byte{0xFF}, 64)
				if err := os.WriteFile(filepath.Join(dir, "MANIFEST"), garbage, 0o600); err != nil {
					t.Fatalf("overwriting the manifest: %v", err)
				}
			},
			want: []string{"cannot be read", "nothing in it was changed", "remove-node"},
		},
	}
	for _, damageCase := range cases {
		t.Run(damageCase.name, func(t *testing.T) {
			dir := writeClosedStore(t, nil)
			damageCase.damage(t, dir)

			_, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
			if err == nil {
				t.Fatal("opening the damaged store succeeded")
			}
			for _, want := range damageCase.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "crash the server") {
				t.Errorf("the refusal claims a server crash: %v", err)
			}
		})
	}
}
