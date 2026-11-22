package config

import "testing"

// TestUnsafeAckBeforeSyncIsOffUnlessSet checks the knob: a node syncs
// before it answers unless the operator turns that off, in the file or with
// the variable.
func TestUnsafeAckBeforeSyncIsOffUnlessSet(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		env  string
		want bool
	}{
		{name: "default", want: false},
		{name: "file", yaml: "storage:\n  unsafe_ack_before_sync: true\n", want: true},
		{name: "variable", env: "true", want: true},
		{name: "variable over file", yaml: "storage:\n  unsafe_ack_before_sync: true\n", env: "false", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_UNSAFE_ACK_BEFORE_SYNC", tc.env)
			path := ""
			if tc.yaml != "" {
				path = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Storage.UnsafeAckBeforeSync != tc.want {
				t.Fatalf("unsafe_ack_before_sync = %v, want %v", cfg.Storage.UnsafeAckBeforeSync, tc.want)
			}
		})
	}
}
