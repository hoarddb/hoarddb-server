package config

import (
	"strings"
	"testing"
)

// TestWALPreallocateKey checks storage.wal_preallocate: "auto" is the default,
// "on" and "auto" come from the file or from HOARDDB_WAL_PREALLOCATE, the
// variable wins over the file, and every other spelling stops the start with
// the refusal that names the key, the variable, the three values and the fix.
func TestWALPreallocateKey(t *testing.T) {
	if got := DefaultConfig().Storage.WALPreallocate; got != WALPreallocateAuto {
		t.Fatalf("DefaultConfig wal_preallocate = %q, want %q", got, WALPreallocateAuto)
	}
	accepted := []struct {
		name string
		yaml string
		env  string
		want string
	}{
		{name: "no file, no variable", want: "auto"},
		{name: "file without the key", yaml: "storage:\n  data_dir: ./data\n", want: "auto"},
		{name: "file on", yaml: "storage:\n  wal_preallocate: \"on\"\n", want: "on"},
		{name: "file auto", yaml: "storage:\n  wal_preallocate: auto\n", want: "auto"},
		{name: "file off", yaml: "storage:\n  wal_preallocate: \"off\"\n", want: "off"},
		{name: "variable on", env: "on", want: "on"},
		{name: "variable auto", env: "auto", want: "auto"},
		{name: "variable over file", yaml: "storage:\n  wal_preallocate: \"on\"\n", env: "off", want: "off"},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_WAL_PREALLOCATE", tc.env)
			path := ""
			if tc.yaml != "" {
				path = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Storage.WALPreallocate != tc.want {
				t.Fatalf("wal_preallocate = %q, want %q", cfg.Storage.WALPreallocate, tc.want)
			}
		})
	}

	refused := []struct{ name, yaml, shown string }{
		{name: "true", yaml: "true", shown: "true"},
		{name: "upper case", yaml: "ON", shown: "ON"},
		{name: "empty but set", yaml: `""`, shown: ""},
		{name: "yes", yaml: "yes", shown: "yes"},
		{name: "something else", yaml: "fsync", shown: "fsync"},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_WAL_PREALLOCATE", "")
			_, err := Load(writeYAML(t, "storage:\n  wal_preallocate: "+tc.yaml+"\n"))
			if err == nil {
				t.Fatalf("Load with wal_preallocate: %s succeeded, want it refused", tc.yaml)
			}
			want := `storage.wal_preallocate (HOARDDB_WAL_PREALLOCATE) is "` + tc.shown + `", but it must be "off", "on" or "auto"` + "\n" +
				`       remove it to use the default, "auto"; it is not a switch: "on" preallocates on every filesystem, "auto" on ext2/3/4 only`
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal %q does not contain the spec text %q", err, want)
			}
		})
	}

	t.Run("variable refuses true", func(t *testing.T) {
		t.Setenv("HOARDDB_WAL_PREALLOCATE", "true")
		if _, err := Load(""); err == nil {
			t.Fatal("Load with HOARDDB_WAL_PREALLOCATE=true succeeded, want it refused")
		}
	})
}

// TestWALPreallocateSetTellsAnExplicitOffFromTheDefault: the startup line says
// "the default" only when nobody set the key, so Load records whether the file
// or the variable did, an explicit off included.
func TestWALPreallocateSetTellsAnExplicitOffFromTheDefault(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml string
		env  string
		want bool
	}{
		{name: "nothing set", want: false},
		{name: "file without the key", yaml: "storage:\n  data_dir: ./data\n", want: false},
		{name: "file off", yaml: "storage:\n  wal_preallocate: \"off\"\n", want: true},
		{name: "file on", yaml: "storage:\n  wal_preallocate: \"on\"\n", want: true},
		{name: "variable off", env: "off", want: true},
		{name: "empty variable", env: "", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_WAL_PREALLOCATE", tc.env)
			path := ""
			if tc.yaml != "" {
				path = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Storage.WALPreallocateSet != tc.want {
				t.Fatalf("WALPreallocateSet = %v, want %v", cfg.Storage.WALPreallocateSet, tc.want)
			}
		})
	}
}
