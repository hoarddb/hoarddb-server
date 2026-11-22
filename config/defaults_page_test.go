package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
)

// defaultsPageRow is one table row of the defaults page of the user docs: the variable and the text of its
// default cell.
type defaultsPageRow struct{ variable, defaultCell string }

func readDefaultsPage(t *testing.T) map[string]defaultsPageRow {
	t.Helper()
	path, err := repofile.Find("docs/user/defaults.md")
	if err != nil {
		t.Fatalf("locating the defaults page: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	rows := map[string]defaultsPageRow{}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "| `HOARDDB_") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "| "), "|")
		if len(cells) < 3 {
			continue
		}
		name := strings.Trim(strings.TrimSpace(cells[0]), "`")
		rows[name] = defaultsPageRow{variable: name, defaultCell: strings.TrimSpace(cells[1])}
	}
	return rows
}

// envDefaults walks DefaultConfig and returns every field that has an environment variable,
// with the default it holds.
func envDefaults(value reflect.Value, ty reflect.Type, into map[string]reflect.Value) {
	for i := 0; i < ty.NumField(); i++ {
		field := ty.Field(i)
		if field.Type.Kind() == reflect.Struct {
			envDefaults(value.Field(i), field.Type, into)
			continue
		}
		if env := field.Tag.Get("env"); env != "" && env != "-" {
			into[env] = value.Field(i)
		}
	}
}

// TestDefaultsPageListsEverySetting keeps the defaults page of the user docs honest: every setting that has
// an environment variable has a row, and a setting whose default is a plain value in
// DefaultConfig shows that value. A default that depends on the machine or on another setting
// (a zero or empty value here) is written in words on the page, and only its row is required.
func TestDefaultsPageListsEverySetting(t *testing.T) {
	cfg := DefaultConfig()
	defaults := map[string]reflect.Value{}
	envDefaults(reflect.ValueOf(cfg).Elem(), reflect.TypeOf(*cfg), defaults)
	page := readDefaultsPage(t)

	// The default of these is computed from the machine at load time.
	machineDependent := map[string]bool{"HOARDDB_AUTH_MAX_DERIVATIONS": true}

	for env, value := range defaults {
		row, ok := page[env]
		if !ok {
			t.Errorf("docs/user/defaults.md has no row for %s", env)
			continue
		}
		if machineDependent[env] || value.IsZero() || value.Kind() == reflect.Slice || value.Kind() == reflect.Pointer {
			continue
		}
		want := fmt.Sprint(value.Interface())
		if !strings.Contains(row.defaultCell, want) {
			t.Errorf("%s: the page shows %q, and DefaultConfig holds %s", env, row.defaultCell, want)
		}
	}
	for env := range page {
		if _, ok := defaults[env]; !ok && !strings.HasPrefix(env, "HOARDDB_ROOT_PASSWORD") && env != "HOARDDB_ROOT_USER" {
			t.Errorf("docs/user/defaults.md lists %s, which no configuration field reads", env)
		}
	}
	if len(page) < 100 {
		t.Fatalf("the page lists only %d settings: the scan stopped matching", len(page))
	}
}
