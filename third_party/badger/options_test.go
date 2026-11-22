/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4/options"
)

func TestOptions(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		// copy all the default options over to a big SuperFlag string
		defaultSuperFlag := generateSuperFlag(DefaultOptions(""))
		// fill an empty Options with values from the SuperFlag
		generated := Options{}.FromSuperFlag(defaultSuperFlag)
		// make sure they're equal
		if !optionsEqual(DefaultOptions(""), generated) {
			t.Fatal("generated default SuperFlag != default Options")
		}
		// check that values are overwritten properly
		overwritten := DefaultOptions("").FromSuperFlag("numgoroutines=1234")
		if overwritten.NumGoroutines != 1234 {
			t.Fatal("Option value not overwritten by SuperFlag value")
		}
	})

	t.Run("special flags", func(t *testing.T) {
		o1 := DefaultOptions("")
		o1.NamespaceOffset = 10
		o1.Compression = options.ZSTD
		o1.ZSTDCompressionLevel = 2
		o1.NumGoroutines = 20

		o2 := DefaultOptions("")
		o2.NamespaceOffset = 10
		o2 = o2.FromSuperFlag("compression=zstd:2; numgoroutines=20;")

		// make sure they're equal
		if !optionsEqual(o1, o2) {
			t.Fatal("generated superFlag != expected options")
		}
	})
}

// optionsEqual just compares the values of two Options structs
func optionsEqual(o1, o2 Options) bool {
	o1v := reflect.ValueOf(&o1).Elem()
	o2v := reflect.ValueOf(&o2).Elem()
	for i := 0; i < o1v.NumField(); i++ {
		if o1v.Field(i).CanInterface() {
			kind := o1v.Field(i).Kind()
			// compare values
			switch kind {
			case reflect.Bool:
				if o1v.Field(i).Bool() != o2v.Field(i).Bool() {
					return false
				}
			case reflect.Int, reflect.Int64:
				if o1v.Field(i).Int() != o2v.Field(i).Int() {
					return false
				}
			case reflect.Uint32, reflect.Uint64:
				if o1v.Field(i).Uint() != o2v.Field(i).Uint() {
					return false
				}
			case reflect.Float64:
				if o1v.Field(i).Float() != o2v.Field(i).Float() {
					return false
				}
			case reflect.String:
				if o1v.Field(i).String() != o2v.Field(i).String() {
					return false
				}
			}
		}
	}
	return true
}

// TestTableWritebackOptionsReachTheTables checks that the hoardDB table sync
// options are passed to every table the DB builds, default to upstream's
// single msync (the stepwise writeback is opt-in: it is safe only on a
// filesystem that commits its journal in order), that the stepwise mode is
// passed through when asked for, and that an unknown mode is refused at Open
// rather than at the first flush.
func TestTableWritebackOptionsReachTheTables(t *testing.T) {
	defaults := DefaultOptions("")
	if defaults.TableSyncMode != options.TableSyncMsync || defaults.TableSyncStepSize != 4<<20 {
		t.Fatalf("defaults: mode %d step %d, want msync and 4 MiB", defaults.TableSyncMode, defaults.TableSyncStepSize)
	}
	if (Options{}).TableSyncMode != options.TableSyncMsync {
		t.Fatalf("the zero Options has mode %d, want msync: an Options built by hand must not opt in to stepwise", Options{}.TableSyncMode)
	}

	cases := []struct {
		name     string
		opts     func(Options) Options
		wantMode options.TableSyncMode
		wantStep int
	}{
		{"default", func(o Options) Options { return o }, options.TableSyncMsync, 4 << 20},
		{"stepwise, 1 MiB", func(o Options) Options {
			return o.WithTableSyncMode(options.TableSyncStepwise).WithTableSyncStepSize(1 << 20)
		}, options.TableSyncStepwise, 1 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(tc.opts(getTestOptions(t.TempDir())))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			tableOpts := buildTableOptions(db)
			if tableOpts.SyncMode != tc.wantMode || tableOpts.SyncStepSize != tc.wantStep {
				t.Errorf("table options: mode %d step %d, want mode %d step %d",
					tableOpts.SyncMode, tableOpts.SyncStepSize, tc.wantMode, tc.wantStep)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}

	_, err := Open(getTestOptions(t.TempDir()).WithTableSyncMode(options.TableSyncMode(7)))
	if err == nil || !strings.Contains(err.Error(), "unknown TableSyncMode 7") {
		t.Fatalf("Open with an unknown mode: %v, want it refused naming the mode", err)
	}
}
