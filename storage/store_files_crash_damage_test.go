package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// damagedLeftoverTables are the ways a kill during the first flush leaves the
// table it was writing: Badger's table.CreateTable leaves a zeroed tail, and a
// copy can be cut anywhere. None of them has a footer checkTableFooter has
// validated, because with a manifest that names no table it never runs.
var damagedLeftoverTables = []struct {
	name   string
	damage func(tableBytes []byte) []byte
}{
	{name: "tail zeroed", damage: func(tableBytes []byte) []byte {
		clear(tableBytes[len(tableBytes)-4096:])
		return tableBytes
	}},
	{name: "cut in half", damage: func(tableBytes []byte) []byte { return tableBytes[:len(tableBytes)/2] }},
	{name: "cut to 3 bytes", damage: func(tableBytes []byte) []byte { return tableBytes[:3] }},
	{name: "empty", damage: func(tableBytes []byte) []byte { return nil }},
	{name: "garbage footer", damage: func(tableBytes []byte) []byte {
		copy(tableBytes[len(tableBytes)-12:], bytes.Repeat([]byte{0xFF}, 12))
		return tableBytes
	}},
	{name: "small garbage footer", damage: func(tableBytes []byte) []byte {
		copy(tableBytes[len(tableBytes)-8:], []byte{0, 0, 0, 3, 0, 0, 0, 2})
		return tableBytes
	}},
	{name: "index byte flipped", damage: func(tableBytes []byte) []byte {
		tableBytes[len(tableBytes)-40] ^= 0xFF
		return tableBytes
	}},
}

// FuzzTableIndexMaxVersion checks that reading a table's highest version
// returns an error, never a panic, whatever the file holds.
func FuzzTableIndexMaxVersion(f *testing.F) {
	realTable, err := os.ReadFile(tableFiles(f, writeClosedStore(f, nil))[0])
	if err != nil {
		f.Fatalf("reading a table: %v", err)
	}
	f.Add(realTable)
	for _, damageCase := range damagedLeftoverTables {
		f.Add(damageCase.damage(append([]byte(nil), realTable...)))
	}
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 3, 0, 0, 0, 2})
	tablePath := filepath.Join(f.TempDir(), "000001.sst")
	f.Fuzz(func(t *testing.T, tableBytes []byte) {
		if err := os.WriteFile(tablePath, tableBytes, 0o600); err != nil {
			t.Fatalf("writing the table: %v", err)
		}
		_, _ = tableIndexMaxVersion(tablePath)
	})
}
