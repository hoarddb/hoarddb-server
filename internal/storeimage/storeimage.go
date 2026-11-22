// Package storeimage builds the images of a store directory the vouched cut's tests open:
// what a power cut can leave, assembled from copies of the files taken at named instants
// (store-vouched-cut-build.md §2). Test use only; no production code imports it.
//
// There is deliberately no function that writes a marker from a version: the marker's bytes
// in an image are the real file's, copied at the instant named, never synthesised from a
// remembered version, and the package's shape is what enforces it.
package storeimage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Capture is the bytes of every regular file of one directory, by file name, as of the
// instant Take ran.
type Capture map[string][]byte

// Take copies every regular file of dir now; the sync marker VOUCHED is included when present.
// The MANIFEST, when present, is copied first: a table is created and synced before the
// MANIFEST names it, so every table the copied MANIFEST names is on disk when the directory
// is listed.
func Take(dir string) (Capture, error) {
	capture := Capture{}
	read := func(name string) error {
		content, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			// Removed between the listing and the read (a memtable log whose flush finished): the
			// directory no longer holds it, so neither does an image of it as of now.
			return nil
		}
		if err != nil {
			return fmt.Errorf("copying %s of %s into an image: %w", name, dir, err)
		}
		capture[name] = content
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, "MANIFEST")); err == nil {
		if err := read("MANIFEST"); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("listing %s for an image: %w", dir, err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || entry.Name() == "MANIFEST" || entry.Name() == "LOCK" {
			continue
		}
		if err := read(entry.Name()); err != nil {
			return nil, err
		}
	}
	return capture, nil
}

// Only returns the files of c whose names keep accepts.
func (c Capture) Only(keep func(name string) bool) Capture {
	kept := Capture{}
	for name, content := range c {
		if keep(name) {
			kept[name] = content
		}
	}
	return kept
}

// Write writes the files of every capture into dst, which it creates; a later capture's file
// replaces an earlier one's of the same name. dst must not exist or must be empty.
func Write(dst string, files ...Capture) error {
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return fmt.Errorf("creating the image directory %s: %w", dst, err)
	}
	merged := Capture{}
	for _, capture := range files {
		for name, content := range capture {
			merged[name] = content
		}
	}
	for name, content := range merged {
		if err := os.WriteFile(filepath.Join(dst, name), content, 0o600); err != nil {
			return fmt.Errorf("writing %s of the image %s: %w", name, dst, err)
		}
	}
	return nil
}
