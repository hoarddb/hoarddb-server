// Command bsoncount prints how many documents each .bson file holds, one "name<TAB>count" line per
// file, sorted by name. It reads only the 4-byte length that opens every document, so it never
// decodes (and never prints) a document's content. The performance certification uses it to compare
// a dump's per-collection counts with what "hoarddb ingest" loaded.
//
//	bsoncount DIRECTORY_OR_FILE...
//
// A directory contributes every *.bson file directly inside it (not subdirectories); the name is the
// file name without ".bson", the bucket name "hoarddb ingest" gives it. A file that is cut short or
// whose length prefix is impossible is an error naming the file and the byte offset.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// minDocumentBytes is the smallest valid BSON document: a 4-byte length and the 0x00 terminator.
const minDocumentBytes = 5

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bsoncount DIRECTORY_OR_FILE...")
		os.Exit(2)
	}
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "bsoncount: %v\n", err)
		os.Exit(1)
	}
}

// run counts the documents of every .bson file named by paths and writes one line per file to out.
func run(paths []string, out io.Writer) error {
	var files []string
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("reading %s: %w", path, err)
		}
		if !info.IsDir() {
			files = append(files, path)
			continue
		}
		matches, err := filepath.Glob(filepath.Join(path, "*.bson"))
		if err != nil {
			return fmt.Errorf("listing %s: %w", path, err)
		}
		files = append(files, matches...)
	}
	sort.Slice(files, func(i, j int) bool { return bucketName(files[i]) < bucketName(files[j]) })
	for _, file := range files {
		count, err := countDocuments(file)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "%s\t%d\n", bucketName(file), count); err != nil {
			return fmt.Errorf("writing the count of %s: %w", file, err)
		}
	}
	return nil
}

func bucketName(path string) string {
	return strings.TrimSuffix(filepath.Base(path), ".bson")
}

// countDocuments walks the length prefixes of one file and returns the number of documents.
func countDocuments(path string) (count int, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			count, err = 0, fmt.Errorf("closing %s: %w", path, closeErr)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("reading the size of %s: %w", path, err)
	}
	size := info.Size()
	var offset int64
	prefix := make([]byte, 4)
	for offset < size {
		if _, err := file.ReadAt(prefix, offset); err != nil {
			return 0, fmt.Errorf("%s: no length prefix at offset %d: %w", filepath.Base(path), offset, err)
		}
		length := int64(binary.LittleEndian.Uint32(prefix))
		if length < minDocumentBytes || offset+length > size {
			return 0, fmt.Errorf("%s: the document at offset %d claims %d bytes, but the file has %d left",
				filepath.Base(path), offset, length, size-offset)
		}
		offset += length
		count++
	}
	return count, nil
}
