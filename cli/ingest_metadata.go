package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// A directory ingest reads <stem>.metadata.json beside each <stem>.bson and
// REPORTS its index definitions; it never creates one (ingest-spec.md §4.5). A
// foreign definition may carry sparse, partial-filter, collation, text or
// geospatial parts, and translating a subset would make a bucket whose unique
// rule differs from the source's while reporting success. An index can only be
// declared when a bucket is created, so the report comes before any create and
// names the line that declares what hoardDB can declare.

// maxMetadataBytes bounds a metadata file. A real one is a few kilobytes; a
// larger file is not one.
const maxMetadataBytes = 4 << 20

// foreignKey is one component of a source index's key.
type foreignKey struct {
	Field string
	// Kind is "asc", "desc", or the source's own word ("2dsphere", "text",
	// "hashed", "2d").
	Kind string
}

// foreignIndex is one index definition found in a metadata file.
type foreignIndex struct {
	Name      string
	Keys      []foreignKey
	Unique    bool
	Sparse    bool
	Partial   bool
	Collation bool
	Expires   bool
	// Other names the options hoardDB has no equivalent for (weights, hidden,
	// wildcardProjection and the like).
	Other []string
}

// foreignMetadata is what a directory ingest takes from one metadata file.
type foreignMetadata struct {
	File    string
	Indexes []foreignIndex
	// Type is the collection type when the file names one other than a plain
	// collection.
	Type     string
	Warnings []string
	// HasUUID is whether the file carried a uuid, which is ignored and
	// reported once for the run.
	HasUUID bool
}

// readMetadataFor reads <stem>.metadata.json beside a .bson file. found is
// false when there is none.
func readMetadataFor(directory string, file directoryFile) (metadata *foreignMetadata, found bool, err error) {
	path := filepath.Join(directory, file.Bucket+".metadata.json")
	info, statErr := os.Stat(path)
	if statErr != nil {
		return nil, false, nil
	}
	name := filepath.Base(path)
	if info.Size() > maxMetadataBytes {
		return nil, true, &ingestUsageError{Message: fmt.Sprintf("%s is %s, which is too large to be a metadata file. Remove it, or move it out of the directory: ingest cannot report indexes it cannot read.", path, formatBytes(info.Size()))}
	}
	contents, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, true, &ingestUsageError{Message: fmt.Sprintf("cannot read %s: %v. Fix its permissions, or move it out of the directory: ingest will not load a collection whose index definitions it cannot report.", path, describeFSError(readErr))}
	}
	metadata, parseErr := parseMetadata(name, file.Bucket, contents)
	if parseErr != nil {
		return nil, true, &ingestUsageError{Message: fmt.Sprintf("%s: %v. Fix or remove it: ingest will not load a collection whose index definitions it cannot report.", path, parseErr)}
	}
	return metadata, true, nil
}

// parseMetadata reads the metadata file of a collection. The file is Extended
// JSON, so a number may be written 1 or {"$numberInt": "1"}; the key's order is
// the index's field order, which is why it is read as an ordered document. The
// whole file is validated as one JSON value first, because the Extended JSON
// reader stops after the first value and reports success (build doc section 3,
// item 5).
func parseMetadata(name, stem string, contents []byte) (*foreignMetadata, error) {
	if !json.Valid(contents) {
		return nil, fmt.Errorf("is not valid JSON")
	}
	var document bson.D
	if err := bson.UnmarshalExtJSON(contents, false, &document); err != nil {
		return nil, fmt.Errorf("cannot be read as Extended JSON: %w", err)
	}
	metadata := &foreignMetadata{File: name}
	var ignored []string
	for _, element := range document {
		switch element.Key {
		case "indexes":
			list, ok := element.Value.(bson.A)
			if !ok {
				return nil, fmt.Errorf("has an \"indexes\" field that is not a list")
			}
			for position, entry := range list {
				index, err := parseForeignIndex(entry)
				if err != nil {
					return nil, fmt.Errorf("index %d: %w", position+1, err)
				}
				metadata.Indexes = append(metadata.Indexes, index)
			}
		case "type":
			if kind, ok := element.Value.(string); ok && kind != "collection" && kind != "" {
				metadata.Type = kind
			}
		case "collectionName":
			if collection, ok := element.Value.(string); ok && collection != stem {
				metadata.Warnings = append(metadata.Warnings, fmt.Sprintf("%s names the collection %q; the bucket is named after the file, %q", name, collection, stem))
			}
		case "uuid":
			metadata.HasUUID = true
		case "options":
			if options, ok := element.Value.(bson.D); ok && len(options) > 0 {
				keys := make([]string, 0, len(options))
				for _, option := range options {
					keys = append(keys, option.Key)
				}
				metadata.Warnings = append(metadata.Warnings, fmt.Sprintf("%s: the collection options (%s) are ignored; a bucket has no such options", name, strings.Join(keys, ", ")))
			}
		default:
			ignored = append(ignored, element.Key)
		}
	}
	if len(ignored) > 0 {
		metadata.Warnings = append(metadata.Warnings, fmt.Sprintf("%s: ignored fields: %s", name, strings.Join(ignored, ", ")))
	}
	return metadata, nil
}

// parseForeignIndex reads one element of "indexes".
func parseForeignIndex(entry interface{}) (foreignIndex, error) {
	definition, ok := entry.(bson.D)
	if !ok {
		return foreignIndex{}, fmt.Errorf("is not an object")
	}
	var index foreignIndex
	for _, element := range definition {
		switch element.Key {
		case "name":
			index.Name, _ = element.Value.(string)
		case "key":
			keys, ok := element.Value.(bson.D)
			if !ok || len(keys) == 0 {
				return foreignIndex{}, fmt.Errorf("has no key")
			}
			for _, part := range keys {
				index.Keys = append(index.Keys, foreignKey{Field: part.Key, Kind: keyKind(part.Value)})
			}
		case "unique":
			index.Unique = truthy(element.Value)
		case "sparse":
			index.Sparse = truthy(element.Value)
		case "partialFilterExpression":
			index.Partial = true
		case "collation":
			index.Collation = true
		case "expireAfterSeconds":
			index.Expires = true
		case "v", "ns", "background":
		default:
			index.Other = append(index.Other, element.Key)
		}
	}
	if len(index.Keys) == 0 {
		return foreignIndex{}, fmt.Errorf("has no key")
	}
	if index.Name == "" {
		index.Name = defaultForeignName(index.Keys)
	}
	return index, nil
}

// keyKind turns a key's value into "asc", "desc" or the source's own word.
func keyKind(value interface{}) string {
	switch number := value.(type) {
	case string:
		return number
	case int32:
		return directionKind(int64(number))
	case int64:
		return directionKind(number)
	case float64:
		return directionKind(int64(number))
	}
	return fmt.Sprint(value)
}

func directionKind(direction int64) string {
	switch {
	case direction > 0:
		return "asc"
	case direction < 0:
		return "desc"
	}
	return "0"
}

func truthy(value interface{}) bool {
	switch flag := value.(type) {
	case bool:
		return flag
	case int32:
		return flag != 0
	case int64:
		return flag != 0
	case float64:
		return flag != 0
	}
	return false
}

// defaultForeignName is the name the source gives an unnamed index.
func defaultForeignName(keys []foreignKey) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		direction := "1"
		switch key.Kind {
		case "desc":
			direction = "-1"
		case "asc":
		default:
			direction = key.Kind
		}
		parts = append(parts, key.Field+"_"+direction)
	}
	return strings.Join(parts, "_")
}
