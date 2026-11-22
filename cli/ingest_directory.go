package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// defaultMaxBuckets is the ceiling --max-buckets starts at (ingest-spec.md
// §11): each file of a directory can become a bucket, and more than this is a
// lot to create in one run. It bounds the run, not what a node holds.
const defaultMaxBuckets = 1000

// directoryFile is one .bson file of a directory ingest and the bucket it
// becomes.
type directoryFile struct {
	// Name is the file name, "users.bson": the key of its resume record.
	Name string
	Path string
	// Bucket is the file name without ".bson".
	Bucket string
	// Metadata is the file's <stem>.metadata.json, nil when there is none.
	Metadata *foreignMetadata
	// Refusal is why this file is not loaded although the directory is, set
	// from its metadata; empty for a file that is.
	Refusal string
}

// directoryPlan is what a directory holds, read and checked without a server.
type directoryPlan struct {
	Path  string
	Files []directoryFile
	// Notes are things in the directory ingest did not read, in words a
	// reader can act on.
	Notes []string
}

// directoryRequest is what resolveDirectory needs. Input is the path as the
// user typed it, quoted in the commands the refusals suggest.
type directoryRequest struct {
	Input      string
	Database   string
	MaxBuckets int
	AllowDots  bool
}

// database is the database the commands in a refusal name: the request's own, or app, the
// placeholder `plan` prints in its next steps, when the request has none.
func (r directoryRequest) database() string {
	if r.Database == "" {
		return "app"
	}
	return r.Database
}

// maxListedNames bounds how many names a refusal or note spells out.
const maxListedNames = 5

// resolveDirectory lists the directory ingest would read and refuses what it
// cannot: a subdirectory (one level only), no .bson file at all, a name that
// cannot be a bucket, more files than --max-buckets, and a dotted bucket name
// without --allow-dots. It makes no connection (spec §4.1, §5.1, §11), so every
// refusal comes before anything is created.
func resolveDirectory(req directoryRequest) (*directoryPlan, error) {
	entries, err := os.ReadDir(req.Input)
	if err != nil {
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot read directory %s: %v. Check the path.", req.Input, describeFSError(err))}
	}
	var subdirectories, compressed, ignored, metadataFiles []string
	plan := &directoryPlan{Path: req.Input}
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case entry.IsDir():
			subdirectories = append(subdirectories, name)
		case isDirectoryBookkeeping(name):
		case strings.HasPrefix(name, "."):
			ignored = append(ignored, name)
		case strings.HasSuffix(name, ".bson") && len(name) > len(".bson"):
			plan.Files = append(plan.Files, directoryFile{
				Name: name, Path: filepath.Join(req.Input, name), Bucket: strings.TrimSuffix(name, ".bson"),
			})
		case strings.HasSuffix(name, ".gz"):
			compressed = append(compressed, name)
		case strings.HasSuffix(name, ".metadata.json"):
			metadataFiles = append(metadataFiles, name)
		default:
			ignored = append(ignored, name)
		}
	}
	if len(subdirectories) > 0 {
		return nil, &ingestUsageError{Message: subdirectoryText(req, subdirectories)}
	}
	if len(plan.Files) == 0 {
		return nil, &ingestUsageError{Message: emptyDirectoryText(req, compressed)}
	}
	if len(compressed) > 0 {
		plan.Notes = append(plan.Notes, fmt.Sprintf("not read, because this version reads uncompressed .bson only: %s. Run 'gunzip -k' on them and ingest the directory again.", listNames(compressed)))
	}
	if len(ignored) > 0 {
		plan.Notes = append(plan.Notes, fmt.Sprintf("not read, because it is not a .bson file: %s", listNames(ignored)))
	}
	if err := checkDirectoryNames(req, plan); err != nil {
		return nil, err
	}
	if err := attachMetadata(req, plan, metadataFiles); err != nil {
		return nil, err
	}
	return plan, nil
}

// attachMetadata reads each file's metadata, or refuses the run when one cannot
// be read, because index definitions that are silently skipped are worse than
// no directory ingest at all (spec §14). A metadata file with no .bson beside
// it is noted.
func attachMetadata(req directoryRequest, plan *directoryPlan, metadataFiles []string) error {
	matched := make(map[string]bool, len(plan.Files))
	for position := range plan.Files {
		file := &plan.Files[position]
		metadata, found, err := readMetadataFor(req.Input, *file)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		matched[file.Bucket+".metadata.json"] = true
		file.Metadata = metadata
		file.Refusal = metadataRefusal(file, req)
		plan.Notes = append(plan.Notes, metadata.Warnings...)
	}
	var withUUID int
	for _, file := range plan.Files {
		if file.Metadata != nil && file.Metadata.HasUUID {
			withUUID++
		}
	}
	if withUUID > 0 {
		plan.Notes = append(plan.Notes, fmt.Sprintf("the uuid in %d metadata file(s) is ignored: a bucket's identity is its name", withUUID))
	}
	var orphans []string
	for _, name := range metadataFiles {
		if !matched[name] {
			orphans = append(orphans, name)
		}
	}
	if len(orphans) > 0 {
		plan.Notes = append(plan.Notes, fmt.Sprintf("not read, because no .bson file sits beside it: %s", listNames(orphans)))
	}
	return nil
}

// metadataRefusal is why a file is not loaded because of what its metadata
// says it is: a view holds no documents of its own, and a time series
// collection has no store type here.
func metadataRefusal(file *directoryFile, req directoryRequest) string {
	var what string
	switch file.Metadata.Type {
	case "":
		return ""
	case "view":
		what = "a view, which holds no documents of its own"
	case "timeseries":
		what = "a time series collection, and hoardDB has no such store"
	default:
		what = fmt.Sprintf("of type %q, which hoardDB does not load", file.Metadata.Type)
	}
	return fmt.Sprintf("%s says this collection is %s, so it is not loaded with the directory. If the .bson holds documents you want, load it on its own: hoarddb ingest %s --into %s.%s",
		file.Metadata.File, what, file.Path, req.database(), file.Bucket)
}

// isDirectoryBookkeeping is the resume record and its lock, which a directory
// ingest writes into the directory it reads.
func isDirectoryBookkeeping(name string) bool {
	return name == directoryRecordName || name == directoryRecordName+".lock"
}

// listNames spells out up to maxListedNames names and counts the rest.
func listNames(names []string) string {
	if len(names) <= maxListedNames {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxListedNames], ", "), len(names)-maxListedNames)
}

func subdirectoryText(req directoryRequest, subdirectories []string) string {
	first := subdirectories[0]
	return fmt.Sprintf("%s holds a subdirectory (%s). Ingest reads the .bson files of one directory and nothing below it, because a dump keeps each database in a directory of its own. "+
		"Point it at the directory that holds the files (%s), or load one file of it: hoarddb ingest %s/<file>.bson --into %s.<bucket>",
		req.Input, listNames(subdirectories), filepath.Join(req.Input, first), filepath.Join(req.Input, first), req.database())
}

func emptyDirectoryText(req directoryRequest, compressed []string) string {
	if len(compressed) > 0 {
		return fmt.Sprintf("%s holds no .bson files, so there is nothing to ingest. It holds compressed files (%s), and this version reads uncompressed .bson only. "+
			"Run 'gunzip -k %s' and ingest the directory again, or re-export without compression.",
			req.Input, listNames(compressed), filepath.Join(req.Input, "*.gz"))
	}
	return fmt.Sprintf("%s holds no .bson files, so there is nothing to ingest. Name a directory of .bson files, or load one file with: hoarddb ingest <file> --into %s.<bucket>", req.Input, req.database())
}

// checkDirectoryNames applies the checks that depend on the bucket names and
// their number.
func checkDirectoryNames(req directoryRequest, plan *directoryPlan) error {
	var dotted []directoryFile
	for _, file := range plan.Files {
		if err := validNameComponentForRestore("bucket", file.Bucket); err != nil {
			return &ingestUsageError{Message: fmt.Sprintf("%s cannot become a bucket: %v. Rename the file, or load it under a name you choose: hoarddb ingest %s --into %s.<bucket>",
				file.Name, err, file.Path, req.database())}
		}
		if strings.Contains(file.Bucket, ".") {
			dotted = append(dotted, file)
		}
	}
	if len(plan.Files) > req.MaxBuckets {
		return &ingestUsageError{Message: tooManyBucketsText(req, len(plan.Files))}
	}
	if len(dotted) > 0 && !req.AllowDots {
		return &ingestUsageError{Message: dottedNamesText(req, dotted)}
	}
	return nil
}

// tooManyBucketsText is ingest-spec.md §11's refusal. It gives the run's own
// bound as the reason: the server enforces no bucket count, so the text names
// none (owner, 2026-11-21).
func tooManyBucketsText(req directoryRequest, files int) string {
	return fmt.Sprintf("%s holds %s .bson files, and a plan reads at most %d from one directory: each file can become a bucket, and that many is a lot to create in one run. "+
		"Plan fewer files at a time, or load the collections you need one at a time: hoarddb ingest %s/<file>.bson --into %s.<bucket>.",
		req.Input, formatCount(int64(files)), req.MaxBuckets, req.Input, req.database())
}

func dottedNamesText(req directoryRequest, dotted []directoryFile) string {
	names := make([]string, 0, len(dotted))
	for _, file := range dotted {
		names = append(names, file.Bucket)
	}
	first := dotted[0]
	return fmt.Sprintf("%s holds %d file(s) whose name has a dot (%s), and ring ingest cannot load into a bucket whose name has a dot: --into reads the text after the first dot as the bucket, and a second dot makes a three-part target it refuses. "+
		"Load each such file under a name without the dot: hoarddb ingest %s --into %s.%s",
		req.Input, len(dotted), listNames(names), first.Path, req.database(), strings.ReplaceAll(first.Bucket, ".", "_"))
}

// directoryRecordName is the resume record a directory ingest keeps inside the
// directory it reads (ingest-spec.md §8.2).
const directoryRecordName = ".hoardingest"
