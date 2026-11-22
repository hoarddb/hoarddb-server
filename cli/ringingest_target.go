package cli

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringIngestTarget is the part of the ring that deciding where a file goes needs. It is a
// small interface over *ringclient.Client so the checks can be tested with a stub and the
// sender stays free to use the client directly.
type ringIngestTarget interface {
	// DatabaseExists reports whether the database exists.
	DatabaseExists(ctx context.Context, database string) (bool, error)
	// CreateDatabase creates the database with the server's defaults. A database that
	// appeared since DatabaseExists answered is not an error.
	CreateDatabase(ctx context.Context, database string) error
	// DescribeBucket returns the declaration in force, with found false for a missing bucket.
	DescribeBucket(ctx context.Context, database, bucket string) (info ringclient.BucketInfo, found bool, err error)
	// CreateBucket declares the bucket.
	CreateBucket(ctx context.Context, database, bucket string, fields []ringclient.FieldDecl) error
}

// ringClientTarget is the ringIngestTarget of a live ring.
type ringClientTarget struct{ client *ringclient.Client }

// isRingNotFound reports whether err is the ring's NOT_FOUND.
func isRingNotFound(err error) bool {
	var refusal *ringclient.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound
}

// DatabaseExists implements ringIngestTarget.
func (t ringClientTarget) DatabaseExists(ctx context.Context, database string) (bool, error) {
	_, err := t.client.DescribeDatabase(ctx, database, ringclient.PageRequest{Limit: 1})
	switch {
	case err == nil:
		return true, nil
	case isRingNotFound(err):
		return false, nil
	}
	return false, fmt.Errorf("looking for database %q: %w", database, err)
}

// CreateDatabase implements ringIngestTarget.
func (t ringClientTarget) CreateDatabase(ctx context.Context, database string) error {
	_, err := t.client.CreateDatabaseWithStorage(ctx, database, 0, "")
	var refusal *ringclient.Error
	if errors.As(err, &refusal) && refusal.Code == cluster.CodeConflict {
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating database %q: %w", database, err)
	}
	return nil
}

// DescribeBucket implements ringIngestTarget.
func (t ringClientTarget) DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, bool, error) {
	info, err := t.client.DescribeBucket(ctx, database, bucket)
	switch {
	case err == nil:
		return info, true, nil
	case isRingNotFound(err):
		return ringclient.BucketInfo{}, false, nil
	}
	return ringclient.BucketInfo{}, false, fmt.Errorf("describing bucket %s.%s: %w", database, bucket, err)
}

// CreateBucket implements ringIngestTarget.
func (t ringClientTarget) CreateBucket(ctx context.Context, database, bucket string, fields []ringclient.FieldDecl) error {
	if _, err := t.client.CreateBucket(ctx, database, bucket, fields); err != nil {
		return fmt.Errorf("creating bucket %s.%s: %w", database, bucket, err)
	}
	return nil
}

// ringIngestParams is what the target checks need from the command line.
type ringIngestParams struct {
	Database, Bucket string
	// File is the input as the user typed it, quoted in messages.
	File     string
	NoCreate bool
	DryRun   bool
}

// preparedTarget says where the file will go and what the run did or would do to get there.
type preparedTarget struct {
	// Declaration is the bucket's fields: the existing ones, or the ones inferred.
	Declaration []ringclient.FieldDecl
	// Notes are sentences about the declaration, such as a field given the word any.
	Notes []string
	// Created* say this run created the database or the bucket; WouldCreate* say a dry run
	// would have.
	// Existing says the bucket was already there and keeps its declaration, which Declaration
	// then holds.
	Existing                               bool
	CreatedDatabase, CreatedBucket         bool
	WouldCreateDatabase, WouldCreateBucket bool
}

// prepareTarget decides where the file goes before any document is written (build plan 4.2
// step 2). A missing database or bucket is created from the scan unless --no-create; an
// existing bucket keeps its declaration, and a file that does not fit it is refused with the
// command that fixes it.
func prepareTarget(ctx context.Context, target ringIngestTarget, params ringIngestParams, scan scanResult) (preparedTarget, error) {
	var prepared preparedTarget
	databaseExists, err := target.DatabaseExists(ctx, params.Database)
	if err != nil {
		return prepared, err
	}
	if !databaseExists {
		if params.NoCreate {
			return prepared, &ingestRefusal{Message: fmt.Sprintf("Database %q not found. Run 'show databases' to list what exists, or 'create database %s', or leave out --no-create.", params.Database, params.Database)}
		}
		if err := ringCreatesDatabase(ctx, target, params, &prepared); err != nil {
			return prepared, err
		}
	} else {
		info, found, err := target.DescribeBucket(ctx, params.Database, params.Bucket)
		if err != nil {
			return prepared, err
		}
		if found {
			prepared.Existing = true
			for _, field := range info.Fields {
				if field.Name != "_id_" {
					prepared.Declaration = append(prepared.Declaration, ringclient.FieldDecl{Name: field.Name, Type: field.Type})
				}
			}
			return prepared, fitExistingBucket(info, params, scan)
		}
	}
	if params.NoCreate {
		return prepared, &ingestRefusal{Message: fmt.Sprintf("Bucket %q not found in database %q. Run 'show buckets in %s' to list what exists, or leave out --no-create and ingest will create it from the file (run with --dry-run first to see the declaration).",
			params.Bucket, params.Database, params.Database)}
	}
	return prepared, ringCreatesBucket(ctx, target, params, scan, &prepared)
}

// ringCreatesDatabase creates the database, or notes that a dry run would.
func ringCreatesDatabase(ctx context.Context, target ringIngestTarget, params ringIngestParams, prepared *preparedTarget) error {
	if params.DryRun {
		prepared.WouldCreateDatabase = true
		return nil
	}
	if err := target.CreateDatabase(ctx, params.Database); err != nil {
		return err
	}
	prepared.CreatedDatabase = true
	return nil
}

// ringCreatesBucket declares the bucket from the scan, or notes that a dry run would.
func ringCreatesBucket(ctx context.Context, target ringIngestTarget, params ringIngestParams, scan scanResult, prepared *preparedTarget) error {
	declaration, notes, err := inferDeclaration(scan.Fields, scan.Order)
	if err != nil {
		var refusal fileRefusal
		if errors.As(err, &refusal) {
			return &ingestRefusal{Message: refusal.fileText(params.File)}
		}
		return err
	}
	prepared.Declaration, prepared.Notes = declaration, notes
	if params.DryRun {
		prepared.WouldCreateBucket = true
		return nil
	}
	if err := target.CreateBucket(ctx, params.Database, params.Bucket, declaration); err != nil {
		return err
	}
	prepared.CreatedBucket = true
	return nil
}

// ringAcceptedWords is, for each declared type word, the words pass 1 uses for the BSON
// types it accepts (typed-fields-spec 3.1). Null is accepted by every field and is not
// listed. The match is by word, so a value a word cannot tell apart is left to the server: an
// int64 over the int32 range in an int32 field is refused for that document alone and reported
// then. (typed-fields-spec 3.1 says a double field refuses an integer above 2^53, but a live
// server accepted an int64 of 1<<60 in one, so nothing here relies on that rule.)
var ringAcceptedWords = map[string][]string{
	"string": {"string"}, "int32": {"int"}, "int": {"int"}, "double": {"double", "int"},
	"decimal": {"128-bit decimal"}, "number": {"int", "double", "128-bit decimal"},
	"bool": {"bool"}, "datetime": {"datetime"}, "bsontimestamp": {"timestamp"},
	"binary": {"binary"}, "uuid": {"binary"}, "objectid": {"objectid"},
	"object": {"object"}, "array": {"array"}, "regex": {"regex"}, "null": {},
}

// ringPlainWord is a field name an alter statement can carry without quoting.
var ringPlainWord = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// mismatch is one field of the file that the existing declaration cannot hold.
type mismatch struct {
	field           string
	words           []string
	documents       int64
	firstOrdinal    int64
	declared        string
	declaredIsStore bool
}

// fitExistingBucket checks the scan against the declaration in force and refuses before
// anything is written when a field is undeclared or cannot hold what the file has.
func fitExistingBucket(info ringclient.BucketInfo, params ringIngestParams, scan scanResult) error {
	declared := map[string]string{}
	for _, field := range info.Fields {
		if field.Name != "_id_" {
			declared[field.Name] = field.Type
		}
	}
	var undeclared []string
	var mismatches []mismatch
	for _, name := range scan.Order {
		declaredType, known := declared[name]
		if !known {
			undeclared = append(undeclared, name)
			continue
		}
		if found := fieldMismatch(name, declaredType, scan); found != nil {
			mismatches = append(mismatches, *found)
		}
	}
	if len(undeclared) > 0 {
		return &ingestRefusal{Message: undeclaredFieldsText(undeclared, params, scan)}
	}
	if len(mismatches) > 0 {
		return &ingestRefusal{Message: mismatchText(mismatches, params)}
	}
	return nil
}

// fieldMismatch returns the problem of one field, or nil when the declared word holds it.
func fieldMismatch(name, declaredType string, scan scanResult) *mismatch {
	if declaredType == "any" {
		return nil
	}
	accepted, scalar := ringAcceptedWords[declaredType]
	found := &mismatch{field: name, declared: declaredType, declaredIsStore: !scalar}
	for word, count := range scan.Fields[name].types {
		if word == "null" || (scalar && containsWord(accepted, word)) {
			continue
		}
		found.words = append(found.words, word)
		found.documents += count
		if first := scan.FirstOf[name][word]; found.firstOrdinal == 0 || first < found.firstOrdinal {
			found.firstOrdinal = first
		}
	}
	if len(found.words) == 0 {
		return nil
	}
	sort.Strings(found.words)
	return found
}

func containsWord(words []string, word string) bool {
	for _, candidate := range words {
		if candidate == word {
			return true
		}
	}
	return false
}

// undeclaredFieldsText names the alter statement that adds the fields, with the type each
// would be declared as. A name an alter statement cannot carry is said apart.
func undeclaredFieldsText(names []string, params ringIngestParams, scan scanResult) string {
	var adds, unwritable []string
	for _, name := range names {
		if !ringPlainWord.MatchString(name) {
			unwritable = append(unwritable, fmt.Sprintf("%q", name))
			continue
		}
		word, _ := ringWordFor(scan.Fields[name])
		adds = append(adds, "add "+name+" "+word)
	}
	var text string
	if len(names) == 1 {
		text = fmt.Sprintf("%s has a field %q that bucket %q does not declare. Nothing was written.", params.File, names[0], params.Bucket)
	} else {
		quoted := make([]string, len(names))
		for i, name := range names {
			quoted[i] = fmt.Sprintf("%q", name)
		}
		text = fmt.Sprintf("%s has %d fields that bucket %q does not declare: %s. Nothing was written.", params.File, len(names), params.Bucket, strings.Join(quoted, ", "))
	}
	switch {
	case len(adds) == 0:
		return text + " Rename them in the file, or remove them from it."
	case len(names) == 1:
		return text + " Add it: " + alterBucketStatement(params, adds) + ", or remove it from the file."
	}
	text += " Add them: " + alterBucketStatement(params, adds) + ", or remove them from the file."
	if len(unwritable) > 0 {
		text += fmt.Sprintf(" A name that is not a plain word (%s) cannot be written in an alter statement: rename it in the file.", strings.Join(unwritable, ", "))
	}
	return text
}

// alterBucketStatement is the quoted alter statement that adds the fields. It is built here, with
// the quote written as a character, so the scan that checks every quoted fix against the
// grammar (TestOneGrammarForEveryQuotedFix) does not read a format template as a statement;
// the text a user sees is parsed by TestUndeclaredFieldIntoExistingBucketNamesTheAlterFix.
func alterBucketStatement(params ringIngestParams, adds []string) string {
	return fmt.Sprintf("%c%s%[1]c", 0x27, "alter bucket "+params.Bucket+" in "+params.Database+" { "+strings.Join(adds, ", ")+" }")
}

// withArticle puts "a" or "an" before a type word: "an int", "an array", "a string".
func withArticle(word string) string {
	if word != "" && strings.ContainsRune("aeiou", rune(word[0])) {
		return "an " + word
	}
	return "a " + word
}

// mismatchText says the first mismatch in full and names the others.
func mismatchText(mismatches []mismatch, params ringIngestParams) string {
	first := mismatches[0]
	var text string
	if first.declaredIsStore {
		text = fmt.Sprintf("Field %q is %s in bucket %q, and a store is filled with its own commands, not from a field of a file. Remove %q from the file, or load into a new bucket: --into %s.%s2.",
			first.field, withArticle(first.declared), params.Bucket, first.field, params.Database, params.Bucket)
	} else {
		text = fmt.Sprintf("Field %q is %s in %s of %s and bucket %q declares %s. A declared type cannot be changed, so load into a new bucket: --into %s.%s2, or fix the %s (the first is document %s).",
			first.field, withArticle(strings.Join(first.words, " or ")), documentsPhrase(first.documents), params.File, params.Bucket, first.declared,
			params.Database, params.Bucket, valuesPhrase(first.documents), formatCount(first.firstOrdinal))
	}
	if len(mismatches) > 1 {
		others := make([]string, 0, len(mismatches)-1)
		for _, other := range mismatches[1:] {
			others = append(others, fmt.Sprintf("%q", other.field))
		}
		text += " The same problem is in " + strings.Join(others, ", ") + "."
	}
	return text
}

func documentsPhrase(count int64) string {
	if count == 1 {
		return "1 document"
	}
	return formatCount(count) + " documents"
}

func valuesPhrase(count int64) string {
	if count == 1 {
		return "value"
	}
	return formatCount(count) + " values"
}
