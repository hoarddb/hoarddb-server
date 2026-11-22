package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The words of a bucket's declaration and of its state that the enumeration acts on.
const (
	// dumpBucketIDField is the id member of a bucket's field table. The doc record's id
	// carries it, so it is not a declared field of the bucket record.
	dumpBucketIDField = "_id_"
	// dumpBlobStoreWord is the store word whose payload a dump does not carry; the bucket
	// record names such a field under unsupported (section 3.1).
	dumpBlobStoreWord = "blobStore"
	// dumpBucketDroppingState is the state of a bucket that is leaving. It is still listed
	// by ShowBuckets and is not dumped (section 3.2 item 2).
	dumpBucketDroppingState = "dropping"
)

// ringDumpDroppingBucketFormat is the warning of a bucket that is being dropped. A
// restore cannot recreate it from a record, so it is left out and the run says so
// instead of failing.
const ringDumpDroppingBucketFormat = "Bucket %s is being dropped, so it is not in this dump.\n"

// The warnings of a bucket that was dropped after the dump had begun to read it. What was
// read before the drop stays in the file and the run goes on (Db4b). They say which part of
// the entity in progress is in the file, because that differs by mode: --parallel 1 writes
// an entity straight to the file, so a part of it can be there; a parallel run reads it into
// a buffer that is thrown away. The texts never carry the server's numeric ids: a developer
// cannot act on "bucket 7 in database 2".
const (
	ringDumpBucketDroppedSerialFormat   = "Bucket %s was dropped while the dump was reading it, so this file holds only what was read before the drop; the entity being read at that moment may be only partly in it.\n"
	ringDumpBucketDroppedParallelFormat = "Bucket %s was dropped while the dump was reading it, so this file holds only what was read before the drop; the entities being read at that moment are not in it.\n"
)

// ringDumpBucketDroppedBeforeReadFormat is the warning of a bucket that was listed and then
// dropped before its declaration could be read: nothing of it was written, and the run goes
// on with the next bucket.
const ringDumpBucketDroppedBeforeReadFormat = "Bucket %s was dropped before the dump could read it, so it is not in this file.\n"

// ringDumpBucketNotFoundFormat is the refusal of --bucket for a bucket the ring does not
// hold. It names the statement that lists what exists (AGENTS.md rule 3).
const ringDumpBucketNotFoundFormat = `Bucket %q not found in database %q. Run 'show buckets in %s' to list what exists.`

// errBucketLeftDuringTheDump ends one bucket's reading when the ring says the bucket is
// dropping or already gone. dumpBucket turns it into the by-name warning and goes on with
// the next bucket, so it never reaches the run's failure path and the exit stays 0.
var errBucketLeftDuringTheDump = errors.New("the bucket was dropped while the dump was reading it")

// bucketLeftDuringTheDump reports whether err is the ring saying the bucket this read names
// has left: the code BUCKET_DROPPING, a NOT_FOUND on a read that names no document, or the
// numeric sentence a node builds when it lost the bucket's declaration.
//
// forDocumentPage is true for a read that names no document - a ListDocuments page and
// DescribeBucket - because there a NOT_FOUND can only mean the
// bucket. A StoreList's NOT_FOUND is the document deleted after its List page, which the
// dump already handles as the "document deleted" case and which is not a bucket that left
// (cli/ringdump_stores.go:documentIsGone).
//
// The sentence is matched by its words and never by its ids: cluster/wire/node.go builds it
// from numeric ids ("bucket 7 in database 2 is being dropped or was dropped: ..."), and a
// developer cannot act on an id. Two conditions are the same event (a node that lost the
// declaration, and an owner that refuses a dropping bucket), so both are a warning.
func bucketLeftDuringTheDump(err error, forDocumentPage bool) bool {
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) {
		return false
	}
	if refusal.Code == cluster.CodeBucketDropping {
		return true
	}
	if strings.Contains(refusal.Message, "is being dropped or was dropped") {
		return true
	}
	return forDocumentPage && refusal.Code == cluster.CodeNotFound
}

// ringDumpNoBucketsFormat is the refusal of a run that wrote no bucket at all: a file with
// no bucket would restore into nothing while looking like a backup (section 5, 3.2 item
// 1).
const ringDumpNoBucketsFormat = `The ring holds no buckets in %s, so there is nothing to dump. Run 'show buckets in <database>' to list what exists, or 'create bucket <name> in <database> { Name string }' to create one.`

// ringDumpAnyDatabase names the scope of a run with no --db flag. Listing the databases
// the session may see would be work that grows with the ring, so the refusal says what
// the scope was instead of naming it.
const ringDumpAnyDatabase = "any database this session may see"

// enumerate writes the dump's db and bucket records: every database in scope, in the
// order the server lists it, each followed by its buckets (sections 3.1 and 3.2 items 1
// and 2). It pages and writes as it goes, so it holds one page and one declaration at a
// time and no list of names: the only names it keeps are the --db ones from the flags.
func (r *ringDumpRun) enumerate() error {
	wanted := make(map[string]bool, len(r.flags.databases))
	for _, name := range r.flags.databases {
		wanted[name] = true
	}
	var cursor []byte
	for {
		page, err := r.source.ShowDatabases(r.ctx, ringclient.PageRequest{Cursor: cursor})
		if err != nil {
			return fmt.Errorf("listing the databases this session may see: %w", err)
		}
		for _, database := range page.Databases {
			if len(wanted) > 0 && !wanted[database.Name] {
				continue
			}
			if err := r.dumpDatabase(database.Name); err != nil {
				return err
			}
		}
		if !page.More {
			return r.refuseWhenNoBucketWasWritten()
		}
		cursor = page.Cursor
	}
}

// dumpDatabase writes a database's db record and then its buckets. The rf and storage
// come from DescribeDatabase; its bucket list is ignored, because the buckets are paged
// by ShowBuckets, where a dropping one can be told apart.
func (r *ringDumpRun) dumpDatabase(name string) error {
	description, err := r.source.DescribeDatabase(r.ctx, name, ringclient.PageRequest{Limit: 1})
	if err != nil {
		return fmt.Errorf("describing database %q: %w", name, err)
	}
	record := &dumpDB{T: dumpKindDB, Name: name, RF: description.RF, Storage: description.Storage}
	if err := r.writer.WriteRecord(record); err != nil {
		return fmt.Errorf("writing the db record of %q: %w", name, err)
	}
	r.counters.Databases++
	return r.dumpBuckets(name)
}

// dumpBuckets writes the bucket records of one database, one ShowBuckets page at a time.
// --bucket names one bucket instead, which is described without scanning the database.
func (r *ringDumpRun) dumpBuckets(database string) error {
	if r.flags.bucket != "" {
		return r.dumpBucket(database, r.flags.bucket, true)
	}
	var cursor []byte
	for {
		page, err := r.source.ShowBuckets(r.ctx, database, ringclient.PageRequest{Cursor: cursor})
		if err != nil {
			return fmt.Errorf("listing the buckets of database %q: %w", database, err)
		}
		for _, bucket := range page.Buckets {
			if bucket.State == dumpBucketDroppingState {
				r.warnDroppingBucket(database, bucket.Name)
				continue
			}
			if err := r.dumpBucket(database, bucket.Name, false); err != nil {
				return err
			}
		}
		if !page.More {
			return nil
		}
		cursor = page.Cursor
	}
}

// dumpBucket writes one bucket's record. named says the bucket came from --bucket, so a
// NOT_FOUND is the refusal that names the listing statement rather than an error the
// operator cannot act on: a named bucket keeps its refusal when it has vanished, and a
// listed one becomes a warning and the run goes on with the next bucket (Db4b).
func (r *ringDumpRun) dumpBucket(database, name string, named bool) error {
	declaration, err := r.source.DescribeBucket(r.ctx, database, name)
	if err != nil {
		var refusal *ringclient.Error
		if named && errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound {
			return &RingError{Exit: ExitRefused,
				Message: fmt.Sprintf(ringDumpBucketNotFoundFormat, name, database, database), Err: err}
		}
		// A bucket that was listed and dropped before its declaration was read left nothing to
		// write: say so by name and keep going. forDocumentPage is true here because a
		// DescribeBucket names no document, so its NOT_FOUND can only be the bucket.
		if !named && bucketLeftDuringTheDump(err, true) {
			r.warnBucketDroppedBeforeRead(database, name)
			return nil
		}
		return fmt.Errorf("describing bucket %s.%s: %w", database, name, err)
	}
	// A bucket can start dropping between the page and its declaration, and --bucket names
	// one without a page, so the state of the declaration is the one that decides.
	if declaration.State == dumpBucketDroppingState {
		r.warnDroppingBucket(database, name)
		return nil
	}
	if err := r.writer.WriteRecord(dumpBucketRecord(declaration)); err != nil {
		return fmt.Errorf("writing the bucket record of %s.%s: %w", database, name, err)
	}
	r.counters.Buckets++
	// The bucket's entities follow here, with this declaration in hand; nothing about the
	// bucket is kept once its records are written, so a run's memory is one page, one
	// declaration and one document page whatever the ring holds.
	if err := r.dumpDocuments(database, name, declaration); err != nil {
		if errors.Is(err, errBucketLeftDuringTheDump) {
			// The bucket left while its documents or stores were read. What was read is
			// already in the file, so the run warns by name and goes on: a dropped bucket is
			// not a failure of the dump, whose exit stays 0 (Db4b, section 3.5).
			r.warnBucketDroppedWhileRead(database, name)
			return nil
		}
		return err
	}
	return nil
}

// refuseWhenNoBucketWasWritten is the empty-dump refusal of section 5: a run that
// enumerated every database in scope and wrote no bucket stops before the file is
// completed, so no final file and no .partial are left behind.
func (r *ringDumpRun) refuseWhenNoBucketWasWritten() error {
	if r.counters.Buckets > 0 {
		return nil
	}
	return &RingError{Message: fmt.Sprintf(ringDumpNoBucketsFormat, r.dumpScopeNames()), Exit: ExitRefused}
}

// dumpScopeNames is the database list the empty-dump refusal names: the --db flags,
// quoted and comma separated, or the phrase for the whole ring. It is built from the flags
// and never from the databases the run listed, so it costs nothing that grows with the ring.
func (r *ringDumpRun) dumpScopeNames() string {
	if len(r.flags.databases) == 0 {
		return ringDumpAnyDatabase
	}
	quoted := make([]string, len(r.flags.databases))
	for index, name := range r.flags.databases {
		quoted[index] = fmt.Sprintf("%q", terminalSafeText(name))
	}
	return strings.Join(quoted, ", ")
}

// warnDroppingBucket says on stderr that a bucket that is leaving is not in the dump.
func (r *ringDumpRun) warnDroppingBucket(database, name string) {
	_, _ = fmt.Fprintf(r.stderr, ringDumpDroppingBucketFormat, terminalSafeText(database+"."+name))
}

// warnBucketDroppedWhileRead says that a bucket was dropped after part of it was written:
// the file holds what was read before the drop. It names the bucket as the developer writes
// it, never by the server's numeric ids.
func (r *ringDumpRun) warnBucketDroppedWhileRead(database, name string) {
	format := ringDumpBucketDroppedParallelFormat
	if r.parallel <= 1 {
		format = ringDumpBucketDroppedSerialFormat
	}
	_, _ = fmt.Fprintf(r.stderr, format, terminalSafeText(database+"."+name))
}

// warnBucketDroppedBeforeRead says that a listed bucket was gone before its declaration
// could be read, so nothing of it is in the file.
func (r *ringDumpRun) warnBucketDroppedBeforeRead(database, name string) {
	_, _ = fmt.Fprintf(r.stderr, ringDumpBucketDroppedBeforeReadFormat, terminalSafeText(database+"."+name))
}

// dumpBucketRecord is the bucket record of a declaration: the fields in the order the
// ring reports them (its declaration order) without _id_, each with the expire_path of
// its queue field, the bucket's own expire_field, its aggregates, and every blobStore
// field under unsupported.
func dumpBucketRecord(declaration ringclient.BucketInfo) *dumpBucket {
	expirePaths := make(map[string]string, len(declaration.ExpireStores))
	for _, store := range declaration.ExpireStores {
		expirePaths[store.Field] = store.Path
	}
	record := &dumpBucket{
		T:           dumpKindBucket,
		DB:          declaration.Database,
		Name:        declaration.Name,
		Fields:      []dumpField{},
		ExpireField: declaration.ExpireField,
		Aggregates:  []dumpAggregate{},
		Unsupported: []string{},
	}
	for _, field := range declaration.Fields {
		if field.Name == dumpBucketIDField {
			continue
		}
		record.Fields = append(record.Fields, dumpField{
			Name:       field.Name,
			Type:       field.Type,
			Of:         field.Of,
			ExpirePath: expirePaths[field.Name],
		})
		if field.Type == dumpBlobStoreWord {
			record.Unsupported = append(record.Unsupported, field.Name)
		}
	}
	for _, aggregate := range declaration.Aggregates {
		record.Aggregates = append(record.Aggregates, dumpAggregate{
			Store: aggregate.Store,
			Kind:  aggregate.Kind,
			Field: aggregate.Field,
			By:    aggregate.By,
		})
	}
	return record
}
