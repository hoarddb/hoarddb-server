package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// restoreFieldDecls turns a bucket record back into the declaration a create carries. The
// dump keeps the fields in declared order and the aggregates as a flat list, so the
// aggregates are put back on the store field each was declared on; their values are not in
// a dump and the server rebuilds them from the documents the restore writes.
func restoreFieldDecls(bucket *dumpBucket) []ringclient.FieldDecl {
	decls := make([]ringclient.FieldDecl, len(bucket.Fields))
	for index, field := range bucket.Fields {
		decls[index] = ringclient.FieldDecl{
			Name:       field.Name,
			Type:       field.Type,
			Of:         field.Of,
			ExpirePath: field.ExpirePath,
		}
	}
	// Both lists come from dumpBucketRecord, which writes an aggregate only for a field it
	// also writes, so every Store names a field here.
	for index, field := range bucket.Fields {
		for _, aggregate := range bucket.Aggregates {
			if aggregate.Store != field.Name {
				continue
			}
			decls[index].Aggregates = append(decls[index].Aggregates, ringclient.AggregateDecl{
				Kind:  aggregate.Kind,
				Field: aggregate.Field,
				By:    aggregate.By,
			})
		}
	}
	return decls
}

// createDatabase makes the target database with the record's replication factor and
// storage. name is the target name and may differ from record.Name, because --as restores
// one database under another name. It is safe to repeat, so a restore resumed after an
// interruption asks for the same database again; the ring refuses every existing name with
// CONFLICT, so the existing row is read to tell a repeat from a name now holding other
// options, and only a repeat is left alone.
func (r *ringRestoreRun) createDatabase(record *dumpDB, name string) error {
	if _, err := retryQuorumCtx(r.ctx, func() (ringclient.DatabaseInfo, error) {
		return r.createSource().CreateDatabaseWithStorage(r.ctx, name, record.RF, record.Storage)
	}); err != nil {
		if r.databaseHoldsRecordOptions(record, name, err) {
			return nil
		}
		return fmt.Errorf("creating database %q: %w", name, err)
	}
	return nil
}

// databaseHoldsRecordOptions reports whether createErr is the CONFLICT of a database that
// already exists with the record's replication factor and storage. Any other error, and a
// row that differs, stays the caller's to report. An unreadable row keeps the create's own
// error, which is the one that says what the create asked for.
func (r *ringRestoreRun) databaseHoldsRecordOptions(record *dumpDB, name string, createErr error) bool {
	var refusal *ringclient.Error
	if !errors.As(createErr, &refusal) || refusal.Code != cluster.CodeConflict {
		return false
	}
	existing, err := retryQuorumCtx(r.ctx, func() (ringclient.DatabaseDescription, error) {
		return r.client.DescribeDatabase(r.ctx, name, ringclient.PageRequest{Limit: 1})
	})
	if err != nil {
		return false
	}
	// An empty storage word asks the server for its default, which is disk.
	wantStorage := record.Storage
	if wantStorage == "" {
		wantStorage = cluster.StorageWordDisk
	}
	return existing.RF == record.RF && existing.Storage == wantStorage
}

// createBucket makes the target bucket with the record's declaration: same fields in the
// same order, each queue field's expiry path, the bucket's expiry field and its aggregates.
func (r *ringRestoreRun) createBucket(record *dumpBucket, database string) error {
	decls := restoreFieldDecls(record)
	for index := range decls {
		decls[index].Of = r.flags.restoreOf(decls[index].Of)
	}
	if _, err := retryQuorumCtx(r.ctx, func() (ringclient.BucketInfo, error) {
		return r.createSource().CreateBucketExpiring(r.ctx, database, record.Name, record.ExpireField, decls)
	}); err != nil {
		return fmt.Errorf("creating bucket %q in database %q: %w", record.Name, database, err)
	}
	return nil
}

// restoreBlobFieldLines is what the restore tells an operator about the fields a dump could
// not carry: a blobStore keeps its payloads outside the dump, so the field is recreated
// empty and the run says so instead of looking complete.
func restoreBlobFieldLines(bucket *dumpBucket, database string) []string {
	lines := make([]string, len(bucket.Unsupported))
	for index, fieldName := range bucket.Unsupported {
		lines[index] = fmt.Sprintf("Bucket %s field %s (blobStore): payloads are not in a dump, so this field restored empty.",
			terminalSafeText(database+"."+bucket.Name), terminalSafeText(fieldName))
	}
	return lines
}

// restoreCreateSource is the part of the ring client a restore creates with.
type restoreCreateSource interface {
	CreateDatabaseWithStorage(ctx context.Context, name string, rf int32, storage string) (ringclient.DatabaseInfo, error)
	CreateBucketExpiring(ctx context.Context, database, name, expireField string, fields []ringclient.FieldDecl) (ringclient.BucketInfo, error)
}

// createSource is what creates go to: the client, unless a test replaced it.
func (r *ringRestoreRun) createSource() restoreCreateSource {
	if r.creator != nil {
		return r.creator
	}
	return r.client
}
