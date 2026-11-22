package cli

import (
	"context"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringDumpSource is every ring call a dump makes, in one place, so a test can answer them
// without a server and check how often each was called. It gains one method per step of
// the slice: Da2 needs the database and bucket listings and the two declarations. The
// methods take exactly what a dump has, and the implementations here are one line each
// over ringclient.
type ringDumpSource interface {
	// ShowDatabases is one page of the databases this session may see.
	ShowDatabases(ctx context.Context, page ringclient.PageRequest) (ringclient.DatabasePage, error)
	// DescribeDatabase is a database's replication factor and storage, and one page of its
	// buckets, which the dump ignores.
	DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error)
	// ShowBuckets is one page of the buckets of database.
	ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error)
	// DescribeBucket is the declaration in force of one bucket.
	DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, error)
	// ListDocuments is one page of the documents of a bucket, with the fields the request
	// projects; a nil filter lists every document.
	ListDocuments(ctx context.Context, database, bucket string, page ringclient.PageRequest) (ringclient.EntryPage, error)
	// StoreList is one page of one store field of one document, in the store's own order;
	// the dump follows its cursor until More is false.
	StoreList(ctx context.Context, database, bucket, id, field string, limit int, after []byte) (ringclient.ElementPage, error)
}

// ringClientSource is the ring itself: one client, one line per method.
type ringClientSource struct {
	client *ringclient.Client
}

// ShowDatabases is one page of the databases this session may see. The server leaves out
// the databases the user may not see (cluster/wire/ringadmin_serve.go:ringServer.showDatabases),
// so the dump is a subset by construction and its report says so.
func (s ringClientSource) ShowDatabases(ctx context.Context, page ringclient.PageRequest) (ringclient.DatabasePage, error) {
	return s.client.ShowDatabases(ctx, page)
}

// DescribeDatabase reads a database's replication factor and storage. The bucket page in
// the reply is not used: ShowBuckets pages the buckets where a dropping one is named.
func (s ringClientSource) DescribeDatabase(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	return s.client.DescribeDatabase(ctx, database, page)
}

// ShowBuckets is one page of the buckets of database, in the server's name-slot order.
func (s ringClientSource) ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	return s.client.ShowBuckets(ctx, database, page)
}

// DescribeBucket reads the declaration in force, which the dump writes as the bucket
// record and the later steps read the documents with.
func (s ringClientSource) DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, error) {
	return s.client.DescribeBucket(ctx, database, bucket)
}

// ListDocuments is one page of a bucket's documents. The filter is nil: a dump carries
// every document the bucket holds.
//
// An empty Fields cannot go on the wire: cluster/wire.EncodeRingList refuses it before it
// is sent ("fields is empty. Send at least one field name ... or leave fields out to get
// every field", cluster/wire/projection.go:checkFieldNames), so a request that names no
// scalar field is sent with the member left out. The node then answers every field, store
// markers included, and the dump drops those members by name from the declaration
// (cli/ringdump_docs.go:ringDumpDocumentBody): the fallback of section 3.2 item 4.
func (s ringClientSource) ListDocuments(ctx context.Context, database, bucket string, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	if len(page.Fields) == 0 {
		page.Fields = nil
	}
	return s.client.Bucket(database, bucket).List(ctx, nil, page)
}

// StoreList is one page of a store field of a document, read through the bucket the dump
// enumerated. The store's own order is the order the dump records (section 3.1, "Order of
// elements"); a fifoStore and a lifoStore list in arrival order.
func (s ringClientSource) StoreList(ctx context.Context, database, bucket, id, field string, limit int, after []byte) (ringclient.ElementPage, error) {
	return s.client.Bucket(database, bucket).StoreList(ctx, id, field, limit, after)
}
