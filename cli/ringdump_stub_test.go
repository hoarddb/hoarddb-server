package cli

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpStub answers the dump's ring calls from scripted pages and declarations, so a test
// can run the verb without a server and count what it asked for. It grows one method per
// step of the slice.
type dumpStub struct {
	// databasePages are served in order; the cursor of a page names the next.
	databasePages []ringclient.DatabasePage
	// bucketPages are served per database, in order, the same way.
	bucketPages map[string][]ringclient.BucketPage
	// descriptions answer DescribeDatabase, by database name.
	descriptions map[string]ringclient.DatabaseDescription
	// declarations answer DescribeBucket, by "database/bucket".
	declarations map[string]ringclient.BucketInfo
	// documentPages are served per bucket, in order, the same way as the listings.
	documentPages map[string][]ringclient.EntryPage
	// elementPages are served per "<db>/<bucket>/<id>/<field>", in order, the same way.
	elementPages map[string][]ringclient.ElementPage
	// failures answer a call instead of its scripted answer, by the key the call is logged
	// under: "ShowDatabases", "DescribeDatabase <db>", "ShowBuckets <db>",
	// "DescribeBucket <db>/<bucket>", "ListDocuments <db>/<bucket>",
	// "StoreList <db>/<bucket>/<id>/<field>".
	failures map[string]error
	// failuresFirst makes the calls of a key answer a scripted error for a while and then
	// answer normally, so a test can have a page refused and then served (Db4a). It is a
	// second table rather than a field of failures because a refusal that passes is a
	// different answer from a key that always fails.
	failuresFirst map[string]failFirst
	// beforeStoreList runs with a StoreList call's arguments before its answer, so a test
	// can act at that moment: a write that lands between an entity's List entry and its
	// store chain, or a Ctrl-C in the middle of a chain. A parallel dump calls it from
	// several goroutines at once, so a hook that blocks or records anything guards itself.
	beforeStoreList func(database, bucket, id, field string)
	// elementPageSource, when set, answers a StoreList call with a page it builds on demand
	// from the field's key and the cursor asked for, instead of the scripted table. The
	// size-gated memory tests of step Db3 generate their elements here, so the stub does
	// not hold the hundreds of megabytes it serves. A page it returns carries its own More
	// and Cursor.
	elementPageSource func(key string, after []byte) (ringclient.ElementPage, error)
	// mu guards calls and the logs in it: a dump with --parallel N reads N entities at
	// once, so every counter and every log below is touched from several goroutines.
	mu    sync.Mutex
	calls dumpStubCalls
}

// callCounts returns a copy of what the dump asked the stub for, so a test can read it
// while a parallel dump is still running.
func (s *dumpStub) callCounts() dumpStubCalls {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// failFirst is one key's scripted refusals: skip calls answer normally, the next count
// calls answer err, and every later call answers its page. skip is what lets a test refuse
// a chain's second page rather than its first (Db4a).
type failFirst struct {
	skip  int
	count int
	err   error
}

// firstFailure is the error one call of key must answer with, and it consumes one of the
// key's scripted refusals. The caller holds the stub's mutex.
func (s *dumpStub) firstFailure(key string) error {
	scripted, scriptedAny := s.failuresFirst[key]
	if !scriptedAny {
		return nil
	}
	if scripted.skip > 0 {
		scripted.skip--
		s.failuresFirst[key] = scripted
		return nil
	}
	if scripted.count <= 0 {
		return nil
	}
	scripted.count--
	s.failuresFirst[key] = scripted
	return scripted.err
}

// dumpStubCalls counts what the dump asked the stub for, so a test can pin that the dump
// reads one page at a time and never asks about a bucket it must skip.
type dumpStubCalls struct {
	ShowDatabases, DescribeDatabase, ShowBuckets, DescribeBucket int
	// describedDatabases is every database DescribeDatabase was called for, in order.
	describedDatabases []string
	// largestBucketPage is the most buckets one ShowBuckets answer held.
	largestBucketPage int
	ListDocuments     int
	// documentFields is the Fields of every ListDocuments request, in order.
	documentFields [][]string
	// documentLimits is the Limit of every ListDocuments request, in order.
	documentLimits []int32
	// documentAfters is the cursor of every ListDocuments request, in order, so a test can
	// check that a retried page was asked for with the cursor the refusal was given.
	documentAfters [][]byte
	// largestDocumentPage is the most documents one ListDocuments answer held.
	largestDocumentPage int
	// StoreList is how many element pages the dump asked for, over every field.
	StoreList int
	// storeListFields is "<db>/<bucket>/<id>/<field>" of every StoreList request, in order.
	storeListFields []string
	// storeListLimits is the Limit of every StoreList request, in order.
	storeListLimits []int
	// storeListAfters is the cursor of every StoreList request, in order.
	storeListAfters [][]byte
}

// storeListCallsFor is how many pages the dump asked the stub for one store field.
func (c dumpStubCalls) storeListCallsFor(key string) int {
	count := 0
	for _, asked := range c.storeListFields {
		if asked == key {
			count++
		}
	}
	return count
}

// ShowDatabases answers one scripted page.
func (s *dumpStub) ShowDatabases(ctx context.Context, page ringclient.PageRequest) (ringclient.DatabasePage, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.DatabasePage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.ShowDatabases++
	if err := s.failures["ShowDatabases"]; err != nil {
		return ringclient.DatabasePage{}, err
	}
	if len(s.databasePages) == 0 {
		return ringclient.DatabasePage{}, nil
	}
	index, err := dumpStubPageIndex(page.Cursor, len(s.databasePages), "databases")
	if err != nil {
		return ringclient.DatabasePage{}, err
	}
	return s.databasePages[index], nil
}

// DescribeDatabase answers with the scripted description.
func (s *dumpStub) DescribeDatabase(ctx context.Context, name string, _ ringclient.PageRequest) (ringclient.DatabaseDescription, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.DatabaseDescription{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.DescribeDatabase++
	s.calls.describedDatabases = append(s.calls.describedDatabases, name)
	if err := s.failures["DescribeDatabase "+name]; err != nil {
		return ringclient.DatabaseDescription{}, err
	}
	description, scripted := s.descriptions[name]
	if !scripted {
		return ringclient.DatabaseDescription{}, &ringclient.Error{Code: cluster.CodeNotFound,
			Message: fmt.Sprintf("database %q not found. Run 'show databases' to list what exists.", name)}
	}
	return description, nil
}

// ShowBuckets answers one scripted page of the database. A database the stub was given no
// page for holds no bucket, as an empty database on the ring does.
func (s *dumpStub) ShowBuckets(ctx context.Context, database string, page ringclient.PageRequest) (ringclient.BucketPage, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.BucketPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.ShowBuckets++
	if err := s.failures["ShowBuckets "+database]; err != nil {
		return ringclient.BucketPage{}, err
	}
	pages := s.bucketPages[database]
	if len(pages) == 0 {
		return ringclient.BucketPage{}, nil
	}
	index, err := dumpStubPageIndex(page.Cursor, len(pages), "the buckets of "+database)
	if err != nil {
		return ringclient.BucketPage{}, err
	}
	if held := len(pages[index].Buckets); held > s.calls.largestBucketPage {
		s.calls.largestBucketPage = held
	}
	return pages[index], nil
}

// DescribeBucket answers with the scripted declaration.
func (s *dumpStub) DescribeBucket(ctx context.Context, database, bucket string) (ringclient.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.BucketInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.DescribeBucket++
	if err := s.failures["DescribeBucket "+database+"/"+bucket]; err != nil {
		return ringclient.BucketInfo{}, err
	}
	declaration, scripted := s.declarations[database+"/"+bucket]
	if !scripted {
		return ringclient.BucketInfo{}, &ringclient.Error{Code: cluster.CodeNotFound,
			Message: fmt.Sprintf("Bucket %q not found in database %q. Run 'show buckets in %s' to list what exists.", bucket, database, database)}
	}
	return declaration, nil
}

// ListDocuments answers one scripted page of a bucket's documents. A bucket the stub was
// given no page for holds no document, as an empty bucket on the ring does.
func (s *dumpStub) ListDocuments(ctx context.Context, database, bucket string, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.EntryPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.ListDocuments++
	s.calls.documentFields = append(s.calls.documentFields, page.Fields)
	s.calls.documentLimits = append(s.calls.documentLimits, page.Limit)
	s.calls.documentAfters = append(s.calls.documentAfters, page.Cursor)
	if err := s.failures["ListDocuments "+database+"/"+bucket]; err != nil {
		return ringclient.EntryPage{}, err
	}
	if err := s.firstFailure("ListDocuments " + database + "/" + bucket); err != nil {
		return ringclient.EntryPage{}, err
	}
	pages := s.documentPages[database+"/"+bucket]
	if len(pages) == 0 {
		return ringclient.EntryPage{}, nil
	}
	index, err := dumpStubPageIndex(page.Cursor, len(pages), "the documents of "+database+"."+bucket)
	if err != nil {
		return ringclient.EntryPage{}, err
	}
	if held := len(pages[index].Entries); held > s.calls.largestDocumentPage {
		s.calls.largestDocumentPage = held
	}
	return pages[index], nil
}

// StoreList answers one scripted page of a store field of a document. A field the stub was
// given no page for holds no element, as an empty store on the ring does.
func (s *dumpStub) StoreList(ctx context.Context, database, bucket, id, field string, limit int, after []byte) (ringclient.ElementPage, error) {
	if err := ctx.Err(); err != nil {
		return ringclient.ElementPage{}, err
	}
	if s.beforeStoreList != nil {
		s.beforeStoreList(database, bucket, id, field)
	}
	key := database + "/" + bucket + "/" + id + "/" + field
	// The lazy source builds its page before the lock is taken: building one is the slow
	// part of a large test, and two workers must be able to build their pages at once.
	var generated ringclient.ElementPage
	var sourceErr error
	if s.elementPageSource != nil {
		generated, sourceErr = s.elementPageSource(key, after)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.StoreList++
	s.calls.storeListFields = append(s.calls.storeListFields, key)
	s.calls.storeListLimits = append(s.calls.storeListLimits, limit)
	s.calls.storeListAfters = append(s.calls.storeListAfters, after)
	if err := s.failures["StoreList "+key]; err != nil {
		return ringclient.ElementPage{}, err
	}
	if err := s.firstFailure("StoreList " + key); err != nil {
		return ringclient.ElementPage{}, err
	}
	if sourceErr != nil {
		return ringclient.ElementPage{}, sourceErr
	}
	if s.elementPageSource != nil {
		return generated, nil
	}
	pages := s.elementPages[key]
	if len(pages) == 0 {
		return ringclient.ElementPage{}, nil
	}
	index, err := dumpStubPageIndex(after, len(pages), "the elements of "+key)
	if err != nil {
		return ringclient.ElementPage{}, err
	}
	return pages[index], nil
}

// dumpStubElementPages splits one store field's elements into pages of size, carrying the
// cursor of the page after each, and keeps the keys and priorities lined up with their
// elements.
func dumpStubElementPages(page ringclient.ElementPage, size int) []ringclient.ElementPage {
	pages := make([]ringclient.ElementPage, 0, (len(page.Elements)+size-1)/size)
	for start := 0; start < len(page.Elements); start += size {
		end := start + size
		if end > len(page.Elements) {
			end = len(page.Elements)
		}
		sliced := ringclient.ElementPage{Elements: page.Elements[start:end]}
		if len(page.Keys) == len(page.Elements) {
			sliced.Keys = page.Keys[start:end]
		}
		if len(page.Priorities) == len(page.Elements) {
			sliced.Priorities = page.Priorities[start:end]
		}
		pages = append(pages, sliced)
	}
	for index := range pages {
		pages[index].More = index+1 < len(pages)
		if pages[index].More {
			pages[index].Cursor = []byte{byte(index + 1)}
		}
	}
	return pages
}

// dumpStubDocumentPages is dumpStubBucketPages for one bucket's documents.
func dumpStubDocumentPages(entries []ringclient.Entry, size int) []ringclient.EntryPage {
	pages := make([]ringclient.EntryPage, 0, (len(entries)+size-1)/size)
	for start := 0; start < len(entries); start += size {
		end := start + size
		if end > len(entries) {
			end = len(entries)
		}
		pages = append(pages, ringclient.EntryPage{Entries: entries[start:end]})
	}
	for index := range pages {
		pages[index].More = index+1 < len(pages)
		if pages[index].More {
			pages[index].Cursor = []byte{byte(index + 1)}
		}
	}
	return pages
}

// dumpStubPageIndex reads the page a cursor names. A cursor the stub never issued is an
// error, so a dump that forgets to follow the cursor fails the test instead of silently
// reading the first page again.
func dumpStubPageIndex(cursor []byte, pages int, what string) (int, error) {
	if len(cursor) == 0 {
		return 0, nil
	}
	if len(cursor) != 1 || int(cursor[0]) >= pages {
		return 0, fmt.Errorf("the dump asked for %s with a cursor of %d bytes the stub never issued for its %d pages", what, len(cursor), pages)
	}
	return int(cursor[0]), nil
}

// dumpStubDatabasePages splits databases into pages of size, each carrying the cursor of
// the page after it, so the dump's loop follows cursors as it does against a server.
func dumpStubDatabasePages(databases []ringclient.DatabaseSummary, size int) []ringclient.DatabasePage {
	pages := make([]ringclient.DatabasePage, 0, (len(databases)+size-1)/size)
	for start := 0; start < len(databases); start += size {
		end := start + size
		if end > len(databases) {
			end = len(databases)
		}
		pages = append(pages, ringclient.DatabasePage{Databases: databases[start:end]})
	}
	for index := range pages {
		pages[index].More = index+1 < len(pages)
		if pages[index].More {
			pages[index].Cursor = []byte{byte(index + 1)}
		}
	}
	return pages
}

// dumpStubBucketPages is dumpStubDatabasePages for one database's buckets.
func dumpStubBucketPages(buckets []ringclient.BucketSummary, size int) []ringclient.BucketPage {
	pages := make([]ringclient.BucketPage, 0, (len(buckets)+size-1)/size)
	for start := 0; start < len(buckets); start += size {
		end := start + size
		if end > len(buckets) {
			end = len(buckets)
		}
		pages = append(pages, ringclient.BucketPage{Buckets: buckets[start:end]})
	}
	for index := range pages {
		pages[index].More = index+1 < len(pages)
		if pages[index].More {
			pages[index].Cursor = []byte{byte(index + 1)}
		}
	}
	return pages
}

// runStubDump runs the verb against a stub source, named as root so the report reads as it
// does on the test host.
func runStubDump(t *testing.T, stub *dumpStub, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code = runRingDumpWithSource(ctx, args, ringDumpEnv{stdout: &out, stderr: &errOut}, stub, "root")
	return code, out.String(), errOut.String()
}
