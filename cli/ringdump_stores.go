package cli

import (
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The store type words this step treats differently from the rest: a blobStore payload is
// in no list reply at all, a fifoStore and a lifoStore are queues whose claimed and
// dead-lettered messages are in no list reply either, and the other three carry keys.
const (
	dumpFifoStoreWord  = "fifoStore"
	dumpLifoStoreWord  = "lifoStore"
	dumpHashStoreWord  = "hashStore"
	dumpBTreeStoreWord = "btreeStore"
	dumpHeapStoreWord  = "heapStore"
)

const (
	// dumpStorePageSize is how many elements one StoreList page asks for: the ceiling
	// ringclient.Bucket.StoreList accepts, so a field costs as few round trips as the
	// server allows (section 3.2 item 5).
	dumpStorePageSize = 1000
	// dumpChunkElements is the most elements one elems record carries (section 3.1).
	dumpChunkElements = 1000
	// dumpChunkBytes is the soft byte cap of one elems chunk: half the node's default
	// document size, derived from the config constant so a changed default moves it. It is
	// a soft cap only because a chunk's first element always goes in, however large.
	dumpChunkBytes = config.DefaultMaxDocumentBytes / 2
)

// ringDumpBlobFieldFormat is the one warning a bucket's blobStore field gets: a dump never
// reads a blob, so its payload is not in the file and a restore cannot recreate it
// (section 3.1, the bucket record's unsupported list).
const ringDumpBlobFieldFormat = "Bucket %s field %s (blobStore): blob payloads are not dumped, so they are not in this file and restore cannot bring them back.\n"

// noteBucketStores is what is said once per bucket, not once per entity: a blobStore
// field's payload is not in the file, and the run remembers that it holds a queue field so
// the closing lines can carry the queue sentence (section 3.1, "What the tool prints").
// It is called as the bucket's entity pass starts, right after the bucket record is
// written, so a bucket with no entity still says both.
func (r *ringDumpRun) noteBucketStores(database, bucket string, declaration ringclient.BucketInfo) {
	for _, field := range declaration.Fields {
		switch field.Type {
		case dumpBlobStoreWord:
			_, _ = fmt.Fprintf(r.stderr, ringDumpBlobFieldFormat,
				terminalSafeText(database+"."+bucket), terminalSafeText(field.Name))
		case dumpFifoStoreWord, dumpLifoStoreWord:
			r.heldQueueField = true
		}
	}
}

// dumpStoreFields writes the elems records of one entity: its non-blob store fields in
// declared order, each read once (section 3.2 item 5, section 3.3). A blobStore field is
// skipped without a call, because its payload is in no list reply and the bucket record
// already names it under unsupported.
func (r *ringDumpRun) dumpStoreFields(entity dumpEntityRecord) error {
	for _, field := range entity.declaration.Fields {
		if !ringStoreType(field.Type) || field.Type == dumpBlobStoreWord {
			continue
		}
		if err := r.dumpStoreField(entity, field); err != nil {
			return err
		}
	}
	return nil
}

// dumpStoreField reads one store field's chain once and writes it as elems records. A page
// can fill several chunks and a chunk can span pages, so the partly filled chunk is
// carried across pages and written when it is full or the chain ends: the run holds one
// chunk and the page it came from, never a whole store (AGENTS.md rule 11).
func (r *ringDumpRun) dumpStoreField(entity dumpEntityRecord, field ringclient.FieldInfo) error {
	chunk := &dumpElemsChunk{}
	seq := int32(0)
	flush := func() error {
		if len(chunk.els) == 0 {
			return nil
		}
		if err := r.writeElemsChunk(entity, field, seq, chunk); err != nil {
			return err
		}
		chunk = &dumpElemsChunk{}
		seq++
		return nil
	}
	pageWhat := fmt.Sprintf("field %q of %s.%s id %q", field.Name, entity.database, entity.bucket, terminalSafeText(entity.id))
	var cursor []byte
	for {
		var page ringclient.ElementPage
		// The closure captures the request, so a retried page is asked for with the same
		// cursor (section 3.5).
		call := func() error {
			answered, err := r.source.StoreList(entity.ctx, entity.database, entity.bucket, entity.id, field.Name, dumpStorePageSize, cursor)
			if err != nil {
				return err
			}
			page = answered
			return nil
		}
		err := r.retryMovedPage(pageWhat, call)
		if documentIsGone(err) {
			// The document was deleted after its List page: the dump keeps the doc record it
			// wrote (a document deleted after the dump passed is still in, section 3.6) and
			// has no store to read. What was already read of this field is still written.
			return flush()
		}
		if bucketLeftDuringTheDump(err, false) {
			// The bucket itself left while this field was read: the NOT_FOUND of a store page
			// is the deleted document above, so anything that reaches here is the bucket's
			// departure, and dumpBucket keeps what was written and warns by name.
			return fmt.Errorf("listing field %q of %s.%s id %q: %w", field.Name, entity.database, entity.bucket, terminalSafeText(entity.id), errBucketLeftDuringTheDump)
		}
		if err != nil {
			return fmt.Errorf("listing field %q of %s.%s id %q: %w", field.Name, entity.database, entity.bucket, terminalSafeText(entity.id), err)
		}
		if err := checkStorePage(field.Type, page); err != nil {
			return fmt.Errorf("reading field %q of %s.%s id %q: %w", field.Name, entity.database, entity.bucket, terminalSafeText(entity.id), err)
		}
		for index, element := range page.Elements {
			if chunk.full(len(element)) {
				if err := flush(); err != nil {
					return err
				}
			}
			chunk.els = append(chunk.els, element)
			chunk.bytes += len(element)
			switch field.Type {
			case dumpHashStoreWord, dumpBTreeStoreWord:
				chunk.keys = append(chunk.keys, page.Keys[index])
			case dumpHeapStoreWord:
				chunk.keys = append(chunk.keys, page.Keys[index])
				chunk.prios = append(chunk.prios, page.Priorities[index])
			}
		}
		if !page.More {
			return flush()
		}
		cursor = page.Cursor
	}
}

// documentIsGone reports whether a store read failed because the document no longer exists,
// which is how a delete that lands between a List page and the read of its stores shows. A
// dump is online, so that is an expected race and not a failure of the run.
func documentIsGone(err error) bool {
	var refusal *ringclient.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound
}

// checkStorePage refuses a page whose keys or priorities do not line up with its elements.
// Restore puts by key, so a page without one key per element cannot be written to the
// file, and reading past the reply's own keys below would be wrong.
func checkStorePage(kind string, page ringclient.ElementPage) error {
	switch kind {
	case dumpHashStoreWord, dumpBTreeStoreWord:
		if len(page.Keys) != len(page.Elements) {
			return fmt.Errorf("the server answered %d elements of a %s field and %d keys, and a %s lists one key per element",
				len(page.Elements), kind, len(page.Keys), kind)
		}
	case dumpHeapStoreWord:
		if len(page.Keys) != len(page.Elements) || len(page.Priorities) != len(page.Elements) {
			return fmt.Errorf("the server answered %d elements of a heapStore field, %d entry ids and %d priorities, and a heapStore lists one of each per element",
				len(page.Elements), len(page.Keys), len(page.Priorities))
		}
	}
	return nil
}

// writeElemsChunk writes one elems record and counts its elements. The keys and priorities
// members are present only for the kinds that have them, so the record's member set is the
// one section 3.1's row lists for that store type.
func (r *ringDumpRun) writeElemsChunk(entity dumpEntityRecord, field ringclient.FieldInfo, seq int32, chunk *dumpElemsChunk) error {
	record := &dumpElems{
		T:     dumpKindElems,
		DB:    entity.database,
		B:     entity.bucket,
		ID:    entity.id,
		Field: field.Name,
		Kind:  field.Type,
		Seq:   seq,
		Els:   chunk.els,
	}
	switch field.Type {
	case dumpHashStoreWord, dumpBTreeStoreWord:
		record.Keys = chunk.keys
	case dumpHeapStoreWord:
		record.Keys, record.Prios = chunk.keys, chunk.prios
	}
	if err := entity.sink.WriteRecord(record); err != nil {
		return fmt.Errorf("writing the elements of field %q of %s.%s id %q: %w", field.Name, entity.database, entity.bucket, terminalSafeText(entity.id), err)
	}
	entity.counters.Elements += int64(len(chunk.els))
	return nil
}

// dumpElemsChunk is one elems record being filled: its elements, the keys and priorities
// the kind carries, and the payload bytes they occupy.
type dumpElemsChunk struct {
	els   []bson.Raw
	keys  []string
	prios []int64
	bytes int
}

// full reports whether an element of next bytes starts a new chunk: at 1000 elements a
// chunk is full, and a chunk that holds something is full when the next element would take
// it past the byte cap. An empty chunk is never full, so the first element always goes in
// however large (section 3.1).
func (c *dumpElemsChunk) full(next int) bool {
	if len(c.els) >= dumpChunkElements {
		return true
	}
	return len(c.els) > 0 && c.bytes+next > dumpChunkBytes
}
