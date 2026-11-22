package cli

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The two bounds of --parallel (section 3.3).
const (
	// ringDumpDefaultParallel is how many entities of one List page a dump reads at once
	// when --parallel is not given. Eight keeps a page of the ring busy without eight
	// buffers of dumpSpillBytes on a small server.
	ringDumpDefaultParallel = 8
	// ringDumpMaxParallel is the most entities a dump reads at once. Each one holds a
	// buffer of up to dumpSpillBytes and may hold a spill file, so the flag bounds both the
	// memory and the file descriptors a dump adds to one bucket.
	ringDumpMaxParallel = 64
)

// dumpSpillerCreated, when a test sets it, receives the spiller of the run, so the
// measurement tests of section 3.3 can read its peak and live counts after the run. It is
// nil in every shipped run, like dumpSpillBytes.
var dumpSpillerCreated func(*dumpSpiller)

// dumpBeforeEntityCopy, when a test sets it, runs in the copier just before the entity at
// index is copied into the file, so a test can hold the copier while the workers run ahead
// and check how many buffers exist then. It is nil in every shipped run, like
// dumpSpillerCreated.
var dumpBeforeEntityCopy func(index int)

// dumpEntryPage writes the entities of one List page to the file in page order: one at a
// time straight through when --parallel is 1, and one buffer per entity otherwise. The
// page order is the order the file gets whatever the number is (section 3.3).
func (r *ringDumpRun) dumpEntryPage(database, bucket string, declaration ringclient.BucketInfo, storeFields []string, entries []ringclient.Entry) error {
	if r.parallel <= 1 {
		return r.dumpEntriesSerial(database, bucket, declaration, storeFields, entries)
	}
	return r.dumpEntriesParallel(database, bucket, declaration, storeFields, entries)
}

// dumpEntriesSerial reads the page's entities one after another, each record going straight
// into the dump writer. It is --parallel 1: no buffer, no spill file, no goroutine, exactly
// what the dump did before the flag existed.
func (r *ringDumpRun) dumpEntriesSerial(database, bucket string, declaration ringclient.BucketInfo, storeFields []string, entries []ringclient.Entry) error {
	for _, entry := range entries {
		// The counters are local to the entity and added to the run's once it is written,
		// so the serial path and a worker count the same way: only the sink differs.
		counts := dumpCounters{}
		entity := dumpEntityRecord{
			ctx:         r.ctx,
			database:    database,
			bucket:      bucket,
			id:          entry.ID,
			body:        entry.Body,
			declaration: declaration,
			storeFields: storeFields,
			sink:        r.writer,
			counters:    &counts,
		}
		err := r.readEntity(entity)
		// The counts are added even when the entity fails: records written straight to the
		// file are in it, so the summary must count them or a run that goes on after a
		// dropped bucket would leave a file that fails its own check.
		r.counters.Entities += counts.Entities
		r.counters.Elements += counts.Elements
		if err != nil {
			return err
		}
	}
	return nil
}

// dumpEntriesParallel reads the page's entities with r.parallel workers, each into its own
// dumpEntityBuffer, and copies the buffers into the dump writer in page order. The file
// and its running checksum are appended to and never rewound, so the file is the one a
// --parallel 1 dump of a quiet ring writes (section 3.3).
//
// A worker starts an entity only when fewer than r.parallel buffers exist, which is what
// bounds the memory: the dispatch loop takes one of the r.parallel slots before it hands an
// entity to a worker, and the copier releases a slot only after that entity's buffer has
// been copied (or closed on failure). The slots are taken in index order, 0, 1, 2, ...
//
// That ordering is why the copier cannot deadlock: when it waits for index i it has
// already copied 0..i-1 and released their slots, so every slot still held belongs to an
// index above i-1, and there are at most r.parallel of them. The loop therefore reached
// index i before it could block (a block needs r.parallel slots held by indices below the
// one it wants, and the copier has released all of those), so index i was dispatched and
// its buffer is on its way. A worker waits only for its own reads, never for the copier or
// for another worker, so the wait graph has no cycle: the copier's progress releases the
// slots the loop waits for, and the workers' progress closes the channels the copier waits
// for.
func (r *ringDumpRun) dumpEntriesParallel(database, bucket string, declaration ringclient.BucketInfo, storeFields []string, entries []ringclient.Entry) error {
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	slots := make(chan struct{}, r.parallel)
	jobs := make(chan int)
	buffers := make([]*dumpEntityBuffer, len(entries))
	errs := make([]error, len(entries))
	// Every entity's finished channel exists before any goroutine starts: the copier may
	// reach an index the dispatch loop has not handed out yet (the argument above says it
	// then holds no slot, so the loop is free to dispatch it), and receiving from a nil
	// channel would wait forever.
	finished := make([]chan struct{}, len(entries))
	for index := range finished {
		finished[index] = make(chan struct{})
	}

	// The workers: r.parallel goroutines for the whole page, never one per entity.
	var workers sync.WaitGroup
	workers.Add(r.parallel)
	for range r.parallel {
		go func() {
			defer workers.Done()
			for index := range jobs {
				errs[index] = r.readEntity(dumpEntityRecord{
					ctx:         ctx,
					database:    database,
					bucket:      bucket,
					id:          entries[index].ID,
					body:        entries[index].Body,
					declaration: declaration,
					storeFields: storeFields,
					sink:        buffers[index],
					counters:    &buffers[index].counts,
				})
				close(finished[index])
			}
		}()
	}

	// The copier: one goroutine for the page. It writes the buffers in index order and adds
	// their counts only once every byte of them is in the file.
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		for index := range entries {
			select {
			case <-finished[index]:
			case <-ctx.Done():
				return
			}
			if dumpBeforeEntityCopy != nil {
				dumpBeforeEntityCopy(index)
			}
			if errs[index] == nil {
				errs[index] = buffers[index].copyTo(r.writer)
				if errs[index] == nil {
					r.counters.Entities += buffers[index].counts.Entities
					r.counters.Elements += buffers[index].counts.Elements
				}
			}
			if errs[index] != nil {
				// The first failure stops the page: a dump does not read on past a failed
				// entity (section 3.5), and every worker must return.
				cancel()
				return
			}
			<-slots
		}
	}()

dispatch:
	for index := range entries {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break dispatch
		}
		buffers[index] = newDumpEntityBuffer(r.spiller)
		select {
		case jobs <- index:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()
	<-copied

	failure := dumpFirstEntityError(errs)
	if failure == nil {
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			failure = fmt.Errorf("reading the entities of %s.%s: %w", database, bucket, ctxErr)
		}
	}
	if failure == nil {
		return nil
	}
	return dumpJoinCleanup(failure, r.closeEntityBuffers(buffers))
}

// dumpFirstEntityError is the failure of the lowest index that has one: several workers can
// fail at once, and the file's first missing entity is the one the operator must read
// about.
func dumpFirstEntityError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// closeEntityBuffers closes every buffer the page made and removes its spill file. It is
// safe for a buffer the copier already closed, so the failure path can call it for the
// whole slice without knowing how far the copier got.
func (r *ringDumpRun) closeEntityBuffers(buffers []*dumpEntityBuffer) error {
	var closeErr error
	for _, buffer := range buffers {
		if buffer == nil {
			continue
		}
		if err := buffer.close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

// dumpJoinCleanup keeps the entity's failure first, because that is the reason the run
// stopped, and adds what could not be cleaned up after it.
func dumpJoinCleanup(failure, cleanup error) error {
	if cleanup == nil {
		return failure
	}
	return errors.Join(failure, cleanup)
}
