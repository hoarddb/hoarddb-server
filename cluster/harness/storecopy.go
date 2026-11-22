package harness

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// storeDir is where node id keeps its Badger store, and storeCopyDir where CopyStore puts the copy.
func (c *Cluster) storeDir(id string) string { return filepath.Join(c.DataDir(id), "badger") }

func (c *Cluster) storeCopyDir(id string) string {
	return filepath.Join(filepath.Dir(c.DataDir(id)), "store-copy")
}

func (c *Cluster) mustBeKilled(id, what string) {
	c.tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.members[id]; !ok || m.node == nil || !m.killed {
		c.tb.Fatalf("harness: %s needs node %q killed: a running node has its store open", what, id)
	}
}

// CopyStore copies the Badger store of the killed node id aside, replacing an earlier copy. A
// kill closes the store cleanly, so the copy is a whole store as of the kill. The node's log
// and its Raft state are not copied.
func (c *Cluster) CopyStore(id string) {
	c.tb.Helper()
	c.mustBeKilled(id, "CopyStore")
	if err := os.RemoveAll(c.storeCopyDir(id)); err != nil {
		c.tb.Fatalf("harness: clearing the store copy of %s: %v", id, err)
	}
	if err := os.CopyFS(c.storeCopyDir(id), os.DirFS(c.storeDir(id))); err != nil {
		c.tb.Fatalf("harness: copying the store of %s: %v", id, err)
	}
}

// RestoreStore puts the store of the killed node id back to its last CopyStore and keeps its
// log and its Raft state as they are. The node then restarts with a log that holds records its
// store has not applied: what a power cut that loses the store's newest writes leaves, made by
// hand because a kill in this harness leaves the store whole.
func (c *Cluster) RestoreStore(id string) {
	c.tb.Helper()
	c.mustBeKilled(id, "RestoreStore")
	if _, err := os.Stat(c.storeCopyDir(id)); err != nil {
		c.tb.Fatalf("harness: node %s has no store copy to restore: %v", id, err)
	}
	if err := os.RemoveAll(c.storeDir(id)); err != nil {
		c.tb.Fatalf("harness: removing the store of %s: %v", id, err)
	}
	if err := os.CopyFS(c.storeDir(id), os.DirFS(c.storeCopyDir(id))); err != nil {
		c.tb.Fatalf("harness: restoring the store of %s: %v", id, err)
	}
}

// HoldBootReplays keeps every pair that a restart defers (its replay waits for a bucket's
// declaration, BUG-0760) deferred until release is called: the background passes that would
// finish the replays wait. Call it while the nodes that will restart are killed; a node that is
// running must have no replay waiting. The hold is process-wide, so one cluster uses it at a
// time. release may be called more than once, and the test's cleanup calls it.
func (c *Cluster) HoldBootReplays() (release func()) {
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	wire.HoldBootReplaysForTest(func() { <-gate })
	c.mu.Lock()
	c.heldReplays = true
	c.mu.Unlock()
	// Registered after the cluster's own cleanup, so it runs first: a node's Close waits for
	// its replay pass, which waits here.
	c.tb.Cleanup(release)
	return release
}

// KillKeepingLiveStore copies node id's live store directory, then kills the node, then puts the
// copy in place of the store, so the restart finds the store as a kill -9 at the copy instant
// leaves it. A kill in this harness is a clean close, which flushes every memtable into tables
// and leaves nothing for the vouched cut to discard; the copy is what a kill -9 leaves instead:
// on Linux a read of a file sees the page cache, Badger's memtable logs and value log are
// shared mappings whose dirty pages a read sees, and a kill -9 keeps the page cache.
//
// Call it with no client write of the test in flight. The copy is taken twice and the two
// compared byte for byte; when they differ (a background commit, a rotation or a flush ran
// during the copy) or a file vanished, both are discarded and the copy is tried again, at most
// five times 200 ms apart. The node's log and Raft state stay as the kill leaves them.
func (c *Cluster) KillKeepingLiveStore(id string) {
	c.tb.Helper()
	c.Node(id) // fails the test when id is not running
	live, image, check := c.storeDir(id), c.storeCopyDir(id), c.storeCopyDir(id)+"-check"
	var lastErr error
	held := false
	for attempt := range 5 {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		if lastErr = copyLiveStore(live, image); lastErr == nil {
			lastErr = copyLiveStore(live, check)
		}
		if lastErr != nil {
			c.tb.Logf("harness: the live copy of %s's store, attempt %d: %v", id, attempt+1, lastErr)
			continue
		}
		if lastErr = sameStoreCopies(image, check); lastErr == nil {
			held = true
			break
		}
		c.tb.Logf("harness: the live copy of %s's store, attempt %d: %v", id, attempt+1, lastErr)
	}
	if !held {
		c.tb.Fatalf("SCENARIO NOT REACHED: the store of %s did not hold still for a copy: %v", id, lastErr)
	}
	c.Kill(id)
	if err := os.RemoveAll(live); err != nil {
		c.tb.Fatalf("harness: removing the store of %s after its kill: %v", id, err)
	}
	if err := os.Rename(image, live); err != nil {
		c.tb.Fatalf("harness: putting the live copy of %s in place of its store: %v", id, err)
	}
	if err := os.RemoveAll(check); err != nil {
		c.tb.Fatalf("harness: removing the second copy of the store of %s: %v", id, err)
	}
}

// liveStoreMarker is the vouched cut's marker file (storage's vouchedFileName).
const liveStoreMarker = "VOUCHED"

// copyLiveStore copies the store directory src, running, into dst, which it empties first, and
// fails when the copied marker is not one the open reads. The marker is copied first: it only moves up while the logs and tables only grow, so a copy whose
// marker is older than its logs is one a crash can leave, and the reverse is not.
func copyLiveStore(src, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return fmt.Errorf("clearing %s: %w", dst, err)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", dst, err)
	}
	// A ring store always has the marker: this build writes it before Badger creates a file, and
	// refuses a store without one (storage.VouchedMarkerMissingError). A directory without it is
	// not a ring store, and a copy of it would be refused at the restart.
	if err := copySparseFile(filepath.Join(src, liveStoreMarker), filepath.Join(dst, liveStoreMarker)); err != nil {
		return fmt.Errorf("copying the marker of %s, which every ring store holds: %w", src, err)
	}
	// The marker is two slots written in place (format 2): a copy read during an advance holds the
	// slot being written torn, and the open then uses the other slot, the marker before the
	// advance, which a crash can leave too. A copy the open would refuse is not a crash's state.
	if _, _, _, err := storage.ReadVouchedMarker(dst); err != nil {
		return fmt.Errorf("the marker copied from %s is not one the open reads: %w", src, err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("listing %s: %w", src, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || name == liveStoreMarker || name == "LOCK" {
			continue
		}
		if err := copySparseFile(filepath.Join(src, name), filepath.Join(dst, name)); err != nil {
			return err
		}
	}
	return nil
}

// liveCopyChunk is the unit copySparseFile reads, writes or skips: the value log is a sparse
// 256 MiB file, and a hole stays a hole.
const liveCopyChunk = 1 << 20

// appendOnlyLog reports a memtable log or a value log: Badger preallocates both and only appends,
// so past the first chunk of zeros nothing was written, and a copy stops reading there. That
// keeps the copy to the bytes written, short enough for a store that is written every few
// hundred milliseconds to hold still for two copies.
func appendOnlyLog(name string) bool {
	return strings.HasSuffix(name, ".mem") || strings.HasSuffix(name, ".vlog")
}

func copySparseFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening %s for the live copy: %w", src, err)
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	info, err := in.Stat()
	if err != nil {
		return fmt.Errorf("reading the size of %s: %w", src, err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", dst, err)
	}
	chunk := make([]byte, liveCopyChunk)
	zero := make([]byte, liveCopyChunk)
	for offset := int64(0); offset < info.Size(); offset += liveCopyChunk {
		n, err := in.ReadAt(chunk, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return errors.Join(fmt.Errorf("reading %s at %d: %w", src, offset, err), out.Close())
		}
		if bytes.Equal(chunk[:n], zero[:n]) {
			if appendOnlyLog(src) {
				break
			}
			continue
		}
		if _, err := out.WriteAt(chunk[:n], offset); err != nil {
			return errors.Join(fmt.Errorf("writing %s at %d: %w", dst, offset, err), out.Close())
		}
	}
	if err := out.Truncate(info.Size()); err != nil {
		return errors.Join(fmt.Errorf("sizing %s: %w", dst, err), out.Close())
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dst, err)
	}
	return nil
}

// sameStoreCopies reports, as an error, the first difference between two copies of a store.
func sameStoreCopies(a, b string) error {
	listA, err := os.ReadDir(a)
	if err != nil {
		return fmt.Errorf("listing %s: %w", a, err)
	}
	listB, err := os.ReadDir(b)
	if err != nil {
		return fmt.Errorf("listing %s: %w", b, err)
	}
	if len(listA) != len(listB) {
		return fmt.Errorf("the two copies hold %d and %d files", len(listA), len(listB))
	}
	for i, entry := range listA {
		if listB[i].Name() != entry.Name() {
			return fmt.Errorf("the two copies differ in their files: %s and %s", entry.Name(), listB[i].Name())
		}
		if err := sameFile(filepath.Join(a, entry.Name()), filepath.Join(b, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// sameFile compares two files chunk by chunk, so a sparse value log is never read whole into memory.
func sameFile(a, b string) (err error) {
	fileA, err := os.Open(a)
	if err != nil {
		return fmt.Errorf("opening %s: %w", a, err)
	}
	defer func() { err = errors.Join(err, fileA.Close()) }()
	fileB, err := os.Open(b)
	if err != nil {
		return fmt.Errorf("opening %s: %w", b, err)
	}
	defer func() { err = errors.Join(err, fileB.Close()) }()
	infoA, err := fileA.Stat()
	if err != nil {
		return fmt.Errorf("reading the size of %s: %w", a, err)
	}
	infoB, err := fileB.Stat()
	if err != nil {
		return fmt.Errorf("reading the size of %s: %w", b, err)
	}
	if infoA.Size() != infoB.Size() {
		return fmt.Errorf("the two copies' %s differ in size: a write ran during the copy", filepath.Base(a))
	}
	chunkA, chunkB, zero := make([]byte, liveCopyChunk), make([]byte, liveCopyChunk), make([]byte, liveCopyChunk)
	for {
		n, errA := io.ReadFull(fileA, chunkA)
		m, errB := io.ReadFull(fileB, chunkB)
		if n != m || !bytes.Equal(chunkA[:n], chunkB[:m]) {
			return fmt.Errorf("the two copies' %s differ: a write, a rotation or a flush ran during the copy", filepath.Base(a))
		}
		if appendOnlyLog(a) && bytes.Equal(chunkA[:n], zero[:n]) {
			return nil // both copies stopped here (copySparseFile)
		}
		endA := errors.Is(errA, io.EOF) || errors.Is(errA, io.ErrUnexpectedEOF)
		endB := errors.Is(errB, io.EOF) || errors.Is(errB, io.ErrUnexpectedEOF)
		if errA != nil && !endA {
			return fmt.Errorf("reading %s: %w", a, errA)
		}
		if errB != nil && !endB {
			return fmt.Errorf("reading %s: %w", b, errB)
		}
		if endA || endB {
			if endA != endB {
				return fmt.Errorf("the two copies' %s differ in size", filepath.Base(a))
			}
			return nil
		}
	}
}
