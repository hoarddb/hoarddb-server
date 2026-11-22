//go:build linux

package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// The BUG-0700 workload. The write-ahead log of a 64 MiB memtable is a 128 MiB
// mapping; the first 24 MiB of it are written without syncs, the way a burst
// of concurrent writes fills it between group syncs, and then each durable
// write appends about 1 KB and syncs.
const (
	amplificationMemtableMB      = 64
	amplificationRampBytes       = 24 << 20
	amplificationRampValueBytes  = 1 << 10
	amplificationRampBatchValues = 512
	amplificationDurableWrites   = 200
	// The bound from the bug file: a durable write of about 1 KB may cost a
	// few pages of the log, its metadata and the value log's header page, not
	// a large folio. Unfixed, the dev host measured 72-85 KB per acknowledged
	// write end to end, and about 1 MiB per write at this offset.
	amplificationBoundPerWrite = 16 << 10
)

// TestAcknowledgedWritesDoNotAmplifyDiskWrites defends BUG-0700: every
// acknowledged write is made durable by DB.Sync, an msync of the memory-mapped
// memtable log. On a kernel that backs file mappings with readahead-sized
// large folios, each msync wrote back a whole folio, up to 1 MiB or more, for
// the few KB the write dirtied. The fix advises the kernel that Badger's log
// mappings are accessed at random (MADV_RANDOM), which keeps their folios at
// one page.
//
// It measures the bytes this process sends to the device per durable write
// (write_bytes in /proc/self/io) and holds them under a bound. A host whose
// kernel gives the mapping small folios cannot show the bug, so a probe
// classifies the host first and the test skips there.
func TestAcknowledgedWritesDoNotAmplifyDiskWrites(t *testing.T) {
	storeDir := diskBackedTempDir(t)
	skipUnlessMsyncAmplifies(t, storeDir, amplificationRampBytes, amplificationBoundPerWrite)

	kv, err := OpenBadgerKV(storeDir, BadgerTuning{MemTableMB: amplificationMemtableMB}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	}()

	rampValue := bytes.Repeat([]byte("r"), amplificationRampValueBytes)
	rampBatches := amplificationRampBytes / (amplificationRampValueBytes * amplificationRampBatchValues)
	for batchIndex := range rampBatches {
		rampBatch := kv.NewBatch()
		for valueIndex := range amplificationRampBatchValues {
			rampKey := fmt.Appendf(nil, "bug-0700-ramp-%04d-%04d", batchIndex, valueIndex)
			if err := rampBatch.Set(rampKey, rampValue); err != nil {
				t.Fatalf("staging ramp write %d of batch %d: %v", valueIndex, batchIndex, err)
			}
		}
		if err := rampBatch.Commit(false); err != nil {
			t.Fatalf("committing ramp batch %d: %v", batchIndex, err)
		}
	}
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the ramp: %v", err)
	}

	durableValue := bytes.Repeat([]byte("d"), 1<<10)
	before := processWriteBytes(t)
	for writeIndex := range amplificationDurableWrites {
		durableBatch := kv.NewBatch()
		if err := durableBatch.Set(fmt.Appendf(nil, "bug-0700-durable-%04d", writeIndex), durableValue); err != nil {
			t.Fatalf("staging durable write %d: %v", writeIndex, err)
		}
		if err := durableBatch.Commit(true); err != nil {
			t.Fatalf("committing durable write %d: %v", writeIndex, err)
		}
	}
	writtenPerWrite := (processWriteBytes(t) - before) / amplificationDurableWrites

	if writtenPerWrite > amplificationBoundPerWrite {
		t.Fatalf("each durable write of %d bytes sent %s to the device (bound %s, %d writes): "+
			"the msync of Badger's memory-mapped log is writing back large page-cache folios; "+
			"its log mappings must be advised MADV_RANDOM at open and after every remap (BUG-0700)",
			len(durableValue), formatBytes(writtenPerWrite), formatBytes(amplificationBoundPerWrite), amplificationDurableWrites)
	}
	t.Logf("each durable write of %d bytes sent %s to the device (bound %s, %d writes)",
		len(durableValue), formatBytes(writtenPerWrite), formatBytes(amplificationBoundPerWrite), amplificationDurableWrites)
}
