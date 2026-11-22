//go:build linux

package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// processWriteBytes reads write_bytes from /proc/self/io: the bytes this
// process caused to be sent to the storage layer. The kernel charges a page
// cache folio to the process that dirties it, at its full size, so a 2 KB write
// that dirties a 1 MiB folio is charged 1 MiB. That is the amplification
// BUG-0700 is about, and the reason this counter, not the bytes handed to
// write calls, is the measure.
func processWriteBytes(t *testing.T) int64 {
	t.Helper()
	ioFile, err := os.Open("/proc/self/io")
	if err != nil {
		t.Fatalf("opening /proc/self/io: %v", err)
	}
	defer func() {
		if err := ioFile.Close(); err != nil {
			t.Errorf("closing /proc/self/io: %v", err)
		}
	}()
	lines := bufio.NewScanner(ioFile)
	for lines.Scan() {
		if counter, found := strings.CutPrefix(lines.Text(), "write_bytes: "); found {
			writtenBytes, err := strconv.ParseInt(counter, 10, 64)
			if err != nil {
				t.Fatalf("parsing write_bytes %q from /proc/self/io: %v", counter, err)
			}
			return writtenBytes
		}
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("reading /proc/self/io: %v", err)
	}
	t.Fatal("/proc/self/io has no write_bytes line; the kernel was built without task I/O accounting")
	return 0
}

// folioProbe is the shape of the probe in BUG-0700: append small records to a
// sparse, memory-mapped file and msync after each, as Badger's write-ahead log
// does once per acknowledged write.
const (
	folioProbeFileBytes   = 40 << 20
	folioProbeRecordBytes = 2 << 10
	folioProbeSyncs       = 32
)

// unadvisedMsyncCostPerSync measures what one msync of a 2 KB append costs on
// this host and filesystem, for a mapping given no access hint (Badger's
// before BUG-0700), once the writer is rampOffset bytes into the file. The
// region before rampOffset is written sequentially without syncs, which is
// what lets the kernel's readahead grow the folios it allocates for the
// mapping; on a kernel that sizes file folios by the readahead window, the
// later msyncs each write back a whole large folio. It returns bytes per sync.
func unadvisedMsyncCostPerSync(t *testing.T, dir string, rampOffset int) int64 {
	t.Helper()
	probePath := filepath.Join(dir, "bug-0700-folio-probe")
	probeFile, err := os.OpenFile(probePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("creating the folio probe file: %v", err)
	}
	defer func() {
		if err := probeFile.Close(); err != nil {
			t.Errorf("closing the folio probe file: %v", err)
		}
		if err := os.Remove(probePath); err != nil {
			t.Errorf("removing the folio probe file: %v", err)
		}
	}()
	if err := probeFile.Truncate(folioProbeFileBytes); err != nil {
		t.Fatalf("sizing the folio probe file: %v", err)
	}
	mapping, err := unix.Mmap(int(probeFile.Fd()), 0, folioProbeFileBytes, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatalf("mapping the folio probe file: %v", err)
	}
	defer func() {
		if err := unix.Munmap(mapping); err != nil {
			t.Errorf("unmapping the folio probe file: %v", err)
		}
	}()

	record := make([]byte, folioProbeRecordBytes)
	for i := range record {
		record[i] = byte(i)
	}
	offset := 0
	for offset < rampOffset {
		offset += copy(mapping[offset:], record)
	}
	if err := unix.Msync(mapping, unix.MS_SYNC); err != nil {
		t.Fatalf("syncing the folio probe ramp: %v", err)
	}
	before := processWriteBytes(t)
	for range folioProbeSyncs {
		offset += copy(mapping[offset:], record)
		if err := unix.Msync(mapping, unix.MS_SYNC); err != nil {
			t.Fatalf("syncing a folio probe record at offset %d: %v", offset, err)
		}
	}
	return (processWriteBytes(t) - before) / folioProbeSyncs
}

// skipUnlessMsyncAmplifies skips the calling test, with the probe's numbers,
// on a host where an unadvised mapping's msync already costs no more than
// boundPerSync: there, Badger without the BUG-0700 fix does not amplify
// either, so a write-volume test cannot fail and must not be read as a pass.
func skipUnlessMsyncAmplifies(t *testing.T, dir string, rampOffset int, boundPerSync int64) {
	t.Helper()
	costPerSync := unadvisedMsyncCostPerSync(t, dir, rampOffset)
	if costPerSync <= boundPerSync {
		t.Skipf("this host cannot show BUG-0700: an msync of a %d-byte append %d MiB into an unadvised mapping writes %s here "+
			"(bound %s), so the kernel gives this file's mapping small folios and unfixed Badger would pass too; "+
			"verify on a kernel with large folios for file mappings (measured: Linux 7.0, ext4, read_ahead_kb 8192)",
			folioProbeRecordBytes, rampOffset>>20, formatBytes(costPerSync), formatBytes(boundPerSync))
	}
	t.Logf("folio probe: an msync of a %d-byte append %d MiB into an unadvised mapping writes %s on this host (bound %s): this host shows the amplification",
		folioProbeRecordBytes, rampOffset>>20, formatBytes(costPerSync), formatBytes(boundPerSync))
}

func formatBytes(byteCount int64) string {
	if byteCount < 10<<10 {
		return fmt.Sprintf("%d B", byteCount)
	}
	return fmt.Sprintf("%.1f KiB", float64(byteCount)/(1<<10))
}
