package server

import (
	"fmt"
	"io/fs"
	"strings"
	"testing"
)

// These tests are §4(b): a blob directory on its own filesystem is a second
// disk that fills — with the node's largest, least bounded bytes — and the
// join guard and the capacity sampler must read it, not only the data
// directory's.

// splitDisks makes s's blob directory and data directory report different
// fullness, as two filesystems would.
func splitDisks(t *testing.T, s *Server, dataPct, blobPct int) {
	t.Helper()
	blobRoot := s.blobRoot()
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.diskStatfs = func(dir string) (diskStat, error) {
			if dir == blobRoot {
				return diskFixture(blobPct), nil
			}
			return diskFixture(dataPct), nil
		}
	})
}

func TestBlobOnOwnFilesystem(t *testing.T) {
	devices := map[string]uint64{"/data": 1, "/data/blobs": 1, "/mnt": 2, "/mnt/blobs": 2}
	deviceOf := func(dir string) (uint64, error) {
		if dev, ok := devices[dir]; ok {
			return dev, nil
		}
		return 0, fmt.Errorf("stat %s: %w", dir, fs.ErrNotExist)
	}
	for _, tt := range []struct {
		blobDir string
		want    bool
	}{
		{"/data/blobs", false},
		{"/mnt/blobs", true},
		// A blob directory not created yet is on the filesystem its nearest
		// existing parent is on: the default before the first blob write.
		{"/data/blobs-not-yet", false},
		{"/mnt/not/yet", true},
	} {
		got, err := blobOnOwnFilesystem("/data", tt.blobDir, deviceOf)
		if err != nil || got != tt.want {
			t.Errorf("blobOnOwnFilesystem(/data, %s) = %v, %v; want %v", tt.blobDir, got, err, tt.want)
		}
	}
}

func TestJoinGuardReadsTheBlobFilesystem(t *testing.T) {
	s := newTestServer(t)
	s.config.Server.DiskHighWaterPct = 50
	splitDisks(t, s, 10, 92)
	guard := &Cluster{s: s}

	if !guard.diskHeadroomOK() {
		t.Fatal("precondition: with the blob directory on the data filesystem, a 10% data disk is headroom")
	}
	s.blobDiskSeparate = true
	if guard.diskHeadroomOK() {
		t.Fatal("the join guard accepted a join with the blob directory's own filesystem at 92% and the mark at 50%")
	}
	msg := guard.diskFullJoinError().Error()
	if !strings.Contains(msg, s.blobRoot()) || !strings.Contains(msg, "92%") || !strings.Contains(msg, "HOARDDB_DISK_HIGH_WATER") {
		t.Fatalf("the refusal must name the blob directory %s, the 92%% it read and the setting to change, got: %s", s.blobRoot(), msg)
	}

	splitDisks(t, s, 92, 10)
	if msg := guard.diskFullJoinError().Error(); strings.Contains(msg, s.blobRoot()) {
		t.Fatalf("with the data filesystem full, the refusal names the blob directory instead: %s", msg)
	}
}

func TestSamplerReportsTheBlobFilesystem(t *testing.T) {
	s := newTestServer(t)
	s.config.Server.DiskHighWaterPct = 85
	splitDisks(t, s, 10, 92)

	shared := newIdleSampler(t, s)
	shared.sampleNow()
	if _, ok := seriesValue(gatherSampler(t, shared), familyRatio, "resource", resourceBlobDisk); ok {
		t.Fatalf("a blob directory on the data filesystem has its own ratio series; one filesystem is one series")
	}
	if len(shared.last.rows) != len(capacityResources) {
		t.Fatalf("a blob directory on the data filesystem adds a status row: %d rows, want %d", len(shared.last.rows), len(capacityResources))
	}

	s.blobDiskSeparate = true
	cs := newIdleSampler(t, s)
	cs.sampleNow()
	gathered := gatherSampler(t, cs)
	if ratio, ok := seriesValue(gathered, familyRatio, "resource", resourceBlobDisk); !ok || ratio < 0.91 || ratio > 0.93 {
		t.Fatalf("%s{resource=%q} = %v (present %v), want 0.92", familyRatio, resourceBlobDisk, ratio, ok)
	}
	if threshold, ok := seriesValue(gathered, familyThreshold, "resource", resourceBlobDisk); !ok || threshold != 0.85 {
		t.Fatalf("%s{resource=%q} = %v (present %v), want the guard's mark 0.85", familyThreshold, resourceBlobDisk, threshold, ok)
	}
	for _, family := range []string{"hoarddb_node_blob_disk_used_bytes", "hoarddb_node_blob_disk_size_bytes"} {
		if len(gathered[family]) != 1 {
			t.Fatalf("%s has %d series, want 1", family, len(gathered[family]))
		}
	}
	if row := rowFor(t, cs, resourceBlobDisk); row.state != stateRed {
		t.Fatalf("the blob_disk row reads %v at 0.92 over a 0.85 mark, want red", row.state)
	}
	if row := rowFor(t, cs, resourceDisk); row.state != stateOK {
		t.Fatalf("the disk row reads %v at 0.10, want ok: the data filesystem is not the full one", row.state)
	}
	if got := len(cs.healthCapacity(cs.last.sampledAt).Rows); got != len(capacityResources)+1 {
		t.Fatalf("status shows %d capacity rows, want %d (the four, and blob_disk)", got, len(capacityResources)+1)
	}
}
