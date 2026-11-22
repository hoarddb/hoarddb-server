package server

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

// leaveFailedCreate makes a create fail after the registry claimed the name.
// The create takes its claim back (BUG-0257), so the name must read as free.
func leaveFailedCreate(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	createTestBucket(t, s, database, "existing", BucketTypeHash)
	refusedIndex := map[string]interface{}{"indices": []interface{}{map[string]interface{}{"field": 7}}}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeBTree, Options: refusedIndex}); err == nil && resp.Success {
		t.Fatalf("a create with a malformed index declaration was accepted: %+v", resp)
	}
	if left := lookupBucket(s, database, bucket); left != nil {
		t.Fatalf("precondition: the failed create left a registry entry under %q (BUG-0257)", bucket)
	}
}

// restoreOverFailedCreateCases are the restore variants aimed at the name of a
// failed create.
var restoreOverFailedCreateCases = []struct {
	name   string
	dryRun bool
	drop   bool
}{
	{name: "drop", drop: true},
	{name: "merge"},
	{name: "dry run", dryRun: true},
	{name: "dry run with drop", dryRun: true, drop: true},
}

// restoreChildEnv names the case a child process of
// TestRestoreOverAFailedCreateTreatsTheNameAsFree runs.
const restoreChildEnv = "HOARDDB_TEST_RESTORE_OVER_FAILED_CREATE"

// TestRestoreOverAFailedCreateTreatsTheNameAsFree: a root restore aimed at the
// name of a failed create must not reach the leftover's nil engine, and must
// answer as it would for a name that does not exist. The dump is a btree
// because the leftover carries the type its create asked for. Each variant
// runs in a child process, because the fault is a panic that leaves a bucket
// lock held: in this process it would hang the server's own cleanup instead of
// failing.
func TestRestoreOverAFailedCreateTreatsTheNameAsFree(t *testing.T) {
	for _, tc := range restoreOverFailedCreateCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRestoreOverAFailedCreateChild$", "-test.count=1")
			child.Env = append(os.Environ(), restoreChildEnv+"="+tc.name)
			output, err := child.CombinedOutput()
			if err != nil {
				t.Fatalf("restore %s over a failed create did not finish cleanly: %v\n%s", tc.name, err, output)
			}
		})
	}
}

// TestRestoreOverAFailedCreateChild is the body of one variant; it does
// nothing unless the parent test started it.
func TestRestoreOverAFailedCreateChild(t *testing.T) {
	name := os.Getenv(restoreChildEnv)
	if name == "" {
		t.Skip("run by TestRestoreOverAFailedCreateTreatsTheNameAsFree")
	}
	const database, bucket = "app", "failed"
	for _, tc := range restoreOverFailedCreateCases {
		if tc.name != name {
			continue
		}
		s := newTestServer(t)
		leaveFailedCreate(t, s, database, bucket)
		start, err := s.startRestore(rootContext(), RestoreStartRequest{
			Database: database, Bucket: bucket, FormatVersion: dumpManifestFormatVersion,
			StoreType: BucketTypeBTree, Bytes: 10, Drop: tc.drop, DryRun: tc.dryRun,
		})
		if err != nil {
			t.Fatalf("restore %s over a failed create: %v, want the answer for a free name", tc.name, err)
		}
		if !start.Created || start.Dropped {
			t.Errorf("restore %s answered created=%v dropped=%v, want created and not dropped (the name holds no bucket)", tc.name, start.Created, start.Dropped)
		}
		if tc.dryRun {
			return
		}
		if _, err := s.InsertCtx(rootContext(), InsertRequest{Database: database, Bucket: bucket, Key: "k", Data: map[string]interface{}{"v": 1}}); err != nil {
			t.Errorf("insert after the restore created the bucket: %v", err)
		}
		return
	}
	t.Fatalf("no restore variant is named %q", name)
}
