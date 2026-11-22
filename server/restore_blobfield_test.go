package server

import (
	"errors"
	"strings"
	"testing"
)

// TestRestoreDropRefusesABucketWithBlobFieldElements is the second
// half of the blob-field restore rule: a blobStore field's elements are in no dump and restore skips them,
// so --drop recreating a bucket that declares one (or restoring a dump that
// declares one over an existing bucket) destroys elements nothing can put
// back. The refusal must come from either side, in a dry run as well, and
// must leave the elements readable.
func TestRestoreDropRefusesABucketWithBlobFieldElements(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	createTestBucket(t, s, "app", "Other", BucketTypeHash)
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	putPhoto(t, s, "cover", []byte("irreplaceable photo bytes"))

	assertPhotoIntact := func(t *testing.T) {
		t.Helper()
		if got := getPhoto(t, s, "cover"); !got.Found || string(got.Content) != "irreplaceable photo bytes" {
			t.Fatalf("a refused --drop must leave Photos[cover] readable; got found=%v content=%q", got.Found, got.Content)
		}
	}
	blobStores := []DumpStore{{Name: "Photos", Engine: "blob"}}

	for _, tc := range []struct {
		name    string
		request RestoreStartRequest
		want    string
	}{
		{
			name:    "existing bucket declares a blob field",
			request: RestoreStartRequest{Database: "app", Bucket: "Post"},
			want:    "Photos",
		},
		{
			name:    "existing bucket declares a blob field, dry run",
			request: RestoreStartRequest{Database: "app", Bucket: "Post", DryRun: true},
			want:    "Photos",
		},
		{
			name:    "dump declares a blob field",
			request: RestoreStartRequest{Database: "app", Bucket: "Other", Stores: blobStores},
			want:    "Photos",
		},
		{
			name:    "dump declares a blob field, dry run",
			request: RestoreStartRequest{Database: "app", Bucket: "Other", Stores: blobStores, DryRun: true},
			want:    "Photos",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := tc.request
			request.FormatVersion, request.StoreType, request.Drop = 2, BucketTypeHash, true
			_, err := s.startRestore(ctx, request)
			if !errors.Is(err, ErrRestoreDropBlobDataLoss) {
				t.Fatalf("restore --drop = %v, want a refusal wrapping ErrRestoreDropBlobDataLoss", err)
			}
			for _, part := range []string{tc.want, "drop bucket"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("the refusal %q does not say %q", err, part)
				}
			}
			assertPhotoIntact(t)
		})
	}
}
