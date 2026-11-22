package server

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// TestTamperedFieldBlobNamesTheDocumentFieldAndElement is the blobStore field
// half of the tampered-chunk claim: the error names the bucket, the document,
// the field and the element, and is the corruption error.
func TestTamperedFieldBlobNamesTheDocumentFieldAndElement(t *testing.T) {
	s, _ := newSealedKVFaultServer(t, 0x10)
	if resp := createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Photos", Engine: "blobStore"}); !resp.Success {
		t.Fatalf("creating Post: %s", resp.Message)
	}
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1: %+v, %v", resp, err)
	}
	putPhoto(t, s, "cover", sealedTestContent())
	payload := onlyPayload(t, s)
	onDisk, err := os.ReadFile(payload)
	if err != nil {
		t.Fatalf("reading the payload: %v", err)
	}
	onDisk[len(onDisk)-1] ^= 0x01
	if err := os.WriteFile(payload, onDisk, 0o600); err != nil {
		t.Fatalf("tampering with the payload: %v", err)
	}

	_, err = s.FieldGet(FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: "cover"})
	if !errors.Is(err, sealed.ErrBlobCorrupt) {
		t.Fatalf("want ErrBlobCorrupt, got: %v", err)
	}
	for _, want := range []string{"app.Post", "p1", "Photos", "cover", "Nothing was returned"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}
