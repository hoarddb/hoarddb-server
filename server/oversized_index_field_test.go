package server

import (
	"strings"
	"testing"
)

// TestOversizedIndexedFieldRefusalSaysWhatToDo defends AGENTS.md rule 3 for
// BUG-0181: an index entry's key holds the field's value, and the store caps a
// key at about 65,000 bytes. The refusal must name the field and the document,
// say nothing was written, say what to do, and keep the key's hex dump out of
// the client text.
func TestOversizedIndexedFieldRefusalSaysWhatToDo(t *testing.T) {
	s := newTestServer(t)
	createTestBucketWithOptions(t, s, "app", "notes", BucketTypeHash, map[string]interface{}{"indices": []interface{}{"body"}})
	oversized := strings.Repeat("x", 3<<20)

	cases := []struct {
		name  string
		write func() (success bool, message string, err error)
	}{
		{"insert", func() (bool, string, error) {
			resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "notes", Key: "n1", Data: map[string]interface{}{"body": oversized}})
			if resp == nil {
				return false, "", err
			}
			return resp.Success, resp.Message, err
		}},
		{"batch insert", func() (bool, string, error) {
			resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "notes",
				Data: []map[string]interface{}{{"_id_": "n1", "body": oversized}}})
			if resp == nil {
				return false, "", err
			}
			return resp.Success, resp.Message, err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			success, message, err := tc.write()
			if err != nil {
				t.Fatalf("write error = %v, want an in-band refusal", err)
			}
			if success {
				t.Fatal("a 3 MiB indexed field was accepted")
			}
			for _, want := range []string{
				`"body"`,
				`"n1"`,
				"nothing was written",
				"65,000",
				"store the long text in a field that is not indexed",
			} {
				if !strings.Contains(message, want) {
					t.Errorf("refusal %q does not contain %q", message, want)
				}
			}
			if hexRun := longHexRun.FindString(message); hexRun != "" {
				t.Errorf("refusal shows the storage key %q to the client; it belongs in the log", hexRun)
			}
		})
	}
}
