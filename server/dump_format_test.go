package server

import "testing"

// Owner ruling 2026-09-30: every dump manifest has one format_version, 3 since
// the dead section (claim/ack build step 17). A bucket
// no longer chooses its version by what it declares, so what differs between
// buckets is the layout — which sections the manifest carries — and nothing
// else.

// TestEveryDumpManifestIsFormatThree: a plain bucket, one with only an index
// declaration, one with only store fields and one with both all stamp the
// same version, and each manifest carries exactly the sections its bucket
// has.
func TestEveryDumpManifestIsFormatThree(t *testing.T) {
	s := newTestServer(t)
	uniqueEmail := []interface{}{map[string]interface{}{"field": "Email", "unique": true}}
	comments := StoreDeclaration{Name: "Comments", Engine: "fifoStore"}
	both := storesOption(comments)
	both["indices"] = uniqueEmail
	both["placement"] = "whole"
	cases := []struct {
		bucket     string
		options    map[string]interface{}
		indexSpecs int
		stores     int
	}{
		{bucket: "Plain"},
		{bucket: "Indexed", options: map[string]interface{}{"placement": "whole", "indices": uniqueEmail}, indexSpecs: 1},
		{bucket: "Nested", options: storesOption(comments), stores: 1},
		{bucket: "Both", options: both, indexSpecs: 1, stores: 1},
	}
	for _, tc := range cases {
		t.Run(tc.bucket, func(t *testing.T) {
			createTestBucketWithOptions(t, s, "app", tc.bucket, BucketTypeHash, tc.options)
			started, err := s.startDump(rootContext(), DumpStartRequest{Database: "app", Bucket: tc.bucket, AcceptStores: true})
			if err != nil {
				t.Fatalf("starting the dump: %v", err)
			}
			manifest := started.Manifest
			if manifest.FormatVersion != 3 {
				t.Errorf("the manifest is format_version %d, want 3: every manifest is", manifest.FormatVersion)
			}
			if len(manifest.IndexSpecs) != tc.indexSpecs || len(manifest.Stores) != tc.stores {
				t.Errorf("the manifest carries %d index specs and %d stores, want %d and %d",
					len(manifest.IndexSpecs), len(manifest.Stores), tc.indexSpecs, tc.stores)
			}
			if _, err := s.dumpDone(rootContext(), DumpDoneRequest{Database: "app", Bucket: tc.bucket}); err != nil {
				t.Fatalf("releasing the dump: %v", err)
			}
		})
	}
}
