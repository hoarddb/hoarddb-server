package server

import (
	"fmt"
	"testing"
)

// sortedPage returns one page of a bucket ordered by CreatedAt ascending, as
// the keys it contains. Ordering by a timestamp with skip/limit is the
// commonest paging workload there is, and it is the one `sort` makes look
// reliable.
func sortedPage(t *testing.T, s *Server, database, bucket string, skip, limit int) []string {
	t.Helper()
	resp, err := s.Scan(ScanRequest{
		Database: database,
		Bucket:   bucket,
		Sort:     map[string]int{"CreatedAt": 1},
		Skip:     skip,
		Limit:    limit,
	})
	if err != nil {
		t.Fatalf("scan skip=%d limit=%d: %v", skip, limit, err)
	}
	keys := make([]string, 0, len(resp.Documents))
	for _, doc := range decodedDocuments(t, resp.Documents) {
		keys = append(keys, doc["_id_"].(string))
	}
	return keys
}

// seedSortedBucket fills a hash bucket with keys doc-00..doc-(n-1) whose
// CreatedAt is 10, 20, 30, … — spaced so a later write can be placed
// unambiguously between two existing documents.
func seedSortedBucket(t *testing.T, s *Server, database, bucket string, n int) {
	t.Helper()
	createTestBucket(t, s, database, bucket, BucketTypeHash)
	for i := 0; i < n; i++ {
		insertScanDoc(t, s, database, bucket, fmt.Sprintf("doc-%02d", i),
			map[string]interface{}{"CreatedAt": float64((i + 1) * 10)})
	}
}

// TestSortedPagingIsNotStableAcrossAConcurrentWrite is a characterisation
// test for DOC-26: it pins what sorted paging does today so that a change to
// it fails here and forces the "limitations" user-docs page and the "hql-spec" user-docs page
// to change with it.
//
// `sort` makes skip/limit paging look reliable, which is strictly worse than
// the unsorted case: an ordering the caller can see is an ordering the caller
// trusts. There is no server-side cursor and no snapshot — Scan re-walks the
// bucket, sorts the whole matched set and only then applies skip/limit
// (scan.go) — so a write that lands between two page requests renumbers every
// document after it.
//
// Both directions are asserted, because they are different bugs to the caller:
// an insert ahead of the boundary pushes an already-returned document onto the
// next page (a DUPLICATE, and an importer double-writes), and a delete ahead
// of the boundary pulls an unseen document back onto the previous page (a
// SKIP, and an exporter loses a row silently).
func TestSortedPagingIsNotStableAcrossAConcurrentWrite(t *testing.T) {
	const (
		database = "app"
		pageSize = 5
		seeded   = 10
	)

	t.Run("an insert before the boundary duplicates a document", func(t *testing.T) {
		s := newTestServer(t)
		seedSortedBucket(t, s, database, "orders", seeded)

		firstPage := sortedPage(t, s, database, "orders", 0, pageSize)
		if len(firstPage) != pageSize {
			t.Fatalf("page one returned %d documents, want %d", len(firstPage), pageSize)
		}

		// CreatedAt 25 sorts between doc-01 (20) and doc-02 (30), i.e. inside
		// the page already returned, so every later document shifts one place
		// down the ordering.
		insertScanDoc(t, s, database, "orders", "doc-late",
			map[string]interface{}{"CreatedAt": float64(25)})

		secondPage := sortedPage(t, s, database, "orders", pageSize, pageSize)

		seen := make(map[string]bool, len(firstPage))
		for _, key := range firstPage {
			seen[key] = true
		}
		var duplicated []string
		for _, key := range secondPage {
			if seen[key] {
				duplicated = append(duplicated, key)
			}
		}
		if len(duplicated) == 0 {
			t.Fatalf("sorted paging survived an insert before the page boundary: "+
				"page one %v, page two %v. If that is now the real behaviour, "+
				"docs/user/limitations.md and docs/user/hql-spec.md both still say it is not.",
				firstPage, secondPage)
		}
		t.Logf("observed DUPLICATE across the boundary: %v (page one %v, page two %v)",
			duplicated, firstPage, secondPage)
	})

	t.Run("a delete before the boundary skips a document", func(t *testing.T) {
		s := newTestServer(t)
		seedSortedBucket(t, s, database, "orders", seeded)

		firstPage := sortedPage(t, s, database, "orders", 0, pageSize)
		if len(firstPage) != pageSize {
			t.Fatalf("page one returned %d documents, want %d", len(firstPage), pageSize)
		}
		// The document that page two would have started with, before the
		// delete renumbers the ordering underneath it.
		wouldHaveBeenNext := sortedPage(t, s, database, "orders", pageSize, 1)
		if len(wouldHaveBeenNext) != 1 {
			t.Fatalf("expected a document at offset %d, got %v", pageSize, wouldHaveBeenNext)
		}

		resp, err := s.Delete(DeleteRequest{Database: database, Bucket: "orders", Key: firstPage[0]})
		if err != nil || !resp.Success {
			t.Fatalf("delete %s: %+v, %v", firstPage[0], resp, err)
		}

		secondPage := sortedPage(t, s, database, "orders", pageSize, pageSize)

		returned := make(map[string]bool, len(firstPage)+len(secondPage))
		for _, key := range firstPage {
			returned[key] = true
		}
		for _, key := range secondPage {
			returned[key] = true
		}
		if returned[wouldHaveBeenNext[0]] {
			t.Fatalf("sorted paging survived a delete before the page boundary: %q was still returned "+
				"(page one %v, page two %v). If that is now the real behaviour, "+
				"docs/user/limitations.md and docs/user/hql-spec.md both still say it is not.",
				wouldHaveBeenNext[0], firstPage, secondPage)
		}
		t.Logf("observed SKIP across the boundary: %q was never returned on either page "+
			"(page one %v, page two %v)", wouldHaveBeenNext[0], firstPage, secondPage)
	})
}
