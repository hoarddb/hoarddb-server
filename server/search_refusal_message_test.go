package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/store"
)

// TestSearchRefusalsCarryNoReadFailureAdvice: a search refused because its
// index is rebuilding, unreadable or conflicted is a deliberate refusal that
// carries its own advice. BUG-0591's next-step sentence ("retry the command
// once ... the node log, which records the failing key") is false for it: the
// bucket was readable, no key failed and nothing was logged.
func TestSearchRefusalsCarryNoReadFailureAdvice(t *testing.T) {
	for _, state := range []store.IndexState{store.IndexBuilding, store.IndexUnreadable} {
		t.Run(string(state), func(t *testing.T) {
			s := newTestServer(t)
			createDB(t, s, "app")
			createIndexedHashBucket(t, s, "app", "people", "email")
			bucket := servedBucket(t, s, "app", "people")
			refused := cloneSpecs(bucket.declaredIndexes())
			refused[0].State = state
			bucket.decl.Load().indexSpecs.store(refused)

			resp, err := s.Search(SearchRequest{Database: "app", Bucket: "people", Filter: map[string]interface{}{"email": "a@example.com"}})
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if !strings.Contains(resp.Message, "index "+refused[0].Name) {
				t.Fatalf("the search was not refused on its %s index: %q", state, resp.Message)
			}
			for _, banned := range []string{"retry the command once", "node log"} {
				if strings.Contains(resp.Message, banned) {
					t.Errorf("the refusal carries %q, which is false for it: %s", banned, resp.Message)
				}
			}
		})
	}
}
