package server

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWritesToOneKeyAllLand is the reproduction: N writers
// race on one key of one bucket. Every write must land — no conflict surfaces
// as an internal error — and the bucket's state must count the key once.
// Distinct keys race too, so a lost update of the state document's entry
// count (two writers loading the same count and both writing count+1) fails
// here rather than passing silently.
func TestConcurrentWritesToOneKeyAllLand(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "race", bucketType)

			const writers = 16
			failures := raceInserts(s, writers, func(writer int) (string, int) { return "k", writer })
			for _, failure := range failures {
				t.Errorf("same-key writer: %s", failure)
			}
			if st := bucketStateOf(t, s, "app", "race"); st.Entries != 1 {
				t.Errorf("after %d writers on one key the bucket counts %d entries, want 1", writers, st.Entries)
			}

			const distinct = 64
			failures = raceInserts(s, distinct, func(writer int) (string, int) { return fmt.Sprintf("d%02d", writer), writer })
			for _, failure := range failures {
				t.Errorf("distinct-key writer: %s", failure)
			}
			if st := bucketStateOf(t, s, "app", "race"); st.Entries != 1+distinct {
				t.Errorf("after %d distinct-key writers the bucket counts %d entries, want %d (a lost update of the state document)",
					distinct, st.Entries, 1+distinct)
			}
		})
	}
}

// raceInserts starts writers inserts at once and returns a description of
// every one that did not succeed.
func raceInserts(s *Server, writers int, keyOf func(writer int) (string, int)) []string {
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []string
		start    = make(chan struct{})
	)
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			key, value := keyOf(writer)
			<-start
			resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "race", Key: key, Data: map[string]interface{}{"v": value}})
			if err == nil && resp.Success {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures = append(failures, fmt.Sprintf("key %q: %v", key, err))
				return
			}
			failures = append(failures, fmt.Sprintf("key %q: %s", key, resp.Message))
		}(writer)
	}
	close(start)
	wg.Wait()
	return failures
}

// TestBucketTransactionsDoNotOverlap (spec §10's test, recast from a
// transaction-interval hook to the lost-update shape above; build doc Step 6,
// S-1): every writer of a hash bucket — insert, batch insert, update,
// multi-update, delete and push — races on one bucket with a unique index. The
// unique check reads Apply's snapshot, which is safe only because every one
// of them holds the bucket mutex from before that snapshot to after its
// commit. Two writers overlapping would lose an update of the state document
// (the count drifts from a recount) or both take one unique value.
func TestBucketTransactionsDoNotOverlap(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "race", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{map[string]interface{}{"field": "Email", "unique": true}}}}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	const rounds = 8
	for round := 0; round < rounds; round++ {
		for i := 0; i < 16; i++ {
			for _, key := range []string{fmt.Sprintf("u-%d-%d", round, i), fmt.Sprintf("del-%d-%d", round, i)} {
				if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "race", Key: key,
					Data: map[string]interface{}{"Email": key + "@example.com", "Group": fmt.Sprintf("g-%d-%d", round, i%4)}}); err != nil || !resp.Success {
					t.Fatalf("seeding %s = %+v, %v", key, resp, err)
				}
			}
		}
		contested := fmt.Sprintf("contested-%d@example.com", round)
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners int
			start   = make(chan struct{})
		)
		race := func(write func() (bool, error), contends bool) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ok, err := write()
				if contends && err == nil && ok {
					mu.Lock()
					winners++
					mu.Unlock()
				}
			}()
		}
		for i := 0; i < 16; i++ {
			i := i
			email := func(key string) string {
				if i%4 == 0 {
					return contested
				}
				return key + "@example.com"
			}
			contends := i%4 == 0
			insertKey, batchKey, pushKey := fmt.Sprintf("ins-%d-%d", round, i), fmt.Sprintf("batch-%d-%d", round, i), fmt.Sprintf("push-%d-%d", round, i)
			race(func() (bool, error) {
				resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "race", Key: insertKey, Data: map[string]interface{}{"Email": email(insertKey)}})
				return err == nil && resp.Success, err
			}, contends)
			race(func() (bool, error) {
				resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "race", Data: []map[string]interface{}{
					{"_id_": batchKey, "Email": email(batchKey)}, {"_id_": batchKey + "-b", "Email": batchKey + "-b@example.com"}}})
				return err == nil && resp.Success, err
			}, contends)
			race(func() (bool, error) {
				key := fmt.Sprintf("u-%d-%d", round, i)
				resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "race", Key: key, Set: map[string]interface{}{"Email": email(key + "-updated")}})
				return err == nil && resp.Success, err
			}, contends)
			race(func() (bool, error) {
				resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "race", Multi: true,
					Filter: map[string]interface{}{"Group": fmt.Sprintf("g-%d-%d", round, i%4)}, Inc: map[string]int64{"Visits": 1}})
				return err == nil && resp.Success, err
			}, false)
			race(func() (bool, error) {
				resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "race", Key: fmt.Sprintf("del-%d-%d", round, i)})
				return err == nil && resp.Success, err
			}, false)
			race(func() (bool, error) {
				resp, err := s.Push(PushRequest{Database: "app", Bucket: "race", Data: map[string]interface{}{"_id_": pushKey, "Email": email(pushKey)}})
				return err == nil && resp.Success, err
			}, contends)
		}
		close(start)
		wg.Wait()
		if winners != 1 {
			t.Errorf("round %d: %d writers took the contested unique value, want exactly 1", round, winners)
		}
		st := bucketStateOf(t, s, "app", "race")
		recounted, err := lookupBucket(s, "app", "race").hashStore.Recount()
		if err != nil {
			t.Fatalf("Recount: %v", err)
		}
		if st.Entries != recounted {
			t.Fatalf("round %d: the state counts %d entries, a recount %d: two writers' applies overlapped and one lost the other's update",
				round, st.Entries, recounted)
		}
	}
}
