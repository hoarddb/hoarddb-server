package server

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// idleAnswers encodes the answers of buckets that never claimed anything: a
// peek, a count, the bucket's info and its listing row, and a pop, of each
// queue type. Entries carry explicit ids so the bytes do not vary.
func idleAnswers(t *testing.T) map[string]string {
	t.Helper()
	s := newTestServer(t)
	encoded := map[string]string{}
	record := func(name string, answer any, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		payload, marshalErr := bson.Marshal(answer)
		if marshalErr != nil {
			t.Fatalf("%s: %v", name, marshalErr)
		}
		encoded[name] = hex.EncodeToString(payload)
	}
	for _, bucketType := range leaseBucketTypes {
		bucket := "idle-" + string(bucketType)
		createTestBucket(t, s, "app", bucket, bucketType)
		for n := 1; n <= 2; n++ {
			resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(n), Data: map[string]interface{}{"_id_": string(rune('a' + n))}})
			if err != nil || !resp.Success {
				t.Fatalf("push: %+v, %v", resp, err)
			}
		}
		peeked, err := s.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: 2})
		record(bucket+"/peek", peeked, err)
		counted, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: bucket})
		record(bucket+"/count", counted, err)
		info, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: bucket})
		if err == nil {
			info.Bucket.ReplicationFactor = 0
			clearReplicationView(&info.Bucket)
		}
		record(bucket+"/info", info, err)
		popped, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
		record(bucket+"/pop", popped, err)
		if _, err := s.Pop(PopRequest{Database: "app", Bucket: bucket}); err != nil {
			t.Fatalf("%s: popping the second entry: %v", bucket, err)
		}
		empty, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
		record(bucket+"/empty pop", empty, err)
	}
	listed, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err == nil {
		for index := range listed.Buckets {
			listed.Buckets[index].ReplicationFactor = 0
			clearReplicationView(&listed.Buckets[index])
		}
	}
	record("list", listed, err)
	return encoded
}

// clearReplicationView removes the owner's view of a row's copies (bucket
// replication spec §12.1), which belongs to replication and not to a queue's
// claims, as the test already removes the factor.
func clearReplicationView(row *BucketInfo) {
	row.InSync, row.State, row.NotInSync, row.PendingCopies = 0, "", nil, nil
}

// idleAnswersOnTheBase are those answers' bytes as the base commit (before any
// pending, dead or expired member existed) encoded them. A bucket that never
// claimed must keep encoding byte for byte the same (claim-ack-spec 8.4, CA-34).
var idleAnswersOnTheBase = map[string]string{
	"idle-fifo/count":     "2200000012636f756e74000200000000000000026d65737361676500010000000000",
	"idle-fifo/empty pop": "2900000008666f756e640000026d657373616765000f000000717565756520697320656d7074790000",
	"idle-fifo/info":      "7400000008666f756e640001026461746162617365000400000061707000036275636b6574004d000000026e616d65000a00000069646c652d6669666f00027479706500050000006669666f0012656e74726965730002000000000000000a73746f7265730008696e6865726974656400010000",
	"idle-fifo/peek":      "4f00000008666f756e640001046974656d73002d00000003300011000000025f69645f000200000062000003310011000000025f69645f000200000063000000026d65737361676500010000000000",
	"idle-fifo/pop":       "3b00000008666f756e640001046974656d73001900000003300011000000025f69645f000200000062000000026d65737361676500010000000000",
	"idle-heap/count":     "2200000012636f756e74000200000000000000026d65737361676500010000000000",
	"idle-heap/empty pop": "2800000008666f756e640000026d657373616765000e0000006865617020697320656d7074790000",
	"idle-heap/info":      "7400000008666f756e640001026461746162617365000400000061707000036275636b6574004d000000026e616d65000a00000069646c652d686561700002747970650005000000686561700012656e74726965730002000000000000000a73746f7265730008696e6865726974656400010000",
	"idle-heap/peek":      "4f00000008666f756e640001046974656d73002d00000003300011000000025f69645f000200000063000003310011000000025f69645f000200000062000000026d65737361676500010000000000",
	"idle-heap/pop":       "3b00000008666f756e640001046974656d73001900000003300011000000025f69645f000200000063000000026d65737361676500010000000000",
	"idle-lifo/count":     "2200000012636f756e74000200000000000000026d65737361676500010000000000",
	"idle-lifo/empty pop": "2900000008666f756e640000026d657373616765000f000000717565756520697320656d7074790000",
	"idle-lifo/info":      "7400000008666f756e640001026461746162617365000400000061707000036275636b6574004d000000026e616d65000a00000069646c652d6c69666f00027479706500050000006c69666f0012656e74726965730002000000000000000a73746f7265730008696e6865726974656400010000",
	"idle-lifo/peek":      "4f00000008666f756e640001046974656d73002d00000003300011000000025f69645f000200000063000003310011000000025f69645f000200000062000000026d65737361676500010000000000",
	"idle-lifo/pop":       "3b00000008666f756e640001046974656d73001900000003300011000000025f69645f000200000063000000026d65737361676500010000000000",
	"list":                "3d01000008666f756e640001026461746162617365000400000061707000046275636b65747300f50000000330004d000000026e616d65000a00000069646c652d6669666f00027479706500050000006669666f0012656e74726965730000000000000000000a73746f7265730008696e686572697465640001000331004d000000026e616d65000a00000069646c652d686561700002747970650005000000686561700012656e74726965730000000000000000000a73746f7265730008696e686572697465640001000332004d000000026e616d65000a00000069646c652d6c69666f00027479706500050000006c69666f0012656e74726965730000000000000000000a73746f7265730008696e6865726974656400010000026d65737361676500010000000002656e6372797074656400030000006e6f0000",
}

// TestIdleBucketAnswersAreByteIdentical: peek, count, info, the listing and pop of
// a bucket that never claimed, and of an empty one, carry no new member at all.
func TestIdleBucketAnswersAreByteIdentical(t *testing.T) {
	got := idleAnswers(t)
	for name, want := range idleAnswersOnTheBase {
		if got[name] != want {
			t.Errorf("%s encodes differently from the base:\n got %s\nwant %s", name, got[name], want)
		}
	}
	if len(got) != len(idleAnswersOnTheBase) {
		t.Errorf("%d answers compared, the base golden holds %d", len(got), len(idleAnswersOnTheBase))
	}
}
