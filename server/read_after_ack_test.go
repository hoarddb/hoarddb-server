package server

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// readAfterAckPost is the part of a pushed post the reader checks.
type readAfterAckPost struct {
	Seq int64 `bson:"seq"`
}

// roundTrip sends one request on c and decodes its answer into response,
// refusing an OpError answer.
func (c *queryConn) roundTrip(op transport.Opcode, request, response interface{}) error {
	payload, err := bson.Marshal(request)
	if err != nil {
		return fmt.Errorf("encoding %s: %w", op, err)
	}
	if err := c.send(transport.NewFrame(op, payload)); err != nil {
		return err
	}
	answer, err := c.read()
	if err != nil {
		return err
	}
	if answer.Opcode == transport.OpError {
		return fmt.Errorf("%s refused: %s", op, answer.Payload)
	}
	if err := bson.Unmarshal(answer.Payload, response); err != nil {
		return fmt.Errorf("decoding the %s answer: %w", op, err)
	}
	return nil
}

// newestPost scans the newest post of a LIFO bucket on c, with or without a
// projection, and returns it with the scan's whole-bucket count.
func newestPost(c *queryConn, bucket string, projection map[string]int) (readAfterAckPost, int64, error) {
	var page ScanResponse
	scan := ScanRequest{Database: "app", Bucket: bucket, Limit: 1, Projection: projection}
	if err := c.roundTrip(transport.OpScan, scan, &page); err != nil {
		return readAfterAckPost{}, 0, err
	}
	if !page.Found || len(page.Documents) != 1 {
		return readAfterAckPost{}, 0, fmt.Errorf("scan of %s: found %v, %d documents, message %q", bucket, page.Found, len(page.Documents), page.Message)
	}
	var newest readAfterAckPost
	if err := bson.Unmarshal(page.Documents[0], &newest); err != nil {
		return readAfterAckPost{}, 0, fmt.Errorf("decoding the newest post of %s: %w", bucket, err)
	}
	return newest, page.Total, nil
}

func pushPost(c *queryConn, bucket string, seq int64) error {
	var pushed PushResponse
	request := PushRequest{Database: "app", Bucket: bucket, Data: map[string]interface{}{"seq": seq, "author": bucket}}
	if err := c.roundTrip(transport.OpLIFOPush, request, &pushed); err != nil {
		return err
	}
	if !pushed.Success {
		return fmt.Errorf("push of seq %d to %s: %s", seq, bucket, pushed.Message)
	}
	return nil
}

// TestAckedLIFOPushIsVisibleToTheNextScanOnAnotherConnection is the
// read-your-writes guard behind a load-test investigation ("newest post has seq 23 with
// no write in flight, expected 24"): once a push to a LIFO bucket
// is acknowledged on one connection, a scan sent afterwards on a different
// connection must return that push as the bucket's newest post, and count it.
//
// A scan reads a snapshot opened when its name resolves (resolveForReadIn) and
// a push is answered only after its Badger commit returns, so the order
// "ack, then scan" must imply "the scan's snapshot holds the push". Sixteen
// other connections push to and scan their own LIFO buckets throughout, so
// snapshots, read views and commits interleave with every checked pair. Both
// scan paths are checked: the unprojected page (scanRawPage, the page reader
// behind the unfiltered scan) and the {seq: 1} projection a social-feed attach reads
// (scanFilteredPage).
func TestAckedLIFOPushIsVisibleToTheNextScanOnAnotherConnection(t *testing.T) {
	iterations := 1000
	if testing.Short() {
		iterations = 200
	}
	const noiseConns = 16

	qs, addr := newTCPEnabledServer(t, nil)
	createTestBucket(t, qs.core, "app", "tl_26", BucketTypeLIFO)
	for n := 0; n < noiseConns; n++ {
		createTestBucket(t, qs.core, "app", fmt.Sprintf("tl_noise%02d", n), BucketTypeLIFO)
	}

	writer := mustDialQueryConn(t, addr, "admin", "test123")
	reader := mustDialQueryConn(t, addr, "admin", "test123")

	stop := make(chan struct{})
	var noise sync.WaitGroup
	noiseErrs := make(chan error, noiseConns)
	for n := 0; n < noiseConns; n++ {
		conn := mustDialQueryConn(t, addr, "admin", "test123")
		bucket := fmt.Sprintf("tl_noise%02d", n)
		noise.Add(1)
		go func() {
			defer noise.Done()
			for seq := int64(0); ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := pushPost(conn, bucket, seq); err != nil {
					noiseErrs <- err
					return
				}
				newest, total, err := newestPost(conn, bucket, nil)
				if err != nil {
					noiseErrs <- err
					return
				}
				if newest.Seq != seq || total != seq+1 {
					noiseErrs <- fmt.Errorf("%s on its own connection: newest seq %d of %d posts after acked seq %d", bucket, newest.Seq, total, seq)
					return
				}
			}
		}()
	}
	defer func() {
		close(stop)
		noise.Wait()
	}()

	// The reader runs on its own goroutine and connection; the writer hands it
	// each seq the moment its push is acknowledged and waits for the verdict,
	// so every check is exact.
	acked := make(chan int64)
	verdicts := make(chan error)
	go func() {
		for seq := range acked {
			verdicts <- checkNewest(reader, "tl_26", seq)
		}
		close(verdicts)
	}()

	var failures []error
	for seq := int64(0); seq < int64(iterations); seq++ {
		if err := pushPost(writer, "tl_26", seq); err != nil {
			failures = append(failures, fmt.Errorf("writer: %w", err))
			break
		}
		acked <- seq
		if err := <-verdicts; err != nil {
			failures = append(failures, err)
			if len(failures) >= 5 {
				break
			}
		}
	}
	close(acked)
	for range verdicts {
	}

	select {
	case err := <-noiseErrs:
		failures = append(failures, fmt.Errorf("noise connection: %w", err))
	default:
	}
	if len(failures) > 0 {
		t.Fatalf("an acknowledged push was not visible to a later scan on another connection (%d iterations): %v", iterations, errors.Join(failures...))
	}
}

// checkNewest asserts that both scan paths on c see seq as the newest post of
// bucket and count seq+1 posts.
func checkNewest(c *queryConn, bucket string, seq int64) error {
	for _, projection := range []map[string]int{nil, {"seq": 1}} {
		newest, total, err := newestPost(c, bucket, projection)
		if err != nil {
			return fmt.Errorf("reader after acked seq %d (projection %v): %w", seq, projection, err)
		}
		if newest.Seq != seq || total != seq+1 {
			return fmt.Errorf("reader after acked seq %d (projection %v): newest seq %d of %d posts, want seq %d of %d",
				seq, projection, newest.Seq, total, seq, seq+1)
		}
	}
	return nil
}
