// bench/smokeperf — functional smoke + quick perf across every store type
// except blob, against a live server over the wire.
//
//	create database smoky, one bucket per type (hash, btree, fifo, lifo, heap)
//	for each type: insert/push N docs, read/pop them back, VERIFY correctness
//	  (a read that returns wrong data is a failed smoke, not a fast one),
//	  then time the read/pop loop for ops/s and p50/p99.
//
// Run:
//
//	go build ./cmd/server        # a server with the shared layout (Steps 0-16)
//	HOARDDB_DATA_DIR=/tmp/smoke HOARDDB_LISTEN=127.0.0.1:4433 \
//	  HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=bench-root-password ./server &
//	go run ./bench/smokeperf -addr=127.0.0.1:7433 -n=1000
//
// Like bench/tcp_client.go (which it mirrors): one TLS socket, sequential ops,
// login admin/bench-root-password. Deliberately no dependency, no new go.mod entries,
// no CI wiring — a throwaway instrument the way bench/probe is.
package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

type bucketSpec struct {
	name, kind string
}

var bucketTypeNames = map[string]server.BucketType{
	"hash":  server.BucketTypeHash,
	"btree": server.BucketTypeBTree,
	"fifo":  server.BucketTypeFIFO,
	"lifo":  server.BucketTypeLIFO,
	"heap":  server.BucketTypeHeap,
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7433", "driver listener address")
	n := flag.Int("n", 1000, "documents per bucket")
	pw := flag.String("password", "bench-root-password", "root password (bench convention)")
	readonly := flag.Bool("readonly", false, "skip create+insert; read back the existing smoky buckets")
	flag.Parse()

	c, err := dial(*addr, *pw)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.close()

	call := func(op transport.Opcode, req, resp interface{}) *transport.Frame {
		payload, err := bson.Marshal(req)
		if err != nil {
			log.Fatalf("marshal %s: %v", op, err)
		}
		frame, err := c.roundtrip(op, payload)
		if err != nil {
			log.Fatalf("%s: %v", op, err)
		}
		if resp != nil {
			if err := bson.Unmarshal(frame.Payload, resp); err != nil {
				log.Fatalf("decode %s response: %v (frame opcode=%s len=%d)", op, err, frame.Opcode, len(frame.Payload))
			}
		}
		return frame
	}

	db := "smoky"
	if !*readonly {
		// -- setup ---------------------------------------------------------
		call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: db}, nil)
		types := []bucketSpec{
			{"users", "hash"},
			{"logs", "btree"},
			{"queue", "fifo"},
			{"stack", "lifo"},
			{"votes", "heap"},
		}
		for _, b := range types {
			if kind, ok := bucketTypeNames[b.kind]; !ok {
				log.Fatalf("unknown bucket kind %q", b.kind)
			} else {
				call(transport.OpCreateBucket, server.CreateBucketRequest{Database: db, Name: b.name, Type: kind}, nil)
			}
		}
	}

	fmt.Printf("hoardDB shared-layout smoke+perf — %d docs/bucket, sequential, host=%s\n", *n, *addr)
	if *readonly {
		fmt.Printf("READONLY: persistence check — re-reading buckets after a restart\n")
	}
	fmt.Printf("%-14s %-30s %10s %16s\n", "bucket/op", "verify", "ops/s", "p50/p99")

	for _, b := range []bucketSpec{{"users", "hash"}, {"logs", "btree"}, {"queue", "fifo"}, {"stack", "lifo"}, {"votes", "heap"}} {
		switch b.kind {
		case "hash", "btree":
			keyedLike(b.name, db, *n, call, *readonly)
		case "fifo", "lifo":
			fifoLike(b.name, db, *n, call, *readonly)
		case "heap":
			heapLike(b.name, db, *n, call, *readonly)
		}
	}
}

// docAt builds a deterministic document for the i-th row.
func docAt(i int) map[string]interface{} {
	return map[string]interface{}{
		"_id_":    fmt.Sprintf("k%07d", i),
		"seq":     int64(i),
		"payload": fmt.Sprintf("job-%d message body %d", i, i*i),
	}
}

// rangeKey is the key the keyed buckets store document i under. The btree range
// is inclusive at both ends, so the full-scan check spans rangeKey(0) to
// rangeKey(n-1) and returns exactly n entries for any n.
func rangeKey(i int) string { return fmt.Sprintf("k%07d", i) }

// keyedLike runs the keyed engines (hash, btree): insert N, read N back and
// verify every value, then time the read loop. btree also runs a range scan.
func keyedLike(name, db string, n int, call func(transport.Opcode, interface{}, interface{}) *transport.Frame, readonly bool) {
	insDur := time.Duration(0)
	if !readonly {
		insStart := time.Now()
		for i := 0; i < n; i++ {
			resp := &server.InsertResponse{}
			req := server.InsertRequest{Database: db, Bucket: name, Key: fmt.Sprintf("k%07d", i), Data: docAt(i)}
			call(transport.OpUpsert, req, resp)
			if !resp.Success {
				log.Fatalf("put %s k%07d failed: %s", name, i, resp.Message)
			}
		}
		insDur = time.Since(insStart)
	}

	mismatches := 0
	rdur := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		resp := &server.FindResponse{}
		call(transport.OpGet, server.FindRequest{Database: db, Bucket: name, Key: fmt.Sprintf("k%07d", i)}, resp)
		rdur = append(rdur, time.Since(t0))
		if !resp.Found {
			log.Fatalf("get %s k%07d: not found", name, i)
		}
		var got map[string]interface{}
		_ = bson.Unmarshal(resp.Data, &got)
		if got["seq"] != int64(i) {
			mismatches++
		}
	}
	label := "put→get, verify"
	if name == "logs" {
		// a full range scan across every key verifies the btree range path
		rResp := &server.BTreeRangeResponse{}
		call(transport.OpBTreeRange, server.BTreeRangeRequest{Database: db, Bucket: name, Low: rangeKey(0), High: rangeKey(n - 1), Limit: n}, rResp)
		if len(rResp.Entries) != n {
			log.Fatalf("range %s: got %d entries, want %d", name, len(rResp.Entries), n)
		}
		label += " +range"
	}
	fmt.Printf("%-14s %-30s %10.0f %16s\n", name, label+" ("+verdict(mismatches, 0)+")", float64(n)/durSum(rdur).Seconds(), pctile(rdur))
	if !readonly {
		fmt.Printf("%-14s %-30s %10.0f %16s\n", name, "insert", float64(n)/insDur.Seconds(), ms(insDur))
	} else {
		fmt.Printf("%-14s %-30s %10s %16s\n", name, "re-read after restart", "", "")
	}
}

// fifoLike pushes N, then verifies order by peeking the head and (in readonly,
// after a restart) popping all N. Peek is non-destructive, so in setup mode the
// data is left in place for the restart test; popping is reserved for proving
// the persisted docs survive with the right order.
func fifoLike(name, db string, n int, call func(transport.Opcode, interface{}, interface{}) *transport.Frame, readonly bool) {
	popOp, pushOp, peekOp := transport.OpFIFOPop, transport.OpFIFOPush, transport.OpFIFOPeek
	if name == "stack" {
		popOp, pushOp, peekOp = transport.OpLIFOPop, transport.OpLIFOPush, transport.OpLIFOPeek
	}
	insDur := time.Duration(0)
	if !readonly {
		insStart := time.Now()
		for i := 0; i < n; i++ {
			resp := &server.PushResponse{}
			call(pushOp, server.PushRequest{Database: db, Bucket: name, Data: docAt(i)}, resp)
			if !resp.Success {
				log.Fatalf("push %s #%d failed: %s", name, i, resp.Message)
			}
		}
		insDur = time.Since(insStart)

		// non-destructive: peek the head and confirm it is the expected first
		// element. Leaves every entry in place so the restart can test them.
		peekResp := &server.PeekResponse{}
		call(peekOp, server.PeekRequest{Database: db, Bucket: name}, peekResp)
		if !peekResp.Found {
			log.Fatalf("peek %s: empty after %d pushes", name, n)
		}
		var got map[string]interface{}
		_ = bson.Unmarshal(peekResp.Items[0], &got)
		wantHead := int64(0) // FIFO head is the first pushed
		if name == "stack" {
			wantHead = int64(n - 1) // LIFO head is the last pushed
		}
		if got["seq"] != wantHead {
			log.Fatalf("peek %s head: seq=%v want %d", name, got["seq"], wantHead)
		}
		fmt.Printf("%-14s %-30s %10.0f %16s\n", name, "push, head verified", float64(n)/insDur.Seconds(), ms(insDur))
		fmt.Printf("%-14s %-30s %10s %16s\n", name, "data left in place for restart", "", "")
		return
	}

	// readonly: pop all N and verify order — proves the persisted docs survived.
	mismatches := 0
	rdur := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		resp := &server.PopResponse{}
		call(popOp, server.PopRequest{Database: db, Bucket: name}, resp)
		rdur = append(rdur, time.Since(t0))
		if !resp.Found {
			log.Fatalf("pop %s #%d: empty", name, i)
		}
		var got map[string]interface{}
		_ = bson.Unmarshal(resp.Items[0], &got)
		want := int64(n - 1 - i) // LIFO pops in reverse order
		if name == "queue" {
			want = int64(i) // FIFO pops in push order
		}
		if got["seq"] != want {
			mismatches++
		}
	}
	order := "LIFO"
	if name == "queue" {
		order = "FIFO"
	}
	fmt.Printf("%-14s %-30s %10.0f %16s\n", name, order+" pop persisted, verify ("+verdict(mismatches, 0)+")", float64(n)/durSum(rdur).Seconds(), pctile(rdur))
}

// heapLike pushes N with priority i, confirming the head (peek), then in
// readonly mode pops N verifying pops come back in descending priority order.
func heapLike(name, db string, n int, call func(transport.Opcode, interface{}, interface{}) *transport.Frame, readonly bool) {
	insDur := time.Duration(0)
	if !readonly {
		insStart := time.Now()
		for i := 0; i < n; i++ {
			resp := &server.PushResponse{}
			// seq == priority == i, so pop order (highest priority first) must
			// return the largest seq first.
			call(transport.OpHeapPush, server.PushRequest{Database: db, Bucket: name, Data: docAt(i), Priority: int64(i)}, resp)
			if !resp.Success {
				log.Fatalf("heap push %s #%d failed: %s", name, i, resp.Message)
			}
		}
		insDur = time.Since(insStart)

		peekResp := &server.PeekResponse{}
		call(transport.OpFIFOPeek, server.PeekRequest{Database: db, Bucket: name}, peekResp)
		if !peekResp.Found {
			log.Fatalf("heap peek %s: empty after %d pushes", name, n)
		}
		var got map[string]interface{}
		_ = bson.Unmarshal(peekResp.Items[0], &got)
		if got["seq"] != int64(n-1) { // highest priority (n-1) is on top
			log.Fatalf("heap peek %s top: seq=%v want %d", name, got["seq"], n-1)
		}
		fmt.Printf("%-14s %-30s %10.0f %16s\n", name, "push, top verified", float64(n)/insDur.Seconds(), ms(insDur))
		fmt.Printf("%-14s %-30s %10s %16s\n", name, "data left in place for restart", "", "")
		return
	}

	mismatches := 0
	rdur := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		resp := &server.PopResponse{}
		call(transport.OpHeapPop, server.PopRequest{Database: db, Bucket: name}, resp)
		rdur = append(rdur, time.Since(t0))
		if !resp.Found {
			log.Fatalf("heap pop %s #%d: empty", name, i)
		}
		var got map[string]interface{}
		_ = bson.Unmarshal(resp.Items[0], &got)
		if got["seq"] != int64(n-1-i) { // highest priority (n-1) popped first
			mismatches++
		}
	}
	fmt.Printf("%-14s %-30s %10.0f %16s\n", name, "heap pop persisted, verify ("+verdict(mismatches, 0)+")", float64(n)/durSum(rdur).Seconds(), pctile(rdur))
}

func verdict(bad, want int) string {
	if bad == want {
		return "PASS"
	}
	return fmt.Sprintf("FAIL mismatches=%d", bad)
}

func ms(d time.Duration) string { return fmt.Sprintf("%v", d.Round(time.Millisecond)) }

func durSum(ds []time.Duration) time.Duration {
	var s time.Duration
	for _, d := range ds {
		s += d
	}
	return s
}

func pctile(ds []time.Duration) string {
	c := append([]time.Duration{}, ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	p50 := c[len(c)/2]
	p99 := c[len(c)*99/100]
	return fmt.Sprintf("%v/%v", p50.Round(time.Microsecond), p99.Round(time.Microsecond))
}

// --- wire plumbing (mirror of bench/tcp_client.go, minus the socket pool) ---

type conn struct {
	raw *tls.Conn
	br  *bufio.Reader
}

func dial(addr, pw string) (*conn, error) {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "smokeperf tool"}}, addr)
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}
	raw, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	c := &conn{raw: raw, br: bufio.NewReader(raw)}
	payload, err := bson.Marshal(&server.LoginRequest{Username: "admin", Password: pw})
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	if _, err := raw.Write(transport.NewFrame(transport.OpAuth, payload).Encode()); err != nil {
		_ = raw.Close()
		return nil, err
	}
	resp, err := transport.DecodeFrame(c.br)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	if resp.Opcode != transport.OpAuthOK {
		_ = raw.Close()
		return nil, fmt.Errorf("login rejected: %s", resp.Opcode)
	}
	return c, nil
}

func (c *conn) roundtrip(op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if _, err := c.raw.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		return nil, fmt.Errorf("write %s: %w", op, err)
	}
	resp, err := transport.DecodeFrame(c.br)
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", op, err)
	}
	return resp, nil
}

func (c *conn) close() { _ = c.raw.Close() }
