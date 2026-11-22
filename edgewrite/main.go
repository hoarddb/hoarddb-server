// Command edgewrite hammers one store type with large documents (default 1MB
// x 10,000 = ~10GB logical) through the TCP driver client, then reads
// everything back and verifies SHA-256 checksums. Throwaway edge-case tool:
// it lives outside the normal bench harness because the harness only writes
// ~55-byte documents, which cannot reach gigabyte scale in reasonable time.
//
// Usage:
//
//	edgewrite -addr=127.0.0.1:7433 -store=hash -docmb=1 -docs=10000 -dbname=Edge
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// minAvailBytes aborts the run while this much free space remains, so a
// runaway cannot take the filesystem to zero.
const minAvailBytes = 6 << 30

func availBytes(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return availFrom(int64(stat.Bavail), int64(stat.Bsize))
}

// availFrom is the free space in bytes. Bavail goes negative on freebsd when
// the reserved blocks are gone, and this figure guards the fill against taking
// the filesystem to zero: a wrapped value would report about 16 EiB free and
// the guard would never fire.
func availFrom(bavail, bsize int64) uint64 {
	if bavail <= 0 || bsize <= 0 {
		return 0
	}
	return uint64(bavail) * uint64(bsize)
}

// payloadFor builds a deterministic ~sizeBytes payload for document i: the
// index stamped into every 1KB block so corruption is localizable, with a
// SHA-256 over the whole thing as the integrity anchor.
func payloadFor(i, sizeBytes int) string {
	var sb strings.Builder
	sb.Grow(sizeBytes)
	block := 0
	for sb.Len() < sizeBytes {
		marker := fmt.Sprintf("|doc-%08d-blk-%06d|", i, block)
		sb.WriteString(marker)
		for sb.Len() < sizeBytes && (sb.Len()%(1024)) != 0 {
			sb.WriteByte(byte('a' + (i+block)%26))
		}
		block++
	}
	s := sb.String()
	return s[:sizeBytes]
}

func digestOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7433", "server address")
	store := flag.String("store", "hash", "store type: hash btree fifo lifo heap blob")
	docMB := flag.Int("docmb", 1, "payload megabytes per document")
	docBytesFlag := flag.Int("docbytes", 0, "payload bytes per document; overrides -docmb when non-zero (use for sizes under 1 MiB)")
	docs := flag.Int("docs", 10000, "number of documents")
	dbName := flag.String("dbname", "Edge", "database name")
	bucket := flag.String("bucket", "bulk", "bucket name")
	dataDir := flag.String("datadir", "", "path monitored for free space (abort below 6GB)")
	verifyOnly := flag.Bool("verifyonly", false, "skip create+write; count and verify -docs existing documents")
	flag.Parse()

	docBytes := *docMB << 20
	if *docBytesFlag > 0 {
		docBytes = *docBytesFlag
	}
	btype := server.BucketType(*store)
	logicalGB := float64(int64(*docs)*int64(docBytes)) / (1 << 30)
	log.Printf("store=%s docs=%d docBytes=%d logical=%.2fGB db=%s", *store, *docs, docBytes, logicalGB, *dbName)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
	defer cancel()

	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "edgewrite benchmark tool"}}, *addr)
	if err != nil {
		log.Fatalf("tls config: %v", err)
	}
	client, err := transport.NewDriverClient(ctx, *addr, tlsCfg)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	send := func(op transport.Opcode, req interface{}, resp interface{}) error {
		payload, err := bson.Marshal(req)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		frame, err := client.SendRequest(ctx, op, payload)
		if err != nil {
			return err
		}
		if frame.Opcode == transport.OpError {
			// Decode the reason. Reporting a bare "server error opcode" here hid a
			// real message: the disk-full run showed an empty Message and no cause,
			// and the empty field was this tool's zero-value response struct, not
			// anything the server failed to send.
			var errResp server.ErrorResponse
			if len(frame.Payload) > 0 {
				if uerr := bson.Unmarshal(frame.Payload, &errResp); uerr == nil && errResp.Message != "" {
					return fmt.Errorf("server error: %s", errResp.Message)
				}
			}
			return fmt.Errorf("server error opcode with no reason in the payload")
		}
		if resp != nil && len(frame.Payload) > 0 {
			if err := bson.Unmarshal(frame.Payload, resp); err != nil {
				return fmt.Errorf("unmarshal: %w", err)
			}
		}
		return nil
	}
	must := func(err error, what string) {
		if err != nil {
			log.Fatalf("%s: %v", what, err)
		}
	}

	must(send(transport.OpAuth, &server.LoginRequest{Username: "admin", Password: "bench-root-password"}, nil), "login")
	if *verifyOnly {
		// Keyed stores only: queues drain destructively, so a non-destructive
		// re-verify is only meaningful where reads don't consume.
		switch btype {
		case server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeBlob:
			verifyExisting(send, must, *dbName, *bucket, *docs, docBytes)
		default:
			log.Fatalf("verifyonly is only valid for keyed stores (hash, btree, blob)")
		}
		return
	}
	must(send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: *dbName}, nil), "create database")
	must(send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: *dbName, Name: *bucket, Type: btype}, nil), "create bucket")

	// Write phase.
	writeStart := time.Now()
	var writeErrs int
	for i := 0; i < *docs; i++ {
		payload := payloadFor(i, docBytes)
		key := fmt.Sprintf("edge_%08d", i)
		doc := map[string]interface{}{"_id_": key, "idx": key, "payload": payload}
		switch btype {
		case server.BucketTypeBlob:
			// A blob bucket is written with BLOB_WRITE; the document's BSON is
			// the blob's bytes, which GET answers as the document.
			encoded, err := bson.Marshal(doc)
			if err != nil {
				log.Fatalf("encoding document %d: %v", i, err)
			}
			var resp server.BlobWriteResponse
			if err := send(opFor(btype, true), &server.BlobWriteRequest{Database: *dbName, Bucket: *bucket, Key: key, Data: encoded}, &resp); err != nil || !resp.Success {
				log.Printf("blob write %d FAILED: %+v %v", i, resp, err)
				if writeErrs++; writeErrs > 10 {
					log.Fatalf("too many write errors, aborting")
				}
			}
		case server.BucketTypeHash, server.BucketTypeBTree:
			var resp server.InsertResponse
			if err := send(opFor(btype, true), &server.InsertRequest{Database: *dbName, Bucket: *bucket, Key: key, Data: doc}, &resp); err != nil || !resp.Success {
				log.Printf("insert %d FAILED: %+v %v", i, resp, err)
				if writeErrs++; writeErrs > 10 {
					log.Fatalf("too many write errors, aborting")
				}
			}
		default:
			var resp server.PushResponse
			req := &server.PushRequest{Database: *dbName, Bucket: *bucket, Data: doc, Priority: int64(i)}
			if err := send(opFor(btype, true), req, &resp); err != nil || !resp.Success {
				log.Printf("push %d FAILED: %+v %v", i, resp, err)
				if writeErrs++; writeErrs > 10 {
					log.Fatalf("too many write errors, aborting")
				}
			}
		}
		if (i+1)%1000 == 0 {
			elapsed := time.Since(writeStart).Seconds()
			avail := availBytes(*dataDir)
			log.Printf("wrote %d/%d docs (%.0f docs/s) avail=%.1fGB", i+1, *docs, float64(i+1)/elapsed, float64(avail)/(1<<30))
			if *dataDir != "" && avail < minAvailBytes {
				log.Fatalf("free space %.1fGB below 6GB floor, aborting before the filesystem fills", float64(avail)/(1<<30))
			}
		}
	}
	writeTime := time.Since(writeStart)
	log.Printf("WRITE DONE: %d docs in %v (%.0f docs/s, %.1f MB/s logical), errors=%d",
		*docs, writeTime.Round(time.Second), float64(*docs)/writeTime.Seconds(),
		float64(int64(*docs)*int64(docBytes))/writeTime.Seconds()/(1<<20), writeErrs)

	// Count check.
	var count server.CountResponse
	must(send(transport.OpCount, &server.CountRequest{Database: *dbName, Bucket: *bucket}, &count), "count")
	log.Printf("COUNT: %d (want %d)", count.Count, *docs)
	if count.Count != int64(*docs) {
		log.Fatalf("count mismatch: got %d want %d — data lost", count.Count, *docs)
	}

	// Read-back + verify phase.
	readStart := time.Now()
	var verified, corrupt, missing int
	switch btype {
	case server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeBlob:
		for i := 0; i < *docs; i++ {
			key := fmt.Sprintf("edge_%08d", i)
			var resp server.FindResponse
			if err := send(opFor(btype, false), &server.FindRequest{Database: *dbName, Bucket: *bucket, Key: key}, &resp); err != nil {
				log.Printf("find %d FAILED: %v", i, err)
				missing++
				continue
			}
			if !resp.Found {
				missing++
				continue
			}
			var doc map[string]interface{}
			if err := bson.Unmarshal([]byte(resp.Data), &doc); err != nil {
				log.Printf("doc %d: unmarshal FAILED: %v", i, err)
				corrupt++
				continue
			}
			gotPayload, _ := doc["payload"].(string)
			if digestOf(gotPayload) != digestOf(payloadFor(i, docBytes)) {
				log.Printf("doc %d: CHECKSUM MISMATCH", i)
				corrupt++
				continue
			}
			verified++
			if (i+1)%2000 == 0 {
				log.Printf("verified %d/%d", i+1, *docs)
			}
		}
	default:
		// Queues/heap drain destructively; expected pop order per type.
		for pop := 0; pop < *docs; pop++ {
			want := pop
			switch btype {
			case server.BucketTypeLIFO:
				want = *docs - 1 - pop
			case server.BucketTypeHeap:
				want = *docs - 1 - pop // max-heap, priority=i pushed ascending
			}
			var resp server.PopResponse
			if err := send(opFor(btype, false), &server.PopRequest{Database: *dbName, Bucket: *bucket}, &resp); err != nil {
				log.Printf("pop %d FAILED: %v", pop, err)
				missing++
				continue
			}
			if !resp.Found {
				log.Printf("pop %d: EMPTY before drain", pop)
				missing++
				break
			}
			var doc map[string]interface{}
			if err := bson.Unmarshal([]byte(resp.Items[0]), &doc); err != nil {
				log.Printf("pop %d: unmarshal FAILED: %v", pop, err)
				corrupt++
				continue
			}
			gotPayload, _ := doc["payload"].(string)
			if digestOf(gotPayload) != digestOf(payloadFor(want, docBytes)) {
				log.Printf("pop %d (want doc %d): CHECKSUM/ORDER MISMATCH", pop, want)
				corrupt++
				continue
			}
			verified++
			if (pop+1)%2000 == 0 {
				log.Printf("verified %d/%d", pop+1, *docs)
			}
		}
	}
	readTime := time.Since(readStart)
	log.Printf("VERIFY DONE: verified=%d corrupt=%d missing=%d in %v (%.1f MB/s)",
		verified, corrupt, missing, readTime.Round(time.Second),
		float64(verified*docBytes)/readTime.Seconds()/(1<<20))
	if corrupt > 0 || missing > 0 || verified != *docs {
		log.Fatalf("INTEGRITY FAILURE: verified=%d corrupt=%d missing=%d want=%d", verified, corrupt, missing, *docs)
		os.Exit(1)
	}
	log.Print("INTEGRITY OK")
}

// verifyExisting counts and checksum-verifies docs already present in a
// keyed bucket. Used after an incident (crash, disk-full) to prove what
// survived: count first, then every document's payload against its
// deterministic digest.
func verifyExisting(
	send func(transport.Opcode, interface{}, interface{}) error,
	must func(error, string),
	dbName, bucket string,
	docs, docBytes int,
) {
	var count server.CountResponse
	must(send(transport.OpCount, &server.CountRequest{Database: dbName, Bucket: bucket}, &count), "count")
	log.Printf("COUNT: %d (want %d)", count.Count, docs)

	readStart := time.Now()
	var verified, corrupt, missing int
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("edge_%08d", i)
		var resp server.FindResponse
		if err := send(transport.OpGet, &server.FindRequest{Database: dbName, Bucket: bucket, Key: key}, &resp); err != nil {
			log.Printf("find %d FAILED: %v", i, err)
			missing++
			continue
		}
		if !resp.Found {
			missing++
			continue
		}
		var doc map[string]interface{}
		if err := bson.Unmarshal([]byte(resp.Data), &doc); err != nil {
			log.Printf("doc %d: unmarshal FAILED: %v", i, err)
			corrupt++
			continue
		}
		gotPayload, _ := doc["payload"].(string)
		if digestOf(gotPayload) != digestOf(payloadFor(i, docBytes)) {
			log.Printf("doc %d: CHECKSUM MISMATCH", i)
			corrupt++
			continue
		}
		verified++
		if (i+1)%2000 == 0 {
			log.Printf("verified %d/%d", i+1, docs)
		}
	}
	readTime := time.Since(readStart)
	log.Printf("VERIFY DONE: verified=%d corrupt=%d missing=%d in %v",
		verified, corrupt, missing, readTime.Round(time.Second))
	if corrupt > 0 || missing > 0 || verified != docs {
		log.Fatalf("INTEGRITY FAILURE: verified=%d corrupt=%d missing=%d want=%d", verified, corrupt, missing, docs)
	}
	log.Print("INTEGRITY OK")
}

// opFor maps a store type to its write/read opcode. Heap reads drain via pop;
// heap has no keyed lookup, so the read opcode is unused for it.
func opFor(btype server.BucketType, write bool) transport.Opcode {
	if write {
		switch btype {
		case server.BucketTypeFIFO:
			return transport.OpFIFOPush
		case server.BucketTypeLIFO:
			return transport.OpLIFOPush
		case server.BucketTypeHeap:
			return transport.OpHeapPush
		case server.BucketTypeBlob:
			// A blob bucket is written with put (BLOB_WRITE); a document PUT
			// into one is refused, and upsert is for hash and btree.
			return transport.OpBlobWrite
		default:
			return transport.OpUpsert
		}
	}
	switch btype {
	case server.BucketTypeFIFO:
		return transport.OpFIFOPop
	case server.BucketTypeLIFO:
		return transport.OpLIFOPop
	case server.BucketTypeHeap:
		return transport.OpHeapPop
	default:
		return transport.OpGet
	}
}
