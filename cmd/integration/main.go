package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

const (
	numUsers         = 100
	numPosts         = 1000
	numTimeline      = 5000
	numNotifications = 2000
	numSessions      = 1000
	batchSize        = 100
)

func main() {
	serverAddr := flag.String("addr", "localhost:7433", "server address")
	fingerprint := flag.String("fingerprint", "", "pinned server fingerprint (SHA256:<hex>), from the server's startup log; required")
	flag.Parse()
	if *fingerprint == "" {
		log.Fatal("missing -fingerprint: copy the SHA256 fingerprint the server prints at startup and pass it here; this harness pins rather than trusting any certificate")
	}
	fmt.Printf("SocialBench 10K Test (TCP) — %s\n", *serverAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Connect via TCP+TLS, pinned to the server's fingerprint so the harness
	// the drivers copy does not demonstrate skipping verification.
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Expected: []string{*fingerprint}}, *serverAddr)
	if err != nil {
		log.Fatalf("Failed to create TLS config: %v", err)
	}
	client, err := transport.NewDriverClient(ctx, *serverAddr, tlsCfg)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = client.Close() }()

	passed, failed := 0, 0
	recordsStored := 0
	start := time.Now()
	var authToken string

	// die reports a fatal setup error and exits non-zero so CI fails loudly.
	die := func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
		fmt.Printf("\n=== Results ===\n  Records stored: %d\n  Passed: %d, Failed: %d\n", recordsStored, passed, failed+1)
		os.Exit(1)
	}

	// Helper: send a request and read the response.
	send := func(op transport.Opcode, req interface{}, resp interface{}) error {
		payload, err := bson.Marshal(req)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		f, err := client.SendRequest(ctx, op, payload)
		if err != nil {
			return err
		}
		if f.Opcode == transport.OpError {
			var errResp server.ErrorResponse
			if err := bson.Unmarshal(f.Payload, &errResp); err != nil {
				return fmt.Errorf("server error (unmarshal failed)")
			}
			return fmt.Errorf("server error: %s", errResp.Message)
		}
		if resp != nil && len(f.Payload) > 0 {
			if err := bson.Unmarshal(f.Payload, resp); err != nil {
				return fmt.Errorf("unmarshal response: %w", err)
			}
		}
		return nil
	}

	// Login
	fmt.Println("\n--- Login ---")
	{
		req := server.LoginRequest{Username: "admin", Password: "bench-root-password"}
		var r server.LoginResponse
		if err := send(transport.OpAuth, &req, &r); err != nil {
			die("  FAIL: %v", err)
		}
		if r.Token == "" {
			die("  FAIL: authentication rejected — check that the server was started with HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=bench-root-password")
		}
		authToken = r.Token
		fmt.Println("  OK")
		passed++
	}
	_ = authToken // captured for completeness; every request here reuses the one authenticated connection

	// CreateDatabase
	fmt.Println("\n--- CreateDatabase ---")
	{
		req := server.CreateDatabaseRequest{Name: "SocialBench"}
		var r server.CreateDatabaseResponse
		if err := send(transport.OpCreateDB, &req, &r); err != nil {
			die("  FAIL: %v", err)
		}
		fmt.Println("  OK")
		passed++
	}

	// Create buckets
	buckets := []struct {
		name  string
		btype server.BucketType
	}{
		{"User", server.BucketTypeHash},
		{"Post", server.BucketTypeBTree},
		{"Timeline", server.BucketTypeFIFO},
		{"Notifications", server.BucketTypeHeap},
		{"Session", server.BucketTypeLIFO},
	}
	for _, b := range buckets {
		req := server.CreateBucketRequest{Database: "SocialBench", Name: b.name, Type: b.btype}
		var r server.CreateBucketResponse
		if err := send(transport.OpCreateBucket, &req, &r); err != nil {
			die("  FAIL create %s: %v", b.name, err)
		}
		fmt.Printf("  Created: %s\n", b.name)
		passed++
	}

	// Seed Users via BatchInsert
	fmt.Printf("\n--- Seeding %d Users (batch=%d) ---\n", numUsers, batchSize)
	usersStart := time.Now()
	totalInserted := 0
	for i := 0; i < numUsers; i += batchSize {
		end := i + batchSize
		if end > numUsers {
			end = numUsers
		}
		docs := make([]map[string]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			un := fmt.Sprintf("user_%04d", j)
			docs = append(docs, map[string]interface{}{
				"Username": un, "Email": un + "@test.com", "FullName": fmt.Sprintf("User %d", j),
			})
		}
		req := server.BatchInsertRequest{Database: "SocialBench", Bucket: "User", Data: docs}
		var r server.BatchInsertResponse
		if err := send(transport.OpBatchUpsert, &req, &r); err != nil {
			fmt.Printf("  FAIL: %v\n", err)
			failed++
			continue
		}
		if !r.Success {
			fmt.Printf("  FAIL: %s\n", r.Message)
			failed++
			continue
		}
		totalInserted += int(r.Count)
		passed++
	}
	fmt.Printf("  Inserted %d users in %v\n", totalInserted, time.Since(usersStart).Round(time.Millisecond))
	recordsStored += totalInserted

	// Seed Posts via BatchInsert
	fmt.Printf("\n--- Seeding %d Posts (batch=%d) ---\n", numPosts, batchSize)
	postsStart := time.Now()
	totalInserted = 0
	for i := 0; i < numPosts; i += batchSize {
		end := i + batchSize
		if end > numPosts {
			end = numPosts
		}
		docs := make([]map[string]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			author := fmt.Sprintf("user_%04d", rand.Intn(numUsers))
			docs = append(docs, map[string]interface{}{
				"AuthorID":   author,
				"Caption":    fmt.Sprintf("Post %d", j),
				"created_at": fmt.Sprintf("2026-09-%02dT%02d:%02d:%02dZ", 1+rand.Intn(30), rand.Intn(24), rand.Intn(60), rand.Intn(60)),
			})
		}
		req := server.BatchInsertRequest{Database: "SocialBench", Bucket: "Post", Data: docs}
		var r server.BatchInsertResponse
		if err := send(transport.OpBatchUpsert, &req, &r); err != nil {
			fmt.Printf("  FAIL: %v\n", err)
			failed++
			continue
		}
		if !r.Success {
			fmt.Printf("  FAIL: %s\n", r.Message)
			failed++
			continue
		}
		totalInserted += int(r.Count)
		passed++
	}
	fmt.Printf("  Inserted %d posts in %v\n", totalInserted, time.Since(postsStart).Round(time.Millisecond))
	recordsStored += totalInserted

	// Seed Timeline via BatchInsert (FIFO bucket)
	fmt.Printf("\n--- Seeding %d Timeline entries (batch=%d) ---\n", numTimeline, batchSize)
	timelineStart := time.Now()
	totalInserted = 0
	for i := 0; i < numTimeline; i += batchSize {
		end := i + batchSize
		if end > numTimeline {
			end = numTimeline
		}
		docs := make([]map[string]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, map[string]interface{}{
				"PostID": fmt.Sprintf("post_%d", rand.Intn(numPosts)),
				"UserID": fmt.Sprintf("user_%04d", rand.Intn(numUsers)),
			})
		}
		req := server.BatchInsertRequest{Database: "SocialBench", Bucket: "Timeline", Data: docs}
		var r server.BatchInsertResponse
		if err := send(transport.OpBatchPut, &req, &r); err != nil {
			fmt.Printf("  FAIL: %v\n", err)
			failed++
			continue
		}
		if !r.Success {
			fmt.Printf("  FAIL: %s\n", r.Message)
			failed++
			continue
		}
		totalInserted += int(r.Count)
		passed++
	}
	fmt.Printf("  Inserted %d timeline entries in %v\n", totalInserted, time.Since(timelineStart).Round(time.Millisecond))
	recordsStored += totalInserted

	// Seed Notifications via BatchInsert (Heap bucket)
	fmt.Printf("\n--- Seeding %d Notifications (batch=%d) ---\n", numNotifications, batchSize)
	notifStart := time.Now()
	totalInserted = 0
	for i := 0; i < numNotifications; i += batchSize {
		end := i + batchSize
		if end > numNotifications {
			end = numNotifications
		}
		docs := make([]map[string]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, map[string]interface{}{
				"UserID": fmt.Sprintf("user_%04d", rand.Intn(numUsers)),
				"Msg":    fmt.Sprintf("Alert %d", j),
			})
		}
		req := server.BatchInsertRequest{Database: "SocialBench", Bucket: "Notifications", Data: docs}
		var r server.BatchInsertResponse
		if err := send(transport.OpBatchPut, &req, &r); err != nil {
			fmt.Printf("  FAIL: %v\n", err)
			failed++
			continue
		}
		if !r.Success {
			fmt.Printf("  FAIL: %s\n", r.Message)
			failed++
			continue
		}
		totalInserted += int(r.Count)
		passed++
	}
	fmt.Printf("  Inserted %d notifications in %v\n", totalInserted, time.Since(notifStart).Round(time.Millisecond))
	recordsStored += totalInserted

	// Seed Sessions via BatchInsert (LIFO bucket)
	fmt.Printf("\n--- Seeding %d Sessions (batch=%d) ---\n", numSessions, batchSize)
	sessStart := time.Now()
	totalInserted = 0
	for i := 0; i < numSessions; i += batchSize {
		end := i + batchSize
		if end > numSessions {
			end = numSessions
		}
		docs := make([]map[string]interface{}, 0, end-i)
		for j := i; j < end; j++ {
			docs = append(docs, map[string]interface{}{
				"UserID": fmt.Sprintf("user_%04d", rand.Intn(numUsers)),
				"IP":     fmt.Sprintf("10.0.%d.%d", rand.Intn(255), rand.Intn(255)),
			})
		}
		req := server.BatchInsertRequest{Database: "SocialBench", Bucket: "Session", Data: docs}
		var r server.BatchInsertResponse
		if err := send(transport.OpBatchPut, &req, &r); err != nil {
			fmt.Printf("  FAIL: %v\n", err)
			failed++
			continue
		}
		if !r.Success {
			fmt.Printf("  FAIL: %s\n", r.Message)
			failed++
			continue
		}
		totalInserted += int(r.Count)
		passed++
	}
	fmt.Printf("  Inserted %d sessions in %v\n", totalInserted, time.Since(sessStart).Round(time.Millisecond))
	recordsStored += totalInserted

	// Verify
	total := numUsers + numPosts + numTimeline + numNotifications + numSessions
	fmt.Printf("\n--- Verification ---\n")
	fmt.Printf("  Total records: %d\n", total)

	// Read the entries back from storage. The counts returned by the insert
	// calls above are the server's own echo of what it accepted, so they cannot
	// catch a write that reported success but never landed — only a count read
	// back from the bucket can.
	fmt.Printf("\n--- Read-back from storage ---\n")
	readBack := []struct {
		bucket string
		want   int64
	}{
		{"User", numUsers},
		{"Post", numPosts},
		{"Timeline", numTimeline},
		{"Notifications", numNotifications},
		{"Session", numSessions},
	}
	for _, e := range readBack {
		req := server.CountRequest{Database: "SocialBench", Bucket: e.bucket}
		var r server.CountResponse
		if err := send(transport.OpCount, &req, &r); err != nil {
			fmt.Printf("  FAIL count %s: %v\n", e.bucket, err)
			failed++
			continue
		}
		if r.Count != e.want {
			fmt.Printf("  FAIL: bucket %-14s reports %d entries, expected %d\n", e.bucket, r.Count, e.want)
			failed++
			continue
		}
		fmt.Printf("  Read-back OK: %-14s %d entries\n", e.bucket, r.Count)
		passed++
	}

	// Pop FIFO
	{
		req := server.PopRequest{Database: "SocialBench", Bucket: "Timeline"}
		var r server.PopResponse
		if err := send(transport.OpFIFOPop, &req, &r); err != nil {
			fmt.Printf("  FAIL FIFO pop: %v\n", err)
			failed++
		} else {
			fmt.Println("  FIFO pop OK")
			passed++
		}
	}
	// Pop Heap
	{
		req := server.PopRequest{Database: "SocialBench", Bucket: "Notifications"}
		var r server.PopResponse
		if err := send(transport.OpHeapPop, &req, &r); err != nil {
			fmt.Printf("  FAIL Heap pop: %v\n", err)
			failed++
		} else {
			fmt.Println("  Heap pop OK")
			passed++
		}
	}
	// Pop LIFO
	{
		req := server.PopRequest{Database: "SocialBench", Bucket: "Session"}
		var r server.PopResponse
		if err := send(transport.OpLIFOPop, &req, &r); err != nil {
			fmt.Printf("  FAIL LIFO pop: %v\n", err)
			failed++
		} else {
			fmt.Println("  LIFO pop OK")
			passed++
		}
	}
	elapsed := time.Since(start)
	if recordsStored != total {
		fmt.Printf("  FAIL: stored %d records, expected %d\n", recordsStored, total)
		failed++
	}
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("  Records stored: %d (expected %d)\n", recordsStored, total)
	fmt.Printf("  Passed: %d, Failed: %d\n", passed, failed)
	fmt.Printf("  Time: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.0f records/sec\n", float64(recordsStored)/elapsed.Seconds())
	if failed > 0 {
		os.Exit(1)
	}
}
