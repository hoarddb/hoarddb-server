// bench/rank_hoarddb.go — the hoardDB leg of the keyed rank (rank_bench.go).
//
// The same wire calls bench/smokeperf's keyedLike makes (OpUpsert, OpGet,
// OpBTreeRange) on one authenticated TLS socket from bench/tcp_client.go, so
// exactly one request is in flight.
package main

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// rankHoardDBDatabase is created by the leg, so a server that already holds it
// — a leg run against a data dir that is not fresh — fails at setup.
const rankHoardDBDatabase = "rank"

type rankHoardDB struct {
	ctx    context.Context
	client *tcpBenchClient
	legID  rankLeg
}

// rankHoardDBDoc is how a stored document decodes from a FindResponse.
type rankHoardDBDoc struct {
	Key     string `bson:"_id_"`
	Seq     int64  `bson:"seq"`
	Payload string `bson:"payload"`
}

func newRankHoardDB(ctx context.Context, addr string, leg rankLeg) (*rankHoardDB, error) {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Insecure: &transport.InsecureTrust{Reason: "rank benchmark harness"}}, addr)
	if err != nil {
		return nil, fmt.Errorf("building the TLS config for %s: %w", addr, err)
	}
	client, err := newTCPBenchClient(ctx, []string{addr}, tlsCfg, 1)
	if err != nil {
		return nil, fmt.Errorf("connecting to hoardDB at %s: %w", addr, err)
	}
	engine := &rankHoardDB{ctx: ctx, client: client, legID: leg}
	if err := engine.call(transport.OpCreateDB, server.CreateDatabaseRequest{Name: rankHoardDBDatabase}, nil); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("creating database %q (is the data dir fresh?): %w", rankHoardDBDatabase, err)
	}
	return engine, nil
}

// call sends one request and decodes its response, turning an OpError frame
// into an error carrying the server's message.
func (h *rankHoardDB) call(op transport.Opcode, request, response interface{}) error {
	payload, err := bson.Marshal(request)
	if err != nil {
		return fmt.Errorf("encoding the %s request: %w", op, err)
	}
	frame, err := h.client.SendRequest(h.ctx, op, payload)
	if err != nil {
		return fmt.Errorf("sending %s: %w", op, err)
	}
	if frame.Opcode == transport.OpError {
		var serverErr server.ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &serverErr); err != nil {
			return fmt.Errorf("%s failed and its error frame did not decode: %w", op, err)
		}
		return fmt.Errorf("%s failed: %s", op, serverErr.Message)
	}
	if response == nil {
		return nil
	}
	if err := bson.Unmarshal(frame.Payload, response); err != nil {
		return fmt.Errorf("decoding the %s response: %w", op, err)
	}
	return nil
}

func (h *rankHoardDB) leg() rankLeg { return h.legID }

// createdKnown is false: OpUpsert's response does not say whether the key was new.
// Freshness is enforced instead by creating the database at setup.
func (h *rankHoardDB) createdKnown() bool { return false }

func (h *rankHoardDB) close() error { return h.client.Close() }

func (h *rankHoardDB) open(store string) (rankStore, error) {
	bucketType, bucketName := server.BucketTypeHash, "users"
	if store == "btree" {
		bucketType, bucketName = server.BucketTypeBTree, "logs"
	}
	if err := h.call(transport.OpCreateBucket, server.CreateBucketRequest{Database: rankHoardDBDatabase, Name: bucketName, Type: bucketType}, nil); err != nil {
		return nil, fmt.Errorf("creating bucket %q: %w", bucketName, err)
	}
	return &rankHoardDBBucket{engine: h, bucket: bucketName}, nil
}

type rankHoardDBBucket struct {
	engine *rankHoardDB
	bucket string
}

func (b *rankHoardDBBucket) upsert(doc rankDoc) (bool, error) {
	var putResponse server.InsertResponse
	request := server.InsertRequest{
		Database: rankHoardDBDatabase,
		Bucket:   b.bucket,
		Key:      doc.Key,
		Data:     map[string]interface{}{"_id_": doc.Key, "seq": doc.Seq, "payload": doc.Payload},
	}
	if err := b.engine.call(transport.OpUpsert, request, &putResponse); err != nil {
		return false, err
	}
	if !putResponse.Success {
		return false, fmt.Errorf("put was not acknowledged: %s", putResponse.Message)
	}
	return false, nil
}

func (b *rankHoardDBBucket) read(key string) (rankDoc, bool, error) {
	var findResponse server.FindResponse
	if err := b.engine.call(transport.OpGet, server.FindRequest{Database: rankHoardDBDatabase, Bucket: b.bucket, Key: key}, &findResponse); err != nil {
		return rankDoc{}, false, err
	}
	if !findResponse.Found {
		return rankDoc{}, false, nil
	}
	var stored rankHoardDBDoc
	if err := bson.Unmarshal(findResponse.Data, &stored); err != nil {
		return rankDoc{}, true, fmt.Errorf("decoding document %s: %w", key, err)
	}
	return rankDoc(stored), true, nil
}

func (b *rankHoardDBBucket) scan(low, high string) ([]rankDoc, error) {
	var rangeResponse server.BTreeRangeResponse
	request := server.BTreeRangeRequest{Database: rankHoardDBDatabase, Bucket: b.bucket, Low: low, High: high}
	if err := b.engine.call(transport.OpBTreeRange, request, &rangeResponse); err != nil {
		return nil, err
	}
	band := make([]rankDoc, 0, len(rangeResponse.Entries))
	// Each entry's Value is the stored document as raw BSON, so it
	// decodes exactly as a point read's FindResponse.Data does in read. The
	// decode stays inside the timed window, as the map decode did at a0d2788:
	// the op is what a client pays to hold the band as values.
	for _, entry := range rangeResponse.Entries {
		var stored rankHoardDBDoc
		if err := bson.Unmarshal(entry.Value, &stored); err != nil {
			return nil, fmt.Errorf("decoding range entry %s: %w", entry.Key, err)
		}
		band = append(band, rankDoc(stored))
	}
	return band, nil
}
