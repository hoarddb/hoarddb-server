package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// witnessWrites inserts documents one at a time into a bucket of a scratch database, on one
// connection. The ring wire cannot drop a database, so the scratch database stays on the
// node; its name carries the time, so a later run never collides with it.
func (r *ringEngine) witnessWrites(ctx context.Context, timed int) ([]time.Duration, error) {
	client, err := r.dialClient(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.Close() }()
	scratch := witnessNamespace(r.cfg.Database)
	if _, err := client.CreateDatabase(ctx, scratch, 0); err != nil {
		return nil, fmt.Errorf("creating the scratch database %q: %w", scratch, err)
	}
	if _, err := client.CreateBucket(ctx, scratch, "w", []ringclient.FieldDecl{{Name: "n", Type: "int"}}); err != nil {
		return nil, fmt.Errorf("creating the scratch bucket in %q: %w", scratch, err)
	}
	bucket := client.Bucket(scratch, "w")
	return timeWrites(timed, func(sequence int) error {
		result, err := bucket.Insert(ctx, strconv.Itoa(sequence), ringclient.Doc{"n": int64(sequence)})
		return applied(result, err)
	})
}

// checkEncryption proves the connections the load client opens are encrypted: the TLS session
// a connection gets and a plaintext probe (probeNodeEncryption), then a real login through the
// load client's own dial, so the path a leg uses is the one proved.
func (r *ringEngine) checkEncryption(ctx context.Context) ([]tlsFinding, error) {
	finding := probeNodeEncryption(r.cfg.Addr, r.cfg.OpTimeout)
	if finding.Encrypted {
		client, err := r.dialClient(ctx)
		if err != nil {
			finding.Encrypted, finding.Detail = false, fmt.Sprintf("the load client cannot log in over its own dial: %v", err)
		} else {
			_ = client.Close()
		}
	}
	return []tlsFinding{finding}, nil
}
