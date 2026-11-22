package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// parallelCreateEngine is a hoard engine over a fake whose every call stays
// open for delay, and whose calls are counted in flight.
func parallelCreateEngine(delay time.Duration, depth int, progress *strings.Builder) (*hoardEngine, *fakeServer) {
	fake := newFakeServer()
	fake.callDelay.Store(int64(delay))
	var inFlight, maxInFlight atomic.Int64
	engine := &hoardEngine{
		cfg:         &config{Engine: engineHoardDB, Database: "social", Placement: "h1", OpTimeout: 1, Conc: depth},
		dial:        func(string) (wireCaller, func(), error) { return fake.dial(&inFlight, &maxInFlight), func() {}, nil },
		progressOut: progress,
	}
	return engine, fake
}

// TestSetupCreatesBucketsConcurrently: with a create that takes a fixed time,
// creating the User bucket and the Outbox FIFOs at depth 8 takes about an
// eighth of the serial time. One connection (the old loop) takes all of it.
func TestSetupCreatesBucketsConcurrently(t *testing.T) {
	const delay = time.Millisecond
	var progress strings.Builder
	engine, fake := parallelCreateEngine(delay, 8, &progress)
	buckets := 1 + outboxBuckets
	serial := time.Duration(buckets) * delay

	start := time.Now()
	if _, err := engine.setupSocial(context.Background(), tinySpec); err != nil {
		t.Fatalf("setupSocial: %v", err)
	}
	elapsed := time.Since(start)

	if len(fake.created) != buckets {
		t.Fatalf("created %d buckets; want %d", len(fake.created), buckets)
	}
	if elapsed > serial/2 {
		t.Errorf("creating %d buckets at depth 8 took %v; serial is %v, want under half of it", buckets, elapsed, serial)
	}
	if !strings.Contains(progress.String(), "seed: buckets 1000/1001 ") {
		t.Errorf("progress = %q; want a line like \"seed: buckets 1000/1001 <s>s\"", progress.String())
	}
}

// refusingCreates fails the create of one bucket and passes everything else on.
type refusingCreates struct {
	wireCaller
	bucket string
	err    error
}

func (c refusingCreates) call(op transport.Opcode, request, response interface{}) error {
	if create, ok := request.(server.CreateBucketRequest); ok && create.Name == c.bucket {
		return c.err
	}
	return c.wireCaller.call(op, request, response)
}

// TestSetupFailsWhenACreateFails: one refused create fails the whole setup and
// names the bucket.
func TestSetupFailsWhenACreateFails(t *testing.T) {
	engine, fake := parallelCreateEngine(0, 8, &strings.Builder{})
	refused := errors.New("disk full")
	engine.dial = func(string) (wireCaller, func(), error) {
		return refusingCreates{wireCaller: fake.dial(nil, nil), bucket: outboxBucket(500), err: refused}, func() {}, nil
	}
	_, err := engine.setupSocial(context.Background(), tinySpec)
	if !errors.Is(err, refused) || !strings.Contains(err.Error(), outboxBucket(500)) {
		t.Fatalf("setupSocial = %v; want an error wrapping %q and naming %s", err, refused, outboxBucket(500))
	}
}
