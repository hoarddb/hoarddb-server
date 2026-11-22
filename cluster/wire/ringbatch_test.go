package wire

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func ringBatchOps() []cluster.Op {
	ops := u65EveryWriteClass()
	return ops
}

func TestRingBatchRoundTripsEveryWriteClass(t *testing.T) {
	req := RingBatchRequest{Prologue: u65Prologue(), Ops: ringBatchOps()}
	raw, err := EncodeRingBatch(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Prologue != req.Prologue || len(got.Ops) != len(req.Ops) {
		t.Fatalf("decoded %+v, want %d ops under %+v", got.Prologue, len(req.Ops), req.Prologue)
	}
	again, err := EncodeRingBatch(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encode differs: %v", err)
	}
}

// TestRingBatchBodiesAreByteIdenticalToKeyedBodies: [GUARD each body is a RING_KEYED body].
func TestRingBatchBodiesAreByteIdenticalToKeyedBodies(t *testing.T) {
	ops := ringBatchOps()[:3]
	raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ops})
	if err != nil {
		t.Fatal(err)
	}
	want := raw[:RingPrologueLen:RingPrologueLen]
	want = append(want, byte(len(ops)))
	for _, op := range ops {
		keyed, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: op})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, keyed[RingPrologueLen:]...)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("batch payload\n got %x\nwant %x", raw, want)
	}
}

func TestARingBatchOfZeroOpsIsRefused(t *testing.T) {
	_, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue()})
	u65AssertRefused(t, err, "at least one")
	raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ringBatchOps()[:1]})
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw[:RingPrologueLen], 0)
	_, err = DecodeRingBatch(raw)
	u65AssertRefused(t, err, "at least one")
}

func TestARingBatchWithTrailingBytesIsRefused(t *testing.T) {
	raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ringBatchOps()[:2]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingBatch(append(bytes.Clone(raw), 0))
	u65AssertRefused(t, err, "follow", "2 operations")
}

func TestARingBatchRefusesAReadClassNamingTheIndex(t *testing.T) {
	ops := ringBatchOps()[:2]
	raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ops})
	if err != nil {
		t.Fatal(err)
	}
	read, err := encodeRingRead(u65EveryRead()[0])
	if err != nil {
		t.Fatal(err)
	}
	raw = append(bytes.Clone(raw), read...)
	raw[RingPrologueLen] = 3
	_, err = DecodeRingBatch(raw)
	u65AssertRefused(t, err, "operation 2", "read")
}

func TestARingBatchOf256IsRefusedByTheEncoderWithTheSplitText(t *testing.T) {
	ops := make([]cluster.Op, 256)
	for i := range ops {
		ops[i] = ringBatchOps()[3]
	}
	_, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ops})
	u65AssertRefused(t, err, "256", "at most 255", "Split it into batches of 255 or fewer")
	if _, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ops[:255]}); err != nil {
		t.Fatalf("255 operations refused: %v", err)
	}
}

func TestARingBatchNamesTheOperationWhereDecodingStopped(t *testing.T) {
	raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ringBatchOps()[:3]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingBatch(raw[:len(raw)-3])
	u65AssertRefused(t, err, "operation 2")
	raw[RingPrologueLen] = 4
	_, err = DecodeRingBatch(raw)
	u65AssertRefused(t, err, "operation 3")
}

func TestRingBatchDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, count := range []int{1, 3, len(ringBatchOps())} {
		raw, err := EncodeRingBatch(RingBatchRequest{Prologue: u65Prologue(), Ops: ringBatchOps()[:count]})
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	rng := rand.New(rand.NewPCG(0xe0, 0xe0))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		req, err := DecodeRingBatch(mutated)
		if err != nil {
			continue
		}
		accepted++
		if again, err := EncodeRingBatch(req); err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted batch %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutant was accepted: the re-encode check never ran")
	}
}
