package main

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func ringTestWrite(elements, documents int) []ringWrite {
	writes := make([]ringWrite, 0, elements+documents)
	for i := 0; i < documents; i++ {
		writes = append(writes, ringWrite{document: &model.Document{Bucket: "Post", ID: "p"}})
	}
	for i := 0; i < elements; i++ {
		writes = append(writes, ringWrite{element: &model.Element{Bucket: "User", Owner: "u", Store: "Feed"}})
	}
	return writes
}

func TestRingFramesUnbatchedIsOneFramePerWrite(t *testing.T) {
	frames := ringFrames(ringTestWrite(9, 1), false)
	if len(frames) != 10 {
		t.Fatalf("unbatched: %d frames for 10 writes, want 10", len(frames))
	}
	for _, frame := range frames {
		if len(frame) != 1 {
			t.Fatalf("unbatched frame holds %d writes", len(frame))
		}
	}
}

func TestRingFramesBatchedPostIsOneFrame(t *testing.T) {
	frames := ringFrames(ringTestWrite(9, 1), true)
	if len(frames) != 1 || len(frames[0]) != 10 {
		t.Fatalf("batched post: %d frames, want 1 frame of 10 writes", len(frames))
	}
	if frames[0][0].document == nil {
		t.Error("the document must stay first in the frame: documents are applied before elements")
	}
}

func TestRingFramesRespectTheFrameCeiling(t *testing.T) {
	frames := ringFrames(ringTestWrite(300, 0), true)
	if len(frames) != 2 || len(frames[0]) != ringBatchMax || len(frames[1]) != 300-ringBatchMax {
		t.Fatalf("300 writes in %d frames (first %d), want 255 then 45", len(frames), len(frames[0]))
	}
	if ringBatchMax != 255 {
		t.Errorf("ringBatchMax = %d, the batch frame holds 255 operations", ringBatchMax)
	}
}

func TestRingFramesOfNothingAreNone(t *testing.T) {
	if frames := ringFrames(nil, true); len(frames) != 0 {
		t.Fatalf("no writes made %d frames", len(frames))
	}
}

func TestParseRingConfigReadsBatch(t *testing.T) {
	env := envOf(map[string]string{"HOARDDB_ROOT_PASSWORD": "secret"})
	for param, want := range map[string]bool{"1": true, "true": true, "0": false, "false": false} {
		got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "batch": param}}, env)
		if err != nil || got.batch != want {
			t.Errorf("batch=%s: got %v, %v want %v", param, got.batch, err, want)
		}
	}
	got, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true"}}, env)
	if err != nil || got.batch {
		t.Errorf("batch is off by default, got %v, %v", got.batch, err)
	}
	if _, err := parseRingConfig(Config{URI: "h:1", Params: map[string]string{"insecure": "true", "batch": "maybe"}}, env); err == nil || !strings.Contains(err.Error(), "batch") {
		t.Errorf("batch=maybe must be refused naming batch, got %v", err)
	}
}

func TestRingBatchModeNames(t *testing.T) {
	if ringBatchMode(true) != "on" || ringBatchMode(false) != "off" {
		t.Fatalf("modes %q %q", ringBatchMode(true), ringBatchMode(false))
	}
}
