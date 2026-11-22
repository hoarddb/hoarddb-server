package server

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 8.2: the two log lines of spec §19.3.13. A stream that leaves the
// in-sync set says so once, with the peer and the reason (and the bucket, for
// a refusal that names one); one that returns says so once, with how long it
// was out and how many entries were replayed.

// awaitingCopyConn answers every replication push that carries entries with
// the receiver's awaiting_copy refusal of its first entry while refuse is set,
// as a node does for a bucket whose copy has not landed. Everything else
// passes.
type awaitingCopyConn struct {
	peerConn
	refuse *atomic.Bool
}

func (a *awaitingCopyConn) refusal(payload []byte) (*transport.Frame, bool, error) {
	if !a.refuse.Load() {
		return nil, false, nil
	}
	var req ReplPushRequest
	if err := bson.Unmarshal(payload, &req); err != nil {
		return nil, false, fmt.Errorf("decoding a push the test refuses: %w", err)
	}
	if len(req.Entries) == 0 {
		return nil, false, nil
	}
	first := req.Entries[0]
	answer, err := bson.Marshal(ReplPushResponse{
		Success:        false,
		AppliedThrough: first.Seq - 1,
		Refusal:        refusalAwaitingCopy,
		Error:          fmt.Sprintf("bucket %q: this node is placed on it and does not hold it yet", first.Bucket),
	})
	if err != nil {
		return nil, false, fmt.Errorf("encoding the test's refusal: %w", err)
	}
	return &transport.Frame{Opcode: transport.OpReplPush, Payload: answer}, true, nil
}

func (a *awaitingCopyConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		if frame, refused, err := a.refusal(payload); err != nil || refused {
			return frame, err
		}
	}
	return a.peerConn.SendRequest(ctx, lane, op, payload)
}

func (a *awaitingCopyConn) SendRequestIfUnchanged(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte, generation uint64) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		if frame, refused, err := a.refusal(payload); err != nil || refused {
			return frame, err
		}
	}
	return a.peerConn.SendRequestIfUnchanged(ctx, lane, op, payload, generation)
}

// streamLines returns the log lines at level about the stream to nodeID: the
// lines whose message is about "replication to a peer" and which name it.
func streamLines(logged *syncBuffer, level, nodeID string) []string {
	var lines []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "level="+level) && strings.Contains(line, `msg="replication to a peer`) &&
			strings.Contains(line, "node_id="+nodeID+" ") {
			lines = append(lines, line)
		}
	}
	return lines
}

// waitForStreamLines polls until the log holds at least one line at level
// about nodeID, and returns them all.
func waitForStreamLines(t *testing.T, logged *syncBuffer, level, nodeID, what string) []string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if lines := streamLines(logged, level, nodeID); len(lines) > 0 {
			return lines
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no %s line about the stream to %s within 60 s", what, level, nodeID)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

var entriesReplayed = regexp.MustCompile(`entries_replayed=(\d+)`)

// TestAStreamThatLeavesTheSetSaysWhyOnce is step 8.2's test. The owner of a
// bucket on three nodes at factor 3 logs one WARN, with a reason, when its
// stream to a stopped replica stalls, however many times the sender retries;
// one INFO, with how long it was out and how many entries it replayed, when
// the replica returns; and, for a receiver's awaiting_copy refusal, one WARN
// that names the bucket.
func TestAStreamThatLeavesTheSetSaysWhyOnce(t *testing.T) {
	logs := make([]*syncBuffer, 3)
	for i := range logs {
		logs[i] = &syncBuffer{}
	}
	tc := newTestClusterTuned(t, 3, 3, func(_ int, cfg *config.Config) {
		// No retention pass: its advance notices are not what is under test.
		cfg.Server.WALRetentionSeconds = 3600
	}, func(index int, c *Cluster) {
		c.logger = slog.New(slog.NewTextHandler(logs[index], &slog.HandlerOptions{Level: slog.LevelInfo}))
	})
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	otherIndex := 3 - ownerIndex - replicaIndex
	ownerLog := logs[ownerIndex]
	replicaID, otherID := tc.addrs[replicaIndex], tc.addrs[otherIndex]
	ownerClient := tc.nodes[ownerIndex].client
	if answer, err := writeConfirmed(ownerClient, "before", "all"); err != nil || answer.confirmed != 3 {
		t.Fatalf("test setup: the write before the stop was answered %+v (%v), want copies_confirmed 3", answer, err)
	}

	base := tc.stopNodeKeepingData(replicaIndex)
	const whileDown = 20
	for i := 0; i < whileDown; i++ {
		if _, err := writeConfirmed(ownerClient, fmt.Sprintf("down-%02d", i), "one"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The sender retries at 0.5, 1 and 2 s: let it fail several times more.
	time.Sleep(4 * time.Second)
	warns := streamLines(ownerLog, "WARN", replicaID)
	if len(warns) != 1 {
		t.Fatalf("a stopped replica: %d WARN lines about the stream to it, want 1:\n%s", len(warns), strings.Join(warns, "\n"))
	}
	if !strings.Contains(warns[0], "reason=") {
		t.Errorf("the WARN for a stopped replica gives no reason: %s", warns[0])
	}

	tc.nodes[replicaIndex] = tc.startNode(replicaIndex, base, tc.token)
	waitForStreamLines(t, ownerLog, "INFO", replicaID, "the stopped replica returned")
	time.Sleep(time.Second)
	infos := streamLines(ownerLog, "INFO", replicaID)
	if len(infos) != 1 {
		t.Fatalf("the replica returned: %d INFO lines about the stream to it, want 1:\n%s", len(infos), strings.Join(infos, "\n"))
	}
	if !strings.Contains(infos[0], "out_for=") {
		t.Errorf("the INFO for a returned replica does not say how long it was out: %s", infos[0])
	}
	match := entriesReplayed.FindStringSubmatch(infos[0])
	if match == nil {
		t.Fatalf("the INFO for a returned replica does not say how many entries were replayed: %s", infos[0])
	}
	if replayed, _ := strconv.Atoi(match[1]); replayed < whileDown {
		t.Errorf("the INFO for a returned replica says %d entries were replayed; %d were written while it was down", replayed, whileDown)
	}
	if warns := streamLines(ownerLog, "WARN", replicaID); len(warns) != 1 {
		t.Errorf("after the replica returned: %d WARN lines about the stream to it, want still 1:\n%s", len(warns), strings.Join(warns, "\n"))
	}

	var refuse atomic.Bool
	refuse.Store(true)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	wrapPeerConn(ownerCluster.peerFor(mustNode(t, ownerCluster, otherID)), func(inner peerConn) peerConn {
		return &awaitingCopyConn{peerConn: inner, refuse: &refuse}
	})
	for i := 0; i < 5; i++ {
		if _, err := writeConfirmed(ownerClient, fmt.Sprintf("refused-%d", i), "one"); err != nil {
			t.Fatal(err)
		}
	}
	waitForStreamLines(t, ownerLog, "WARN", otherID, "a receiver's awaiting_copy refusal")
	time.Sleep(3 * time.Second)
	refusals := streamLines(ownerLog, "WARN", otherID)
	if len(refusals) != 1 {
		t.Fatalf("an awaiting_copy refusal: %d WARN lines about the stream, want 1:\n%s", len(refusals), strings.Join(refusals, "\n"))
	}
	if !strings.Contains(refusals[0], "bucket="+testBucket) {
		t.Errorf("the WARN for an awaiting_copy refusal does not name the bucket %q: %s", testBucket, refusals[0])
	}
	refuse.Store(false)
	waitForStreamLines(t, ownerLog, "INFO", otherID, "the refusing replica accepted again")
}
