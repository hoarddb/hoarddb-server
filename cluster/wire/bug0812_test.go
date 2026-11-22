package wire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// BUG-0812: an answer that carries an ApplyFailure (the record is durable, its apply failed, and
// nodeSender.Write has replayed the pair without error) was reported to the client as a success
// by the insert and by the declaration writes of create, alter and drop bucket. Node.send turns
// the same answer into an UNAVAILABLE refusal; these tests hold the other four to it.

// bug0812Refusal fails the test unless err is an UNAVAILABLE refusal that says the write is
// durable and names the bucket, and returns its message.
func bug0812Refusal(t *testing.T, err error, bucket string) string {
	t.Helper()
	if err == nil {
		t.Fatal("the answer carried an apply failure and the client was told success (err is nil)")
	}
	message := u39Code(t, err, cluster.CodeUnavailable)
	for _, want := range []string{bucket, "durable", "applying it failed"} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q does not contain %q", message, want)
		}
	}
	if !strings.Contains(message, "again") && !strings.Contains(message, "Retry") {
		t.Errorf("the refusal %q does not say what to do", message)
	}
	return message
}

// bug0812AnswerWithApplyFailure is what nodeSender.Write returns after a row 9 whose replay
// succeeded: the owner's answer, unchanged, with the failure still on it.
func bug0812AnswerWithApplyFailure(answer WriteAnswer) WriteAnswer {
	answer.ApplyFailure = errors.New("disk on fire")
	return answer
}

// TestAnInsertWhoseApplyFailedIsNotAnsweredApplied is the repro of the bug file: the first
// insert is durable in the log and its apply fails, and Node.Insert must not answer it as a
// success. [BREAK: drop answer.ApplyFailure in Coordinator.Insert]
func TestAnInsertWhoseApplyFailedIsNotAnsweredApplied(t *testing.T) {
	node := s0OpenClusterNode(t, t.TempDir(), nil)
	release := s0HoldPasses(node)
	defer release()
	s0Schema(t, node)
	ids, _ := s0IDsInOneSlot(t, 1)
	documents, ok := node.writer.cfg.Applier.(*DocumentApplier)
	if !ok {
		t.Fatalf("the node's applier is %T, want *DocumentApplier", node.writer.cfg.Applier)
	}
	node.writer.cfg.Applier = &bug0809FailFirstInsert{DocumentApplier: documents}

	doc := bson.D{{Key: "_id_", Value: ids[0]}, {Key: "Name", Value: ids[0]}}
	inserted, err := node.Insert(context.Background(), u30iDB, "User", doc, WithRequestID([16]byte{0x08, 0x12, 1}))
	t.Logf("the insert whose apply failed answered %+v, err %v", inserted, err)
	bug0812Refusal(t, err, "User")
}

// TestADeclarationWriteWhoseApplyFailedIsNotAcknowledged covers create, alter and drop bucket:
// each orders its ring document through write/Run and reads only the answer's Outcome.
func TestADeclarationWriteWhoseApplyFailedIsNotAcknowledged(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		node, _ := u30iFresh(t)
		dbID, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		config := node.creator.cfg
		config.Sender = &u40Sender{inner: config.Sender, answer: bug0812AnswerWithApplyFailure}
		_, err = NewCreateBucket(config).Run(context.Background(), dbID, "Order", []FieldEntry{{Type: "int", Name: "Total"}})
		message := bug0812Refusal(t, err, "Order")
		if !strings.Contains(message, "create bucket Order") {
			t.Errorf("the refusal %q does not name the command to run again", message)
		}
	})
	t.Run("alter", func(t *testing.T) {
		rig := newU40Rig(t)
		rig.sender.answer = bug0812AnswerWithApplyFailure
		_, err := rig.alter.Run(context.Background(), rig.dbID, "User", u40Add("Email", "string"))
		message := bug0812Refusal(t, err, "User")
		if !strings.Contains(message, "alter bucket User") {
			t.Errorf("the refusal %q does not name the command to run again", message)
		}
	})
	t.Run("drop", func(t *testing.T) {
		node, _ := u30iFresh(t)
		dbID, err := node.database(u30iDB)
		if err != nil {
			t.Fatal(err)
		}
		config := node.dropper.cfg
		config.Sender = &u40Sender{inner: config.Sender, answer: bug0812AnswerWithApplyFailure}
		err = NewDropBucket(config).Run(context.Background(), dbID, "User")
		message := bug0812Refusal(t, err, "User")
		if !strings.Contains(message, "drop bucket User") {
			t.Errorf("the refusal %q does not name the command to run again", message)
		}
	})
}
