package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParsePhasesKeepsRunOrderAndRejectsUnknownNames(t *testing.T) {
	got, err := parsePhases("get_serial, insert_serial")
	if err != nil || !reflect.DeepEqual(got, []string{"insert_serial", "get_serial"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if got, err := parsePhases(""); err != nil || got != nil {
		t.Fatalf("empty list gave %v, %v", got, err)
	}
	if _, err := parsePhases("insert_serial,bogus"); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown phase accepted: %v", err)
	}
}

func TestExecuteRunsOnlyTheSelectedPhase(t *testing.T) {
	cfg := smokeConfig(t, engineMySQL)
	cfg.Phases = []string{"insert_serial"}
	eng := newFakeEngine(map[string]string{"innodb_flush_log_at_trx_commit": "1", "sync_binlog": "1"})
	if err := execute(context.Background(), cfg, eng, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(eng.store.docs) != 64 {
		t.Fatalf("store holds %d documents, want exactly the 64 serial inserts and no preload", len(eng.store.docs))
	}
}

func TestLedgerHoldsOneLinePerAckAndVerifyFindsAMissingID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ack.ledger")
	ledger, err := openAckLedger(path, "database=d bucket=1 decl=1 seen=1 payload_field=1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := ledger.record(fmt.Sprintf("id-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.close(); err != nil {
		t.Fatal(err)
	}
	// a torn last line was never acknowledged by the ledger and is dropped
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = file.WriteString("id-torn")
	_ = file.Close()
	header, ids, err := readLedger(path)
	if err != nil || len(ids) != 5 || !strings.HasPrefix(header, "database=d") {
		t.Fatalf("header %q ids %v err %v", header, ids, err)
	}
	eng := newFakeEngine(nil)
	worker, _ := eng.newWorker(context.Background())
	for _, id := range ids {
		if id != "id-3" {
			_ = worker.insert(context.Background(), id, "x")
		}
	}
	missing, err := verifyIDs(context.Background(), worker, ids)
	if err != nil || !reflect.DeepEqual(missing, []string{"id-3"}) {
		t.Fatalf("missing %v err %v", missing, err)
	}
}

func TestExecuteWithALedgerRecordsEveryAckedInsert(t *testing.T) {
	cfg := smokeConfig(t, engineMySQL)
	cfg.Phases = []string{"insert_serial", "insert_parallel"}
	cfg.Ledger = filepath.Join(t.TempDir(), "ack.ledger")
	eng := &ledgerFakeEngine{fakeEngine: newFakeEngine(map[string]string{"innodb_flush_log_at_trx_commit": "1", "sync_binlog": "1"})}
	if err := execute(context.Background(), cfg, eng, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	_, ids, err := readLedger(cfg.Ledger)
	if err != nil || len(ids) != 128 {
		t.Fatalf("ledger holds %d ids (err %v), want 128", len(ids), err)
	}
	for _, id := range ids {
		if _, ok := eng.store.docs[id]; !ok {
			t.Fatalf("ledger id %s was never stored", id)
		}
	}
	if _, err := openAckLedger(cfg.Ledger, "x"); err == nil {
		t.Fatal("an existing ledger was overwritten")
	}
}

func TestVerifyStopsOnATransportErrorInsteadOfCallingItMissing(t *testing.T) {
	_, err := verifyIDs(context.Background(), brokenGetWorker{}, []string{"a"})
	if err == nil || errors.Is(err, errNotFound) {
		t.Fatalf("err %v", err)
	}
}

type ledgerFakeEngine struct{ *fakeEngine }

func (e *ledgerFakeEngine) ledgerHeader() string {
	return "database=d bucket=1 decl=1 seen=1 payload_field=1"
}

type brokenGetWorker struct{}

func (brokenGetWorker) insert(context.Context, string, string) error { return nil }
func (brokenGetWorker) get(context.Context, string) error            { return errors.New("connection reset") }
func (brokenGetWorker) close()                                       {}
