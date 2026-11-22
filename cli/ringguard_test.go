package cli

import (
	"errors"
	"strings"
	"testing"
)

// TestUpdateParsesIfAndReturn: a guarded update carries the two options to the owner,
// and a guard without an operator is still refused by the operator check.
func TestUpdateParsesIfAndReturn(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Order { Status string, Total int }",
		`db.Order.insert({_id_: "o1", Status: "processing", Total: 1})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, `db.Order.update({_id_: "o1", if: {Status: "processing"}, return: "after", $set: {Status: "shipped"}})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "OK  updated id=o1")
	counts := fake.buckets["app.Order"].optionCounts
	if len(counts) != 1 || counts[0] != 2 {
		t.Fatalf("the bucket was sent %v options, want one update with 2", counts)
	}
	_, err = u52Run(session, `db.Order.update({_id_: "o1", if: {Status: "processing"}})`)
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "an operator") {
		t.Fatalf("a guard without an operator: %v, want PARSE_ERROR naming an operator", err)
	}
}

// TestUpsertWithIfIsRefused: an upsert has no document to guard, and says so before sending.
func TestUpsertWithIfIsRefused(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Shipment { Status string }",
		`db.Shipment.insert({_id_: "s1", Status: "processing"})`); err != nil {
		t.Fatal(err)
	}
	_, err := u52Run(session, `db.Shipment.upsert({_id_: "s1", if: {Status: "processing"}, Status: "shipped"})`)
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "an upsert has no document to guard") {
		t.Fatalf("an upsert with if: %v, want PARSE_ERROR about a document to guard", err)
	}
	if doc := fake.buckets["app.Shipment"].docs["s1"]; doc["Status"] != "processing" {
		t.Fatalf("the refused upsert changed the document to %v", doc["Status"])
	}
}

// TestDeleteParsesIfAndRefusesReturnAfter: a delete guards with if and returns before, and
// a delete has no after to return.
func TestDeleteParsesIfAndRefusesReturnAfter(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Order { Status string }",
		`db.Order.insert({_id_: "o1", Status: "processing"})`,
		`db.Order.insert({_id_: "o2", Status: "processing"})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, `db.Order.delete({_id_: "o1", if: {Status: "processing"}, return: "before"})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "OK  deleted id=o1")
	if counts := fake.buckets["app.Order"].optionCounts; len(counts) != 1 || counts[0] != 2 {
		t.Fatalf("the bucket was sent %v options, want one delete with 2", counts)
	}
	_, err = u52Run(session, `db.Order.delete({_id_: "o2", return: "after"})`)
	var failed *RingError
	if !errors.As(err, &failed) || !strings.Contains(failed.Message, "a delete has no after") {
		t.Fatalf("a delete with return after: %v, want the no-after refusal", err)
	}
	if _, still := fake.buckets["app.Order"].docs["o2"]; !still {
		t.Fatal("the refused delete removed the document")
	}
}

// TestIfRefusesAnOperator: a guard is equalities, and a range refusal names the alternative.
func TestIfRefusesAnOperator(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Order { Status string, Total int }",
		`db.Order.insert({_id_: "o1", Status: "processing", Total: 1})`); err != nil {
		t.Fatal(err)
	}
	_, err := u52Run(session, `db.Order.update({_id_: "o1", if: {Total: {$gt: 3}}, $set: {Status: "shipped"}})`)
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "equalities only") {
		t.Fatalf("a range guard: %v, want PARSE_ERROR saying equalities only", err)
	}
}

// TestNotAppliedPrintsTheFieldAndTheCurrentDocument: a refused guard is a refusal that
// shows the field, the document and the next command.
func TestNotAppliedPrintsTheFieldAndTheCurrentDocument(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Order { Status string }",
		`db.Order.insert({_id_: "refuse-me", Status: "processing"})`); err != nil {
		t.Fatal(err)
	}
	_, err := u52Run(session, `db.Order.update({_id_: "refuse-me", if: {Status: "shipped"}, $set: {Status: "done"}})`)
	var failed *RingError
	if !errors.As(err, &failed) {
		t.Fatalf("a refused guard: %v, want a *RingError", err)
	}
	for _, said := range []string{"NOT APPLIED", "Status is", "Nothing was changed", "Re-read with", `"cancelled"`, `not "shipped"`} {
		if !strings.Contains(failed.Message, said) {
			t.Errorf("the refusal does not say %q:\n%s", said, failed.Message)
		}
	}
	if failed.Exit != ExitRefused {
		t.Fatalf("the refusal exits %d, want %d", failed.Exit, ExitRefused)
	}
}

func TestUpdateHelpShowsTheGuardExample(t *testing.T) {
	for topic, wants := range map[string][]string{
		"update": {`if: {Name: "Ada"}`, `return: "after"`, "null matches a field that was never set", "An upsert takes no if"},
		"delete": {`if: {Name: "Ada"}`, `return: "before"`},
	} {
		page, err := HelpFor(topic)
		if err != nil {
			t.Fatalf("help %s: %v", topic, err)
		}
		for _, want := range wants {
			if !strings.Contains(page, want) {
				t.Errorf("help %s does not show %q:\n%s", topic, want, page)
			}
		}
	}
}

// TestInsertAndUpsertRefuseIfAndReturn: if and return are options of update and delete, never
// document fields of an insert or an upsert, even when the bucket declares a field of that
// name; the statement is refused before anything is sent.
func TestInsertAndUpsertRefuseIfAndReturn(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket Note { Name string, return string, if string }"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`db.Note.insert({_id_: "n1", return: "after"})`,
		`db.Note.upsert({_id_: "n1", return: "after", Name: "x"})`,
		`db.Note.insert({_id_: "n1", if: {Name: "x"}})`,
		`db.Note.upsert({_id_: "n1", if: 5})`,
		`db.Note.upsert({_id_: "n1", if: "x"})`,
	} {
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "update") {
			t.Errorf("%s: %v, want PARSE_ERROR naming update", statement, err)
		}
	}
	if docs := fake.buckets["app.Note"].docs; len(docs) != 0 {
		t.Fatalf("a refused statement stored %v", docs)
	}
}
