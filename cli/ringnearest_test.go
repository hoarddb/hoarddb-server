package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// nearestVector is a $binary literal holding the two float32s 1.0 and 0.0, little-endian.
const nearestVector = `{"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}`

// nearestCall builds the nearest call of Friends on user ada from a document literal.
func nearestCall(t *testing.T, literal string) ringStoreCall {
	t.Helper()
	return ringStoreCall{
		verb:   "nearest",
		name:   "User",
		id:     "ada",
		field:  ringclient.FieldInfo{Name: "Friends"},
		values: []Value{ringLiteralDoc(t, literal)},
	}
}

// nearestLast is the last printed line, or "" when nothing was printed.
func nearestLast(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	return lines[len(lines)-1]
}

// nearestRaw encodes one element document as the owner returns it.
func nearestRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return bson.Raw(encoded)
}

// TestNearestLinesPrintScoreKeyElement pins the column order: score, key, element.
func TestNearestLinesPrintScoreKeyElement(t *testing.T) {
	session, _ := u52Session()
	result := ringclient.NearestResult{
		Elements: []bson.Raw{nearestRaw(t, bson.D{{Key: "Name", Value: "Grace"}}), nearestRaw(t, bson.D{{Key: "Name", Value: "Linus"}})},
		Keys:     []string{"grace", "linus"},
		Scores:   []float64{1, 0.5},
		Scanned:  2,
	}
	lines, err := session.nearestLines(ringStoreCall{verb: "nearest"}, result, "Vector")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("lines %q, want two hits", lines)
	}
	if !strings.HasPrefix(lines[0], "1.000000  grace  ") || !strings.Contains(lines[0], "Grace") {
		t.Fatalf("first line %q, want score, key, element", lines[0])
	}
	if !strings.HasPrefix(lines[1], "0.500000  linus  ") {
		t.Fatalf("second line %q, want score 0.500000 then key linus", lines[1])
	}
}

// TestNearestLinesSkippedLine pins the line that says elements were never embedded in the field.
func TestNearestLinesSkippedLine(t *testing.T) {
	session, _ := u52Session()
	result := ringclient.NearestResult{
		Elements: []bson.Raw{nearestRaw(t, bson.D{{Key: "Name", Value: "Grace"}})},
		Keys:     []string{"grace"},
		Scores:   []float64{0.9},
		Skipped:  2,
	}
	lines, err := session.nearestLines(ringStoreCall{verb: "nearest"}, result, "Vector")
	if err != nil {
		t.Fatal(err)
	}
	if got := nearestLast(lines); got != `skipped: 2 elements without field "Vector"` {
		t.Fatalf("last line %q, want the plural skipped line", got)
	}
	result.Skipped = 1
	lines, err = session.nearestLines(ringStoreCall{verb: "nearest"}, result, "Vector")
	if err != nil {
		t.Fatal(err)
	}
	if got := nearestLast(lines); got != `skipped: 1 element without field "Vector"` {
		t.Fatalf("last line %q, want the singular skipped line", got)
	}
}

// TestNearestLinesEmpty pins what a developer sees when nothing matched.
func TestNearestLinesEmpty(t *testing.T) {
	session, _ := u52Session()
	lines, err := session.nearestLines(ringStoreCall{verb: "nearest"}, ringclient.NearestResult{}, "Vector")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0] != "(empty)" {
		t.Fatalf("no hits and nothing skipped printed %q, want (empty)", lines)
	}
	lines, err = session.nearestLines(ringStoreCall{verb: "nearest"}, ringclient.NearestResult{Skipped: 3}, "Vector")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0] != `skipped: 3 elements without field "Vector"` {
		t.Fatalf("no hits and three skipped printed %q, want only the skipped line", lines)
	}
}

// TestNearestQueryRefusals pins the refusal text of each way the argument can be wrong.
func TestNearestQueryRefusals(t *testing.T) {
	cases := []struct {
		literal string
		want    string
	}{
		{`{vector: 5}`, "takes a binary here: write the vector as"},
		{`{k: 3}`, "needs the vector: {vector: <binary>}"},
		{`{vector: ` + nearestVector + `, k: 0}`, "takes k from 1 to 100"},
		{`{vector: ` + nearestVector + `, k: 101}`, "takes k from 1 to 100"},
		{`{vector: ` + nearestVector + `, metric: "euclid"}`, "takes metric cosine, dot or l2"},
		{`{vectr: ` + nearestVector + `}`, `"vectr" is not one of them`},
	}
	for _, c := range cases {
		_, err := nearestCall(t, c.literal).nearestQueryOf()
		if err == nil {
			t.Fatalf("%s was accepted", c.literal)
		}
		msg := err.Error()
		if !strings.Contains(msg, c.want) || !strings.Contains(msg, "Run 'db.User[\"ada\"].Friends.nearest(") {
			t.Fatalf("%s refused with %q, want %q and the statement that works", c.literal, msg, c.want)
		}
	}
}

// TestNearestQueryReadsEveryMember pins that each member reaches the client query.
func TestNearestQueryReadsEveryMember(t *testing.T) {
	query, err := nearestCall(t, `{vector: `+nearestVector+`, k: 3, metric: "l2", field: "Emb"}`).nearestQueryOf()
	if err != nil {
		t.Fatal(err)
	}
	if query.K != 3 || query.Metric != "l2" || query.Field != "Emb" {
		t.Fatalf("query %+v, want K 3, metric l2, field Emb", query)
	}
	if want := []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0}; !bytes.Equal(query.Vector, want) {
		t.Fatalf("vector %v, want %v", query.Vector, want)
	}
}

// TestNearestExecutorAsksTheBucketAndPrints pins that the verb asks the bucket for the nearest elements,
// hands it k, and prints each hit with its score and key.
func TestNearestExecutorAsksTheBucketAndPrints(t *testing.T) {
	ctx := context.Background()
	session, fake := u52Session()
	u82Prepare(ctx, t, session, u82Bucket)
	if _, err := session.Execute(ctx, `db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"grace", "linus"} {
		statement := `db.User["ada"].Friends.put("` + key + `", {Vector: ` + nearestVector + `})`
		if _, err := session.Execute(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	call := ringStoreCall{
		bucket:   fake.Bucket("app", "User"),
		database: "app",
		name:     "User",
		id:       "ada",
		field:    ringclient.FieldInfo{Name: "Friends"},
		verb:     "nearest",
		values:   []Value{ringLiteralDoc(t, `{vector: `+nearestVector+`, k: 3}`)},
	}
	output, err := session.nearest(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Lines) != 2 || !strings.HasPrefix(output.Lines[0], "1.000000  grace  ") || !strings.HasPrefix(output.Lines[1], "1.000000  linus  ") {
		t.Fatalf("k 3 printed %q, want grace then linus at score 1.000000", output.Lines)
	}
	call.values = []Value{ringLiteralDoc(t, `{vector: `+nearestVector+`, k: 1}`)}
	output, err = session.nearest(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Lines) != 1 || !strings.HasPrefix(output.Lines[0], "1.000000  grace  ") {
		t.Fatalf("k 1 printed %q, want only grace: the bucket must receive k", output.Lines)
	}
}

// TestNearestLinesRefusesAShortScoreList: a result with elements and no score for them is an error, not a panic.
func TestNearestLinesRefusesAShortScoreList(t *testing.T) {
	session, _ := u52Session()
	result := ringclient.NearestResult{
		Elements: []bson.Raw{nearestRaw(t, bson.D{{Key: "Name", Value: "Grace"}}), nearestRaw(t, bson.D{{Key: "Name", Value: "Linus"}})},
		Scores:   []float64{1},
		Scanned:  2,
	}
	_, err := session.nearestLines(ringStoreCall{verb: "nearest"}, result, "Vector")
	if err == nil || !strings.Contains(err.Error(), "2 elements and 1 scores") {
		t.Fatalf("error = %v, want a refusal naming 2 elements and 1 scores", err)
	}
}
