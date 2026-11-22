package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u85Session is a live server with app.User and the document ada, every store type declared.
func u85Session(t *testing.T) (*RingSession, func(statement string) ([]string, error)) {
	t.Helper()
	session, ctx := u52LiveSession(t)
	if _, err := u82Statements(ctx, session, "create database app", "use app",
		"create bucket User { Name string, Fifo fifoStore, Lifo lifoStore, Hash hashStore, Btree btreeStore, Heap heapStore, Blob blobStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	return session, func(statement string) ([]string, error) {
		output, err := session.Execute(ctx, statement)
		return output.Lines, err
	}
}

func u85Fail(t *testing.T, err error, code string, words ...string) {
	t.Helper()
	var refused *RingError
	if err == nil {
		t.Fatalf("accepted, want %s", code)
	}
	if !errors.As(err, &refused) {
		t.Fatalf("error %v is not a RingError", err)
	}
	if code != "" && refused.Code != code {
		t.Fatalf("code %s (%s), want %s", refused.Code, refused.Message, code)
	}
	for _, word := range words {
		if !strings.Contains(refused.Message, word) {
			t.Errorf("the message %q does not say %q", refused.Message, word)
		}
	}
}

// TestU85GetReadsOneKeyOfAHashOrBtree: db.User["ada"].Friends.get("grace") of the help grammar.
func TestU85GetReadsOneKeyOfAHashOrBtree(t *testing.T) {
	_, run := u85Session(t)
	for _, store := range []string{"Hash", "Btree"} {
		target := `db.User["ada"].` + store
		if _, err := run(target + `.put("grace", {since: 2024})`); err != nil {
			t.Fatal(err)
		}
		lines, err := run(target + `.get("grace")`)
		if err != nil || len(lines) != 1 || lines[0] != `{"since": 2024}` {
			t.Fatalf("%s.get = %q, %v, want the element alone", store, lines, err)
		}
		if lines, _ := run(target + ".count()"); len(lines) != 1 || lines[0] != "1" {
			t.Fatalf("a get removed from %s: count %q", store, lines)
		}
		_, err = run(target + `.get("linus")`)
		u85Fail(t, err, "NOT_FOUND", `"linus"`, "Run '"+target+".list()'")
		_, err = run(target + `.get()`)
		u85Fail(t, err, "PARSE_ERROR", target+`.get("<key>")`)
	}
	if _, err := run(`db.User["ada"].Fifo.get("11111111111111111111111111111111")`); err != nil {
		t.Fatalf("get of a lease on a queue still runs: %v", err)
	}
}

// TestU85ListShowsKeyThenElement: the v1.0 style, the key first, two spaces, the element.
func TestU85ListShowsKeyThenElement(t *testing.T) {
	_, run := u85Session(t)
	for _, store := range []string{"Hash", "Btree"} {
		for _, key := range []string{"linus", "grace"} {
			if _, err := run(`db.User["ada"].` + store + `.put("` + key + `", {n: 1})`); err != nil {
				t.Fatal(err)
			}
		}
		lines, err := run(`db.User["ada"].` + store + `.list()`)
		if err != nil || strings.Join(lines, "|") != `grace  {"n": 1}|linus  {"n": 1}` {
			t.Fatalf("%s list = %q, %v", store, lines, err)
		}
	}
	if _, err := run(`db.User["ada"].Fifo.push({a: 1})`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`db.User["ada"].Fifo.list()`, `db.User["ada"].Fifo.peek()`} {
		if lines, err := run(statement); err != nil || strings.Join(lines, "|") != `{"a": 1}` {
			t.Fatalf("%s = %q, %v: a queue shows the element alone", statement, lines, err)
		}
	}
}

// TestU85HeapPagesShowEntryIDThenPriorityThenElement is checked on a hand-built page: no
// client verb pushes to a heapStore yet.
func TestU85HeapPagesShowEntryIDThenPriorityThenElement(t *testing.T) {
	session := NewRingSession(NewRingAPI(nil))
	element, err := bson.Marshal(bson.D{{Key: "job", Value: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	page := ringclient.ElementPage{Elements: []bson.Raw{element, element}, Keys: []string{"first", "second"}, Priorities: []int64{9, -2}}
	lines, err := session.ringPageLines(page)
	if err != nil || strings.Join(lines, "|") != `first  9  {"job": "x"}|second  -2  {"job": "x"}` {
		t.Fatalf("heap page = %q, %v", lines, err)
	}
}

func TestU85PopTakesSeveralOrNamesTheLimit(t *testing.T) {
	_, run := u85Session(t)
	for _, label := range []string{"1", "2", "3", "4"} {
		if _, err := run(`db.User["ada"].Fifo.push({n: ` + label + `})`); err != nil {
			t.Fatal(err)
		}
	}
	lines, err := run(`db.User["ada"].Fifo.pop(3)`)
	if err != nil || strings.Join(lines, "|") != `{"n": 1}|{"n": 2}|{"n": 3}` {
		t.Fatalf("pop(3) = %q, %v", lines, err)
	}
	lines, err = run(`db.User["ada"].Fifo.pop({limit: 5})`)
	if err != nil || strings.Join(lines, "|") != `{"n": 4}` {
		t.Fatalf("pop({limit: 5}) = %q, %v: v1.0's {limit: n} works as it does for peek", lines, err)
	}
	_, err = run(`db.User["ada"].Fifo.pop(3)`)
	u85Fail(t, err, "NOT_FOUND", "is empty")
	for _, call := range []string{"pop(0)", "pop(101)", "pop(1000)"} {
		_, err = run(`db.User["ada"].Fifo.` + call)
		u85Fail(t, err, "PARSE_ERROR", "1 to 100", `Fifo.pop(<n>)`)
	}
	if _, err := run(`db.User["ada"].Fifo.push({n: 9})`); err != nil {
		t.Fatal(err)
	}
	if lines, err := run(`db.User["ada"].Fifo.pop()`); err != nil || strings.Join(lines, "|") != `{"n": 9}` {
		t.Fatalf("pop() = %q, %v", lines, err)
	}
}

func TestU85PeekCapIsOneHundred(t *testing.T) {
	_, run := u85Session(t)
	if _, err := run(`db.User["ada"].Fifo.push({n: 1})`); err != nil {
		t.Fatal(err)
	}
	if _, err := run(`db.User["ada"].Fifo.peek(100)`); err != nil {
		t.Fatalf("peek(100): %v", err)
	}
	for _, call := range []string{"peek(101)", "peek(1000)", "peek(0)"} {
		_, err := run(`db.User["ada"].Fifo.` + call)
		u85Fail(t, err, "PARSE_ERROR", "1 to 100")
		if strings.Contains(err.Error(), "1000") {
			t.Errorf("%s: %v still says 1000", call, err)
		}
	}
}

// TestU85GetMissNamesAFixThatRuns: a heapStore cannot be put to from this client yet, and a key
// with a quote must be quoted so the suggested statement parses.
func TestU85GetMissNamesAFixThatRuns(t *testing.T) {
	_, run := u85Session(t)
	_, err := run(`db.User["ada"].Heap.get("job")`)
	u85Fail(t, err, "NOT_FOUND", `Heap.list()`, "entry id", "no heap push statement")
	if strings.Contains(err.Error(), ".put(") {
		t.Errorf("a heapStore miss suggests put: %v", err)
	}
	_, err = run(`db.User["ada"].Hash.get("a\"b")`)
	var refused *RingError
	if !errors.As(err, &refused) || !strings.Contains(refused.Message, `put("a\"b", {…})`) {
		t.Fatalf("a key with a quote = %v, want the suggestion to quote it", err)
	}
	fix := `db.User["ada"].Hash.put("a\"b", {n: 1})`
	if _, err := run(fix); err != nil {
		t.Fatalf("the suggested statement %s does not run: %v", fix, err)
	}
}
