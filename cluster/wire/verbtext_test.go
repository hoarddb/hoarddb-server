package wire

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestAStoreVerbThatDoesNotFitTheFieldNamesTheDatabaseBucketAndField: [rule 3: the refusal of a verb on a field
// of the wrong store type names the database, bucket and field, the type the field is declared as, what the verb
// takes, and the command that lists the bucket's fields].
func TestAStoreVerbThatDoesNotFitTheFieldNamesTheDatabaseBucketAndField(t *testing.T) {
	node, _ := u63Fresh(t)
	heapArg, err := HeapPushArg(5, "e1", u63Element("x"))
	if err != nil {
		t.Fatal(err)
	}
	before := u63Position(node)
	_, err = u63Raw(node, "Table", cluster.VerbPush, heapArg)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, want := range []string{`field "Table"`, `bucket "Box"`, "database " + `"` + u30iDB + `"`, "hashStore", "push",
		"a heapStore field with {p, e, v}", "Nothing was written", "describe bucket Box in " + u30iDB} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("push on a hashStore: %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "or its arg is not the layout") {
		t.Errorf("the text still blames two causes at once: %q", err)
	}
	if after := u63Position(node); after != before {
		t.Fatalf("pair moved from %+v to %+v", before, after)
	}
	putArg, err := encodePutArg("k", u63Element("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = u63Raw(node, "Ranked", cluster.VerbPut, putArg)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, want := range []string{`field "Ranked"`, "heapStore", "put takes a hashStore or btreeStore field with {k, v}"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("put on a heapStore: %q lacks %q", err, want)
		}
	}
}

// TestAStoreVerbWithTheWrongArgLayoutNamesTheFieldAndTheLayout: [the field's type fits the verb but the arg is not
// the layout: the text names the field and the layout the verb takes there, not a table of every verb].
func TestAStoreVerbWithTheWrongArgLayoutNamesTheFieldAndTheLayout(t *testing.T) {
	node, _ := u63Fresh(t)
	putArg, err := encodePutArg("k", u63Element("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = u63Raw(node, "Queue", cluster.VerbPush, putArg)
	u30iCode(t, err, cluster.CodeInvalidArgument)
	for _, want := range []string{`field "Queue"`, `bucket "Box"`, "a fifoStore", "arg", "{v}", "Nothing was written"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("a put-shaped arg pushed to a fifoStore: %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "does not fit the field") {
		t.Errorf("the type fits, so the text must not say the verb does not fit the field: %q", err)
	}
}
