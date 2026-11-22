package cli

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

func TestParseExpireClauses(t *testing.T) {
	cases := []struct {
		line        string
		wantFields  []FieldDecl
		wantExpire  string
		wantDropped bool
		wantSet     string
	}{
		{line: "create bucket Session { UserId objectid, ExpiresAt datetime, expire at ExpiresAt }",
			wantFields: []FieldDecl{{Name: "UserId", Type: "objectid"}, {Name: "ExpiresAt", Type: "datetime"}}, wantExpire: "ExpiresAt"},
		{line: "create bucket Z { expire string }", wantFields: []FieldDecl{{Name: "expire", Type: "string"}}},
		{line: `create bucket Device { Name string, Log fifoStore expire at "ts" }`,
			wantFields: []FieldDecl{{Name: "Name", Type: "string"}, {Name: "Log", Type: "fifoStore", ExpirePath: "ts"}}},
		{line: `create bucket Device { Log lifoStore expire at "meta.at" }`,
			wantFields: []FieldDecl{{Name: "Log", Type: "lifoStore", ExpirePath: "meta.at"}}},
		{line: "alter bucket Session { set expire at ExpiresAt }", wantSet: "ExpiresAt"},
		{line: "alter bucket Session { drop expire at }", wantDropped: true},
		// A field named expire is dropped as 'drop expire' before a comma or the brace, as 'drop index }' is.
		{line: "alter bucket Session { drop expire }"},
	}
	for _, c := range cases {
		st, err := Parse(c.line, "app")
		if err != nil {
			t.Errorf("%q: %v", c.line, err)
			continue
		}
		body, ok := st.Arg("fields")
		if !ok {
			body, _ = st.Arg("changes")
		}
		if c.wantFields != nil && !reflect.DeepEqual(body.Fields, c.wantFields) {
			t.Errorf("%q: fields %+v, want %+v", c.line, body.Fields, c.wantFields)
		}
		wantExpire := c.wantExpire + c.wantSet
		if body.ExpireField != wantExpire || body.DropExpire != c.wantDropped {
			t.Errorf("%q: expire %q drop %v, want %q %v", c.line, body.ExpireField, body.DropExpire, wantExpire, c.wantDropped)
		}
	}
	st, err := Parse("alter bucket Session { drop expire }", "app")
	if err != nil {
		t.Fatal(err)
	}
	if changes, _ := st.Arg("changes"); len(changes.Changes) != 1 || changes.Changes[0].Name != "expire" || changes.Changes[0].Add {
		t.Errorf("'drop expire }' did not drop a field named expire: %+v", changes.Changes)
	}
}

func TestExpireRefusalsQuoteTheGrammar(t *testing.T) {
	cases := []struct{ line, want string }{
		{`create bucket Z { H hashStore expire at "ts" }`, "fifoStore and lifoStore"},
		{`create bucket Z { Log fifoStore expire at ts }`, "a quoted path"},
		{`create bucket Z { Log fifoStore expire at "a.b.c.d" }`, "at most 3"},
		{"create bucket Z { A datetime, expire at A, expire at A }", "one expire clause"},
		{"create bucket Z { expire at }", "names a datetime field"},
	}
	quoted := regexp.MustCompile(`'((?:create|alter) bucket [^']*)'`)
	for _, c := range cases {
		_, err := Parse(c.line, "app")
		var parseErr *ParseError
		if err == nil || !errorsAs(err, &parseErr) {
			t.Errorf("%q: want a ParseError, got %v", c.line, err)
			continue
		}
		if !strings.Contains(parseErr.Message, c.want) {
			t.Errorf("%q: refusal %q lacks %q", c.line, parseErr.Message, c.want)
		}
		fixes := 0
		for _, m := range quoted.FindAllStringSubmatch(parseErr.Message, -1) {
			if strings.Contains(m[1], "…") {
				continue // the form, whose … is a placeholder; the fix is the other quote
			}
			fixes++
			if _, err := Parse(m[1], "app"); err != nil {
				t.Errorf("%q: the fix %q does not parse: %v", c.line, m[1], err)
			}
		}
		if fixes == 0 {
			t.Errorf("%q: refusal %q quotes no statement to fix it with", c.line, parseErr.Message)
		}
	}
}

func TestHelpExpireSaysTheFieldHoldsTheExpiryInstant(t *testing.T) {
	page, err := HelpFor("expire")
	if err != nil {
		t.Fatal(err)
	}
	page = strings.Join(strings.Fields(page), " ") // the page wraps its paragraphs
	for _, want := range []string{"EXPIRY instant, not the creation time", "now()", "own clocks", "fifoStore and lifoStore", "at most 64 expired elements", "reads every element", "Two small gaps", "before it was seeded"} {
		if !strings.Contains(page, want) {
			t.Errorf("help expire lacks %q:\n%s", want, page)
		}
	}
	count, err := HelpFor("count")
	if err != nil || !strings.Contains(count, "expire") {
		t.Errorf("help count does not mention expiry: %v\n%s", err, count)
	}
}

func TestCreateAlterAndDescribeAnExpiry(t *testing.T) {
	session, fake := u52Session()
	got, err := u52Run(session,
		"create database app", "use app",
		`create bucket Session { UserId string, ExpiresAt datetime, Log fifoStore expire at "ts", expire at ExpiresAt }`,
		"describe bucket Session",
		"alter bucket Session { drop expire at }",
		"alter bucket Session { set expire at ExpiresAt }",
	)
	if err != nil {
		t.Fatalf("%v\n%s", err, got)
	}
	for _, want := range []string{
		"OK  created bucket app.Session (3 fields)",
		"Expires:             at ExpiresAt",
		`  Log          fifoStore   expire at "ts"`,
		"OK  altered bucket app.Session: expiry removed",
		"OK  altered bucket app.Session: expires at ExpiresAt",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	if bucket := fake.buckets["app.Session"]; bucket.info.ExpireField != "ExpiresAt" || len(bucket.info.ExpireStores) != 1 {
		t.Errorf("the server was left with %q %+v", bucket.info.ExpireField, bucket.info.ExpireStores)
	}
}

// TestDescribePrintsTheExpireBlock: describe shows the declaration and the number of items past
// their time, and "10000+" when the server stopped counting.
func TestDescribePrintsTheExpireBlock(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		`create bucket Session { UserId string, ExpiresAt datetime, Log fifoStore expire at "ts", expire at ExpiresAt }`); err != nil {
		t.Fatal(err)
	}
	fake.buckets["app.Session"].info.ExpirePending = 12
	got, err := u52Run(session, "describe bucket Session")
	if err != nil {
		t.Fatalf("%v\n%s", err, got)
	}
	for _, want := range []string{"Expires:             at ExpiresAt", `  Log          fifoStore   expire at "ts"`, "Past their time:     12 (hidden from reads, removed on the next retention tick)"} {
		if !strings.Contains(got, want) {
			t.Errorf("describe lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "does not count") {
		t.Errorf("describe still says this build does not count:\n%s", got)
	}
	fake.buckets["app.Session"].info.ExpirePending, fake.buckets["app.Session"].info.ExpirePendingMore = 10000, true
	got, err = u52Run(session, "describe bucket Session")
	if err != nil || !strings.Contains(got, "Past their time:     10000+ ") {
		t.Errorf("a capped count prints %q, %v; want 10000+", got, err)
	}
}

// TestGetOfAnExpiredDocumentPrintsExpiredTrue: a keyed get that hits an expired document is the
// ordinary miss followed by a trailing "expired: true" line; a plain miss does not print it.
func TestGetOfAnExpiredDocumentPrintsExpiredTrue(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket Session { UserId string, ExpiresAt datetime, expire at ExpiresAt }"); err != nil {
		t.Fatal(err)
	}
	fake.failures = map[string]error{"Get": &ringclient.Error{Code: cluster.CodeNotFound, Message: "document not found", Detail: &cluster.Detail{Expired: true}}}
	_, err := session.Execute(context.Background(), `db.Session["s1"].get()`)
	var refusal *RingError
	if !errors.As(err, &refusal) || refusal.Code != "NOT_FOUND" {
		t.Fatalf("get of an expired document: %v, want the NOT_FOUND miss", err)
	}
	lines := strings.Split(refusal.Message, "\n")
	if lines[len(lines)-1] != "expired: true" || !strings.Contains(lines[0], `document "s1" is not in app.Session`) {
		t.Fatalf("the miss reads %q, want the ordinary text then a trailing expired: true", refusal.Message)
	}
	fake.failures = map[string]error{"Get": &ringclient.Error{Code: cluster.CodeNotFound, Message: "document not found"}}
	if _, err := session.Execute(context.Background(), `db.Session["s1"].get()`); err == nil || strings.Contains(err.Error(), "expired: true") {
		t.Fatalf("a plain miss reads %v, want no expired line", err)
	}
}
