package cli

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// BUG-0458: a mistyped page size answered the default page. Each test runs
// against a handler with no connection, so a statement that reached send says
// ErrNotConnected and a refusal does not: that is the "no request was sent"
// assertion, and the valid-option rows prove the handler is not refusing
// everything.

func runFieldList(optionText string) error {
	handler := &CommandHandler{terminal: &strings.Builder{}}
	return handler.fieldList(server.FieldRequest{}, []string{optionText}, "db.Post[p1].Tags")
}

func runFind(optionText string) error {
	handler := &CommandHandler{terminal: &strings.Builder{}}
	return handler.handleFind("find", "app", "Post", []string{`{}`, optionText})
}

type optionCase struct {
	name       string
	options    string
	wantInText []string
}

func requireRefusalBeforeSend(t *testing.T, err error, wantInText []string) {
	t.Helper()
	if err == nil || errors.Is(err, ErrNotConnected) {
		t.Fatalf("got %v, want a usage refusal before any request is sent", err)
	}
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("got %T %v, want a *UsageError", err, err)
	}
	for _, want := range wantInText {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
}

func requireReachesSend(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("got %v, want the statement to reach send (ErrNotConnected)", err)
	}
}

var stringLimit = []string{`"limit"`, "string", "whole number", "{limit: 3}"}

func TestListLimitStringIsRefused(t *testing.T) {
	requireRefusalBeforeSend(t, runFieldList(`{limit: "3"}`), stringLimit)
	requireReachesSend(t, runFieldList(`{limit: 3}`))
	requireReachesSend(t, runFieldList(`{limit: 2}`))
}

func TestListLimitFractionIsRefused(t *testing.T) {
	for _, tc := range []optionCase{
		{"fraction", `{limit: 1.5}`, []string{`"limit"`, "1.5", "whole number", "{limit: 3}"}},
		{"zero", `{limit: 0}`, []string{`"limit"`, "0", "at least 1", "{limit: 3}"}},
		{"negative", `{limit: -1}`, []string{`"limit"`, "-1", "at least 1", "{limit: 3}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRefusalBeforeSend(t, runFieldList(tc.options), tc.wantInText)
		})
	}
}

func TestListUnknownOptionIsRefused(t *testing.T) {
	requireRefusalBeforeSend(t, runFieldList(`{limt: 3}`), []string{`"limt"`, "limit", "after"})
	requireReachesSend(t, runFieldList(`{after: "a"}`))
}

func TestFindLimitStringIsRefused(t *testing.T) {
	requireRefusalBeforeSend(t, runFind(`{limit: "3"}`), stringLimit)
	requireReachesSend(t, runFind(`{limit: 3}`))
}

func TestFindLimitFractionIsRefused(t *testing.T) {
	for _, tc := range []optionCase{
		{"fraction", `{limit: 1.5}`, []string{`"limit"`, "1.5", "whole number", "{limit: 3}"}},
		{"zero", `{limit: 0}`, []string{`"limit"`, "0", "at least 1", "{limit: 3}"}},
		{"negative", `{limit: -1}`, []string{`"limit"`, "-1", "at least 1", "{limit: 3}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRefusalBeforeSend(t, runFind(tc.options), tc.wantInText)
		})
	}
}

// TestFindSkipMistypeIsRefused is BUG-0478: a skip the reader cannot use was
// dropped, and the scan ran from the start with no word of it.
func TestFindSkipMistypeIsRefused(t *testing.T) {
	for _, tc := range []optionCase{
		{"string", `{skip: "2"}`, []string{`"skip"`, "whole number", "{skip: 20}"}},
		{"fraction", `{skip: 1.5}`, []string{`"skip"`, "1.5", "whole number", "{skip: 20}"}},
		{"negative", `{skip: -1}`, []string{`"skip"`, "-1", "whole number", "{skip: 20}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRefusalBeforeSend(t, runFind(tc.options), tc.wantInText)
		})
	}
	requireReachesSend(t, runFind(`{skip: 0}`))
	requireReachesSend(t, runFind(`{skip: 20}`))
}

// TestFindSortMistypeIsRefused is BUG-0478: a sort that is not a map of field
// to 1 or -1 returned unsorted documents with no error.
func TestFindSortMistypeIsRefused(t *testing.T) {
	for _, tc := range []optionCase{
		{"string", `{sort: "Age"}`, []string{`"sort"`, "Age", "{sort: {Age: 1}}"}},
		{"direction word", `{sort: {Age: "asc"}}`, []string{`"sort"`, "asc", "{sort: {Age: 1}}"}},
		{"other number", `{sort: {Age: 2}}`, []string{`"sort"`, "Age", "1", "-1", "{sort: {Age: 1}}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRefusalBeforeSend(t, runFind(tc.options), tc.wantInText)
		})
	}
	requireReachesSend(t, runFind(`{sort: {Age: -1}}`))
}

// TestFindProjectionMistypeIsRefused is BUG-0478: a projection that is not a
// map of field to 1 or 0 returned whole documents with no error.
func TestFindProjectionMistypeIsRefused(t *testing.T) {
	for _, tc := range []optionCase{
		{"array", `{projection: ["Name"]}`, []string{`"projection"`, "1 (include) or 0 (exclude)", "{projection: {Name: 1}}"}},
		{"other number", `{projection: {Name: 2}}`, []string{`"projection"`, "Name", "0", "1", "{projection: {Name: 1}}"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireRefusalBeforeSend(t, runFind(tc.options), tc.wantInText)
		})
	}
	requireReachesSend(t, runFind(`{projection: {Name: 1}}`))
	requireReachesSend(t, runFind(`{projection: {Email: 0}}`))
}

func TestFindUnknownOptionIsRefused(t *testing.T) {
	requireRefusalBeforeSend(t, runFind(`{limt: 3}`), []string{`"limt"`, "skip", "limit", "sort", "projection"})
	requireReachesSend(t, runFind(`{skip: 1, limit: 2, sort: {a: 1}, projection: {a: 1}}`))
}

// TestPageSizeOptionIsReadByOneSharedReader pins the sharing: both levels
// read limit through readLimitOption, so a level that grows its own reader
// back fails here. The peek-N build's reader builds on this function.
func TestPageSizeOptionIsReadByOneSharedReader(t *testing.T) {
	allowed := map[string]bool{"limit": true}
	limit, err := readLimitOption(map[string]interface{}{"limit": float64(7)}, allowed)
	if err != nil || limit != 7 {
		t.Fatalf("readLimitOption(7) = %d, %v, want 7, nil", limit, err)
	}
	if limit, err = readLimitOption(map[string]interface{}{}, allowed); err != nil || limit != 0 {
		t.Fatalf("readLimitOption(absent) = %d, %v, want 0 (server default), nil", limit, err)
	}
	if _, err = readLimitOption(map[string]interface{}{"limit": "3"}, allowed); err == nil {
		t.Fatal("readLimitOption(\"3\") = nil error, want a refusal")
	}
	if _, err = readLimitOption(map[string]interface{}{"cap": 3}, allowed); err == nil {
		t.Fatal("readLimitOption with an unknown key = nil error, want a refusal")
	}

	for _, file := range []string{"declare.go", "handlers.go"} {
		source := readSourceFile(t, file)
		if strings.Contains(source, `options["limit"]`) {
			t.Errorf("%s reads options[\"limit\"] itself; page sizes go through readLimitOption", file)
		}
	}
}

func readSourceFile(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("reading %s: %v", name, err)
	}
	return string(contents)
}
