package cli

import (
	"errors"
	"strings"
	"testing"
)

// TestLiveFieldsProjectionAgainstTheRealServer: the sample session of the unit, on a real
// server: only Name and _id_, only what was asked, and the refusals with their fixes.
func TestLiveFieldsProjectionAgainstTheRealServer(t *testing.T) {
	session, ctx := u52LiveSession(t)
	got := u52Transcript(t, ctx, session,
		"create database app",
		"use app",
		"create bucket User { Name string, Bio string, Age int, Purchases fifoStore }",
		`db.User.insert({_id_: "ada", Name: "Ada", Bio: "wrote the first program", Age: 36})`,
		`db.User.insert({_id_: "bob", Name: "Bob", Bio: "builds things", Age: 41})`,
		`db.User["ada"].Purchases.push({Item: "coffee"})`,
		`db.User["ada"].get()`,
		`db.User["ada"].get({fields: ["Name"]})`,
		`db.User["ada"].get({fields: ["Name", "Age"]})`,
		`db.User.list({fields: ["Name"], limit: 1})`,
		`db.User.list({filter: {Bio: "builds things"}, fields: ["Name"]})`)
	t.Logf("\n%s", got)
	for _, want := range []string{
		`{"Age": 36, "Bio": "wrote the first program", "Name": "Ada", "Purchases": "fifoStore (1 element)", "_id_": "ada"}`,
		"db.User[\"ada\"].get({fields: [\"Name\"]})\n{\"Name\": \"Ada\", \"_id_\": \"ada\"}\n",
		"{\"Age\": 36, \"Name\": \"Ada\", \"_id_\": \"ada\"}\n",
		"db.User.list({fields: [\"Name\"], limit: 1})\n{\"Name\": \"Ada\", \"_id_\": \"ada\"}\nmore: db.User.list({fields: [\"Name\"], limit: 1, cursor: \"",
		"{\"Name\": \"Bob\", \"_id_\": \"bob\"}\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("transcript lacks %q", want)
		}
	}
	for statement, said := range map[string][]string{
		`db.User["ada"].get({fields: ["Nmae"]})`:        {"FIELD_TYPE", `"Nmae"`, "describe bucket User in app"},
		`db.User.list({fields: ["Purchases"]})`:         {"FIELD_TYPE", `"Purchases"`, `db.User["<id>"].Purchases.list()`},
		`db.User["ada"].get({fields: ["Name", "Bio"]})`: nil,
	} {
		_, err := session.Execute(ctx, statement)
		if said == nil {
			if err != nil {
				t.Errorf("%s: %v", statement, err)
			}
			continue
		}
		var failed *RingError
		if !errors.As(err, &failed) {
			t.Errorf("%s: %v, want a refusal", statement, err)
			continue
		}
		for _, word := range said {
			if !strings.Contains(failed.Code+" "+failed.Message, word) {
				t.Errorf("%s: %q lacks %q", statement, failed.Code+" "+failed.Message, word)
			}
		}
	}
}
