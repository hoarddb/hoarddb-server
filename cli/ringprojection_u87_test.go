package cli

import (
	"errors"
	"strings"
	"testing"
)

// The fields argument of get and list (U87): db.User.list({fields: ["Name"]}) and
// db.User["ada"].get({fields: ["Name"]}).

// TestFieldsArgumentIsPassedAndRepeatedInTheMoreHint: [GUARD list sends fields to the
// client and the 'more:' line keeps them, so the next page is projected too].
func TestFieldsArgumentIsPassedAndRepeatedInTheMoreHint(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Age int }",
		`db.User.insert({_id_: "a", Name: "Ada", Age: 36})`, `db.User.insert({_id_: "b", Name: "Bob", Age: 41})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, `db.User.list({filter: {Age: 36}, fields: ["Name"]})`, `db.User.list({fields: ["Age", "Name"], limit: 1})`, `db.User["b"].get({fields: ["Age"]})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, `{"Name": "Ada", "_id_": "a"}`, `{"Age": 36, "Name": "Ada", "_id_": "a"}`,
		`more: db.User.list({fields: ["Age", "Name"], limit: 1, cursor: "01"})`, `{"Age": 41, "_id_": "b"}`)
	got, err = u52Run(session, `db.User["a"].get({})`, `db.User["a"].get()`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, `{"Age": 36, "Name": "Ada", "_id_": "a"}`, `{"Age": 36, "Name": "Ada", "_id_": "a"}`)
}

// TestFieldsArgumentMistakesNameTheFix: [GUARD a fields argument that is not a list of
// quoted names, or an argument get or count does not take, is refused with the statement to run].
func TestFieldsArgumentMistakesNameTheFix(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string }", `db.User.insert({_id_: "a", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	for statement, said := range map[string][]string{
		`db.User.list({fields: "Name"})`:                        {"fields", `["Name", "Bio"]`, `db.User.list({fields: ["Name", "Bio"]`},
		`db.User.list({fields: []})`:                            {"fields", "at least one", `["Name"]`},
		`db.User.list({fields: [1]})`:                           {"fields", "double quotes", `["Name"]`},
		`db.User.list({fields: ["Name", "Name"]})`:              {"fields", `"Name"`, "twice"},
		`db.User["a"].get({fields: "Name"})`:                    {"fields", `["Name", "Bio"]`, `db.User["<id>"].get({fields: ["Name", "Bio"]})`},
		`db.User["a"].get({limit: 1})`:                          {"get takes fields", `"limit" is not one`},
		`db.User.count({fields: ["Name"]})`:                     {"count takes filter", `"fields" is not one`},
		`db.User["a"].get({fields: ["Nope"]})`:                  {"FIELD_TYPE"},
		`db.User["a"].get({fields: ["Name"], fields: ["Bio"]})`: {"PARSE_ERROR", "fields", "twice", `db.User["<id>"].get({fields: ["Name", "Bio"]})`},
	} {
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) {
			t.Errorf("%s: %v, want a RingError", statement, err)
			continue
		}
		text := failed.Code + " " + failed.Message
		for _, word := range said {
			if !strings.Contains(text, word) {
				t.Errorf("%s: %q lacks %q", statement, text, word)
			}
		}
	}
}
