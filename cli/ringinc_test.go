package cli

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// TestIncParsesAndCompletes: [GUARD: the verb absent from the table, so it neither parses nor completes]. inc
// is offered after a hashStore or btreeStore field, parses with and without the {return: "after"} option, adds
// to the element a second time, prints the element when asked, and a bad option is refused naming the form.
func TestIncParsesAndCompletes(t *testing.T) {
	if offered := CompleteWith(nil, `db.User["ada"].Friends.`, "app"); !slices.Contains(offered, "inc") {
		t.Fatalf("completion after a store field offers %v, want inc among the verbs", offered)
	}
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Friends hashStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`,
		`db.User["ada"].Friends.inc("grace", {visits: 1})`,
		`db.User["ada"].Friends.inc("grace", {visits: 2}, {return: "after"})`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`OK  inc "grace": Friends now holds 1 element`, `"visits"`, "3"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output:\n%s\nwant it to contain %q", got, want)
		}
	}
	for _, bad := range []string{
		`db.User["ada"].Friends.inc("grace")`,
		`db.User["ada"].Friends.inc("grace", {visits: 1}, {return: "before"})`,
		`db.User["ada"].Friends.inc("grace", {visits: 1}, {fetch: true})`,
		`db.User["ada"].Friends.inc("grace", "visits")`,
	} {
		if _, err := session.Execute(context.Background(), bad); err == nil || !strings.Contains(err.Error(), "inc") {
			t.Errorf("%s was accepted or refused without naming the verb: %v", bad, err)
		}
	}
	if verb, ok := ringVerbByName("inc"); !ok || verb.Example == "" || !strings.Contains(verb.Example, ".inc(") {
		t.Fatalf("the inc verb has no runnable example: %+v", verb)
	}
}

// TestIncGuardIsRefusedAsLaterRelease: [GUARD: parsing if: and ignoring it, which would run an unguarded add].
func TestIncGuardIsRefusedAsLaterRelease(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Friends hashStore }", `db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	_, err := session.Execute(context.Background(), `db.User["ada"].Friends.inc("login", {n: 1}, {return: "after", if: {n: {$lt: 5}}})`)
	if err == nil || !strings.Contains(err.Error(), "not in this release") {
		t.Fatalf("an inc with if: gave %v, want the refusal that guards on an element are not in this release", err)
	}
	got, _ := u52Run(session, `db.User["ada"].Friends.count()`)
	if strings.TrimSpace(got) != "0" {
		t.Fatalf("the refused inc created the element: count printed %q", got)
	}
}

// TestIncRepeatedKeyIsRefused: [GUARD: collapsing {n: 1, n: 5} into a map, which adds one of them silently].
func TestIncRepeatedKeyIsRefused(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Friends hashStore }", `db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	_, err := session.Execute(context.Background(), `db.User["ada"].Friends.inc("login", {n: 1, n: 5})`)
	if err == nil || !strings.Contains(err.Error(), `"n"`) || !strings.Contains(err.Error(), "once") {
		t.Fatalf("an inc with a repeated key gave %v, want a refusal that names the key and says to give it once", err)
	}
	got, _ := u52Run(session, `db.User["ada"].Friends.count()`)
	if strings.TrimSpace(got) != "0" {
		t.Fatalf("the refused inc created the element: count printed %q", got)
	}
}
