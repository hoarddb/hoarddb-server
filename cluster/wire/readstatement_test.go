package wire

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func TestFindIsNotAStatement(t *testing.T) {
	good := map[string]GetStatement{
		`db.Users["ada"].get()`:  {Bucket: "Users", ID: "ada", Quoted: true},
		`db.Users[42].get()`:     {Bucket: "Users", ID: "42", Quoted: false},
		`db.Users["a\"b"].get()`: {Bucket: "Users", ID: `a"b`, Quoted: true},
		`db.Users["a\\b"].get()`: {Bucket: "Users", ID: `a\b`, Quoted: true},
		`db.user_2["x y"].get()`: {Bucket: "user_2", ID: "x y", Quoted: true},
	}
	for text, want := range good {
		got, err := ParseGetStatement(text)
		if err != nil || got != want {
			t.Errorf("ParseGetStatement(%s) = %+v, %v; want %+v", text, got, err, want)
			continue
		}
		if got.String() != text {
			t.Errorf("%+v prints %s, want %s", got, got.String(), text)
		}
	}
	bad := []string{
		`db.Users.find("ada")`,
		`db.Users.find({_id_: "ada"})`,
		`db.Users.get("ada")`,
		`db.Users["ada"].find()`,
		`db.Users["ada"].get("ada")`,
		`db.Users.list("ada")`,
		`db.Users["ada"].list()`,
		`db.Users[].get()`,
		`db.Users["ada"].get`,
		`db.["ada"].get()`,
		`db.Users["ada"].get() `,
		`db.Users["a\nb"].get()`,
		`db.Users[007].get()`,
		``,
	}
	for _, text := range bad {
		_, err := ParseGetStatement(text)
		var coded *cluster.Error
		if err == nil {
			t.Errorf("ParseGetStatement(%q) was accepted", text)
			continue
		}
		if !asCoded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("ParseGetStatement(%q) = %v, want INVALID_ARGUMENT", text, err)
		}
	}
	_, err := ParseGetStatement(`db.Users.find("ada")`)
	if err == nil || !strings.Contains(err.Error(), `db.Users["<id>"].get()`) {
		t.Errorf("the refusal of find must name the one spelling: %v", err)
	}
}

func asCoded(err error, target **cluster.Error) bool {
	for err != nil {
		if coded, ok := err.(*cluster.Error); ok {
			*target = coded
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

func TestStatementParserSurvivesMutation(t *testing.T) {
	seeds := []string{`db.Users["ada"].get()`, `db.Users[42].get()`, `db.Users["a\"b"].get()`, `db.u["x"].get()`}
	for _, seed := range seeds {
		if _, err := ParseGetStatement(seed); err != nil {
			t.Fatalf("the seed %s must parse: %v", seed, err)
		}
	}
	accepted := 0
	random := rand.New(rand.NewSource(36))
	const alphabet = `db.[]"\()0123456789abc _-{}`
	for round := 0; round < 100000; round++ {
		text := []byte(seeds[random.Intn(len(seeds))])
		for edits := 1 + random.Intn(3); edits > 0 && len(text) > 0; edits-- {
			at := random.Intn(len(text))
			switch random.Intn(3) {
			case 0:
				text[at] = alphabet[random.Intn(len(alphabet))]
			case 1:
				text = append(text[:at], text[at+1:]...)
			default:
				text = append(text[:at], append([]byte{alphabet[random.Intn(len(alphabet))]}, text[at:]...)...)
			}
		}
		parsed, err := ParseGetStatement(string(text))
		if err != nil {
			continue
		}
		accepted++
		if parsed.String() != string(text) {
			t.Fatalf("%q was accepted and re-encodes as %q", text, parsed.String())
		}
	}
	if accepted < 1000 {
		t.Fatalf("only %d of 100000 mutations parsed: the property was barely exercised", accepted)
	}
}
