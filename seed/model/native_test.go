package model

import (
	"reflect"
	"strings"
	"testing"
)

// embeds reports whether an element is one of the two that carry a copy in the native variant.
func embeds(e Element) bool {
	return (e.Bucket == "User" && e.Store == "Feed") || (e.Bucket == "Room" && e.Store == "Members")
}

func nativeElement(t *testing.T, n *Native, e Element) Element {
	t.Helper()
	extra, err := n.Embed(e)
	if err != nil {
		t.Fatalf("Embed(%s/%s/%s %s): %v", e.Bucket, e.Owner, e.Store, e.Key, err)
	}
	out := e
	out.Body = append(append([]Field(nil), e.Body...), extra...)
	return out
}

// Strip(Embed(e)) must give back e and, for the two embedding stores, exactly the document the
// copy claims to be: the Post of a feed element, the User of a member.
func TestNativeStripInvertsEmbed(t *testing.T) {
	for _, layout := range []Layout{Social, Chat, Ledger, Agent} {
		for _, scale := range []int{1, 2} {
			g, err := New(layout, 7, scale)
			if err != nil {
				t.Fatal(err)
			}
			n := g.Native()
			docs := map[string]Document{}
			for unit := range g.Units() {
				docs[unit.Doc.Bucket+"/"+unit.Doc.ID] = unit.Doc
			}
			copies := 0
			for unit := range g.Units() {
				for _, e := range unit.Elements {
					common, doc, err := Strip(nativeElement(t, n, e))
					if err != nil {
						t.Fatalf("%s scale %d: Strip: %v", layout, scale, err)
					}
					if !reflect.DeepEqual(common, e) {
						t.Fatalf("%s scale %d: Strip(Embed(e)) = %v, want %v", layout, scale, common, e)
					}
					if !embeds(e) {
						if doc != nil {
							t.Fatalf("%s scale %d: %s.%s carries no copy but Strip returned the document %s/%s", layout, scale, e.Bucket, e.Store, doc.Bucket, doc.ID)
						}
						continue
					}
					copies++
					want := docs[map[string]string{"Feed": "Post/" + bodyText(e, "post"), "Members": "User/" + e.Key}[e.Store]]
					if doc == nil || doc.Bucket != want.Bucket || doc.ID != want.ID {
						t.Fatalf("%s scale %d: %s element %s names document %v, want %s/%s", layout, scale, e.Store, e.Key, doc, want.Bucket, want.ID)
					}
					for _, field := range doc.Fields {
						if !hasField(want.Fields, field) {
							t.Fatalf("%s scale %d: document %s/%s field %s=%v is not what the source holds %v", layout, scale, doc.Bucket, doc.ID, field.Name, field.Value, want.Fields)
						}
					}
					if len(doc.Fields) == 0 {
						t.Fatalf("%s scale %d: document %s/%s has no fields", layout, scale, doc.Bucket, doc.ID)
					}
				}
			}
			if (layout == Social || layout == Chat) && copies == 0 {
				t.Fatalf("%s scale %d: no element carried a copy, the test proved nothing", layout, scale)
			}
			if (layout == Ledger || layout == Agent) && copies != 0 {
				t.Fatalf("%s: %d elements carried a copy, none should", layout, copies)
			}
		}
	}
}

func bodyText(e Element, name string) string {
	for _, f := range e.Body {
		if f.Name == name {
			s, _ := f.Value.(string)
			return s
		}
	}
	return ""
}

func hasField(fields []Field, want Field) bool {
	for _, f := range fields {
		if f.Name == want.Name && f.Value == want.Value {
			return true
		}
	}
	return false
}

// Every element that is not User.Feed or Room.Members is left alone: no extra field, and Strip
// returns the same element and no document.
func TestNativeLeavesEveryOtherElementAlone(t *testing.T) {
	for _, layout := range Layouts {
		g, err := New(layout, 3, 1)
		if err != nil {
			t.Fatal(err)
		}
		n := g.Native()
		for unit := range g.Units() {
			for _, e := range unit.Elements {
				extra, err := n.Embed(e)
				if err != nil {
					t.Fatalf("%s: Embed: %v", layout, err)
				}
				if embeds(e) != (len(extra) > 0) {
					t.Fatalf("%s: %s.%s got %d extra fields, embeds=%v", layout, e.Bucket, e.Store, len(extra), embeds(e))
				}
				if embeds(e) {
					continue
				}
				same, doc, err := Strip(e)
				if err != nil || doc != nil || !reflect.DeepEqual(same, e) {
					t.Fatalf("%s: Strip changed %s.%s: %v %v %v", layout, e.Bucket, e.Store, same, doc, err)
				}
			}
		}
	}
}

// The embedded fields are appended last, in a fixed order: feed {author, text}, member {name}.
func TestNativeAppendsFieldsInAFixedOrder(t *testing.T) {
	social, err := New(Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for e := range social.Elements("User.Feed") {
		extra, err := social.Native().Embed(e)
		if err != nil || len(extra) != 2 || extra[0].Name != "author" || extra[1].Name != "text" {
			t.Fatalf("feed embed = %v, %v; want author then text", extra, err)
		}
		break
	}
	chat, err := New(Chat, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for e := range chat.Elements("Room.Members") {
		extra, err := chat.Native().Embed(e)
		if err != nil || len(extra) != 1 || extra[0].Name != "name" {
			t.Fatalf("member embed = %v, %v; want name", extra, err)
		}
		break
	}
}

func TestNativeFingerprintIsStable(t *testing.T) {
	print := func(layout Layout, seed uint64, scale int) string {
		g, err := New(layout, seed, scale)
		if err != nil {
			t.Fatal(err)
		}
		return g.Native().Fingerprint()
	}
	for _, layout := range []Layout{Social, Chat} {
		a, b := print(layout, 1, 1), print(layout, 1, 1)
		if a == "" || a != b {
			t.Fatalf("%s: fingerprint %q then %q, want the same non-empty value", layout, a, b)
		}
		if other := print(layout, 2, 1); other == a {
			t.Fatalf("%s: seeds 1 and 2 give the same fingerprint %q", layout, a)
		}
		if other := print(layout, 1, 2); other == a {
			t.Fatalf("%s: scales 1 and 2 give the same fingerprint %q", layout, a)
		}
	}
	for _, layout := range []Layout{Ledger, Agent, Video} {
		if got := print(layout, 1, 1); got != "" {
			t.Fatalf("%s embeds nothing, its fingerprint must be empty, got %q", layout, got)
		}
	}
}

// Strip must refuse a body that is not exactly the native one, naming the way out.
func TestStripRefusesACommonBody(t *testing.T) {
	social, err := New(Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	chat, err := New(Chat, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var feed, member Element
	for e := range social.Elements("User.Feed") {
		feed = e
		break
	}
	for e := range chat.Elements("Room.Members") {
		member = e
		break
	}
	cases := map[string]Element{
		"a common feed element":    feed,
		"a common member":          member,
		"a feed with a text only":  withBody(feed, append(append([]Field(nil), feed.Body...), Field{"text", "x"})),
		"a feed with extra fields": withBody(nativeElement(t, social.Native(), feed), append(nativeElement(t, social.Native(), feed).Body, Field{"more", "x"})),
		"a feed with a number author": withBody(feed, append(append([]Field(nil), feed.Body...),
			Field{"author", int64(3)}, Field{"text", "x"})),
		"a member with the name first": withBody(member, []Field{{"name", "n"}, {"user", "u1"}, {"since", int64(1)}}),
		"a member with a text name": withBody(member, append(append([]Field(nil), member.Body...),
			Field{"name", int64(5)})),
	}
	for name, e := range cases {
		_, _, err := Strip(e)
		if err == nil || !strings.Contains(err.Error(), "-layout-variant native") {
			t.Errorf("%s: Strip error = %v, want a refusal that names -layout-variant native", name, err)
		}
	}
}

func withBody(e Element, body []Field) Element {
	e.Body = body
	return e
}

// Embedding an element whose source document is not in the generator is an error, not an empty copy.
func TestNativeEmbedRefusesAnUnknownSource(t *testing.T) {
	g, err := New(Social, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ghost := Element{Bucket: "User", Owner: "u0", Store: "Feed", Body: []Field{{"post", "p-nope"}, {"at", int64(1)}}}
	if _, err := g.Native().Embed(ghost); err == nil || !strings.Contains(err.Error(), "p-nope") {
		t.Fatalf("Embed of an unknown post = %v, want an error naming it", err)
	}
}
