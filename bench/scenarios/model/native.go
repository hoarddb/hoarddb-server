package model

import (
	"fmt"
	"hash/fnv"
)

// The native layout variant (docs: native-layout-variant-spec) changes exactly two element bodies
// of the layouts and nothing else:
//
//   - a User.Feed element of the social layout, {post, at}, also carries the post's author and
//     text, copied from the Post document: {post, at, author, text};
//   - a Room.Members element of the chat layout, {user, since}, also carries the member's name,
//     copied from the User document: {user, since, name}.
//
// This file is the one definition of those copies. The seeder, the hoardDB adapter and the audit
// all call it, so the layout cannot drift between them. The embedded fields are appended after
// the common ones, and Strip removes exactly those, so a stripped element is the common element
// byte for byte (the verifier compares the order and types of a body).

// embedKind says which copy an element carries.
type embedKind int

const (
	embedNone embedKind = iota
	embedFeed
	embedMember
)

// embedOf names the copy a store of a bucket carries.
func embedOf(bucket, store string) embedKind {
	switch {
	case bucket == "User" && store == "Feed":
		return embedFeed
	case bucket == "Room" && store == "Members":
		return embedMember
	}
	return embedNone
}

// IsNativeStore reports whether elements of the store bucket.store carry an embedded copy in the
// native variant (User.Feed and Room.Members).
func IsNativeStore(bucket, store string) bool { return embedOf(bucket, store) != embedNone }

// variantHint is the way out every refusal names.
const variantHint = "This database holds the common layout; seed it with -layout-variant native"

// Native knows, for one generated layout, the documents the embedded copies are taken from.
type Native struct {
	generator *Generator
	posts     map[string]Document
	users     map[string]Document
}

// Native returns the native variant of this generator. A layout without an embedding store
// (ledger, agent, video) gets a Native that embeds nothing.
func (g *Generator) Native() *Native {
	n := &Native{generator: g}
	switch g.layout {
	case Social:
		n.posts = map[string]Document{}
		for doc := range g.Documents("Post") {
			n.posts[doc.ID] = doc
		}
	case Chat:
		n.users = map[string]Document{}
		for doc := range g.Documents("User") {
			n.users[doc.ID] = doc
		}
	}
	return n
}

// stringField returns the string field name of fields.
func stringField(fields []Field, name string) (string, bool) {
	for _, field := range fields {
		if field.Name == name {
			text, ok := field.Value.(string)
			return text, ok
		}
	}
	return "", false
}

// FeedEmbed is the copy a User.Feed element carries of the post it names: author and text, in
// that order. The seeder, the planner's writes and the audit all take it from the Post document.
func FeedEmbed(post Document) ([]Field, error) {
	author, okAuthor := stringField(post.Fields, "Author")
	text, okText := stringField(post.Fields, "Text")
	if !okAuthor || !okText {
		return nil, fmt.Errorf("post %s has no Author and Text to copy into a feed element", post.ID)
	}
	return []Field{{Name: "author", Value: author}, {Name: "text", Value: text}}, nil
}

// MemberEmbed is the copy a Room.Members element carries of the member it names: the name.
func MemberEmbed(user Document) ([]Field, error) {
	name, ok := stringField(user.Fields, "Name")
	if !ok {
		return nil, fmt.Errorf("user %s has no Name to copy into a member element", user.ID)
	}
	return []Field{{Name: "name", Value: name}}, nil
}

// Embed returns the fields to append to the body of e in the native variant: author and text
// for a User.Feed element, name for a Room.Members element, nil for every other element. It
// fails when the document the copy is taken from is not in the generated layout.
func (n *Native) Embed(e Element) ([]Field, error) {
	switch embedOf(e.Bucket, e.Store) {
	case embedFeed:
		id, _ := stringField(e.Body, "post")
		post, ok := n.posts[id]
		if !ok {
			return nil, fmt.Errorf("feed element of %s names post %q, which the %s layout does not hold", e.Owner, id, n.generator.layout)
		}
		return FeedEmbed(post)
	case embedMember:
		id, _ := stringField(e.Body, "user")
		user, ok := n.users[id]
		if !ok {
			return nil, fmt.Errorf("member element of %s names user %q, which the %s layout does not hold", e.Owner, id, n.generator.layout)
		}
		return MemberEmbed(user)
	}
	return nil, nil
}

// Fingerprint hashes the embedded fields of every seeded element that carries a copy, with the
// place of the element, so a seed and a run can be shown to agree on the native layout. It is
// empty for a layout that embeds nothing.
func (n *Native) Fingerprint() string {
	var store string
	switch n.generator.layout {
	case Social:
		store = "User.Feed"
	case Chat:
		store = "Room.Members"
	default:
		return ""
	}
	digest := fnv.New64a()
	for e := range n.generator.Elements(store) {
		extra, err := n.Embed(e)
		if err != nil {
			// A generated element always has its source; folding the error in keeps a
			// broken layout from matching a good one.
			_, _ = fmt.Fprintf(digest, "error|%v\n", err)
			continue
		}
		_, _ = fmt.Fprintf(digest, "%s/%s/%s|%s|%d", e.Bucket, e.Owner, e.Store, e.Key, e.Seq)
		for _, field := range extra {
			_, _ = fmt.Fprintf(digest, "|%s=%q", field.Name, field.Value)
		}
		_, _ = fmt.Fprintln(digest)
	}
	return fmt.Sprintf("native-%s-%016x", n.generator.layout, digest.Sum64())
}

// nativeShape lists the field names of a native body in order and whether each is text (true)
// or a whole number (false).
type nativeShape []struct {
	name string
	text bool
}

var (
	feedShape   = nativeShape{{"post", true}, {"at", false}, {"author", true}, {"text", true}}
	memberShape = nativeShape{{"user", true}, {"since", false}, {"name", true}}
)

// check reports why body is not of this shape, or "".
func (s nativeShape) check(body []Field) string {
	if len(body) != len(s) {
		return fmt.Sprintf("has %d fields, the native body has %d", len(body), len(s))
	}
	for i, want := range s {
		got := body[i]
		if got.Name != want.name {
			return fmt.Sprintf("has field %d named %q, the native body has %q there", i+1, got.Name, want.name)
		}
		_, isText := got.Value.(string)
		_, isNumber := got.Value.(int64)
		if (want.text && !isText) || (!want.text && !isNumber) {
			return fmt.Sprintf("has field %q of type %T, the native body wants %s", got.Name, got.Value, map[bool]string{true: "text", false: "an int64"}[want.text])
		}
	}
	return ""
}

// Strip turns a native element into the common one and the document its copy claims to be: for
// a User.Feed element the Post (Author, Text and At, the element's own at) and for a
// Room.Members element the User (Name). An element of any other store is returned unchanged
// with no document. A body that is not exactly the native one, a common body included, is an
// error that says how to seed the native layout.
func Strip(e Element) (Element, *Document, error) {
	var shape nativeShape
	switch embedOf(e.Bucket, e.Store) {
	case embedFeed:
		shape = feedShape
	case embedMember:
		shape = memberShape
	default:
		return e, nil, nil
	}
	if why := shape.check(e.Body); why != "" {
		return Element{}, nil, fmt.Errorf("%s element of %s %s %s. %s", e.Store, e.Bucket, e.Owner, why, variantHint)
	}
	common := e
	common.Body = append([]Field(nil), e.Body[:2]...)
	if embedOf(e.Bucket, e.Store) == embedFeed {
		doc := Document{Bucket: "Post", ID: e.Body[0].Value.(string), Fields: []Field{
			{Name: "Author", Value: e.Body[2].Value}, {Name: "Text", Value: e.Body[3].Value}, {Name: "At", Value: e.Body[1].Value}}}
		return common, &doc, nil
	}
	doc := Document{Bucket: "User", ID: e.Body[0].Value.(string), Fields: []Field{{Name: "Name", Value: e.Body[2].Value}}}
	return common, &doc, nil
}
