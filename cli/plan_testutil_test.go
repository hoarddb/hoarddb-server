package cli

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// writeBlogDump writes the spec 7.3 example dataset into dir: users.bson (200
// documents), posts.bson (2,000) and comments.bson (24,000). The generator is
// seeded, so every run writes the same bytes and testdata/plan/blog.model.json
// is what it yields. Two comments name a post that does not exist, so the
// comments-to-posts reference resolves at 999 per-mille, as the committed
// model says.
func writeBlogDump(t *testing.T, dir string) {
	t.Helper()
	random := rand.New(rand.NewSource(20261105))
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	var users, posts, comments []byte
	for index := range 200 {
		avatar := make([]byte, 180000)
		random.Read(avatar)
		users = appendBSON(t, users, bson.D{
			{Key: "_id", Value: fmt.Sprintf("u%03d", index)},
			{Key: "name", Value: fmt.Sprintf("Person %03d", index)},
			{Key: "email", Value: fmt.Sprintf("user%03d@example.test", index)},
			{Key: "avatar", Value: primitive.Binary{Subtype: 0, Data: avatar}},
		})
	}
	for index := range 2000 {
		tags := make(bson.A, 1+random.Intn(5))
		for tag := range tags {
			tags[tag] = fmt.Sprintf("tag%d", random.Intn(50))
		}
		posts = appendBSON(t, posts, bson.D{
			{Key: "_id", Value: fmt.Sprintf("p%04d", index)},
			{Key: "author_id", Value: fmt.Sprintf("u%03d", random.Intn(200))},
			{Key: "title", Value: fmt.Sprintf("Post %04d", index)},
			{Key: "body", Value: strings.Repeat("lorem ipsum ", 8+random.Intn(8))},
			{Key: "tags", Value: tags},
			{Key: "created_at", Value: primitive.NewDateTimeFromTime(start.Add(time.Duration(index) * time.Minute))},
		})
	}
	for index := range 24000 {
		postID := fmt.Sprintf("p%04d", random.Intn(2000))
		if index == 123 || index == 20001 {
			postID = "p9999"
		}
		var identifier primitive.ObjectID
		random.Read(identifier[:])
		comments = appendBSON(t, comments, bson.D{
			{Key: "_id", Value: identifier},
			{Key: "post_id", Value: postID},
			{Key: "author_id", Value: fmt.Sprintf("u%03d", random.Intn(200))},
			{Key: "ts", Value: primitive.NewDateTimeFromTime(start.Add(time.Duration(index) * time.Second))},
			{Key: "body", Value: "comment text"},
		})
	}
	for name, content := range map[string][]byte{"users.bson": users, "posts.bson": posts, "comments.bson": comments} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
}

// appendBSON marshals one document onto out.
func appendBSON(t *testing.T, out []byte, document bson.D) []byte {
	t.Helper()
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatalf("marshalling a fixture document: %v", err)
	}
	return append(out, encoded...)
}
