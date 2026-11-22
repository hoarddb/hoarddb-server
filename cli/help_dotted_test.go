package cli

import (
	"bytes"
	"regexp"
	"testing"
)

// dottedSubBucket matches a help example that reads as a nested bucket: two
// name components before the method, as in db.Post.Comments.push(...).
var dottedSubBucket = regexp.MustCompile(`db\.[A-Za-z]+\.[A-Za-z]+\.[a-z]+\(`)

// TestHelpTeachesNoDottedSubBucket checks that the help does not teach the dotted form. The help
// printed db.Post.Comments.push(...) under "Sub-bucket examples", and the
// parser sends that as one flat bucket named "Post.Comments": there is no
// nesting yet. Neither the main help nor any topic may teach the form.
func TestHelpTeachesNoDottedSubBucket(t *testing.T) {
	var out bytes.Buffer
	handler := NewCommandHandler(&out, false)
	handler.handleHelp()
	for _, topic := range helpTopicList() {
		requireHelpTopic(t, handler, topic)
	}
	for _, example := range dottedSubBucket.FindAllString(out.String(), -1) {
		t.Errorf("the help teaches %q as a sub-bucket, but the CLI sends it as one flat bucket with a dot in its name", example)
	}
}
