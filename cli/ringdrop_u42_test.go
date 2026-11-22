package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// TestDropBucketSaysWhatWentAndAsksNothing: the ring statement drops the bucket the statement
// names in the database it names or the selected one, prints one OK line, and asks nothing first,
// as the drop bucket of v1.0 does not; a bucket that is not there is the server's NOT_FOUND with
// its fix.
func TestDropBucketSaysWhatWentAndAsksNothing(t *testing.T) {
	session, fake := u52Session()
	got, err := u52Run(session, "create database app", "create database shop", "use app",
		"create bucket User { Name string }", "create bucket Order in shop { Total int }", "drop bucket User")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "OK  dropping bucket app.User; its documents are deleted in the background\n") {
		t.Fatalf("printed:\n%s", got)
	}
	if _, ok := fake.buckets["app.User"]; ok {
		t.Fatal("the bucket is still there")
	}
	got, err = u52Run(session, "drop bucket Order in shop")
	if err != nil || got != "OK  dropping bucket shop.Order; its documents are deleted in the background\n" {
		t.Fatalf("drop in another database: %q, %v", got, err)
	}
	_, err = u52Run(session, "drop bucket User")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "NOT_FOUND" || !strings.Contains(failed.Message, "show buckets in app") {
		t.Fatalf("a second drop = %v, want NOT_FOUND with the fix", err)
	}
}

// TestDropBucketNeedsADatabaseAndIsNotAPrompt: with no database selected the statement is refused
// before anything is sent, naming the two ways to choose one; and piped input drops without a
// question, the same as a terminal.
func TestDropBucketNeedsADatabaseAndIsNotAPrompt(t *testing.T) {
	session, _ := u52Session()
	_, err := u52Run(session, "drop bucket User")
	var failed *RingError
	if !errors.As(err, &failed) || !strings.Contains(failed.Message, "use <db>") {
		t.Fatalf("no database: %v", err)
	}
	session, fake := u52Session()
	var out, errOut bytes.Buffer
	script := "create database app\nuse app\ncreate bucket User { Name string }\ndrop bucket User\n"
	if code := session.RunPiped(context.Background(), strings.NewReader(script), &out, &errOut); code != 0 {
		t.Fatalf("piped run exited %d: %s", code, errOut.String())
	}
	if _, ok := fake.buckets["app.User"]; ok || !strings.Contains(out.String(), "OK  dropping bucket app.User; its documents are deleted in the background") {
		t.Fatalf("piped drop printed %q and left the bucket %v", out.String(), ok)
	}
}
