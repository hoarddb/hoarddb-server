package server

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// TestClusterTokenTextsNameWhatIsTrueNow: a start no longer prints a cluster
// token and reads HOARDDB_CLUSTER_TOKEN only to warn that it does nothing
// (commit 4e1bc202d; cmd/server warns). The texts an operator reads when a
// member refuses to start must not send them back to a first-start print or
// to that variable.
func TestClusterTokenTextsNameWhatIsTrueNow(t *testing.T) {
	dataDir := t.TempDir()
	member := memberTokenChanged(dataDir, dataDir+"/users.json", errors.New("cause"))
	node := &Server{config: &config.Config{}, credentials: CredentialKeys{}}
	node.config.Storage.DataDir = dataDir
	_, requestErr := node.RequestClusterToken()

	for name, text := range map[string]string{
		"clusterTokenSource":    clusterTokenSource,
		"clusterTokenMoveAside": clusterTokenMoveAside,
		"memberTokenChanged":    member.Error(),
		"RequestClusterToken":   requestErr.Error(),
	} {
		if strings.Contains(text, "HOARDDB_CLUSTER_TOKEN") {
			t.Errorf("%s still tells the operator to set HOARDDB_CLUSTER_TOKEN, which a start does not read:\n%s", name, text)
		}
		if strings.Contains(text, "first start printed") {
			t.Errorf("%s still says a first start prints the token, which it no longer does:\n%s", name, text)
		}
	}
}

// TestAMalformedClusterTokenRefusalGivesAFixThatCanBeFollowed: a sealing start still refuses a
// cluster.token that is neither sealed nor a token file. A first start writes none and nothing
// reads one, and no other build was released, so the text names none. No member holds a token to copy and no
// command makes one, so the refusal must not send the operator to another member: the fix is to
// move the file aside and start again, and the text says nothing is lost by it (AGENTS.md rule 3).
func TestAMalformedClusterTokenRefusalGivesAFixThatCanBeFollowed(t *testing.T) {
	dataDir := t.TempDir()
	tokenPath := filepath.Join(dataDir, clusterTokenFileName)
	leftover := []byte("not a token file")
	if err := os.WriteFile(tokenPath, leftover, 0o600); err != nil {
		t.Fatalf("writing the leftover cluster.token: %v", err)
	}
	master := bytes.Repeat([]byte{0x61}, 32)
	sealing := CredentialKeys{nodeMaster: &NodeMaster{bytes: master, kid: sealed.Kid(master)}, seal: true}

	err := sealing.sealCredentialFiles(dataDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("a sealing start accepted a cluster.token that is neither sealed nor a token file")
	}
	for _, want := range []string{tokenPath, "move cluster.token aside", "start again", "a first start writes no cluster.token", "nothing uses the token in one, so nothing is lost without it"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	for _, removed := range []string{"earlier build", "left over", "previous release"} {
		if strings.Contains(err.Error(), removed) {
			t.Errorf("the refusal still says %q, and no other build was released:\n%v", removed, err)
		}
	}
	for _, unfollowable := range []string{"another member", "back in its place", "from a backup"} {
		if strings.Contains(err.Error(), unfollowable) {
			t.Errorf("the refusal still says %q, which no operator can act on for a file nothing uses:\n%v", unfollowable, err)
		}
	}
	after, readErr := os.ReadFile(tokenPath)
	if readErr != nil {
		t.Fatalf("reading cluster.token after the refusal: %v", readErr)
	}
	if !bytes.Equal(after, leftover) {
		t.Errorf("the refusal says nothing was changed, but cluster.token is now %q", after)
	}
}
