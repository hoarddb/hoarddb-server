package cluster

import (
	"strings"
	"testing"
)

// TestDeclUnavailableNamesAnUnansweredReadAndTheCheck is BUG-0770: the refusal said the
// slot owner "did not answer" and sent the developer to replace-node, for a read that had
// only waited its turn; and a refusal built from ids left "<slot>" and "<node>" in the text.
// It must say the read did not complete, name the check, and send nobody to replace a node.
func TestDeclUnavailableNamesAnUnansweredReadAndTheCheck(t *testing.T) {
	text := CodeDeclUnavailable.Text(CondDeclUnavailable, map[string]string{"db": "1", "bucket": "3182714196521298707"})
	for _, want := range []string{"'nodes'", "'status'", "Serving: ok", "busy", "send the write again"} {
		if !strings.Contains(text, want) {
			t.Errorf("DECL_UNAVAILABLE text lacks %q:\n%s", want, text)
		}
	}
	for _, banned := range []string{"replace-node", "<slot>", "<node>", "is owned by", "state=active serving=ok", "it did not answer, and"} {
		if strings.Contains(text, banned) {
			t.Errorf("DECL_UNAVAILABLE text still has %q:\n%s", banned, text)
		}
	}
}
