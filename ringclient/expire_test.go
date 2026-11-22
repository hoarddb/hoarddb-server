package ringclient

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func TestIsExpiredReadsTheStructuredDetailNotTheText(t *testing.T) {
	expired := &Error{Code: cluster.CodeNotFound, Message: "any words at all", Detail: &cluster.Detail{Expired: true}}
	plain := &Error{Code: cluster.CodeNotFound, Message: "document \"x\" expired somewhere in this sentence"}
	if !IsExpired(fmt.Errorf("get: %w", expired)) {
		t.Error("a NOT_FOUND with detail expired was not reported expired")
	}
	if IsExpired(plain) {
		t.Error("a plain NOT_FOUND whose text mentions expired was reported expired: a driver acts on the detail, never the text")
	}
	if IsExpired(errors.New("other")) || IsExpired(nil) {
		t.Error("a non-refusal was reported expired")
	}
	if !errors.Is(expired, ErrNotFound) {
		t.Error("an expired miss is still ErrNotFound")
	}
}
