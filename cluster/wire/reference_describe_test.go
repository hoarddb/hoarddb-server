package wire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode"
)

// denyDescribeOf refuses describe on one bucket and allows the rest.
type denyDescribeOf struct{ bucket string }

func (d denyDescribeOf) Authorize(_ context.Context, access RingAccess) error {
	if access.Bucket == d.bucket && access.Class == RingClassDescribeBucket {
		return errors.New("no grant")
	}
	return nil
}

// TestDescribePrintsNoNameOrIdForAReferenceItMayNotName: [GUARD describe of a bucket declared
// of an ungranted bucket prints <unavailable>, never the name and never the bucket's id].
func TestDescribePrintsNoNameOrIdForAReferenceItMayNotName(t *testing.T) {
	world, ctx := referenceWorld(t)
	of := func(authz RingAuthorizer) string {
		t.Helper()
		world.server.authz = authz
		reply, err := world.server.describe(ctx, AdminDescribeBucket, u30iDB, "Message")
		if err != nil {
			t.Fatal(err)
		}
		return reply.Fields[1].Of
	}
	if got := of(u70AllowAll{}); got != "User" {
		t.Fatalf("a granted reference prints %q, want User", got)
	}
	got := of(denyDescribeOf{bucket: "User"})
	if got != referenceUnavailable {
		t.Fatalf("an ungranted reference prints %q, want %q", got, referenceUnavailable)
	}
	if strings.ContainsFunc(got, unicode.IsDigit) || strings.Contains(got, "User") {
		t.Fatalf("the unavailable reference leaks the id or the name: %q", got)
	}
}
