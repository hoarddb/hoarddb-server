package cli

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// A user across real cluster members (ring user store steps M2a.3 and M2a.4): created through one
// member over the ring, it logs in on another once that member holds the cluster's password key
// and has applied the create; a witness refuses the login with its text; a drop through the second
// member ends it on the first. The first administrator comes from node-a's root task, given a root
// password as cmd/server gives it.

// ringUsersDial logs in as user on addr, failing the test unless it works within timeout.
func ringUsersDial(t *testing.T, addr, user, password string, timeout time.Duration) *ringclient.Client {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: user, Password: password, Insecure: true})
		cancel()
		if err == nil {
			t.Cleanup(func() { _ = client.Close() })
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s could not log in on %s within %v: %v", user, addr, timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestAUserCreatedThroughOneMemberLogsInOnAnother: the user table is the cluster's.
func TestAUserCreatedThroughOneMemberLogsInOnAnother(t *testing.T) {
	nodes := ringJoinCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rootA := ringUsersDial(t, nodes[0].driver, "root", "live-test-password", 10*time.Second)
	if _, err := rootA.Join(ctx, nodes[1].id, nodes[1].listen, false); err != nil {
		t.Fatalf("join node-b: %v", err)
	}
	if _, err := rootA.Join(ctx, nodes[2].id, nodes[2].listen, true); err != nil {
		t.Fatalf("join the witness: %v", err)
	}
	list, err := rootA.ShowUsers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootA.CreateUser(ctx, "ada", "ada-password-12", []ringclient.UserGrant{{Role: "reader", DB: "app"}}, list.DropSeq); err != nil {
		t.Fatalf("create ada through node-a: %v", err)
	}
	ringUsersDial(t, nodes[1].driver, "ada", "ada-password-12", 30*time.Second)

	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	_, err = ringclient.Dial(dialCtx, ringclient.Options{Addr: nodes[2].driver, User: "ada", Password: "ada-password-12", Insecure: true})
	dialCancel()
	if err == nil || !strings.Contains(err.Error(), "this node is a witness") {
		t.Fatalf("a login on the witness answered %v, want the witness text", err)
	}

	rootB := ringUsersDial(t, nodes[1].driver, "root", "live-test-password", 30*time.Second)
	if _, err := rootB.DropUser(ctx, "ada", nil); err != nil {
		t.Fatalf("drop ada through node-b: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
		client, err := ringclient.Dial(dialCtx, ringclient.Options{Addr: nodes[0].driver, User: "ada", Password: "ada-password-12", Insecure: true})
		dialCancel()
		if err != nil {
			break
		}
		_ = client.Close()
		if time.Now().After(deadline) {
			t.Fatal("ada still logs in on node-a 10 s after node-b dropped it")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTheCLIUserStatementsWorkThroughAClusterMember is U2's end-to-end proof on a cluster: a ring
// CLI session on node-a runs create user and alter user password (each from its own password
// file) and show users; the user then logs in on node-b with the new password, and the old one is
// refused there; a create of a name that exists exits 2 (USER_EXISTS).
func TestTheCLIUserStatementsWorkThroughAClusterMember(t *testing.T) {
	nodes := ringJoinCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rootA := ringUsersDial(t, nodes[0].driver, "root", "live-test-password", 10*time.Second)
	if _, err := rootA.Join(ctx, nodes[1].id, nodes[1].listen, false); err != nil {
		t.Fatalf("join node-b: %v", err)
	}
	create := NewRingSession(NewRingAPI(rootA), WithRingPasswordSource(u80Passwords(t, "dora-first-pw-1"), false, io.Discard))
	got := u52Transcript(t, ctx, create, "create user dora roles [reader@*]", "show users")
	if !strings.Contains(got, `user "dora" created (revision `) || !strings.Contains(got, "dora: enabled, roles [reader@*]") {
		t.Fatalf("create through node-a printed:\n%s", got)
	}
	alter := NewRingSession(NewRingAPI(rootA), WithRingPasswordSource(u80Passwords(t, "dora-second-pw-2"), false, io.Discard))
	got = u52Transcript(t, ctx, alter, "alter user dora password")
	if !strings.Contains(got, `user "dora" altered (revision `) || strings.Contains(got, "dora-second-pw-2") {
		t.Fatalf("alter through node-a printed:\n%s", got)
	}
	ringUsersDial(t, nodes[1].driver, "dora", "dora-second-pw-2", 30*time.Second)
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	old, err := ringclient.Dial(dialCtx, ringclient.Options{Addr: nodes[1].driver, User: "dora", Password: "dora-first-pw-1", Insecure: true})
	dialCancel()
	if err == nil {
		_ = old.Close()
		t.Fatal("the old password still logs in on node-b after the CLI's alter")
	}
	_, err = alter.Execute(ctx, "create user dora")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "USER_EXISTS" || failed.Exit != ExitRefused {
		t.Fatalf("create of an existing name on a cluster: %v (%+v)", err, failed)
	}
}
