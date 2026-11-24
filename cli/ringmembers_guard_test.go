package cli

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The member set's three guards that a dump through a healthy cluster never reaches (BUG-0903):
// the end-of-run membership check, the refusal of a bucket name two members list, and the
// refusal of a --member that names no member.

// TestTheEndOfRunCheckRefusesWhenAMemberWentDown: a member marked down moves the epoch, so a
// run whose pages were read before the change cannot know each document was read once. The check
// a dump makes before it completes its file must refuse with exit 8. (Written by the verifier of
// the fix.)
func TestTheEndOfRunCheckRefusesWhenAMemberWentDown(t *testing.T) {
	nodes := startDumpCluster(t, dumpClusterOptions{advertise: true})
	seedDumpCluster(t, nodes)
	ctx := context.Background()
	options := ringclient.Options{Addr: nodes[0].driver, User: "root", Password: dumpClusterPassword, Insecure: true}
	entry, err := ringclient.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = entry.Close() }()
	set, err := openRingMembers(ctx, ringMemberDial{entry: entry, options: options, notice: io.Discard, verb: "dump"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer set.close()
	if err := set.checkUnchanged(ctx, "dump", nil); err != nil {
		t.Fatalf("the check refused a cluster whose membership did not change: %v", err)
	}
	if err := nodes[2].service.Stop(); err != nil {
		t.Fatal(err)
	}
	waitMemberDown(t, nodes[0], "node-c")
	// node-a is checked first and has applied the change waitMemberDown saw.
	err = set.checkUnchanged(ctx, "dump", nil)
	if err == nil {
		t.Fatal("the end-of-run check passed after node-c was marked down")
	}
	failed := ringFailure(err)
	if failed.Exit != ExitUnavailable || !strings.Contains(failed.Message, "membership changed while the dump ran") {
		t.Errorf("the check answered exit %d %q; want exit %d and the membership-changed text", failed.Exit, failed.Message, ExitUnavailable)
	}
}

// TestABucketListedByTwoMembersIsRefused: at one epoch the members' shares are disjoint, so a name
// listed twice means they overlap, and documents would be read twice too. The listing refuses
// instead of merging. The overlap is made by giving the set one node's connection twice.
func TestABucketListedByTwoMembersIsRefused(t *testing.T) {
	_, client := newRingTestHost(t)
	dumpTestDatabase(t, client, "app")
	set := &ringMemberSet{members: []ringReadMember{{id: "a", client: client, entry: true}, {id: "b", client: client}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var cursor []byte
	for {
		page, err := set.ShowBuckets(ctx, "app", ringclient.PageRequest{Cursor: cursor})
		if err != nil {
			failed := ringFailure(err)
			if failed.Exit != ExitUnavailable || !strings.Contains(failed.Message, `bucket "Seed" of database "app" was listed by two members`) {
				t.Errorf("the overlap was refused with exit %d %q; want exit %d and the listed-by-two-members text", failed.Exit, failed.Message, ExitUnavailable)
			}
			return
		}
		if !page.More {
			t.Fatal("a bucket listed by both members was returned twice without a refusal")
		}
		cursor = page.Cursor
	}
}

// TestAMemberFlagThatNamesNoMemberIsRefused: a --member with a mistyped id would be ignored while
// the member it meant is refused for having no address; it is refused itself, naming the members.
func TestAMemberFlagThatNamesNoMemberIsRefused(t *testing.T) {
	addr, client := newRingTestHost(t)
	dumpTestDatabase(t, client, "app")
	outPath := filepath.Join(t.TempDir(), "never.hoarddump")
	code, stdout, stderr := dumpOnHost(t, addr, "--out", outPath, "--member", "nope=127.0.0.1:1")
	if code != ExitUsage || !strings.Contains(stderr, `--member names "nope", which is not a data member of this cluster`) {
		t.Errorf("--member nope: exit %d, want %d and the not-a-member text\nstdout: %s\nstderr: %s", code, ExitUsage, stdout, stderr)
	}
	requireNoDumpFile(t, outPath)
}
