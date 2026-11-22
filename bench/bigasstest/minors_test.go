package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

func TestCheckFreshSeesAStrayDocumentInAnyBucket(t *testing.T) {
	p, fake := fakeRun(t, nil)
	session, _ := fake.Session(context.Background(), 0)
	ops, _ := session.Layout(model.Social, "bat_social")
	stray := WriteOp{Layout: model.Social, Documents: []model.Document{{Bucket: "Post", ID: "strayPost", Fields: []model.Field{{Name: "Author", Value: "u0"}}}}}
	if _, err := ops.Write(context.Background(), stray); err != nil {
		t.Fatal(err)
	}
	if err := checkFresh(context.Background(), p.World, model.Social, ops); err == nil || !strings.Contains(err.Error(), "Post") {
		t.Errorf("a stray document in a bucket the sample misses must be refused, got %v", err)
	}
}

func TestFreshSampleCoversManyOwners(t *testing.T) {
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = string(rune('a'+i%26)) + string(rune('0'+i/26))
	}
	picked := sampleIDs(ids)
	seen := map[string]bool{}
	for _, id := range picked {
		seen[id] = true
	}
	if len(seen) < 8 || !seen[ids[0]] || !seen[ids[99]] {
		t.Errorf("sample %v", picked)
	}
	if got := sampleIDs(ids[:3]); len(got) != 3 {
		t.Errorf("a short list is sampled whole, got %v", got)
	}
}

func TestRecordRefusesAnUnknownTLSOrVersion(t *testing.T) {
	for _, key := range []string{"tls", "engine_version"} {
		rec := completeRecord()
		rec.Settings[key] = "unknown"
		if err := rec.validate(); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("settings.%s = unknown must be refused, got %v", key, err)
		}
	}
}

func TestVerifyRequiresAcknowledgedPushesToBeVisible(t *testing.T) {
	w := testWorld(t)
	ds := w.sets[model.Social]
	var user string
	for _, id := range ds.ids("User") {
		if n := len(ds.elements(StoreRef{"User", id, "Inbox"})); n < 5 {
			user = id
			break
		}
	}
	if user == "" {
		t.Skip("every inbox is full at this scale")
	}
	ref := StoreRef{"User", user, "Inbox"}
	op := ReadOp{Layout: model.Social, Kind: ReadSocialInbox, Entity: user, Limit: 20}
	page := Reply{Elements: seededReply(ds, ref, 20)}
	floor := func(r StoreRef) int {
		if r == ref {
			return 1
		}
		return 0
	}
	if err := verifyReadFloor(w, newRegistry(), op, page, nil); err != nil {
		t.Fatalf("without acknowledged pushes the seeded page is fine: %v", err)
	}
	if err := verifyReadFloor(w, newRegistry(), op, page, floor); !errors.Is(err, errShortPage) {
		t.Errorf("a page that omits an acknowledged push must be a miss, got %v", err)
	}
}

func TestClaimOrderIsAudited(t *testing.T) {
	good := onlyOp(t, "agent.task", nil)
	if !good.AuditOK {
		t.Fatalf("a FIFO queue failed the audit: %s", good.AuditError)
	}
	bad := onlyOp(t, "agent.task", func(f *fakeEngine) { f.claimNewest = true })
	if bad.AuditOK || !strings.Contains(bad.AuditError, "claim order") {
		t.Errorf("a queue that hands out the newest task passed: ok=%v %q", bad.AuditOK, bad.AuditError)
	}
}

func TestSessionCloseErrorsReachTheRecord(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.closeError = errors.New("connection reset") })
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.CloseErrors) == 0 || !strings.Contains(rec.CloseErrors[0], "connection reset") {
		t.Errorf("close errors %v", rec.CloseErrors)
	}
}

func TestPlannerFieldLookupFailsInsteadOfPanicking(t *testing.T) {
	if _, err := fieldString([]model.Field{{Name: "Author", Value: int64(3)}}, "Author"); err == nil {
		t.Error("a non-string field must be an error")
	}
	if _, err := fieldString(nil, "Author"); err == nil {
		t.Error("a missing field must be an error")
	}
	if got, err := fieldString([]model.Field{{Name: "Author", Value: "u1"}}, "Author"); err != nil || got != "u1" {
		t.Errorf("got %q %v", got, err)
	}
}

func TestErrorSamplesCarryNoAddresses(t *testing.T) {
	quad := strings.Join([]string{"10", "1", "2", "3"}, ".") // built at run time: no address literals in the tree
	for _, in := range []string{
		"dial tcp " + quad + ":5432: connect: connection refused",
		"read tcp [fe80::1]:5432->[2001:db8::2]:41000: timeout",
		"lookup db-7.example.com:27017: no such host",
	} {
		u := newVuserState()
		u.sample(in)
		got := u.samples[0]
		if strings.Contains(got, quad) || strings.Contains(got, "fe80") || strings.Contains(got, "2001") || strings.Contains(got, "example.com") {
			t.Errorf("%q kept an address: %q", in, got)
		}
		if !strings.Contains(got, "<addr>") {
			t.Errorf("%q: %q", in, got)
		}
	}
}

func TestRunFlagsAreRecordedAndSecretsAreNot(t *testing.T) {
	cfg := runConfig{Database: "bat", DiskGuard: false, WarmMax: 0, RequestTimeout: 5e9,
		Params: map[string]string{"w": "1", "password": "hunter2", "journal": "true"}}
	got := describeRunFlags(cfg)
	if got["database"] != "bat" || got["disk_guard"] != "false" || got["warm_max"] != "0s" || got["param.w"] != "1" || got["param.journal"] != "true" {
		t.Errorf("run flags %v", got)
	}
	for key, value := range got {
		if strings.Contains(value, "hunter2") {
			t.Errorf("%s leaks a secret: %q", key, value)
		}
	}
	if got["param.password"] != "<redacted>" {
		t.Errorf("param.password = %q", got["param.password"])
	}
	p, _ := fakeRun(t, nil)
	p.RunFlags = got
	rec, err := runRepetition(context.Background(), p)
	if err != nil || rec.RunFlags["database"] != "bat" {
		t.Errorf("record run flags %v %v", rec.RunFlags, err)
	}
}

func TestWarmMaxZeroNeedsAllowCold(t *testing.T) {
	base := []string{"-engine", "ring", "-facts", "f.json", "-out", "o", "-reseed-cmd", "true", "-data-dir", "/d", "-warm-max", "0"}
	if _, err := parseRunFlags(base); err == nil || !strings.Contains(err.Error(), "-allow-cold") {
		t.Errorf("a cold comparison run must be refused, got %v", err)
	}
	if _, err := parseRunFlags(append(base, "-allow-cold")); err != nil {
		t.Errorf("-allow-cold must permit it: %v", err)
	}
}
