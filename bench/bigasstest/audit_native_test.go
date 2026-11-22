package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/bench/scenarios/model"
)

// nativeRun runs one repetition of only one operation on a native fake.
func nativeRun(t *testing.T, only string, mutate func(*fakeEngine)) *Record {
	t.Helper()
	p, _ := fakeRun(t, func(f *fakeEngine) {
		f.native = true
		if mutate != nil {
			mutate(f)
		}
	})
	mix, err := newMix(benchLayouts, only)
	if err != nil {
		t.Fatal(err)
	}
	p.Mix = mix
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// With the copies right, the whole mix passes on the native fake, reads and audit.
func TestRunnerPassesTheWholeMixOnTheNativeFake(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.native = true })
	rec, err := runRepetition(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Failed || !rec.AuditOK || rec.VerifyFailures != 0 || rec.Errors != 0 {
		t.Fatalf("failed=%v audit=%q verify=%d errors=%d samples=%v", rec.Failed, rec.AuditError, rec.VerifyFailures, rec.Errors, rec.ErrorSamples)
	}
	if rec.Native != "on" || rec.Settings["native"] != "on" {
		t.Fatalf("record native %q, settings %v", rec.Native, rec.Settings)
	}
	for _, layout := range []model.Layout{model.Social, model.Chat} {
		gen, err := model.New(layout, 7, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := rec.NativeFingerprint[string(layout)], gen.Native().Fingerprint(); got == "" || got != want {
			t.Errorf("%s native fingerprint %q, want the model's %q", layout, got, want)
		}
	}
	for _, layout := range []model.Layout{model.Ledger, model.Agent} {
		if got, there := rec.NativeFingerprint[string(layout)]; there {
			t.Errorf("%s embeds nothing but the record has a fingerprint %q", layout, got)
		}
	}
}

// A post whose Feed copy holds the wrong text passes every read (a page never reaches a written
// element) and is caught only by the audit.
func TestAuditCatchesAWrongEmbeddedText(t *testing.T) {
	rec := nativeRun(t, string(WriteSocialPost), func(f *fakeEngine) { f.corruptCopy = true })
	if rec.Overall.Requests == 0 {
		t.Fatal("no post was written")
	}
	if rec.VerifyFailures != 0 {
		t.Fatalf("the reads should not see a written copy, got %d verify failures", rec.VerifyFailures)
	}
	if rec.AuditOK || !rec.Failed || !strings.Contains(rec.AuditError, "embedded copy") || !strings.Contains(rec.AuditError, "Feed") {
		t.Fatalf("audit ok=%v failed=%v error=%q, want a failed audit naming the embedded copy of a Feed", rec.AuditOK, rec.Failed, rec.AuditError)
	}
}

func TestAuditCatchesAWrongEmbeddedName(t *testing.T) {
	rec := nativeRun(t, string(WriteChatJoin), func(f *fakeEngine) { f.corruptCopy = true })
	if rec.AuditOK || !strings.Contains(rec.AuditError, "embedded copy") || !strings.Contains(rec.AuditError, "Members") {
		t.Fatalf("audit ok=%v error=%q, want a failed audit naming the embedded copy of a Members", rec.AuditOK, rec.AuditError)
	}
}

// The audit also reads back the seeded elements of the stores it audits.
func TestAuditChecksTheSeededCopiesOfAWrittenStore(t *testing.T) {
	rec := nativeRun(t, string(WriteSocialPost), func(f *fakeEngine) { f.corruptSeeded = true })
	if rec.AuditOK || !strings.Contains(rec.AuditError, "embedded copy") {
		t.Fatalf("audit ok=%v error=%q, want a failed audit naming a seeded embedded copy", rec.AuditOK, rec.AuditError)
	}
}

func TestAuditPassesCorrectCopies(t *testing.T) {
	for _, only := range []WriteKind{WriteSocialPost, WriteChatJoin} {
		if rec := nativeRun(t, string(only), nil); !rec.AuditOK || rec.Failed {
			t.Errorf("%s: audit ok=%v failed=%v error=%q", only, rec.AuditOK, rec.Failed, rec.AuditError)
		}
	}
}

// A native run needs an adapter that can show the copies; one that cannot is not audited as if
// it passed.
func TestNativeAuditNeedsAnEmbedReader(t *testing.T) {
	p, _ := fakeRun(t, func(f *fakeEngine) { f.native = true })
	w := p.World
	b := newBook(w)
	b.native = true
	mix, err := newMix(benchLayouts, string(WriteSocialPost))
	if err != nil {
		t.Fatal(err)
	}
	planner := newPlanner(w, mix, 1, 0, 5)
	req := planner.next()
	b.begin(req.Write)
	b.ack(req.Write, Reply{})
	ops := &noEmbedReader{&countStub{counts: map[StoreRef]int64{}}}
	problems, err := b.auditEmbeds(context.Background(), model.Social, ops)
	if err != nil || len(problems) != 1 || !strings.Contains(problems[0], "EmbedReader") {
		t.Fatalf("auditEmbeds = %v, %v; want one problem naming EmbedReader", problems, err)
	}
}

type noEmbedReader struct{ *countStub }

// The audit of a common run never asks for copies.
func TestCommonAuditDoesNotReadEmbeds(t *testing.T) {
	p, _ := fakeRun(t, nil)
	rec, err := runRepetition(context.Background(), p)
	if err != nil || !rec.AuditOK || rec.Native != "" || len(rec.NativeFingerprint) != 0 {
		t.Fatalf("common run: err=%v audit=%v native=%q fingerprint=%v", err, rec.AuditOK, rec.Native, rec.NativeFingerprint)
	}
}

func TestLegNameCarriesNative(t *testing.T) {
	common, native, batched := completeRecord(), completeRecord(), completeRecord()
	native.Native, batched.Native, batched.Batch = "on", "on", "on"
	common.Native = "off"
	if common.legName() != completeRecord().legName() {
		t.Error("a native=off record keeps the leg name it always had")
	}
	if !strings.HasSuffix(native.legName(), "-native") || !strings.HasSuffix(batched.legName(), "-batched-native") {
		t.Fatalf("legs %q and %q, want -native and -batched-native at the end", native.legName(), batched.legName())
	}
	if dir := t.TempDir(); recordPath(dir, native) == recordPath(dir, common) {
		t.Fatal("native on and off records would overwrite each other")
	}
	encoded, err := json.Marshal(native)
	if err != nil || !strings.Contains(string(encoded), `"native":"on"`) {
		t.Fatalf("the record must carry native: %s %v", encoded, err)
	}
}

func TestRecordRefusesNativeWithoutFingerprint(t *testing.T) {
	rec := completeRecord()
	rec.Native = "on"
	if err := rec.validate(); err == nil || !strings.Contains(err.Error(), "native_fingerprint") {
		t.Fatalf("a native record without its fingerprint must be refused naming it, got %v", err)
	}
	rec.NativeFingerprint = map[string]string{"social": "native-social-0123456789abcdef"}
	if err := rec.validate(); err != nil {
		t.Fatalf("a native record with a fingerprint: %v", err)
	}
	rec = completeRecord()
	rec.Native = "off"
	if err := rec.validate(); err != nil {
		t.Fatalf("a native=off record needs no fingerprint: %v", err)
	}
}

// The summary groups by leg, so a native leg is a table of its own and says which layout it ran;
// it is never averaged into the common one.
func TestNativeRecordsNeverShareASummaryGroupWithCommon(t *testing.T) {
	common, native := recordAt(4, 1, 100, 2, false), recordAt(4, 1, 900, 2, false)
	common.Native = "off"
	native.Native, native.NativeFingerprint = "on", map[string]string{"social": "native-social-1"}
	text := summaryTable([]*Record{common, native})
	if strings.Count(text, "median of the successful repetitions") != 2 {
		t.Fatalf("want two tables, one per leg:\n%s", text)
	}
	if !strings.Contains(text, "hoardDB layout: native") {
		t.Errorf("the native table must say so:\n%s", text)
	}
	if strings.Contains(text, "500.0") {
		t.Errorf("native and common figures were averaged:\n%s", text)
	}
	if strings.Count(text, "hoardDB layout: native") != 1 {
		t.Errorf("only the native table says so:\n%s", text)
	}
}

// A native seed and a common seed give the same visible pages and the same counts once the copies
// are stripped: every read of social and chat, for every owner, returns the same elements and
// documents in the same order, and every store and bucket holds the same number of elements.
func TestNativeAndCommonSeedsGiveTheSameVisiblePages(t *testing.T) {
	common, native := newFakeEngine("bat"), newFakeEngine("bat")
	native.native = true
	for _, layout := range []model.Layout{model.Social, model.Chat} {
		for _, f := range []*fakeEngine{common, native} {
			if err := f.load(layout, 7, 2); err != nil {
				t.Fatal(err)
			}
		}
	}
	w, err := newWorld(7, 2)
	if err != nil {
		t.Fatal(err)
	}
	render := func(reply Reply) string {
		var out strings.Builder
		for _, e := range reply.Elements {
			out.WriteString(canonElement(e) + "\n")
		}
		for _, d := range reply.Documents {
			out.WriteString(d.Bucket + "/" + d.ID + "|" + canonBody(d.Fields) + "\n")
		}
		return out.String()
	}
	pages := 0
	for _, layout := range []model.Layout{model.Social, model.Chat} {
		a, err := (&fakeSession{common}).Layout(layout, layoutDatabase("bat", layout))
		if err != nil {
			t.Fatal(err)
		}
		b, err := (&fakeSession{native}).Layout(layout, layoutDatabase("bat", layout))
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range contractReads[layout] {
			if kind == ReadSocialFollowScan {
				continue
			}
			for _, owner := range w.readOwners(kind) {
				op := *w.readOp(layout, kind, owner)
				want, err := a.Read(context.Background(), op)
				if err != nil {
					t.Fatal(err)
				}
				got, err := b.Read(context.Background(), op)
				if err != nil {
					t.Fatal(err)
				}
				if render(got) != render(want) {
					t.Fatalf("%s of %s differs between the common and the native seed:\ncommon:\n%s\nnative:\n%s", kind, owner, render(want), render(got))
				}
				pages++
			}
		}
		var refs []StoreRef
		for ref := range w.sets[layout].stores {
			refs = append(refs, ref)
		}
		for _, bucket := range []string{"User", "Post", "Room", "Member", "Group", "Follow"} {
			refs = append(refs, StoreRef{Bucket: bucket})
		}
		countsA, err := a.Count(context.Background(), refs)
		if err != nil {
			t.Fatal(err)
		}
		countsB, err := b.Count(context.Background(), refs)
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range refs {
			if countsA[ref] != countsB[ref] {
				t.Fatalf("%s/%s/%s holds %d in the common seed and %d in the native one", ref.Bucket, ref.Owner, ref.Store, countsA[ref], countsB[ref])
			}
		}
	}
	if pages < 100 {
		t.Fatalf("only %d pages were compared", pages)
	}
}
