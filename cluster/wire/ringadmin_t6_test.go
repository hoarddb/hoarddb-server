package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The T6 tests cover the seeding operator surface: three RING_ADMIN commands
// (seeding_status, seeding_retry, seeding_forget), their codec, and the server end that
// forwards them to a SeedingView.

func t6Slot(v int32) *int32 { return &v }

func t6Requests() []RingAdminRequest {
	return []RingAdminRequest{
		{Cmd: AdminSeedingStatus},
		{Cmd: AdminSeedingStatus, Database: "app"},
		{Cmd: AdminSeedingStatus, Database: "app", Slot: t6Slot(0)},
		{Cmd: AdminSeedingStatus, Database: "app", Slot: t6Slot(slotCount - 1), Limit: u75Int32(10), Cursor: []byte{0, 5, 'a'}},
		{Cmd: AdminSeedingStatus, Limit: u75Int32(seedingMaxPage)},
		{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(2638)},
		{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(0)},
		{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(7), Anyway: true},
	}
}

func t6Row(database string, slot int32, state SeedState) RingAdminSeedRow {
	return RingAdminSeedRow{Database: database, Slot: slot, State: string(state)}
}

func t6Replies() []RingAdminReply {
	full := RingAdminSeedRow{Database: "app", Slot: 2638, State: string(SeedStalled), Source: "node-b", BytesDone: 41 << 20, BytesKnown: 66 << 20,
		Entries: 123456, IdleMS: 372000, Refusal: "no chunk applied since 14:02:11"}
	return []RingAdminReply{
		{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}}},
		{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{full, t6Row("app", 2639, SeedPlacing), t6Row("zeta", 1, SeedReady), t6Row("zeta", 2, SeedRetired), t6Row("zeta", 3, SeedSeeding), t6Row("zeta", 4, SeedRetiring)},
			More: true, Cursor: []byte{0x0a, 0x4f, 'z', 'e', 't', 'a'}}},
		{Cmd: AdminSeedingRetry, Seeding: &RingAdminSeeding{Database: "app", Slot: 2638}},
		{Cmd: AdminSeedingForget, Seeding: &RingAdminSeeding{Database: "app", Slot: 0}},
	}
}

// TestT6SeedingCommandsRoundTrip: [GUARD every request and reply shape of the three commands,
// optional members present and absent, is encoded and decoded to the same value].
func TestT6SeedingCommandsRoundTrip(t *testing.T) {
	for _, req := range t6Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%+v: %v", req, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("request round trip gave %+v, %v, want %+v", got, err, req)
		}
	}
	for _, reply := range t6Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%+v: %v", reply.Seeding, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil || !reflect.DeepEqual(got, reply) {
			t.Fatalf("reply round trip gave %+v, %v, want %+v", got.Seeding, err, reply.Seeding)
		}
	}
}

// TestT6SeedingRequestRefusalsNameTheFix: [GUARD each limit of a request is checked and the
// text names the member and what to send instead].
func TestT6SeedingRequestRefusalsNameTheFix(t *testing.T) {
	cases := []struct {
		name string
		req  RingAdminRequest
		want []string
	}{
		{"status with a bucket", RingAdminRequest{Cmd: AdminSeedingStatus, Bucket: "User"}, []string{"seeding_status", "bucket", "Remove"}},
		{"status slot without database", RingAdminRequest{Cmd: AdminSeedingStatus, Slot: t6Slot(1)}, []string{"seeding_status", "slot", "database"}},
		{"status slot negative", RingAdminRequest{Cmd: AdminSeedingStatus, Database: "app", Slot: t6Slot(-1)}, []string{"slot", "0 to 8191"}},
		{"status slot past the last", RingAdminRequest{Cmd: AdminSeedingStatus, Database: "app", Slot: t6Slot(slotCount)}, []string{"slot", "8191"}},
		{"status limit negative", RingAdminRequest{Cmd: AdminSeedingStatus, Limit: u75Int32(-1)}, []string{"limit", "default"}},
		{"status limit over the ceiling", RingAdminRequest{Cmd: AdminSeedingStatus, Limit: u75Int32(seedingMaxPage + 1)}, []string{"limit", fmt.Sprint(seedingMaxPage)}},
		{"status long cursor", RingAdminRequest{Cmd: AdminSeedingStatus, Cursor: bytes.Repeat([]byte{1}, ringMaxName+3)}, []string{"cursor", "unchanged"}},
		{"status long database", RingAdminRequest{Cmd: AdminSeedingStatus, Database: strings.Repeat("d", ringMaxName+1)}, []string{"database"}},
		{"retry without a slot", RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app"}, []string{"seeding_retry", "slot", "status seeding"}},
		{"retry without a database", RingAdminRequest{Cmd: AdminSeedingRetry, Slot: t6Slot(1)}, []string{"seeding_retry", "database"}},
		{"retry with a limit", RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(1), Limit: u75Int32(5)}, []string{"seeding_retry", "limit", "Remove"}},
		{"forget with a cursor", RingAdminRequest{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(1), Cursor: []byte{1}}, []string{"seeding_forget", "cursor", "Remove"}},
		{"retry with anyway", RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(1), Anyway: true}, []string{"seeding_retry", "anyway", "seeding_forget"}},
		{"status with anyway", RingAdminRequest{Cmd: AdminSeedingStatus, Anyway: true}, []string{"seeding_status", "anyway", "seeding_forget"}},
		{"forget slot past the last", RingAdminRequest{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(9000)}, []string{"slot", "8191"}},
	}
	for _, tc := range cases {
		_, err := EncodeRingAdmin(tc.req)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: encoded or wrong error: %v", tc.name, err)
			continue
		}
		for _, word := range tc.want {
			if !strings.Contains(coded.Message, word) {
				t.Errorf("%s: the refusal %q does not mention %q", tc.name, coded.Message, word)
			}
		}
	}
}

func t6Rows(n int) []RingAdminSeedRow {
	rows := make([]RingAdminSeedRow, n)
	for i := range rows {
		rows[i] = t6Row("app", int32(i%slotCount), SeedReady)
	}
	return rows
}

// TestT6SeedingReplyRefusalsNameTheMember: [GUARD each bound of a reply row, the state words
// and the members a command does not answer with].
func TestT6SeedingReplyRefusalsNameTheMember(t *testing.T) {
	row := func(edit func(*RingAdminSeedRow)) RingAdminReply {
		r := t6Row("app", 5, SeedSeeding)
		edit(&r)
		return RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{r}}}
	}
	cases := []struct {
		name  string
		reply RingAdminReply
		want  []string
	}{
		{"no seeding member", RingAdminReply{Cmd: AdminSeedingStatus}, []string{"seeding_status", "rows"}},
		{"status without rows", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{}}, []string{"seeding_status", "rows"}},
		{"status with a database", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}, Database: "app"}}, []string{"seeding_status", "nothing else"}},
		{"retry with rows", RingAdminReply{Cmd: AdminSeedingRetry, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}, Database: "app"}}, []string{"seeding_retry", "database and slot"}},
		{"retry with more", RingAdminReply{Cmd: AdminSeedingRetry, Seeding: &RingAdminSeeding{Database: "app", More: true}}, []string{"seeding_retry"}},
		{"retry with no database", RingAdminReply{Cmd: AdminSeedingRetry, Seeding: &RingAdminSeeding{Slot: 1}}, []string{"seeding_retry", "database"}},
		{"forget slot past the last", RingAdminReply{Cmd: AdminSeedingForget, Seeding: &RingAdminSeeding{Database: "app", Slot: slotCount}}, []string{"slot"}},
		{"seeding on another command", RingAdminReply{Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{}, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}}}, []string{"show_databases", "seeding"}},
		{"seeding row on describe_node", RingAdminReply{Cmd: AdminDescribeNode, Node: u83Replies()[0].Node, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}}}, []string{"describe_node"}},
		{"bucket member on status", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: []RingAdminSeedRow{}}, Buckets: []RingAdminListed{}}, []string{"seeding_status", "nothing else"}},
		{"empty database", row(func(r *RingAdminSeedRow) { r.Database = "" }), []string{"database"}},
		{"slot past the last", row(func(r *RingAdminSeedRow) { r.Slot = slotCount }), []string{"slot", "8191"}},
		{"unknown state", row(func(r *RingAdminSeedRow) { r.State = "paused" }), []string{"state", "placing", "retiring", "retired"}},
		{"empty state", row(func(r *RingAdminSeedRow) { r.State = "" }), []string{"state"}},
		{"upper case state", row(func(r *RingAdminSeedRow) { r.State = "SEEDING" }), []string{"state"}},
		{"negative bytes done", row(func(r *RingAdminSeedRow) { r.BytesDone = -1 }), []string{"bytes_done"}},
		{"negative bytes known", row(func(r *RingAdminSeedRow) { r.BytesKnown = -1 }), []string{"bytes_known"}},
		{"negative entries", row(func(r *RingAdminSeedRow) { r.Entries = -1 }), []string{"entries"}},
		{"negative idle", row(func(r *RingAdminSeedRow) { r.IdleMS = -1 }), []string{"idle_ms"}},
		{"long source", row(func(r *RingAdminSeedRow) { r.Source = strings.Repeat("s", ringMaxNodeText+1) }), []string{"source"}},
		{"long refusal", row(func(r *RingAdminSeedRow) { r.Refusal = strings.Repeat("r", seedingMaxRefusal+1) }), []string{"refusal"}},
		{"non UTF-8 refusal", row(func(r *RingAdminSeedRow) { r.Refusal = "\xff\xfe" }), []string{"refusal", "UTF-8"}},
		{"too many rows", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: t6Rows(seedingMaxPage + 1)}}, []string{"rows", fmt.Sprint(seedingMaxPage)}},
		{"more without a cursor", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: t6Rows(1), More: true}}, []string{"cursor", "more"}},
		{"a cursor without more", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: t6Rows(1), Cursor: []byte{0, 1, 'a'}}}, []string{"cursor", "more"}},
		{"a cursor over the bound", RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: t6Rows(1), More: true, Cursor: make([]byte, seedingCursorMax+1)}}, []string{"cursor"}},
	}
	for _, tc := range cases {
		_, err := EncodeRingAdminReply(tc.reply)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: encoded or wrong error: %v", tc.name, err)
			continue
		}
		for _, word := range tc.want {
			if !strings.Contains(coded.Message, word) {
				t.Errorf("%s: the refusal %q does not mention %q", tc.name, coded.Message, word)
			}
		}
	}
}

// TestT6SeedingDecodersAreStrict: [GUARD canonical layout: extras, order, types, written
// defaults and widened numbers are refused for the requests and the replies].
func TestT6SeedingDecodersAreStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	status := bson.E{Key: "cmd", Value: AdminSeedingStatus}
	retry := bson.E{Key: "cmd", Value: AdminSeedingRetry}
	db := bson.E{Key: "database", Value: "app"}
	slot := bson.E{Key: "slot", Value: int32(5)}
	requests := map[string][]byte{
		"status extra member":      doc(status, bson.E{Key: "extra", Value: int32(1)}),
		"status slot before db":    doc(status, slot, db),
		"status slot as int64":     doc(status, db, bson.E{Key: "slot", Value: int64(5)}),
		"status slot as string":    doc(status, db, bson.E{Key: "slot", Value: "5"}),
		"status database twice":    doc(status, db, db),
		"status limit as int64":    doc(status, bson.E{Key: "limit", Value: int64(5)}),
		"status cursor as string":  doc(status, bson.E{Key: "cursor", Value: "x"}),
		"status written cursor":    doc(status, bson.E{Key: "cursor", Value: []byte{}}),
		"retry slot before db":     doc(retry, slot, db),
		"retry missing slot":       doc(retry, db),
		"retry slot as double":     doc(retry, db, bson.E{Key: "slot", Value: float64(5)}),
		"retry reordered with cmd": doc(db, slot, retry),
	}
	for name, raw := range requests {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("request %s: accepted", name)
		}
	}
	rowDoc := func(mutate func(bson.D) bson.D) bson.D {
		row := bson.D{{Key: "database", Value: "app"}, {Key: "slot", Value: int32(5)}, {Key: "state", Value: "seeding"}, {Key: "source", Value: "node-b"},
			{Key: "bytes_done", Value: int64(1)}, {Key: "bytes_known", Value: int64(2)}, {Key: "entries", Value: int64(3)}, {Key: "idle_ms", Value: int64(4)}, {Key: "refusal", Value: ""}}
		if mutate != nil {
			row = mutate(row)
		}
		return row
	}
	statusReply := func(mutate func(bson.D) bson.D, tail ...bson.E) []byte {
		elements := bson.D{{Key: "cmd", Value: AdminSeedingStatus}, {Key: "rows", Value: bson.A{rowDoc(mutate)}}}
		elements = append(elements, tail...)
		raw, err := bson.Marshal(elements)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := DecodeRingAdminReply(statusReply(nil)); err != nil {
		t.Fatalf("the plain status reply is refused: %v", err)
	}
	replies := map[string][]byte{
		"slot as int64":        statusReply(func(d bson.D) bson.D { d[1].Value = int64(5); return d }),
		"bytes as int32":       statusReply(func(d bson.D) bson.D { d[4].Value = int32(1); return d }),
		"reordered members":    statusReply(func(d bson.D) bson.D { d[3], d[4] = d[4], d[3]; return d }),
		"row extra member":     statusReply(func(d bson.D) bson.D { return append(d, bson.E{Key: "eta", Value: int64(9)}) }),
		"row missing refusal":  statusReply(func(d bson.D) bson.D { return d[:8] }),
		"unknown state":        statusReply(func(d bson.D) bson.D { d[2].Value = "paused"; return d }),
		"written more false":   statusReply(nil, bson.E{Key: "more", Value: false}),
		"written empty cursor": statusReply(nil, bson.E{Key: "cursor", Value: []byte{}}),
		"extra member":         statusReply(nil, bson.E{Key: "total", Value: int32(9)}),
		"rows not array":       doc(status, bson.E{Key: "rows", Value: "x"}),
		"retry slot missing":   doc(retry, db),
		"retry extra":          doc(retry, db, slot, bson.E{Key: "state", Value: "x"}),
		"retry as int64":       doc(retry, db, bson.E{Key: "slot", Value: int64(5)}),
	}
	for name, raw := range replies {
		if _, err := DecodeRingAdminReply(raw); err == nil {
			t.Errorf("reply %s: accepted", name)
		}
	}
}

// TestT6SeedingDecodersSurviveMutation: [GUARD 100000 mutations: no panic, an accepted body
// re-encodes to the same bytes].
func TestT6SeedingDecodersSurviveMutation(t *testing.T) {
	var requests, replies [][]byte
	for _, req := range t6Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, raw)
	}
	for _, reply := range t6Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, raw)
	}
	rng := rand.New(rand.NewPCG(6, 6))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, requests[rng.IntN(len(requests))])
		if req, err := DecodeRingAdmin(mutated); err == nil {
			acceptedRequests++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, replies[rng.IntN(len(replies))])
		if reply, err := DecodeRingAdminReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeRingAdminReply(reply); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestT6SeedingDecodeAllocationIsBounded: [GUARD a body that lies about its length, and a
// rows array that lies about its own, allocate a bounded amount before they are refused].
func TestT6SeedingDecodeAllocationIsBounded(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0}
	lie = append(lie, byte(len(AdminSeedingStatus)+1), 0, 0, 0)
	lie = append(lie, AdminSeedingStatus...)
	lie = append(lie, 0, 0x04, 'r', 'o', 'w', 's', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdmin(lie)
			_, _ = DecodeRingAdminReply(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying seeding body of %d bytes allocated %d bytes", len(lie), perOp)
	}
}

// TestT6AFullPageOfWorstCaseRowsFitsOneFrame: [GUARD the page ceiling times the largest row
// the codec accepts is a reply the encoder takes, so no accepted view can make a status fail].
func TestT6AFullPageOfWorstCaseRowsFitsOneFrame(t *testing.T) {
	worst := RingAdminSeedRow{Database: strings.Repeat("d", ringMaxName), Slot: slotCount - 1, State: string(SeedStalled), Source: strings.Repeat("s", ringMaxNodeText),
		BytesDone: 1 << 62, BytesKnown: 1 << 62, Entries: 1 << 62, IdleMS: 1 << 62, Refusal: strings.Repeat("r", seedingMaxRefusal)}
	rows := make([]RingAdminSeedRow, seedingMaxPage)
	for i := range rows {
		rows[i] = worst
	}
	raw, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminSeedingStatus, Seeding: &RingAdminSeeding{Rows: rows, More: true, Cursor: bytes.Repeat([]byte{1}, ringMaxName+2)}})
	if err != nil {
		t.Fatalf("a worst-case page does not encode: %v", err)
	}
	if _, err := DecodeRingAdminReply(raw); err != nil {
		t.Fatalf("a worst-case page does not decode: %v", err)
	}
}

// TestT6TheThreeClassesAreRingClasses: [GUARD the seeding classes are listed with the others,
// numbered after drop_bucket, and distinct, so an authorizer table covers them].
func TestT6TheThreeClassesAreRingClasses(t *testing.T) {
	want := map[RingClass]RingClass{RingClassSeedingStatus: 0x9C, RingClassSeedingRetry: 0x9D, RingClassSeedingForget: 0x9E}
	listed := map[RingClass]bool{}
	for _, class := range RingClasses() {
		listed[class] = true
	}
	for class, number := range want {
		if class != number || !listed[class] {
			t.Errorf("class 0x%02x listed=%v, want number 0x%02x and listed", class, listed[class], number)
		}
	}
}

// t6View is a SeedingView that records what it was asked.
type t6View struct {
	mu           sync.Mutex
	rows         []SeedRow
	retried      []string
	forgot       []string
	forgotAnyway []bool
	retryErr     error
	forgetEr     error
}

func (v *t6View) Status() []SeedRow {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]SeedRow(nil), v.rows...)
}

func (v *t6View) Retry(_ context.Context, database string, slot int) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.retried = append(v.retried, fmt.Sprintf("%s/%d", database, slot))
	return v.retryErr
}

func (v *t6View) Forget(_ context.Context, database string, slot int, anyway bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.forgot = append(v.forgot, fmt.Sprintf("%s/%d", database, slot))
	v.forgotAnyway = append(v.forgotAnyway, anyway)
	return v.forgetEr
}

// t6Gate allows or denies and records each ask.
type t6Gate struct {
	mu    sync.Mutex
	deny  bool
	asked []RingAccess
}

func (g *t6Gate) Authorize(_ context.Context, access RingAccess) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked = append(g.asked, access)
	if g.deny {
		return errU70NoGrant
	}
	return nil
}

func t6Serve(t *testing.T, node *Node, gate RingAuthorizer, request RingAdminRequest) *transport.Frame {
	t.Helper()
	handler, err := NewRingHandler(node, u66AuthNext, gate)
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(request)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// TestT6ANodeWithNoSeedingAnswersEmptyAndRefusesRetryAndForget: [GUARD the default view has
// no rows, and retry and forget are SEED_NOT_HELD with the one conventions text naming the
// database, the slot and status seeding].
func TestT6ANodeWithNoSeedingAnswersEmptyAndRefusesRetryAndForget(t *testing.T) {
	node, _ := u63Fresh(t)
	gate := &t6Gate{}
	reply := u75Reply(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus}))
	if reply.Seeding == nil || reply.Seeding.Rows == nil || len(reply.Seeding.Rows) != 0 || reply.Seeding.More {
		t.Fatalf("an idle node answered %+v, want an empty page", reply.Seeding)
	}
	retry := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(2638)}), cluster.CodeSeedNotHeld)
	for _, want := range []string{`slot 2638 of database "app" is absent`, "restarted", "status seeding"} {
		if !strings.Contains(retry.Message, want) {
			t.Errorf("retry: the refusal %q does not contain %q", retry.Message, want)
		}
	}
	forget := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(2638)}), cluster.CodeSeedNotHeld)
	for _, want := range []string{`slot 2638 of database "app" has no record`, "forget", "status seeding"} {
		if !strings.Contains(forget.Message, want) {
			t.Errorf("forget: the refusal %q does not contain %q", forget.Message, want)
		}
	}
	if strings.Contains(forget.Message, "restart") {
		t.Errorf("forget: the refusal talks of restarting: %q", forget.Message)
	}
	if view := node.SeedingView(); view == nil {
		t.Fatal("a node with no view set has a nil SeedingView")
	}
}

// TestT6TheServerForwardsToTheNodesView: [GUARD status filters by database and slot, orders by
// database then slot, pages with a cursor, and retry and forget reach the view with the
// database and slot sent].
func TestT6TheServerForwardsToTheNodesView(t *testing.T) {
	node, _ := u63Fresh(t)
	view := &t6View{rows: []SeedRow{
		{Database: "zeta", Slot: 3, State: SeedReady}, {Database: "app", Slot: 9, State: SeedSeeding, Source: "node-b", BytesDone: 5, BytesKnown: 10, Entries: 2, Idle: 1500 * time.Millisecond},
		{Database: "app", Slot: 2, State: SeedStalled, Source: "node-c", Refusal: "no chunk applied"}, {Database: "zeta", Slot: 1, State: SeedRetired},
		{Database: "app", Slot: 7, State: SeedPlacing},
	}}
	node.SetSeedingView(view)
	gate := &t6Gate{}
	page := func(req RingAdminRequest) *RingAdminSeeding {
		t.Helper()
		reply := u75Reply(t, t6Serve(t, node, gate, req))
		if reply.Seeding == nil {
			t.Fatalf("%+v: no seeding member", req)
		}
		return reply.Seeding
	}
	keys := func(rows []RingAdminSeedRow) string {
		var parts []string
		for _, r := range rows {
			parts = append(parts, fmt.Sprintf("%s/%d", r.Database, r.Slot))
		}
		return strings.Join(parts, " ")
	}
	all := page(RingAdminRequest{Cmd: AdminSeedingStatus})
	if got := keys(all.Rows); got != "app/2 app/7 app/9 zeta/1 zeta/3" || all.More {
		t.Errorf("all rows: %s more=%v", got, all.More)
	}
	if row := all.Rows[2]; row.State != "seeding" || row.Source != "node-b" || row.BytesDone != 5 || row.BytesKnown != 10 || row.Entries != 2 || row.IdleMS != 1500 {
		t.Errorf("row app/9 is %+v", row)
	}
	if got := keys(page(RingAdminRequest{Cmd: AdminSeedingStatus, Database: "zeta"}).Rows); got != "zeta/1 zeta/3" {
		t.Errorf("zeta rows: %s", got)
	}
	if got := keys(page(RingAdminRequest{Cmd: AdminSeedingStatus, Database: "app", Slot: t6Slot(7)}).Rows); got != "app/7" {
		t.Errorf("app/7 rows: %s", got)
	}
	first := page(RingAdminRequest{Cmd: AdminSeedingStatus, Limit: u75Int32(2)})
	if keys(first.Rows) != "app/2 app/7" || !first.More || len(first.Cursor) == 0 {
		t.Fatalf("first page %s more=%v cursor=%x", keys(first.Rows), first.More, first.Cursor)
	}
	second := page(RingAdminRequest{Cmd: AdminSeedingStatus, Limit: u75Int32(2), Cursor: first.Cursor})
	third := page(RingAdminRequest{Cmd: AdminSeedingStatus, Limit: u75Int32(2), Cursor: second.Cursor})
	if keys(second.Rows) != "app/9 zeta/1" || !second.More || keys(third.Rows) != "zeta/3" || third.More || len(third.Cursor) != 0 {
		t.Errorf("pages %s | %s | %s (more %v %v)", keys(first.Rows), keys(second.Rows), keys(third.Rows), second.More, third.More)
	}
	if got := page(RingAdminRequest{Cmd: AdminSeedingStatus, Database: "nobody"}); got.Rows == nil || len(got.Rows) != 0 {
		t.Errorf("an unknown database gave %+v, want an empty page", got)
	}
	u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus, Cursor: []byte{0xff}}), cluster.CodeInvalidArgument)
	retried := u75Reply(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(2)}))
	if retried.Cmd != AdminSeedingRetry || retried.Seeding == nil || retried.Seeding.Database != "app" || retried.Seeding.Slot != 2 {
		t.Errorf("retry answered %+v", retried.Seeding)
	}
	forgot := u75Reply(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingForget, Database: "zeta", Slot: t6Slot(1)}))
	if forgot.Cmd != AdminSeedingForget || forgot.Seeding == nil || forgot.Seeding.Database != "zeta" || forgot.Seeding.Slot != 1 {
		t.Errorf("forget answered %+v", forgot.Seeding)
	}
	if !reflect.DeepEqual(view.retried, []string{"app/2"}) || !reflect.DeepEqual(view.forgot, []string{"zeta/1"}) {
		t.Errorf("the view saw retry %v and forget %v", view.retried, view.forgot)
	}
	u75Reply(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingForget, Database: "zeta", Slot: t6Slot(3), Anyway: true}))
	if !reflect.DeepEqual(view.forgotAnyway, []bool{false, true}) {
		t.Errorf("the view saw anyway %v, want [false true]: the flag must reach the view (BUG-0795)", view.forgotAnyway)
	}
}

// TestT6ARefusalOfTheViewReachesTheClientWithItsCode: [GUARD a *cluster.Error from the view is
// the answer as it is, and any other error is UNAVAILABLE so a retry is safe].
func TestT6ARefusalOfTheViewReachesTheClientWithItsCode(t *testing.T) {
	node, _ := u63Fresh(t)
	view := &t6View{retryErr: SeedNotHeld("app", 5, "ready"), forgetEr: SeedHeld("app", 5, SeedSeeding)}
	node.SetSeedingView(view)
	gate := &t6Gate{}
	notHeld := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(5)}), cluster.CodeSeedNotHeld)
	if !strings.Contains(notHeld.Message, "is ready") || !strings.Contains(notHeld.Message, "status seeding") {
		t.Errorf("SEED_NOT_HELD text: %q", notHeld.Message)
	}
	held := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingForget, Database: "app", Slot: t6Slot(5)}), cluster.CodeSeedHeld)
	for _, want := range []string{`slot 5 of database "app" is seeding`, "retired record", "status seeding app 5", "seeding retry app 5"} {
		if !strings.Contains(held.Message, want) {
			t.Errorf("SEED_HELD text %q does not contain %q", held.Message, want)
		}
	}
	view.retryErr = errors.New("open _seed/4f2a: permission denied")
	unavailable := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingRetry, Database: "app", Slot: t6Slot(5)}), cluster.CodeUnavailable)
	for _, leak := range []string{"_seed", "4f2a", "permission denied", "Nothing was changed"} {
		if strings.Contains(unavailable.Message, leak) {
			t.Errorf("the UNAVAILABLE text carries %q: %s", leak, unavailable.Message)
		}
	}
	if !strings.Contains(unavailable.Message, "status seeding") {
		t.Errorf("the UNAVAILABLE text names no fix: %s", unavailable.Message)
	}
}

// TestT6AViewRowTheCodecWouldRefuseIsFixedOrFailsClosed: [GUARD an over-long refusal or source
// is cut on a rune boundary, a negative counter is zero, and a row with an unknown state fails
// the call with UNAVAILABLE instead of an unreadable reply].
func TestT6AViewRowTheCodecWouldRefuseIsFixedOrFailsClosed(t *testing.T) {
	node, _ := u63Fresh(t)
	long := strings.Repeat("é", seedingMaxRefusal)
	view := &t6View{rows: []SeedRow{{Database: "app", Slot: 1, State: SeedStalled, Source: strings.Repeat("s", 1000), Refusal: long, BytesDone: -5, Entries: -1, Idle: -time.Second}}}
	node.SetSeedingView(view)
	gate := &t6Gate{}
	reply := u75Reply(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus}))
	row := reply.Seeding.Rows[0]
	if len(row.Refusal) > seedingMaxRefusal || len(row.Refusal) < seedingMaxRefusal-2 || len(row.Source) != ringMaxNodeText || row.BytesDone != 0 || row.Entries != 0 || row.IdleMS != 0 {
		t.Errorf("the row was not made readable: refusal %d bytes, source %d bytes, %+v", len(row.Refusal), len(row.Source), row)
	}
	view.rows = []SeedRow{{Database: "shop", Slot: 3, State: SeedReady}, {Database: "app", Slot: 1234, State: "paused"}}
	bad := u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus}), cluster.CodeUnavailable)
	if !strings.Contains(bad.Message, `database "app"`) || !strings.Contains(bad.Message, "slot 1234") || strings.Contains(bad.Message, "paused") {
		t.Errorf("the refusal does not name the bad row alone: %s", bad.Message)
	}
	view.rows = []SeedRow{{Database: "app", Slot: slotCount, State: SeedReady}}
	u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus}), cluster.CodeUnavailable)
	view.rows = []SeedRow{{Database: "app", Slot: 2, State: ""}}
	u66Refusal(t, t6Serve(t, node, gate, RingAdminRequest{Cmd: AdminSeedingStatus}), cluster.CodeUnavailable)
}

// TestT6EveryCommandAsksTheClassBeforeTheViewIsTouched: [GUARD one class-only ask per command
// with the class of that command, and a refusal is the one PERMISSION_DENIED text that names
// no database, slot or user, with the view untouched].
func TestT6EveryCommandAsksTheClassBeforeTheViewIsTouched(t *testing.T) {
	node, _ := u63Fresh(t)
	view := &t6View{rows: []SeedRow{{Database: "app", Slot: 1, State: SeedReady}}}
	node.SetSeedingView(view)
	cases := []struct {
		req   RingAdminRequest
		class RingClass
	}{
		{RingAdminRequest{Cmd: AdminSeedingStatus, Database: "secretdb"}, RingClassSeedingStatus},
		{RingAdminRequest{Cmd: AdminSeedingRetry, Database: "secretdb", Slot: t6Slot(1234)}, RingClassSeedingRetry},
		{RingAdminRequest{Cmd: AdminSeedingForget, Database: "secretdb", Slot: t6Slot(1234)}, RingClassSeedingForget},
	}
	allow := &t6Gate{}
	for _, tc := range cases {
		allow.asked = nil
		t6Serve(t, node, allow, tc.req)
		if !reflect.DeepEqual(allow.asked, []RingAccess{{Class: tc.class}}) {
			t.Errorf("%s asked %+v, want one class-only ask of 0x%02x", tc.req.Cmd, allow.asked, tc.class)
		}
	}
	view.retried, view.forgot = nil, nil
	deny := &t6Gate{deny: true}
	for _, tc := range cases {
		refusal := u66Refusal(t, t6Serve(t, node, deny, tc.req), cluster.CodePermissionDenied)
		for _, leak := range []string{"secretdb", "1234", "app"} {
			if strings.Contains(refusal.Message, leak) {
				t.Errorf("%s: the denial names %q: %s", tc.req.Cmd, leak, refusal.Message)
			}
		}
		if !strings.Contains(refusal.Message, "cluster-manager") {
			t.Errorf("%s: the denial does not name the role that allows it: %s", tc.req.Cmd, refusal.Message)
		}
	}
	if len(view.retried) != 0 || len(view.forgot) != 0 {
		t.Errorf("a denied session reached the view: retry %v forget %v", view.retried, view.forgot)
	}
}

// TestT6StatusReadsNoBucket: [GUARD status costs the rows of the view and nothing per bucket:
// with 10000 buckets declared it starts no Badger get or iterator].
func TestT6StatusReadsNoBucket(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	for i := range 10_000 {
		if _, err := node.CreateBucket(ctx, u30iDB, fmt.Sprintf("S%05d", i)); err != nil {
			t.Fatal(err)
		}
	}
	node.SetSeedingView(&t6View{rows: []SeedRow{{Database: u30iDB, Slot: 3, State: SeedSeeding}}})
	handler, err := NewRingHandler(node, u66AuthNext, u70AllowAll{})
	if err != nil {
		t.Fatal(err)
	}
	body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminSeedingStatus})
	if err != nil {
		t.Fatal(err)
	}
	before := u83Reads()
	frame, err := handler(ctx, transport.NewFrameWithID(transport.OpRingAdmin, u66ID(), body))
	if err != nil {
		t.Fatal(err)
	}
	if reads := u83Reads() - before; reads != 0 {
		t.Errorf("seeding_status started %d Badger reads on a node with 10000 buckets, want 0", reads)
	}
	if reply := u75Reply(t, frame); len(reply.Seeding.Rows) != 1 {
		t.Errorf("status answered %+v", reply.Seeding)
	}
}

// TestT6TheRefusalConstructorsRenderTheOneText: [GUARD SeedNotHeld and SeedHeld are
// *cluster.Error with the registered codes, and their text is the conventions text with the
// placeholders filled].
func TestT6TheRefusalConstructorsRenderTheOneText(t *testing.T) {
	var coded *cluster.Error
	if !errors.As(SeedNotHeld("app", 12, "absent"), &coded) || coded.Code != cluster.CodeSeedNotHeld ||
		!strings.Contains(coded.Message, `slot 12 of database "app"`) || strings.Contains(coded.Message, "<") {
		t.Errorf("SeedNotHeld gave %+v", coded)
	}
	if !errors.As(SeedHeld("app", 12, SeedReady), &coded) || coded.Code != cluster.CodeSeedHeld ||
		!strings.Contains(coded.Message, "is ready on this node") || strings.Contains(coded.Message, "<") {
		t.Errorf("SeedHeld gave %+v", coded)
	}
}

// TestSeedOnlyCopyIsSeedHeldAndNamesTheAnywayForm: the refusal constructor renders the one text
// with the slot and the database, and no placeholder is left. [BREAK: leave <db> unfilled]
func TestSeedOnlyCopyIsSeedHeldAndNamesTheAnywayForm(t *testing.T) {
	var coded *cluster.Error
	if !errors.As(SeedOnlyCopy("app", 12), &coded) || coded.Code != cluster.CodeSeedHeld ||
		!strings.Contains(coded.Message, "'seeding forget app 12 anyway'") || strings.Contains(coded.Message, "<slot>") || strings.Contains(coded.Message, "<db>") {
		t.Errorf("SeedOnlyCopy gave %+v", coded)
	}
}

// TestT6TheRowsArrayIsCountedBeforeItIsUnpacked: [GUARD a reply of many tiny rows is refused on the
// count of its array, so the rows are never built: decoding 20000 empty row documents allocates a
// bounded amount].
func TestT6TheRowsArrayIsCountedBeforeItIsUnpacked(t *testing.T) {
	rows := make(bson.A, 20000)
	for i := range rows {
		rows[i] = bson.D{}
	}
	raw, err := bson.Marshal(bson.D{{Key: "cmd", Value: AdminSeedingStatus}, {Key: "rows", Value: rows}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingAdminReply(raw); err == nil || !strings.Contains(err.Error(), "rows") || !strings.Contains(err.Error(), fmt.Sprint(seedingMaxPage)) {
		t.Fatalf("20000 rows were accepted or refused without the bound: %v", err)
	}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdminReply(raw)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<16 {
		t.Fatalf("refusing 20000 rows allocated %d bytes", perOp)
	}
}

// TestAdminRefusesAnywayOnOtherCommands: anyway belongs to seeding_forget alone; any other
// command that carries it is refused naming the member, instead of ignoring it. [GUARD
// adminStray not updated: the member is dropped silently]
func TestAdminRefusesAnywayOnOtherCommands(t *testing.T) {
	slot := t6Slot(3)
	others := []RingAdminRequest{
		{Cmd: AdminShowBuckets, Database: "app"},
		{Cmd: AdminDescribeDatabase, Database: "app"},
		{Cmd: AdminShowDatabases},
		{Cmd: AdminDropBucket, Database: "app", Bucket: "User"},
		{Cmd: AdminSeedingRetry, Database: "app", Slot: slot},
		{Cmd: AdminSeedingStatus, Database: "app"},
	}
	for _, req := range others {
		req.Anyway = true
		_, err := EncodeRingAdmin(req)
		if err == nil || !strings.Contains(err.Error(), "anyway") {
			t.Errorf("%s with anyway: err = %v, want a refusal that names anyway", req.Cmd, err)
		}
	}
	if _, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminSeedingForget, Database: "app", Slot: slot, Anyway: true}); err != nil {
		t.Errorf("seeding_forget with anyway: %v", err)
	}
}
