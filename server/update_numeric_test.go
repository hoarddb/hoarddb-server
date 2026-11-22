package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/transport"
)

// $inc and $mul read a field through a helper that answered
// "not a number" and the caller threw the answer away, so $inc on "Ada" wrote
// the delta over the string and $inc on 2.5 wrote 1 over the double.

func storedDocument(t *testing.T, s *Server, bucket, key string) map[string]interface{} {
	t.Helper()
	found, err := s.Find(FindRequest{Database: "app", Bucket: bucket, Key: key})
	if err != nil || !found.Found {
		t.Fatalf("Find(%q) = %+v, %v", key, found, err)
	}
	var stored map[string]interface{}
	if err := bson.Unmarshal(found.Data, &stored); err != nil {
		t.Fatalf("decoding %q: %v", key, err)
	}
	return stored
}

func numericServer(t *testing.T, key string, fields map[string]interface{}) *Server {
	t.Helper()
	s := newTestServer(t)
	createTestBucket(t, s, "app", "P", BucketTypeHash)
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "P", Key: key, Data: fields}); err != nil || !resp.Success {
		t.Fatalf("Insert: %+v, %v", resp, err)
	}
	return s
}

// equalDocuments compares decoded documents; their encodings cannot be
// compared byte for byte because a Go map marshals its keys in random order.
func equalDocuments(left, right map[string]interface{}) bool {
	return reflect.DeepEqual(left, right)
}

func TestUpdateIncRefusesNonIntegerField(t *testing.T) {
	cases := []struct {
		name     string
		stored   interface{}
		wantType string
	}{
		{"string", "Ada", "a string"},
		{"bool", true, "a boolean"},
		{"array", bson.A{int32(1)}, "an array"},
		{"sub-document", bson.M{"a": int32(1)}, "a document"},
		{"date", primitive.NewDateTimeFromTime(time.Unix(1700000000, 0)), "a datetime"},
		{"null", nil, "null"},
		{"decimal128", primitive.NewDecimal128(0, 5), "a 128-bit decimal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := numericServer(t, "a", map[string]interface{}{"Name": tc.stored})
			before := storedDocument(t, s, "P", "a")

			resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Name": 1}})
			if err == nil {
				t.Fatalf("$inc on %s answered %+v, want a refusal", tc.name, resp)
			}
			for _, want := range []string{`"Name"`, `"a"`, "app.P", tc.wantType, "$set"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			if after := storedDocument(t, s, "P", "a"); !equalDocuments(before, after) {
				t.Errorf("a refused $inc changed the document: %v -> %v", before, after)
			}
		})
	}
}

func TestUpdateIncAddsToAFractionalDouble(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Score": 2.5})
	resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Score": 1}})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := storedDocument(t, s, "P", "a")["Score"]; got != 3.5 {
		t.Errorf("stored Score = %v (%T), want the double 3.5", got, got)
	}
	if got := resp.Doc["Score"]; got != 3.5 {
		t.Errorf("answered Score = %v (%T), want the double 3.5", got, got)
	}
}

func TestUpdateIncResultTypes(t *testing.T) {
	cases := []struct {
		name   string
		stored interface{}
		delta  int64
		want   interface{}
	}{
		{"int32 stays an integer", int32(5), 3, int64(8)},
		{"int64 stays an integer", int64(5), -7, int64(-2)},
		{"a whole double is read as an integer, as before", float64(41), 1, int64(42)},
		{"a double beyond int64 stays a double", 1e30, 1, 1e30},
		{"a fractional double stays a double", 0.25, -1, -0.75},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := numericServer(t, "a", map[string]interface{}{"N": tc.stored})
			if _, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"N": tc.delta}}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if got := storedDocument(t, s, "P", "a")["N"]; got != tc.want {
				t.Errorf("N = %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestUpdateIncCreatesAnAbsentField(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Name": "Ada"})
	if _, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Visits": 4}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := storedDocument(t, s, "P", "a")["Visits"]; got != int64(4) {
		t.Errorf("Visits = %v (%T), want int64 4", got, got)
	}
}

func TestUpdateIncRefusesAnInt64Overflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stored interface{}
		delta  int64
	}{
		{"above", int64(math.MaxInt64), 1},
		{"below", int64(math.MinInt64), -1},
		{"a delta that crosses the top", int64(math.MaxInt64 - 1), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := numericServer(t, "a", map[string]interface{}{"N": tc.stored})
			_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"N": tc.delta}})
			if err == nil {
				t.Fatal("an overflowing $inc was acknowledged")
			}
			for _, want := range []string{`"N"`, "overflow", "$set"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			for _, echoed := range []string{fmt.Sprint(tc.stored), fmt.Sprint(tc.delta)} {
				if strings.Contains(err.Error(), echoed) {
					t.Errorf("refusal %q echoes the value %s: a refusal names fields and types, never values", err, echoed)
				}
			}
			if got := storedDocument(t, s, "P", "a")["N"]; got != tc.stored {
				t.Errorf("a refused $inc left N = %v, want %v", got, tc.stored)
			}
		})
	}
}

func TestUpdateWithOneRefusedOperatorWritesNothing(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Name": "Ada", "Visits": int32(1), "Tag": "x"})
	before := storedDocument(t, s, "P", "a")
	_, err := s.Update(UpdateRequest{
		Database: "app", Bucket: "P", Key: "a",
		Set:   map[string]interface{}{"Tag": "changed"},
		Unset: []string{"Visits"},
		Inc:   map[string]int64{"Name": 1, "Fresh": 1},
	})
	if err == nil {
		t.Fatal("an update with a refused operator was acknowledged")
	}
	if after := storedDocument(t, s, "P", "a"); !equalDocuments(before, after) {
		t.Errorf("a refused update wrote part of itself: %v -> %v", before, after)
	}
}

func TestMultiUpdateWithOneRefusedDocumentWritesNothing(t *testing.T) {
	s := numericServer(t, "good", map[string]interface{}{"Kind": "k", "N": int32(1)})
	if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "P", Key: "bad", Data: map[string]interface{}{"Kind": "k", "N": "text"}}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Multi: true,
		Filter: map[string]interface{}{"Kind": "k"}, Inc: map[string]int64{"N": 1}})
	if err == nil {
		t.Fatal("a multi-update over a string field was acknowledged")
	}
	if !strings.Contains(err.Error(), `"bad"`) {
		t.Errorf("refusal %q does not name the document", err)
	}
	if got := storedDocument(t, s, "P", "good")["N"]; got != int32(1) {
		t.Errorf("the refused multi-update changed another document: N = %v (%T)", got, got)
	}
}

func TestUpdateMulRefusesNonNumericField(t *testing.T) {
	for name, stored := range map[string]interface{}{
		"string": "Ada", "bool": true, "array": bson.A{int32(1)}, "decimal128": primitive.NewDecimal128(0, 5),
	} {
		t.Run(name, func(t *testing.T) {
			s := numericServer(t, "a", map[string]interface{}{"Price": stored})
			before := storedDocument(t, s, "P", "a")
			_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Mul: map[string]float64{"Price": 2}})
			if err == nil {
				t.Fatal("$mul on a non-number was acknowledged")
			}
			for _, want := range []string{`"Price"`, `"a"`, "app.P", "$set"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			if after := storedDocument(t, s, "P", "a"); !equalDocuments(before, after) {
				t.Errorf("a refused $mul changed the document: %v -> %v", before, after)
			}
		})
	}
}

func TestUpdateMulResultTypes(t *testing.T) {
	cases := []struct {
		name   string
		stored interface{}
		factor float64
		want   interface{}
	}{
		{"int32 by a whole factor stays an integer", int32(6), 7, int64(42)},
		{"int64 by a whole factor stays an integer", int64(-3), 4, int64(-12)},
		{"an integer by a fractional factor is a double", int64(5), 0.5, 2.5},
		{"a double stays a double", 100.0, 1.5, 150.0},
		{"a whole double by a whole factor stays a double", 4.0, 2, 8.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := numericServer(t, "a", map[string]interface{}{"N": tc.stored})
			if _, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Mul: map[string]float64{"N": tc.factor}}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if got := storedDocument(t, s, "P", "a")["N"]; got != tc.want {
				t.Errorf("N = %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestUpdateMulRefusesAnInt64Overflow(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"N": int64(math.MaxInt64)})
	_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Mul: map[string]float64{"N": 2}})
	if err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("an overflowing $mul answered %v, want an overflow refusal", err)
	}
	for _, echoed := range []string{fmt.Sprint(int64(math.MaxInt64)), "2"} {
		if strings.Contains(err.Error(), echoed) {
			t.Errorf("refusal %q echoes the value %s: a refusal names fields and types, never values", err, echoed)
		}
	}
	if got := storedDocument(t, s, "P", "a")["N"]; got != int64(math.MaxInt64) {
		t.Errorf("a refused $mul left N = %v", got)
	}
}

func TestUpdateMulLeavesAnAbsentFieldAbsent(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Name": "Ada"})
	if _, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Mul: map[string]float64{"Price": 2}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, present := storedDocument(t, s, "P", "a")["Price"]; present {
		t.Error("$mul created a field that was absent")
	}
}

func TestUpdateWithAMixedRefusedMulWritesNothing(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Name": "Ada", "Price": 10.0})
	before := storedDocument(t, s, "P", "a")
	_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a",
		Mul: map[string]float64{"Price": 3, "Name": 2}})
	if err == nil {
		t.Fatal("an update with a refused $mul was acknowledged")
	}
	if after := storedDocument(t, s, "P", "a"); !equalDocuments(before, after) {
		t.Errorf("a refused update wrote part of itself: %v -> %v", before, after)
	}
}

func TestARefusedUpdateReachesTheClientAsInvalidArgument(t *testing.T) {
	s := numericServer(t, "a", map[string]interface{}{"Name": "Ada"})
	_, err := s.Update(UpdateRequest{Database: "app", Bucket: "P", Key: "a", Inc: map[string]int64{"Name": 1}})
	if !errors.Is(err, ErrUpdateOperand) {
		t.Fatalf("refusal %v does not wrap ErrUpdateOperand", err)
	}
	if got := codeFor(err); got != transport.ErrInvalidArgument {
		t.Errorf("code = %v, want INVALID_ARGUMENT", got)
	}
}

// A log holding a refused update replays to the same document the live apply
// left: the refusal is a function of the stored document and the operators, so
// replay reaches the owner's answer, and the valid update after it still lands.
func TestReplayRefusesTheSameUpdateTheLiveApplyRefused(t *testing.T) {
	var logged bytes.Buffer
	s := newReplayTestServer(t, t.TempDir(),
		slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer closeReplayTestServer(t, s)
	w := s.wal
	appendEntry := func(op transport.Opcode, bucket, key string, request any) {
		t.Helper()
		if _, err := w.Append(bucket, byte(op), []byte(key), mustMarshalBSON(t, request)); err != nil {
			t.Fatalf("logging %s: %v", op, err)
		}
	}
	appendEntry(transport.OpCreateDB, "", "", CreateDatabaseRequest{Name: "app"})
	appendEntry(transport.OpCreateBucket, "P", "", CreateBucketRequest{Database: "app", Name: "P", Type: BucketTypeHash})
	appendEntry(transport.OpPut, "P", "a", InsertRequest{Database: "app", Bucket: "P", Key: "a",
		Data: map[string]interface{}{"Name": "Ada", "Visits": int32(1)}})
	appendEntry(transport.OpUpdate, "P", "a", UpdateRequest{Database: "app", Bucket: "P", Key: "a",
		Set: map[string]interface{}{"Tag": "lost"}, Inc: map[string]int64{"Name": 1}})
	appendEntry(transport.OpUpdate, "P", "a", UpdateRequest{Database: "app", Bucket: "P", Key: "a",
		Inc: map[string]int64{"Visits": 1}})

	if err := s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log failed: %v", err)
	}
	if summary := logged.String(); !strings.Contains(summary, "skipped=1") || !strings.Contains(summary, "applied=4") {
		t.Errorf("replay should apply the four valid entries and refuse the one update, got: %s", summary)
	}
	stored := storedDocument(t, s, "P", "a")
	if stored["Name"] != "Ada" {
		t.Errorf("replay applied a refused update: Name = %v", stored["Name"])
	}
	if _, present := stored["Tag"]; present {
		t.Error("replay wrote part of a refused update")
	}
	if stored["Visits"] != int64(2) {
		t.Errorf("replay lost the valid update after the refused one: Visits = %v (%T)", stored["Visits"], stored["Visits"])
	}
}

// Every replica re-runs the operators on its own copy, so a refused update has
// to be refused by all of them and leave all of them with one value.
func TestReplicasHoldOneValueAfterARefusedAndAValidUpdate(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "a",
		Data: map[string]interface{}{"Name": "Ada", "Visits": int32(1)}, WriteConcern: "all"})

	refused := tc.frame(0, transport.OpUpdate, updateWithConcernAndSet{
		Database: testDatabase, Bucket: testBucket, Key: "a", WriteConcern: "all",
		Set: map[string]interface{}{"Tag": "lost"}, Inc: map[string]int64{"Name": 1},
	})
	if refused.Opcode != transport.OpError {
		t.Fatalf("the cluster acknowledged a $inc on a string: %s", refused.Opcode)
	}
	tc.ok(0, transport.OpUpdate, updateWithConcernAndSet{
		Database: testDatabase, Bucket: testBucket, Key: "a", WriteConcern: "all",
		Inc: map[string]int64{"Visits": 1},
	})

	pollUntil(t, "every replica holds Name=Ada, Visits=2 and no Tag", func() bool {
		for index := range tc.nodes {
			found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: "a"})
			if err != nil || !found.Found {
				return false
			}
			var stored map[string]interface{}
			if bson.Unmarshal(found.Data, &stored) != nil {
				return false
			}
			if _, tagged := stored["Tag"]; tagged || stored["Name"] != "Ada" || stored["Visits"] != int64(2) {
				return false
			}
		}
		return true
	})
}

type updateWithConcernAndSet struct {
	Database     string                 `bson:"database"`
	Bucket       string                 `bson:"bucket"`
	Key          string                 `bson:"key"`
	Set          map[string]interface{} `bson:"set,omitempty"`
	Inc          map[string]int64       `bson:"inc,omitempty"`
	WriteConcern string                 `bson:"writeconcern"`
}
