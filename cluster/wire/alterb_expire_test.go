package wire

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func (r *u40Rig) insertExpiring(id string, at time.Time) error {
	doc := append(u30iUser(id, "Ada", 36), bson.E{Key: "ExpiresAt", Value: primitive.NewDateTimeFromTime(at)})
	_, err := r.node.Insert(context.Background(), u30iDB, "User", doc)
	return err
}

func TestSetExpireAtIsSeenOnlyOnceTheBumpIsPublished(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("ExpiresAt", "datetime"))
	crash := &crashingBumper{}
	config := r.node.alterer.cfg
	config.Sender, config.Bumper = r.sender, crash
	interrupted := NewAlterBucket(config)
	set := AlterChange{SetExpire: "ExpiresAt"}
	if _, err := interrupted.Run(context.Background(), r.dbID, "User", set); !errors.Is(err, errU40Crash) {
		t.Fatalf("the interrupted alter returned %v, want the crash", err)
	}
	decl := r.declaration()
	if decl.Pending == nil || decl.Pending.Expire == nil || decl.Pending.Expire.BucketField == 0 {
		t.Fatalf("the pending table does not carry the expiry: %+v", decl.Pending)
	}
	if decl.Expire.BucketField != 0 {
		t.Fatal("the expiry was written into field 11 before the bump: nodes would see it at cache refresh, not at the bump")
	}
	future := time.Now().Add(time.Hour)
	if err := r.insertExpiring("before", future); err != nil {
		t.Fatalf("insert before the bump: %v", err)
	}
	if got := len(expiryQueue(t, r.node.kv)); got != 0 {
		t.Fatalf("%d expiry entries before the bump, want 0: the write saw an expiry that is not published", got)
	}
	r.run(set) // the same alter finishes the interrupted one and commits the bump
	if err := r.insertExpiring("after", future); err != nil {
		t.Fatalf("insert after the bump: %v", err)
	}
	if got := len(expiryQueue(t, r.node.kv)); got != 1 {
		t.Fatalf("%d expiry entries after the bump, want 1", got)
	}
	if got := r.declaration(); machine.EffectiveExpire(got, r.node.topology.Published(r.dbID)).BucketField == 0 {
		t.Fatalf("the published declaration has no expiry: %+v", got)
	}
}

func TestSetExpireRefusalsNameTheFix(t *testing.T) {
	r := newU40Rig(t)
	for name, tc := range map[string]struct {
		change AlterChange
		want   []string
	}{
		"a string field": {AlterChange{SetExpire: "Name"}, []string{"EXPIRE_FIELD_TYPE", "declared string", "add ExpiresAt datetime", "set expire at"}},
		"no such field":  {AlterChange{SetExpire: "Nope"}, []string{"EXPIRE_FIELD_TYPE", "does not declare", "add Nope datetime"}},
		"twice":          {AlterChange{SetExpire: "Age", DropExpire: true}, []string{"EXPIRE_TWICE"}},
		"not declared":   {AlterChange{DropExpire: true}, []string{"EXPIRE_NOT_DECLARED", "describe bucket User in app"}},
	} {
		_, err := r.alter.Run(context.Background(), r.dbID, "User", tc.change)
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: %v, want INVALID_ARGUMENT", name, err)
			continue
		}
		for _, want := range tc.want {
			if !strings.Contains(refusal.Message, want) {
				t.Errorf("%s: %q lacks %q", name, refusal.Message, want)
			}
		}
	}
}

// TestDroppingAFieldThatExpiresRemovesItsExpiryInTheSameAlter: a field that is part of the expiry (the
// bucket's datetime field, or a queue that expires its elements) can be dropped with a plain
// `drop <Field>`; the alter removes that part of the expiry in the same pending table, so there is
// nothing to clear first and no refusal that names a fix which does not work.
func TestDroppingAFieldThatExpiresRemovesItsExpiryInTheSameAlter(t *testing.T) {
	base := machine.FieldTable{NextFieldID: 11, Entries: []machine.FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"}, {ID: 9, Type: "fifoStore", Name: "Log"}, {ID: 10, Type: "datetime", Name: "At"}}}
	current := machine.ExpireTable{BucketField: 10, Stores: []machine.ExpireStore{{FieldID: 9, Path: "ts"}}}
	for name, tc := range map[string]struct {
		drop string
		want machine.ExpireTable
	}{
		"the queue":             {"Log", machine.ExpireTable{BucketField: 10}},
		"the bucket's datetime": {"At", machine.ExpireTable{Stores: []machine.ExpireStore{{FieldID: 9, Path: "ts"}}}},
	} {
		in := planInput{db: "app", bucket: "Device", base: base, change: AlterChange{Drop: []string{tc.drop}}}
		next, changed, err := planAlter(in)
		if err != nil || !changed {
			t.Fatalf("%s: planAlter = %v, %v", name, changed, err)
		}
		got, expireChanged, err := planExpire(in, next, current)
		if err != nil || !expireChanged || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("drop %s: expiry = %+v changed %v err %v, want %+v", tc.drop, got, expireChanged, err, tc.want)
		}
		// The same alter planned again over the table after it (the confirm after the bump) changes nothing.
		after, _, _ := planAlter(planInput{db: "app", bucket: "Device", base: next, change: in.change})
		if again, changedAgain, err := planExpire(planInput{db: "app", bucket: "Device", base: next, change: in.change}, after, got); err != nil || changedAgain || !reflect.DeepEqual(again, got) {
			t.Fatalf("drop %s planned twice: %+v changed %v err %v, want it idempotent", tc.drop, again, changedAgain, err)
		}
	}
}

// TestDroppingTheExpiringQueueOfARealBucketLeavesNoExpiry runs it through a node: create with an expiring
// queue, drop the queue, and the published declaration holds no store expiry.
func TestDroppingTheExpiringQueueOfARealBucketLeavesNoExpiry(t *testing.T) {
	r := newU40Rig(t)
	if _, err := r.node.CreateBucketExpiring(context.Background(), u30iDB, "Device", ExpireSpec{StorePaths: map[string]string{"Log": "ts"}},
		Field{Name: "Name", Type: "string"}, Field{Name: "Log", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.node.AlterBucket(context.Background(), u30iDB, "Device", AlterChange{Drop: []string{"Log"}}); err != nil {
		t.Fatalf("dropping an expiring queue: %v", err)
	}
	decl, _, found, err := r.node.fetcher.Read(context.Background(), r.dbID, "Device")
	if err != nil || !found {
		t.Fatal(found, err)
	}
	if got := machine.EffectiveExpire(decl, r.node.topology.Published(r.dbID)); len(got.Stores) != 0 {
		t.Fatalf("the published declaration still expires %+v", got.Stores)
	}
}

// TestPlanAlterKeepsEveryMemberOfTheBaseTable: the new table is the base cloned, not rebuilt
// member by member, so a member added to FieldTable later (field defaults) is not lost by an alter.
func TestPlanAlterKeepsEveryMemberOfTheBaseTable(t *testing.T) {
	base := machine.FieldTable{}
	value := reflect.ValueOf(&base).Elem()
	for i := range value.NumField() {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.Uint16, reflect.Uint64, reflect.Int64:
			field.SetUint(7)
		case reflect.Slice:
			field.Set(reflect.MakeSlice(field.Type(), 1, 1))
		case reflect.Map:
			field.Set(reflect.MakeMapWithSize(field.Type(), 0))
		case reflect.String:
			field.SetString("x")
		case reflect.Bool:
			field.SetBool(true)
		default:
			t.Fatalf("FieldTable member %s has kind %s: teach this test to fill it", value.Type().Field(i).Name, field.Kind())
		}
	}
	base.Entries = []machine.FieldEntry{{ID: 1, Type: "any", Name: "_id_"}}
	next, changed, err := planAlter(planInput{db: "app", bucket: "B", base: base, change: AlterChange{Add: []FieldEntry{{Name: "A", Type: "string"}}}, epoch: 3})
	if err != nil || !changed {
		t.Fatalf("planAlter = %v, %v", changed, err)
	}
	nextValue := reflect.ValueOf(next)
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		if name == "Entries" || name == "NextFieldID" {
			continue // the alter changes these on purpose
		}
		if !reflect.DeepEqual(nextValue.Field(i).Interface(), value.Field(i).Interface()) {
			t.Errorf("planAlter dropped FieldTable.%s: it rebuilds the table member by member", name)
		}
	}
}

// TestSetExpireAtIsRefusedOnABucketThatAlreadyHoldsDocuments: nothing builds expiry entries for the
// documents already in a bucket, so reads would hide them once their time passed and nothing would
// ever remove them. Until a backfill exists the alter is refused with the way to get expiry.
func TestSetExpireAtIsRefusedOnABucketThatAlreadyHoldsDocuments(t *testing.T) {
	r := newU40Rig(t)
	r.run(u40Add("ExpiresAt", "datetime"))
	if err := r.insertUser("existing"); err != nil {
		t.Fatal(err)
	}
	_, err := r.node.AlterBucket(context.Background(), u30iDB, "User", AlterChange{SetExpire: "ExpiresAt"})
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("set expire at on a populated bucket: %v, want INVALID_ARGUMENT", err)
	}
	for _, want := range []string{"already holds documents", "create bucket", "expire at", "empty it first"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("the refusal %q lacks %q", refusal.Message, want)
		}
	}
	if got := r.declaration(); got.Pending != nil && got.Pending.Expire != nil {
		t.Fatalf("the refused alter left a pending expiry: %+v", got.Pending)
	}
	// An expiry the bucket already has is not a change, so repeating it is not refused.
	empty := newU40Rig(t)
	empty.run(u40Add("ExpiresAt", "datetime"))
	empty.run(AlterChange{SetExpire: "ExpiresAt"})
	if err := empty.insertUser("later"); err != nil {
		t.Fatal(err)
	}
	if _, err := empty.node.AlterBucket(context.Background(), u30iDB, "User", AlterChange{SetExpire: "ExpiresAt"}); err != nil {
		t.Fatalf("repeating the expiry the bucket already has: %v", err)
	}
}

// TestAnAbandonedPendingExpiryIsNotCarriedIntoTheNextAlter: an alter that wrote its pending table and failed
// before its bump leaves a table and an expiry that name field ids of THAT table. The next alter builds its
// table from the published one, so an id the abandoned table gave to a datetime may mean another field there.
// The unpublished pending is replaced whole; its expiry never rides into a pending table it was not planned for.
func TestAnAbandonedPendingExpiryIsNotCarriedIntoTheNextAlter(t *testing.T) {
	for name, next := range map[string]AlterChange{
		"a string takes the id":                                u40Add("Y", "string"),
		"a datetime takes the id (an expiry nobody asked for)": u40Add("Until", "datetime"),
	} {
		t.Run(name, func(t *testing.T) {
			r := newU40Rig(t)
			crash := &crashingBumper{}
			config := r.node.alterer.cfg
			config.Sender, config.Bumper = r.sender, crash
			abandoned := NewAlterBucket(config)
			if _, err := abandoned.Run(context.Background(), r.dbID, "User", AlterChange{Add: []FieldEntry{{Name: "At", Type: "datetime"}}, SetExpire: "At"}); !errors.Is(err, errU40Crash) {
				t.Fatalf("the abandoned alter returned %v, want the crash", err)
			}
			if got := r.declaration(); got.Pending == nil || got.Pending.Expire == nil {
				t.Fatalf("the abandoned alter left no pending expiry: %+v", got.Pending)
			}
			r.run(next)
			decl := r.declaration()
			if got := machine.EffectiveExpire(decl, r.node.topology.Published(r.dbID)); got.BucketField != 0 || len(got.Stores) != 0 {
				t.Fatalf("the later alter published the expiry %+v it did not ask for", got)
			}
			if decl.Pending != nil && decl.Pending.Expire != nil {
				t.Fatalf("the later alter's pending table carries the abandoned expiry %+v", decl.Pending.Expire)
			}
		})
	}
}
