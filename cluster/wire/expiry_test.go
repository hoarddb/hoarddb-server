package wire

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestExpireItemsOfPicksTheTouchedFields (expiry build E2a): the owner stamps an expire item
// only for the field the op touches, never for the declaration's order.
func TestExpireItemsOfPicksTheTouchedFields(t *testing.T) {
	table := machine.ExpireTable{
		BucketField: 4,
		Stores:      []machine.ExpireStore{{FieldID: 7, Path: "ts"}},
	}
	op := func(class byte, fieldIDs ...uint16) cluster.Op {
		built := cluster.Op{Class: class}
		for _, fieldID := range fieldIDs {
			built.Fields = append(built.Fields, cluster.FieldOp{FieldID: fieldID})
		}
		return built
	}
	cases := []struct {
		name  string
		table machine.ExpireTable
		op    cluster.Op
		want  []cluster.ExpireItem
	}{
		{"insert carrying the bucket field", table, op(cluster.OpInsert, 4), []cluster.ExpireItem{{FieldID: 4}}},
		{"insert not carrying the bucket field", table, op(cluster.OpInsert, 5), []cluster.ExpireItem{{FieldID: 4}}},
		{"upsert carrying the bucket field", table, op(cluster.OpUpsert, 4, 5), []cluster.ExpireItem{{FieldID: 4}}},
		{"update touching another field", table, op(cluster.OpUpdate, 5), nil},
		{"update touching the bucket field", table, op(cluster.OpUpdate, 4), []cluster.ExpireItem{{FieldID: 4}}},
		{"delete", table, op(cluster.OpDelete), []cluster.ExpireItem{{FieldID: 4}, {FieldID: 7, Path: []byte("ts")}}},
		{"delete of a bucket whose only expiry is a store", machine.ExpireTable{Stores: table.Stores}, op(cluster.OpDelete), []cluster.ExpireItem{{FieldID: 7, Path: []byte("ts")}}},
		{"push to an expiring store", table, op(cluster.OpStoreVerb, 7), []cluster.ExpireItem{{FieldID: 7, Path: []byte("ts")}}},
		{"push to another store", table, op(cluster.OpStoreVerb, 8), nil},
		{"claim on an expiring store", table, op(cluster.OpClaim, 7), []cluster.ExpireItem{{FieldID: 7, Path: []byte("ts")}}},
		{"ack on an expiring store", table, op(cluster.OpAck, 7), []cluster.ExpireItem{{FieldID: 7, Path: []byte("ts")}}},
		{"release on an expiring store", table, op(cluster.OpRelease, 7), nil},
		{"extend on an expiring store", table, op(cluster.OpExtend, 7), nil},
		{"zero table insert", machine.ExpireTable{}, op(cluster.OpInsert, 4), nil},
		{"zero table update", machine.ExpireTable{}, op(cluster.OpUpdate, 4), nil},
		{"zero table delete", machine.ExpireTable{}, op(cluster.OpDelete), nil},
		{"zero table push", machine.ExpireTable{}, op(cluster.OpStoreVerb, 7), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := expireItemsOf(tc.table, tc.op)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("expireItemsOf = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestExpiryReadsWrittenFieldsNeverOpFields fails if the expiry code reads op.Fields directly:
// field defaults will fill a datetime into the written fields only, and a read of op.Fields
// would silently never expire a defaulted document.
func TestExpiryReadsWrittenFieldsNeverOpFields(t *testing.T) {
	for _, file := range []string{"expiry.go", "writepath_apply.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(source), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(line, "Op.Fields") || strings.Contains(line, "op.Fields") {
				t.Errorf("%s:%d reads Fields directly (%q); use WrittenFields()", file, number+1, trimmed)
			}
		}
	}
}
