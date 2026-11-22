package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

func u68Hash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func u68Doc() bson.D {
	return bson.D{
		{Key: "_id_", Value: "u1"},
		{Key: "Name", Value: "Ada"},
		{Key: "Small", Value: int32(7)},
		{Key: "Big", Value: int64(1) << 40},
		{Key: "Plain", Value: 12},
		{Key: "Score", Value: 3.25},
		{Key: "Flag", Value: true},
		{Key: "Nothing", Value: nil},
		{Key: "Blob", Value: []byte{1, 2, 3}},
		{Key: "When", Value: time.UnixMilli(1700000000123).UTC()},
		{Key: "Nest", Value: bson.D{{Key: "a", Value: "b"}, {Key: "n", Value: int32(1)}}},
		{Key: "List", Value: bson.A{"x", int32(2), bson.D{{Key: "k", Value: false}}}},
	}
}

// TestU68WireEncodingsGoldenBytes: [GUARD any byte change from the BSON library swap]. The
// field values, element and filter bodies and the rendered text are what go on the wire
// and into the log, so they must stay byte-identical.
func TestU68WireEncodingsGoldenBytes(t *testing.T) {
	entries := []machine.FieldEntry{}
	for id, name := range []string{"Name", "Small", "Big", "Plain", "Score", "Flag", "Blob", "When", "Nest", "List"} {
		entries = append(entries, machine.FieldEntry{ID: uint16(id + 2), Type: "any", Name: name})
	}
	ref := bucketRef{db: "app", name: "Box", table: machine.FieldTable{Entries: entries}}
	nilDoc := bson.D{{Key: "Name", Value: nil}}
	if _, _, err := ref.documentOp(nilDoc); err == nil {
		t.Error("a nil field value encoded; the driver refuses it (no encoder found for <nil>)")
	}
	scalars := bson.D{}
	for _, element := range u68Doc() {
		if element.Key != "Nothing" {
			scalars = append(scalars, element)
		}
	}
	id, fields, err := ref.documentOp(scalars)
	if err != nil {
		t.Fatal(err)
	}
	if id != "u1" {
		t.Fatalf("id = %q", id)
	}
	got := map[string]string{}
	for _, field := range fields {
		got["field-"+entries[field.FieldID-2].Name] = u68Hash(field.Value)
	}
	for name, doc := range map[string]bson.D{"marshal-doc": u68Doc(), "marshal-empty": {}, "marshal-filter": {{Key: "Name", Value: "Ada"}, {Key: "Flag", Value: true}}} {
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		got[name] = u68Hash(raw)
	}
	body, err := bson.Marshal(u68Doc())
	if err != nil {
		t.Fatal(err)
	}
	got["render"] = u68Hash([]byte(RenderDocument(Document{Body: body}, machine.Declaration{})))
	for name, hash := range got {
		if want := u68Golden[name]; want != hash {
			t.Errorf("%q: sha256 %s, want %s", name, hash, want)
		}
	}
	if len(got) != len(u68Golden) {
		t.Errorf("recorded %d encodings, golden has %d", len(got), len(u68Golden))
	}
}

var u68Golden = map[string]string{
	"marshal-filter": "c08211403faeade1cf411f978bf202f88f5347578a76831b5137b54f83cdff0c",
	"field-Name":     "b0ee94a283a91aef67820d355454d09978cb499b40a0bb6c58f046472bb1fce7",
	"field-Small":    "88a3b523197bd7fa3c080df35f367e0485134b33624830bb9ba6e32943f2de5f",
	"field-Big":      "1c841b4685d56f52835ca22ba8050c30b7cc1639a92e90715f73972eb5de3f5c",
	"field-Plain":    "dc76c191f16890ddb66e6c7243ad06aa4eadaf4ef3dab369dd05171206fedf17",
	"field-Score":    "c89a304e41912a8d439ec0b22404581bbfe4fead57102aedb4432c3542a5efc5",
	"field-Blob":     "012630de48542f814db00ca89577d5cfbdced697e9fb8c5a856090d1d4a8848a",
	"field-When":     "acc7620941c7ea454093fb883f0460406d2fa0bfd064544bcc1ffecc81e3f2a7",
	"render":         "31834189c41770317f031da93839adf0f3bc135d84b5c054a28c21e4a2a49252",
	"field-Flag":     "fb8da7eb5b1b399e7321179dac9e9f65773d7331e1e30554e3911e4325e1ef19",
	"field-Nest":     "fa001b57b624f7e1c9b6242625dabcd954f3d66af72792298d5dbfdf83b03d07",
	"field-List":     "172c4bcc7c044f82a096c87e774ae7d1d6e55e985ee4380040e9255e2dd60da8",
	"marshal-doc":    "9ee50b39adc023013c1306935f03c6401371a2d4d64ff0592696e1d7a4af4c6e",
	"marshal-empty":  "49e8e3297545c15ab6a79471a7a34d43e24a8f1cb25ea3d8417c61f699267a3f",
}
