package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// keyedPutPayload is a put of about 1 KB that names its key, with or without
// one now() in it.
func keyedPutPayload(b *testing.B, withNow bool) []byte {
	b.Helper()
	fields := map[string]interface{}{"_id_": "user-0001", "Name": "ada", "Age": int32(42)}
	for field := range 12 {
		fields[strings.Repeat(string(rune('a'+field)), 4)] = strings.Repeat("x", 64)
	}
	if withNow {
		fields["CreatedAt"] = NowMarker()
	}
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "users", Key: "user-0001", Data: fields})
	if err != nil {
		b.Fatalf("encoding the put: %v", err)
	}
	return payload
}

// BenchmarkResolveDocumentIDsKeyedPut is the coordinator's pre-log cost for a
// keyed put of about 1 KB: the common write, with no now(), and one with.
func BenchmarkResolveDocumentIDsKeyedPut(b *testing.B) {
	for _, form := range []struct {
		name    string
		withNow bool
	}{{"without now", false}, {"with now", true}} {
		b.Run(form.name, func(b *testing.B) {
			payload := keyedPutPayload(b, form.withNow)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := resolveDocumentIDs(transport.OpPut, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
