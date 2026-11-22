package wire

import (
	"runtime"
	"strings"
	"testing"
)

// TestGetManyAnswerBytesIsTheEncodedLength: the size an answer charges to the budget is the
// length of its encoded reply, for every shape the handler keeps (a found document, with and
// without a served_from) and across the sizes where the BSON length digits change.
func TestGetManyAnswerBytesIsTheEncodedLength(t *testing.T) {
	for _, served := range []string{"", servedOwner, servedLocal} {
		for _, size := range []int{0, 1, 9, 10, 99, 100, 1000, 65535, 65536, 1 << 20} {
			reply := RingReply{Found: ringTrue(), Elements: [][]byte{make([]byte, size)}, Served: served}
			raw, err := EncodeRingReply(reply)
			if err != nil {
				t.Fatal(err)
			}
			if got := getManyAnswerBytes(reply); got != int64(len(raw)) {
				t.Errorf("served %q, body %d bytes: charged %d, encoded %d", served, size, got, len(raw))
			}
		}
	}
}

// TestGetManyAnswerBytesDoesNotEncodeTheDocument: measuring an answer must not copy its body; a
// 24 MiB document was once encoded a second time only to count its bytes.
func TestGetManyAnswerBytesDoesNotEncodeTheDocument(t *testing.T) {
	reply := RingReply{Found: ringTrue(), Elements: [][]byte{[]byte(strings.Repeat("x", 8<<20))}, Served: servedOwner}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	getManyAnswerBytes(reply)
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("measuring an 8 MiB answer allocated %d bytes", allocated)
	}
}
