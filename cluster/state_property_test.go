package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// stateTryDecode runs DecodeClusterState and reports a panic instead of
// dying, so the test can print the input that caused it. The code under test
// has no recover of its own.
func stateTryDecode(raw []byte) (st ClusterState, panicked any, err error) {
	defer func() { panicked = recover() }()
	st, err = DecodeClusterState(raw)
	return st, nil, err
}

// stateMutate returns a damaged copy of golden: a flipped byte, a dropped or
// inserted byte, two top-level elements swapped, one duplicated, a changed
// length prefix or a changed element type byte.
func stateMutate(r *rand.Rand, golden []byte) []byte {
	out := append([]byte{}, golden...)
	elems, _ := vendorbsoncore.Document(golden).Elements()
	switch r.Intn(7) {
	case 0:
		out[r.Intn(len(out))] = byte(r.Intn(256))
	case 1:
		i := r.Intn(len(out))
		out = append(out[:i], out[i+1:]...)
	case 2:
		i := r.Intn(len(out) + 1)
		out = append(out[:i], append([]byte{byte(r.Intn(256))}, out[i:]...)...)
	case 3:
		i, j := r.Intn(len(elems)), r.Intn(len(elems))
		elems[i], elems[j] = elems[j], elems[i]
		out = stateJoin(elems)
	case 4:
		i := r.Intn(len(elems))
		elems = append(elems[:i+1], elems[i:]...)
		out = stateJoin(elems)
	case 5:
		out[r.Intn(4)] = byte(r.Intn(256))
	default:
		// A type byte: the first byte of a random top-level element, or
		// of an element inside the members array.
		off := 4
		target := r.Intn(len(elems))
		for i := 0; i < target; i++ {
			off += len(elems[i])
		}
		out[off] = byte(r.Intn(0x13))
	}
	return out
}

func stateJoin(elems []vendorbsoncore.Element) []byte {
	var body []byte
	for _, e := range elems {
		body = append(body, e...)
	}
	return vendorbsoncore.BuildDocument(nil, body)
}

func TestDecodeNeverPanicsAndIsCanonical(t *testing.T) {
	t.Setenv("TZ", "UTC")
	r := rand.New(rand.NewSource(20260501))
	goldens := [][]byte{stateMinimalGolden(t), stateFullGolden(t)}
	accepted := 0
	const mutations = 100000
	for n := 0; n < mutations; n++ {
		raw := stateMutate(r, goldens[n%2])
		st, panicked, err := stateTryDecode(raw)
		if panicked != nil {
			t.Fatalf("mutation %d: DecodeClusterState panicked (%v) on input %x", n, panicked, raw)
		}
		if err != nil {
			stateWantInvalid(t, err)
			continue
		}
		accepted++
		again, encErr := EncodeClusterState(st)
		if encErr != nil || !bytes.Equal(again, raw) {
			t.Fatalf("mutation %d: decode accepted non-canonical bytes (re-encode err %v)\n input %x\n again %x", n, encErr, raw, again)
		}
	}
	t.Logf("%d mutations, %d decoded (all canonical), %d refused", mutations, accepted, mutations-accepted)
}

func stateRandomState(r *rand.Rand) ClusterState {
	st := ClusterState{ClusterVersion: uint8(1 + r.Intn(255)), Epoch: r.Int63(), StateSeq: r.Int63(), NextDatabaseID: r.Int63(),
		Settings: Settings{StaleReads: r.Intn(2) == 0, HeartbeatMs: r.Int63(), StepDownMs: r.Int63(), FailoverMs: r.Int63(),
			DeadMs: r.Int63(), ProbeIntervalMs: r.Int63(), ProbeTimeoutMs: r.Int63(), ProbeFailures: r.Int31()}}
	r.Read(st.ClusterID[:])
	for i := r.Intn(6); i > 0; i-- {
		st.Voters = append(st.Voters, fmt.Sprintf("v%d-%d", r.Intn(1000), len(st.Voters)))
	}
	for i := r.Intn(6); i > 0; i-- {
		st.Members = append(st.Members, Member{ID: fmt.Sprintf("n%d-%d", r.Intn(1000), len(st.Members)),
			Incarnation: r.Int63(), DataAddr: fmt.Sprintf("h%d:%d", r.Intn(99), r.Intn(65535)),
			State: MemberState(1 + r.Intn(5)), Suffrage: []string{"voter", "nonvoter"}[r.Intn(2)],
			Witness: r.Intn(2) == 0, JoinedEpoch: r.Int63()})
		if r.Intn(2) == 0 {
			st.Members[len(st.Members)-1].ClientAddr = fmt.Sprintf("c%d.example:%d", r.Intn(99), r.Intn(65535))
		}
	}
	for i := r.Intn(5); i > 0; i-- {
		st.Databases = append(st.Databases, Database{Name: fmt.Sprintf("d%d-%d", r.Intn(1000), len(st.Databases)),
			ID: r.Int63(), RF: r.Int31(), SchemaVersion: r.Int63()})
	}
	for i := r.Intn(4); i > 0; i-- {
		bits := make([]byte, 1024)
		r.Read(bits)
		st.SeedBitmaps = append(st.SeedBitmaps, SeedBitmap{Node: fmt.Sprintf("n%d", r.Intn(3)), DBID: int64(len(st.SeedBitmaps)), Bits: bits})
	}
	return st
}

func stateReorder(r *rand.Rand, st ClusterState) ClusterState {
	st.Voters = append([]string{}, st.Voters...)
	st.Members = append([]Member{}, st.Members...)
	st.Databases = append([]Database{}, st.Databases...)
	st.SeedBitmaps = append([]SeedBitmap{}, st.SeedBitmaps...)
	r.Shuffle(len(st.Voters), func(i, j int) { st.Voters[i], st.Voters[j] = st.Voters[j], st.Voters[i] })
	r.Shuffle(len(st.Members), func(i, j int) { st.Members[i], st.Members[j] = st.Members[j], st.Members[i] })
	r.Shuffle(len(st.Databases), func(i, j int) { st.Databases[i], st.Databases[j] = st.Databases[j], st.Databases[i] })
	r.Shuffle(len(st.SeedBitmaps), func(i, j int) { st.SeedBitmaps[i], st.SeedBitmaps[j] = st.SeedBitmaps[j], st.SeedBitmaps[i] })
	return st
}

func TestRandomStatesRoundTripWithIdenticalBytes(t *testing.T) {
	r := rand.New(rand.NewSource(77))
	const states = 20000
	for n := 0; n < states; n++ {
		st := stateRandomState(r)
		a := stateMustEncode(t, st)
		b := stateMustEncode(t, stateReorder(r, st))
		if !bytes.Equal(a, b) {
			t.Fatalf("state %d: input array order changed the bytes", n)
		}
		back, err := DecodeClusterState(a)
		if err != nil {
			t.Fatalf("state %d: %v", n, err)
		}
		if again := stateMustEncode(t, back); !bytes.Equal(again, a) {
			t.Fatalf("state %d: round trip changed the bytes", n)
		}
		other, err := DecodeClusterState(b)
		if err != nil || !reflect.DeepEqual(back, other) {
			t.Fatalf("state %d: two orderings decoded differently (%v)", n, err)
		}
	}
}

// stateReplaceElem swaps the top-level element at index i (see the canonical
// order: 0 v, 1 cluster_id, 2 epoch, 3 state_seq, 4 next_database_id,
// 5 voters, 6 members, 7 databases, 8 seed_bitmaps, 9 settings).
func stateReplaceElem(t *testing.T, i int, elem vendorbsoncore.Element) []byte {
	t.Helper()
	good := stateMustEncode(t, stateFull())
	return stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
		e[i] = elem
		return e
	})
}

func stateRowOf(key string, row []byte) vendorbsoncore.Element {
	return vendorbsoncore.AppendArrayElement(nil, key, vendorbsoncore.BuildDocument(nil, row))
}

func stateMustRefuse(t *testing.T, raw []byte, mention string) {
	t.Helper()
	_, panicked, err := stateTryDecode(raw)
	if panicked != nil {
		t.Fatalf("decode panicked: %v on %x", panicked, raw)
	}
	stateWantInvalid(t, err)
	if !strings.Contains(err.Error(), mention) {
		t.Fatalf("the refusal should mention %q: %v", mention, err)
	}
}

func TestDecodeRefusesArrayWhereDocumentExpected(t *testing.T) {
	emptyArray := vendorbsoncore.AppendArrayElement(nil, "0", vendorbsoncore.BuildDocument(nil, nil))
	scoped := vendorbsoncore.AppendCodeWithScopeElement(nil, "0", "x", vendorbsoncore.BuildDocument(nil, nil))
	for _, row := range [][]byte{emptyArray, scoped} {
		for i, key := range map[int]string{7: "members", 8: "databases", 9: "seed_bitmaps"} {
			stateMustRefuse(t, stateReplaceElem(t, i, stateRowOf(key, row)), "document")
		}
	}
	stateMustRefuse(t, stateReplaceElem(t, 10, vendorbsoncore.AppendArrayElement(nil, "settings", vendorbsoncore.BuildDocument(nil, nil))), "settings")
}

// stateRawBinary writes a binary element byte for byte, without the inner
// length bsoncore adds for the obsolete subtype 0x02.
func stateRawBinary(key string, subtype byte, payload []byte) vendorbsoncore.Element {
	out := append([]byte{0x05}, key...)
	out = append(out, 0)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(payload)))
	out = append(out, subtype)
	return append(out, payload...)
}

func TestDecodeRefusesBinarySubtype02(t *testing.T) {
	for _, payload := range [][]byte{{1, 2}, make([]byte, 16), {}} {
		stateMustRefuse(t, stateReplaceElem(t, 2, stateRawBinary("cluster_id", 0x02, payload)), "cluster_id")
	}
	for _, payload := range [][]byte{make([]byte, 1024), {1}} {
		row := vendorbsoncore.AppendStringElement(nil, "node", "n1")
		row = vendorbsoncore.AppendInt64Element(row, "db_id", 1)
		row = append(row, stateRawBinary("bits", 0x02, payload)...)
		elem := stateRowOf("seed_bitmaps", vendorbsoncore.AppendDocumentElement(nil, "0", vendorbsoncore.BuildDocument(nil, row)))
		stateMustRefuse(t, stateReplaceElem(t, 9, elem), "bits")
	}
}

func TestDecodeRefusesBoolByte02(t *testing.T) {
	good := stateMustEncode(t, stateFull())
	for _, b := range []byte{2, 0xFF} {
		raw := append([]byte{}, good...)
		raw[bytes.Index(raw, []byte("witness\x00"))+len("witness\x00")] = b
		stateMustRefuse(t, raw, "witness")
	}
	raw := append([]byte{}, good...)
	raw[bytes.Index(raw, []byte("stale_reads\x00"))+len("stale_reads\x00")] = 7
	stateMustRefuse(t, raw, "stale_reads")
}

func TestDecodeRefusesStringWithoutTrailingNUL(t *testing.T) {
	good := stateMustEncode(t, stateFull())
	at := bytes.Index(good, []byte("n1\x00\x12incarnation")) + 2
	noNul := append([]byte{}, good...)
	noNul[at] = 'x'
	stateMustRefuse(t, noNul, "not a valid BSON document") // hoarddb-bson v0.7.3 validates string framing first
	embedded := append([]byte{}, good...)
	embedded[at-1] = 0 // "n\x00" followed by the terminator
	stateMustRefuse(t, embedded, "NUL")
	notUTF8 := append([]byte{}, good...)
	notUTF8[at-2] = 0xff
	stateMustRefuse(t, notUTF8, "UTF-8")
}

func TestDecodeRefusesWrongNumericAndArrayKeyForms(t *testing.T) {
	stateMustRefuse(t, stateReplaceElem(t, 3, vendorbsoncore.AppendDoubleElement(nil, "epoch", 1)), "epoch")
	stateMustRefuse(t, stateReplaceElem(t, 4, vendorbsoncore.AppendInt32Element(nil, "state_seq", 1)), "state_seq")
	keys := vendorbsoncore.AppendStringElement(nil, "1", "n1")
	keys = vendorbsoncore.AppendStringElement(keys, "0", "n2")
	stateMustRefuse(t, stateReplaceElem(t, 6, stateRowOf("voters", keys)), "index key")
	skipped := vendorbsoncore.AppendStringElement(nil, "0", "n1")
	skipped = vendorbsoncore.AppendStringElement(skipped, "2", "n2")
	stateMustRefuse(t, stateReplaceElem(t, 6, stateRowOf("voters", skipped)), "index key")
	notString := vendorbsoncore.AppendInt32Element(nil, "0", 1)
	stateMustRefuse(t, stateReplaceElem(t, 6, stateRowOf("voters", notString)), "voters")
}

func TestDecodeDuplicateRowsSayHowToFixThem(t *testing.T) {
	dup := func(mutate func(*ClusterState)) []byte {
		st := stateFull()
		mutate(&st)
		return encodeClusterStateInGivenOrder(st)
	}
	stateMustRefuse(t, dup(func(s *ClusterState) { s.Voters = []string{"n1", "n1"} }), "once")
	stateMustRefuse(t, dup(func(s *ClusterState) { s.Members[1].ID = "n1" }), "once")
	stateMustRefuse(t, dup(func(s *ClusterState) { s.Databases[1].Name = "app" }), "once")
	stateMustRefuse(t, dup(func(s *ClusterState) { s.SeedBitmaps = append(s.SeedBitmaps, s.SeedBitmaps[0]) }), "once")
}
