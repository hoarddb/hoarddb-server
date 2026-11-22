package cluster

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	vendorbson "go.mongodb.org/mongo-driver/bson"
	vendorbsontype "go.mongodb.org/mongo-driver/bson/bsontype"
	vendorbsoncore "go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// stateHex decodes a hex fragment written by hand from the spec's byte
// tables (membership Formats 2).
func stateHex(t *testing.T, parts ...string) []byte {
	t.Helper()
	out, err := hex.DecodeString(strings.Join(parts, ""))
	if err != nil {
		t.Fatalf("bad hex in the test: %v", err)
	}
	return out
}

var stateSettings = Settings{
	StaleReads: false, HeartbeatMs: 100, StepDownMs: 2000, FailoverMs: 5000,
	DeadMs: 600000, ProbeIntervalMs: 1000, ProbeTimeoutMs: 500, ProbeFailures: 3,
}

func stateClusterID() [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

// stateMinimal is the spec's minimal state: one node n1, data_addr h:1,
// epoch 1, state_seq 1, next_database_id 1, default settings.
func stateMinimal() ClusterState {
	return ClusterState{
		ClusterVersion: 1, ClusterID: stateClusterID(), Epoch: 1, StateSeq: 1, NextDatabaseID: 1,
		Voters:   []string{"n1"},
		Members:  []Member{{ID: "n1", Incarnation: 1, DataAddr: "h:1", State: MemberActive, Suffrage: "voter", JoinedEpoch: 1, JoinedStateSeq: 1}},
		Settings: stateSettings,
	}
}

func stateBits(first, last byte) []byte {
	bits := make([]byte, 1024)
	bits[0], bits[1023] = first, last
	return bits
}

// stateFull has three members in three states, two voters, two databases,
// one seed bitmap and the settings.
func stateFull() ClusterState {
	return ClusterState{
		ClusterVersion: 1, ClusterID: stateClusterID(), Epoch: 7, StateSeq: 42, NextDatabaseID: 3,
		Voters: []string{"n1", "n2"},
		Members: []Member{
			{ID: "n1", Incarnation: 1, DataAddr: "h:1", State: MemberActive, Suffrage: "voter", JoinedEpoch: 1, JoinedStateSeq: 1},
			{ID: "n2", Incarnation: 2, DataAddr: "h:2", State: MemberDown, Suffrage: "voter", JoinedEpoch: 2, JoinedStateSeq: 3},
			{ID: "n3", Incarnation: 1, DataAddr: "h:3", State: MemberJoining, Suffrage: "nonvoter", JoinedEpoch: 7, JoinedStateSeq: 40},
		},
		Databases:   []Database{{Name: "app", ID: 1, RF: 3, SchemaVersion: 2}, {Name: "logs", ID: 2, RF: 1, SchemaVersion: 1}},
		SeedBitmaps: []SeedBitmap{{Node: "n1", DBID: 1, Bits: stateBits(0x80, 0x01)}},
		Settings:    stateSettings,
	}
}

// stateMinimalGolden is the 490-byte document of membership Formats 2, one
// element per line, worked out by hand from that section's byte tables.
func stateMinimalGolden(t *testing.T) []byte {
	return stateHex(t,
		"61020000",       // document length, little-endian
		"10760001000000", // v
		"10636c75737465725f76657273696f6e0001000000",                         // cluster_version
		"05636c75737465725f69640010000000000102030405060708090a0b0c0d0e0f10", // cluster_id
		"1265706f6368000100000000000000",                                     // epoch
		"1273746174655f736571000100000000000000",                             // state_seq
		"126e6578745f64617461626173655f6964000100000000000000",               // next_database_id
		"04766f74657273000f000000023000030000006e310000",                     // voters
		"046d656d6265727300a0000000",                                         // members: type, name, array length
		"03300098000000",                                                     // member 0 header, length
		"02696400030000006e3100",                                             //   id
		"12696e6361726e6174696f6e000100000000000000",                         //   incarnation
		"02646174615f616464720004000000683a3100",                             //   data_addr
		"027374617465000700000061637469766500",                               //   state
		"0273756666726167650006000000766f74657200",                           //   suffrage
		"087769746e6573730000",                                               //   witness
		"126a6f696e65645f65706f6368000100000000000000",                       //   joined_epoch
		"126a6f696e65645f73746174655f736571000100000000000000",               //   joined_state_seq
		"00",                                   //   member terminator
		"00",                                   // members array terminator
		"046461746162617365730005000000",       // databases: type, name, array length
		"00",                                   // databases array terminator
		"04736565645f6269746d6170730005000000", // seed_bitmaps: type, name, array length
		"00",                                   // seed_bitmaps array terminator
		"0373657474696e677300ae000000087374616c655f72656164730000126865617274626561745f6d7300640000000000000012737465705f646f776e5f6d7300d007000000000000126661696c6f7665725f6d7300881300000000000012646561645f6d7300c0270900000000001270726f62655f696e74657276616c5f6d7300e8030000000000001270726f62655f74696d656f75745f6d7300f4010000000000001070726f62655f6661696c75726573000300000000", // settings
		"047573657273000500000000",                       // users: type, name, empty array
		"027065707065725f6b69640001000000"+"00",          // pepper_kid: empty string
		"12757365725f64726f705f736571000000000000000000", // user_drop_seq: 0
		"04757365725f7265717565737473000500000000",       // user_requests: type, name, empty array
		"00", // document terminator
	)
}

// stateFullGolden is the fully populated state, worked out the same way.
func stateFullGolden(t *testing.T) []byte {
	return stateHex(t,
		"7b080000",       // document length, little-endian
		"10760001000000", // v
		"10636c75737465725f76657273696f6e0001000000",                         // cluster_version
		"05636c75737465725f69640010000000000102030405060708090a0b0c0d0e0f10", // cluster_id
		"1265706f6368000700000000000000",                                     // epoch
		"1273746174655f736571002a00000000000000",                             // state_seq
		"126e6578745f64617461626173655f6964000300000000000000",               // next_database_id
		"04766f746572730019000000023000030000006e3100023100030000006e320000", // voters
		"046d656d6265727300d8010000",                                         // members: type, name, array length
		"03300098000000",                                                     // member 0 header, length
		"02696400030000006e3100",                                             //   id
		"12696e6361726e6174696f6e000100000000000000",                         //   incarnation
		"02646174615f616464720004000000683a3100",                             //   data_addr
		"027374617465000700000061637469766500",                               //   state
		"0273756666726167650006000000766f74657200",                           //   suffrage
		"087769746e6573730000",                                               //   witness
		"126a6f696e65645f65706f6368000100000000000000",                       //   joined_epoch
		"126a6f696e65645f73746174655f736571000100000000000000",               //   joined_state_seq
		"00",                     //   member terminator
		"03310096000000",         // member 1 header, length
		"02696400030000006e3200", //   id
		"12696e6361726e6174696f6e000200000000000000",           //   incarnation
		"02646174615f616464720004000000683a3200",               //   data_addr
		"0273746174650005000000646f776e00",                     //   state
		"0273756666726167650006000000766f74657200",             //   suffrage
		"087769746e6573730000",                                 //   witness
		"126a6f696e65645f65706f6368000200000000000000",         //   joined_epoch
		"126a6f696e65645f73746174655f736571000300000000000000", //   joined_state_seq
		"00",                     //   member terminator
		"0332009c000000",         // member 2 header, length
		"02696400030000006e3300", //   id
		"12696e6361726e6174696f6e000100000000000000",           //   incarnation
		"02646174615f616464720004000000683a3300",               //   data_addr
		"02737461746500080000006a6f696e696e6700",               //   state
		"02737566667261676500090000006e6f6e766f74657200",       //   suffrage
		"087769746e6573730000",                                 //   witness
		"126a6f696e65645f65706f6368000700000000000000",         //   joined_epoch
		"126a6f696e65645f73746174655f736571002800000000000000", //   joined_state_seq
		"00",                             //   member terminator
		"00",                             // members array terminator
		"0464617461626173657300ae000000", // databases: type, name, array length
		"03300051000000",                 // database 0 header, length
		"026e616d65000400000061707000",   //   name
		"126964000100000000000000",       //   id
		"1072660003000000",               //   rf
		"12736368656d615f76657273696f6e000200000000000000", //   schema_version
		"0273746f7261676500050000006469736b00",             //   storage (disk)
		"00",                                               //   database terminator
		"03310052000000",                                   // database 1 header, length
		"026e616d6500050000006c6f677300",                   //   name
		"126964000200000000000000",                         //   id
		"1072660001000000",                                 //   rf
		"12736368656d615f76657273696f6e000100000000000000", //   schema_version
		"0273746f7261676500050000006469736b00",             //   storage (disk)
		"00",                                               //   database terminator
		"00",                                               // databases array terminator
		"04736565645f6269746d6170730034040000",             // seed_bitmaps: type, name, array length
		"0330002c040000",                                   // bitmap 0 header, length
		"026e6f646500030000006e3100",                       //   node
		"1264625f6964000100000000000000",                   //   db_id
		"0562697473000004000000",                           //   bits: type, name, length 1024, subtype 0
		"80"+strings.Repeat("00", 1022)+"01",               // 1024 bytes of bits
		"00",                                               //   bitmap terminator
		"00",                                               // seed_bitmaps array terminator
		"0373657474696e677300ae000000087374616c655f72656164730000126865617274626561745f6d7300640000000000000012737465705f646f776e5f6d7300d007000000000000126661696c6f7665725f6d7300881300000000000012646561645f6d7300c0270900000000001270726f62655f696e74657276616c5f6d7300e8030000000000001270726f62655f74696d656f75745f6d7300f4010000000000001070726f62655f6661696c75726573000300000000", // settings
		"047573657273000500000000",                       // users: type, name, empty array
		"027065707065725f6b69640001000000"+"00",          // pepper_kid: empty string
		"12757365725f64726f705f736571000000000000000000", // user_drop_seq: 0
		"04757365725f7265717565737473000500000000",       // user_requests: type, name, empty array
		"00", // document terminator
	)
}

func stateWantInvalid(t *testing.T, err error) {
	t.Helper()
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != CodeInvalidArgument {
		t.Fatalf("want INVALID_ARGUMENT, got %v", err)
	}
}

func TestClusterStateGoldenBytes(t *testing.T) {
	min := stateMinimalGolden(t)
	if len(min) != 609 {
		t.Fatalf("the hand-written minimal golden is %d bytes, membership Formats 2 says 516 with joined_state_seq, cluster_version adds 21 and the four empty user fields 72", len(min))
	}
	got, err := EncodeClusterState(stateMinimal())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, min) {
		t.Fatalf("minimal state: encoder gave %d bytes\n got %x\nwant %x", len(got), got, min)
	}
	full := stateFullGolden(t)
	if len(full) != 2171 {
		t.Fatalf("the hand-written full golden is %d bytes, want 2171 (2099 before the four empty user fields)", len(full))
	}
	got, err = EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, full) {
		t.Fatalf("full state: encoder gave %d bytes\n got %x\nwant %x", len(got), got, full)
	}
}

func TestClusterStateRoundTrip(t *testing.T) {
	for name, st := range map[string]ClusterState{"minimal": stateMinimal(), "full": stateFull()} {
		raw, err := EncodeClusterState(st)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		back, err := DecodeClusterState(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(back, st) {
			t.Fatalf("%s: round trip changed the state\n got %+v\nwant %+v", name, back, st)
		}
		again, err := EncodeClusterState(back)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s: re-encoding gave different bytes (err %v)", name, err)
		}
	}
}

// stateShuffled returns the full state with every array in a random order.
func stateShuffled(r *rand.Rand) ClusterState {
	st := stateFull()
	st.SeedBitmaps = append(st.SeedBitmaps,
		SeedBitmap{Node: "n1", DBID: 2, Bits: stateBits(1, 2)},
		SeedBitmap{Node: "n2", DBID: 1, Bits: stateBits(3, 4)},
		SeedBitmap{Node: "n1", DBID: 10, Bits: stateBits(5, 6)})
	r.Shuffle(len(st.Voters), func(i, j int) { st.Voters[i], st.Voters[j] = st.Voters[j], st.Voters[i] })
	r.Shuffle(len(st.Members), func(i, j int) { st.Members[i], st.Members[j] = st.Members[j], st.Members[i] })
	r.Shuffle(len(st.Databases), func(i, j int) { st.Databases[i], st.Databases[j] = st.Databases[j], st.Databases[i] })
	r.Shuffle(len(st.SeedBitmaps), func(i, j int) { st.SeedBitmaps[i], st.SeedBitmaps[j] = st.SeedBitmaps[j], st.SeedBitmaps[i] })
	return st
}

func TestArraysEncodeSorted(t *testing.T) {
	r := rand.New(rand.NewSource(19))
	var first []byte
	for i := 0; i < 100; i++ {
		raw, err := EncodeClusterState(stateShuffled(r))
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = raw
		} else if !bytes.Equal(first, raw) {
			t.Fatalf("run %d: the same logical state encoded to different bytes", i)
		}
	}
	back, err := DecodeClusterState(first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Voters, []string{"n1", "n2"}) ||
		back.Members[0].ID != "n1" || back.Members[2].ID != "n3" ||
		back.Databases[0].Name != "app" {
		t.Fatalf("arrays decoded out of order: %+v", back)
	}
	wantBitmaps := [][2]any{{"n1", int64(1)}, {"n1", int64(2)}, {"n1", int64(10)}, {"n2", int64(1)}}
	for i, w := range wantBitmaps {
		if back.SeedBitmaps[i].Node != w[0] || back.SeedBitmaps[i].DBID != w[1] {
			t.Fatalf("bitmap %d is (%s,%d), want %v: db_id sorts as a number", i, back.SeedBitmaps[i].Node, back.SeedBitmaps[i].DBID, w)
		}
	}
}

// stateRebuild re-assembles the top-level document of raw after edit has
// changed its element list, so a test can build a document the encoder
// itself would never write.
func stateRebuild(t *testing.T, raw []byte, edit func([]vendorbsoncore.Element) []vendorbsoncore.Element) []byte {
	t.Helper()
	elems, err := vendorbsoncore.Document(raw).Elements()
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	for _, e := range edit(elems) {
		body = append(body, e...)
	}
	return vendorbsoncore.BuildDocument(nil, body)
}

func stateElem(key string, typ vendorbsontype.Type) vendorbsoncore.Element {
	switch typ {
	case vendorbson.TypeArray:
		return vendorbsoncore.AppendArrayElement(nil, key, vendorbsoncore.BuildDocument(nil, nil))
	case vendorbson.TypeInt32:
		return vendorbsoncore.AppendInt32Element(nil, key, 1)
	}
	return vendorbsoncore.AppendInt64Element(nil, key, 1)
}

func TestNoBucketFieldDecodes(t *testing.T) {
	good, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"buckets", "next_bucket_id", "next_field_id"} {
		withIt := stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			out := append([]vendorbsoncore.Element{}, e[:8]...)
			out = append(out, stateElem(key, vendorbson.TypeArray))
			return append(out, e[8:]...)
		})
		_, err := DecodeClusterState(withIt)
		stateWantInvalid(t, err)
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("the refusal must name %q: %v", key, err)
		}
	}
	// A bucket row nested in a database row is refused as well.
	st := stateFull()
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	nested := bytes.Replace(raw, []byte("schema_version\x00"), []byte("buckets_versio\x00"), 1)
	if bytes.Equal(nested, raw) {
		t.Fatal("test bug: nothing replaced")
	}
	_, err = DecodeClusterState(nested)
	stateWantInvalid(t, err)
}

func TestClusterStateHoldsNoBucketsAndIsCanonical(t *testing.T) {
	typ := reflect.TypeOf(ClusterState{})
	for i := 0; i < typ.NumField(); i++ {
		if strings.Contains(strings.ToLower(typ.Field(i).Name), "bucket") && typ.Field(i).Name != "SeedBitmaps" {
			t.Fatalf("ClusterState has a bucket field %s", typ.Field(i).Name)
		}
	}
	raw, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	elems, err := vendorbsoncore.Document(raw).Elements()
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, e := range elems {
		keys = append(keys, e.Key())
	}
	want := []string{"v", "cluster_version", "cluster_id", "epoch", "state_seq", "next_database_id", "voters", "members", "databases", "seed_bitmaps", "settings", "users", "pepper_kid", "user_drop_seq", "user_requests"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("top-level fields %v, want %v", keys, want)
	}
	if bytes.Contains(raw, []byte("bucket")) {
		t.Fatal("the encoded state mentions a bucket")
	}
	withBucket := stateRebuild(t, raw, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
		return append(e, stateElem("buckets", vendorbson.TypeArray))
	})
	_, err = DecodeClusterState(withBucket)
	stateWantInvalid(t, err)

	// One logical state, one byte string, whatever order it was built in.
	a := stateMustEncode(t, stateFull())
	b := stateMustEncode(t, stateShuffled(rand.New(rand.NewSource(1))))
	c := stateMustEncode(t, stateFull())
	if !bytes.Equal(a, c) {
		t.Fatal("encoding is not deterministic")
	}
	if bytes.Equal(a, b) {
		t.Fatal("test bug: the shuffled state has more bitmaps and should differ")
	}
}

func TestDecodeRefusesNonCanonicalBytes(t *testing.T) {
	good, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	unsorted := func(mutate func(*ClusterState)) []byte {
		st := stateFull()
		mutate(&st)
		return encodeClusterStateInGivenOrder(st)
	}
	cases := map[string][]byte{
		"voters unsorted": unsorted(func(s *ClusterState) { s.Voters = []string{"n2", "n1"} }),
		"members unsorted": unsorted(func(s *ClusterState) {
			s.Members[0], s.Members[1] = s.Members[1], s.Members[0]
		}),
		"databases unsorted": unsorted(func(s *ClusterState) {
			s.Databases[0], s.Databases[1] = s.Databases[1], s.Databases[0]
		}),
		"bitmaps unsorted": unsorted(func(s *ClusterState) {
			s.SeedBitmaps = []SeedBitmap{{Node: "n2", DBID: 1, Bits: stateBits(0, 0)}, {Node: "n1", DBID: 1, Bits: stateBits(0, 0)}}
		}),
		"duplicate voter":  unsorted(func(s *ClusterState) { s.Voters = []string{"n1", "n1"} }),
		"duplicate member": unsorted(func(s *ClusterState) { s.Members[1].ID = "n1" }),
		"unknown member state word": unsorted(func(s *ClusterState) {
			s.Members[0].State = MemberState(9)
		}),
		"duplicate key": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			return append(e[:2:2], append([]vendorbsoncore.Element{e[2]}, e[2:]...)...)
		}),
		"duplicate key at the end": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			return append(e, e[2])
		}),
		"unknown field": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			return append(e, stateElem("extra", vendorbson.TypeInt64))
		}),
		"fields reordered": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			e[2], e[3] = e[3], e[2]
			return e
		}),
		"field missing": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element { return e[1:] }),
		"epoch as int32": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			e[2] = stateElem("epoch", vendorbson.TypeInt32)
			return e
		}),
		"v is 2": stateRebuild(t, good, func(e []vendorbsoncore.Element) []vendorbsoncore.Element {
			e[0] = vendorbsoncore.AppendInt32Element(nil, "v", 2)
			return e
		}),
		"trailing byte": append(append([]byte{}, good...), 0),
		"truncated":     good[:len(good)-1],
		"empty":         nil,
		"length prefix lies": func() []byte {
			b := append([]byte{}, good...)
			b[0]++
			return b
		}(),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeClusterState(raw)
			stateWantInvalid(t, err)
		})
	}
}

func TestSeedBitmapBodyIs1024Bytes(t *testing.T) {
	// cluster/machine imports cluster, so this package cannot import
	// machine.BitmapBytes; the length is checked here against the literal.
	for _, n := range []int{0, 1023, 1025} {
		st := stateFull()
		st.SeedBitmaps[0].Bits = make([]byte, n)
		_, err := EncodeClusterState(st)
		stateWantInvalid(t, err)
	}
	// A well-formed document whose bitmap body is one byte off must be
	// refused for its length, not for a broken BSON length prefix.
	for _, n := range []int{1023, 1025} {
		st := stateFull()
		st.SeedBitmaps[0].Bits = make([]byte, n)
		_, err := DecodeClusterState(encodeClusterStateInGivenOrder(st))
		stateWantInvalid(t, err)
		if !strings.Contains(err.Error(), "1024") {
			t.Fatalf("the refusal must name the 1024-byte body: %v", err)
		}
	}
}

func stateMustEncode(t *testing.T, st ClusterState) []byte {
	t.Helper()
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestClusterStateVectorsCarryJoinedStateSeq: the member row has grown by exactly one int64 element,
// joined_state_seq (26 bytes: type, the 17-byte name, 8 value bytes), so the minimal state is
// 490+26 = 516 bytes (537 with the 21-byte cluster_version) and the full one 1964+3*26 = 2042 before
// later fields, and a decoded row gives the value back.
// [BREAK: the encoder skips joined_state_seq].
func TestClusterStateVectorsCarryJoinedStateSeq(t *testing.T) {
	const elementBytes = 1 + len("joined_state_seq") + 1 + 8
	if elementBytes != 26 {
		t.Fatalf("the element is %d bytes, the spec says 26", elementBytes)
	}
	min := stateMustEncode(t, stateMinimal())
	if len(min) != 490+elementBytes+21+72 {
		t.Fatalf("the minimal state is %d bytes, want 490 plus the 26-byte element, the 21-byte cluster_version and the 72 bytes of the four empty user fields", len(min))
	}
	if !bytes.Contains(min, []byte("joined_state_seq\x00")) {
		t.Fatal("the encoded member row names no joined_state_seq")
	}
	got, err := DecodeClusterState(stateMustEncode(t, stateFull()))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{1, 3, 40} {
		if got.Members[i].JoinedStateSeq != want {
			t.Errorf("member %s decoded joined_state_seq %d, want %d", got.Members[i].ID, got.Members[i].JoinedStateSeq, want)
		}
	}
}

// TestRegisterMemberStampsItsOwnStateSeq: BOOTSTRAP and REGISTER_MEMBER record the state_seq of their
// own commit, a repeat registration of the same incarnation leaves it where it was, and a replace
// (the joining commit of a down member under a newer incarnation) records the replace's own.
func TestRegisterMemberStampsItsOwnStateSeq(t *testing.T) {
	var st ClusterState
	fsmApply(t, &st, CommandBootstrap, fsmBootstrapBody("n1"))
	fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody("n2", 1, "h:2", false))
	first, _ := fsmMember(st, "n2")
	if root, _ := fsmMember(st, "n1"); root.JoinedStateSeq != 1 || first.JoinedStateSeq != 2 || st.StateSeq != 2 {
		t.Fatalf("n1 joined at %d, n2 at %d, state_seq %d; want 1, 2, 2", root.JoinedStateSeq, first.JoinedStateSeq, st.StateSeq)
	}
	fsmApply(t, &st, CommandRegisterMember, fsmRegisterBody("n2", 1, "h:2", false))
	if again, _ := fsmMember(st, "n2"); again.JoinedStateSeq != 2 || st.StateSeq != 3 {
		t.Fatalf("a repeat registration moved joined_state_seq to %d", again.JoinedStateSeq)
	}
	fsmApply(t, &st, CommandSetMemberState, fsmSetMemberStateBody("n2", "down"))
	fsmApply(t, &st, CommandSetMemberState, fsmReplaceBody("n2", 2, true))
	if replaced, _ := fsmMember(st, "n2"); replaced.JoinedStateSeq != st.StateSeq {
		t.Fatalf("a replace recorded joined_state_seq %d, its own commit is %d", replaced.JoinedStateSeq, st.StateSeq)
	}
}

// TestAMemberWithNoClientAddrHasNoDriverAddr: the data address is the internode listener and serves
// no driver, so DriverAddr does not fall back to it. [BREAK: return DataAddr when ClientAddr is empty]
func TestAMemberWithNoClientAddrHasNoDriverAddr(t *testing.T) {
	if got := (Member{DataAddr: "h:4433"}).DriverAddr(); got != "" {
		t.Errorf("a member with no client address has driver address %q", got)
	}
	if got := (Member{DataAddr: "h:4433", ClientAddr: "h:7434"}).DriverAddr(); got != "h:7434" {
		t.Errorf("driver address %q, want h:7434", got)
	}
}

func TestStateSnapshotRoundTripsTheStorageByte(t *testing.T) {
	st := stateFull()
	st.Databases[0].Storage = StorageDisk
	st.Databases[1].Storage = StorageMemory
	raw, err := EncodeClusterState(st)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeClusterState(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(back.Databases) != 2 || back.Databases[0].Storage != StorageDisk || back.Databases[1].Storage != StorageMemory {
		t.Fatalf("storage bytes after the round trip = %+v, want disk then memory", back.Databases)
	}
	if !reflect.DeepEqual(back, st) {
		t.Fatalf("round trip changed the state:\n got %+v\nwant %+v", back, st)
	}
	// An old-format row (no storage element) is refused, not read as disk.
	old := stateFull()
	oldRaw, err := EncodeClusterState(old)
	if err != nil {
		t.Fatal(err)
	}
	stripped := bytes.ReplaceAll(oldRaw, []byte("storage\x00"), []byte("sXorage\x00"))
	if _, err := DecodeClusterState(stripped); err == nil {
		t.Fatal("a snapshot whose database row lacks the storage element decoded")
	}
}

// TestADatabaseRowWithNoStorageElementIsRefusedAsMissing: the row an older build wrote has every
// element but the last, so the refusal is "missing", not "unknown key". [BREAK: make the storage
// read optional in decodeStateDatabase]
func TestADatabaseRowWithNoStorageElementIsRefusedAsMissing(t *testing.T) {
	var row []byte
	row = vendorbsoncore.AppendStringElement(row, "name", "app")
	row = vendorbsoncore.AppendInt64Element(row, "id", 7)
	row = vendorbsoncore.AppendInt32Element(row, "rf", 1)
	row = vendorbsoncore.AppendInt64Element(row, "schema_version", 1)
	oldRow := vendorbsoncore.BuildDocument(nil, row)
	const want = `database is missing field "storage"`
	if _, err := decodeStateDatabase(oldRow); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("decodeStateDatabase of a row with no storage element = %v, want it to contain %q", err, want)
	}

	raw, err := EncodeClusterState(stateFull())
	if err != nil {
		t.Fatal(err)
	}
	old := stateRebuild(t, raw, func(elems []vendorbsoncore.Element) []vendorbsoncore.Element {
		out := make([]vendorbsoncore.Element, len(elems))
		for i, e := range elems {
			if e.Key() == "databases" {
				rows := vendorbsoncore.AppendDocumentElement(nil, "0", oldRow)
				e = vendorbsoncore.AppendArrayElement(nil, "databases", vendorbsoncore.BuildDocument(nil, rows))
			}
			out[i] = e
		}
		return out
	})
	if _, err := DecodeClusterState(old); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("DecodeClusterState of a snapshot whose row has no storage element = %v, want it to contain %q", err, want)
	}
}
