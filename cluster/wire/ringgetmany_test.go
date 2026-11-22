package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// getManyReads is a frame's worth of keys that spans two buckets, mixes a plain get with a
// get_fields and carries the local option on one key.
func getManyReads() []RingRead {
	reads := u65EveryRead()
	plain, fields := reads[0], reads[7]
	other := plain
	other.BucketID = 42
	other.EncID = []byte{0x50, 'p', '2', 0, 0}
	local := other
	local.Options = RingReadOptions{Local: true}
	fieldsLocal := fields
	fieldsLocal.Options = RingReadOptions{Local: true}
	return []RingRead{plain, fields, other, local, fieldsLocal, plain}
}

// keyBody is the body of a lone RING_KEYED read: what a get-many key carries after its length.
func keyBody(t *testing.T, read RingRead) []byte {
	t.Helper()
	keyed, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	return keyed[RingPrologueLen:]
}

// lengthPrefixed is one key of a frame: [len:2 BE][body].
func lengthPrefixed(body []byte) []byte {
	return append(binary.BigEndian.AppendUint16(nil, uint16(len(body))), body...)
}

// getManyFrame lays out a frame by hand: the prologue, the count byte and each key length-prefixed.
func getManyFrame(count byte, bodies ...[]byte) []byte {
	out := binary.BigEndian.AppendUint64(nil, u65Prologue().Epoch)
	out = binary.BigEndian.AppendUint64(out, u65Prologue().StateSeq)
	out = binary.BigEndian.AppendUint32(out, u65Prologue().DeadlineMs)
	out = append(out, count)
	for _, body := range bodies {
		out = append(out, lengthPrefixed(body)...)
	}
	return out
}

func TestGetManyCodecRoundTripAndMutation(t *testing.T) {
	req := RingGetManyRequest{Prologue: u65Prologue(), Reads: getManyReads()}
	raw, err := EncodeRingGetMany(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingGetMany(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Prologue != req.Prologue || len(got.Reads) != len(req.Reads) {
		t.Fatalf("decoded %d keys under %+v, want %d under %+v", len(got.Reads), got.Prologue, len(req.Reads), req.Prologue)
	}
	for index, read := range got.Reads {
		want := req.Reads[index]
		if read.Class != want.Class || read.BucketID != want.BucketID || !bytes.Equal(read.EncID, want.EncID) || read.Options.Local != want.Options.Local {
			t.Fatalf("key %d decoded as %+v, want %+v", index, read, want)
		}
	}
	again, err := EncodeRingGetMany(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("re-encode differs: %v", err)
	}

	rng := rand.New(rand.NewPCG(0xe1, 0xe1))
	seeds := [][]byte{raw}
	for _, count := range []int{1, 2, 3} {
		seed, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: getManyReads()[:count]})
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, seed)
	}
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		decoded, err := DecodeRingGetMany(mutated)
		if err != nil {
			continue
		}
		accepted++
		if again, err := EncodeRingGetMany(decoded); err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted get-many %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutant was accepted: the re-encode check never ran")
	}
}

// TestGetManyBodiesAreByteIdenticalToKeyedBodies: [GUARD each key is a 2-byte length and a RING_KEYED read body].
func TestGetManyBodiesAreByteIdenticalToKeyedBodies(t *testing.T) {
	reads := getManyReads()
	raw, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: reads})
	if err != nil {
		t.Fatal(err)
	}
	want := append(bytes.Clone(raw[:RingPrologueLen]), byte(len(reads)))
	for _, read := range reads {
		want = append(want, lengthPrefixed(keyBody(t, read))...)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("get-many payload\n got %x\nwant %x", raw, want)
	}
}

func TestGetManyCountBounds(t *testing.T) {
	plain := u65EveryRead()[0]
	reads := make([]RingRead, 256)
	for i := range reads {
		reads[i] = plain
	}
	_, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: reads})
	u65AssertRefused(t, err, "at most 255 keys", "this has 256", "Split it into frames of 255 or fewer")
	raw, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: reads[:255]})
	if err != nil {
		t.Fatalf("255 keys refused: %v", err)
	}
	if got, err := DecodeRingGetMany(raw); err != nil || len(got.Reads) != 255 {
		t.Fatalf("255 keys decode as %d keys, %v", len(got.Reads), err)
	}
	// The count byte cannot say 256, so the decoder's bound is the bytes: a frame that says 255
	// and carries 254 is refused naming the first missing key.
	short := bytes.Clone(raw)
	short[RingPrologueLen] = 255
	short = short[:len(short)-len(raw[RingPrologueLen+1:])/255]
	_, err = DecodeRingGetMany(short)
	u65AssertRefused(t, err, "key 254 of 255", "Nothing was read")

	_, err = EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue()})
	u65AssertRefused(t, err, "carries 0 keys", "Send 1 to 255 keys, or use Get for one")
	zero := append(bytes.Clone(raw[:RingPrologueLen]), 0)
	_, err = DecodeRingGetMany(zero)
	u65AssertRefused(t, err, "carries 0 keys", "Send 1 to 255 keys, or use Get for one")
	_, err = DecodeRingGetMany(raw[:RingPrologueLen])
	u65AssertRefused(t, err, "20-byte client prologue", "count")
	_, err = DecodeRingGetMany(nil)
	u65AssertRefused(t, err, "20-byte client prologue")
}

func TestGetManyRefusesWritesAndOtherReadClasses(t *testing.T) {
	good, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: getManyReads()[:3]})
	if err != nil {
		t.Fatal(err)
	}
	withKey := func(body []byte) []byte {
		out := bytes.Clone(good)
		out[RingPrologueLen] = 4
		return append(out, lengthPrefixed(body)...)
	}
	write, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: u65EveryWriteClass()[2]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingGetMany(withKey(write[RingPrologueLen:]))
	u65AssertRefused(t, err, "key 3 of 4", "an update (class 0x02)", "reads documents only", "RING_BATCH", "Bucket.In(batch)")

	// A write body after a plain get is named for what it is: the length says where the get ends, so
	// a class byte that is also a possible trailer length is never guessed at.
	afterGet := getManyFrame(2, keyBody(t, getManyReads()[0]), write[RingPrologueLen:])
	_, err = DecodeRingGetMany(afterGet)
	u65AssertRefused(t, err, "key 1 of 2", "an update (class 0x02)")

	names := map[byte]string{
		RingClassPeek: "a peek", RingClassStoreList: "a store list", RingClassStoreCount: "a store count",
		RingClassLeaseGet: "a lease_get", RingClassStoreGet: "a store_get",
	}
	for _, read := range u65EveryRead() {
		name, refused := names[read.Class]
		if !refused {
			continue
		}
		body, err := encodeRingRead(read)
		if err != nil {
			t.Fatal(err)
		}
		_, err = DecodeRingGetMany(withKey(body))
		u65AssertRefused(t, err, "key 3 of 4", name, "reads whole documents (get 0x81, get_fields 0x89)", "StoreList")
		_, err = EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: []RingRead{read}})
		u65AssertRefused(t, err, "key 0 of 1", name)
	}

	_, err = DecodeRingGetMany(withKey([]byte{0x7f, 0, 0}))
	u65AssertRefused(t, err, "key 3 of 4", "0x7f")
}

func TestGetManyRefusesExpandNamingTheKey(t *testing.T) {
	plain := u65EveryRead()[0]
	expanding := plain
	expanding.Options = RingReadOptions{Expand: []RingExpandSpec{{Field: "Author"}}}
	_, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: []RingRead{plain, expanding}})
	u65AssertRefused(t, err, "key 1 of 2", "asks for expand", "GetExpanded", "StoreListExpanded")
	body, err := encodeRingRead(expanding)
	if err != nil {
		t.Fatal(err)
	}
	first, err := encodeRingRead(plain)
	if err != nil {
		t.Fatal(err)
	}
	raw := getManyFrame(2, first, body)
	_, err = DecodeRingGetMany(raw)
	u65AssertRefused(t, err, "key 1 of 2", "asks for expand", "GetExpanded")
}

func TestGetManyRefusesTrailingBytesTruncationAndLyingCounts(t *testing.T) {
	raw, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: getManyReads()[:3]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingGetMany(append(bytes.Clone(raw), 0))
	u65AssertRefused(t, err, "key 2 of 3", "1 bytes follow the last key", "Nothing was read")
	_, err = DecodeRingGetMany(raw[:len(raw)-3])
	u65AssertRefused(t, err, "key 2 of 3", "claims", "remain", "Nothing was read")
	lying := bytes.Clone(raw)
	lying[RingPrologueLen] = 4
	_, err = DecodeRingGetMany(lying)
	u65AssertRefused(t, err, "key 3 of 4", "is missing", "Nothing was read")
}

// TestGetManyFramingIsTheLengthPrefixAndNothingIsGuessed: [FRAMING owner ruling 2026-11-09]. Each
// key is [len:2 BE][body]; the decoder cuts the body at len and decodes that slice whole.
func TestGetManyFramingIsTheLengthPrefixAndNothingIsGuessed(t *testing.T) {
	reads := getManyReads()
	plain, local := keyBody(t, reads[0]), keyBody(t, reads[3])
	if len(local) <= len(plain) {
		t.Fatal("the local key has no trailer: the straddle case below means nothing")
	}

	// Two keys back to back, the first with a trailer, the second with one too: decoded by length.
	got, err := DecodeRingGetMany(getManyFrame(3, local, local, plain))
	if err != nil || len(got.Reads) != 3 || !got.Reads[0].Options.Local || !got.Reads[1].Options.Local || got.Reads[2].Options.Local {
		t.Fatalf("keys with and without trailers back to back decoded as %+v, %v", got.Reads, err)
	}

	// A length of 0 is refused naming the key.
	_, err = DecodeRingGetMany(getManyFrame(2, plain, nil))
	u65AssertRefused(t, err, "key 1 of 2", "length of 0", "Nothing was read")

	// A length longer than the bytes left.
	longer := getManyFrame(2, plain, plain)
	binary.BigEndian.PutUint16(longer[RingPrologueLen+1:], uint16(len(plain)+500))
	_, err = DecodeRingGetMany(longer)
	u65AssertRefused(t, err, "key 0 of 2", "claims", "bytes", "remain", "Nothing was read")

	// A length shorter than the body leaves its tail outside the key and the key is cut short.
	shorter := getManyFrame(2, plain, plain)
	binary.BigEndian.PutUint16(shorter[RingPrologueLen+1:], uint16(len(plain)-3))
	_, err = DecodeRingGetMany(shorter)
	u65AssertRefused(t, err, "key 0 of 2", "Nothing was read")

	// A length that ends inside the key's own trailer: the trailer straddles the declared length.
	straddle := getManyFrame(2, local, plain)
	binary.BigEndian.PutUint16(straddle[RingPrologueLen+1:], uint16(len(local)-2))
	_, err = DecodeRingGetMany(straddle)
	u65AssertRefused(t, err, "key 0 of 2", "trailer", "Nothing was read")

	// A length that runs past the key into the next one: bytes after the key's last field, inside the key.
	past := getManyFrame(2, plain, plain)
	binary.BigEndian.PutUint16(past[RingPrologueLen+1:], uint16(len(plain)+2))
	_, err = DecodeRingGetMany(past)
	u65AssertRefused(t, err, "key 0 of 2", "Nothing was read")

	// A write class byte right after a get is only ever the next key's class, never a trailer length.
	write, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Op: u65EveryWriteClass()[2]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = DecodeRingGetMany(getManyFrame(2, plain, write[RingPrologueLen:]))
	u65AssertRefused(t, err, "key 1 of 2", "an update (class 0x02)")
	// A frame that ends right after a key's length claims bytes that are not there.
	_, err = DecodeRingGetMany(getManyFrame(1, plain)[:RingPrologueLen+1+2])
	u65AssertRefused(t, err, "key 0 of 1", "claims", "remain")
}

func TestGetManyAllowsTheSameKeyTwice(t *testing.T) {
	plain := u65EveryRead()[0]
	raw, err := EncodeRingGetMany(RingGetManyRequest{Prologue: u65Prologue(), Reads: []RingRead{plain, plain}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingGetMany(raw)
	if err != nil || len(got.Reads) != 2 {
		t.Fatalf("a repeated key decoded as %d keys, %v", len(got.Reads), err)
	}
}

// TestGetManyAddsNoVocabularyAndNoStateLevel: [GUARD a green-stays-green pin, it cannot fail without the
// feature]. RING_GET_MANY is client-only: it replicates nothing and writes no record, so the on-disk
// vocabulary golden does not change and cluster.StateLevel does not change with it (spec section 2.4).
// The golden's first line is the level it was recorded at, so the one comparison pins both: the day
// someone adds a word a node writes, or bumps the level, the golden differs and this fails until the
// golden is regenerated with the reason. The level is read from the constant, not written here, so a
// later bump for another feature does not edit this test.
func TestGetManyAddsNoVocabularyAndNoStateLevel(t *testing.T) {
	want, err := os.ReadFile(onDiskGolden)
	if err != nil {
		t.Fatal(err)
	}
	if level := fmt.Sprintf("level %d\n", cluster.StateLevel); !strings.HasPrefix(string(want), level) {
		t.Fatalf("the on-disk golden is not recorded at the build's level (%q): RING_GET_MANY is a client opcode and needs no bump", level)
	}
	if got := strings.Join(onDiskLines(t), "\n") + "\n"; got != string(want) {
		t.Fatalf("the on-disk vocabulary changed with the get-many build:\n%s", got)
	}
}
