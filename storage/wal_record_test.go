package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/rand"
	"os"
	"strings"
	"testing"
)

var testRequestID = [16]byte{0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2, 0xE1, 0xF0}

// workedInsertBody is the 76-byte record body of conv §9's worked record: the class byte,
// the flags byte (0x00: no index stamp), Formats 1's 40-byte payload header and the insert
// form (storage-format spec §1.2).
const workedInsertBody = "01" + "00" +
	"0000000000000029" + "0000000000000007" + "0000000000000007" +
	"00000000000000000000000000000000" +
	"5070310000" + "00000008" + "0203000000703100" + "0001" +
	"0002" + "00000009" + "0204000000416461" + "00"

// TestWALRecordV2Layout pins the 18-byte header of record format 2: the version at 0, the
// request id at offset 1 and the op index at 17, then the body. The class is the entry's op
// byte and the body's first byte, not a header field, and the body length is the entry's.
func TestWALRecordV2Layout(t *testing.T) {
	record := EncodeWALRecord(0x02, testRequestID, []byte{0x05, 0x00, 9, 9})
	if len(record) != 18+4 {
		t.Fatalf("record is %d bytes, want an 18-byte header and the 4-byte body", len(record))
	}
	if record[0] != 0x02 {
		t.Errorf("byte 0 is %#02x, want version 0x02", record[0])
	}
	if !bytes.Equal(record[1:17], testRequestID[:]) {
		t.Errorf("bytes 1-16 are %x, want the request id %x", record[1:17], testRequestID)
	}
	if record[17] != 0x02 {
		t.Errorf("byte 17 is %#02x, want the op index 0x02", record[17])
	}
	if !bytes.Equal(record[18:], []byte{0x05, 0x00, 9, 9}) {
		t.Errorf("the body starts at offset 18 and is %x, want 05000909", record[18:])
	}
	if WALRecordHeaderLen != 18 {
		t.Errorf("WALRecordHeaderLen = %d, want 18", WALRecordHeaderLen)
	}
	decoded, err := DecodeWALRecord(0x05, record)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	if decoded.Class != 0x05 || decoded.OpIndex != 0x02 || decoded.RequestID != testRequestID || !bytes.Equal(decoded.Body, []byte{0x05, 0x00, 9, 9}) {
		t.Errorf("decoded %+v", decoded)
	}
}

// TestRetainedLogRecordCarriesTheRequestID appends a record through the real
// log under its position key and reads it back after a reopen: the request id
// survives in the record, which is the dedupe key's only durable home on a
// copy that replays (conv §9, §18).
func TestRetainedLogRecordCarriesTheRequestID(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	position := LogPosition{Epoch: 42, Seq: 55401}
	body := []byte{0x05, 0x00, 'b', 'o', 'd', 'y'}
	record := EncodeWALRecord(3, testRequestID, body)
	if _, err := w.AppendFrom("app.users", 0x05, PositionKey(17, 3, position), record, WALOrigin{Kind: WALOriginOwn, Epoch: 42}); err != nil {
		t.Fatalf("AppendFrom: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	entries, err := reopened.ReadFrom(1)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ReadFrom gave %d entries, err %v", len(entries), err)
	}
	slot, db, gotPosition, err := DecodePositionKey(entries[0].Key)
	if err != nil || slot != 17 || db != 3 || gotPosition != position {
		t.Errorf("the logged key decodes to slot %d db %d pos %v err %v", slot, db, gotPosition, err)
	}
	got, err := DecodeWALRecord(entries[0].Op, entries[0].Data)
	if err != nil {
		t.Fatalf("DecodeWALRecord: %v", err)
	}
	if got.RequestID != testRequestID || got.OpIndex != 3 || got.Class != 0x05 || !bytes.Equal(got.Body, body) {
		t.Errorf("the logged record decodes to %+v, want request id %x op index 3 class 5 body", got, testRequestID)
	}
}

// TestVectorsCarryTheRequestIDAtOffsetOne reads the frozen retained-log rows of
// wal-origin-vectors.json: the record begins with the 18-byte header, the request id sits
// at offset 1 and the body starts 01 00 (class, flags), so a rebuild that indexes the body
// at offset 0 finds no ring entries (write spec §8 test 18).
func TestVectorsCarryTheRequestIDAtOffsetOne(t *testing.T) {
	body, err := hex.DecodeString(workedInsertBody)
	if err != nil {
		t.Fatal(err)
	}
	wantKey := PositionKey(17, 3, LogPosition{Epoch: 42, Seq: 55401})
	wantRecord := EncodeWALRecord(0, testRequestID, body)
	var checked int
	for _, vector := range loadOriginVectors(t) {
		if vector.KeyHex == "" {
			continue
		}
		checked++
		if vector.Op != 0x01 {
			t.Errorf("%s: the entry op is %#x, want the record's class 0x01", vector.Name, vector.Op)
		}
		_, key, record := vector.loggedBytes(t)
		if !bytes.Equal(key, wantKey) {
			t.Errorf("%s: key %x is not PositionKey(17, 3, 42:55401) = %x", vector.Name, key, wantKey)
		}
		if !bytes.Equal(record, wantRecord) {
			t.Errorf("%s: record %x is not what EncodeWALRecord writes, %x", vector.Name, record, wantRecord)
		}
		if len(record) != 94 || !bytes.Equal(record[1:17], testRequestID[:]) || record[18] != 0x01 || record[19] != 0x00 {
			t.Errorf("%s: want 94 bytes, the request id at offset 1 and the body starting 01 00; got %x", vector.Name, record)
			continue
		}
		// The op the log hands back is the one DecodeWALRecord cross-checks;
		// a hand-passed class would hide a row the build refuses.
		segment, err := hex.DecodeString(vector.Hex)
		if err != nil {
			t.Fatalf("%s: hex: %v", vector.Name, err)
		}
		dir := t.TempDir()
		if err := os.WriteFile(segmentFilePath(dir, 1), segment, 0o600); err != nil {
			t.Fatal(err)
		}
		w, err := OpenWAL(dir, nil)
		if err != nil {
			t.Fatalf("%s: OpenWAL: %v", vector.Name, err)
		}
		entries, err := w.ReadFrom(1)
		_ = w.Close()
		if err != nil || len(entries) != 1 {
			t.Fatalf("%s: read %d entries, err %v", vector.Name, len(entries), err)
		}
		decoded, err := DecodeWALRecord(entries[0].Op, entries[0].Data)
		if err != nil {
			t.Errorf("%s: %v", vector.Name, err)
			continue
		}
		if decoded.RequestID != testRequestID || hex.EncodeToString(decoded.Body) != workedInsertBody {
			t.Errorf("%s: decoded %+v, want the worked record's id and body", vector.Name, decoded)
		}
	}
	if checked != 2 {
		t.Errorf("the vectors hold %d retained-log rows, want an Own and a Replica row", checked)
	}
}

func TestWorkedRecordIsNinetyFourBytes(t *testing.T) {
	body, err := hex.DecodeString(workedInsertBody)
	if err != nil {
		t.Fatal(err)
	}
	record := EncodeWALRecord(0, testRequestID, body)
	if len(body) != 76 || len(record) != 94 {
		t.Errorf("worked record is %d bytes with a %d-byte body, want 94 and 76", len(record), len(body))
	}
}

// TestARecordWhoseBodyClassDiffersIsRefused is R2's first form at the record decoder: the
// entry's op byte and the body's first byte must agree, or the record is not trusted.
func TestARecordWhoseBodyClassDiffersIsRefused(t *testing.T) {
	record := EncodeWALRecord(0, testRequestID, []byte{0x01, 0x00, 'x'})
	if _, err := DecodeWALRecord(0x01, record); err != nil {
		t.Fatalf("a record whose body agrees with the entry: %v", err)
	}
	_, err := DecodeWALRecord(0x05, record)
	var mismatch *WALRecordClassError
	if !errors.As(err, &mismatch) || mismatch.Body != 0x01 || mismatch.Entry != 0x05 {
		t.Fatalf("DecodeWALRecord(op 0x05, body class 0x01) = %v, want a *WALRecordClassError naming both", err)
	}
	if !strings.Contains(err.Error(), "the body says class 0x01 and the entry 0x05") {
		t.Fatalf("refusal %q does not name both classes", err)
	}
}

func TestDecodeWALRecordRefusals(t *testing.T) {
	good := EncodeWALRecord(0, testRequestID, []byte{0x01, 0x00, 'x', 'y'})
	badVersion := append([]byte(nil), good...)
	badVersion[0] = 1
	cases := map[string][]byte{
		"empty":              nil,
		"header only":        good[:18],
		"header and class":   good[:19],
		"wrong version":      badVersion,
		"version 3":          append([]byte{3}, good[1:]...),
		"body class differs": append(append([]byte(nil), good[:18]...), 0x02, 0x00, 'x', 'y'),
		"truncated to 17":    good[:17],
	}
	for name, input := range cases {
		if _, err := DecodeWALRecord(0x01, input); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	_, err := DecodeWALRecord(0x01, badVersion)
	var version *WALRecordVersionError
	if !errors.As(err, &version) || version.Version != 1 {
		t.Fatalf("a version-1 record: err = %v, want a *WALRecordVersionError naming 0x01", err)
	}
	if _, err := DecodeWALRecord(0x01, good[:19]); err == nil || !strings.Contains(err.Error(), "19") {
		t.Fatalf("a 19-byte record: err = %v, want a refusal naming its length", err)
	}
}

// TestDecodeWALRecordMutations flips, drops and inserts bytes of valid
// records 100,000 times: the decoder never panics, and whatever it accepts
// re-encodes to the same bytes.
func TestDecodeWALRecordMutations(t *testing.T) {
	rng := rand.New(rand.NewSource(25))
	for i := 0; i < 100000; i++ {
		class := byte(1 + rng.Intn(9))
		body := make([]byte, 2+rng.Intn(40))
		rng.Read(body)
		body[0] = class
		record := EncodeWALRecord(byte(rng.Intn(256)), testRequestID, body)
		for flips := rng.Intn(4); flips >= 0; flips-- {
			switch rng.Intn(3) {
			case 0:
				record[rng.Intn(len(record))] ^= byte(1 + rng.Intn(255))
			case 1:
				record = record[:rng.Intn(len(record)+1)]
			default:
				at := rng.Intn(len(record) + 1)
				record = append(record[:at], append([]byte{byte(rng.Intn(256))}, record[at:]...)...)
			}
			if len(record) == 0 {
				break
			}
		}
		decoded, err := DecodeWALRecord(class, record)
		if err != nil {
			continue
		}
		if decoded.Class != class || len(decoded.Body) < 2 {
			t.Fatalf("accepted %x as class %#02x with a %d-byte body", record, decoded.Class, len(decoded.Body))
		}
		if again := EncodeWALRecord(decoded.OpIndex, decoded.RequestID, decoded.Body); !bytes.Equal(again, record) {
			t.Fatalf("accepted %x re-encodes to %x", record, again)
		}
	}
}
