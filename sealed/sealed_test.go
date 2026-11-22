package sealed

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	// Generate a random 32-byte parent key
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	tests := []struct {
		name      string
		purpose   byte
		identity  string
		gen       uint64
		plaintext string
	}{
		{"users", PurposeUsers, "users", 1, "user data"},
		{"users gen2", PurposeUsers, "users", 2, "updated users"},
		{"node master wrap", PurposeNodeMaster, "node-master", 1, "32 bytes of node master"},
		{"store key", PurposeStoreKey, "store", 1, "store data key"},
		{"epoch registry", PurposeEpochRegistry, "wal", 1, "epoch registry bson"},
		{"empty plaintext", PurposeUsers, "users", 1, ""},
		{"large plaintext", PurposeUsers, "users", 1, string(make([]byte, 4096)) + "\x01"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sealed, err := Seal(tt.purpose, parentKey, tt.identity, tt.gen, []byte(tt.plaintext))
			if err != nil {
				t.Fatalf("Seal failed: %v", err)
			}

			identity := func(gen uint64) string {
				return fmt.Sprintf("%s/%d", tt.identity, gen)
			}

			plaintext, gen, err := Open(tt.purpose, parentKey, identity, sealed)
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			if gen != tt.gen {
				t.Errorf("gen mismatch: got %d, want %d", gen, tt.gen)
			}

			if !bytes.Equal(plaintext, []byte(tt.plaintext)) {
				t.Errorf("plaintext mismatch: got %q, want %q", string(plaintext), tt.plaintext)
			}
		})
	}
}

func TestPurposeSwapRefused(t *testing.T) {
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	plaintext := []byte("test data")

	// Seal with purpose 0x03
	sealed, err := Seal(PurposeStoreKey, parentKey, "db/bucket", 1, plaintext)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Try to open with a different purpose
	identity := func(gen uint64) string {
		return fmt.Sprintf("db/bucket/%d", gen)
	}

	_, _, err = Open(PurposeUsers, parentKey, identity, sealed)
	if err == nil {
		t.Error("Open with wrong purpose should have failed but succeeded")
	}
}

func TestTamperedHeaderByteRefused(t *testing.T) {
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	plaintext := []byte("test data")

	sealed, err := Seal(PurposeUsers, parentKey, "users", 1, plaintext)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	identity := func(gen uint64) string {
		return fmt.Sprintf("users/%d", gen)
	}

	// Test each byte of the 34-byte header
	for i := 0; i < EnvelopeHeaderLen; i++ {
		t.Run(fmt.Sprintf("byte_%d", i), func(t *testing.T) {
			tampered := make([]byte, len(sealed))
			copy(tampered, sealed)
			tampered[i] ^= 0xFF // flip all bits

			_, _, err := Open(PurposeUsers, parentKey, identity, tampered)
			if err == nil {
				t.Errorf("Open with tampered byte %d should have failed", i)
			}
		})
	}
}

func TestTamperedGenRefused(t *testing.T) {
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	plaintext := []byte("test data")

	sealed, err := Seal(PurposeUsers, parentKey, "users", 1, plaintext)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Tamper with gen field (bytes 14-21)
	tampered := make([]byte, len(sealed))
	copy(tampered, sealed)
	tampered[14] ^= 0xFF

	identity := func(gen uint64) string {
		return fmt.Sprintf("users/%d", gen)
	}

	_, _, err = Open(PurposeUsers, parentKey, identity, tampered)
	if err == nil {
		t.Error("Open with tampered gen should have failed")
	}
}

func TestTruncatedBlobRefused(t *testing.T) {
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	plaintext := []byte("test data")

	sealed, err := Seal(PurposeUsers, parentKey, "users", 1, plaintext)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	identity := func(gen uint64) string {
		return fmt.Sprintf("users/%d", gen)
	}

	// Test every truncation length
	for i := 0; i < len(sealed); i++ {
		truncated := sealed[:i]

		_, _, err := Open(PurposeUsers, parentKey, identity, truncated)
		if err == nil {
			t.Errorf("Open with truncated blob (len=%d) should have failed", i)
		}

		// Ensure we never get a partial plaintext
		// (this is implicitly tested by the error above)
	}
}

func TestWrongParentNamesBothKids(t *testing.T) {
	parentKey := make([]byte, 32)
	if _, err := rand.Read(parentKey); err != nil {
		t.Fatalf("generating parent key: %v", err)
	}

	plaintext := []byte("test data")

	sealed, err := Seal(PurposeUsers, parentKey, "users", 1, plaintext)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	// Use a different parent key
	wrongParent := make([]byte, 32)
	if _, err := rand.Read(wrongParent); err != nil {
		t.Fatalf("generating wrong parent key: %v", err)
	}

	identity := func(gen uint64) string {
		return fmt.Sprintf("users/%d", gen)
	}

	_, _, err = Open(PurposeUsers, wrongParent, identity, sealed)
	if err == nil {
		t.Error("Open with wrong parent should have failed")
	}

	// Error should name both kids
	if !bytes.Contains([]byte(err.Error()), []byte("kid")) {
		t.Errorf("error message should mention kid: %v", err)
	}
}

func TestFileHeaderRejectsNonZeroFlags(t *testing.T) {
	fh := &FileHeader{
		Magic:    Magic,
		Version:  FileVersion,
		Purpose:  0x04, // WAL
		Flags:    0x01, // non-zero
		Reserved: 0x00,
		EpochID:  1,
		Gen:      0,
	}

	header := MarshalFileHeader(fh)

	_, err := ParseFileHeader(header)
	if err == nil {
		t.Error("ParseFileHeader with non-zero flags should have failed")
	}

	// Error should mention flags
	if !bytes.Contains([]byte(err.Error()), []byte("flags")) {
		t.Errorf("error message should mention flags: %v", err)
	}
}

func TestFileHeaderRejectsNonZeroReserved(t *testing.T) {
	fh := &FileHeader{
		Magic:    Magic,
		Version:  FileVersion,
		Purpose:  0x04, // WAL
		Flags:    0x00,
		Reserved: 0x01, // non-zero
		EpochID:  1,
		Gen:      0,
	}

	header := MarshalFileHeader(fh)

	_, err := ParseFileHeader(header)
	if err == nil {
		t.Error("ParseFileHeader with non-zero reserved should have failed")
	}

	// Error should mention reserved
	if !bytes.Contains([]byte(err.Error()), []byte("reserved")) {
		t.Errorf("error message should mention reserved: %v", err)
	}
}

func TestFileHeaderRoundTrip(t *testing.T) {
	fh := &FileHeader{
		Magic:    Magic,
		Version:  FileVersion,
		Purpose:  0x04, // WAL
		Flags:    0x00,
		Reserved: 0x00,
		EpochID:  12345,
		Gen:      0,
	}

	header := MarshalFileHeader(fh)
	if len(header) != FileHeaderLen {
		t.Errorf("MarshalFileHeader length: got %d, want %d", len(header), FileHeaderLen)
	}

	parsed, err := ParseFileHeader(header)
	if err != nil {
		t.Fatalf("ParseFileHeader failed: %v", err)
	}

	if parsed.EpochID != fh.EpochID {
		t.Errorf("EpochID mismatch: got %d, want %d", parsed.EpochID, fh.EpochID)
	}

	if parsed.Purpose != fh.Purpose {
		t.Errorf("Purpose mismatch: got %02x, want %02x", parsed.Purpose, fh.Purpose)
	}
}

// TestFrozenSealVectorBindsTheHeaderAsAAD pins the exact bytes of one envelope
// per purpose Seal is used for, with a fixed nonce through the seal seam. It is
// the test that dies when the header drops out of the AAD: every other header
// check survives that, because magic, version and purpose are validated by
// Open, the kid is compared with Kid(parent), purpose and gen feed the
// derivation, and the nonce is the GCM nonce. Only the tag, computed over the
// AAD, moves. The vectors were produced once by this implementation and are
// frozen, like the driver vectors: regenerating them to make this pass defeats
// the test.
func TestFrozenSealVectorBindsTheHeaderAsAAD(t *testing.T) {
	parentKey := mustDecodeHex(t, "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	nonce := mustDecodeHex(t, "a0a1a2a3a4a5a6a7a8a9aaab")
	wrappedKey := mustDecodeHex(t, "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
	vectors := []struct {
		name         string
		purpose      byte
		identity     string
		gen          uint64
		plaintext    []byte
		wantEnvelope string
	}{
		{"users.json", PurposeUsers, "users", 1, []byte(`{"format_version":1}`),
			"00484442020161653231366332650000000000000001a0a1a2a3a4a5a6a7a8a9aaabc32e2b50a51f3607942cca2bab75c9acff13a8a602535e4d84696d40d66f4d9b810b252a"},
		{"node master wrap", PurposeNodeMaster, "node-master", 1, wrappedKey,
			"00484442020261653231366332650000000000000001a0a1a2a3a4a5a6a7a8a9aaab3321ccdc5a2195beb4196f4760c46624110ec77be4fb3f9fd60ed8651218eeb7d2d4d420a2fdc966f310afee4979f1e5"},
		{"node store key wrap", PurposeStoreKey, "store", 1, wrappedKey,
			"00484442020361653231366332650000000000000001a0a1a2a3a4a5a6a7a8a9aaab8822e49b739d2299d2e2af002688158ed925f8ea41231f61cfe9c8a3bf8edaa0d0c734a2856369c931537033162d56e1"},
		{"WAL epoch registry, second generation", PurposeEpochRegistry, "wal", 2, []byte("epoch registry bson"),
			"00484442020861653231366332650000000000000002a0a1a2a3a4a5a6a7a8a9aaab02bc710b84c2ca9e6ba0c75f65b1a067580035df498d74b8010f819126cb605829c1fd"},
	}
	for _, vector := range vectors {
		t.Run(vector.name, func(t *testing.T) {
			envelope, err := seal(envelopeRequest{
				purpose:   vector.purpose,
				parent:    parentKey,
				identity:  vector.identity,
				gen:       vector.gen,
				plaintext: vector.plaintext,
			}, nonce)
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			if got := hex.EncodeToString(envelope); got != vector.wantEnvelope {
				t.Fatalf("the envelope moved: the header is no longer the AAD, or the format changed\n got: %s\nwant: %s", got, vector.wantEnvelope)
			}
			opened, gen, err := Open(vector.purpose, parentKey, func(gen uint64) string {
				return fmt.Sprintf("%s/%d", vector.identity, gen)
			}, envelope)
			if err != nil {
				t.Fatalf("Open of the frozen envelope: %v", err)
			}
			if !bytes.Equal(opened, vector.plaintext) || gen != vector.gen {
				t.Fatalf("Open returned %q at gen %d, want %q at gen %d", opened, gen, vector.plaintext, vector.gen)
			}
		})
	}
}

// TestSealRefusesANonceOfTheWrongLength keeps the seam from sealing under a
// nonce GCM would reject or, worse, one a caller truncated.
func TestSealRefusesANonceOfTheWrongLength(t *testing.T) {
	parentKey := make([]byte, 32)
	for _, nonceLen := range []int{0, 11, 13} {
		_, err := seal(envelopeRequest{purpose: PurposeUsers, parent: parentKey, identity: "users", gen: 1}, make([]byte, nonceLen))
		if err == nil {
			t.Errorf("seal accepted a %d-byte nonce", nonceLen)
		}
	}
}

func mustDecodeHex(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decoding %q: %v", encoded, err)
	}
	return decoded
}
