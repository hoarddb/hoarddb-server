package sealed

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// Envelope format constants for the 34-byte header.
const (
	EnvelopeHeaderLen = 34
	Magic             = 0x00484442 // 0x00 'H' 'D' 'B'
	Version           = 0x02

	// Purpose byte values
	PurposeUsers      = 0x01
	PurposeNodeMaster = 0x02
	// PurposeStoreKey seals the node store's data key, keys/store.wrapped
	// (spec rev 4 §2.3). Under the per-bucket layout 0x03 sealed a bucket's
	// data key; the byte is unchanged, so a store wrapped then still opens.
	PurposeStoreKey = 0x03
	PurposeWALEntry = 0x04
	// PurposeSegmentEntry is reserved: never reuse it; its artifact was removed
	// by the shared storage layout (GAP-31). The byte stays named because it is
	// part of a versioned envelope: a freed byte gets reused, and a reader of
	// the old artifact, if one survives anywhere, then misreads the new one.
	PurposeSegmentEntry = 0x05
	// PurposeBlobChunk seals a blob payload file (sealed/blobfile.go). It is
	// live; only 0x05 and 0x07 are reserved.
	PurposeBlobChunk = 0x06
	// PurposeBlobIndex is reserved: never reuse it; its artifact (the blob
	// index file) was removed by the shared storage layout (GAP-31). See
	// PurposeSegmentEntry for why the name stays.
	PurposeBlobIndex     = 0x07
	PurposeEpochRegistry = 0x08
	// PurposeAuthKey, PurposeClusterToken and PurposeTLSKey seal the node's
	// credential files under the node master when encryption at rest is on:
	// auth.token.key, cluster.token and keys/server.key. Each file has its
	// own byte, so one cannot be presented where another is expected.
	PurposeAuthKey      = 0x09
	PurposeClusterToken = 0x0A
	PurposeTLSKey       = 0x0B
	// PurposeClusterPepper seals auth.cluster.pepper, the ring cluster's password key (ring user
	// store spec 5.4), under the node master like the three files above.
	PurposeClusterPepper = 0x0C

	// FileHeader format constants for sealed append-only files
	FileHeaderLen = 24
	FileVersion   = 0x02
	FileNonceLen  = 12
	FileTagLen    = 16
)

// Seal encrypts plaintext under a derived key, returning an envelope blob.
// purpose, parent, and identity define the key hierarchy per §2.5; Seal
// appends "/" + gen to identity itself, so a caller passes it without the
// generation. gen must not be 0.
func Seal(purpose byte, parent []byte, identity string, gen uint64, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, FileNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("sealing: generating nonce: %w", err)
	}
	return seal(envelopeRequest{purpose: purpose, parent: parent, identity: identity, gen: gen, plaintext: plaintext}, nonce)
}

// envelopeRequest is what one envelope seals: the key it derives and the
// plaintext it encrypts.
type envelopeRequest struct {
	purpose   byte
	parent    []byte
	identity  string
	gen       uint64
	plaintext []byte
}

// seal is Seal with the nonce supplied. It exists so that the frozen seal
// vector can pin exact bytes: against a random nonce no test can see the
// header drop out of the AAD, because every header byte is also checked
// explicitly or fed into the derivation. Only this package can choose a
// nonce; every production seal goes through Seal.
func seal(req envelopeRequest, nonce []byte) ([]byte, error) {
	if req.gen == 0 {
		return nil, fmt.Errorf("sealing: generation must be >= 1")
	}
	if len(nonce) != FileNonceLen {
		return nil, fmt.Errorf("sealing: nonce must be %d bytes, got %d", FileNonceLen, len(nonce))
	}
	key, err := FileKey(req.parent, req.purpose, fmt.Sprintf("%s/%d", req.identity, req.gen))
	if err != nil {
		return nil, fmt.Errorf("sealing: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("sealing: creating cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("sealing: creating GCM: %w", err)
	}

	header := make([]byte, EnvelopeHeaderLen, EnvelopeHeaderLen+len(req.plaintext)+gcm.Overhead())
	binary.BigEndian.PutUint32(header[0:4], Magic)
	header[4] = Version
	header[5] = req.purpose
	copy(header[6:14], Kid(req.parent)) // the parent's kid: 8 ASCII hex characters
	binary.BigEndian.PutUint64(header[14:22], req.gen)
	copy(header[22:34], nonce)

	// The AAD is the whole header, byte for byte (spec §2.3). The ciphertext
	// is sealed into its own slice because an AEAD's dst may not overlap its
	// additional data.
	ciphertext := gcm.Seal(nil, nonce, req.plaintext, header)
	return append(header, ciphertext...), nil
}

// Open decrypts an envelope blob under the given parent key.
// identity is a callback that receives the parsed gen and returns the full identity string.
// This allows the caller to construct identities like "db/bucket/gen" without parsing the header.
func Open(purpose byte, parent []byte, identity func(gen uint64) string, blob []byte) ([]byte, uint64, error) {
	if len(blob) < EnvelopeHeaderLen {
		return nil, 0, fmt.Errorf("opening sealed envelope: blob too short (%d bytes)", len(blob))
	}

	// Parse and validate the header
	header := blob[:EnvelopeHeaderLen]

	// Check magic
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != Magic {
		return nil, 0, fmt.Errorf("opening sealed envelope: bad magic %08x", magic)
	}

	// Check version
	if header[4] != Version {
		return nil, 0, fmt.Errorf("opening sealed envelope: wrong version %02x", header[4])
	}

	// Check purpose
	if header[5] != purpose {
		return nil, 0, fmt.Errorf("opening sealed envelope: wrong purpose %02x, expected %02x", header[5], purpose)
	}

	// Check kid
	headerKid := string(header[6:14])
	expectedKid := Kid(parent)
	if headerKid != expectedKid {
		return nil, 0, fmt.Errorf("opening sealed envelope: wrong parent key (header kid %s, offered key kid %s)", headerKid, expectedKid)
	}

	// Read gen
	gen := binary.BigEndian.Uint64(header[14:22])
	nonce := header[22:34]

	// Derive the key with the full identity
	identityFull := identity(gen)
	key, err := FileKey(parent, purpose, identityFull)
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed envelope: %w", err)
	}

	// Decrypt
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed envelope: creating cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed envelope: creating GCM: %w", err)
	}

	ciphertext := blob[EnvelopeHeaderLen:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, header)
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed envelope: decryption failed: %w", err)
	}

	return plaintext, gen, nil
}

// FileHeader is the 24-byte plaintext header for sealed append-only files.
type FileHeader struct {
	Magic    uint32 // 0x00 'H' 'D' 'B'
	Version  byte   // 0x02
	Purpose  byte   // 0x04 WAL, 0x05 segment, 0x06 blob
	Flags    byte   // must be 0x00
	Reserved byte   // must be 0x00
	EpochID  uint64 // big-endian
	Gen      uint64 // big-endian, always 0 for WAL/segment/blob
}

// MarshalFileHeader marshals a FileHeader to bytes.
func MarshalFileHeader(fh *FileHeader) []byte {
	b := make([]byte, FileHeaderLen)
	binary.BigEndian.PutUint32(b[0:4], fh.Magic)
	b[4] = fh.Version
	b[5] = fh.Purpose
	b[6] = fh.Flags
	b[7] = fh.Reserved
	binary.BigEndian.PutUint64(b[8:16], fh.EpochID)
	binary.BigEndian.PutUint64(b[16:24], fh.Gen)
	return b
}

// ParseFileHeader parses a 24-byte header from a sealed file.
func ParseFileHeader(data []byte) (*FileHeader, error) {
	if len(data) < FileHeaderLen {
		return nil, fmt.Errorf("parsing sealed file header: data too short (%d bytes)", len(data))
	}

	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("parsing sealed file header: bad magic %08x at offset 0", magic)
	}

	version := data[4]
	if version != FileVersion {
		return nil, fmt.Errorf("parsing sealed file header: wrong version %02x at offset 4", version)
	}

	purpose := data[5]
	// Valid purposes: 0x04 (WAL), 0x05 (segment), 0x06 (blob)
	if purpose != 0x04 && purpose != 0x05 && purpose != 0x06 {
		return nil, fmt.Errorf("parsing sealed file header: unknown purpose %02x at offset 5", purpose)
	}

	flags := data[6]
	if flags != 0x00 {
		return nil, fmt.Errorf("parsing sealed file header: flags must be 0x00, got %02x at offset 6", flags)
	}

	reserved := data[7]
	if reserved != 0x00 {
		return nil, fmt.Errorf("parsing sealed file header: reserved must be 0x00, got %02x at offset 7", reserved)
	}

	fh := &FileHeader{
		Magic:    magic,
		Version:  version,
		Purpose:  purpose,
		Flags:    flags,
		Reserved: reserved,
		EpochID:  binary.BigEndian.Uint64(data[8:16]),
		Gen:      binary.BigEndian.Uint64(data[16:24]),
	}

	return fh, nil
}
