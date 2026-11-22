package sealed

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// The sealed blob payload file (at-rest encryption spec §4.2.1): the 24-byte
// file header with purpose 0x06, then n chunks of
// [12-byte nonce][ciphertext][16-byte tag]. Every chunk but the last holds
// exactly BlobChunkSize plaintext bytes and the last holds 1 to BlobChunkSize.
// Nothing in the file records the chunk size, the chunk count or the content
// length: the count comes from the blob's metadata, and a reader without that
// metadata is not a supported reader.

// BlobChunkSize is the plaintext length of every chunk but the last. It is a
// constant of the format for (version 0x02, purpose 0x06, flags 0x00): no byte
// on disk records it, so a different size is a new flags value, which this
// codec refuses at the header.
const BlobChunkSize = 64 << 10

// blobSealLimit is the most chunks one file key may seal (spec §2.2's
// sealLimit). Every chunk of a payload is a seal under the same AES-GCM key
// with a random 96-bit nonce, which is safe only far below 2^32 seals. The
// count is known before the first byte is written, so the bound is one
// comparison in SealBlobFile and no counter. A variable so a test can lower it.
var blobSealLimit int64 = 1 << 31

// blobNonceSource is where SealBlobFile reads each chunk's 12-byte nonce. It
// is crypto/rand in every build; only this package's frozen-vector test sets
// it, because against random nonces no test can pin the bytes of a file, and
// so none can see the index or the count move inside the AAD's layout.
var blobNonceSource io.Reader = rand.Reader

// blobChunkOverhead is what sealing adds to each chunk: its stored nonce and
// its tag.
const blobChunkOverhead = FileNonceLen + FileTagLen

var (
	// ErrBlobKeyMissing is outcome 1 of spec §4.2.1 h: the payload's header
	// names an epoch the blob key registry does not hold. It is a key problem,
	// not damage — the payload is intact. A BlobKeySource wraps it, and the
	// codec returns it so the caller can tell it from ErrBlobCorrupt with
	// errors.Is.
	ErrBlobKeyMissing = errors.New("the blob key registry does not hold the epoch this payload was sealed under")

	// ErrBlobCorrupt is outcome 2 of spec §4.2.1 h: the file is not a valid
	// sealed payload of the length its metadata requires, or a chunk failed
	// authentication. Every error that wraps it is a *BlobCorruptError, which
	// carries the reason. Outcome 3 — content that opened but does not match
	// the metadata's checksum — is store.ErrBlobChecksumMismatch, because only
	// the store holds the checksum.
	ErrBlobCorrupt = errors.New("the blob payload is corrupt")

	// ErrBlobSealLimit is SealBlobFile's refusal of content that would need
	// more chunks than one file key may seal (spec §2.2). Nothing is written.
	ErrBlobSealLimit = errors.New("the blob needs more chunks than one file key may seal")

	// ErrBlobEmpty is SealBlobFile's refusal to seal zero bytes. A zero-length
	// blob is inline content and never a sealed file (spec §4.2.1 c): a file
	// with a header and no chunk would carry no tag, so nothing in it would be
	// authenticated.
	ErrBlobEmpty = errors.New("a zero-length blob is never a sealed payload file")
)

// BlobCorruptReason says which of spec §4.2.1 d's checks refused a payload.
type BlobCorruptReason int

// The reasons, in the order the reader checks them.
const (
	// BlobCorruptSizeZero: the metadata's size is below 1. A file-backed blob
	// always holds at least one chunk.
	BlobCorruptSizeZero BlobCorruptReason = iota + 1
	// BlobCorruptWrongLength: the file is not exactly 24 + size + 28·n bytes.
	BlobCorruptWrongLength
	// BlobCorruptNoSealedHeader: the first 24 bytes are not a version 0x02,
	// purpose 0x06, flags 0, reserved 0, gen 0 header. A sealed node never
	// reads a payload as plaintext, so this is corruption and not a fallback.
	BlobCorruptNoSealedHeader
	// BlobCorruptChunkAuthentication: a chunk did not open under the file key
	// and its AAD.
	BlobCorruptChunkAuthentication
)

// BlobCorruptError is the error behind ErrBlobCorrupt. It carries what the
// codec knows — the payload file name, the reason and, where they apply, the
// lengths, the epoch id and the chunk index and count — and never a key byte,
// a content byte or a hash of the content. The layer that knows the database,
// the bucket and the user's key completes the message an operator reads (spec
// §4.2.1 h); errors.As gives it these fields so it does not parse text.
type BlobCorruptError struct {
	// PayloadName is the payload file name the reader recomputed.
	PayloadName string
	Reason      BlobCorruptReason
	// Size is the metadata's size the reader was given.
	Size int64
	// FileLen and WantFileLen are set for BlobCorruptWrongLength.
	FileLen     int64
	WantFileLen int64
	// HeaderProblem names the header field that refused, for
	// BlobCorruptNoSealedHeader.
	HeaderProblem string
	// Epoch is the header's epoch id, for BlobCorruptChunkAuthentication.
	Epoch uint64
	// Chunk is the index, counted from 0, of the chunk that failed, and Chunks
	// the total count, for BlobCorruptChunkAuthentication.
	Chunk  int64
	Chunks int64
}

func (e *BlobCorruptError) Error() string {
	switch e.Reason {
	case BlobCorruptSizeZero:
		return fmt.Sprintf("payload %s is corrupt: its metadata records a size of %d bytes, and a file-backed blob holds at least 1", e.PayloadName, e.Size)
	case BlobCorruptWrongLength:
		if e.FileLen == e.WantFileLen {
			return fmt.Sprintf("payload %s is corrupt: its metadata requires %d bytes, and reading it did not yield exactly that many", e.PayloadName, e.WantFileLen)
		}
		return fmt.Sprintf("payload %s is corrupt: it holds %d bytes, its metadata requires %d", e.PayloadName, e.FileLen, e.WantFileLen)
	case BlobCorruptNoSealedHeader:
		return fmt.Sprintf("payload %s is corrupt: it has no valid sealed header (%s) — a sealed node never reads a payload as plaintext", e.PayloadName, e.HeaderProblem)
	default:
		return fmt.Sprintf("payload %s is corrupt: chunk %d of %d (counted from 0, sealed under blob key epoch %d) failed authentication: the file was changed or damaged after it was written", e.PayloadName, e.Chunk, e.Chunks, e.Epoch)
	}
}

// Unwrap makes errors.Is(err, ErrBlobCorrupt) true.
func (e *BlobCorruptError) Unwrap() error { return ErrBlobCorrupt }

// BlobKeyMissingError is the error behind ErrBlobKeyMissing on the read path:
// the payload's header names an epoch the key source does not hold. It is a
// different failure from BlobCorruptError with a different fix — restore the
// registry, not the blob — so it is a different type. Like BlobCorruptError it
// names the payload file and the epoch, and the caller that knows the
// database, the bucket and the user's key completes the message.
type BlobKeyMissingError struct {
	// PayloadName is the payload file name the reader recomputed.
	PayloadName string
	// Epoch is the epoch id in the payload's header.
	Epoch uint64
}

func (e *BlobKeyMissingError) Error() string {
	return fmt.Sprintf("payload %s was sealed under blob key epoch %d, which the blob key registry (keys/blobs.wrapped) does not hold. "+
		"The payload is intact. Restore keys/blobs.wrapped from a backup taken after epoch %d was created", e.PayloadName, e.Epoch, e.Epoch)
}

// Unwrap makes errors.Is(err, ErrBlobKeyMissing) true.
func (e *BlobKeyMissingError) Unwrap() error { return ErrBlobKeyMissing }

// BlobKeySource is what the codec needs from the blob epoch registry
// (keys/blobs.wrapped). The server implements it; the blob store will accept
// one, and a store given none is a plaintext node's.
type BlobKeySource interface {
	// CurrentEpoch is the epoch id new payload files are sealed under.
	CurrentEpoch() uint64
	// FileKey returns the 32-byte file key HKDF(epoch key, 0x06, payloadName).
	// It wraps ErrBlobKeyMissing when the registry does not hold epoch.
	FileKey(epoch uint64, payloadName string) ([]byte, error)
}

// blobChunkCount is n = ⌈size / BlobChunkSize⌉ for a size of at least 1. It is
// written so that no size overflows it: the size it is given on the read path
// is an unauthenticated store value.
func blobChunkCount(size int64) int64 {
	return (size-1)/BlobChunkSize + 1
}

// blobFileLen is 24 + size + 28·n for a size of at least 1. A size so large
// that the sum overflows yields a negative length, which equals no file's.
func blobFileLen(size int64) int64 {
	return FileHeaderLen + size + blobChunkOverhead*blobChunkCount(size)
}

// newBlobAEAD is AES-256-GCM under a file key. The length is checked here
// because aes.NewCipher also takes 16 and 24 bytes, and a key source that
// returned one would silently seal under a weaker cipher.
func newBlobAEAD(fileKey []byte) (cipher.AEAD, error) {
	if len(fileKey) != 32 {
		return nil, fmt.Errorf("the blob file key is %d bytes, want 32", len(fileKey))
	}
	block, err := aes.NewCipher(fileKey)
	if err != nil {
		return nil, fmt.Errorf("creating the blob file cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating the blob file AEAD: %w", err)
	}
	return aead, nil
}

// Offsets inside a chunk's AAD, header(24) ‖ BE64(index) ‖ BE64(count).
const (
	blobAADIndexOffset = FileHeaderLen
	blobAADCountOffset = FileHeaderLen + 8
	blobAADLen         = FileHeaderLen + 16
)

// newBlobChunkAAD returns the AAD of chunk 0 of a file of chunkCount chunks.
// The caller sets the index for each later chunk with setBlobChunkIndex; the
// header and the count are the same for every chunk of a file.
func newBlobChunkAAD(header []byte, chunkCount int64) []byte {
	aad := make([]byte, blobAADLen)
	copy(aad, header)
	binary.BigEndian.PutUint64(aad[blobAADCountOffset:], uint64(chunkCount))
	return aad
}

// setBlobChunkIndex sets the chunk index, counted from 0, in aad.
func setBlobChunkIndex(aad []byte, index int64) {
	binary.BigEndian.PutUint64(aad[blobAADIndexOffset:blobAADCountOffset], uint64(index))
}

// blobHeaderProblem names the first field of a payload's 24 header bytes that
// is not the sealed blob header's, or returns "" when all are. The codec
// checks them itself: ParseFileHeader also accepts a WAL's purpose, the
// retired 0x05 and any gen, and with those a wrong file would fail later, at a
// chunk, with an error that names the wrong cause. The problem never quotes a
// byte it read: on a sealed node the file may be someone's plaintext.
func blobHeaderProblem(header []byte) string {
	switch {
	case binary.BigEndian.Uint32(header[0:4]) != Magic:
		return "it does not begin with the sealed-file magic"
	case header[4] != FileVersion:
		return fmt.Sprintf("its version is not 0x%02x", FileVersion)
	case header[5] != PurposeBlobChunk:
		return fmt.Sprintf("its purpose is not 0x%02x, a blob payload's", PurposeBlobChunk)
	case header[6] != 0x00:
		return "its flags are not 0x00, the only payload layout this version reads"
	case header[7] != 0x00:
		return "its reserved byte is not 0x00"
	case binary.BigEndian.Uint64(header[16:24]) != 0:
		return "its generation is not 0"
	}
	return ""
}

// readPayload fills buffer from r. A reader that ends first is a payload
// shorter than its declared length, which is corruption; any other failure is
// an I/O error and is reported as one, not as damage to the payload.
func readPayload(r io.Reader, buffer []byte, payloadName string, size int64) error {
	_, err := io.ReadFull(r, buffer)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return blobReaderDisagrees(payloadName, size)
	}
	if err != nil {
		return fmt.Errorf("reading payload %s: %w", payloadName, err)
	}
	return nil
}

// blobReaderDisagrees is the corruption of a reader that did not hold exactly
// the bytes the declared file length promised.
func blobReaderDisagrees(payloadName string, size int64) *BlobCorruptError {
	wantFileLen := blobFileLen(size)
	return &BlobCorruptError{PayloadName: payloadName, Reason: BlobCorruptWrongLength, Size: size, FileLen: wantFileLen, WantFileLen: wantFileLen}
}

// BlobFileLen is the exact length of the sealed payload file of a blob of size
// bytes: 24 + size + 28·n. A size below 1 is an error, because no sealed file
// of it exists.
func BlobFileLen(size int64) (int64, error) {
	if size < 1 {
		return 0, fmt.Errorf("a sealed blob payload holds at least 1 byte of content, not %d", size)
	}
	return blobFileLen(size), nil
}

// SealBlobRequest is one payload to seal.
type SealBlobRequest struct {
	Keys BlobKeySource
	// PayloadName is the payload's final file name, the file key's identity:
	// recomputed by the caller with store.PayloadFileName from slot, bucket id
	// and blob id; never taken from metadata Path, and never the temporary's
	// name.
	PayloadName string
	Content     []byte
}

// SealBlobFile writes the sealed payload file of req.Content to w: the header
// under the key source's current epoch, then each chunk under a fresh random
// nonce. It refuses zero bytes with ErrBlobEmpty, and content of more chunks
// than one file key may seal with ErrBlobSealLimit; a refusal writes nothing.
//
// Beyond req.Content it holds one sealed chunk at a time. An error after the
// first write leaves a partial file in w: the caller writes to the payload's
// temporary and removes it, as it does for any failed put.
func SealBlobFile(w io.Writer, req SealBlobRequest) error {
	if len(req.Content) == 0 {
		return fmt.Errorf("sealing payload %s: %w", req.PayloadName, ErrBlobEmpty)
	}
	contentLen := int64(len(req.Content))
	chunkCount := blobChunkCount(contentLen)
	if chunkCount > blobSealLimit {
		return fmt.Errorf("sealing payload %s: its %d bytes are %d chunks, over the limit of %d seals under one file key: %w",
			req.PayloadName, contentLen, chunkCount, blobSealLimit, ErrBlobSealLimit)
	}
	epoch := req.Keys.CurrentEpoch()
	fileKey, err := req.Keys.FileKey(epoch, req.PayloadName)
	if err != nil {
		return fmt.Errorf("sealing payload %s under blob key epoch %d: %w", req.PayloadName, epoch, err)
	}
	aead, err := newBlobAEAD(fileKey)
	if err != nil {
		return fmt.Errorf("sealing payload %s under blob key epoch %d: %w", req.PayloadName, epoch, err)
	}

	header := MarshalFileHeader(&FileHeader{Magic: Magic, Version: FileVersion, Purpose: PurposeBlobChunk, EpochID: epoch})
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("writing the header of payload %s: %w", req.PayloadName, err)
	}
	aad := newBlobChunkAAD(header, chunkCount)
	sealedChunk := make([]byte, FileNonceLen, blobChunkOverhead+BlobChunkSize)
	for index := range chunkCount {
		chunkStart := index * BlobChunkSize
		chunk := req.Content[chunkStart:min(chunkStart+BlobChunkSize, contentLen)]
		nonce := sealedChunk[:FileNonceLen]
		if _, err := io.ReadFull(blobNonceSource, nonce); err != nil {
			return fmt.Errorf("generating the nonce of chunk %d of payload %s: %w", index, req.PayloadName, err)
		}
		setBlobChunkIndex(aad, index)
		// The nonce is stored ahead of its chunk: Seal appends the ciphertext
		// and tag to it.
		if _, err := w.Write(aead.Seal(nonce, nonce, chunk, aad)); err != nil {
			return fmt.Errorf("writing chunk %d of %d of payload %s: %w", index, chunkCount, req.PayloadName, err)
		}
	}
	return nil
}

// blobChunkRun is what opening a payload's chunks needs once the header has
// been checked and the file key derived.
type blobChunkRun struct {
	payloadName string
	epoch       uint64
	fileKey     []byte
	// header is the file's 24 header bytes as read, the head of every AAD.
	header []byte
	// size is the content length, which fixes where each chunk ends.
	size int64
	// chunkCount is the count bound into every chunk's AAD.
	chunkCount int64
}

// openBlobChunks opens every chunk of body, in order, and returns the content
// only if all of them opened. It is separate from OpenBlobFile so that a test
// can reach the AAD without the length check in front of it: that check
// refuses a file with a chunk dropped from its end under every AAD variant,
// and so cannot show that the count is bound.
func openBlobChunks(body io.Reader, run blobChunkRun) ([]byte, error) {
	aead, err := newBlobAEAD(run.fileKey)
	if err != nil {
		return nil, fmt.Errorf("opening payload %s under blob key epoch %d: %w", run.payloadName, run.epoch, err)
	}
	aad := newBlobChunkAAD(run.header, run.chunkCount)
	content := make([]byte, 0, run.size)
	sealedChunk := make([]byte, blobChunkOverhead+BlobChunkSize)
	for index := range run.chunkCount {
		sealedLen := blobChunkOverhead + min(BlobChunkSize, run.size-int64(len(content)))
		if err := readPayload(body, sealedChunk[:sealedLen], run.payloadName, run.size); err != nil {
			return nil, err
		}
		setBlobChunkIndex(aad, index)
		// Open appends into content's spare capacity, so the content is
		// assembled in the one buffer it is returned in.
		opened, err := aead.Open(content, sealedChunk[:FileNonceLen], sealedChunk[FileNonceLen:sealedLen], aad)
		if err != nil {
			return nil, &BlobCorruptError{
				PayloadName: run.payloadName, Reason: BlobCorruptChunkAuthentication,
				Size: run.size, Epoch: run.epoch, Chunk: index, Chunks: run.chunkCount,
			}
		}
		content = opened
	}
	return content, nil
}

// OpenBlobRequest is one payload to open.
type OpenBlobRequest struct {
	Keys BlobKeySource
	// PayloadName is the payload's final file name, the file key's identity:
	// recomputed by the caller with store.PayloadFileName from slot, bucket id
	// and blob id; never taken from metadata Path.
	PayloadName string
	// Size is the size field of the blob's metadata, read from the store and
	// never inferred from the file.
	Size int64
	// FileLen is the payload file's length on disk.
	FileLen int64
}

// OpenBlobFile reads a sealed payload file from r and returns its content, in
// the order of spec §4.2.1 d: the size is at least 1; the file's length is
// exactly 24 + size + 28·n, checked before anything is read; the header is a
// blob payload's; the header's epoch is in the registry; every chunk opens, in
// order. It is all or nothing: on any error the returned content is nil, also
// when earlier chunks opened. It never returns the file's bytes as they are —
// a file that is not a sealed payload is corruption, not plaintext.
//
// The errors are of three kinds a caller tells apart with errors.Is: one
// wrapping ErrBlobKeyMissing, a *BlobCorruptError (ErrBlobCorrupt), or an I/O
// or key-source failure that is neither. They name the payload file, the
// epoch and the chunk; the caller, which knows the database, the bucket and
// the user's key, completes the message. Comparing the content with the
// metadata's checksum is the caller's last step, after this returns.
//
// Beyond the returned content it holds one sealed chunk at a time.
func OpenBlobFile(r io.Reader, req OpenBlobRequest) ([]byte, error) {
	if req.Size < 1 {
		return nil, &BlobCorruptError{PayloadName: req.PayloadName, Reason: BlobCorruptSizeZero, Size: req.Size}
	}
	if wantFileLen := blobFileLen(req.Size); req.FileLen != wantFileLen {
		return nil, &BlobCorruptError{
			PayloadName: req.PayloadName, Reason: BlobCorruptWrongLength,
			Size: req.Size, FileLen: req.FileLen, WantFileLen: wantFileLen,
		}
	}
	header := make([]byte, FileHeaderLen)
	if err := readPayload(r, header, req.PayloadName, req.Size); err != nil {
		return nil, err
	}
	if problem := blobHeaderProblem(header); problem != "" {
		return nil, &BlobCorruptError{PayloadName: req.PayloadName, Reason: BlobCorruptNoSealedHeader, Size: req.Size, HeaderProblem: problem}
	}
	epoch := binary.BigEndian.Uint64(header[8:16])
	fileKey, err := req.Keys.FileKey(epoch, req.PayloadName)
	if errors.Is(err, ErrBlobKeyMissing) {
		return nil, &BlobKeyMissingError{PayloadName: req.PayloadName, Epoch: epoch}
	}
	if err != nil {
		return nil, fmt.Errorf("opening payload %s, sealed under blob key epoch %d: %w", req.PayloadName, epoch, err)
	}
	content, err := openBlobChunks(r, blobChunkRun{
		payloadName: req.PayloadName, epoch: epoch, fileKey: fileKey, header: header,
		size: req.Size, chunkCount: blobChunkCount(req.Size),
	})
	if err != nil {
		return nil, err
	}
	// FileLen was a claim until now: a reader holding more than it declared is
	// not the file the length check passed.
	var trailing [1]byte
	if _, err := io.ReadFull(r, trailing[:]); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("reading the end of payload %s: %w", req.PayloadName, err)
		}
		return nil, blobReaderDisagrees(req.PayloadName, req.Size)
	}
	return content, nil
}
