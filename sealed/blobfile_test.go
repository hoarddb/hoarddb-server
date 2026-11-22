package sealed

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
)

// blobTestPayloadName is a payload file name of the shape
// store.PayloadFileName writes: slot, bucket id, blob id.
const blobTestPayloadName = "2a7-0123456789abcdef-01890a5d-ac96-774b-bcce-b302099a8057"

// blobTestKeySource is a BlobKeySource over epoch keys held in a map. With
// acceptAnyEpoch it serves the current epoch's key under every epoch id, which
// is what isolates the header's place in the AAD: with distinct keys an edited
// epoch id fails at the key, whether or not the header is authenticated.
type blobTestKeySource struct {
	currentEpoch   uint64
	epochKeys      map[uint64][]byte
	acceptAnyEpoch bool
}

func (source *blobTestKeySource) CurrentEpoch() uint64 { return source.currentEpoch }

func (source *blobTestKeySource) FileKey(epoch uint64, payloadName string) ([]byte, error) {
	if source.acceptAnyEpoch {
		epoch = source.currentEpoch
	}
	epochKey, held := source.epochKeys[epoch]
	if !held {
		return nil, fmt.Errorf("blob key epoch %d: %w", epoch, ErrBlobKeyMissing)
	}
	return FileKey(epochKey, PurposeBlobChunk, payloadName)
}

// blobTestEpochKey is a 32-byte key whose every byte is fill, so a test can
// search an error text for it.
func blobTestEpochKey(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, 32)
}

func newBlobTestKeySource() *blobTestKeySource {
	return &blobTestKeySource{
		currentEpoch: 1,
		epochKeys:    map[uint64][]byte{1: blobTestEpochKey(0x11), 2: blobTestEpochKey(0x22)},
	}
}

// blobTestContent is size bytes that differ from chunk to chunk, so a chunk
// opened in another's place is visible as wrong content and not only as a
// failed tag.
func blobTestContent(size int) []byte {
	content := make([]byte, size)
	for offset := range content {
		content[offset] = byte(offset%251) ^ byte(offset/BlobChunkSize)
	}
	return content
}

// corruptReason returns the reason of the *BlobCorruptError in err, failing
// the test when err is not one: a refusal for any other cause proves nothing
// about the check under test.
func corruptReason(t *testing.T, err error) *BlobCorruptError {
	t.Helper()
	if !errors.Is(err, ErrBlobCorrupt) {
		t.Fatalf("want an error wrapping ErrBlobCorrupt, got: %v", err)
	}
	var corrupt *BlobCorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("want a *BlobCorruptError, got %T: %v", err, err)
	}
	return corrupt
}

// TestBlobFileOverheadMatchesTheFormula pins 24 + size + 28·n (spec §4.2.1 b,
// j) at the figures the spec publishes, and the chunk count at each boundary.
func TestBlobFileOverheadMatchesTheFormula(t *testing.T) {
	overheads := []struct {
		size         int64
		wantChunks   int64
		wantOverhead int64
	}{
		{1, 1, 52},
		{65_535, 1, 52},
		{65_536, 1, 52},
		{65_537, 2, 80},
		{131_072, 2, 80},
		{131_073, 3, 108},
		{16 << 20, 256, 7_192},
		{32 << 20, 512, 14_360},
	}
	for _, overhead := range overheads {
		if chunks := blobChunkCount(overhead.size); chunks != overhead.wantChunks {
			t.Errorf("size %d: %d chunks, want %d", overhead.size, chunks, overhead.wantChunks)
		}
		fileLen, err := BlobFileLen(overhead.size)
		if err != nil {
			t.Errorf("BlobFileLen(%d): %v", overhead.size, err)
			continue
		}
		if fileLen-overhead.size != overhead.wantOverhead {
			t.Errorf("size %d: the sealed file is %d bytes, %d of overhead; want %d of overhead",
				overhead.size, fileLen, fileLen-overhead.size, overhead.wantOverhead)
		}
	}
	for _, size := range []int64{0, -1} {
		if fileLen, err := BlobFileLen(size); err == nil {
			t.Errorf("BlobFileLen(%d) = %d with no error; no sealed file of that size exists", size, fileLen)
		}
	}
}

// TestBlobSealRefusesZeroBytesAndWritesNothing is spec §4.2.1 c's writer rule:
// sealing zero bytes would produce a header with no tag, which nothing
// authenticates.
func TestBlobSealRefusesZeroBytesAndWritesNothing(t *testing.T) {
	for _, content := range [][]byte{nil, {}} {
		var written bytes.Buffer
		err := SealBlobFile(&written, SealBlobRequest{Keys: newBlobTestKeySource(), PayloadName: blobTestPayloadName, Content: content})
		if !errors.Is(err, ErrBlobEmpty) {
			t.Errorf("sealing %d bytes: want ErrBlobEmpty, got: %v", len(content), err)
		}
		if written.Len() != 0 {
			t.Errorf("sealing %d bytes wrote %d bytes; a refused seal writes nothing", len(content), written.Len())
		}
	}
}

// TestBlobOpenTreatsSizeZeroAsCorruption is spec §4.2.1 c's reader rule, and
// step 1 of §4.2.1 d: a file-backed record of size 0 is corruption whatever
// the file holds — here a bare, well-formed header, which is the file a reader
// that inferred the count from the length would open as an empty blob.
func TestBlobOpenTreatsSizeZeroAsCorruption(t *testing.T) {
	bareHeader := MarshalFileHeader(&FileHeader{Magic: Magic, Version: FileVersion, Purpose: PurposeBlobChunk, EpochID: 1})
	for _, size := range []int64{0, -1} {
		content, err := OpenBlobFile(bytes.NewReader(bareHeader), OpenBlobRequest{
			Keys: newBlobTestKeySource(), PayloadName: blobTestPayloadName, Size: size, FileLen: int64(len(bareHeader)),
		})
		if content != nil {
			t.Errorf("size %d: OpenBlobFile returned %d bytes of content; a refusal returns nil", size, len(content))
		}
		if err == nil {
			t.Errorf("size %d: a bare header opened as a blob; want corruption", size)
			continue
		}
		if corrupt := corruptReason(t, err); corrupt.Reason != BlobCorruptSizeZero {
			t.Errorf("size %d: refused for reason %d (%v); want BlobCorruptSizeZero", size, corrupt.Reason, err)
		}
	}
}

// sealBlobTestFile seals content under name and returns the file's bytes.
func sealBlobTestFile(t *testing.T, keys BlobKeySource, name string, content []byte) []byte {
	t.Helper()
	var file bytes.Buffer
	if err := SealBlobFile(&file, SealBlobRequest{Keys: keys, PayloadName: name, Content: content}); err != nil {
		t.Fatalf("sealing %d bytes as %s: %v", len(content), name, err)
	}
	return file.Bytes()
}

// openBlobTestFile opens file as the payload of a blob of size bytes, with the
// file's true length.
func openBlobTestFile(file []byte, keys BlobKeySource, name string, size int64) ([]byte, error) {
	return OpenBlobFile(bytes.NewReader(file), OpenBlobRequest{Keys: keys, PayloadName: name, Size: size, FileLen: int64(len(file))})
}

// blobTestChunk returns the bounds of sealed chunk index within a file whose
// chunks before it are all full.
func blobTestChunk(file []byte, index int) (start, end int) {
	start = FileHeaderLen + index*(BlobChunkSize+blobChunkOverhead)
	end = min(start+BlobChunkSize+blobChunkOverhead, len(file))
	return start, end
}

// mustRefuse asserts that an open returned no content and a corruption error,
// and returns the error's detail.
func mustRefuse(t *testing.T, content []byte, err error) *BlobCorruptError {
	t.Helper()
	if content != nil {
		t.Fatalf("the open returned %d bytes of content; a refused payload returns none", len(content))
	}
	if err == nil {
		t.Fatal("the payload opened; want a refusal")
	}
	return corruptReason(t, err)
}

// threeChunkSize is a content length of two full chunks and a short third.
const threeChunkSize = 2*BlobChunkSize + 100

// TestBlobFileRoundTripAtEveryChunkBoundary seals and opens at each boundary
// of the geometry, up to the largest blob a frame can carry, and asserts the
// file's length and chunk count each time.
func TestBlobFileRoundTripAtEveryChunkBoundary(t *testing.T) {
	keys := newBlobTestKeySource()
	roundTrips := []struct {
		size       int
		wantChunks int64
	}{
		{1, 1}, {65_535, 1}, {65_536, 1}, {65_537, 2}, {131_072, 2}, {131_073, 3}, {16 << 20, 256}, {32 << 20, 512},
	}
	for _, roundTrip := range roundTrips {
		t.Run(fmt.Sprintf("%d bytes", roundTrip.size), func(t *testing.T) {
			content := blobTestContent(roundTrip.size)
			file := sealBlobTestFile(t, keys, blobTestPayloadName, content)
			wantLen := int64(FileHeaderLen) + int64(roundTrip.size) + blobChunkOverhead*roundTrip.wantChunks
			if int64(len(file)) != wantLen {
				t.Fatalf("the sealed file is %d bytes, want 24 + %d + 28·%d = %d", len(file), roundTrip.size, roundTrip.wantChunks, wantLen)
			}
			if chunks := blobChunkCount(int64(roundTrip.size)); chunks != roundTrip.wantChunks {
				t.Fatalf("%d chunks, want %d", chunks, roundTrip.wantChunks)
			}
			if bytes.Contains(file, content[:min(len(content), 32)]) && len(content) >= 32 {
				t.Fatal("the sealed file holds the content's first 32 bytes in the clear")
			}
			opened, err := openBlobTestFile(file, keys, blobTestPayloadName, int64(roundTrip.size))
			if err != nil {
				t.Fatalf("opening: %v", err)
			}
			if !bytes.Equal(opened, content) {
				t.Fatal("the opened content differs from what was sealed")
			}
		})
	}
}

// TestBlobChunkReorderRefused swaps two full chunks. Only the chunk index in
// the AAD refuses this: both chunks are sealed under the same key, header and
// count.
func TestBlobChunkReorderRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	firstStart, firstEnd := blobTestChunk(file, 0)
	secondStart, secondEnd := blobTestChunk(file, 1)
	reordered := append([]byte(nil), file...)
	copy(reordered[firstStart:firstEnd], file[secondStart:secondEnd])
	copy(reordered[secondStart:secondEnd], file[firstStart:firstEnd])

	content, err := openBlobTestFile(reordered, keys, blobTestPayloadName, threeChunkSize)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 || corrupt.Chunks != 3 {
		t.Fatalf("refused as reason %d at chunk %d of %d; want an authentication failure at chunk 0 of 3: %v", corrupt.Reason, corrupt.Chunk, corrupt.Chunks, err)
	}
}

// TestBlobChunkCountIsBoundInTheAAD drops the last of three chunks and opens
// what is left with the count a reader inferring it from the length would use,
// two, WITHOUT the length check in front. The remaining chunks were sealed
// under a count of three, so they must not open. Going through OpenBlobFile
// instead would prove nothing: its length check refuses this file whether or
// not the count is in the AAD.
func TestBlobChunkCountIsBoundInTheAAD(t *testing.T) {
	keys := newBlobTestKeySource()
	sealedContent := blobTestContent(threeChunkSize)
	file := sealBlobTestFile(t, keys, blobTestPayloadName, sealedContent)
	fileKey, err := keys.FileKey(1, blobTestPayloadName)
	if err != nil {
		t.Fatalf("deriving the file key: %v", err)
	}
	header := file[:FileHeaderLen]
	run := blobChunkRun{payloadName: blobTestPayloadName, epoch: 1, fileKey: fileKey, header: header, size: threeChunkSize, chunkCount: 3}

	// The control: the same call with the true count opens, so a refusal below
	// is the count's doing and not a mistake in how this test calls the opener.
	whole, err := openBlobChunks(bytes.NewReader(file[FileHeaderLen:]), run)
	if err != nil || !bytes.Equal(whole, sealedContent) {
		t.Fatalf("the untouched chunks did not open under their own count: %v", err)
	}

	_, lastStart := blobTestChunk(file, 1)
	run.size, run.chunkCount = 2*BlobChunkSize, 2
	content, err := openBlobChunks(bytes.NewReader(file[FileHeaderLen:lastStart]), run)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 {
		t.Fatalf("refused as reason %d at chunk %d; want an authentication failure at chunk 0: %v", corrupt.Reason, corrupt.Chunk, err)
	}
}

// readCounter counts the bytes read through it.
type readCounter struct {
	source    io.Reader
	bytesRead int
}

func (counter *readCounter) Read(buffer []byte) (int, error) {
	n, err := counter.source.Read(buffer)
	counter.bytesRead += n
	return n, err
}

// TestBlobChunkDroppedFromTheEndRefusedByTheLengthCheck is the same cut seen
// through OpenBlobFile with the honest size: the length check refuses it, and
// does so before a single byte of the file is read, so before any AEAD runs.
func TestBlobChunkDroppedFromTheEndRefusedByTheLengthCheck(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	_, lastStart := blobTestChunk(file, 1)
	truncated := &readCounter{source: bytes.NewReader(file[:lastStart])}

	content, err := OpenBlobFile(truncated, OpenBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Size: threeChunkSize, FileLen: int64(lastStart)})
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptWrongLength || corrupt.FileLen != int64(lastStart) || corrupt.WantFileLen != int64(len(file)) {
		t.Fatalf("refused as reason %d (holds %d, wants %d); want a wrong length of %d against %d: %v",
			corrupt.Reason, corrupt.FileLen, corrupt.WantFileLen, lastStart, len(file), err)
	}
	if truncated.bytesRead != 0 {
		t.Fatalf("%d bytes were read before the length was refused; the length check runs first", truncated.bytesRead)
	}
}

// TestBlobTruncationCoordinatedWithAForgedSizeRefused is the cut the length
// check cannot see: the file ends at a chunk boundary and the metadata's size
// — an unauthenticated store value — is edited to match. The length then
// agrees, and only the count in the AAD is left to refuse it, at chunk 0.
func TestBlobTruncationCoordinatedWithAForgedSizeRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	_, lastStart := blobTestChunk(file, 1)

	content, err := openBlobTestFile(file[:lastStart], keys, blobTestPayloadName, 2*BlobChunkSize)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 || corrupt.Chunks != 2 {
		t.Fatalf("refused as reason %d at chunk %d of %d; want an authentication failure at chunk 0 of 2: %v", corrupt.Reason, corrupt.Chunk, corrupt.Chunks, err)
	}
}

// TestBlobFileTruncatedToTheBareHeaderRefused is the review's B1 attack: a
// payload cut back to its 24-byte header has no chunk, so a reader that
// inferred the count from the length would evaluate no AEAD and return an
// empty blob under a header nobody authenticated.
func TestBlobFileTruncatedToTheBareHeaderRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	bareHeader := file[:FileHeaderLen]

	// The honest size is refused by the length check, before a byte is read:
	// without that check the file would still be refused further on, when its
	// first chunk could not be read, and this leg could not tell the two apart.
	counted := &readCounter{source: bytes.NewReader(bareHeader)}
	content, err := OpenBlobFile(counted, OpenBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Size: threeChunkSize, FileLen: FileHeaderLen})
	if corrupt := mustRefuse(t, content, err); corrupt.Reason != BlobCorruptWrongLength || corrupt.FileLen != FileHeaderLen {
		t.Fatalf("with the honest size: refused as reason %d; want a wrong length of 24 bytes: %v", corrupt.Reason, err)
	}
	if counted.bytesRead != 0 {
		t.Fatalf("with the honest size: %d bytes were read before the length was refused", counted.bytesRead)
	}
	content, err = openBlobTestFile(bareHeader, keys, blobTestPayloadName, 0)
	if corrupt := mustRefuse(t, content, err); corrupt.Reason != BlobCorruptSizeZero {
		t.Fatalf("with the size forged to 0: refused as reason %d; want size zero: %v", corrupt.Reason, err)
	}
}

// blobTestOtherPayloadName is a second blob in the same bucket.
const blobTestOtherPayloadName = "2a7-0123456789abcdef-01890a5d-ac96-774b-bcce-b302099a8058"

// TestBlobChunkMovedToAnotherBlobRefused splices chunk 1 of blob B into blob A
// at the same position; the two are the same size, sealed under one epoch.
func TestBlobChunkMovedToAnotherBlobRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	fileA := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	fileB := sealBlobTestFile(t, keys, blobTestOtherPayloadName, blobTestContent(threeChunkSize))
	start, end := blobTestChunk(fileA, 1)
	spliced := append([]byte(nil), fileA...)
	copy(spliced[start:end], fileB[start:end])

	content, err := openBlobTestFile(spliced, keys, blobTestPayloadName, threeChunkSize)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 1 {
		t.Fatalf("refused as reason %d at chunk %d; want an authentication failure at chunk 1: %v", corrupt.Reason, corrupt.Chunk, err)
	}
}

// TestBlobFileOpenedUnderAnotherPayloadNameRefused opens blob B's whole file
// as blob A: what a metadata Path edited to name another blob's file amounts
// to, once the reader recomputes the name instead of trusting Path.
func TestBlobFileOpenedUnderAnotherPayloadNameRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	fileB := sealBlobTestFile(t, keys, blobTestOtherPayloadName, blobTestContent(threeChunkSize))

	content, err := openBlobTestFile(fileB, keys, blobTestPayloadName, threeChunkSize)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 {
		t.Fatalf("refused as reason %d at chunk %d; want an authentication failure at chunk 0: %v", corrupt.Reason, corrupt.Chunk, err)
	}
}

// withBlobEpoch returns file with its header's epoch id replaced.
func withBlobEpoch(file []byte, epoch uint64) []byte {
	edited := append([]byte(nil), file...)
	binary.BigEndian.PutUint64(edited[8:16], epoch)
	return edited
}

// TestBlobEpochSwapRefused edits the header's epoch id from 1 to 2. The first
// leg uses a key source that serves the SAME key under both epochs, so the
// only thing that changed is the header inside the AAD: with distinct keys the
// swap fails at the key whether or not the header is authenticated, and the
// test could not see the header leave the AAD.
func TestBlobEpochSwapRefused(t *testing.T) {
	t.Run("the same key under both epochs", func(t *testing.T) {
		keys := newBlobTestKeySource()
		keys.acceptAnyEpoch = true
		file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
		if _, err := openBlobTestFile(file, keys, blobTestPayloadName, threeChunkSize); err != nil {
			t.Fatalf("the untouched file did not open: %v", err)
		}

		content, err := openBlobTestFile(withBlobEpoch(file, 2), keys, blobTestPayloadName, threeChunkSize)
		corrupt := mustRefuse(t, content, err)
		if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 || corrupt.Epoch != 2 {
			t.Fatalf("refused as reason %d at chunk %d under epoch %d; want an authentication failure at chunk 0 under epoch 2: %v",
				corrupt.Reason, corrupt.Chunk, corrupt.Epoch, err)
		}
	})
	t.Run("a different key under the other epoch", func(t *testing.T) {
		keys := newBlobTestKeySource()
		file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
		content, err := openBlobTestFile(withBlobEpoch(file, 2), keys, blobTestPayloadName, threeChunkSize)
		if corrupt := mustRefuse(t, content, err); corrupt.Reason != BlobCorruptChunkAuthentication {
			t.Fatalf("refused as reason %d; want an authentication failure: %v", corrupt.Reason, err)
		}
	})
	t.Run("an epoch the registry does not hold", func(t *testing.T) {
		keys := newBlobTestKeySource()
		file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
		content, err := openBlobTestFile(withBlobEpoch(file, 9), keys, blobTestPayloadName, threeChunkSize)
		if content != nil {
			t.Fatalf("the open returned %d bytes of content", len(content))
		}
		if !errors.Is(err, ErrBlobKeyMissing) {
			t.Fatalf("want ErrBlobKeyMissing for epoch 9, got: %v", err)
		}
		if errors.Is(err, ErrBlobCorrupt) {
			t.Fatalf("a missing epoch is a key problem, not corruption; got: %v", err)
		}
	})
}

// TestBlobHeaderValidatedByTheCodecItself edits one header field at a time.
// ParseFileHeader accepts a WAL's purpose, the retired 0x05 and any gen, so
// the codec checks them itself; each refusal must name the header and not a
// chunk, which is what shows the explicit check ran instead of the AEAD
// failing on the edited AAD. The key source serves every epoch, so nothing
// here can be refused for a missing key.
func TestBlobHeaderValidatedByTheCodecItself(t *testing.T) {
	keys := newBlobTestKeySource()
	keys.acceptAnyEpoch = true
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	headerEdits := []struct {
		name   string
		offset int
		value  byte
	}{
		{"magic", 1, 'X'},
		{"version 0x01", 4, 0x01},
		{"purpose 0x04, a WAL's", 5, PurposeWALEntry},
		{"purpose 0x05, retired", 5, PurposeSegmentEntry},
		{"flags 0x01", 6, 0x01},
		{"reserved 0x01", 7, 0x01},
		{"gen 77", 23, 77},
	}
	for _, headerEdit := range headerEdits {
		t.Run(headerEdit.name, func(t *testing.T) {
			edited := append([]byte(nil), file...)
			edited[headerEdit.offset] = headerEdit.value
			content, err := openBlobTestFile(edited, keys, blobTestPayloadName, threeChunkSize)
			corrupt := mustRefuse(t, content, err)
			if corrupt.Reason != BlobCorruptNoSealedHeader {
				t.Fatalf("refused as reason %d; want the header's own check: %v", corrupt.Reason, err)
			}
			if !strings.Contains(err.Error(), "no valid sealed header") || strings.Contains(err.Error(), "chunk") {
				t.Fatalf("the refusal must name the header and no chunk, got: %v", err)
			}
		})
	}
}

// TestBlobPlaintextFileOnASealedNodeRefused is spec §4.2.1 e on a sealed node:
// a payload that is not a sealed file is corruption, and its bytes are never
// returned as content.
func TestBlobPlaintextFileOnASealedNodeRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	const size = threeChunkSize
	sealedLen, err := BlobFileLen(size)
	if err != nil {
		t.Fatalf("BlobFileLen: %v", err)
	}

	t.Run("a plaintext file of exactly the sealed length", func(t *testing.T) {
		content, err := openBlobTestFile(blobTestContent(int(sealedLen)), keys, blobTestPayloadName, size)
		corrupt := mustRefuse(t, content, err)
		if corrupt.Reason != BlobCorruptNoSealedHeader || !strings.Contains(err.Error(), "no valid sealed header") {
			t.Fatalf("refused as reason %d; want no valid sealed header: %v", corrupt.Reason, err)
		}
	})
	t.Run("the plaintext file a put would have written", func(t *testing.T) {
		content, err := openBlobTestFile(blobTestContent(size), keys, blobTestPayloadName, size)
		corrupt := mustRefuse(t, content, err)
		if corrupt.Reason != BlobCorruptWrongLength {
			t.Fatalf("refused as reason %d; want a wrong length: %v", corrupt.Reason, err)
		}
	})
	t.Run("a valid-looking header over plaintext", func(t *testing.T) {
		file := blobTestContent(int(sealedLen))
		copy(file, MarshalFileHeader(&FileHeader{Magic: Magic, Version: FileVersion, Purpose: PurposeBlobChunk, EpochID: 1}))
		content, err := openBlobTestFile(file, keys, blobTestPayloadName, size)
		corrupt := mustRefuse(t, content, err)
		if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 0 {
			t.Fatalf("refused as reason %d at chunk %d; want an authentication failure at chunk 0: %v", corrupt.Reason, corrupt.Chunk, err)
		}
	})
}

// TestBlobNoPartialPlaintextOnALaterChunkFailure damages the last chunk of
// three. The first two open; none of their plaintext may be returned.
func TestBlobNoPartialPlaintextOnALaterChunkFailure(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	damaged := append([]byte(nil), file...)
	damaged[len(damaged)-FileTagLen-1] ^= 0x01

	content, err := openBlobTestFile(damaged, keys, blobTestPayloadName, threeChunkSize)
	corrupt := mustRefuse(t, content, err)
	if corrupt.Reason != BlobCorruptChunkAuthentication || corrupt.Chunk != 2 || corrupt.Chunks != 3 {
		t.Fatalf("refused as reason %d at chunk %d of %d; want an authentication failure at chunk 2 of 3: %v", corrupt.Reason, corrupt.Chunk, corrupt.Chunks, err)
	}
}

// TestBlobReaderThatDisagreesWithItsDeclaredLengthRefused covers a caller
// whose FileLen is right for the size while the reader holds fewer or more
// bytes: the declared length is a claim until the reader has been drained.
func TestBlobReaderThatDisagreesWithItsDeclaredLengthRefused(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))
	readers := map[string][]byte{
		"ends early":     file[:len(file)-1],
		"has extra data": append(append([]byte(nil), file...), 0x00),
	}
	for name, held := range readers {
		t.Run(name, func(t *testing.T) {
			content, err := OpenBlobFile(bytes.NewReader(held), OpenBlobRequest{
				Keys: keys, PayloadName: blobTestPayloadName, Size: threeChunkSize, FileLen: int64(len(file)),
			})
			if corrupt := mustRefuse(t, content, err); corrupt.Reason != BlobCorruptWrongLength {
				t.Fatalf("refused as reason %d; want a wrong length: %v", corrupt.Reason, err)
			}
		})
	}
}

// TestBlobSealsOfTheSameContentDiffer guards the nonce: it is fresh random per
// chunk, never derived from position and never left fixed.
func TestBlobSealsOfTheSameContentDiffer(t *testing.T) {
	keys := newBlobTestKeySource()
	content := blobTestContent(threeChunkSize)
	first := sealBlobTestFile(t, keys, blobTestPayloadName, content)
	second := sealBlobTestFile(t, keys, blobTestPayloadName, content)

	seenNonces := make(map[string]bool)
	for _, file := range [][]byte{first, second} {
		for index := range 3 {
			start, _ := blobTestChunk(file, index)
			nonce := string(file[start : start+FileNonceLen])
			if seenNonces[nonce] {
				t.Fatalf("nonce %x is used twice across two seals of one content", nonce)
			}
			seenNonces[nonce] = true
		}
	}
	if bytes.Equal(first, second) {
		t.Fatal("two seals of the same content are byte-identical")
	}
}

// TestBlobSealRefusesPastTheLimitBeforeWritingAnything asserts spec §2.2's
// bound for a blob file key. The largest blob a frame carries is 512 chunks
// against a limit of 2^31, so the refusal is reachable only with the limit
// lowered; the count is known up front, so the refusal comes before the first
// byte and there is no partial file to remove.
func TestBlobSealRefusesPastTheLimitBeforeWritingAnything(t *testing.T) {
	if blobSealLimit != 1<<31 {
		t.Fatalf("the seal limit is %d, want 2^31", blobSealLimit)
	}
	if frameChunks := blobChunkCount(32 << 20); frameChunks >= blobSealLimit {
		t.Fatalf("a 32 MiB blob is %d chunks, not below the seal limit %d", frameChunks, blobSealLimit)
	}
	blobSealLimit = 8
	t.Cleanup(func() { blobSealLimit = 1 << 31 })
	keys := newBlobTestKeySource()

	var atTheLimit bytes.Buffer
	if err := SealBlobFile(&atTheLimit, SealBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Content: blobTestContent(8 * BlobChunkSize)}); err != nil {
		t.Fatalf("8 chunks at a limit of 8 must seal: %v", err)
	}

	var pastTheLimit bytes.Buffer
	err := SealBlobFile(&pastTheLimit, SealBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Content: blobTestContent(8*BlobChunkSize + 1)})
	if !errors.Is(err, ErrBlobSealLimit) {
		t.Fatalf("9 chunks at a limit of 8: want ErrBlobSealLimit, got: %v", err)
	}
	if !strings.Contains(err.Error(), "9 chunks") || !strings.Contains(err.Error(), "limit of 8") {
		t.Errorf("the refusal must name the chunk count and the limit, got: %v", err)
	}
	if pastTheLimit.Len() != 0 {
		t.Fatalf("the refused seal wrote %d bytes; it must write none", pastTheLimit.Len())
	}
}

// patternReader yields the byte stream whose byte k is (start + k) mod 256.
type patternReader struct {
	next byte
}

func (pattern *patternReader) Read(buffer []byte) (int, error) {
	for offset := range buffer {
		buffer[offset] = pattern.next
		pattern.next++
	}
	return len(buffer), nil
}

// TestBlobFrozenVectorMatchesAnIndependentImplementation pins the exact bytes
// of three sealed files — one full chunk, a one-byte last chunk, and an exact
// multiple — for a known epoch key, payload name, nonce stream and content.
// The expected values were NOT produced by this package: they are the output
// of testdata/blob-seal-vector.py (testdata/blob-seal-vector.txt), which
// derives the key and seals with the Python `cryptography` package from the
// spec's description alone. So this test fails if the header's layout, the
// derivation's info string, the index's base or width, the count, the byte
// order of either, or the position of nonce and tag ever move — every one of
// which round-trips cleanly inside this package. The vector is frozen:
// regenerating the expected values from this code defeats the test.
func TestBlobFrozenVectorMatchesAnIndependentImplementation(t *testing.T) {
	epochKey := mustDecodeHex(t, "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	const epochID = 258
	const wantHeader = "004844420206000000000000000001020000000000000000"
	const firstNonce = "a0a1a2a3a4a5a6a7a8a9aaab"
	const secondNonce = "acadaeafb0b1b2b3b4b5b6b7"
	vectors := []struct {
		size          int
		wantFileLen   int
		wantSHA256    string
		wantFirstTag  string
		wantLastNonce string
		wantLastTag   string
	}{
		{65_536, 65_588, "afda251219ddfba8ca45c17b110a1abc07358b161d370a4aae25efac4b39629c",
			"fd29815283595a72c1e9adb60d19245d", firstNonce, "fd29815283595a72c1e9adb60d19245d"},
		{65_537, 65_617, "3ed5f7e860ea49ad982bf1884d1cb62203e5d103473521aa9642757df7f3520f",
			"caac9a45244a1b8c79fdcd933b5e4ebb", secondNonce, "b1c4b10be59c1329ec0b5bcbad212211"},
		{131_072, 131_152, "d5fc18915e59d854ac2f51cd0e5d84f0d35c130bac18ac70a346c30512fab1c9",
			"caac9a45244a1b8c79fdcd933b5e4ebb", secondNonce, "b9b1cb56221973c494f7c75349a62d26"},
	}
	keys := &blobTestKeySource{currentEpoch: epochID, epochKeys: map[uint64][]byte{epochID: epochKey}}
	t.Cleanup(func() { blobNonceSource = rand.Reader })

	for _, vector := range vectors {
		t.Run(fmt.Sprintf("%d bytes", vector.size), func(t *testing.T) {
			content := make([]byte, vector.size)
			for offset := range content {
				content[offset] = byte(offset % 251)
			}
			blobNonceSource = &patternReader{next: 0xA0}
			file := sealBlobTestFile(t, keys, blobTestPayloadName, content)

			if len(file) != vector.wantFileLen {
				t.Fatalf("the sealed file is %d bytes, want %d", len(file), vector.wantFileLen)
			}
			lastStart := len(file) - (vector.size-1)%BlobChunkSize - 1 - blobChunkOverhead
			pinned := []struct {
				what      string
				got, want string
			}{
				{"header", hex.EncodeToString(file[:FileHeaderLen]), wantHeader},
				{"first chunk's nonce", hex.EncodeToString(file[FileHeaderLen : FileHeaderLen+FileNonceLen]), firstNonce},
				{"first chunk's tag", hex.EncodeToString(file[min(len(file), FileHeaderLen+blobChunkOverhead+BlobChunkSize)-FileTagLen:][:FileTagLen]), vector.wantFirstTag},
				{"last chunk's nonce", hex.EncodeToString(file[lastStart : lastStart+FileNonceLen]), vector.wantLastNonce},
				{"last chunk's tag", hex.EncodeToString(file[len(file)-FileTagLen:]), vector.wantLastTag},
			}
			for _, pin := range pinned {
				if pin.got != pin.want {
					t.Errorf("the %s moved: the sealed blob format changed\n got: %s\nwant: %s", pin.what, pin.got, pin.want)
				}
			}
			fileHash := sha256.Sum256(file)
			if got := hex.EncodeToString(fileHash[:]); got != vector.wantSHA256 {
				t.Errorf("the sealed file's SHA-256 moved: the sealed blob format changed\n got: %s\nwant: %s", got, vector.wantSHA256)
			}

			opened, err := openBlobTestFile(file, keys, blobTestPayloadName, int64(vector.size))
			if err != nil || !bytes.Equal(opened, content) {
				t.Fatalf("the frozen file did not open to its content: %v", err)
			}
		})
	}
}

// TestBlobFailureKindsAreDistinguishable is spec §4.2.1 h at the codec: a
// missing epoch and a damaged payload are told apart in code, not by matching
// text, and each carries the fields the layer above completes its message
// from.
func TestBlobFailureKindsAreDistinguishable(t *testing.T) {
	keys := newBlobTestKeySource()
	file := sealBlobTestFile(t, keys, blobTestPayloadName, blobTestContent(threeChunkSize))

	_, err := openBlobTestFile(withBlobEpoch(file, 9), keys, blobTestPayloadName, threeChunkSize)
	var keyMissing *BlobKeyMissingError
	if !errors.As(err, &keyMissing) {
		t.Fatalf("an epoch the registry does not hold: want a *BlobKeyMissingError, got %T: %v", err, err)
	}
	if keyMissing.Epoch != 9 || keyMissing.PayloadName != blobTestPayloadName {
		t.Errorf("the error names payload %q and epoch %d; want %q and 9", keyMissing.PayloadName, keyMissing.Epoch, blobTestPayloadName)
	}
	if !errors.Is(err, ErrBlobKeyMissing) || errors.Is(err, ErrBlobCorrupt) {
		t.Errorf("a missing epoch must be ErrBlobKeyMissing and not ErrBlobCorrupt: %v", err)
	}
	if !strings.Contains(err.Error(), "epoch 9") || !strings.Contains(err.Error(), "intact") || !strings.Contains(err.Error(), "keys/blobs.wrapped") {
		t.Errorf("the text must name the epoch and the registry and say the payload is intact, got: %v", err)
	}

	damaged := append([]byte(nil), file...)
	damaged[FileHeaderLen+FileNonceLen] ^= 0x01
	_, err = openBlobTestFile(damaged, keys, blobTestPayloadName, threeChunkSize)
	corrupt := corruptReason(t, err)
	if errors.Is(err, ErrBlobKeyMissing) || errors.As(err, &keyMissing) {
		t.Errorf("a damaged chunk must not read as a missing key: %v", err)
	}
	if corrupt.PayloadName != blobTestPayloadName || corrupt.Epoch != 1 || corrupt.Chunk != 0 || corrupt.Chunks != 3 {
		t.Errorf("the error names payload %q, epoch %d, chunk %d of %d; want %q, 1, 0 of 3",
			corrupt.PayloadName, corrupt.Epoch, corrupt.Chunk, corrupt.Chunks, blobTestPayloadName)
	}
}

// failingReader fails every read, as a disk does.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("input/output error") }

// failingWriter fails every write.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("no space left on device") }

// shortKeySource serves a key of the wrong length, as a broken registry would.
type shortKeySource struct{ blobTestKeySource }

func (source *shortKeySource) FileKey(epoch uint64, payloadName string) ([]byte, error) {
	fileKey, err := source.blobTestKeySource.FileKey(epoch, payloadName)
	if err != nil {
		return nil, err
	}
	return fileKey[:16], nil
}

// TestBlobErrorsCarryNoKeyOrContentHash drives every failure path of the codec
// with a recognisable epoch key and a recognisable content, and searches each
// error's text for the epoch key, the file key, the content's marker and the
// content's SHA-256. Each path is first checked to have failed for the reason
// it is listed under, so a path that stopped failing cannot pass by silence.
func TestBlobErrorsCarryNoKeyOrContentHash(t *testing.T) {
	epochKey := []byte("EPOCH-KEY-SENTINEL-0123456789abc")
	keys := &blobTestKeySource{currentEpoch: 1, epochKeys: map[uint64][]byte{1: epochKey}}
	fileKey, err := keys.FileKey(1, blobTestPayloadName)
	if err != nil {
		t.Fatalf("deriving the file key: %v", err)
	}
	const marker = "PLAINTEXT-MARKER"
	content := bytes.Repeat([]byte(marker), threeChunkSize/len(marker)+1)[:threeChunkSize]
	contentHash := sha256.Sum256(content)
	file := sealBlobTestFile(t, keys, blobTestPayloadName, content)
	sealedLen := int64(len(file))

	damaged := append([]byte(nil), file...)
	damaged[len(damaged)-1] ^= 0x01
	plaintextOfSealedLen := bytes.Repeat([]byte(marker), len(file)/len(marker)+1)[:len(file)]
	shortKeys := &shortKeySource{blobTestKeySource: *keys}
	noCurrentEpoch := &blobTestKeySource{currentEpoch: 7, epochKeys: keys.epochKeys}

	open := func(held []byte, source BlobKeySource, size, fileLen int64) error {
		opened, err := OpenBlobFile(bytes.NewReader(held), OpenBlobRequest{Keys: source, PayloadName: blobTestPayloadName, Size: size, FileLen: fileLen})
		if opened != nil {
			t.Fatalf("a failing open returned %d bytes of content", len(opened))
		}
		return err
	}
	seal := func(w io.Writer, source BlobKeySource, sealed []byte) error {
		return SealBlobFile(w, SealBlobRequest{Keys: source, PayloadName: blobTestPayloadName, Content: sealed})
	}
	_, ioErr := OpenBlobFile(failingReader{}, OpenBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Size: threeChunkSize, FileLen: sealedLen})

	const corrupt, keyMissing, other = "corrupt", "key missing", "other"
	failures := []struct {
		path     string
		err      error
		wantKind string
	}{
		{"open: size zero", open(file, keys, 0, sealedLen), corrupt},
		{"open: wrong length", open(content, keys, threeChunkSize, threeChunkSize), corrupt},
		{"open: plaintext in place of a sealed file", open(plaintextOfSealedLen, keys, threeChunkSize, sealedLen), corrupt},
		{"open: a damaged chunk", open(damaged, keys, threeChunkSize, sealedLen), corrupt},
		{"open: the reader ends early", open(file[:len(file)-1], keys, threeChunkSize, sealedLen), corrupt},
		{"open: the reader holds more", open(append(append([]byte(nil), file...), marker...), keys, threeChunkSize, sealedLen), corrupt},
		{"open: an epoch not held", open(withBlobEpoch(file, 9), keys, threeChunkSize, sealedLen), keyMissing},
		{"open: a key of the wrong length", open(file, shortKeys, threeChunkSize, sealedLen), other},
		{"open: the read fails", ioErr, other},
		{"seal: zero bytes", seal(io.Discard, keys, nil), other},
		{"seal: the current epoch is not held", seal(io.Discard, noCurrentEpoch, content), keyMissing},
		{"seal: a key of the wrong length", seal(io.Discard, shortKeys, content), other},
		{"seal: the write fails", seal(failingWriter{}, keys, content), other},
	}
	forbidden := map[string]string{
		"the epoch key":                string(epochKey),
		"the epoch key in hex":         hex.EncodeToString(epochKey),
		"the file key":                 string(fileKey),
		"the file key in hex":          hex.EncodeToString(fileKey),
		"the content's marker":         marker,
		"the content's SHA-256":        string(contentHash[:]),
		"the content's SHA-256 in hex": hex.EncodeToString(contentHash[:]),
	}
	for _, failure := range failures {
		if failure.err == nil {
			t.Errorf("%s: no error; the path did not fail", failure.path)
			continue
		}
		var corruptErr *BlobCorruptError
		var keyMissingErr *BlobKeyMissingError
		isCorrupt := errors.As(failure.err, &corruptErr)
		isKeyMissing := errors.Is(failure.err, ErrBlobKeyMissing)
		kind := other
		switch {
		case isCorrupt:
			kind = corrupt
		case isKeyMissing:
			kind = keyMissing
		}
		if kind != failure.wantKind {
			t.Errorf("%s: failed as %q, want %q: %v", failure.path, kind, failure.wantKind, failure.err)
		}
		if strings.HasPrefix(failure.path, "open:") && isKeyMissing && !errors.As(failure.err, &keyMissingErr) {
			t.Errorf("%s: want a *BlobKeyMissingError, got %T", failure.path, failure.err)
		}
		// The offending text is not printed: a failure message is a log line too.
		for what, secret := range forbidden {
			if strings.Contains(failure.err.Error(), secret) {
				t.Errorf("%s: the error text contains %s", failure.path, what)
			}
		}
	}
}

// allocatedBy returns the bytes allocated while run executes.
func allocatedBy(run func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestBlobSealAndOpenHoldOneChunkBeyondTheContent bounds what the codec
// allocates for the largest blob a frame carries: sealing allocates about one
// chunk, and opening allocates the content once plus about one chunk. A sealed
// node serves many blobs at once, so a codec that built the whole sealed file
// in memory, or assembled the content twice, would double each one's cost.
func TestBlobSealAndOpenHoldOneChunkBeyondTheContent(t *testing.T) {
	const size = 32 << 20
	const slack = 1 << 20
	keys := newBlobTestKeySource()
	content := blobTestContent(size)
	file := sealBlobTestFile(t, keys, blobTestPayloadName, content)

	var sealErr error
	sealed := allocatedBy(func() {
		sealErr = SealBlobFile(io.Discard, SealBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Content: content})
	})
	if sealErr != nil {
		t.Fatalf("sealing: %v", sealErr)
	}
	if sealed > slack {
		t.Errorf("sealing %d bytes allocated %d; want under %d, about one chunk", size, sealed, slack)
	}

	var opened []byte
	var openErr error
	reader := bytes.NewReader(file)
	openAllocated := allocatedBy(func() {
		opened, openErr = OpenBlobFile(reader, OpenBlobRequest{Keys: keys, PayloadName: blobTestPayloadName, Size: size, FileLen: int64(len(file))})
	})
	if openErr != nil || len(opened) != size {
		t.Fatalf("opening: %d bytes, %v", len(opened), openErr)
	}
	if openAllocated > size+slack {
		t.Errorf("opening %d bytes allocated %d; want under %d, the content once and about one chunk", size, openAllocated, size+slack)
	}
}
