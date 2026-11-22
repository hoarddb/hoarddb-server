package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The sub-scan kinds of conventions section 6.1, all on the data channel.
const (
	// KindSlotList is SLOT_LIST (0x0E): one page request to one owner.
	KindSlotList byte = 0x0E
	// KindSlotListReply is SLOT_LIST_REPLY (0x0F).
	KindSlotListReply byte = 0x0F
	// KindSlotCount is SLOT_COUNT (0x10).
	KindSlotCount byte = 0x10
	// KindSlotCountReply is SLOT_COUNT_REPLY (0x11).
	KindSlotCountReply byte = 0x11
)

const (
	// maxSubScanBody is the frame ceiling of conventions section 6 (32 MiB); no body is
	// read past it, so a lying length header cannot make a decoder allocate more.
	maxSubScanBody = 32 << 20
	// slotMaskBytes is the 8192-slot bitmap of conventions section 6.3.
	slotMaskBytes = 1024
	// slotCount is the number of slots; a slot number is 0 to slotCount-1.
	slotCount = slotMaskBytes * 8
	// maxSlotFilterBytes bounds the filter document (build SPEC-GAP U32c-3).
	maxSlotFilterBytes = 1 << 20
	// slotFilterDepth is the nesting a filter document may use: an equality document of
	// scalar fields needs 1, the rest is headroom for a later operator.
	slotFilterDepth = 4
)

// SlotListRequest is the SLOT_LIST body (conventions section 6.3).
type SlotListRequest struct {
	// DBID and BucketID are the resolved ids. The wire carries them as BSON int64; the
	// bits are kept as they are (build SPEC-GAP U32c-2).
	DBID, BucketID uint64
	// Filter is an optional BSON equality document; nil means no filter.
	Filter []byte
	// Mask is the 1024-byte slot bitmap, bit s of byte s/8 being slot s.
	Mask []byte
	// ResumeSlot and ResumeID are the last emitted position; zero and empty on the first page.
	ResumeSlot uint16
	ResumeID   []byte
	// Want is this owner's record limit; MaxBytes is its byte share of the page.
	Want     int32
	MaxBytes int64
	// Fields is the projection a caller asked for. The body does not carry it yet, so
	// a non-nil value is refused rather than sent as every field (SPEC-GAP U87-5).
	Fields []uint16
}

// SlotListReply is the SLOT_LIST_REPLY body.
type SlotListReply struct {
	// Entries is the concatenation of SlotRecord frames in (slot, encID) order.
	Entries []byte
	// More is true when the owner stopped at Want or at its byte share.
	More bool
}

// SlotCountRequest is the SLOT_COUNT body.
type SlotCountRequest struct {
	// DBID and BucketID are the resolved ids, as in SlotListRequest.
	DBID, BucketID uint64
	// Filter is an optional BSON equality document; nil means no filter.
	Filter []byte
	// Mask is the 1024-byte slot bitmap.
	Mask []byte
}

// SlotCountReply is the SLOT_COUNT_REPLY body.
type SlotCountReply struct {
	// Count is the number of distinct live document ids that satisfy the filter.
	Count int64 `bson:"count"`
}

// SlotRecord is one entry of a SLOT_LIST_REPLY: [slot:2][uvarint idlen][encID][uvarint bodylen][body].
type SlotRecord struct {
	Slot  uint16
	EncID []byte
	Body  []byte
}

// The wire documents. Field order is the spec's order and is the canonical byte layout:
// a decoder accepts only a body that re-encodes to itself.
type slotListDoc struct {
	DBID       int64    `bson:"db_id"`
	BucketID   int64    `bson:"bucket_id"`
	Filter     bson.Raw `bson:"filter,omitempty"`
	Mask       []byte   `bson:"mask"`
	ResumeSlot int32    `bson:"resume_slot"`
	ResumeID   []byte   `bson:"resume_id"`
	Want       int32    `bson:"want"`
	MaxBytes   int64    `bson:"max_bytes"`
}

type slotListReplyDoc struct {
	Entries []byte `bson:"entries"`
	More    bool   `bson:"more"`
}

type slotCountDoc struct {
	DBID     int64    `bson:"db_id"`
	BucketID int64    `bson:"bucket_id"`
	Filter   bson.Raw `bson:"filter,omitempty"`
	Mask     []byte   `bson:"mask"`
}

func slotScanRefusal(format string, args ...any) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(format, args...) +
		" The message was dropped; a node that sends this has a bug, so report it with its version"}
}

func checkSlotMask(mask []byte) error {
	if len(mask) != slotMaskBytes {
		return slotScanRefusal("a sub-scan slot mask is exactly %d bytes (one bit per slot), got %d.", slotMaskBytes, len(mask))
	}
	return nil
}

func checkSlotFilter(filter []byte) error {
	if filter == nil {
		return nil
	}
	if _, err := bson.Validate(filter, bson.Limits{Depth: slotFilterDepth, Size: maxSlotFilterBytes}); err != nil {
		return slotScanRefusal("the sub-scan filter is not a valid BSON document of at most %d bytes: %v.", maxSlotFilterBytes, err)
	}
	return nil
}

func checkSlotListRequest(req SlotListRequest) error {
	if err := checkSlotMask(req.Mask); err != nil {
		return err
	}
	if err := checkSlotFilter(req.Filter); err != nil {
		return err
	}
	if req.ResumeSlot >= slotCount {
		return slotScanRefusal("resume_slot %d is past the last slot, %d.", req.ResumeSlot, slotCount-1)
	}
	if len(req.ResumeID) > maxEncodedID {
		return slotScanRefusal("resume_id is %d bytes and an encoded id is at most %d.", len(req.ResumeID), maxEncodedID)
	}
	if req.Want < 1 {
		return slotScanRefusal("want is %d and a page asks an owner for at least 1 record.", req.Want)
	}
	if req.MaxBytes < 1 {
		return slotScanRefusal("max_bytes is %d and an owner's byte share is at least 1.", req.MaxBytes)
	}
	if req.Fields != nil {
		return slotScanRefusal("fields is set, but the peer SLOT_LIST body does not carry fields yet (SPEC-GAP U87-5), so the owner would return every field. Leave fields unset until the scatter unit adds the member.")
	}
	return nil
}

func encodeSlotScanDoc(kind string, doc any) ([]byte, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding a %s: %w", kind, err)
	}
	if len(raw) > maxSubScanBody {
		return nil, slotScanRefusal("a %s would be %d bytes and a frame holds at most %d.", kind, len(raw), maxSubScanBody)
	}
	return raw, nil
}

// decodeSlotScanDoc unmarshals body into doc, then insists the body is exactly what the
// encoder would write for it: wrong field order, a missing or extra field, a widened
// integer and trailing bytes all fail that comparison, so an accepted body re-encodes
// identically.
func decodeSlotScanDoc(kind string, body []byte, doc any, canonical func() ([]byte, error)) error {
	if len(body) > maxSubScanBody {
		return slotScanRefusal("a %s is %d bytes and a frame holds at most %d.", kind, len(body), maxSubScanBody)
	}
	if err := bson.Unmarshal(body, doc); err != nil {
		return slotScanRefusal("a %s is not a valid BSON document of its kind: %v.", kind, err)
	}
	again, err := canonical()
	if err != nil {
		// Only a body whose embedded filter is malformed fails to re-encode.
		return slotScanRefusal("a %s holds an element that cannot be read back: %v.", kind, err)
	}
	if !bytes.Equal(again, body) {
		return slotScanRefusal("a %s does not have the layout of conventions section 6.3 (fields in order, none missing or extra, the stated integer widths).", kind)
	}
	return nil
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

func orNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// EncodeSlotList returns the SLOT_LIST body.
func EncodeSlotList(req SlotListRequest) ([]byte, error) {
	if err := checkSlotListRequest(req); err != nil {
		return nil, err
	}
	return encodeSlotScanDoc("SLOT_LIST", slotListDoc{
		DBID: int64(req.DBID), BucketID: int64(req.BucketID), Filter: req.Filter, Mask: req.Mask,
		ResumeSlot: int32(req.ResumeSlot), ResumeID: nonNil(req.ResumeID), Want: req.Want, MaxBytes: req.MaxBytes,
	})
}

// DecodeSlotList reads a SLOT_LIST body.
func DecodeSlotList(body []byte) (SlotListRequest, error) {
	var doc slotListDoc
	canonical := func() ([]byte, error) { return bson.Marshal(doc) }
	if err := decodeSlotScanDoc("SLOT_LIST", body, &doc, canonical); err != nil {
		return SlotListRequest{}, err
	}
	if doc.ResumeSlot < 0 || doc.ResumeSlot >= slotCount {
		// Checked before the uint16 conversion, which would wrap a value like 65536 to a legal slot.
		return SlotListRequest{}, slotScanRefusal("resume_slot %d is outside 0 to %d.", doc.ResumeSlot, slotCount-1)
	}
	req := SlotListRequest{
		DBID: uint64(doc.DBID), BucketID: uint64(doc.BucketID), Filter: orNil(doc.Filter), Mask: doc.Mask,
		ResumeSlot: uint16(doc.ResumeSlot), ResumeID: orNil(doc.ResumeID), Want: doc.Want, MaxBytes: doc.MaxBytes,
	}
	if err := checkSlotListRequest(req); err != nil {
		return SlotListRequest{}, fmt.Errorf("reading SLOT_LIST: %w", err)
	}
	return req, nil
}

func checkSlotListReply(reply SlotListReply) error {
	if len(reply.Entries) > maxSubScanBody {
		return slotScanRefusal("a reply's entries are %d bytes and a frame holds at most %d.", len(reply.Entries), maxSubScanBody)
	}
	if _, err := walkSlotRecords(reply.Entries, nil); err != nil {
		return err
	}
	return nil
}

// EncodeSlotListReply returns the SLOT_LIST_REPLY body.
func EncodeSlotListReply(reply SlotListReply) ([]byte, error) {
	if err := checkSlotListReply(reply); err != nil {
		return nil, err
	}
	return encodeSlotScanDoc("SLOT_LIST_REPLY", slotListReplyDoc{Entries: nonNil(reply.Entries), More: reply.More})
}

// DecodeSlotListReply reads a SLOT_LIST_REPLY body. Its entries are checked frame by
// frame, so a caller can walk them with ParseSlotRecords without a second validation.
func DecodeSlotListReply(body []byte) (SlotListReply, error) {
	var doc slotListReplyDoc
	canonical := func() ([]byte, error) { return bson.Marshal(doc) }
	if err := decodeSlotScanDoc("SLOT_LIST_REPLY", body, &doc, canonical); err != nil {
		return SlotListReply{}, err
	}
	reply := SlotListReply{Entries: orNil(doc.Entries), More: doc.More}
	if err := checkSlotListReply(reply); err != nil {
		return SlotListReply{}, fmt.Errorf("reading SLOT_LIST_REPLY: %w", err)
	}
	return reply, nil
}

// EncodeSlotCount returns the SLOT_COUNT body.
func EncodeSlotCount(req SlotCountRequest) ([]byte, error) {
	if err := checkSlotMask(req.Mask); err != nil {
		return nil, err
	}
	if err := checkSlotFilter(req.Filter); err != nil {
		return nil, err
	}
	return encodeSlotScanDoc("SLOT_COUNT", slotCountDoc{DBID: int64(req.DBID), BucketID: int64(req.BucketID), Filter: req.Filter, Mask: req.Mask})
}

// DecodeSlotCount reads a SLOT_COUNT body.
func DecodeSlotCount(body []byte) (SlotCountRequest, error) {
	var doc slotCountDoc
	canonical := func() ([]byte, error) { return bson.Marshal(doc) }
	if err := decodeSlotScanDoc("SLOT_COUNT", body, &doc, canonical); err != nil {
		return SlotCountRequest{}, err
	}
	req := SlotCountRequest{DBID: uint64(doc.DBID), BucketID: uint64(doc.BucketID), Filter: orNil(doc.Filter), Mask: doc.Mask}
	if err := checkSlotMask(req.Mask); err != nil {
		return SlotCountRequest{}, fmt.Errorf("reading SLOT_COUNT: %w", err)
	}
	if err := checkSlotFilter(req.Filter); err != nil {
		return SlotCountRequest{}, fmt.Errorf("reading SLOT_COUNT: %w", err)
	}
	return req, nil
}

// EncodeSlotCountReply returns the SLOT_COUNT_REPLY body.
func EncodeSlotCountReply(reply SlotCountReply) ([]byte, error) {
	if reply.Count < 0 {
		return nil, slotScanRefusal("count is %d and a count is never negative.", reply.Count)
	}
	return encodeSlotScanDoc("SLOT_COUNT_REPLY", reply)
}

// DecodeSlotCountReply reads a SLOT_COUNT_REPLY body.
func DecodeSlotCountReply(body []byte) (SlotCountReply, error) {
	var doc SlotCountReply
	canonical := func() ([]byte, error) { return bson.Marshal(doc) }
	if err := decodeSlotScanDoc("SLOT_COUNT_REPLY", body, &doc, canonical); err != nil {
		return SlotCountReply{}, err
	}
	if doc.Count < 0 {
		return SlotCountReply{}, slotScanRefusal("count is %d and a count is never negative.", doc.Count)
	}
	return doc, nil
}

// AppendSlotRecord appends one record frame to entries.
func AppendSlotRecord(entries []byte, rec SlotRecord) ([]byte, error) {
	if rec.Slot >= slotCount {
		return nil, slotScanRefusal("a record names slot %d and the last slot is %d.", rec.Slot, slotCount-1)
	}
	if len(rec.EncID) == 0 || len(rec.EncID) > maxEncodedID {
		return nil, slotScanRefusal("a record's id is %d bytes and must be 1 to %d.", len(rec.EncID), maxEncodedID)
	}
	entries = binary.BigEndian.AppendUint16(entries, rec.Slot)
	entries = binary.AppendUvarint(entries, uint64(len(rec.EncID)))
	entries = append(entries, rec.EncID...)
	entries = binary.AppendUvarint(entries, uint64(len(rec.Body)))
	return append(entries, rec.Body...), nil
}

// ParseSlotRecords splits a reply's Entries into records that alias entries. A frame that
// is cut short, names a slot past the table or an id outside 1 to 1024 bytes is refused.
func ParseSlotRecords(entries []byte) ([]SlotRecord, error) {
	var records []SlotRecord
	if _, err := walkSlotRecords(entries, &records); err != nil {
		return nil, err
	}
	return records, nil
}

// walkSlotRecords checks every frame of entries and, when out is not nil, appends each.
// Every length is compared with the bytes left before it is used, so nothing is allocated
// from a length the input cannot back.
func walkSlotRecords(entries []byte, out *[]SlotRecord) (int, error) {
	count := 0
	rest := entries
	for len(rest) > 0 {
		if len(rest) < 3 {
			return 0, slotScanRefusal("record %d is cut short: it needs a 2-byte slot and an id length and %d bytes remain.", count, len(rest))
		}
		slot := binary.BigEndian.Uint16(rest)
		if slot >= slotCount {
			return 0, slotScanRefusal("record %d names slot %d and the last slot is %d.", count, slot, slotCount-1)
		}
		rest = rest[2:]
		encID, after, err := cutUvarintField(rest, "id", count)
		if err != nil {
			return 0, err
		}
		if len(encID) == 0 || len(encID) > maxEncodedID {
			return 0, slotScanRefusal("record %d has an id of %d bytes and it must be 1 to %d.", count, len(encID), maxEncodedID)
		}
		body, after, err := cutUvarintField(after, "body", count)
		if err != nil {
			return 0, err
		}
		rest = after
		if out != nil {
			*out = append(*out, SlotRecord{Slot: slot, EncID: encID, Body: body})
		}
		count++
	}
	return count, nil
}

func cutUvarintField(src []byte, what string, index int) (field, rest []byte, err error) {
	length, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, nil, slotScanRefusal("record %d has a %s length that is not a valid varint.", index, what)
	}
	src = src[n:]
	if length > uint64(len(src)) {
		return nil, nil, slotScanRefusal("record %d says its %s is %d bytes and %d remain.", index, what, length, len(src))
	}
	return src[:length], src[length:], nil
}
