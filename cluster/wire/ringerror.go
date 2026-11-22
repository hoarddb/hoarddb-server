package wire

import (
	"bytes"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

const (
	// maxRingErrorMessage bounds the message text (build SPEC-GAP U65-8): a
	// message names a thing and a command, never a document.
	maxRingErrorMessage = 1 << 16
	// maxRingErrorPayload is the message plus headroom for the code and the
	// largest detail (two 256-byte strings and a few integers).
	maxRingErrorPayload = maxRingErrorMessage + 4096
	// ringErrorDepth is the nesting an error payload uses: the document and its detail.
	ringErrorDepth = 2
)

// RingError is the BSON payload of an OpError: {code: int32, message: string,
// detail?: document} (entry-ring-client-wire.md section 3).
type RingError struct {
	// Code is the ring code of conventions Errors.
	Code cluster.Code
	// Message is the same text on every surface; it names the fix.
	Message string
	// Detail is the structure a client acts on, one shape per code, or nil.
	Detail *cluster.Detail
}

// The wire documents. Field order is queries Formats' order and is the canonical
// byte layout. The note names the members, not their types; the integer widths
// are build SPEC-GAP U65-7.
type (
	ringErrorDoc struct {
		Code    int32    `bson:"code"`
		Message string   `bson:"message"`
		Detail  bson.Raw `bson:"detail,omitempty"`
	}
	notOwnerDetailDoc struct {
		Owner     string `bson:"owner"`
		OwnerAddr string `bson:"owner_addr"`
		Epoch     int64  `bson:"epoch"`
		Slot      int32  `bson:"slot"`
	}
	staleEpochDetailDoc struct {
		Epoch     int64  `bson:"epoch"`
		Owner     string `bson:"owner"`
		OwnerAddr string `bson:"owner_addr"`
	}
	ownerDownDetailDoc struct {
		Owner        string `bson:"owner"`
		MissingSlots int32  `bson:"missing_slots"`
	}
	noQuorumDetailDoc struct {
		Voters   int32 `bson:"voters"`
		Answered int32 `bson:"answered"`
	}
	leaseLapsedDetailDoc struct {
		Node     string `bson:"node"`
		LapsedMs int64  `bson:"lapsed_ms"`
	}
	notFoundDetailDoc struct {
		Expired bool `bson:"expired"`
	}
	rfUnsupportedDetailDoc struct {
		Rf          int32 `bson:"rf"`
		Voters      int32 `bson:"voters"`
		DataMembers int32 `bson:"data_members"`
	}
)

// ringDetailMembers names, per code, the Detail members its shape uses; every
// other member must be zero so one detail has one encoding.
var ringDetailMembers = map[cluster.Code][]string{
	cluster.CodeNotOwner:      {"owner", "owner_addr", "epoch", "slot"},
	cluster.CodeStaleEpoch:    {"epoch", "owner", "owner_addr"},
	cluster.CodeOwnerDown:     {"owner", "missing_slots"},
	cluster.CodeNoQuorum:      {"voters", "answered"},
	cluster.CodeLeaseLapsed:   {"node", "lapsed_ms"},
	cluster.CodeRFUnsupported: {"rf", "voters", "data_members"},
	cluster.CodeNotFound:      {"expired"},
}

func ringDetailSet(detail cluster.Detail) map[string]bool {
	return map[string]bool{
		"owner": detail.Owner != "", "owner_addr": detail.OwnerAddr != "", "node": detail.Node != "",
		"epoch": detail.Epoch != 0, "slot": detail.Slot != 0, "voters": detail.Voters != 0,
		"answered": detail.Answered != 0, "rf": detail.Rf != 0, "data_members": detail.DataMembers != 0,
		"missing_slots": detail.MissingSlots != 0, "lapsed_ms": detail.LapsedMs != 0, "expired": detail.Expired,
	}
}

func ringCodeRegistered(code cluster.Code) bool {
	for _, known := range cluster.Codes() {
		if known == code {
			return true
		}
	}
	return false
}

func checkRingDetailInt(name string, n int) error {
	if n < 0 || n > math.MaxInt32 {
		return ringRefusal("detail %s is %d and must be 0 to %d", name, n, math.MaxInt32)
	}
	return nil
}

func checkRingDetailText(name, text string, required bool) error {
	if required && text == "" {
		return ringRefusal("detail %s is empty; name the node", name)
	}
	if len(text) > ringMaxNodeText {
		return ringRefusal("detail %s is %d bytes and at most %d are taken", name, len(text), ringMaxNodeText)
	}
	return nil
}

func checkRingDetail(code cluster.Code, detail cluster.Detail) error {
	members, ok := ringDetailMembers[code]
	if !ok {
		return ringRefusal("%s has no detail shape; send the message alone", code)
	}
	used := make(map[string]bool, len(members))
	for _, name := range members {
		used[name] = true
	}
	for name, set := range ringDetailSet(detail) {
		if set && !used[name] {
			return ringRefusal("%s carries %s in its detail and its shape is %v", code, name, members)
		}
	}
	if code == cluster.CodeNotFound && !detail.Expired {
		return ringRefusal("a NOT_FOUND carries the detail {expired: true} or none: {expired: false} would be a second spelling of a plain miss. Send the message alone")
	}
	if detail.Epoch > math.MaxInt64 {
		return ringRefusal("detail epoch %d is above the BSON int64 range", detail.Epoch)
	}
	if detail.Slot >= slotCount {
		return ringRefusal("detail slot %d is past the last slot, %d", detail.Slot, slotCount-1)
	}
	return firstError(
		checkRingDetailInt("voters", detail.Voters), checkRingDetailInt("answered", detail.Answered),
		checkRingDetailInt("rf", detail.Rf), checkRingDetailInt("data_members", detail.DataMembers),
		checkRingDetailInt("missing_slots", detail.MissingSlots), checkRingDetailInt("lapsed_ms", detail.LapsedMs),
		checkRingDetailText("owner", detail.Owner, code == cluster.CodeNotOwner || code == cluster.CodeStaleEpoch || code == cluster.CodeOwnerDown),
		checkRingDetailText("owner_addr", detail.OwnerAddr, false),
		checkRingDetailText("node", detail.Node, code == cluster.CodeLeaseLapsed),
	)
}

func checkRingError(refusal RingError) error {
	if !ringCodeRegistered(refusal.Code) {
		return ringRefusal("error code 0x%02x is not a registered ring code (conventions Errors). Send a code from that table", uint16(refusal.Code))
	}
	if refusal.Message == "" {
		return ringRefusal("an error needs a message that names the thing and the fix")
	}
	if len(refusal.Message) > maxRingErrorMessage {
		return ringRefusal("the error message is %d bytes and at most %d are taken", len(refusal.Message), maxRingErrorMessage)
	}
	if refusal.Detail != nil {
		return checkRingDetail(refusal.Code, *refusal.Detail)
	}
	return nil
}

// ringDetailDocOf returns the wire document of detail for code; the caller has
// checked the code has a shape.
func ringDetailDocOf(code cluster.Code, detail cluster.Detail) any {
	switch code {
	case cluster.CodeNotOwner:
		return notOwnerDetailDoc{Owner: detail.Owner, OwnerAddr: detail.OwnerAddr, Epoch: int64(detail.Epoch), Slot: int32(detail.Slot)}
	case cluster.CodeStaleEpoch:
		return staleEpochDetailDoc{Epoch: int64(detail.Epoch), Owner: detail.Owner, OwnerAddr: detail.OwnerAddr}
	case cluster.CodeOwnerDown:
		return ownerDownDetailDoc{Owner: detail.Owner, MissingSlots: int32(detail.MissingSlots)}
	case cluster.CodeNoQuorum:
		return noQuorumDetailDoc{Voters: int32(detail.Voters), Answered: int32(detail.Answered)}
	case cluster.CodeLeaseLapsed:
		return leaseLapsedDetailDoc{Node: detail.Node, LapsedMs: int64(detail.LapsedMs)}
	case cluster.CodeNotFound:
		return notFoundDetailDoc{Expired: detail.Expired}
	default:
		return rfUnsupportedDetailDoc{Rf: int32(detail.Rf), Voters: int32(detail.Voters), DataMembers: int32(detail.DataMembers)}
	}
}

func ringErrorDocOf(refusal RingError) (ringErrorDoc, error) {
	doc := ringErrorDoc{Code: int32(refusal.Code), Message: refusal.Message}
	if refusal.Detail == nil {
		return doc, nil
	}
	raw, err := bson.Marshal(ringDetailDocOf(refusal.Code, *refusal.Detail))
	if err != nil {
		return ringErrorDoc{}, fmt.Errorf("encoding the detail of %s: %w", refusal.Code, err)
	}
	doc.Detail = raw
	return doc, nil
}

// EncodeRingError returns the OpError payload for refusal. It refuses what
// DecodeRingError would not accept, with INVALID_ARGUMENT.
func EncodeRingError(refusal RingError) ([]byte, error) {
	if err := checkRingError(refusal); err != nil {
		return nil, err
	}
	doc, err := ringErrorDocOf(refusal)
	if err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding an error payload: %w", err)
	}
	return raw, nil
}

// decodeRingDetail unpacks the detail document of code into the Detail members of its shape.
func decodeRingDetail(code cluster.Code, raw []byte) (cluster.Detail, error) {
	unpack := func(into any) error {
		if err := bson.Unmarshal(raw, into); err != nil {
			return ringRefusal("the detail of %s is not a BSON document of its shape: %v", code, err)
		}
		return nil
	}
	switch code {
	case cluster.CodeNotOwner:
		var doc notOwnerDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Owner: doc.Owner, OwnerAddr: doc.OwnerAddr, Epoch: uint64(max(doc.Epoch, 0)), Slot: uint64(max(doc.Slot, 0))}, err
	case cluster.CodeStaleEpoch:
		var doc staleEpochDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Epoch: uint64(max(doc.Epoch, 0)), Owner: doc.Owner, OwnerAddr: doc.OwnerAddr}, err
	case cluster.CodeOwnerDown:
		var doc ownerDownDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Owner: doc.Owner, MissingSlots: int(doc.MissingSlots)}, err
	case cluster.CodeNoQuorum:
		var doc noQuorumDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Voters: int(doc.Voters), Answered: int(doc.Answered)}, err
	case cluster.CodeLeaseLapsed:
		var doc leaseLapsedDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Node: doc.Node, LapsedMs: int(min(doc.LapsedMs, math.MaxInt32))}, err
	case cluster.CodeNotFound:
		var doc notFoundDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Expired: doc.Expired}, err
	case cluster.CodeRFUnsupported:
		var doc rfUnsupportedDetailDoc
		err := unpack(&doc)
		return cluster.Detail{Rf: int(doc.Rf), Voters: int(doc.Voters), DataMembers: int(doc.DataMembers)}, err
	}
	return cluster.Detail{}, ringRefusal("%s has no detail shape, yet the payload carries one", code)
}

// DecodeRingError reads an OpError payload written by EncodeRingError. It never
// panics: the payload is validated (size, depth) before it is unpacked, and accepted only
// if it re-encodes to the same bytes.
func DecodeRingError(raw []byte) (RingError, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringErrorDepth, Size: maxRingErrorPayload}); err != nil {
		return RingError{}, ringRefusal("an error payload is not a valid BSON document: %v", err)
	}
	var doc ringErrorDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingError{}, ringRefusal("an error payload is not a valid BSON document of its kind: %v", err)
	}
	if doc.Code < 0 || doc.Code > math.MaxUint16 {
		return RingError{}, ringRefusal("error code %d is outside 0 to 65535", doc.Code)
	}
	refusal := RingError{Code: cluster.Code(doc.Code), Message: doc.Message}
	if len(doc.Detail) != 0 {
		detail, err := decodeRingDetail(refusal.Code, doc.Detail)
		if err != nil {
			return RingError{}, err
		}
		refusal.Detail = &detail
	}
	if err := checkRingError(refusal); err != nil {
		return RingError{}, err
	}
	again, err := EncodeRingError(refusal)
	if err != nil {
		return RingError{}, err
	}
	if !bytes.Equal(again, raw) {
		return RingError{}, ringRefusal("an error payload does not have the layout of entry-ring-client-wire.md section 3 (code, message, then the detail of the code's shape, in order, none duplicated or extra, the stated types)")
	}
	return refusal, nil
}
