package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// DecodeByComponent returns the value an aggregate's by key was made from: the inverse of
// EncodeComponent for the types a by value may be. A whole number comes back as an int64
// whether it was written as an int or a double, because the two are one key. A binary value is
// returned as primitive.Binary.
func DecodeByComponent(encoded []byte) (any, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("an aggregate by key is empty")
	}
	body := encoded[1:]
	switch encoded[0] {
	case tagNull:
		return nil, nil
	case tagBool:
		if len(body) != 1 {
			return nil, fmt.Errorf("a bool by key is 2 bytes, got %d", len(encoded))
		}
		return body[0] == 1, nil
	case tagDateTime:
		if len(body) != 8 {
			return nil, fmt.Errorf("a datetime by key is 9 bytes, got %d", len(encoded))
		}
		return primitive.DateTime(int64(binary.BigEndian.Uint64(body) ^ (1 << 63))), nil
	case tagObjectID:
		if len(body) != 12 {
			return nil, fmt.Errorf("an ObjectID by key is 13 bytes, got %d", len(encoded))
		}
		var id primitive.ObjectID
		copy(id[:], body)
		return id, nil
	case tagString, tagBinary:
		raw, err := unescapeComponent(body)
		if err != nil {
			return nil, err
		}
		if encoded[0] == tagString {
			return string(raw), nil
		}
		return primitive.Binary{Data: raw}, nil
	case tagNumber:
		return decodeNumberComponent(body)
	}
	return nil, fmt.Errorf("an aggregate by key has tag 0x%02x, which no by value encodes to", encoded[0])
}

// unescapeComponent reverses appendEscaped: 0x00 0xFF is a zero byte and 0x00 0x00 ends the value.
func unescapeComponent(body []byte) ([]byte, error) {
	var out bytes.Buffer
	for i := 0; i < len(body); i++ {
		if body[i] != 0x00 {
			out.WriteByte(body[i])
			continue
		}
		if i+1 >= len(body) {
			return nil, fmt.Errorf("an escaped by key ends inside an escape")
		}
		switch body[i+1] {
		case 0xFF:
			out.WriteByte(0x00)
			i++
		case 0x00:
			if i+2 != len(body) {
				return nil, fmt.Errorf("an escaped by key has %d bytes after its terminator", len(body)-i-2)
			}
			return out.Bytes(), nil
		default:
			return nil, fmt.Errorf("an escaped by key has 0x00 0x%02x, which is neither an escape nor a terminator", body[i+1])
		}
	}
	return nil, fmt.Errorf("an escaped by key has no terminator")
}

func decodeNumberComponent(body []byte) (any, error) {
	if len(body) != 17 {
		return nil, fmt.Errorf("a number by key is 18 bytes, got %d", len(body)+1)
	}
	image, class, payload := binary.BigEndian.Uint64(body[0:8]), body[8], binary.BigEndian.Uint64(body[9:17])
	switch class {
	case classSignedInteger:
		return int64(payload ^ (1 << 63)), nil
	case classUnsignedInteger:
		return float64(payload), nil
	case classOther:
		bits := ^image
		if image>>63 == 1 {
			bits = image &^ (1 << 63)
		}
		return math.Float64frombits(bits), nil
	}
	return nil, fmt.Errorf("a number by key has class 0x%02x", class)
}
