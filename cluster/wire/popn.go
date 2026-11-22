package wire

import (
	"encoding/binary"
	"fmt"
)

// A pop {n} records all its elements in one dedupe result: for each element, its length as
// a uvarint and then its bytes. A pop with no arg records the element as it is, so a
// retry's reader tells the two apart by the request it re-sends (build SPEC-GAP U85-3).

// packedLen is the bytes one element takes in a pop {n} result.
func packedLen(element int) int {
	var prefix [binary.MaxVarintLen64]byte
	return binary.PutUvarint(prefix[:], uint64(element)) + element
}

// packPopped joins elements into the result a pop {n} records; none gives no result.
func packPopped(elements [][]byte) []byte {
	var size int
	for _, element := range elements {
		size += packedLen(len(element))
	}
	if size == 0 {
		return nil
	}
	out := make([]byte, 0, size)
	for _, element := range elements {
		out = binary.AppendUvarint(out, uint64(len(element)))
		out = append(out, element...)
	}
	return out
}

// unpackPopped splits a pop {n} result back into its elements, refusing a length that runs
// past the bytes there are. The slices share result's memory.
func unpackPopped(result []byte) ([][]byte, error) {
	var elements [][]byte
	for len(result) > 0 {
		length, width := binary.Uvarint(result)
		if width <= 0 || length > uint64(len(result)-width) {
			return nil, fmt.Errorf("a popped element claims more bytes than the %d the result holds", len(result))
		}
		elements = append(elements, result[width:width+int(length)])
		result = result[width+int(length):]
	}
	return elements, nil
}
