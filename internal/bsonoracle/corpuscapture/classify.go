package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// The key layout is storage/keys.go: slot (2 bytes), bucket id (8), namespace
// (1), then the engine's remainder. The tap keeps the first 12 bytes, which
// reach the record kind under the delivery and replication namespaces; a key
// of exactly 11 bytes (a bucket's state record) has no kind byte.
//
// The namespace bytes below are restated here and not imported, so this tool
// builds without the server tree; TestClassifyKeyPrefix is the table that a
// change to a namespace byte turns red.
const (
	slotCount      = 4096
	nodeRecordSlot = 0xFFFF
	// namespaceOffset is the position of the namespace byte: after the slot (2)
	// and the bucket id (8).
	namespaceOffset = 10
	// maxDocumentDepth is server/nesting_depth.go:maxDocumentDepth. A record
	// the server stores as it received it nests at most this deep.
	maxDocumentDepth = 100
)

// site is where a recorded value was written: its class and the deepest
// nesting the server accepts for a record of that class
// (server/nesting_depth.go:recordDepthLimit).
type site struct {
	class      string
	depthLimit int
}

var bucketNamespaces = map[byte]string{
	0x01: "documents",
	0x02: "versions",
	0x03: "index",
	0x04: "queue",
	0x05: "state",
	0x06: "blobmeta",
	0x07: "fieldstore",
}

var nodeNamespaces = map[byte]string{
	0x01: "node-catalog",
	0x08: "node-reclaim",
	0x09: "node-allocator",
	0x0A: "node-materialize",
	0x0B: "node-blobregistry",
}

var deliveryKinds = map[byte]string{
	'h': "delivery-lease-header",
	'd': "delivery-deadline",
	'e': "delivery-claimed",
	'c': "delivery-return-count",
	'x': "delivery-dead",
}

// classifyKeyPrefix names the site of a key prefix (hex). kept is false for a
// key outside the shared layout, which a storage test made up (a "k-0001" key
// reads as slot 0x6b2d): such a value was not written by the server and is not
// part of the corpus.
//
// A dead entry's limit depends on the store type, which the key does not say:
// a heap's entry is one level deeper than a queue's (recordDepthLimit), so the
// class takes the heap's 102, the largest. Every other class takes the stored
// limit; the records no client document reaches (catalog, allocator) are
// shallow and are held to it too.
func classifyKeyPrefix(prefixHex string) (found site, kept bool) {
	prefix, err := hex.DecodeString(prefixHex)
	if err != nil || len(prefix) < namespaceOffset+1 {
		return site{}, false
	}
	slot := binary.BigEndian.Uint16(prefix)
	if slot >= slotCount && slot != nodeRecordSlot {
		return site{}, false
	}
	// A bucket's state record is the bare 11-byte prefix: no kind byte follows.
	namespace, kind := prefix[namespaceOffset], byte(0)
	if len(prefix) > namespaceOffset+1 {
		kind = prefix[namespaceOffset+1]
	}
	if slot == nodeRecordSlot {
		if namespace == 0x0C {
			return site{class: fmt.Sprintf("node-replication-%c", kind), depthLimit: maxDocumentDepth}, true
		}
		if class, known := nodeNamespaces[namespace]; known {
			return site{class: class, depthLimit: maxDocumentDepth}, true
		}
		return site{class: fmt.Sprintf("node-namespace-%02x", namespace), depthLimit: maxDocumentDepth}, true
	}
	switch {
	case namespace == 0x0D:
		class, known := deliveryKinds[kind]
		if !known {
			class = fmt.Sprintf("delivery-kind-%02x", kind)
		}
		if class == "delivery-dead" {
			return site{class: class, depthLimit: maxDocumentDepth + 2}, true
		}
		return site{class: class, depthLimit: maxDocumentDepth}, true
	case bucketNamespaces[namespace] != "":
		return site{class: bucketNamespaces[namespace], depthLimit: maxDocumentDepth}, true
	}
	return site{class: fmt.Sprintf("namespace-%02x", namespace), depthLimit: maxDocumentDepth}, true
}

// shareOutCap divides a cap on the total number of records between classes:
// a class that has fewer records than an equal share keeps all of them, and
// the rest divide what is left. Deterministic: classes are visited by size,
// then by name.
func shareOutCap(classSizes map[string]int, totalCap int) map[string]int {
	classes := make([]string, 0, len(classSizes))
	for class := range classSizes {
		classes = append(classes, class)
	}
	sort.Slice(classes, func(left, right int) bool {
		if classSizes[classes[left]] != classSizes[classes[right]] {
			return classSizes[classes[left]] < classSizes[classes[right]]
		}
		return classes[left] < classes[right]
	})
	shares := make(map[string]int, len(classes))
	remaining := totalCap
	for position, class := range classes {
		equalShare := remaining / (len(classes) - position)
		shares[class] = min(classSizes[class], equalShare)
		remaining -= shares[class]
	}
	return shares
}
