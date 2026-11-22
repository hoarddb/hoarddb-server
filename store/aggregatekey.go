package store

import "github.com/hoarddb/hoarddb-server/storage"

// InnerAggregate is the inner byte of a maintained aggregate's key inside a
// field store: storePrefix ‖ 0x06 ‖ aggID ‖ by. It is declared here, not in
// storage, because storage pins the bucket-level namespace table, and 0x06 is
// NSBlobMeta there. Inside a field store the bytes are free, and the readers in
// fieldstore.go skip this key rather than decode it as an element.
const InnerAggregate byte = 0x06

// AggregateKey is the key of one aggregate's sum or count under a store. by is
// the EncodeComponent form of the group value, so keys sort by value and a
// numeric by and its float twin are one key; nil by is the plain sum.
func AggregateKey(storePrefix []byte, aggID uint8, by []byte) []byte {
	key := make([]byte, 0, len(storePrefix)+2+len(by))
	key = append(key, storePrefix...)
	key = append(key, InnerAggregate, aggID)
	return append(key, by...)
}

// AggregateRange bounds every key of one aggregate under a store: its plain
// sum and each by value, and no other aggregate's keys.
func AggregateRange(storePrefix []byte, aggID uint8) storage.IterOptions {
	lower := AggregateKey(storePrefix, aggID, nil)
	return storage.IterOptions{Lower: lower, Upper: prefixEnd(lower)}
}
