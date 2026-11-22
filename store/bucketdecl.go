package store

import (
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// BucketsBucketID is the id of the _buckets system bucket in every database. It mirrors
// machine.SystemBucketID, and TestSystemBucketVersionIsZero keeps the two equal.
const BucketsBucketID uint64 = 0xFFFF_FFFF_FFFF_FFFF

// BucketsBucketDecl is the compiled-in declaration of _buckets (conv 19): the eight
// fields of the declaration and reverse-index families, version 0, active. It mirrors
// machine.SystemDeclaration(), which the machine uses because it may not import store.
// Callers read it and never edit it.
var BucketsBucketDecl = Declaration{
	BucketID: BucketsBucketID,
	Version:  0,
	State:    machine.StateActive,
	Fields: FieldTable{
		NextFieldID: 9,
		Entries: []machine.FieldEntry{
			{ID: 1, Type: "any", Name: "_id_"},
			{ID: 2, Type: "binary", Name: "bucket_id"},
			{ID: 3, Type: "int", Name: "version"},
			{ID: 4, Type: "string", Name: "state"},
			{ID: 5, Type: "binary", Name: "fields"},
			{ID: 6, Type: "binary", Name: "swept"},
			{ID: 7, Type: "string", Name: "name"},
			{ID: 8, Type: "binary", Name: "pending"},
		},
	},
}

// BucketsFamily names the two kinds of document _buckets holds (conv 19).
type BucketsFamily int

const (
	// BucketsDeclaration is a declaration document: its `_id_` is the bucket name, a string.
	BucketsDeclaration BucketsFamily = iota + 1
	// BucketsReverseIndex is a reverse-index document: its `_id_` is the bucket id, binary.
	BucketsReverseIndex
)

// BucketsFamilyOf tells the two document families apart by the tag byte of the encoded
// `_id_`, which is read before EncodedComponentLen. EncodeComponent is prefix-free and
// the tags differ, so no key of one family can fall inside the range of the other.
func BucketsFamilyOf(encID []byte) (BucketsFamily, error) {
	if len(encID) == 0 {
		return 0, errors.New("a _buckets document with no encoded _id_")
	}
	switch encID[0] {
	case tagString:
		return BucketsDeclaration, nil
	case tagBinary:
		return BucketsReverseIndex, nil
	}
	return 0, fmt.Errorf("a _buckets document whose _id_ tag is 0x%02x: want 0x%02x (name) or 0x%02x (id)", encID[0], tagString, tagBinary)
}
