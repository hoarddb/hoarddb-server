package main

import (
	"encoding/binary"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
)

// hoardBucketName and hoardPayloadField are the bucket the hoarddb engine
// creates through RING_ADMIN: the implicit _id_ plus one string field.
const (
	hoardBucketName   = "Doc"
	hoardPayloadField = "Payload"
)

// hoardBucket holds what a RING_KEYED header needs, exactly as the
// create_bucket reply told it. Nothing is read from the node in process.
type hoardBucket struct {
	BucketID          uint64
	DeclVersion       int64
	SchemaVersionSeen int64
	PayloadFieldID    uint16
}

// bucketFromAdminReply builds the header values from a create_bucket or
// describe_bucket reply.
func bucketFromAdminReply(reply wire.RingAdminReply) (hoardBucket, error) {
	if reply.BucketID == 0 {
		return hoardBucket{}, fmt.Errorf("the %s reply carries bucket id 0: the server did not create the bucket", reply.Cmd)
	}
	bucket := hoardBucket{BucketID: reply.BucketID, DeclVersion: reply.DeclVersion, SchemaVersionSeen: reply.SchemaVersionSeen}
	for _, field := range reply.Fields {
		if field.Name == hoardPayloadField {
			bucket.PayloadFieldID = field.ID
			return bucket, nil
		}
	}
	return hoardBucket{}, fmt.Errorf("the %s reply lists no %s field, so the insert has nowhere to put the payload", reply.Cmd, hoardPayloadField)
}

// insertOp is the RING_KEYED insert of one document.
func (b hoardBucket) insertOp(id, payload string) (cluster.Op, error) {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		return cluster.Op{}, fmt.Errorf("encoding id %q: %w", id, err)
	}
	return cluster.Op{
		Class: cluster.OpInsert, BucketID: b.BucketID, DeclVersion: b.DeclVersion, SchemaVersionSeen: b.SchemaVersionSeen,
		EncID: encoded, IDValue: typedString(id),
		Fields: []cluster.FieldOp{{FieldID: b.PayloadFieldID, Value: typedString(payload)}},
	}, nil
}

// getRead is the RING_KEYED get of one document by id.
func (b hoardBucket) getRead(id string) (wire.RingRead, error) {
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		return wire.RingRead{}, fmt.Errorf("encoding id %q: %w", id, err)
	}
	return wire.RingRead{
		Class: wire.RingClassGet, BucketID: b.BucketID, DeclVersion: b.DeclVersion, SchemaVersionSeen: b.SchemaVersionSeen, EncID: encoded,
	}, nil
}

// typedString is a field value on the wire: BSON type tag 0x02, the length
// including the terminator, the text, a NUL.
func typedString(text string) []byte {
	out := append([]byte{0x02}, binary.LittleEndian.AppendUint32(nil, uint32(len(text)+1))...)
	return append(append(out, text...), 0)
}
