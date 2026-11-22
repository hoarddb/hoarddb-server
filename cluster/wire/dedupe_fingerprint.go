package wire

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// errRequestIDReused refuses a write whose request id has a live dedupe entry written for a
// different operation. Answering it with the first operation's result would tell the client
// a write it never made was applied.
var errRequestIDReused = &cluster.Error{Code: cluster.CodeInvalidArgument, Message: "this request id already named a different operation. " +
	"Nothing was done. Use a new id for a new operation, or resend exactly the operation the id was first used for"}

// opFingerprint hashes what the client asked: the class, the bucket, the document, and the
// fields, lease and count it carried. It leaves out everything the owner stamps or the
// route decides (declaration and schema versions, the clock, the nonce, the previous end), so
// a retry after a schema bump or on another path hashes the same, and it hashes the logged
// op of a copy or a replay to the value the owner wrote. Fields are hashed in the order sent.
func opFingerprint(op cluster.Op) uint64 {
	var framed []byte
	framed = binary.BigEndian.AppendUint64(framed, uint64(op.Class))
	framed = binary.BigEndian.AppendUint64(framed, op.BucketID)
	for _, raw := range [][]byte{op.EncID, op.IDValue} {
		framed = binary.BigEndian.AppendUint64(framed, uint64(len(raw)))
		framed = append(framed, raw...)
	}
	framed = binary.BigEndian.AppendUint64(framed, uint64(op.Count))
	framed = append(framed, op.LeaseID[:]...)
	// The return mode is logged and the guard is not: a retry carries both, the record only the
	// first, and they must hash alike.
	framed = binary.BigEndian.AppendUint64(framed, uint64(op.Return))
	framed = binary.BigEndian.AppendUint64(framed, uint64(len(op.Fields)))
	for _, field := range op.Fields {
		framed = binary.BigEndian.AppendUint64(framed, uint64(field.FieldID)<<16|uint64(field.Verb)<<8|uint64(field.OpCode))
		framed = binary.BigEndian.AppendUint64(framed, uint64(len(field.Value)))
		framed = append(framed, field.Value...)
	}
	return xxhash.Sum64(framed)
}
