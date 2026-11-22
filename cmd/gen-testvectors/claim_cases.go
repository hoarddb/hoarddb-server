package main

import (
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// claimCases are the vectors of the lease verbs and the dead list (claim-ack-spec
// 13.4): claim on the three ordered types, ack, release and extend of a lease, the
// dead verbs on a bucket with no dead entry, the members the existing answers
// gained, and the refusals.
//
// A lease verb that names the lease its setup claimed carries setupLeaseMarker in
// lease_id; the generator sends the real id and records "normalised", and a
// harness replaying the vector fills it from the setup claim's answer. The notes
// say "claimed count" and never "pending" (TestNotesDoNotPromiseUnlandedWork).
func claimCases() []func(db string) exchangeCase {
	cases := []func(db string) exchangeCase{
		queueClaimCase("fifo claim", "fifo", transport.OpFIFOPush, server.BucketTypeFIFO, "A claim takes entries in pop order, as a pop does, but moves them under a lease instead of removing them. item_ids are the handles an ack names; deliveries is absent while every value is 1."),
		queueClaimCase("lifo claim", "lifo", transport.OpLIFOPush, server.BucketTypeLIFO, "The newest entries come first, as a lifo pop takes them."),
		queueClaimCase("heap claim", "heap", transport.OpHeapPush, server.BucketTypeHeap, "The highest priorities come first. A heap's item_ids carry the element's id, which the server mints, so they vary."),
		func(db string) exchangeCase {
			return exchangeCase{
				name: "claim of an empty queue", group: "write", storeType: "fifo",
				notes:      "Nothing to take is not an error: found is false, there is no lease, and nothing is written.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    claimCall(db, "q", 1, 0),
				wantOpcode: transport.OpQueueClaim, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "claim with a redelivery", group: "write", storeType: "fifo",
				notes: "The setup claims an entry and releases it. The next claim takes it again, and deliveries says it is the second delivery. " +
					"The lease_id of the setup's release is the setup claim's.",
				setup: append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2),
					claimCall(db, "q", 1, 0), leaseCall(transport.OpQueueRelease, db, "q", setupLeaseMarker)),
				request:    claimCall(db, "q", 1, 0),
				wantOpcode: transport.OpQueueClaim, wantField: map[string]any{"found": true},
				varyPaths: []string{"items[]._id_"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "ack of a whole lease", group: "write", storeType: "fifo",
				notes:      "Without item_ids an ack settles every entry of the lease. lease_id is the id the setup claim returned.",
				setup:      append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2), claimCall(db, "q", 2, 0)),
				request:    leaseCall(transport.OpQueueAck, db, "q", setupLeaseMarker),
				wantOpcode: transport.OpQueueAck, wantField: map[string]any{"success": true, "lease_state": "acked", "acked": int32(2)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "ack with item ids", group: "write", storeType: "fifo",
				notes: "item_ids name the entries to settle. A fifo's handles are positions, so these are stable: the first entry's is " +
					"0000000000000000, and the second id here names nothing the lease holds, so it is counted as missing and the lease keeps one entry.",
				setup: append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2), claimCall(db, "q", 2, 0)),
				request: call{op: transport.OpQueueAck, payload: server.LeaseRequest{
					Database: db, Bucket: "q", LeaseID: setupLeaseMarker, ItemIDs: []string{"0000000000000000", "00000000000000ff"},
				}},
				wantOpcode: transport.OpQueueAck, wantField: map[string]any{"success": true, "acked": int32(1), "missing": int32(1), "remaining": int32(1)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "ack of an unknown lease", group: "write", storeType: "fifo",
				notes: "A lease nothing issued is an in-band answer, not an error: success is false and lease_state says unknown. " +
					"The id is well formed (40 hexadecimal characters) and no bucket issued it.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    leaseCall(transport.OpQueueAck, db, "q", "0123456789abcdef0123456789abcdef01234567"),
				wantOpcode: transport.OpQueueAck, wantField: map[string]any{"success": false, "lease_state": "unknown"},
				wantMessage: []string{"is unknown on", "Claim again with"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "release", group: "write", storeType: "fifo",
				notes:      "A release gives every entry of the lease back to the queue at once. They count as delivered once.",
				setup:      append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2), claimCall(db, "q", 2, 0)),
				request:    leaseCall(transport.OpQueueRelease, db, "q", setupLeaseMarker),
				wantOpcode: transport.OpQueueRelease, wantField: map[string]any{"success": true, "lease_state": "released", "released": int32(2)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "extend", group: "write", storeType: "fifo",
				notes:      "Extend moves the deadline to now plus lease_ms. lease_ms is required and within one second and one hour; lease_deadline varies.",
				setup:      append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 1), claimCall(db, "q", 1, 0)),
				request:    call{op: transport.OpQueueExtend, payload: server.LeaseRequest{Database: db, Bucket: "q", LeaseID: setupLeaseMarker, LeaseMs: 120000}},
				wantOpcode: transport.OpQueueExtend, wantField: map[string]any{"success": true, "lease_state": "extended", "lease_ms": int64(120000)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "dead list of a bucket with none", group: "read", storeType: "fifo",
				notes:      "The dead list is a read the bucket's owner answers. An empty one is found false and a message, with no next.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpDeadList, payload: server.DeadListRequest{Database: db, Bucket: "q"}},
				wantOpcode: transport.OpDeadList, wantField: map[string]any{"found": false},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "requeue with no dead entry", group: "write", storeType: "fifo",
				notes:      "A dead_id nothing holds is refused in band and by name: done is 0 and refused says which id and what to do.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpDeadRequeue, payload: server.DeadMoveRequest{Database: db, Bucket: "q", DeadID: "0000000000000000"}},
				wantOpcode: transport.OpDeadRequeue, wantField: map[string]any{"success": false, "done": int32(0)},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "discard all of none", group: "write", storeType: "fifo",
				notes:      "all: true on a bucket with no dead entry does nothing and succeeds.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpDeadDiscard, payload: server.DeadMoveRequest{Database: db, Bucket: "q", All: true}},
				wantOpcode: transport.OpDeadDiscard, wantField: map[string]any{"success": true, "done": int32(0)},
			}
		},
		claimedBucketCase("peek of a bucket whose entries are all claimed",
			call{op: transport.OpFIFOPeek, payload: server.PeekRequest{Database: "", Bucket: "q"}}, transport.OpFIFOPeek,
			map[string]any{"found": false, "pending": int64(2)},
			"Claimed entries are not shown by a peek; the claimed count beside the answer says how many there are, so an empty answer can say why."),
		claimedBucketCase("count of a bucket whose entries are all claimed",
			call{op: transport.OpCount, payload: server.CountRequest{Database: "", Bucket: "q"}}, transport.OpCount,
			map[string]any{"count": int64(0), "pending": int64(2)},
			"len() counts the entries available to take: none. The claimed count is beside it."),
		claimedBucketCase("bucket info of a bucket whose entries are all claimed",
			call{op: transport.OpBucketInfo, payload: server.BucketInfoRequest{Database: "", Bucket: "q"}}, transport.OpBucketInfo,
			nil,
			"The bucket's row carries the claimed count, absent at zero. The same row is a bucket's entry in a listing."),
		claimedBucketCase("pop of a bucket whose entries are all claimed",
			call{op: transport.OpFIFOPop, payload: server.PopRequest{Database: "", Bucket: "q"}}, transport.OpFIFOPopResponse,
			map[string]any{"found": false, "pending": int64(2)},
			"Nothing is available to pop, and the claimed count says the queue is not empty."),
		func(db string) exchangeCase {
			return exchangeCase{
				name: "claim limit above the maximum", group: "errors", storeType: "fifo",
				notes:      "A claim takes at most 100 entries; nothing is claimed.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    claimCall(db, "q", 101, 0),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"101 is above the claim maximum of 100"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "lease without bounds", group: "errors", storeType: "fifo",
				notes:      "A lease is one second to one hour. A longer one is refused and nothing is claimed.",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    claimCall(db, "q", 1, 7_200_000),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"7200000 ms is above the lease maximum of 1h"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "item id that is a document id", group: "errors", storeType: "fifo",
				notes: "item_ids are the handles claim returned in item_ids, not the documents' _id_ values. A document id is refused " +
					"with the message that says so, and nothing is acknowledged.",
				setup: append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 1), claimCall(db, "q", 1, 0)),
				request: call{op: transport.OpQueueAck, payload: server.LeaseRequest{
					Database: db, Bucket: "q", LeaseID: setupLeaseMarker, ItemIDs: []string{"job-7"},
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"is not a handle claim returned"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "claim on a hash bucket", group: "errors", storeType: "hash",
				notes: "A hash has no order to take from. The refusal is an ERROR frame INVALID_ARGUMENT, write_outcome " +
					"not_applied, whose message names the verbs that work and ends with the verbs the type takes (BUG-0469; it was in band, found false).",
				setup:      bucketSetup(db, "h", server.BucketTypeHash),
				request:    claimCall(db, "h", 1, 0),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"claim is not defined for hash buckets", "A hash bucket takes:"},
			}
		},
	}
	return cases
}

// claimCall is a claim of limit entries for leaseMs (0 is the default lease).
func claimCall(db, bucket string, limit int, leaseMs int64) call {
	return call{op: transport.OpQueueClaim, payload: server.ClaimRequest{Database: db, Bucket: bucket, Limit: limit, LeaseMs: leaseMs}}
}

// leaseCall is an ack, a release or an extend of leaseID with no other member.
func leaseCall(op transport.Opcode, db, bucket, leaseID string) call {
	return call{op: op, payload: server.LeaseRequest{Database: db, Bucket: bucket, LeaseID: leaseID}}
}

// queueSetup creates bucket and pushes count entries into it.
func queueSetup(db, bucket string, storeType server.BucketType, pushOp transport.Opcode, count int) []call {
	documents := []map[string]any{ada(), grace(), linus()}
	steps := bucketSetup(db, bucket, storeType)
	for index := 0; index < count; index++ {
		steps = append(steps, push(pushOp, db, bucket, documents[index], int64(index+1)))
	}
	return steps
}

// queueClaimCase is a claim of two of three entries in a bucket of one type.
func queueClaimCase(name, bucket string, pushOp transport.Opcode, storeType server.BucketType, notes string) func(db string) exchangeCase {
	return func(db string) exchangeCase {
		varying := []string{"items[]._id_"}
		if storeType == server.BucketTypeHeap {
			varying = append(varying, "item_ids[]")
		}
		return exchangeCase{
			name: name, group: "write", storeType: string(storeType), notes: notes,
			setup:      queueSetup(db, bucket, storeType, pushOp, 3),
			request:    claimCall(db, bucket, 2, 60000),
			wantOpcode: transport.OpQueueClaim, wantField: map[string]any{"found": true, "lease_ms": int64(60000)},
			varyPaths: varying,
		}
	}
}

// claimedBucketCase is a read of a fifo "q" whose two entries are both claimed.
// Its request is built with the exchange's database name.
func claimedBucketCase(name string, request call, wantOpcode transport.Opcode, wantField map[string]any, notes string) func(db string) exchangeCase {
	return func(db string) exchangeCase {
		switch payload := request.payload.(type) {
		case server.PeekRequest:
			payload.Database = db
			request.payload = payload
		case server.CountRequest:
			payload.Database = db
			request.payload = payload
		case server.BucketInfoRequest:
			payload.Database = db
			request.payload = payload
		case server.PopRequest:
			payload.Database = db
			request.payload = payload
		}
		return exchangeCase{
			name: name, group: "read", storeType: "fifo", notes: notes,
			setup:      append(queueSetup(db, "q", server.BucketTypeFIFO, transport.OpFIFOPush, 2), claimCall(db, "q", 2, 0)),
			request:    request,
			wantOpcode: wantOpcode, wantField: wantField,
		}
	}
}

// claimOverCapCases is appended after every other exchange, as alterBucketCases
// is, so no earlier case's number or database name moves.
func claimOverCapCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "claim of an entry over the hard cap", group: "errors", storeType: "heap",
				notes: "A claim moves entries a node can commit in one batch. The element at the front of this heap has an _id_ of 9000 bytes, over the 8 KiB " +
					"a claim takes (a fifo or lifo entry is refused the same way when its payload is over the batch limit, near 4.7 MiB). The refusal is an ERROR frame " +
					"INVALID_ARGUMENT, write_outcome not_applied: nothing was taken, and the message says to take the element with pop.",
				setup: append(bucketSetup(db, "h", server.BucketTypeHeap),
					push(transport.OpHeapPush, db, "h", map[string]any{"_id_": strings.Repeat("x", 9000), "Name": "Ada"}, 1)),
				request:    claimCall(db, "h", 1, 0),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"claim refused"},
			}
		},
	}
}
