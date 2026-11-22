package cluster

import (
	"bytes"
	"cmp"
	"slices"
	"strconv"

	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The command bodies the Raft verbs propose. Each is one BSON document in the
// shape conventions section 13 lists, built with bsoncore so the field order
// is fixed. Commands that nothing in this package proposes have no builder.

func bodyDoc(elems ...[]byte) []byte {
	return bsoncore.BuildDocument(nil, bytes.Join(elems, nil))
}

// BootstrapBody is the body of BOOTSTRAP: the minted cluster id, the cluster version the cluster
// starts at (the bootstrapping build's highest level) and the bootstrapping node's own Member row.
func BootstrapBody(clusterID [16]byte, self Member, clusterVersion uint8) []byte {
	member := []byte{}
	member = append(member, bsoncore.AppendStringElement(nil, "id", self.ID)...)
	member = append(member, bsoncore.AppendInt64Element(nil, "incarnation", self.Incarnation)...)
	member = append(member, bsoncore.AppendStringElement(nil, "data_addr", self.DataAddr)...)
	if self.ClientAddr != "" {
		member = append(member, bsoncore.AppendStringElement(nil, "client_addr", self.ClientAddr)...)
	}
	if self.Witness {
		member = append(member, bsoncore.AppendBooleanElement(nil, "witness", true)...)
	}
	return bodyDoc(
		bsoncore.AppendBinaryElement(nil, "cluster_id", bsontype.BinaryGeneric, clusterID[:]),
		bsoncore.AppendInt32Element(nil, "cluster_version", int32(clusterVersion)),
		bsoncore.AppendDocumentElement(nil, "self", bodyDoc(member)),
	)
}

// RegisterMemberBody is the body of REGISTER_MEMBER. placingSet is the
// 1024-byte bitmap of the slots the joiner will pull; levels is the range the joiner declared
// (Min and Max are carried), which the registration is judged against.
func RegisterMemberBody(m Member, placingSet []byte, levels StateRange) []byte {
	elems := [][]byte{
		bsoncore.AppendStringElement(nil, "id", m.ID),
		bsoncore.AppendInt64Element(nil, "incarnation", m.Incarnation),
		bsoncore.AppendStringElement(nil, "data_addr", m.DataAddr),
	}
	if m.ClientAddr != "" {
		elems = append(elems, bsoncore.AppendStringElement(nil, "client_addr", m.ClientAddr))
	}
	return bodyDoc(append(elems,
		bsoncore.AppendBooleanElement(nil, "witness", m.Witness),
		bsoncore.AppendBinaryElement(nil, "placing_set", bsontype.BinaryGeneric, placingSet),
		bsoncore.AppendInt32Element(nil, "min_level", int32(levels.Min)),
		bsoncore.AppendInt32Element(nil, "max_level", int32(levels.Max)),
	)...)
}

// SetClusterVersionBody is the body of SET_CLUSTER_VERSION: the cluster moves from version from
// to version to, and members are the ids and incarnations the proposer checked, written sorted by
// id, so a member that joined or restarted since the check refuses the command.
func SetClusterVersionBody(from, to uint8, members []Member) []byte {
	sorted := slices.SortedFunc(slices.Values(members), func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	rows := make([][]byte, len(sorted))
	for i, m := range sorted {
		rows[i] = bsoncore.AppendDocumentElement(nil, strconv.Itoa(i), bodyDoc(
			bsoncore.AppendStringElement(nil, "id", m.ID),
			bsoncore.AppendInt64Element(nil, "incarnation", m.Incarnation),
		))
	}
	return bodyDoc(
		bsoncore.AppendInt32Element(nil, "from", int32(from)),
		bsoncore.AppendInt32Element(nil, "to", int32(to)),
		bsoncore.AppendArrayElement(nil, "members", bodyDoc(rows...)),
	)
}

// SetVotersBody is the body of SET_VOTERS.
func SetVotersBody(id string, voting bool) []byte {
	return bodyDoc(
		bsoncore.AppendStringElement(nil, "id", id),
		bsoncore.AppendBooleanElement(nil, "voting", voting),
	)
}

// SeedBitmapBody is the body of SEEDED and RETIRE: the (node, database) row of
// ClusterState.seed_bitmaps the command writes and the 1,024-byte bitmap of slots it
// names. SEEDED sets every named bit and RETIRE clears every named bit, so either
// command applied twice leaves the same row and a replayed report converges.
func SeedBitmapBody(node string, dbID int64, bits []byte) []byte {
	return bodyDoc(
		bsoncore.AppendStringElement(nil, "node", node),
		bsoncore.AppendInt64Element(nil, "db_id", dbID),
		bsoncore.AppendBinaryElement(nil, "bits", bsontype.BinaryGeneric, bits),
	)
}

// BumpSchemaBody is the body of BUMP_SCHEMA: database dbID goes from schema_version from to
// schema_version to. The command is refused SCHEMA_NOT_CURRENT unless from is the committed
// version, which is what makes two alters of one database take turns.
func BumpSchemaBody(dbID, from, to int64) []byte {
	return bodyDoc(
		bsoncore.AppendInt64Element(nil, "db_id", dbID),
		bsoncore.AppendInt64Element(nil, "from", from),
		bsoncore.AppendInt64Element(nil, "to", to),
	)
}

// MaxBumpRetired bounds the field ids one BUMP_SCHEMA note names: an alter drops at most the
// 999 fields a table can hold.
const MaxBumpRetired = 1000

// BumpNote says which bucket a BUMP_SCHEMA is about and what each node must then delete from
// its own copy of that bucket's data. The bump itself moves a database's schema_version and
// names no bucket (conv 20), so without a note a node could not know what to sweep (SPEC-GAP
// U43-1). A note is optional: a bump with none (a test, a restore) starts no sweep.
type BumpNote struct {
	// BucketID is the bucket the bump changed.
	BucketID uint64
	// Drop marks the bucket dropping: every node sweeps all of its keys (conv 21 step 4).
	Drop bool
	// Retired are the field ids the alter retired, ascending and unique: every node sweeps
	// those ids' keys (placement 5). Empty when Drop is set.
	Retired []uint16
	// Epoch is the committed epoch the alter stamped on the ids it retired.
	Epoch uint64
}

// BumpSchemaNoteBody is BumpSchemaBody carrying note. A zero BucketID builds the plain body.
func BumpSchemaNoteBody(dbID, from, to int64, note BumpNote) []byte {
	if note.BucketID == 0 {
		return BumpSchemaBody(dbID, from, to)
	}
	elems := [][]byte{
		bsoncore.AppendInt64Element(nil, "db_id", dbID),
		bsoncore.AppendInt64Element(nil, "from", from),
		bsoncore.AppendInt64Element(nil, "to", to),
		bsoncore.AppendInt64Element(nil, "bucket_id", int64(note.BucketID)),
	}
	if note.Drop {
		elems = append(elems, bsoncore.AppendBooleanElement(nil, "drop", true))
	}
	if len(note.Retired) > 0 {
		packed := make([]byte, 0, 2*len(note.Retired))
		for _, id := range note.Retired {
			packed = append(packed, byte(id>>8), byte(id))
		}
		elems = append(elems,
			bsoncore.AppendBinaryElement(nil, "retired", bsontype.BinaryGeneric, packed),
			bsoncore.AppendInt64Element(nil, "epoch", int64(note.Epoch)))
	}
	return bodyDoc(elems...)
}

// ParseBumpNote reads the note of a BUMP_SCHEMA body. ok is false for a body with none and for
// a body the command would refuse.
func ParseBumpNote(body []byte) (note BumpNote, ok bool) {
	fields, err := newCommandFields("BUMP_SCHEMA", bsoncore.Document(body))
	if err != nil {
		return BumpNote{}, false
	}
	note, ok, err = readBumpNote(fields)
	if err != nil {
		return BumpNote{}, false
	}
	return note, ok
}
