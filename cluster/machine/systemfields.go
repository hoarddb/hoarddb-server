package machine

// SystemOptionalReferences is declaration field 13: the fields a bucket declares 'of' another
// bucket (fetch-through). The format registry owns the numbers.
const SystemOptionalReferences uint16 = 13

// SystemOptionalAggregates is declaration field 12, the maintained aggregates of a bucket's
// store fields.
const SystemOptionalAggregates uint16 = 12

// systemOptionalFields are the declaration fields a write to the _buckets system bucket may
// carry although the compiled table of _buckets stays the eight fields of conventions 19: the
// table is a fixed layout, and a declaration field that is absent unless a bucket uses it is
// admitted by id instead of by a table entry. This one set is the whole mechanism: field 10 is the
// ordered-index table, 11 the expiry declaration, 12 the aggregates and 13 the references, and a feature that writes
// its field through the owner's field check lists its id here and in nothing else. The format
// registry owns the numbers.
var systemOptionalFields = map[uint16]bool{10: true, 11: true, SystemOptionalAggregates: true, SystemOptionalReferences: true}

// systemWritable reports whether id is a declaration field a write to the _buckets system
// bucket may carry without the compiled table naming it.
func systemWritable(bucketID uint64, id uint16) bool {
	return bucketID == SystemBucketID && systemOptionalFields[id]
}
