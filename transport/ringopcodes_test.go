package transport

import "testing"

// TestU65RingOpcodesAreTheNoteTable pins the entry-ring client opcodes of
// entry-ring-client-wire.md section 1: values, names, and which are reads.
func TestU65RingOpcodesAreTheNoteTable(t *testing.T) {
	table := []struct {
		op       Opcode
		value    byte
		name     string
		readOnly bool
	}{
		{OpRingList, 0xDA, "RING_LIST", true},
		{OpRingCount, 0xDB, "RING_COUNT", true},
		{OpRingKeyed, 0xDC, "RING_KEYED", false},
		{OpRingAdmin, 0xDD, "RING_ADMIN", false},
		{OpRingTopology, 0xDE, "RING_TOPOLOGY", true},
		{OpRingReply, 0xDF, "RING_REPLY", false},
		{OpRingBatch, 0xE0, "RING_BATCH", false},
		{OpRingGetMany, 0xE1, "RING_GET_MANY", true},
	}
	for _, row := range table {
		if byte(row.op) != row.value {
			t.Errorf("%s = 0x%02X, want 0x%02X", row.name, byte(row.op), row.value)
		}
		if got := row.op.String(); got != row.name {
			t.Errorf("0x%02X.String() = %q, want %q", row.value, got, row.name)
		}
		if got := row.op.IsReadOnly(); got != row.readOnly {
			t.Errorf("%s.IsReadOnly() = %v, want %v", row.name, got, row.readOnly)
		}
	}
}
