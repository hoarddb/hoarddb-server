package harness

import "fmt"

// SmallLogSegments makes a node's log rotate every size bytes, so a test that writes a few
// hundred records leaves segments that TruncateLogBelow can drop. Call it before the writes.
func (c *Cluster) SmallLogSegments(id string, size int64) { c.Node(id).SetLogSegmentBytes(size) }

// TruncateLogBelow drops the segments of a node's log that hold only sequences below seq,
// through the guarded seam wire.Node.TruncateLogBelow: it refuses a seq at or above a floor
// that a catch-up or a seed on that node pins, and the error says which. A seq past the log's
// tail leaves only the segment still being written. It returns the node's refusal so a test
// can assert it.
func (c *Cluster) TruncateLogBelow(id string, seq uint64) error {
	if err := c.Node(id).TruncateLogBelow(seq); err != nil {
		return fmt.Errorf("truncating the log of %s: %w", id, err)
	}
	return nil
}
