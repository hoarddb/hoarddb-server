package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// The snapshot file is the whole FSM state (membership Formats 3):
//
//	[0:4]   magic "HDCS"
//	[4]     version 0x01
//	[5]     flags: bit 0 = the body is the BSON document, set by every writer; bits 1-7 reserved 0
//	[6:8]   reserved zeros
//	[8:16]  epoch, big-endian
//	[16:24] state_seq, big-endian
//	[24:28] body length n, big-endian
//	[28:28+n] body: the ClusterState document of formats section 2
//	[28+n:]   CRC-32C of [0, 28+n), big-endian
const (
	snapshotMagic       = "HDCS"
	snapshotVersion     = 0x01
	snapshotFlagBSON    = 0x01
	snapshotHeaderBytes = 28
	snapshotCRCBytes    = 4
)

var snapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeSnapshot writes the snapshot file of st: the 28-byte header, the
// canonical ClusterState document and the CRC-32C over both. The counters in
// the header repeat the body so `status` reads them without decoding it.
func EncodeSnapshot(st ClusterState) ([]byte, error) {
	body, err := EncodeClusterState(st)
	if err != nil {
		return nil, fmt.Errorf("encode snapshot body: %w", err)
	}
	raw := make([]byte, snapshotHeaderBytes, snapshotHeaderBytes+len(body)+snapshotCRCBytes)
	copy(raw, snapshotMagic)
	raw[4], raw[5] = snapshotVersion, snapshotFlagBSON
	binary.BigEndian.PutUint64(raw[8:], uint64(st.Epoch))
	binary.BigEndian.PutUint64(raw[16:], uint64(st.StateSeq))
	binary.BigEndian.PutUint32(raw[24:], uint32(len(body)))
	raw = append(raw, body...)
	return binary.BigEndian.AppendUint32(raw, crc32.Checksum(raw, snapshotCRCTable)), nil
}

func snapshotRefusal(format string, args ...any) error {
	return &Error{Code: CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

// DecodeSnapshot reads the bytes EncodeSnapshot writes. A bad magic, version,
// flag, checksum or length, or header counters that disagree with the body,
// is refused INVALID_ARGUMENT. Damage (a length, a checksum, a magic that is not ours) is left
// to the library, which falls back to an older snapshot; everything else, a version, flags, a
// field or a value this build does not know, is a build difference (SnapshotBuildDifference).
// It never panics, and a file it accepts encodes back to the same bytes.
func DecodeSnapshot(raw []byte) (ClusterState, error) {
	if len(raw) < snapshotHeaderBytes+snapshotCRCBytes {
		return ClusterState{}, snapshotRefusal("snapshot is %d bytes, shorter than its %d-byte header and %d-byte checksum: it is not a snapshot file", len(raw), snapshotHeaderBytes, snapshotCRCBytes)
	}
	if !bytes.Equal(raw[:4], []byte(snapshotMagic)) {
		return ClusterState{}, snapshotRefusal("snapshot starts with %q, not %q: it is not a hoardDB snapshot", raw[:4], snapshotMagic)
	}
	if raw[4] != snapshotVersion {
		return ClusterState{}, differs(snapshotRefusal("snapshot version %d is not supported: this build reads version %d", raw[4], snapshotVersion),
			"is version %d and this build reads version %d", raw[4], snapshotVersion)
	}
	if raw[5] != snapshotFlagBSON || raw[6] != 0 || raw[7] != 0 {
		return ClusterState{}, differs(snapshotRefusal("snapshot flags are %#02x and reserved bytes %#02x%02x: this build reads flags 0x01 (BSON body) and zeros", raw[5], raw[6], raw[7]),
			"sets flags this build does not define")
	}
	bodyLen := binary.BigEndian.Uint32(raw[24:])
	if uint64(bodyLen)+snapshotHeaderBytes+snapshotCRCBytes != uint64(len(raw)) {
		return ClusterState{}, snapshotRefusal("snapshot header says a %d-byte body but the file is %d bytes: it is truncated or has trailing bytes", bodyLen, len(raw))
	}
	crcAt := len(raw) - snapshotCRCBytes
	if want := crc32.Checksum(raw[:crcAt], snapshotCRCTable); binary.BigEndian.Uint32(raw[crcAt:]) != want {
		return ClusterState{}, snapshotRefusal("snapshot checksum does not match its contents: the file is damaged")
	}
	st, err := DecodeClusterState(raw[snapshotHeaderBytes:crcAt])
	if err != nil {
		// The checksum matched, so the bytes are what their writer wrote: whatever this build
		// cannot read in them is a difference between builds, not damage.
		var known *buildDifference
		if !errors.As(err, &known) {
			err = differs(err, "cannot be read by this build (%v)", err)
		}
		return ClusterState{}, fmt.Errorf("decode snapshot body: %w", err)
	}
	if binary.BigEndian.Uint64(raw[8:]) != uint64(st.Epoch) || binary.BigEndian.Uint64(raw[16:]) != uint64(st.StateSeq) {
		return ClusterState{}, snapshotRefusal("snapshot header counters (epoch %d, state_seq %d) disagree with its body (epoch %d, state_seq %d)", binary.BigEndian.Uint64(raw[8:]), binary.BigEndian.Uint64(raw[16:]), st.Epoch, st.StateSeq)
	}
	return st, nil
}

// SnapshotBuildDifference is the end of the sentence that says what this build cannot read in a
// snapshot (text 2 of the mixed-build spec), and ok is false for a snapshot that is merely
// damaged or for an error that is not about a snapshot's content.
func SnapshotBuildDifference(err error) (phrase string, ok bool) {
	var difference *buildDifference
	if errors.As(err, &difference) {
		return difference.phrase, true
	}
	return "", false
}

// ForeignSnapshotError is the refusal at start of a newest snapshot this build cannot read. It is
// typed so the server's main can exit with DivergedExitCode: one status means "this is the wrong
// build for this directory, do not restart it", whether the disagreement is found at start (here)
// or later in the log.
type ForeignSnapshotError struct {
	// Text is text 2 of the mixed-build spec.
	Text string
}

func (e *ForeignSnapshotError) Error() string { return "start raft: " + e.Text }

// snapshotDifferenceText is text 2: what a node says when it cannot use a snapshot another
// build wrote. subject is "this node's newest raft snapshot (<id>)" or "the snapshot the leader
// sent".
func snapshotDifferenceText(subject, phrase string) string {
	return subject + " " + phrase + " so it cannot use that snapshot: it was written by another build, or this build cannot read its own snapshot. " +
		"Start this node on that build. If that is not possible, keep its data directory untouched and replace the node: " +
		"move its ring directory aside and keep it (at replication factor 1 it may hold the only copy), start it again with the same node id, keys and address, " +
		"wait until 'nodes' shows it down, then run 'replace-node <node> --with <node>@<addr>' on any member."
}
