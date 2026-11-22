package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// TestAnAheadStampIsADivergedNotStateMoved: a command stamped above the state's StateSeq is a
// *Diverged (this node and the proposer disagree about the state), never STATE_MOVED, and it is
// told apart before the shape check so a command version this build refuses is still told
// apart. A stamp below StateSeq stays STATE_MOVED. Break: compare after checkCommandShape, or
// return STATE_MOVED for the ahead stamp.
func TestAnAheadStampIsADivergedNotStateMoved(t *testing.T) {
	st := fsmCluster(t)
	before := fsmEncoded(t, st)
	body := fsmCreateDatabaseBody("app", 1, 1)
	cases := []struct {
		name string
		cmd  Command
	}{
		{"one ahead", Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq) + 1, Body: body}},
		{"far ahead", Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq) + 1000, Body: body}},
		{"a version this build refuses", Command{Version: CommandVersion + 1, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq) + 1, Body: body}},
		{"a kind this build does not know", Command{Version: CommandVersion, Kind: 0x1F, BaseSeq: uint64(st.StateSeq) + 1, Body: body}},
	}
	for _, tc := range cases {
		_, err := st.ApplyCommand(tc.cmd)
		var diverged *Diverged
		if !errors.As(err, &diverged) {
			t.Fatalf("%s: got %v, want a *Diverged", tc.name, err)
		}
		var refusal *Error
		if errors.As(err, &refusal) {
			t.Fatalf("%s: a *Diverged must not also read as the refusal %v: a proposer would restamp it as STATE_MOVED", tc.name, refusal.Code)
		}
		if diverged.BaseSeq != tc.cmd.BaseSeq || diverged.StateSeq != st.StateSeq {
			t.Fatalf("%s: the *Diverged carries base %d and state %d, want %d and %d", tc.name, diverged.BaseSeq, diverged.StateSeq, tc.cmd.BaseSeq, st.StateSeq)
		}
	}
	if !bytes.Equal(before, fsmEncoded(t, st)) {
		t.Fatal("a command stamped ahead changed the state")
	}
	_, err := st.ApplyCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(st.StateSeq) - 1, Body: body})
	fsmWantCode(t, err, CodeStateMoved)
}

// TestTheCommandHeaderKeepsBaseSeqAtBytesTwoToTen is the golden of the rule for every future
// command version: the raw-header check reads the stamp at bytes 2 to 10 before decoding, so
// they never move.
func TestTheCommandHeaderKeepsBaseSeqAtBytesTwoToTen(t *testing.T) {
	const stamp = 0x0102030405060708
	raw, err := EncodeCommand(Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: stamp, Body: fsmCreateDatabaseBody("app", 1, 1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint64(raw[2:10]); got != stamp {
		t.Fatalf("bytes 2 to 10 of the header read %#x, want the stamp %#x", got, uint64(stamp))
	}
	if got, _ := rawBaseSeq(raw); got != stamp {
		t.Fatalf("rawBaseSeq reads %#x, want %#x", got, uint64(stamp))
	}
}

func fsmWantDiverged(t *testing.T, err error) {
	t.Helper()
	var diverged *Diverged
	if !errors.As(err, &diverged) {
		t.Fatalf("got %v, want a *Diverged", err)
	}
}
