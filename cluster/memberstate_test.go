package cluster

import (
	"errors"
	"testing"
)

// TestMemberStateBytesAreFrozen pins conventions section 14: the wire byte of
// each member state is defined once, and drivers decode the same table.
func TestMemberStateBytesAreFrozen(t *testing.T) {
	frozen := []struct {
		state MemberState
		b     byte
		name  string
	}{
		{MemberJoining, 1, "joining"},
		{MemberActive, 2, "active"},
		{MemberDown, 3, "down"},
		{MemberLeaving, 4, "leaving"},
		{MemberDead, 5, "dead"},
	}
	for _, row := range frozen {
		if byte(row.state) != row.b {
			t.Errorf("%s = %d, want %d", row.name, byte(row.state), row.b)
		}
		got, err := DecodeMemberState(row.b)
		if err != nil || got != row.state {
			t.Errorf("DecodeMemberState(%d) = %v, %v; want %s", row.b, got, err, row.name)
		}
		if row.state.String() != row.name {
			t.Errorf("state %d prints %q, want %q", row.b, row.state.String(), row.name)
		}
	}
}

// TestSixthMemberStateIsRefused: any byte outside 1..5 is INVALID_ARGUMENT.
func TestSixthMemberStateIsRefused(t *testing.T) {
	for _, b := range []byte{0, 6, 7, 255} {
		_, err := DecodeMemberState(b)
		var coded *Error
		if !errors.As(err, &coded) || coded.Code != CodeInvalidArgument {
			t.Errorf("DecodeMemberState(%d) error = %v, want INVALID_ARGUMENT", b, err)
		}
	}
}
