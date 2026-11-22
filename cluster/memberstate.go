package cluster

import "fmt"

// MemberState is a member's state as one byte on the wire. The values are
// defined here and nowhere else (conventions section 14): `members`,
// PROBE_REPLY and every driver's table carry them.
type MemberState uint8

// The five member states and their frozen wire bytes.
const (
	MemberJoining MemberState = 1
	MemberActive  MemberState = 2
	MemberDown    MemberState = 3
	MemberLeaving MemberState = 4
	MemberDead    MemberState = 5
)

// String returns the state's word as `nodes` prints it.
func (s MemberState) String() string {
	switch s {
	case MemberJoining:
		return "joining"
	case MemberActive:
		return "active"
	case MemberDown:
		return "down"
	case MemberLeaving:
		return "leaving"
	case MemberDead:
		return "dead"
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// DecodeMemberState reads a wire byte. Any byte outside 1..5 is refused with
// INVALID_ARGUMENT, because a state this build does not know cannot be
// ranked safely.
func DecodeMemberState(b byte) (MemberState, error) {
	if b < byte(MemberJoining) || b > byte(MemberDead) {
		return 0, &Error{
			Code:    CodeInvalidArgument,
			Message: fmt.Sprintf("member state byte %d is not a state: use 1 joining, 2 active, 3 down, 4 leaving or 5 dead", b),
		}
	}
	return MemberState(b), nil
}
