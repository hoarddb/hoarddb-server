package cluster

// StateLevel is the one version integer of the replicated-state vocabulary and the on-disk
// formats (mixed-build spec 13.6). It is bumped by any change to a command, a command field, a
// value domain a command accepts, a snapshot field, what a command does to the state, or an
// on-disk format, in the same commit that adds its row to stateVersions and regenerates the
// goldens under testdata/. Level 0 is a build from before this check. The numbering restarted at 1
// in the first release (1.1): the development levels before it are not supported, and a release's
// level is recorded in stateVersions and testdata/state-releases.txt.
const StateLevel uint8 = 1

// StateRange is the levels one build can act at: Min is the lowest it applies and writes, Max
// the highest it can apply, Writes what it emits now. Before the first release every build
// declares Min == Max == Writes == StateLevel, so two builds overlap only when they are the same
// level; from the second release on Min is the previous release's level (MinStateLevel), with no
// change to the handshake bytes or the check.
type StateRange struct {
	Min, Max, Writes uint8
}

// BuildStateRange is the range this build declares: from MinStateLevel to StateLevel.
func BuildStateRange() StateRange {
	return StateRange{Min: MinStateLevel(), Max: StateLevel, Writes: StateLevel}
}

// Overlaps reports whether a node declaring peer and a node declaring r can act together: the
// ranges share a level, and what each writes is inside what the other applies.
func (r StateRange) Overlaps(peer StateRange) bool {
	if !r.Valid() || !peer.Valid() || r == (StateRange{}) || peer == (StateRange{}) {
		return false
	}
	if peer.Max < r.Min || r.Max < peer.Min {
		return false
	}
	return peer.Writes >= r.Min && peer.Writes <= r.Max && r.Writes >= peer.Min && r.Writes <= peer.Max
}

// Valid reports whether r is a range a build can declare: all zeros (none declared) or
// 1 <= Min <= Writes <= Max.
func (r StateRange) Valid() bool {
	if r == (StateRange{}) {
		return true
	}
	return r.Min >= 1 && r.Min <= r.Writes && r.Writes <= r.Max
}
