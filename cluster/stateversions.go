package cluster

// StateVersionRow maps one state level to the first release that carries it. FirstRelease is
// empty until a release ships the level; once set it is never edited
// (TestEveryStateLevelHasARowAndRowsOnlyGrow), because an operator is told to upgrade to it.
type StateVersionRow struct {
	Level        uint8
	FirstRelease string
}

// stateVersions has one row per level, contiguous from 1. Adding a row is part of the commit that
// bumps StateLevel.
//
// The convention MinStateLevel reads: FirstRelease is recorded only on the level a release shipped
// at, its final level, which is the level StateLevel had in the release commit. The development
// levels of a cycle, below it, keep FirstRelease empty, and so do the levels after it until the
// next release commit records its own. A row of an intermediate level that named the release
// would make MinStateLevel of the next cycle that level, a cluster version no released build ran,
// and refuse nothing it should while admitting a range no release declared.
var stateVersions = []StateVersionRow{
	{Level: 1, FirstRelease: "1.1"},
}

// StateVersions returns a copy of the release table.
func StateVersions() []StateVersionRow {
	return append([]StateVersionRow(nil), stateVersions...)
}

// ReleaseOf returns the first release that carries level, or "" when none is recorded.
func ReleaseOf(level uint8) string {
	for _, row := range stateVersions {
		if row.Level == level {
			return row.FirstRelease
		}
	}
	return ""
}

// MinStateLevel is the lowest cluster version this build runs: the level of the previous release,
// which is the newest row below StateLevel that records a release, or StateLevel itself when no
// earlier release is recorded. A level is not a release: the level moves once per merge that
// changes the vocabulary, so one release cycle spans several levels, and only a released level is
// a cluster version an operator can have left a directory at (cluster-version spec 4.1).
func MinStateLevel() uint8 { return minStateLevelOf(stateVersions, StateLevel) }

// minStateLevelOf is MinStateLevel over a given table and current level, for the test of the rule.
func minStateLevelOf(rows []StateVersionRow, level uint8) uint8 {
	previous := level
	for _, row := range rows {
		if row.Level < level && row.FirstRelease != "" {
			previous = row.Level
		}
	}
	return previous
}
