// Package version is the single source of build metadata for both binaries.
// The three vars are overwritten by ldflags in build-cli/build-server and in
// the Dockerfile; a plain `go build` leaves them at their defaults, and
// String() then reports devel, which is honest — untagged builds are not a
// release. Everything that reports a version — the -v flag, the startup log
// line, the audit header, the wire field (server.Version) and the REPL verb —
// reads this package, so the flag and the log cannot drift.
package version

import "fmt"

// Version defaults to "devel" so the wire field AuthResponse.ServerVersion is
// never empty on a plain build; ldflags -X replaces it with the tag.
var (
	Version   = "devel"
	Commit    = ""
	BuildDate = ""
)

// String formats the full line both binaries print and the server logs:
// version, commit and build date in one stable shape.
func String() string {
	return fmt.Sprintf("version=%s commit=%s build_date=%s", Version, Commit, BuildDate)
}
