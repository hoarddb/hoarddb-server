package server

import (
	"errors"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// blobReadFix is the sentence that completes a failed payload read for an
// operator (at-rest encryption spec §4.2.1 h). A payload that fails
// authentication is damage, and the fix is a copy of the blob; the other two
// failures a read can have — a missing key epoch and a content that does not
// match its checksum — already carry their own fix in their text, so nothing
// is added to them.
func blobReadFix(err error) string {
	if errors.Is(err, sealed.ErrBlobCorrupt) {
		return ". Nothing was returned. Restore this blob from a replica or a backup; the node's other blobs are unaffected"
	}
	return ""
}
