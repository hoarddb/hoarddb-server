package cli

import (
	"fmt"
	"strings"
)

// ingestRefusal is a refusal ingest makes with a message that names the fix. It
// maps to exit code 2.
type ingestRefusal struct{ Message string }

// Error returns the refusal text.
func (r *ingestRefusal) Error() string { return terminalSafeText(r.Message) }

// ingestUsageError is a mistake in how ingest was invoked, found before any
// connection is made. It maps to exit code 1.
type ingestUsageError struct{ Message string }

// Error returns the usage text.
func (e *ingestUsageError) Error() string { return terminalSafeText(e.Message) }

// splitInto splits --into at the first dot (spec Q15): database names may hold
// dots, and the CLI's own db.x.y grammar has the same convention.
func splitInto(into string) (string, string, error) {
	database, bucket, found := strings.Cut(into, ".")
	if !found || database == "" || bucket == "" {
		return "", "", &ingestUsageError{Message: fmt.Sprintf("--into %q must be <database>.<bucket>, for example: --into app.users", into)}
	}
	for _, part := range [][2]string{{"database", database}, {"bucket", bucket}} {
		if err := validNameComponentForRestore(part[0], part[1]); err != nil {
			return "", "", &ingestUsageError{Message: fmt.Sprintf("--into %q: %v. Use a plain name such as --into app.users", into, err)}
		}
	}
	return database, bucket, nil
}
