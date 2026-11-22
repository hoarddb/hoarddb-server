package config

import (
	"fmt"
	"strings"
)

// WriteConcernValues is the list a refusal shows. It is the one place the
// accepted spellings live: the server parses requests with
// CanonicalWriteConcern, so a request and HOARDDB_WRITE_CONCERN agree.
const WriteConcernValues = "1, one, majority, all"

// CanonicalWriteConcern maps a spelling of a write concern to its canonical
// name ("one", "majority" or "all"). ok is false for anything else, so a typo
// is never mistaken for the weakest level.
func CanonicalWriteConcern(spelling string) (canonical string, ok bool) {
	switch strings.ToLower(strings.TrimSpace(spelling)) {
	case "1", "one":
		return "one", true
	case "majority":
		return "majority", true
	case "all", "all_nodes":
		return "all", true
	}
	return "", false
}

// validateWriteConcern refuses a configured concern the server would not
// understand. Empty means "the default for the replication factor".
func validateWriteConcern(configured string) error {
	if configured == "" {
		return nil
	}
	if _, ok := CanonicalWriteConcern(configured); !ok {
		return fmt.Errorf(`server.write_concern (HOARDDB_WRITE_CONCERN) is %q, which is not a write concern
       set it to one of: %s, or remove it to use the default for the replication factor`,
			configured, WriteConcernValues)
	}
	return nil
}
