package server

import (
	"context"
	"fmt"

	"github.com/hoarddb/hoarddb-server/transport"
)

// authorizeUnscoped is the rule of every ring command that is node-wide and not a database's:
// the seeding commands and the five membership commands. Cluster roles are cluster-wide (owner
// ruling, 2026-11-01), so the legacy operation op stands for (its opRules row must be action
// with no database) is asked on "*": only an unscoped grant (cluster-manager@* or root@*)
// allows it, and a grant on one database is denied. The database a request names is never asked,
// so the answer cannot depend on anything the request names, and identityAllows is called once.
// A denial is audited as the legacy gate audits it, under op. It is default-deny if the legacy
// rule stops being that rule.
func (a ringAuthorizer) authorizeUnscoped(ctx context.Context, action Action, op transport.Opcode) error {
	rule, ok := opRules[op]
	if !ok || rule.Action != action || rule.Scope != ScopeNone {
		return fmt.Errorf("%s is no longer a %s operation without a database; refusing the ring command that stands for it (default deny)", op, action)
	}
	identity, err := ringIdentity(ctx)
	if err != nil {
		return err
	}
	if identityAllows(identity, action, unscopedResource) {
		return nil
	}
	a.server.recordDenial(identity.Username, action, unscopedResource, op, identity.Grants)
	return &sentencedError{message: fmt.Sprintf("permission denied: user %q has no unscoped grant that allows %q", identity.Username, action.String()), cause: ErrPermissionDenied}
}
