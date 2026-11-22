package server

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The mutation methods below are the single writer path for the user store.
// Each takes am.mu for the whole read-modify-write, re-checks its invariants
// against committed state inside that lock, and persists before returning.
// Checking outside the lock is the bug the spec calls out (spec 5.3.1): two
// concurrent drops would each see the other administrator present and both
// succeed.

// saltInUseLocked reports whether any record already uses this salt. Caller
// holds am.mu.
func (am *AuthManager) saltInUseLocked(salt string) bool {
	for _, record := range am.users {
		if record.Credentials.Salt == salt {
			return true
		}
	}
	return false
}

// adminCountLocked returns the number of enabled users holding admin
// authority, excluding the named user. Caller holds am.mu.
func (am *AuthManager) adminCountExcludingLocked(exclude string) int {
	count := 0
	for name, record := range am.users {
		if name == exclude {
			continue
		}
		if record.Enabled && record.hasAdminGrant() {
			count++
		}
	}
	return count
}

// grantsHaveAdmin reports whether an enabled account holding grants passes the one
// last-administrator predicate (authrules.CanAdministerUsers).
func grantsHaveAdmin(grants []Grant) bool {
	return authrules.CanAdministerUsers(true, toRuleGrants(grants))
}

// toRuleGrants is grants as authrules reads them.
func toRuleGrants(grants []Grant) []authrules.Grant {
	rules := make([]authrules.Grant, len(grants))
	for i, grant := range grants {
		rules[i] = authrules.Grant(grant)
	}
	return rules
}

// scopedRuleGrants is a session's grants as authrules reads them.
func scopedRuleGrants(grants []transport.ScopedGrant) []authrules.Grant {
	rules := make([]authrules.Grant, len(grants))
	for i, grant := range grants {
		rules[i] = authrules.Grant(grant)
	}
	return rules
}

// lastAdminRefusal is the refusal of a change that would leave no enabled account able to manage
// users (ring user store spec 6.2, B8). change is the statement, such as `drop user "root"`.
func lastAdminRefusal(change string) error {
	return &sentencedError{cause: ErrLastAdmin, message: change + " was refused: it is the last enabled account that can manage users " +
		"(root, or user-manager on every database). Give another enabled account that authority first, for example " +
		"'grant role user-manager@* to ops', then run the statement again"}
}

// wouldRemoveLastAdminLocked reports whether changing record would leave the
// store with no enabled administrator. It is only meaningful when record is
// currently an enabled admin that the change strips of that authority.
// Caller holds am.mu.
func (am *AuthManager) wouldRemoveLastAdminLocked(record *UserRecord) bool {
	if !record.Enabled || !record.hasAdminGrant() {
		return false
	}
	return am.adminCountExcludingLocked(record.Username) == 0
}

// userListHint is the fix appended to every unknown-user refusal.
const userListHint = "run 'user list' to see the users"

// credentialRefusal wraps a refusal of client-derived credential material so
// codeFor can see ErrCredentialInvalid, and says where a correct credential
// comes from: the server never sees the password, so only a client that
// derives the Argon2id hash can fix it.
func credentialRefusal(operation, username string, cause error) error {
	return fmt.Errorf("%s %q: %w: %w; derive the credential again with the 'hoarddb' CLI or a current driver, which build it correctly",
		operation, username, ErrCredentialInvalid, cause)
}

// checkUsernameNotReserved refuses the name a node presents to its peers
// (NodeUser), because an account with it would make audit rows ambiguous.
func checkUsernameNotReserved(username string) error {
	if username == NodeUser {
		return fmt.Errorf("%w: %q names the cluster's node identity; choose another username", ErrReservedUsername, NodeUser)
	}
	return nil
}

// Grantor is the account asking for a user change, with the grants its
// session holds. A nil *Grantor is the server acting for itself (bootstrap,
// the vector generator, tests) and has no ceiling.
type Grantor struct {
	Username string
	Grants   []transport.ScopedGrant
}

// checkCeiling refuses a change by a grantor who is not root when it would
// reach past the grantor's own roles: every grant it gives, and every grant
// the account it changes already holds, must be one the grantor holds. Without
// it an account administrator could give itself root, or take over a root
// account by setting its password (spec 8.3). Caller holds am.mu, so the
// target's grants cannot change between this check and the write.
func (g *Grantor) checkCeiling(change, target string, targetGrants, given []Grant) error {
	if g == nil {
		return nil
	}
	breach, ok := authrules.CheckCeiling(scopedRuleGrants(g.Grants), toRuleGrants(targetGrants), toRuleGrants(given))
	switch {
	case ok:
		return nil
	case breach.Given:
		return fmt.Errorf("%s: %w: %q cannot give %s@%s, because a user administrator who is not root gives only roles it holds itself, on a database it holds them on (%s holds %s). Ask a root user to run 'grant role %s@%s to %s'",
			change, ErrPermissionDenied, g.Username, breach.Grant.Role, breach.Grant.DB, g.Username, renderGrants(g.Grants), breach.Grant.Role, breach.Grant.DB, target)
	}
	return fmt.Errorf("%s: %w: %q cannot change %q, which holds %s@%s, because a user administrator who is not root changes only accounts whose roles it holds itself (%s holds %s). Ask a root user to make this change",
		change, ErrPermissionDenied, g.Username, target, breach.Grant.Role, breach.Grant.DB, g.Username, renderGrants(g.Grants))
}

// CreateUser adds a user under the writer lock, refusing a duplicate name, a
// reused salt, an out-of-clamp credential, or a store at the supported
// ceiling (spec 6.2, 9). It is the server acting for itself; a client's
// create goes through CreateUserAs.
func (am *AuthManager) CreateUser(username string, material CredentialMaterial, grants []Grant, enabled bool) error {
	return am.CreateUserAs(nil, username, material, grants, enabled)
}

// CreateUserAs is CreateUser on behalf of grantor, who may give only roles it
// holds itself unless it is root.
func (am *AuthManager) CreateUserAs(grantor *Grantor, username string, material CredentialMaterial, grants []Grant, enabled bool) error {
	return am.CreateUserWith(CreateSpec{Grantor: grantor, Username: username, Material: material, Grants: grants, Enabled: enabled})
}

// CreateSpec is one create: who asks, the account, and the drop fence it was read at. It is a
// struct because the operation has six parts (AGENTS.md rule 7).
type CreateSpec struct {
	// Grantor is who asks; nil is the server acting for itself.
	Grantor  *Grantor
	Username string
	Material CredentialMaterial
	Grants   []Grant
	Enabled  bool
	// ExpectDropSeq, when set, is the DropSeq the caller read; the create is refused when a drop
	// has happened since. It is judged after the name-exists check, under the same lock.
	ExpectDropSeq *int64
}

// DropSeq is the store's drop fence: what the next fenced create carries.
func (am *AuthManager) DropSeq() int64 {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.dropSeq
}

// CreateUserWith creates the account spec names, refusing what CreateUserAs refuses and a stale
// drop fence.
func (am *AuthManager) CreateUserWith(spec CreateSpec) error {
	if err := am.refuseInCluster("create user"); err != nil {
		return err
	}
	grantor, username, material, grants, enabled := spec.Grantor, spec.Username, spec.Material, spec.Grants, spec.Enabled
	if username == "" {
		return fmt.Errorf("create user: %w; give the account a name: 'create user <name> roles [reader@<database>]'", ErrUsernameRequired)
	}
	if err := checkUsernameNotReserved(username); err != nil {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	if err := checkUsername(username); err != nil {
		return err
	}
	if err := validateCredentialMaterial(material, am.argon2); err != nil {
		return credentialRefusal("create user", username, err)
	}
	grants, err := checkGrants(grants)
	if err != nil {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	if err := checkGrantCount(fmt.Sprintf("create user %s", clipQuoted(username)), username, len(grants)); err != nil {
		return err
	}
	if err := grantor.checkCeiling(fmt.Sprintf("create user %q", username), username, nil, grants); err != nil {
		return err
	}

	// The client sent its raw Argon2 hash; the stored value is that hash
	// HMAC'd with the current key's pepper (spec 6.2.1). Captured before
	// am.mu because the keyring's prune callback reads am.mu.
	key := am.keyring.Current()
	if len(key.Bytes) == 0 {
		return fmt.Errorf("create user %q: no signing key loaded", username)
	}
	clientHash, err := base64.StdEncoding.DecodeString(material.Hash)
	if err != nil {
		return credentialRefusal("create user", username, fmt.Errorf("the hash is not base64: %w", err))
	}
	pepper := PepperForKey(key, am.deploymentID)
	material.Hash = base64.StdEncoding.EncodeToString(PepperHash(pepper, clientHash))
	material.PepperKID = key.ID

	am.mu.Lock()
	defer am.mu.Unlock()
	if _, exists := am.users[username]; exists {
		return fmt.Errorf("create user %q: %w; choose another name, or change the existing account with 'alter user %s ...'", username, ErrUserExists, username)
	}
	if spec.ExpectDropSeq != nil && *spec.ExpectDropSeq != am.dropSeq {
		return &sentencedError{cause: ErrUserDroppedSince, message: fmt.Sprintf(
			"create user %s was not applied: a user was dropped since the users were read (drop at %d, read at %d). Run 'user describe %s' to see whether it exists, then read the users again and run the statement again.",
			clipQuoted(username), am.dropSeq, *spec.ExpectDropSeq, clipBare(username))}
	}
	if len(am.users) >= maxUsers {
		return fmt.Errorf("create user %q: %w: this store supports %d users; remove accounts before adding more, for example 'drop user <name>' ('user list' shows them)",
			username, ErrTooManyUsers, maxUsers)
	}
	if am.saltInUseLocked(material.Salt) {
		return fmt.Errorf("create user %q: %w: choose a fresh random salt (reusing one salt across accounts makes a single leak a lookup table)",
			username, ErrSaltReused)
	}

	now := time.Now().UTC()
	am.users[username] = &UserRecord{
		Username:          username,
		Roles:             grants,
		Credentials:       material,
		Enabled:           enabled,
		CreatedAt:         now,
		PasswordChangedAt: now,
	}
	if err := am.persistLocked(); err != nil {
		delete(am.users, username)
		return fmt.Errorf("create user %q: %w", username, err)
	}
	return nil
}

// AlterSpec is the set of changes an alter applies. A nil field means
// unchanged; a non-nil Roles replaces the whole set (an empty slice clears
// it). It exists because a sixth positional parameter is unreadable
// (AGENTS.md rule 7) and because "absent" and "empty" have different meanings
// here (spec 12.4).
// AlterSpec is one alter operation: who, what, and the fence it was read at.
// A nil Grants or Enabled means unchanged; a non-nil Grants replaces the whole
// set (an explicitly empty slice clears it). It carries the username and the
// version rather than taking them as further arguments, because the operation
// is the struct (AGENTS.md rule 7) and because "absent" and "empty" have
// different meanings here (spec 12.4).
type AlterSpec struct {
	// Grantor is who asks; nil is the server acting for itself.
	Grantor    *Grantor
	Username   string
	Grants     *[]Grant
	Enabled    *bool
	Version    uint64
	Credential *CredentialMaterial
}

// AlterUser applies an alter to a user. Version is the config version the
// client last saw; a mismatch refuses so a concurrent grant is not silently
// dropped by a replacement (spec 6.1). An already authenticated connection
// keeps its old roles until its next refresh; a disable closes that user's
// sessions from the handler, not from here.
//
// A non-nil Credential is the same client-derived material CreateUser
// accepts, run through the same clamp and pepper, so there is exactly one
// password path (spec 12.3).
//
// The epoch is bumped only when credentials are replaced or the account is
// disabled. A role change reaches a live session at its next refresh through
// MintClaims; bumping the epoch would refuse that refresh and force a password
// re-entry, which is stricter than spec 8.8 promises (spec 3.2, 12.4).
func (am *AuthManager) AlterUser(spec AlterSpec) (uint64, error) {
	if err := am.refuseInCluster("alter user"); err != nil {
		return 0, err
	}
	username := spec.Username
	var newGrants []Grant
	if spec.Grants != nil {
		var err error
		newGrants, err = checkGrants(*spec.Grants)
		if err != nil {
			return 0, fmt.Errorf("alter user %q: %w", username, err)
		}
		if err := checkGrantCount(fmt.Sprintf("alter user %s", clipQuoted(username)), username, len(newGrants)); err != nil {
			return 0, err
		}
	}
	var peppered *CredentialMaterial
	if spec.Credential != nil {
		material := *spec.Credential
		if err := validateCredentialMaterial(material, am.argon2); err != nil {
			return 0, credentialRefusal("alter user", username, err)
		}
		key := am.keyring.Current()
		if len(key.Bytes) == 0 {
			return 0, fmt.Errorf("alter user %q: no signing key loaded", username)
		}
		clientHash, err := base64.StdEncoding.DecodeString(material.Hash)
		if err != nil {
			return 0, credentialRefusal("alter user", username, fmt.Errorf("the hash is not base64: %w", err))
		}
		pepper := PepperForKey(key, am.deploymentID)
		material.Hash = base64.StdEncoding.EncodeToString(PepperHash(pepper, clientHash))
		material.PepperKID = key.ID
		peppered = &material
	}

	am.mu.Lock()
	defer am.mu.Unlock()
	record, ok := am.users[username]
	if !ok {
		return 0, fmt.Errorf("alter user %q: %w; %s", username, ErrUserNotFound, userListHint)
	}
	if spec.Version != am.version {
		return 0, fmt.Errorf("alter user %q: %w: this user store is at version %d, the request carried %d; re-read the user and retry",
			username, ErrUserVersion, am.version, spec.Version)
	}
	if err := spec.Grantor.checkCeiling(fmt.Sprintf("alter user %q", username), username, record.Roles, newGrants); err != nil {
		return 0, err
	}

	// Resolve the prospective record: an absent field leaves the current value
	// alone, and an explicitly empty roles list replaces the set with nothing.
	newRoles := record.Roles
	if spec.Grants != nil {
		newRoles = newGrants
	}
	newEnabled := record.Enabled
	if spec.Enabled != nil {
		newEnabled = *spec.Enabled
	}

	// The last-admin guard considers the prospective state, so it covers both
	// a disable and a role set that strips admin authority.
	if record.Enabled && record.hasAdminGrant() {
		stillAdmin := newEnabled && grantsHaveAdmin(newRoles)
		if !stillAdmin && am.adminCountExcludingLocked(username) == 0 {
			return 0, lastAdminRefusal(fmt.Sprintf("alter user %q", username))
		}
	}
	if peppered != nil && am.saltInUseLocked(peppered.Salt) {
		return 0, fmt.Errorf("alter user %q: %w: choose a fresh random salt (reusing one salt across accounts makes a single leak a lookup table)",
			username, ErrSaltReused)
	}

	credentialsReplaced := peppered != nil
	justDisabled := record.Enabled && !newEnabled
	bumpEpoch := credentialsReplaced || justDisabled

	previousRoles := record.Roles
	previousEnabled := record.Enabled
	previousEpoch := record.Epoch
	previousCredentials := record.Credentials
	previousChanged := record.PasswordChangedAt

	record.Roles = newRoles
	record.Enabled = newEnabled
	if credentialsReplaced {
		record.Credentials = *peppered
		record.PasswordChangedAt = time.Now().UTC()
	}
	previousGeneration := record.LoginGeneration
	if bumpEpoch {
		record.Epoch++
		record.LoginGeneration++
	}
	if err := am.persistLocked(); err != nil {
		record.Roles = previousRoles
		record.Enabled = previousEnabled
		record.Epoch = previousEpoch
		record.LoginGeneration = previousGeneration
		record.Credentials = previousCredentials
		record.PasswordChangedAt = previousChanged
		return 0, fmt.Errorf("alter user %q: %w", username, err)
	}
	if bumpEpoch {
		am.revokeLoginsLocked(username)
	}
	return am.version, nil
}

// DropUser removes a user under the writer lock, refusing when it would
// leave no enabled administrator.
func (am *AuthManager) DropUser(username string) error {
	return am.DropUserAs(nil, username)
}

// DropUserAs is DropUser on behalf of grantor, who may drop only an account
// whose roles it holds itself unless it is root.
func (am *AuthManager) DropUserAs(grantor *Grantor, username string) error {
	if err := am.refuseInCluster("drop user"); err != nil {
		return err
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	record, ok := am.users[username]
	if !ok {
		return fmt.Errorf("drop user %q: %w; %s", username, ErrUserNotFound, userListHint)
	}
	if err := grantor.checkCeiling(fmt.Sprintf("drop user %q", username), username, record.Roles, nil); err != nil {
		return err
	}
	if am.wouldRemoveLastAdminLocked(record) {
		return lastAdminRefusal(fmt.Sprintf("drop user %q", username))
	}

	delete(am.users, username)
	if err := am.persistLocked(); err != nil {
		am.users[username] = record
		return fmt.Errorf("drop user %q: %w", username, err)
	}
	am.dropSeq = max(am.dropSeq+1, int64(am.version))
	am.revokeLoginsLocked(username)
	return nil
}

// checkGrantCount refuses a change that would leave an account holding more than
// wire.MaxUserGrants grants: past it no ring reply could list the account, so the change is
// refused when it is made rather than the next read failing for every reader.
func checkGrantCount(statement, username string, count int) error {
	if count <= wire.MaxUserGrants {
		return nil
	}
	return &sentencedError{cause: ErrTooManyGrants, message: fmt.Sprintf(
		"%s was refused: an account holds at most %d grants and this change would give it %d. Revoke grants it no longer needs first ('revoke role <role>@<database> from %s'), or grant the role on * instead of one database at a time",
		statement, wire.MaxUserGrants, count, clipBare(username))}
}

// clipQuoted quotes at most 64 bytes of a name a refusal repeats, as the cluster's appliers do,
// so the text does not grow with what a caller sent.
func clipQuoted(name string) string {
	if len(name) <= 64 {
		return strconv.Quote(name)
	}
	return strconv.Quote(name[:64]) + "…"
}

// clipBare is a name for a statement a refusal quotes: at most 64 bytes, unquoted.
func clipBare(name string) string {
	if len(name) <= 64 {
		return name
	}
	return name[:64] + "…"
}

// GrantRole adds one grant, idempotently. A role change bumps the epoch so a
// refresh picks up the new authority (spec 5.3.5, 8.8).
func (am *AuthManager) GrantRole(username string, grant Grant) (uint64, error) {
	return am.GrantRoleAs(nil, username, grant)
}

// GrantRoleAs is GrantRole on behalf of grantor, who may give only a role it
// holds, to an account whose roles it holds, unless it is root.
func (am *AuthManager) GrantRoleAs(grantor *Grantor, username string, grant Grant) (uint64, error) {
	if err := am.refuseInCluster("grant role"); err != nil {
		return 0, err
	}
	normalized, err := checkGrants([]Grant{grant})
	if err != nil {
		return 0, fmt.Errorf("grant role %s@%s to %q: %w", grant.Role, grant.DB, username, err)
	}
	grant = normalized[0]
	am.mu.Lock()
	defer am.mu.Unlock()
	record, ok := am.users[username]
	if !ok {
		return 0, fmt.Errorf("grant role %s@%s to %q: %w; %s", grant.Role, grant.DB, username, ErrUserNotFound, userListHint)
	}
	if err := grantor.checkCeiling(fmt.Sprintf("grant role %s@%s to %q", grant.Role, grant.DB, username), username, record.Roles, []Grant{grant}); err != nil {
		return 0, err
	}
	for _, existing := range record.Roles {
		if existing == grant {
			return am.version, nil
		}
	}
	if err := checkGrantCount(fmt.Sprintf("grant role %s@%s to %s", grant.Role, grant.DB, clipQuoted(username)), username, len(record.Roles)+1); err != nil {
		return 0, err
	}
	record.Roles = append(record.Roles, grant)
	record.Epoch++
	if err := am.persistLocked(); err != nil {
		record.Roles = record.Roles[:len(record.Roles)-1]
		record.Epoch--
		return 0, fmt.Errorf("grant role %s@%s to %q: %w", grant.Role, grant.DB, username, err)
	}
	return am.version, nil
}

// RevokeRole removes one grant, refusing when it is the last admin grant of
// the last enabled administrator.
func (am *AuthManager) RevokeRole(username string, grant Grant) (uint64, error) {
	return am.RevokeRoleAs(nil, username, grant)
}

// RevokeRoleAs is RevokeRole on behalf of grantor, who may change only an
// account whose roles it holds itself unless it is root.
func (am *AuthManager) RevokeRoleAs(grantor *Grantor, username string, grant Grant) (uint64, error) {
	if err := am.refuseInCluster("revoke role"); err != nil {
		return 0, err
	}
	// A retired name is refused with its replacement, but an unknown role is
	// not: a role stored as free text must stay revocable.
	if _, renamed := authrules.RenamedRole(grant.Role); renamed {
		return 0, fmt.Errorf("revoke role %s@%s from %q: %w", grant.Role, grant.DB, username, errUnknownRole(grant.Role))
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	record, ok := am.users[username]
	if !ok {
		return 0, fmt.Errorf("revoke role %s@%s from %q: %w; %s", grant.Role, grant.DB, username, ErrUserNotFound, userListHint)
	}
	if err := grantor.checkCeiling(fmt.Sprintf("revoke role %s@%s from %q", grant.Role, grant.DB, username), username, record.Roles, nil); err != nil {
		return 0, err
	}
	// Every copy goes: a store written before duplicates were collapsed can
	// hold the same grant twice, and removing one would leave the access.
	remaining := make([]Grant, 0, len(record.Roles))
	for _, existing := range record.Roles {
		if existing != grant {
			remaining = append(remaining, existing)
		}
	}
	if len(remaining) == len(record.Roles) {
		return am.version, nil
	}
	if record.Enabled && record.hasAdminGrant() && !grantsHaveAdmin(remaining) && am.adminCountExcludingLocked(username) == 0 {
		return 0, lastAdminRefusal(fmt.Sprintf("revoke role %s@%s from %q", grant.Role, grant.DB, username))
	}

	previousRoles := record.Roles
	previousEpoch := record.Epoch
	record.Roles = remaining
	record.Epoch++
	if err := am.persistLocked(); err != nil {
		record.Roles = previousRoles
		record.Epoch = previousEpoch
		return 0, fmt.Errorf("revoke role %s@%s from %q: %w", grant.Role, grant.DB, username, err)
	}
	return am.version, nil
}

// UserInfos returns every user as the wire-safe shape, ordered by name. It
// never includes credential material.
func (am *AuthManager) UserInfos() []UserInfo {
	am.mu.RLock()
	defer am.mu.RUnlock()
	infos := make([]UserInfo, 0, len(am.users))
	for _, record := range am.users {
		infos = append(infos, userInfo(record))
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Username < infos[j].Username })
	return infos
}

// UserInfo returns one user's wire-safe shape.
func (am *AuthManager) UserInfo(username string) (UserInfo, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	record, ok := am.users[username]
	if !ok {
		return UserInfo{}, false
	}
	return userInfo(record), true
}

// userInfo flattens a record into the wire shape.
func userInfo(record *UserRecord) UserInfo {
	roles := make([]UserRole, 0, len(record.Roles))
	for _, grant := range record.Roles {
		roles = append(roles, UserRole(grant))
	}
	return UserInfo{
		Username:          record.Username,
		Roles:             roles,
		Enabled:           record.Enabled,
		Epoch:             record.Epoch,
		CreatedAt:         record.CreatedAt,
		PasswordChangedAt: record.PasswordChangedAt,
	}
}

// Version returns the current config version. Callers use it to present the
// optimistic-concurrency token for a later alter.
func (am *AuthManager) Version() uint64 {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.version
}
