package cli

import (
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/termio"
	"github.com/hoarddb/hoarddb-server/transport"
)

// resolveNewPassword returns the new password for user creation or change.
// It never accepts a password on the command line. A terminal is prompted
// twice with no echo; a piped session takes the password from the lines that
// follow the statement in the script (one line with --password-stdin, two
// otherwise); --password-file (0600 expected) works everywhere (spec 6.3).
func (h *CommandHandler) resolveNewPassword() (string, error) {
	switch {
	case h.passwordStdin:
		return h.passwordFromScript(1)
	case h.passwordFile != "":
		return h.passwordFromFile()
	case h.scriptLines != nil:
		return h.passwordFromScript(2)
	}
	return termio.ReadPasswordTwice("New password: ", "Repeat new password: ")
}

// newUserCredential reads the new password for username, refuses one shorter
// than the server's minimum, and derives the credential locally: only the
// derived material leaves this process.
func (h *CommandHandler) newUserCredential(username string) (server.UserCredential, error) {
	password, err := h.resolveNewPassword()
	if err != nil {
		return server.UserCredential{}, err
	}
	if err := h.checkPasswordMinimum(username, password); err != nil {
		return server.UserCredential{}, err
	}
	material, err := server.NewCredential(password, server.DefaultArgon2Params())
	if err != nil {
		return server.UserCredential{}, fmt.Errorf("deriving the credential for user %q: %w", username, err)
	}
	return server.UserCredential{
		Algo: material.Algo, Salt: material.Salt, Hash: material.Hash, Params: material.Params,
	}, nil
}

// checkPasswordMinimum applies the server's password_min_length. The server
// cannot: the wire request carries only Argon2id material, never the password
// (BUG-0493). The number is the one the server reported, so
// HOARDDB_PASSWORD_MIN_LENGTH on the server is the single source of the
// policy. Length is counted in characters, not bytes, which is what the
// documentation and the operator mean by it.
func (h *CommandHandler) checkPasswordMinimum(username, password string) error {
	if h.passwordMinLength <= 0 {
		return refusedf("the server did not report its minimum password length, so the new password for user %q cannot be checked "+
			"and nothing was changed; upgrade the server to the version of this CLI", username)
	}
	if length := server.PasswordCharacters(password); length < h.passwordMinLength {
		return refusedf("the password for user %q is %d characters; this server requires at least %d. "+
			"Choose a longer password, or ask an administrator to change HOARDDB_PASSWORD_MIN_LENGTH; nothing was changed",
			username, length, h.passwordMinLength)
	}
	return nil
}

// passwordFromScript reads count password lines from the statement stream of a
// piped session and requires them to agree.
func (h *CommandHandler) passwordFromScript(count int) (string, error) {
	if h.scriptLines == nil {
		return "", usagef("--password-stdin reads the line after the statement in a piped script, and this session is a terminal; " +
			"leave the flag off to be prompted without echo, or use --password-file <0600 file>")
	}
	entries := make([]string, 0, count)
	for len(entries) < count {
		line, err := h.scriptLines()
		if err != nil {
			return "", usagef("the script ends where the new password should be (%d line(s) expected after the statement); "+
				"add the password line(s) after the statement, or use --password-file <0600 file>", count)
		}
		entries = append(entries, strings.TrimRight(line, "\r\n"))
	}
	for _, entry := range entries[1:] {
		if entry != entries[0] {
			return "", usagef("the next %d line(s) of the script were read as the password and do not agree, so nothing was changed; "+
				"if the script has no password lines, those lines were statements: put the password line(s) after the statement, "+
				"or use --password-file <0600 file>", count)
		}
	}
	if entries[0] == "" {
		return "", usagef("the next line of the script was read as the password and is empty, so nothing was changed; " +
			"if the script has no password lines, add them after the statement, or use --password-file <0600 file>")
	}
	return entries[0], nil
}

// passwordFromFile reads the new password from --password-file.
func (h *CommandHandler) passwordFromFile() (string, error) {
	info, err := os.Stat(h.passwordFile)
	if err != nil {
		return "", usagef("read password file: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		h.notef("warning: password file %s is mode %04o, readable beyond its owner\n", h.passwordFile, perm)
	}
	data, err := os.ReadFile(h.passwordFile)
	if err != nil {
		return "", usagef("read password file: %v", err)
	}
	password := strings.TrimSpace(string(data))
	if password == "" {
		return "", usagef("the password file %s is empty", h.passwordFile)
	}
	return password, nil
}

// newRequestID returns a UUIDv4 string for the master's request_id dedup.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// rolesFromCommand parses the bracketed roles list from a command option.
func rolesFromCommand(cmd *Cmd) ([]server.UserRole, error) {
	raw, ok := cmd.Options["roles"]
	if !ok {
		return nil, nil
	}
	expr := strings.TrimSpace(fmt.Sprint(raw))
	expr = strings.TrimPrefix(expr, "[")
	expr = strings.TrimSuffix(expr, "]")
	var roles []server.UserRole
	for _, entry := range strings.Split(expr, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		spec, err := parseRoleSpec(entry)
		if err != nil {
			return nil, usagef("%v", err)
		}
		roles = append(roles, server.UserRole{Role: spec.Role, DB: spec.DB})
	}
	return roles, nil
}

// printUserInfo renders one user without any credential material.
func (h *CommandHandler) printUserInfo(user server.UserInfo, indent string) {
	status := "enabled"
	if !user.Enabled {
		status = "disabled"
	}
	roles := make([]string, 0, len(user.Roles))
	for _, role := range user.Roles {
		roles = append(roles, role.Role+"@"+role.DB)
	}
	sort.Strings(roles)
	if len(roles) == 0 {
		roles = []string{"(none)"}
	}
	h.printf("%s%s: %s, roles [%s], epoch %d\n", indent, terminalSafe(user.Username), status, terminalSafe(strings.Join(roles, ", ")), user.Epoch)
}

// handleUserList prints every user, the master, and the config version.
func (h *CommandHandler) handleUserList() error {
	var resp server.UserListResponse
	if err := h.send(transport.OpUserList, &server.UserListRequest{}, &resp); err != nil {
		return fmt.Errorf("listing users: %w", err)
	}
	h.printf("USERS (master: %s, config version %d)\n", masterLabel(resp), resp.Version)
	h.printf("---\n")
	if len(resp.Users) == 0 {
		h.printf("(no users)\n")
		return nil
	}
	for _, user := range resp.Users {
		h.printUserInfo(user, "")
	}
	return nil
}

// handleUserDescribe prints one user without any credential material.
func (h *CommandHandler) handleUserDescribe(cmd *Cmd) error {
	var resp server.UserGetResponse
	if err := h.send(transport.OpUserGet, &server.UserGetRequest{Username: cmd.Name}, &resp); err != nil {
		return fmt.Errorf("describing user %q: %w", cmd.Name, err)
	}
	if !resp.Found {
		return refusedf("user %q not found; run 'user list' to see the users", cmd.Name)
	}
	h.printf("user:  %s\n", terminalSafe(resp.User.Username))
	status := "enabled"
	if !resp.User.Enabled {
		status = "disabled"
	}
	h.printf("status: %s\n", status)
	roles := make([]string, 0, len(resp.User.Roles))
	for _, role := range resp.User.Roles {
		roles = append(roles, role.Role+"@"+role.DB)
	}
	sort.Strings(roles)
	h.printf("roles:  [%s]\n", terminalSafe(strings.Join(roles, ", ")))
	h.printf("epoch:  %d\n", resp.User.Epoch)
	h.printf("created: %s\n", resp.User.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	h.printf("password changed: %s\n", resp.User.PasswordChangedAt.Format("2006-01-02T15:04:05Z07:00"))
	return nil
}

// handleUserCreate prompts for (or reads) a password, checks it against the
// server's minimum, derives the credential locally, and sends only the derived
// material.
func (h *CommandHandler) handleUserCreate(cmd *Cmd) error {
	roles, err := rolesFromCommand(cmd)
	if err != nil {
		return err
	}
	credential, err := h.newUserCredential(cmd.Name)
	if err != nil {
		return err
	}
	req := &server.UserCreateRequest{
		RequestID:  newRequestID(),
		Username:   cmd.Name,
		Credential: credential,
		Roles:      roles,
	}
	var resp server.UserMutationResponse
	if err := h.send(transport.OpUserCreate, req, &resp); err != nil {
		return fmt.Errorf("creating user %q: %w", cmd.Name, err)
	}
	if !resp.Success {
		return refusedf("creating user %q: %s", cmd.Name, resp.Message)
	}
	h.printf("user %q created (config version %d)\n", cmd.Name, resp.Version)
	return nil
}

// handleUserAlter enables/disables a user or replaces its role set. It reads
// the current version first so the optimistic-concurrency token is current.
func (h *CommandHandler) handleUserAlter(cmd *Cmd) error {
	// detail keeps the case the user typed because the roles expression is cut
	// from it: role and database names are case-sensitive. Only keyword is
	// folded, for matching disable, enable, password and roles.
	detail := strings.TrimSpace(fmt.Sprint(cmd.Options["detail"]))
	keyword := strings.ToLower(detail)
	var current server.UserGetResponse
	if err := h.send(transport.OpUserGet, &server.UserGetRequest{Username: cmd.Name}, &current); err != nil {
		return fmt.Errorf("reading user %q: %w", cmd.Name, err)
	}
	if !current.Found {
		return refusedf("user %q not found; run 'user list' to see the users", cmd.Name)
	}

	req := &server.UserAlterRequest{
		RequestID: newRequestID(), Username: cmd.Name, Version: current.Version,
	}
	var wanted []string
	switch {
	case keyword == "disable":
		disabled := false
		req.Enabled = &disabled
		wanted = append(wanted, "enabled")
	case keyword == "enable":
		enable := true
		req.Enabled = &enable
		wanted = append(wanted, "enabled")
	case keyword == "password":
		credential, err := h.newUserCredential(cmd.Name)
		if err != nil {
			return err
		}
		req.Credentials = &credential
		wanted = append(wanted, "credentials")
	case strings.HasPrefix(keyword, "roles"):
		expr := strings.TrimSpace(detail[len("roles"):])
		if !strings.HasPrefix(expr, "[") || !strings.HasSuffix(expr, "]") {
			return usagef("roles must be a bracketed list, for example: roles [writer@app, reader@logs]")
		}
		parsed, err := rolesFromCommand(&Cmd{Options: map[string]interface{}{"roles": expr}})
		if err != nil {
			return err
		}
		req.Roles = &parsed
		wanted = append(wanted, "roles")
	default:
		return usagef("usage: alter user <name> disable|enable|password|roles [role@db, ...]")
	}

	var resp server.UserMutationResponse
	if err := h.send(transport.OpUserAlter, req, &resp); err != nil {
		return fmt.Errorf("altering user %q: %w", cmd.Name, err)
	}
	if !resp.Success {
		return refusedf("altering user %q: %s", cmd.Name, resp.Message)
	}
	// A server that does not know a field cannot name it in Changed. An older
	// server ignores the unknown credentials field and answers success, so a
	// password change would no-op silently during a rolling upgrade; requiring
	// our own field in the echo is what makes that visible (spec 6.1 item 3).
	// The duplicate-request-id response is the one legitimate case where the
	// fields are not re-applied.
	if !strings.HasPrefix(resp.Message, "duplicate request ignored") {
		if missing := missingFields(resp.Changed, wanted); len(missing) > 0 {
			return refusedf("altering user %q: the server did not confirm changing %s (it reported %v); this server may predate the field, so nothing was applied",
				cmd.Name, strings.Join(missing, ", "), resp.Changed)
		}
	}
	h.printf("user %q altered (config version %d)\n", cmd.Name, resp.Version)
	return nil
}

// missingFields returns the wanted fields absent from changed. A server that
// does not know a field cannot name it, so a missing entry means the change
// was not applied (spec 6.1 item 3).
func missingFields(changed, wanted []string) []string {
	present := make(map[string]struct{}, len(changed))
	for _, field := range changed {
		present[field] = struct{}{}
	}
	var missing []string
	for _, field := range wanted {
		if _, ok := present[field]; !ok {
			missing = append(missing, field)
		}
	}
	return missing
}

// handleUserDrop removes a user.
func (h *CommandHandler) handleUserDrop(cmd *Cmd) error {
	req := &server.UserDropRequest{RequestID: newRequestID(), Username: cmd.Name}
	var resp server.UserMutationResponse
	if err := h.send(transport.OpUserDrop, req, &resp); err != nil {
		return fmt.Errorf("dropping user %q: %w", cmd.Name, err)
	}
	if !resp.Success {
		return refusedf("dropping user %q: %s", cmd.Name, resp.Message)
	}
	h.printf("user %q dropped\n", cmd.Name)
	return nil
}

// handleUserGrant adds one grant.
func (h *CommandHandler) handleUserGrant(cmd *Cmd) error {
	spec, err := parseRoleSpec(fmt.Sprint(cmd.Options["role"]))
	if err != nil {
		return usagef("%v", err)
	}
	req := &server.UserGrantRequest{
		RequestID: newRequestID(), Username: cmd.Name, Role: server.UserRole{Role: spec.Role, DB: spec.DB},
	}
	var resp server.UserMutationResponse
	if err := h.send(transport.OpUserGrant, req, &resp); err != nil {
		return fmt.Errorf("granting %s@%s to %q: %w", spec.Role, spec.DB, cmd.Name, err)
	}
	if !resp.Success {
		return refusedf("granting %s@%s to %q: %s", spec.Role, spec.DB, cmd.Name, resp.Message)
	}
	h.printf("granted %s@%s to %q (config version %d)\n", spec.Role, spec.DB, cmd.Name, resp.Version)
	return nil
}

// handleUserRevoke removes one grant.
func (h *CommandHandler) handleUserRevoke(cmd *Cmd) error {
	spec, err := parseRoleSpec(fmt.Sprint(cmd.Options["role"]))
	if err != nil {
		return usagef("%v", err)
	}
	req := &server.UserRevokeRequest{
		RequestID: newRequestID(), Username: cmd.Name, Role: server.UserRole{Role: spec.Role, DB: spec.DB},
	}
	var resp server.UserMutationResponse
	if err := h.send(transport.OpUserRevoke, req, &resp); err != nil {
		return fmt.Errorf("revoking %s@%s from %q: %w", spec.Role, spec.DB, cmd.Name, err)
	}
	if !resp.Success {
		return refusedf("revoking %s@%s from %q: %s", spec.Role, spec.DB, cmd.Name, resp.Message)
	}
	h.printf("revoked %s@%s from %q (config version %d)\n", spec.Role, spec.DB, cmd.Name, resp.Version)
	return nil
}

// handleMaster prints the node that serves user changes.
func (h *CommandHandler) handleMaster() error {
	var resp server.UserListResponse
	if err := h.send(transport.OpUserList, &server.UserListRequest{}, &resp); err != nil {
		return fmt.Errorf("finding the cluster master: %w", err)
	}
	h.printf("cluster master for user changes: %s\n", masterLabel(resp))
	return nil
}

// masterLabel names the master and where a client connects to it. The server
// leaves the address empty until the master's own word on it has reached the
// node answering, and a ring address is never shown in its place: nothing
// answers a client there (BUG-0709).
func masterLabel(resp server.UserListResponse) string {
	if resp.MasterAddr == "" {
		if resp.MasterAddrNote != "" {
			return terminalSafe(resp.Master) + " (no address to give: " + terminalSafe(resp.MasterAddrNote) + ")"
		}
		return terminalSafe(resp.Master) + " (its client address has not reached this node yet; run 'master' again in a few seconds)"
	}
	return terminalSafe(resp.Master) + " at " + terminalSafe(resp.MasterAddr)
}
