// Package cli provides the interactive command-line interface for hoardDB.
//
// The parser implements HQL (hoardDB Query Language), a document query language
// with store-type-native operations.
package cli

import (
	"fmt"
	"strings"
)

// Cmd represents a parsed HQL command.
type Cmd struct {
	// Type is one of: global, bucket, admin, user.
	Type string

	// Database is the target database name (set via "use <db>").
	Database string

	// Bucket is the dot-separated bucket path (e.g., "Post.Comments").
	Bucket string

	// Method is the operation to perform (e.g., "insert", "push", "pop").
	Method string

	// Name is used for create/drop/alter/grant/revoke targets (user, db, bucket names).
	Name string

	// Args holds positional arguments (e.g., document bodies, filter objects).
	Args []string

	// Options holds named options parsed from the command (e.g., {upsert: true}).
	Options map[string]interface{}

	// DocKey, Field and Path address a document's store field:
	// db.Post["p1"].Comments.push({...}) is DocKey p1, Field Comments; an L3
	// store, db.Post["p1"].Comments[3].Flags.pop(), adds Path [Comments, 3]
	// and Field Flags. DocKey is empty for every other bucket command.
	DocKey string
	Field  string
	Path   []interface{}
}

// secondStatement reports the text after a ';' that sits outside quotes and
// brackets and is followed by more than whitespace and further semicolons. A
// bare trailing ';' ends a statement; anything after one starts another, which a
// line does not support.
func secondStatement(line string) (string, bool) {
	var quote byte
	depth := 0
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '\'':
			quote = c
		case c == '{' || c == '[' || c == '(':
			depth++
		case (c == '}' || c == ']' || c == ')') && depth > 0:
			depth--
		case c == ';' && depth == 0:
			rest := strings.TrimSpace(strings.TrimLeft(line[i:], "; \t"))
			if rest != "" {
				return strings.TrimSpace(strings.TrimRight(rest, "; \t")), true
			}
			return "", false
		}
	}
	return "", false
}

// ParseCommand parses an HQL command line into a Cmd struct.
// It handles the following forms:
//
//	Global: use <db>, show databases/buckets/users, describe, status, help, exit, quit
//	Bucket: db.<bucket>.<method>(args)
//	Admin:  create/drop database <name>, define/drop bucket <name>
//	User:   create/alter/drop user <name>, grant/revoke <role> to/from <user>
func ParseCommand(line string) (*Cmd, error) {
	// Trim whitespace and trailing semicolons.
	line = strings.TrimSpace(line)
	if second, found := secondStatement(line); found {
		return nil, fmt.Errorf("one statement per line: put %q on its own line (several statements on one line are not supported yet)", second)
	}
	line = strings.TrimRight(line, ";")
	if line == "" {
		return nil, fmt.Errorf("empty command")
	}

	// Remove trailing semicolon after trimming whitespace.
	if i := strings.LastIndexByte(line, ';'); i == len(line)-1 {
		line = line[:i]
		line = strings.TrimSpace(line)
	}

	words := strings.Fields(line)
	if len(words) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	if isBatchStatement(line) {
		return parseBatch(line)
	}

	// Lowercase the first word for keyword matching.
	keyword := strings.ToLower(words[0])

	switch keyword {
	case "use":
		return parseUse(words)
	case "show":
		return parseShow(words)
	case "describe":
		return parseDescribe(words)
	case "status":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "status"}, nil
	case "encryption":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "encryption"}, nil
	case "version":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "version"}, nil
	case "help":
		return &Cmd{Type: "global", Method: "help", Args: words[1:]}, nil
	case "exit", "quit":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "exit"}, nil
	case "nodes":
		if len(words) > 2 || (len(words) == 2 && !strings.EqualFold(words[1], "history")) {
			return nil, usagef("usage: nodes [history] (got %q)", strings.Join(words[1:], " "))
		}
		return &Cmd{Type: "global", Method: "nodes", Args: words[1:]}, nil
	case "join":
		return parseJoin(words)
	case "migrate":
		return parseMigrate(words)
	case "consistent-get":
		return parseConsistentGet(words)
	case "write-concern":
		return parseWriteConcern(words)
	case "set":
		return parseSet(words)
	case "remove-node":
		return parseRemoveNode(words)
	case "user":
		return parseUser(words)
	case "master":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "master"}, nil
	case "theme":
		if err := refuseTrailingWords(words); err != nil {
			return nil, err
		}
		return &Cmd{Type: "global", Method: "theme"}, nil
	case "create":
		return parseCreate(words)
	case "drop":
		return parseDrop(words)
	case "alter":
		return parseAlter(words)
	case "disable":
		return parseDisableEnable(words, false)
	case "enable":
		return parseDisableEnable(words, true)
	case "grant":
		return parseGrant(words)
	case "revoke":
		return parseRevoke(words)
	case "define":
		// define bucket was renamed to create bucket. This arm stays a loud,
		// named error rather than falling through to "unknown command: define":
		// the docs are unreleased, so there is no compatibility promise, but a
		// rename that answers silence is how AGENTS.md rule 10 gets broken.
		return nil, fmt.Errorf("define was renamed: use 'create bucket <name> { type: hash }'\n(run 'help' for the full command list)")
	case "connect":
		return parseConnect(words)
	case "dump":
		return &Cmd{Type: "dump", Args: words[1:]}, nil
	case "restore":
		return &Cmd{Type: "restore", Args: words[1:]}, nil
	default:
		// Try bucket method: db.<bucket>.<method>(args)
		if strings.Contains(line, ".") {
			return parseBucketMethod(line)
		}
		return nil, fmt.Errorf("unknown command: %s", keyword)
	}
}

// refuseTrailingWords is the check for a statement that takes no arguments: a
// stray word is refused, naming it, rather than ignored.
func refuseTrailingWords(words []string) error {
	if len(words) > 1 {
		return usagef("usage: %s takes no arguments (got %q)", strings.ToLower(words[0]), strings.Join(words[1:], " "))
	}
	return nil
}

// readInClause reads the optional `in <database>` clause at words[at]. It
// returns the database ("" when there is no clause) and the words after the
// clause, so show, create, describe, drop and alter all read the clause the same
// way. A dangling `in` with no database is refused with the caller's usage line
// rather than being taken for part of the statement's body.
func readInClause(words []string, at int, usage string) (database string, rest []string, err error) {
	if len(words) <= at {
		return "", nil, nil
	}
	if !strings.EqualFold(words[at], "in") {
		return "", words[at:], nil
	}
	if len(words) <= at+1 {
		return "", nil, usagef("%s", usage)
	}
	return words[at+1], words[at+2:], nil
}

// parseUse handles: use <database>
func parseUse(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: use <database>")
	}
	if len(words) > 2 {
		return nil, usagef("usage: use <database> takes one name (got %q)", strings.Join(words[1:], " "))
	}
	return &Cmd{
		Type:     "global",
		Method:   "use",
		Database: words[1],
	}, nil
}

// parseShow handles: show databases|buckets|users [in <database>]
func parseShow(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: show databases|buckets|users [in <database>]")
	}

	target := strings.ToLower(words[1])
	cmd := &Cmd{Type: "global", Method: "show"}

	switch target {
	case "databases", "dbs", "db":
		cmd.Args = []string{"databases"}
	case "buckets":
		cmd.Args = []string{"buckets"}
		database, _, err := readInClause(words, 2, "usage: show buckets [in <database>]")
		if err != nil {
			return nil, err
		}
		cmd.Database = database
	case "users":
		cmd.Args = []string{"users"}
	case "banner":
		cmd.Args = []string{"banner"}
	default:
		return nil, fmt.Errorf("unknown show target: %s (use databases, buckets, users, or banner)", target)
	}

	return cmd, nil
}

// describeBucketUsage is the line printed for a describe bucket statement that
// does not parse.
const describeBucketUsage = "usage: describe bucket <name> [in <database>]"

// parseDescribe handles: describe bucket <name> [in <database>] and describe database <name>
func parseDescribe(words []string) (*Cmd, error) {
	if len(words) < 3 {
		return nil, fmt.Errorf("%s", describeBucketUsage)
	}
	target := strings.ToLower(words[1])
	switch target {
	case "bucket":
		database, rest, err := readInClause(words, 3, describeBucketUsage)
		if err != nil {
			return nil, err
		}
		if len(rest) > 0 {
			return nil, usagef("%s (got %q after the name)", describeBucketUsage, strings.Join(rest, " "))
		}
		return &Cmd{
			Type:     "global",
			Method:   "describe",
			Database: database,
			Name:     words[2],
			Args:     []string{target},
		}, nil
	case "database":
		return &Cmd{
			Type:   "global",
			Method: "describe",
			Name:   words[2],
			Args:   []string{target},
		}, nil
	default:
		return nil, fmt.Errorf("unknown describe target: %s", target)
	}
}

// parseCreate handles:
//
//	create database <name>
//	create bucket <name> [in <db>] { … }
//	create user "<name>" with password "<pwd>" { roles: [...], databases: [...] }
func parseCreate(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: create database <name>, create bucket <name> { ... }, or create user <name>")
	}

	target := strings.ToLower(words[1])
	switch target {
	case "database", "db":
		if len(words) < 3 {
			return nil, fmt.Errorf("usage: create database <name>")
		}
		if err := refuseDatabaseReplicationFactor(words[2], words[3:]); err != nil {
			return nil, err
		}
		return &Cmd{
			Type:     "admin",
			Method:   "create_database",
			Database: words[2],
		}, nil
	case "bucket":
		return parseCreateBucket(words)
	case "user":
		return parseCreateUser(words)
	default:
		return nil, fmt.Errorf("unknown create target: %s", target)
	}
}

// refuseDatabaseReplicationFactor rejects `create database <name> with
// replication_factor N` in this (non-ring) session. The clause used to be dropped before any
// handler saw it, so the command reported success and the operator believed a replica set
// existed. The message points at the ring session, where the factor is a database option.
func refuseDatabaseReplicationFactor(name string, rest []string) error {
	for _, word := range rest {
		if !strings.EqualFold(strings.Trim(word, ",:"), "replication_factor") {
			continue
		}
		return fmt.Errorf(
			"replication_factor is not a setting of this session: the clause was accepted and ignored, so database %q would have been created with no extra copies. "+
				"Copies belong to the ring: run 'create database %s { replication_factor: N }'. "+
				"This server has no per-node replication factor any more: HOARDDB_REPLICATION_FACTOR above 1 is refused at start",
			name, name)
	}
	return nil
}

// parseCreateBucket handles: create bucket <name> [in <db>] { … }. The
// optional `in <db>` names the database directly, so the promise the two
// server-side recovery messages make ("create bucket <name> in <db>") is a
// command the parser actually accepts. Without it the bucket goes to the
// session's current database, exactly as define did.
func parseCreateBucket(words []string) (*Cmd, error) {
	name := ""
	if len(words) > 2 {
		name = words[2]
	}
	if name == "" || strings.HasPrefix(name, "{") {
		return nil, fmt.Errorf("usage: create bucket <name> { type: %s }", storeTypeList())
	}

	database, rest, err := readInClause(words, 3, "usage: create bucket <name> [in <database>] { type: "+storeTypeList()+" }")
	if err != nil {
		return nil, err
	}
	detail := strings.Join(rest, " ")

	return &Cmd{
		Type:     "admin",
		Method:   "create_bucket",
		Database: database,
		Name:     name,
		Args:     []string{name},
		Options:  map[string]interface{}{"schema": detail},
	}, nil
}

// parseCreateUser handles: create user <name> roles [role@db, ...]. Passwords
// are never part of the command line: the CLI prompts twice, or reads
// --password-file/--password-stdin, so a credential never lands in shell
// history or process arguments (spec 6.3).
func parseCreateUser(words []string) (*Cmd, error) {
	if len(words) < 3 {
		return nil, fmt.Errorf("usage: create user <name> [roles [role@db, ...]]")
	}
	name := strings.Trim(words[2], "\"'")
	cmd := &Cmd{Type: "user", Method: "create_user", Name: name, Args: []string{name}}

	rest := strings.TrimSpace(strings.Join(words[3:], " "))
	if rest == "" {
		return cmd, nil
	}
	if !strings.HasPrefix(strings.ToLower(rest), "roles") {
		return nil, fmt.Errorf("usage: create user <name> roles [role@db, ...]")
	}
	rolesExpr := strings.TrimSpace(rest[len("roles"):])
	if !strings.HasPrefix(rolesExpr, "[") || !strings.HasSuffix(rolesExpr, "]") {
		return nil, fmt.Errorf("roles must be a bracketed list, for example: roles [writer@app, reader@logs]")
	}
	cmd.Options = map[string]interface{}{"roles": rolesExpr}
	return cmd, nil
}

// parseUser handles: user list, user describe <name>.
func parseUser(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: user list | user describe <name>")
	}
	switch strings.ToLower(words[1]) {
	case "list":
		return &Cmd{Type: "user", Method: "list_users"}, nil
	case "describe":
		if len(words) < 3 {
			return nil, fmt.Errorf("usage: user describe <name>")
		}
		name := strings.Trim(words[2], "\"'")
		return &Cmd{Type: "user", Method: "describe_user", Name: name, Args: []string{name}}, nil
	default:
		return nil, fmt.Errorf("unknown user command %q; try 'user list' or 'user describe <name>'", words[1])
	}
}

// dropBucketUsage is the line printed for a drop bucket statement that does
// not parse.
const dropBucketUsage = "usage: drop bucket <name> [in <database>]"

// parseDrop handles:
//
//	drop database <name>
//	drop bucket <name> [in <database>]
//	drop user "<name>"
func parseDrop(words []string) (*Cmd, error) {
	if len(words) < 3 {
		return nil, fmt.Errorf("usage: drop database|bucket <name> | drop user <name>")
	}

	target := strings.ToLower(words[1])
	switch target {
	case "database", "db":
		return &Cmd{
			Type:     "admin",
			Method:   "drop_database",
			Database: words[2],
		}, nil
	case "bucket":
		database, rest, err := readInClause(words, 3, dropBucketUsage)
		if err != nil {
			return nil, err
		}
		if len(rest) > 0 {
			return nil, usagef("%s (got %q after the name)", dropBucketUsage, strings.Join(rest, " "))
		}
		return &Cmd{
			Type:     "admin",
			Method:   "drop_bucket",
			Database: database,
			Name:     words[2],
		}, nil
	case "user":
		// May have quoted or unquoted name.
		name := words[2]
		name = strings.Trim(name, "\"'")
		return &Cmd{
			Type:   "user",
			Method: "drop_user",
			Name:   name,
			Args:   []string{name},
		}, nil
	default:
		return nil, fmt.Errorf("unknown drop target: %s", target)
	}
}

// parseAlter handles:
//
//	alter user "<name>" password "<new_password>"
//	alter user "<name>" roles [<roles>]
//	alter user "<name>" databases [<databases>]
func parseAlter(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: alter user <name> <field> <value>")
	}

	target := strings.ToLower(words[1])
	switch target {
	case "user":
		if len(words) < 4 {
			return nil, fmt.Errorf("usage: alter user <name> disable|enable|roles [role@db, ...]")
		}
		username := strings.Trim(words[2], "\"'")
		return &Cmd{
			Type:    "user",
			Method:  "alter_user",
			Name:    username,
			Args:    []string{username},
			Options: map[string]interface{}{"detail": strings.Join(words[3:], " ")},
		}, nil
	case "bucket":
		const alterBucketUsage = "usage: alter bucket <name> [in <database>] { Tags hashStore }"
		if len(words) < 4 {
			return nil, fmt.Errorf("%s", alterBucketUsage)
		}
		database, rest, err := readInClause(words, 3, alterBucketUsage)
		if err != nil {
			return nil, err
		}
		if len(rest) == 0 {
			return nil, fmt.Errorf("%s", alterBucketUsage)
		}
		return &Cmd{
			Type:     "admin",
			Method:   "alter_bucket",
			Database: database,
			Name:     words[2],
			Options:  map[string]interface{}{"detail": strings.Join(rest, " ")},
		}, nil
	default:
		return nil, fmt.Errorf("unknown alter target: %s", target)
	}
}

// parseDisableEnable handles: disable/enable user "<name>"
func parseDisableEnable(words []string, enable bool) (*Cmd, error) {
	if len(words) < 3 {
		return nil, fmt.Errorf("usage: %s user <name>", words[0])
	}

	target := strings.ToLower(words[1])
	if target != "user" {
		return nil, fmt.Errorf("expected 'user', got '%s'", target)
	}

	name := words[2]
	name = strings.Trim(name, "\"'")

	method := "disable_user"
	if strings.ToLower(words[0]) == "enable" {
		method = "enable_user"
	}

	return &Cmd{
		Type:   "user",
		Method: method,
		Name:   name,
		Args:   []string{name},
	}, nil
}

// parseGrant handles:
//
//	grant <role> to "<username>"
//	grant access to "<database>" for "<username>"
func parseGrant(words []string) (*Cmd, error) {
	if len(words) < 4 {
		return nil, fmt.Errorf("usage: grant <role> to <user> | grant access to <db> for <user>")
	}

	// Check for "grant access to <db> for <user>"
	if strings.ToLower(words[1]) == "access" {
		if len(words) < 6 || strings.ToLower(words[2]) != "to" || strings.ToLower(words[4]) != "for" {
			return nil, fmt.Errorf("usage: grant access to <database> for <user>")
		}
		dbName := words[3]
		dbName = strings.Trim(dbName, "\"'")
		userName := words[5]
		userName = strings.Trim(userName, "\"'")
		return &Cmd{
			Type:    "user",
			Method:  "grant_access",
			Name:    userName,
			Args:    []string{dbName, userName},
			Options: map[string]interface{}{"database": dbName, "user": userName},
		}, nil
	}

	// grant [role] <role@db> to <user>
	args := words[1:]
	if len(args) > 0 && strings.ToLower(args[0]) == "role" {
		args = args[1:]
	}
	if len(args) < 3 || strings.ToLower(args[1]) != "to" {
		return nil, fmt.Errorf("usage: grant role <role@db> to <user>")
	}
	role := args[0]
	userName := strings.Trim(args[2], "\"'")
	if _, err := parseRoleSpec(role); err != nil {
		return nil, err
	}

	return &Cmd{
		Type:    "user",
		Method:  "grant_role",
		Name:    userName,
		Args:    []string{role, userName},
		Options: map[string]interface{}{"role": role},
	}, nil
}

// parseRoleSpec splits a role specification into {role, db}. A bare role
// means every database, matching spec 3.1's db: "*".
func parseRoleSpec(spec string) (roleSpec, error) {
	role, db, found := strings.Cut(spec, "@")
	if !found {
		return roleSpec{Role: spec, DB: "*"}, nil
	}
	if role == "" || db == "" {
		return roleSpec{}, fmt.Errorf("role %q must be <role>@<database> or a bare <role>", spec)
	}
	return roleSpec{Role: role, DB: db}, nil
}

// roleSpec is the CLI's parsed form of a role specification.
type roleSpec struct {
	Role string
	DB   string
}

// parseRevoke handles:
//
//	revoke <role> from "<username>"
//	revoke access from "<database>" for "<username>"
func parseRevoke(words []string) (*Cmd, error) {
	if len(words) < 4 {
		return nil, fmt.Errorf("usage: revoke <role> from <user> | revoke access from <db> for <user>")
	}

	// Check for "revoke access from <db> for <user>"
	if strings.ToLower(words[1]) == "access" {
		if len(words) < 6 || strings.ToLower(words[2]) != "from" || strings.ToLower(words[4]) != "for" {
			return nil, fmt.Errorf("usage: revoke access from <database> for <user>")
		}
		dbName := words[3]
		dbName = strings.Trim(dbName, "\"'")
		userName := words[5]
		userName = strings.Trim(userName, "\"'")
		return &Cmd{
			Type:    "user",
			Method:  "revoke_access",
			Name:    userName,
			Args:    []string{dbName, userName},
			Options: map[string]interface{}{"database": dbName, "user": userName},
		}, nil
	}

	// revoke [role] <role@db> from <user>
	args := words[1:]
	if len(args) > 0 && strings.ToLower(args[0]) == "role" {
		args = args[1:]
	}
	if len(args) < 3 || strings.ToLower(args[1]) != "from" {
		return nil, fmt.Errorf("usage: revoke role <role@db> from <user>")
	}
	role := args[0]
	userName := strings.Trim(args[2], "\"'")
	if _, err := parseRoleSpec(role); err != nil {
		return nil, err
	}

	return &Cmd{
		Type:    "user",
		Method:  "revoke_role",
		Name:    userName,
		Args:    []string{role, userName},
		Options: map[string]interface{}{"role": role},
	}, nil
}

// parseDefine's production path is retired: the verb it parsed is now
// create bucket (parseCreateBucket), and `define` is refused up the switch
// with a rename error.

// parseConnect handles: connect <address>
func parseConnect(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: connect <address>")
	}
	return &Cmd{
		Type:   "global",
		Method: "connect",
		Name:   words[1],
		Args:   []string{words[1]},
	}, nil
}

// parseJoin handles: join <address>  (add a node to the cluster; the add
// triggers rebalancing). The legacy --seed form is still accepted.
func parseJoin(words []string) (*Cmd, error) {
	cmd := &Cmd{
		Type:   "global",
		Method: "join",
	}
	if len(words) >= 2 && !strings.HasPrefix(words[1], "--") {
		return &Cmd{Type: "global", Method: "join", Options: map[string]interface{}{"seed": words[1]}}, nil
	}
	for i := 1; i < len(words); i++ {
		switch words[i] {
		case "--seed":
			if i+1 >= len(words) {
				return nil, fmt.Errorf("--seed requires a value")
			}
			if cmd.Options == nil {
				cmd.Options = map[string]interface{}{}
			}
			cmd.Options["seed"] = words[i+1]
			i++
		default:
			return nil, fmt.Errorf("unexpected argument: %s", words[i])
		}
	}
	if _, ok := cmd.Options["seed"]; !ok {
		return nil, fmt.Errorf("usage: join <address>  (add a node to the cluster; this starts rebalancing)")
	}
	return cmd, nil
}

// parseConsistentGet handles: consistent-get <database> <bucket> <key> (quorum
// read + read repair).
func parseConsistentGet(words []string) (*Cmd, error) {
	if len(words) < 4 {
		return nil, fmt.Errorf("usage: consistent-get <database> <bucket> <key>")
	}
	return &Cmd{Type: "global", Method: "consistent-get", Args: []string{words[1], words[2], words[3]}}, nil
}

// parseMigrate handles: migrate <database> <bucket>  (pull a bucket onto the
// node this CLI is connected to if that node is its ring owner, so the bucket
// becomes servable there).
func parseMigrate(words []string) (*Cmd, error) {
	if len(words) < 3 {
		return nil, fmt.Errorf("usage: migrate <database> <bucket>")
	}
	return &Cmd{Type: "global", Method: "migrate", Args: []string{words[1], words[2]}}, nil
}

// parseSet handles: set <pretty|extjson|pager> [on|off] — a session's own output
// settings, changed while the session runs. The flags of the same names set them
// at startup, and leaving the value out asks what a setting is. The words are
// passed on whole: the settings live on the session, which is what refuses a
// value it does not know.
func parseSet(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: set <pretty|extjson|pager> [on|off]")
	}
	return &Cmd{Type: "global", Method: "set", Args: words[1:]}, nil
}

// parseWriteConcern handles: write-concern <one|majority|all> — the session
// durability level for subsequent writes.
func parseWriteConcern(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: write-concern <one|majority|all>")
	}
	return &Cmd{Type: "global", Method: "write-concern", Args: words[1:2]}, nil
}

// parseRemoveNode handles: remove-node <node-id>
func parseRemoveNode(words []string) (*Cmd, error) {
	if len(words) < 2 {
		return nil, fmt.Errorf("usage: remove-node <node-id>")
	}
	if len(words) > 2 {
		return nil, fmt.Errorf("unexpected argument: %s", words[2])
	}
	return &Cmd{
		Type:   "global",
		Method: "remove-node",
		Name:   words[1],
		Args:   []string{words[1]},
	}, nil
}

// parseBucketMethod parses db.<bucket>.<method>(args) notation. It splits at
// the last dot, so db.Post.Comments.push({...}) addresses one flat bucket
// named "Post.Comments": there is no nesting yet (GAP-22).
func parseBucketMethod(line string) (*Cmd, error) {
	// Must start with "db." (case-insensitive).
	if !strings.HasPrefix(strings.ToLower(line), "db.") {
		return nil, fmt.Errorf("expected 'db.<bucket>.<method>(...)', got: %s", line)
	}

	// Strip the "db." prefix.
	rest := line[3:]

	// Find the method call: it starts at the first '(' after the bucket path.
	// The bucket path can have dots (e.g., Post.Comments), so we need to find
	// the method name which comes after the last dot before '('.
	methodIdx := methodParenIndex(rest)
	if methodIdx == -1 {
		return nil, fmt.Errorf("expected parentheses for method call: %s", line)
	}

	methodPart := rest[:methodIdx]
	argsPart := rest[methodIdx:]

	// A bracket after the bucket name is a document's store field:
	// db.Post["p1"].Comments.push(...). It is read before the last-dot split,
	// and a dotted name with no bracket keeps its meaning — one flat bucket
	// (nested buckets S-6; spec §13.6).
	if bracket := strings.IndexByte(methodPart, '['); bracket > 0 {
		args, err := parseArgs(argsPart)
		if err != nil {
			return nil, fmt.Errorf("parse args: %w", err)
		}
		cmd := &Cmd{Type: "bucket", Bucket: methodPart[:bracket], Args: args}
		if err := parseFieldAddress(cmd, methodPart[bracket:]); err != nil {
			return nil, fmt.Errorf(`%w — a store field is addressed as db.<bucket>["<key>"].<field>.<verb>(...): %s`, err, line)
		}
		return cmd, nil
	}

	// Find the last dot in methodPart to separate bucket from method.
	lastDot := strings.LastIndexByte(methodPart, '.')
	if lastDot == -1 {
		return nil, fmt.Errorf("expected bucket.method notation: %s", line)
	}

	bucket := methodPart[:lastDot]
	method := methodPart[lastDot+1:]

	// Parse arguments from parentheses.
	args, err := parseArgs(argsPart)
	if err != nil {
		return nil, fmt.Errorf("parse args: %w", err)
	}

	return &Cmd{
		Type:   "bucket",
		Bucket: bucket,
		Method: method,
		Args:   args,
	}, nil
}

// methodParenIndex returns the index of the '(' that opens a method call in
// the text after "db.", or -1. It is the first '(' outside a bracketed document
// key and outside a quoted string. The last '(' of the line is not the method's:
// an argument can hold calls of its own, ISODate("...") and now().
func methodParenIndex(rest string) int {
	bracketDepth := 0
	var quote byte
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		switch {
		case quote != 0:
			if ch == quote {
				quote = 0
			}
		case ch == '"' || ch == '\'':
			quote = ch
		case ch == '[':
			bracketDepth++
		case ch == ']' && bracketDepth > 0:
			bracketDepth--
		case ch == '(' && bracketDepth == 0:
			return i
		}
	}
	return -1
}

// parseArgs extracts arguments from a parenthesized expression like (arg1, arg2).
func parseArgs(parenExpr string) ([]string, error) {
	// Remove outer parentheses.
	if len(parenExpr) < 2 || parenExpr[0] != '(' || parenExpr[len(parenExpr)-1] != ')' {
		return nil, fmt.Errorf("expected parentheses, got: %s", parenExpr)
	}

	inner := parenExpr[1 : len(parenExpr)-1]
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return nil, nil
	}

	// Simple split by comma, respecting braces and quotes.
	var args []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(inner); i++ {
		ch := inner[i]

		if inQuote {
			current.WriteByte(ch)
			if ch == quoteChar && (i == 0 || inner[i-1] != '\\') {
				inQuote = false
			}
			continue
		}

		switch ch {
		case '"', '\'':
			inQuote = true
			quoteChar = ch
			current.WriteByte(ch)
		case '{', '[', '(':
			depth++
			current.WriteByte(ch)
		case '}', ']', ')':
			depth--
			current.WriteByte(ch)
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args, nil
}

// isBatchStatement reports whether line is a batch { ... } statement: the
// word batch, then a brace, with or without space between.
func isBatchStatement(line string) bool {
	if len(line) < len("batch") || !strings.EqualFold(line[:len("batch")], "batch") {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(line[len("batch"):]), "{") || strings.TrimSpace(line) == line[:len("batch")]
}
