# User management

hoardDB authenticates a user per connection and authorizes every operation
against the roles that user holds. Where the users live depends on how the node
runs:

- **A node started without `cluster.enabled`** keeps its users in `users.json`
  beside its data directory; they survive restarts.
- **A cluster member** (`cluster.enabled` on) keeps no `users.json`. Its users,
  roles and grants are part of the cluster's replicated state: a user created,
  changed or dropped through any member takes effect on every member.

This page is the operator's view. It assumes a running server and a CLI session;
[getting-started.md](getting-started.md) covers both. The user and role statements
run over the same connection as every other statement.

## Where user changes run

**On a node without a cluster** a user change runs on that node and changes its
`users.json`.

**In a cluster** a user change is a cluster command. Send it to any data member:
that member passes it to the cluster, every member applies it in the same order,
and the answer comes once the member you asked shows the change, so a login on
that member right after the answer sees it. A change needs a majority of the
cluster's voters; a login does not, and is checked against the member's own copy
of the users. What that means:

- **When no majority answers**, a change ends with one of two texts, and both
  name what to run next. "was not applied ... so nothing changed" means no leader
  took it; run the statement again once a majority is up. "may or may not have
  been applied" means the member sent it and the answer was lost: look at the user
  (`user describe <name>`) before running it again.
- **A member cut off from the others** keeps accepting logins from its own copy,
  including logins of an account dropped, disabled or given a new password on the
  other side. Its sessions of that account end when it rejoins and applies the
  change. On a healthy cluster a change reaches every member a moment after it
  is answered.
- **The CLI's user and role statements work on any data member**: they send the
  same commands as a program does (see
  [Managing users from a program](#managing-users-from-a-program)).

### A cluster's first administrator

The first data node of a new cluster, started with `HOARDDB_ROOT_PASSWORD` (or
`HOARDDB_ROOT_PASSWORD_FILE`, or the password it asks for on its first start from
a terminal), creates the cluster's first administrator, `root` on every database,
once it has started, and logs `created the cluster's first administrator`. Only
the password's hash is kept, in the cluster's state; the password is not stored
and not logged.

- A node of a cluster that already has users ignores a root password, and logs
  and prints `HOARDDB_ROOT_PASSWORD was not used: this cluster already has users.
  Log in as an existing administrator, or unset it.` Restarting the first node
  with the setting still there prints it too; nothing changes.
- A node that joins a cluster, and a witness, never ask for a root password.
- A cluster started without one has no users, and every login answers `this
  cluster has no users yet`. Set `HOARDDB_ROOT_PASSWORD` on one data node (not a
  witness) and start it again: it creates the administrator.
- When two data nodes are given a root password, one administrator is created;
  the other node prints the warning above.
- A witness serves no logins and no statements. Connect to a data node.

### Keep two administrators

In a cluster, keep at least two enabled accounts that hold user-manager or root,
with different people or vaults holding their passwords. A cluster cannot reset a
password offline: `hoarddb-server -reset-password` refuses on a node with
`cluster.enabled`, writes nothing, and says to log in on any node as another
administrator and run `alter user <name> password` (or, from a program,
`ringclient`'s `AlterUser` with a new password).

If every administrator is locked out, the cluster's users cannot be recovered and
the cluster must be rebuilt: stop every node, start each on a new, empty data
directory (a new `ring` directory, `auth.token.key` and `auth.cluster.pepper`),
with `HOARDDB_ROOT_PASSWORD` set on one data node, join the others, and load the
data again with `hoarddb restore` from a dump taken earlier. A dump needs a
login, so take dumps while an administrator can still log in: the old data
directories keep the data, but no account can read it. The rebuilt cluster
refuses every token of the old one.

### Logging in to a cluster member

A member checks passwords with the cluster's password key, which it holds in
`auth.cluster.pepper` in its data directory. A member without that key refuses
every password login, before it checks anything, and says why:

- the cluster has no users yet;
- the member has not received the key from another member yet (it asks again
  whenever the cluster changes and at least every 30 seconds; log in on another
  member meanwhile);
- the member is a witness, which serves no logins: connect to a data member.

A member started with a `users.json` in its data directory refuses to start: the
file is never read in a cluster. Move it aside as the refusal says (`mv <path>
<path>.not-clustered`) and start the member again. Users of a node that ran
without a cluster are not carried into a cluster; create them again.

## The user store on disk

On a node without a cluster, `users.json` sits in the data directory, mode
`0600`. A cluster member has no such file; the rest of this section is about the
file.

The file is **encrypted** with AES-256-GCM, so it cannot be edited to inject a
user or revive a disabled one. Which key seals it depends on whether the node
runs with encryption at rest (see "Encryption at rest" in
`docs/user/configuration.md`):

- **Encryption at rest on** (`encryption.mode` `on` or `require`): the file is
  sealed under the node master key, which the keystore protects and which is not
  in the data directory. A copy of the data directory does not hand out password
  hashes.
- **Encryption at rest off** (the default): the file is sealed under the node's
  auth signing key, which *is* in the data directory: `auth.token.key`, on a
  standalone node and on a ring cluster node alike. A copy of
  the whole data directory can read it.

What the two modes mean, in this version (the full account is in
[Configuration](configuration.md#encryption-at-rest)):

- Encryption at rest is **per node** and **off by default**. It is decided when a node's data
  directory is first started and cannot be switched afterwards.
- `on` and `require` behave the same.
- An encrypted node seals `blob` payload files too: content of 64 KiB and over is written to
  encrypted files, and smaller content is stored inside the node's encrypted store.
- A `dump` is plaintext whatever the mode.
- There is no key rotation command yet. The node master can be exported and imported with
  `hoarddb-server -export-key <file>` and `-import-key <file>` while the server is stopped.
- If the wrapped node master (`keys/node-master.key.wrapped`) is missing from a data directory
  that holds sealed data, the server refuses to start rather than create a new one. Restore the
  file from a backup, or import the exported node master.

That makes the key part of the store's durability, not only its security:

- **Keep the key with your backups.** With encryption at rest on, that is the
  keystore's KEK, kept apart from the data directory; a node started without it
  refuses to start. With it off, it is the key file above, and a
  node that has lost it refuses to start with:

      users.json is encrypted with key <kid>, but that key is not in the keyring
      — restore the original auth.token.key or lose the users

- **`--reset-password` is not a recovery path for that case.** It reads the same
  file and fails the same way; it can only rewrite a store it can already read.
- **Decommissioning a node means deleting its `users.json`**: the copy is
  readable by that node and by anyone holding its key material.

A `users.json` written before this encryption existed is plain JSON and still
loads as-is: the format is detected from the file itself, so there is no
migration step and no flag to set.

## Create a user

Passwords are prompted, never passed as a flag. On a terminal the CLI asks
twice and does not echo:

```
[h]oardDB> create user alice roles [writer@app, reader@logs]
New password:
Repeat new password:
user "alice" created (revision 2)
```

A username is at most 255 bytes and holds no control characters (the escape
byte, a newline, DEL): a terminal acts on them when it prints a name. `create
user` refuses such a name and says so. Accounts made before this rule keep
working, and can still be altered and dropped.

In a script the statements arrive on stdin, so the password comes from one of
three places:

```
# 1. The two lines that follow the statement in the script (a piped session
#    cannot prompt, so it reads the next two lines of the same input).
printf 'create user alice roles [writer@app]\nalice-s3cret-pw\nalice-s3cret-pw\nexit\n' | hoardDB

# 2. One line after the statement, with --password-stdin.
printf 'create user alice roles [writer@app]\nalice-s3cret-pw\nexit\n' | hoardDB --password-stdin

# 3. A 0600 file, for a password that must not appear in the script.
echo 'create user alice roles [writer@app]' | hoardDB --password-file /run/secrets/alice.pw
```

A script that leaves out the password lines has its next statements read as the
password (the CLI refuses when they do not agree, and names `--password-file`;
it cannot tell identical lines from a password). If the script ends where the password should be, the CLI stops with an error
that names `--password-file`; it never answers a bare `EOF`. `--password-stdin`
on a terminal is refused, because the line would be typed in the clear: leave
the flag off to be prompted without echo.

A script with more password lines than its source takes (a second line with
`--password-stdin`, any password line with `--password-file`, a third line
otherwise) stops at the first extra line with exit 1. That line is not printed,
because it may be a password; remove it from the script.

There is no `--password` flag on any user command: a password on a command line
is visible in `ps` and shell history, and a pre-hashed credential is an
offline-crackable artifact of a password people reuse. The server never receives
the plaintext on this path — the CLI derives the Argon2id hash locally and sends
only the salt, hash and parameters, which the server peppers before storing.
Because the server never sees the password, the CLI applies the server's minimum
length (`HOARDDB_PASSWORD_MIN_LENGTH`, which the server reports at login) and
refuses a shorter password, naming the minimum, before it sends anything.

Before it creates the user, the CLI reads the users. If a user is dropped between
that read and the create, the server refuses the create (`CONFLICT`) and nothing
is created: run the statement again.

## Roles

Roles are named for what they do, so the intuition transfers. A role is always
bound to a database: `writer@app` grants read and write on `app`. Use `db:
"*"` (`writer@*`) for every database.

| Role | Grants | Covers |
| --- | --- | --- |
| `reader` | reader | get, exists, count, list, search, btree scans, blob read |
| `writer` | reader + writer | put, insert, update, delete, batch put, blob write, queue/heap mutations |
| `db-manager` | reader + writer + db-manager | create/drop bucket, index changes, bucket stats, retention |
| `user-manager` | user-manager | create/alter/drop users, grants, password changes, within its own roles (below) |
| `cluster-manager` | cluster-manager | node add/remove, rebalance, migration, gossip, `nodes` |
| `backup-operator` | backup-operator (+ reader) | dump/backup, including buckets it cannot read directly |
| `restore-operator` | restore-operator | restore/import |
| `root` | everything, all databases | the bootstrap administrator |

Two notes on scope. **A role only applies to the database it was granted on**:
`writer@app` + `reader@logs` can write `app` and cannot write `logs`. A user
with no grant on a database is refused there. And **user and cluster
administration are store-wide in v1** — the database in a `user-manager` or
`cluster-manager` grant is not used to narrow the action.

**A `user-manager` works within its own roles.** Unless it also holds `root@*`,
it gives only roles it holds itself, on a database it holds them on, and it
creates, alters, drops, grants to or revokes from only accounts whose roles it
holds itself. A user with `user-manager@*` and `db-manager@app` can create an
`app` reader or writer and reset that user's password; it cannot give
`root`, `cluster-manager`, `backup-operator` or any role on `billing`, and it
cannot change a `root` account. The refusal names the grant and says to ask a
root user. `writer` includes `reader`, `db-manager` includes both, and
`backup-operator` includes `reader`.

`show databases` follows the grants too: it lists the databases a user holds a
role on, and every database for a role on `*`.

Refusals, the server log and the audit log name these roles (`lacks "writer"`,
`grant role writer@app to alice`). Some other spellings of these roles, such as
`dbAdmin` for `db-manager`, are not accepted, and a refusal that meets one says
what the role is called. Granting the same role on the same database twice stores
it once, and one `revoke role` removes it.

A `users.json` that holds one of those spellings is not loaded. The server
refuses to start on it and names each one with the role to use.
`users.json` is sealed, so it cannot be edited by hand, and `--reset-password`
refuses it too. To recover, stop the server, move `users.json` aside, and start
it again with `HOARDDB_ROOT_PASSWORD` set, which creates the root user afresh.
That drops every other account; create them again with the role names above.

## Read users back

```
[h]oardDB> user list
USERS (revision 2)
---
admin: enabled, roles [root@*], epoch 0
alice: enabled, roles [reader@logs, writer@app], epoch 0

[h]oardDB> user describe alice
user:  alice
status: enabled
roles:  [reader@logs, writer@app]
epoch:  0
created: 2026-09-19T06:45:01Z
password changed: 2026-09-19T06:45:01Z
```

`user list` and `user describe` never show a hash, a salt or a pepper. The
revision in the `USERS` line, and after each change, is the user store's: it
moves with every change to any user.

## Change a user

Grant and revoke are incremental:

```
[h]oardDB> grant role db-manager@logs to alice
granted db-manager@logs to "alice" (revision 3)
[h]oardDB> revoke role db-manager@logs from alice
revoked db-manager@logs from "alice" (revision 4)
```

`alter user ... roles [...]` **replaces** the whole role set. The CLI reads the
user first and sends the revision it read, so the change is refused (`CONFLICT`)
if the user store changed in between; run `user describe <name>` and the
statement again. Prefer grant/revoke when you are adding or removing one role:

```
[h]oardDB> alter user alice roles [reader@app, writer@logs]
user "alice" altered (revision 5)
```

Disable and enable:

```
[h]oardDB> alter user alice disable
user "alice" altered (revision 6)
[h]oardDB> alter user alice enable
user "alice" altered (revision 7)
```

Change a password (prompted, or from the script as under "Create a user"):

```
[h]oardDB> alter user alice password
New password:
Repeat new password:
user "alice" altered (revision 8)
```

A password change bumps the user's `epoch`, which refuses every token minted
before it at the next refresh. The old password stops working immediately.

## Drop a user

```
[h]oardDB> drop user alice
user "alice" dropped
```

The last enabled account that can manage users (`root`, or `user-manager` on
every database) cannot be dropped, disabled, or stripped of that grant; hoardDB
refuses and names the grant to give another account first. A `cluster-manager`
does not count. A lockout is not recoverable through the CLI.

## What happens to an open connection

- **Role changes, grants and revokes** apply at the user's **next AUTH**. An
  already authenticated connection is not terminated and keeps the roles it
  authenticated with; a token refresh picks up the narrowed or widened set.
- **Disable, drop and a password change** close that user's existing sessions
  on the node that takes the change, at once. A request on such a connection is
  refused with `NOT_AUTHENTICATED` and the connection is closed; the client must
  log in again (and, after a drop or disable, is refused). On a node without a
  cluster, sessions the same user has open on other nodes are not touched. In a
  cluster every member ends its own sessions of that user when it applies the
  change.

## Managing users from a program

The Go client (`ringclient`) manages users over the same connection it reads and
writes with: `CreateUser`, `AlterUser`, `DropUser`, `GrantRole`, `RevokeRole`,
`ShowUsers` and `DescribeUser`. Each needs a session holding `user-manager@*` or
`root@*`; a grant on one database is not enough. The password never leaves the
program: the client derives an Argon2id credential from it and sends only that.

On a node started without `cluster.enabled` these calls change the node's own
users, as the statements above do; on a cluster member they change the cluster's
users, on every member. The refusals are the same:

```go
list, err := client.ShowUsers(ctx)
if err != nil {
	return err
}
_, err = client.CreateUser(ctx, "ada", password,
	[]ringclient.UserGrant{{Role: "reader", DB: "app"}}, list.DropSeq)
```

- `CreateUser` takes the `DropSeq` of the last `ShowUsers` or `DescribeUser`. If a
  user was dropped since that read, the create is refused (`CONFLICT`) and nothing
  is created: read the users again and create with the new value. To retry a
  create whose answer you did not get, send it again with the same
  `ringclient.WithRequestID` and the same `DropSeq`: it is applied at most once and
  never brings back an account dropped in between.
- `AlterUser`, `DropUser`, `GrantRole` and `RevokeRole` take an optional
  revision (`Revision` from `ShowUsers` or `DescribeUser`). With one, a change to
  a user that changed since it was read is refused (`CONFLICT`), and the refusal
  says to describe the user and repeat the change against what it shows. In a
  cluster a user's revision moves only when that user changes; on a node without
  a cluster it moves with any change to any user.
- Granting a role the user holds, and revoking one it does not hold, succeed and
  change nothing. Dropping a name that does not exist is refused (`NOT_FOUND`).
  Creating a name that exists is refused `USER_EXISTS`; dropping or disabling the
  last account that can manage users, and a create past 10,000 users, are refused
  `INVALID_ARGUMENT`. Neither clears on a retry; `CONFLICT` does, after a new read.
- The server cannot check a password's length, because it receives only the
  credential. `client.PasswordMinLength()` is the minimum the server reported at
  login (0 when it reported none); check a new password against it before
  `CreateUser` or `AlterUser`, as the CLI does.

- An account holds at most 64 grants; a grant beyond that is refused.
- In a cluster, a change retried with the same `ringclient.WithRequestID` is
  answered as done while the cluster remembers that request id: until 256 later
  user changes have been made in the cluster (a driver retrying inside its own
  deadline is well within that, unless a script makes hundreds of user changes
  in those seconds). Past that bound the request id is forgotten, and only the
  fence the client sent still holds: a create carries its `DropSeq`, so it is
  refused and never brings a dropped account back; an alter, drop, grant or
  revoke is held only by the revision the client passed. **Pass the revision
  when you may retry a change**: a retry without one, past the bound, is fenced
  against the user as it is at that moment and can apply again (for example a
  retried drop can drop an account created again under the same name since).
- In a cluster the request id alone names a remembered change: the cluster does
  not compare the retried command or account with the first. A program that
  reused a request id for a different change would be answered as done with
  nothing applied, so draw a new id for every change (ringclient does) and reuse
  one only to retry that same change. An all-zero id is refused. A change that found no majority answers
  `UNAVAILABLE` with one of the two texts of
  [Where user changes run](#where-user-changes-run); the server answers within
  20 seconds, before the client's own default deadline of 30 seconds.

## Reading a refusal

A permission refusal names the user, the missing action, the resource, the roles
the user actually holds, and the command that fixes it:

```
[h]oardDB> create database other
CLI error: create database "other": server error (5): permission denied: user "alice" lacks "db-manager" on "other"
       alice's roles: reader@app
       ask an administrator to run: grant role db-manager@other to alice
```

Repeated identical refusals reach the audit log once per minute with a count of
the repeats after that, so a misconfigured client in a retry loop does not flood
the log.
