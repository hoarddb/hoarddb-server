# Add a node to a cluster

This page is the procedure for putting one more node into a running cluster, and
the table of what each refusal means. Every node trusts only the keys it was told
about, so adding a node means telling **every** member about it. The steps below
use three existing nodes (`node-a`, `node-b`, `node-c`) and a new one (`node-d`);
the addresses are documentation addresses, so use your own.

**Before you start: this release can only add a node while the cluster holds no
database.** `join` is refused once any database exists, with `cannot join
<node>@<addr>: this cluster holds database "<db>" and this build does not seed a
joining node's slots`. Form the whole cluster first, then `create database`. A
member that lost its data can be brought back while the cluster holds data, under
its own id and at its own address (see [If a member lost its data](#if-a-member-lost-its-data)).
The statements `leave` and `remove-node` are not available in this build, so a
node cannot be taken out yet (see [Removing a node](#removing-a-node)).

## 1. Find a node's fingerprint

A node's fingerprint is the SHA-256 of its public key, and it prints it on every
start, on the `entry-ring node open` line, in the `fingerprint` field:

```bash
hoarddb-server 2>&1 | grep 'entry-ring node open'
```

```
{"level":"INFO","msg":"entry-ring node open","id":"node-d","data_addr":"192.0.2.4:4433","mode":"join","fingerprint":"SHA256:9f2c…","dir":"data/ring"}
```

Copy the whole `SHA256:<64 hex>` value. The key is created on the node's first
start and lives in `<data dir>/keys`; it does not change on a restart.

## 2. Add the Nth node

1. **Start the new node pinning every node that exists.** List every existing node
   in `HOARDDB_SEED_NODES` as `<node-id>@<address>`, and pin every one of them in
   `HOARDDB_SEED_FINGERPRINTS` as `<node-id>=SHA256:<64 hex>`. Do not list the new
   node itself.

   ```bash
   HOARDDB_NODE_ID=node-d \
   HOARDDB_CLUSTER_ENABLED=true \
   HOARDDB_CLUSTER_LISTEN=192.0.2.4:4433 \
   HOARDDB_SEED_NODES=node-a@192.0.2.1:4433,node-b@192.0.2.2:4433,node-c@192.0.2.3:4433 \
   HOARDDB_SEED_FINGERPRINTS=node-a=SHA256:<a's hex>,node-b=SHA256:<b's hex>,node-c=SHA256:<c's hex> \
   hoarddb-server
   ```

   The node waits to be joined. It logs `this node holds no cluster state, so it
   serves its peers only once a member admits it`, which is the expected state here,
   and prints its own fingerprint (step 1). The `join` of step 3 is what admits it;
   until then it refuses every member's connection except the identity request. If the listen address is `0.0.0.0` or `[::]`, also set
   `HOARDDB_CLUSTER_ADVERTISE=192.0.2.4:4433`: the node refuses to start without it.

   Why every existing node, not just one: the cluster's Raft leader dials the new
   node during the join, and the leader can be any member. A node accepts a
   connection only from a key it has pinned, so a new node that pinned only
   `node-a` refuses the leader when the leader is `node-b`, and the join cannot
   complete.

2. **Pin the new node on every existing member, one at a time.** Add
   `node-d=SHA256:<d's hex>` to `HOARDDB_SEED_FINGERPRINTS` (or `hash_ring.seed_fingerprints`)
   on `node-a`, then restart it. A node reads its pinned keys only when it starts;
   there is no reload. Do not touch `HOARDDB_SEED_NODES` on the members. Restart
   them **one at a time**: connect to the restarted member, run `nodes`, and
   continue only when it is listed again. Restarting two members of a three-voter
   cluster at once loses the majority until one is back.

3. **Join.** On any member, with the address the new node advertises
   (`HOARDDB_CLUSTER_ADVERTISE`, else `HOARDDB_CLUSTER_LISTEN`):

   ```
   [h]oardDB> join node-d@192.0.2.4:4433
   ```

   ```
   Added node-d@192.0.2.4:4433 as a member: joining (not a voter: the cluster already has its 3 voters) (epoch 4)
   ```

   The statement is forwarded to the leader, which asks the new node who it is,
   adds it, and activates it once it holds its slots. A cluster has
   `cluster.voters` voters (3 by default, witnesses included). A node that joins
   while a slot is free votes at once; here all three are taken, so `node-d` is a
   member that does not vote. `promote node-d` gives it a vote only after a slot is
   free, and freeing one needs `leave` or `remove-node`, which this build does not
   have. Running `join` again answers the same line, so it is safe to repeat.
   `nodes` lists the members.

## If a member's key changes

Each node's key is its identity, and the others hold its fingerprint in their
configuration. A node started on a **fresh data directory** (or with
`keys/server.key` moved away) generates a new key, and the other members refuse
it: they are pinned to the old one. Two ways out:

- **Restore the old key.** Put `keys/server.key` and `keys/server.crt` back from a
  backup of that node's data directory and restart it. Nothing else changes.
  Back these two files up when you create a node.
- **Accept the new key.** Replace (do not add) the `<node-id>=SHA256:` entry with
  the new fingerprint on every other member, one restart at a time, and wait until
  `nodes` lists each one again before the next: with one voter of three already
  down, a second restarted voter pauses writes and DDL until it is back. Two entries
  for one id stop the start with `pins <id> to two keys`. A node that lost its whole
  data directory has also lost its data: once the members trust its new key, bring
  it back as [If a member lost its data](#if-a-member-lost-its-data) says.

## If a member lost its data

A member whose `<data dir>/ring` directory was lost, damaged or moved aside is
brought back under its own id with `replace-node`: it joins again, pulls every
slot it owns from the other copies, and becomes active when it holds them. The
steps use `node-c` at `192.0.2.3:4433`.

1. **Stop the node and move its ring directory aside.** Keep the directory you
   moved: it may hold the only copy of some data. Keep `<data dir>/keys`: its key is
   what the other members trust, so no pin changes on any member.

   ```bash
   mv <data dir>/ring <data dir>/ring.lost
   ```

2. **Start it again with the same node id, seeds and address.**
   `HOARDDB_NODE_ID`, `HOARDDB_CLUSTER_LISTEN` (and `HOARDDB_CLUSTER_ADVERTISE` if
   you set it) are the ones it had, and `HOARDDB_SEED_NODES` names the other
   members. The first node of a cluster usually lists no seeds; started on an empty
   `ring` directory with no seeds it would start a new cluster of its own, so list
   the other members before you start it. The node logs `this node holds no cluster
   state, so it serves its peers only once a member admits it` and refuses every
   member's connection until the statement of step 4 admits it. `join` does not
   bring it back: for a member that is active or down it answers from the member
   row, and for a voter that had not finished joining it is refused with `... is a
   voter of this cluster and the node at <addr> holds no cluster state`.

3. **Wait until `nodes` shows it down** (or dead) on any other member. Until then
   the cluster still counts it as active, and the statement is refused.

4. **Run `replace-node` on any member**, with the node's own id and address:

   ```
   [h]oardDB> replace-node node-c --with node-c@192.0.2.3:4433
   ```

   ```
   Replacing node-c@192.0.2.3:4433: joining, then active when it holds the 5461 pairs it owns (epoch 7). Run 'status seeding' on node-c to follow it
   ```

   A pair is one slot of one database. `status seeding`, run on `node-c`, lists the
   pulls in progress; `nodes` shows it `active` when it holds them all, and it votes
   again if it voted before. Running the statement again while it is joining
   answers the same way and finishes a replace that was cut short.

While it pulls, a database with replication factor 2 or more is served by its
other copies. **A database with replication factor 1 had only that node's copy.**
`replace-node` refuses when the node held the only copy of some pairs, and changes
nothing. It counts a copy only on a member that is active (or joining) and has
reported it: a copy on a member that is down or dead does not count, and neither
does one a member has not reported yet (members report every few seconds). If
another member that holds copies is down, start it first and wait until `nodes`
shows it active; right after the data was written, wait a few seconds and retry.
Every pair the node owned counts, whether or not it holds documents, so a database
with replication factor 1 asks for `--accept-loss` even when it is empty. The count
reads what the cluster has recorded: a copy lost on a member the cluster still
shows active is not seen, so make sure the other members are healthy before you
replace a node. If a replace that accepted a loss is cut short, run it again with
`--accept-loss`, as its message says.

```
replace-node node-c would leave 2731 pairs of database "cache" (replication factor 1) with no copy: no other active member has reported one, so their documents would be lost. Nothing was changed. ...
```

Putting the moved directory back and restarting the node keeps that data. If it is
gone, add `--accept-loss`: the node is brought back with those pairs empty, and the
documents that were only on it are lost. The line it answers ends with how many
pairs it gave up (`It accepted the loss of <n> pairs in <k> database(s), which only
node-c held.`), and the leader logs the same.

```
[h]oardDB> replace-node node-c --with node-c@192.0.2.3:4433 --accept-loss
```

`replace-node` needs a leader, so a majority of the voters must be up. In a cluster
of two voters, one lost node leaves no majority and the statement cannot run:
restore that node's data (put the moved `ring` directory back and restart it) so
the cluster has a majority again before you replace any node.

`replace-node` brings a node back at its own address only, and only a node that
lost its data: a node that still holds its cluster state is refused (`holds
cluster state, so it did not lose its data`), because restarting it as it is
brings it back by itself. Do not move a running member's `ring` directory aside
to repair it; stop the node first.

## The cluster's password key

A cluster's users are checked against a password key that every data node keeps in
`auth.cluster.pepper`, in its data directory, beside `auth.token.key`. Until the cluster has users,
no node holds the key and `status` on every data node says `none yet`.

- The key is created by the data node that creates the cluster's first administrator: the first
  node, started with `HOARDDB_ROOT_PASSWORD` (or `HOARDDB_ROOT_PASSWORD_FILE`, or the password it
  asks for on its first start from a terminal). That node writes the key to its own
  `auth.cluster.pepper` before anything else, then creates the administrator through the cluster,
  and logs `created the cluster's first administrator`. Every other data node, including one that
  joins later, then asks a member for the key over the pinned connection between nodes, once the
  cluster has committed it as a member.
- Do not set a root password on a node you are adding. If you do, it changes nothing: the node logs
  and prints `HOARDDB_ROOT_PASSWORD was not used: this cluster already has users. Log in as an
  existing administrator, or unset it.` A node that joins never asks for a root password, with or
  without a terminal.
- If the first node was started without a root password, the cluster has no users and every login
  answers `this cluster has no users yet`. Stop that node, set `HOARDDB_ROOT_PASSWORD` on it (or on
  any data node of the cluster, not a witness) and start it again: the administrator is created as
  long as the cluster has no users. If two data nodes are started with a root password at once, one
  administrator is created and the other node prints the warning above.
- A witness never holds the key, never asks for it and serves no logins or statements: a login on
  a witness answers `this node is a witness: it holds no password key and serves no logins or
  statements`. Connect clients and administrators to data nodes. A witness given a root password
  prints `HOARDDB_ROOT_PASSWORD was not used: this node is a witness` at start.
- A data node reconfigured as a witness
  (`cluster.witness`) no longer reads its `auth.cluster.pepper`, but the file stays in its data
  directory; move it out if that node should hold no copy.
- The key never goes into the cluster's replicated metadata, a log line or an error.
- `status` on a node shows its `Password key`: `held` when the node holds the key, `fetching` while
  a data node waits for it, `none yet` when the cluster has no users, and `witness`. After `join`, a
  script that needs logins on the new node waits until `status` on it says `held`.
- A file that names another key than the cluster's is moved to `auth.cluster.pepper.wrong-<kid>`
  and kept for you. That copy is not sealed when encryption at rest is turned on later; delete it
  yourself once you no longer need it.

**Keep at least two copies.** Once a cluster has users, they are lost for good, while the data
stays, when every copy of the key is gone: the key cannot be recreated. Three shapes hold a single
copy:

- a cluster of one data node, with or without witnesses, always;
- any cluster, between the moment its first administrator is created and the moment a second data
  node holds the key (normally well under a second, longer while the other data nodes are down);
- at `encryption.mode: on` or `require`, any node whose node master is lost, because its sealed
  copy can no longer be opened.

Back up each data node's data directory, which holds `auth.cluster.pepper`, and, with encryption
on, its node master escrow. Run at least two data nodes so a second copy exists. A data node that
lost only its copy asks for it again; `status` shows `fetching` until it arrives. If no data node
holds the key any more, restore `auth.cluster.pepper` from a backup of that node's own data
directory: with encryption at rest on, a sealed copy opens only on the node that sealed it.

## Removing a node

`leave` and `remove-node` answer `<statement> is not available yet: this build does not run it. Run 'nodes' to see the members; join, promote and replace-node are available now`. In this release a node can be stopped,
not removed from the membership. Plan the cluster's size before you create data.

## When it refuses

| You see | It means | Fix |
| --- | --- | --- |
| `no fingerprint is pinned for seed "node-b", so the peer listener cannot tell its key from a stranger's` (at start) | The node lists a seed with no pin | Add `node-b=SHA256:<hex>` to `HOARDDB_SEED_FINGERPRINTS` on this node and on `node-b`, restart both |
| `seed "..." names no node id` (at start) | A seed written as `host:port` | Write it `<node-id>@host:port` on every node |
| `cluster.advertise is not set, and the peer listener binds "0.0.0.0:4433", a wildcard address` (at start), or `this node's cluster address is "...", a wildcard that peers cannot dial` | Peers cannot dial a wildcard | Set `HOARDDB_CLUSTER_ADVERTISE=<this node's address>:<port>` and restart |
| `hash_ring.seed_fingerprints pins node-b to two keys` or `pins SHA256:... to both ... and ...` (at start) | Two keys for one id, or one key for two ids | Keep the entry that node prints on its `entry-ring node open` line |
| `hash_ring.seed_nodes lists this node (...) at ..., but this node's address is ...` (at start) | The seed list gives this node another address | Write the seed with the address in `cluster.advertise`, else `cluster.listen` |
| `cannot join node-d@...: no fingerprint is pinned for node-d` | The member you ran `join` on (or the leader) has no pin for the new node | Step 2 on that member, restart it, retry |
| `cannot join node-d@...: ... did not answer the identity request (...)` | The new node is down, unreachable, or refused this member's key because it did not pin it. This is the symptom of a new node that pinned only some members | Check it is running with `cluster.enabled: true`; make sure it pins **every** existing node (step 1); restart it with the full lists, retry |
| `cannot join node-d@...: node-d advertises <addr> (cluster.advertise, else cluster.listen)` | You typed another address than the node advertises | Run `join node-d@<addr>` with the address named, or set `cluster.advertise` on it |
| `cannot join ...: <addr> is a loopback or link-local address` | The new node listens on `127.0.0.1` while another member is on another machine | Set `HOARDDB_LISTEN` and, for a wildcard, `HOARDDB_CLUSTER_ADVERTISE` to its real address, restart, retry |
| `cannot join ...: it has bootstrapped a cluster of its own` | The new node started with no seeds and made its own cluster | Stop it, start it on a fresh data directory with `HOARDDB_SEED_NODES` naming this cluster |
| `cannot join ...: the node at <addr> presented the key pinned under node-d but says it is <other>` | One id pinned to another node's key | Correct `node-d=SHA256:<hex>` to the key node-d itself prints, on this node and on node-d, restart, retry |
| `cannot join node-d@...: node node-d runs build <build>, which supports cluster versions <a> to <b>, and this cluster runs version <V>` | The new node's build is not the cluster's: older, newer, or one from before the version check (`which sends no state version`) | Run `hoarddb-server -v` on a cluster member and on `node-d`, start `node-d` on the member's build (or the release the text names), then `join node-d@<addr>` again. Nothing was added |
| `cannot join ...: this cluster holds database "..."` | `join` is refused while a database exists | Form the cluster before `create database` |
| `... is not in this node's pinned fingerprints; add it to seed_fingerprints` (a node refusing a connection) | A node presented a key this node has not pinned | Pin it here (step 2) and restart |
| `... presented SHA256:..., but member "node-b" is pinned to SHA256:...: another member's key at this address is refused` | A member's key changed, or the address now belongs to another node | [If a member's key changes](#if-a-members-key-changes) |
| `no fingerprint is pinned for member "node-b", so a dial to ... cannot be verified` | A member joined whose pin this node never got | Add `node-b=SHA256:<hex>` and restart this node |
| `... is not available yet` (`leave`, `remove-node`) | Not available yet | See [Removing a node](#removing-a-node) |
| `node node-c is active at epoch <e>, and replace-node gives the identity of a node that is down or dead to a new process` | The cluster has not seen the node down yet, or it is still running | Stop it if it runs, wait until `nodes` shows it down, run `replace-node` again |
| `cannot replace node-c at <typed>: this release replaces a node at its own address, <addr>` | You typed another address than the member's | Start the node at `<addr>` and run `replace-node node-c --with node-c@<addr>` |
| `replace-node node-c would leave <n> pairs of database "<db>" ... with no copy` | The node held the only copy of those pairs | Put the moved `ring` directory back and restart the node, or accept the loss with `--accept-loss` |
| `cannot replace node-c: the node at <addr> holds cluster state, so it did not lose its data` | The node restarted on its own `ring` directory | Nothing to replace: it rejoins by itself; `nodes` shows it active again |
| `cannot replace node-c: <addr> did not answer the identity request` | The node is not running, or not at that address | Start it as [If a member lost its data](#if-a-member-lost-its-data) step 2 says, retry |
| `replace-node node-c: node-c is committed joining, and ... did not finish` | The statement was cut short after its first change | Run the same `replace-node` again: it finishes it |
| `this node holds no cluster state, so it serves its peers only once a member admits it` (at start), or `refused <peer> on the raft channel: this node holds no cluster state and no member has admitted it` (in its log) | The node started with no cluster state and no member has joined it yet | A new node: run `join <node>@<addr>` on any member (step 3). A member that lost its data: [If a member lost its data](#if-a-member-lost-its-data) |

## A node on another build is refused

Every node declares the cluster versions its build can run: `hoarddb-server -v` prints `state_level=<n> state_range=<min>-<max>`, and `status` prints the version the cluster runs as `Cluster version`. In this release every build prints `state_level=1 state_range=1-1`, so two nodes work together only when they print the same range. A node whose build cannot act at the cluster's version is refused:

- **When you add it.** `join` stops before anything is registered, with `cannot join <node>@<addr>: node <node> runs build ..., and this cluster runs version <V> ...`. Start the new node on the build the cluster runs, or upgrade it to the release the line names, and run `join` again. Nothing was changed. A node on a **newer** build is refused too: a newer build cannot join an older cluster until the cluster is upgraded, so use the build the cluster runs or upgrade every member first.
- **When a member comes back on another build.** Every member refuses its connections and logs `refused <channel> connection from node "<node>": it speaks state versions ...` once a minute. `status` lists it under `Refused peers`. Start the node on the cluster's build. A member whose own build is the older one logs `refused <channel> connection from node "<node>": it writes cluster version <w>, above the highest this build runs (<max>)` instead: the line says how to tell which side to fix. If `status` against `<node>` prints `Cluster version: <w>`, upgrade the refusing member; if the cluster still runs the refusing member's version, start `<node>` on a build whose `state_range` includes it. When a node closes the connection right after the handshake and says nothing, the members log `peer <node> closed the connection right after the handshake: it may run another build`.

Do not run two builds in one cluster. Upgrading one node at a time is not available: upgrade every node to the same build.

## A node that stops itself: exit code 70

A node stops with exit status **70** when its cluster state disagrees with the cluster's. It refused a committed entry that the cluster accepted (or met an entry it cannot read, or one stamped ahead of its own state), so staying up would serve a stale view and could drop writes the cluster accepted. It does not stay up quietly. Before it exits it logs one error line of this shape:

```text
this node's cluster state and the node that proposed committed entry <index> (term <term>, the leader of that term, <node>) disagree: <reason>. Stopping so this node cannot serve a stale view ...
```

Find it in the node's log:

```bash
docker compose logs <node> | grep disagree     # Docker Compose
journalctl -u <unit> | grep disagree           # systemd
```

The line ends with what to do, and in short:

1. Start the stopped nodes on the build the proposing member runs. `nodes` lists the members. Check a build with `hoarddb-server -v`.
2. Keep the data directory. Do not delete or move it: with replication factor 1 it holds the only copy.
3. If the node stops again with the same line, its stored state or its raft log is stale or damaged. Replace the node: keep its data directory, move its `ring` directory aside and follow [If a member lost its data](#if-a-member-lost-its-data). If every node stops this way, there is no node to copy from: the way back is the copy of the data directory taken before the upgrade, restored on every node (see [Compatibility and versions](compatibility.md)).

While a node replays its log after a start, a client request to it is answered UNAVAILABLE `replaying its log`. Retry it: the node serves once its log is applied, and a node that has diverged never serves.

The `status` command says which: `Serving: replaying its log` while the log is applied and `Serving: diverged` on a node that is stopping, and a health check answers not healthy with the same reason. On a node with authentication on, a client login is refused until the node serves, so `status` can only be used once it does; before that, read the node's log.

**Restart policy.** Do not let a supervisor restart a node that exits 70 without limit.

- **Docker Compose.** `docker-compose.yml` and `docker-compose.hoarddb.yml` set `restart: unless-stopped`. Compose cannot exclude one exit code, so a node that exits 70 restarts and stops again on the same line. Each loop is one replay that ends at that line, so the loop is cheap, and the line repeats in `docker compose logs`. To end the loop, stop the node on purpose: `docker compose stop <node>`. Compose does not restart a container you stopped.
- **systemd.** No unit ships with hoardDB. If you write one, exclude the code:

  ```ini
  [Service]
  Restart=on-failure
  RestartPreventExitStatus=70
  ```

  A node that exits 70 then stays down until you start it on the right build with `systemctl start <unit>`. A start refused because this build cannot read the node's newest snapshot exits 70 too, so the same setting keeps it down.

**A node whose build cannot run the cluster version.** A node started on a build whose `state_range` does not include the cluster's version refuses to start, or stops when it meets the version in a snapshot or a log entry, with exit status 70 and a line that starts `the cluster version is <V> and this build runs versions <min> to <max>`. The line names the build to start. Nothing in the data directory is changed: keep it, and start the node on a build whose `state_range` includes `<V>` (`hoarddb-server -v` prints it). A line about the `CLUSTER_VERSION` marker file says the file is not one this build writes: move the file aside and start the node again, as the line says.

## Why it works this way

Every member checks every connection against the fingerprints it has pinned before
a byte is read: a connection from a key it does not know is refused during the TLS
handshake, and a dial to a member must find exactly the key pinned under that
member's id. There is no trust on first use, so a stranger who reaches a peer port
learns nothing and can join nothing. The cost is the one you just paid: adding a
node touches the configuration of every member.
