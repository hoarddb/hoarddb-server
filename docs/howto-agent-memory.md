# How-to: agent memory

An agent or assistant keeps four kinds of state: the messages of a session, the
scratch values of a run, a queue of tasks that workers claim, and the long-term
memories of a user, each with a vector for recall. Every one of those is "this
session's", "this run's" or "this user's" data. That makes it the clearest case
for a layout where the data lives on the entity it belongs to.

## The application and its reads

| Read or write | How often | Shape |
| --- | --- | --- |
| Append a turn to a session | every turn | one write, oldest first |
| Read the last turns of a session | every turn | a page of one session's messages |
| Set or read a scratch value of a run | every step | a key on one run |
| Claim the next tasks of a run, do them, acknowledge | every worker loop | lease, then remove |
| Store a memory of a user, with its vector | after a turn | a keyed write on one user |
| Recall a user's memories most like a query | before a turn | read one user's memories, compare |
| List the sessions a user owns | on login | the sessions whose owner is this user |

Everything on the hot path is about one entity. Only the last read crosses the
kind, and only by one field.

## The layouts this application usually gets

**A relational schema.** Five tables: `sessions`, `messages (session_id, seq,
role, text, at)` with a key on `(session_id, seq)` so a session's last page is a
backward range scan, `runs`, `scratch (run_id, key, value)` with a primary key on
the pair, `tasks (run_id, seq, payload, state, leased_until)` with the lease kept
as columns and a worker loop that updates them, and `memories (user_id, id, text,
vec)` with the vector as bytes. Every read is a join or a range on a foreign key;
the task queue is the part that goes wrong, because a lease is a row the worker
must update and a crashed worker leaves it stuck until something sweeps it.

**A document store.** One collection per entity, never one document per session
holding every message, because a document that grows without bound is the
problem every document store warns about. `messages` is indexed on
`(session_id, seq)`, `scratch` on `(run_id, key)`, `tasks` on `(run_id, state,
seq)`. The lease is again a field the worker sets and a sweeper clears.

**An in-memory data structure server.** The closest shape to hoardDB's: a list per
session, a hash per run, a list per task queue with a second "in progress" list
the worker moves tasks into, and a hash per user for memories with the vector as
a binary string. The structures match the reads exactly, which is why teams
reach for it. What it lacks is a schema, durability by default, a lease that
expires on its own, and a way to say which of those lists belong to the same
entity.

## The hoardDB layout

```
create bucket Session in agents { Owner string, Messages fifoStore }
create bucket Run     in agents { Session string, Scratch hashStore, Tasks fifoStore }
create bucket User    in agents { Name string, Memories hashStore }
```

Three buckets, one per entity, and every store is a field of the document it
belongs to.

**Why `Messages` is a FIFO on the session.** The read is "this session's turns, in
order". A FIFO keeps them in arrival order and gives them back from the front with
`peek`, so the oldest turns are read first, and `count` says how many there are.
A conversation window that wants the newest turns first would declare a
`lifoStore` instead; the choice is the read's, not the engine's.

**Why `Scratch` is a hash on the run.** Scratch values are set and read by key,
one run at a time. A hash store gives `put` and `get` by key on one document.
There is no table of every run's scratch to index, because no read ever asks for
that.

**Why `Tasks` is a FIFO with `claim` and `ack`.** The worker loop is the part the
other layouts get wrong, so it is a verb here, not a convention. `claim` leases
the next tasks to one worker and hands back a lease id. `ack` with that id
removes them for good. A worker that dies before acknowledging loses its lease
when it expires, and the tasks come back to the front of the queue by
themselves. Nothing sweeps, and no task is lost or done twice while the lease is
held.

**Why `Memories` is a hash on the user with the vector inside the element.** A
memory is written once by key and read back by key or as the user's whole set.
The vector is a field of the element, stored as binary, so a user's memories and
their vectors travel together, with the user. Recall is "read this user's
memories and compare", which is one keyed read of one document's store.

**Why `Owner` is a plain field on `Session`.** "The sessions this user owns" is
the one read that crosses the kind. It is served by `list` with an equality
filter on a declared scalar field, and that is the right tool for a read that
runs once per login. If it ran on every turn, the layout would add a
`Sessions hashStore` to `User` and write both sides, the way the chat and
social examples do for membership.

## What changes in the application

- A turn is one `push`; reading the window is one `peek` or `list`. There is no
  session table to join.
- The worker loop becomes `claim`, work, `ack`. Code that set a `state` column,
  retried on a stale lease and swept stuck rows goes away.
- Recall reads one user's memories and compares the vectors in the application.
  That is the correct read for a few thousand memories per user; a server-side
  read for it is not available yet.
- A session's owner is written once, as a field. Listing a user's sessions is a
  filtered list, which runs on the nodes that hold the bucket's slots.
- Everything is one write per fact. There is no second table to keep in step
  except the one you choose to add for a hot cross-entity read.

## Cost and limits

**What a document costs.** A session with its messages is one document and one
store: a few keys in the node's one shared store, no file, no descriptor and no
goroutine of its own. Ten thousand sessions cost ten thousand documents' worth of
keys and nothing else; the server's work per turn does not grow with the number
of sessions.

**What this layout does not serve.**

- A text search across every session's messages. Messages live on their session;
  a search across all of them is a scan of every session, and the layout does
  not pretend otherwise.
- Sorting all users by how many memories they hold, or any ranking across the
  kind by a store's size.
- Recall by similarity across every user's memories in one read. Per user, yes;
  across users, that is one keyed read per user, which is fine for a tenant of
  dozens and wrong for a tenant of millions.
- A lease longer than the configured maximum, or a task payload above the element
  limit. See [Limits](limits.md).

If one of those is on your hot path, this is not the layout for it, and the
[Limitations](limitations.md) page is the honest next read.

## Try it

Start a server and seed the layout as described on the [How-to](howto.md) page,
with `hoarddb seed agent`. The transcript below was captured against that data: forty
users, sixty sessions of forty turns, sixty runs with six scratch keys and ten
tasks each, two hundred memories per user.

```
[h]oardDB [agents]> use scenarios_agent
Switched to database 'scenarios_agent'
[h]oardDB [scenarios_agent]> db.Session.list({limit: 2})
{"Messages": "fifoStore (40 elements)", "Owner": "u7", "_id_": "s15"}
{"Messages": "fifoStore (40 elements)", "Owner": "u8", "_id_": "s23"}
more: db.Session.list({limit: 2, cursor: "0100000000000000016919e017f7c24abc01b906507332330000045e47dd"})
[h]oardDB [scenarios_agent]> db.Session["s15"].Messages.count()
40
[h]oardDB [scenarios_agent]> db.Session["s15"].Messages.peek(2)
{"at": 1700015000, "role": "user", "text": "shjkrcuf lfoajtg erofxlvx qevvmh gclvusafg duf jn mkpvynxx yxoabsvn aei qtrnwezxs ejjclk zud gmuobdyx pendnkx ggku auqiuxl zetgu ctbt hi"}
{"at": 1700015010, "role": "assistant", "text": "fzgqa ki fanmm ovtjiku ftmpq ihmxqlj mjx ozxwypwwb dvjaubz yqf khzfiyqiz ozqtu grsu sp lu tbxwivr xmioujoi xptrylfif wqed mgukrvrpy dtwqvamd uypoghk kxhd akpbcx"}
[h]oardDB [scenarios_agent]> db.Session["s15"].Messages.push({role: "user", text: "and in metric units please", at: 1700015200})
OK  pushed: Messages now holds 41 elements
[h]oardDB [scenarios_agent]> db.Session.list({filter: {Owner: "u7"}, limit: 3})
{"Messages": "fifoStore (41 elements)", "Owner": "u7", "_id_": "s15"}
```

A list of documents shows each store field as its type and element count, not
its contents; the contents are read on the document. The seeded text is random
words, so the shape is real and the prose is not.

The run: scratch by key, and the worker loop.

```
[h]oardDB [scenarios_agent]> db.Run["run17"].Scratch.list()
k0  {"value": "lfcuwan ahmqhuj hwumbk dybnmtw"}
k1  {"value": "nyryyfdqu ecbs okuli xgqhl"}
k2  {"value": "obk zuyfr di nohliwq"}
k3  {"value": "jmkeam ng khm jieubrku"}
k4  {"value": "txvhczq gcnp usaze bx"}
k5  {"value": "ogy yvpvq xjz aesewkzfm"}
[h]oardDB [scenarios_agent]> db.Run["run17"].Scratch.put("k2", {value: "step 3 done", at: 1700015100})
OK  put "k2": Scratch now holds 6 elements
[h]oardDB [scenarios_agent]> db.Run["run17"].Scratch.get("k2")
{"at": 1700015100, "value": "step 3 done"}
[h]oardDB [scenarios_agent]> db.Run["run17"].Tasks.count()
10
[h]oardDB [scenarios_agent]> db.Run["run17"].Tasks.claim(2)
lease 000001a207f708deae90258337c216c1
{"payload": "mwxpbdy tawr jcd xafeli kcxsnvq mk lykuttwrh oykzctwgw", "task": "t0"}
{"payload": "tawr clqt cjhijr dby jgcia ckzaqjx ovqjfwad aujy", "task": "t1"}
[h]oardDB [scenarios_agent]> db.Run["run17"].Tasks.count()
8
[h]oardDB [scenarios_agent]> db.Run["run17"].Tasks.ack("000001a207f708deae90258337c216c1")
OK  acked lease 000001a207f708deae90258337c216c1
[h]oardDB [scenarios_agent]> db.Run["run17"].Tasks.count()
8
```

The count drops when the tasks are claimed, because a claimed task is leased to
one worker and no other worker sees it. `ack` makes the removal final. Had the
worker died instead, the lease would have run out and the two tasks would be at
the front of the queue again.

The user's memories, with the vector as a binary field of the element. The
base64 of the 256-byte vector is shortened here; the server returns all of it.

```
[h]oardDB [scenarios_agent]> db.User["u7"].Memories.count()
200
[h]oardDB [scenarios_agent]> db.User["u7"].Memories.get("m003")
{"text": "cwqvfwzch brs aoxvjz tqqatu riduupnxq vratqzrm xxrbm usvmpg xv fm riduupnxq sxej", "vec": {"$binary":{"base64":"cOZOviBzZT6Yf2M+hJtPPyS3d7+qqSU/TBR1vyh+NL/o7hu+oFNyP3gCAb78ZI2+jNgzv8SMdz+YE0C/MBT8vWDnBj5aXls/KBcPv/w0cL+oj2y/fidZP8SYfz9gUzU/0J7yPZD8Qz/432W/EG1CvrCvOj/otmq/DCBSv9gvAT+Uufa+9OOkPvTLZ78Yz7S+SC5QPzBVTj5awz+/XMdYP2DXRL4M28A+yHBoPu4cAT9CC0e/8CE2v2T3kr7IkvS+0JhvPjiyVj5QtmM+VDKhPuBZFz6ozIe+MNuyPVj3Pz8gl/U9ALoHPLAYm73kOnm/wFPnPWBhvz0Mc90+uPknPw==","subType":"00"}}}
[h]oardDB [scenarios_agent]> db.User["u7"].Memories.get("m-u7-03")
CLI error: NOT_FOUND: Memories of "u7" in scenarios_agent.User has no key "m-u7-03". Run 'db.User["u7"].Memories.list()' to see what it holds, or 'db.User["u7"].Memories.put("m-u7-03", {…})' to add one
```

The last line is a deliberate miss: a key that is not there answers with the
statement that lists what is, and the statement that would add it.
