# Let hoardDB suggest a design: `hoarddb plan`

`hoarddb plan` reads what your application stores and how it reads it, and prints a
design you can read, edit and run: `create bucket` statements, the evidence for each,
what each design costs, and a scorecard that says how well the application fits
hoardDB at all. It connects to nothing and creates nothing. **You pick the
structure; the command never applies its own suggestion.**

It takes one of two inputs:

- **a sample of your data**: BSON files or JSON Lines files, read the way
  [`hoarddb ingest`](ingest.md) reads them, at most 10,000 documents per file;
- **a model file**: a small JSON description of your kinds, their fields and what
  refers to what. You, a script or an AI agent can write one. A single file ending in `.json` is read as a model
  when its first object has `version` and `kinds`; otherwise it is read as a JSON Lines dump.

Either input can come with an **access trace**: one line per thing your application
does ("list comments by post, oldest first"). The data cannot tell a queue from a
stack; the trace can.

## Try it in two minutes

Save a model of a small blog (three kinds, 24,000 comments) and a three-line trace:

```sh
cat > blog.model.json <<'EOF'
{
 "version": 1,
 "source": "hand",
 "app": "blog",
 "kinds": [
  {"name": "users", "documents": 200, "sampled": 200, "fields": [
   {"name": "_id", "type": "string", "present_permille": 1000, "distinct_permille": 1000},
   {"name": "name", "type": "string", "present_permille": 1000}]},
  {"name": "posts", "documents": 2000, "sampled": 2000, "fields": [
   {"name": "_id", "type": "string", "present_permille": 1000, "distinct_permille": 1000},
   {"name": "title", "type": "string", "present_permille": 1000},
   {"name": "author_id", "type": "string", "present_permille": 1000, "ref": {"to": "users", "resolve_permille": 1000}}]},
  {"name": "comments", "documents": 24000, "sampled": 24000, "fields": [
   {"name": "_id", "type": "string", "present_permille": 1000, "distinct_permille": 1000},
   {"name": "post_id", "type": "string", "present_permille": 1000, "ref": {"to": "posts", "resolve_permille": 999}},
   {"name": "author_id", "type": "string", "present_permille": 1000, "ref": {"to": "users", "resolve_permille": 1000}},
   {"name": "body", "type": "string", "present_permille": 1000},
   {"name": "ts", "type": "datetime", "present_permille": 1000}]}
 ]
}
EOF
cat > blog.trace.jsonl <<'EOF'
{"op": "get",  "coll": "users",    "by": ["_id"]}
{"op": "list", "coll": "comments", "by": ["post_id"],   "sort": ["ts:asc"],  "limit": 20, "weight": 10}
{"op": "list", "coll": "comments", "by": ["author_id"], "sort": ["ts:desc"], "limit": 10}
EOF
```

Ask for a plan, with no questions:

```sh
hoarddb plan blog.model.json --trace blog.trace.jsonl --no-input
```

```text
wrote hoarddb-plan.md: 2 buckets, 1 choice open; verdict: yes
```

That wrote `hoarddb-plan.md`. Use `--out -` to print it instead. This is the whole file:

````text
<!-- hoarddb-plan v1 -->
# hoardDB plan

A suggestion, not a change: nothing here was created, and you pick every structure.

## 1. Inputs

- source: hand; thresholds version 1
- comments: 24,000 documents (24,000 sampled)
- posts: 2,000 documents (2,000 sampled)
- users: 200 documents (200 sampled)
- trace: 3 lines

## 2. Scorecard

| # | Rule | Cell | Finding |
| --- | --- | --- | --- |
| 1 | dynamic filters | P | none found |
| 2 | client sorting | P | none found |
| 3 | non-reducible joins | P | none found |
| 4 | permissions in the query | P | none found |
| 5 | atomic multi-entity writes | P | none found |

P is a pass, p is partial (admin surface only, or a sort that comes from a setting), F is a fail.

### Verdict: yes

#### Confidence and limits

No rule found a trace line to count against.
This verdict is built on what the inputs claim: the sample, the model and the trace. It does not see how often each read runs (only the `weight` you gave), which lines are on the admin surface (only the `surface` you gave), the counts and fans you supplied, or any query you left out. A line left out can only make the verdict kinder.
To change a verdict, change the design the trace describes and then update the trace. Editing a line to move a cell changes the claim, not the application.

## 3. Query reductions

- key read (trace line 1)
  - source: `get users by _id`
  - hoardDB: `db.User["<id>"].get()`
  - cost: seek
- child rows by parent key, ordered (trace line 2)
  - source: `list comments by post_id sort ts:asc limit 20`
  - hoardDB: `db.Post["<id>"].Comments.list({limit: 20})  (fifoStore)`
  - cost: seek
- read across owners (trace line 3)
  - source: `list comments by author_id sort ts:desc limit 10`
  - hoardDB: `not served by the store field; choose the bucket design`
  - cost: scan  needs indexes (not available yet)

## 4. Kinds and suggestions

### Post (from posts; grade: conflict)

- Comments is a store field (fifoStore; grade: conflict), from comments
  - [S1] comments has _id in every sampled document: the key
  - [S5] comments.author_id refers to users (1000 per mille resolve)
  - [T6] trace line 3 reads comments by author_id: a read across owners, so a bucket
  - [S5] comments.post_id refers to posts (999 per mille resolve)
  - [S6] comments refers only to posts through post_id (999 per mille resolve) and nothing refers to comments: a candidate owned collection of posts
  - [T2] trace line 2 lists comments within one document of posts sorted ts:asc
  - alternatives: bucket Comment; both: Comments and a Comment bucket written twice
- [S1] posts has _id in every sampled document: the key
- open choice: comments.placement

### User (from users; grade: trace)

- [S1] users has _id in every sampled document: the key
- [T1] trace line 1 reads users by _id

## 5. Will not port

Nothing in the inputs is outside what hoardDB serves.

## 6. Cost

Computed from the formats, not measured. A bucket costs no file, descriptor, mapping or goroutine.

| Design | Buckets | Store fields | Declaration bytes | Documents | Store elements | Keys per document |
| --- | --- | --- | --- | --- | --- | --- |
| Design A (as suggested) | 2 | 1 | 898 | 2,200 | 24,000 | Post 1+2+12, User 1+1+0 |
| Design B (each conflict as its own bucket) | 3 | 0 | 1308 | 26,200 | 0 | Post 1+2+0, User 1+1+0 |
| Design C (both: store field and bucket, written twice) | 3 | 1 | 1334 | 26,200 | 24,000 | Post 1+2+12, User 1+1+0 |

Design A (as suggested): reads that scan today or cannot be served: not possible with the store field: list comments by author_id sort ts:desc limit 10 (weight 1)

Design B (each conflict as its own bucket): reads that scan today or cannot be served: scan of every comments document: list comments by post_id sort ts:asc limit 20 (weight 10); scan: list comments by author_id sort ts:desc limit 10 (weight 1)

Design C (both: store field and bucket, written twice): reads that scan today or cannot be served: scan of every comments document: list comments by post_id sort ts:asc limit 20 (weight 10); scan: list comments by author_id sort ts:desc limit 10 (weight 1)

## 7. HQL

1 choice is open; this section has more than one statement for `Comment`. Pick one.

### Post

```text
create bucket Post { Title string, AuthorId string, Comments fifoStore }
```

### Comment (alternative: Comments as its own bucket)

```text
create bucket Comment { PostId string, AuthorId string, Body string, Ts datetime }
-- instead of the Comments field above; with both, keep Post and add this bucket
-- not available yet: index Comment.AuthorId (list filters scan)
```

### User

```text
create bucket User { Name string }
```

## Next

1. In a hoardDB shell, pick a database: `create database app`, then `use app`.
2. Run the statement you picked for each bucket.
3. Load each file into its bucket: `hoarddb ingest <file> --into app.<bucket>`.
````

Read it in this order:

1. **Scorecard.** Five rules decide whether an application fits: dynamic filters,
   client-chosen sorting, joins that are not a fetch by key, permissions evaluated
   inside the query, and writes to several kinds that must commit together. `P` is a
   pass, `p` a partial, `F` a fail. The verdict is one of `yes`, `yes with caveats`,
   `only with redesign of ...` and `not a good candidate for hoardDB`. A no is printed
   as plainly as a yes.
2. **Query reductions.** Each traced read, and the hoardDB read that replaces it, with
   its cost: `seek`, `keyed batch`, `merge`, `scatter` or `scan`.
3. **Kinds and suggestions.** One block per bucket, with the evidence behind each choice.
4. **Will not port.** What hoardDB does not serve (text search, ad-hoc grouping, joins on
   non-keys, writes that must commit together across kinds), and what to use instead.
5. **Cost.** Declaration bytes per design, computed from the storage format and not
   measured.
6. **HQL.** The statements. When a choice is open, every candidate is its own complete
   statement under its own heading: copy exactly one per bucket.

## When it asks

The example has one conflict: `comments` belong to a post (oldest first, 24,000 of them),
but one traced read lists them by author, across posts, which a store field cannot serve.
In a terminal, without `--no-input`, the command asks:

```text

comments: store field of Post, or its own bucket?
  The data says: comments refers only to posts through post_id (999 per mille resolve) and nothing refers to comments: a candidate owned collection of posts
  The trace says: trace line 3 reads comments by author_id: a read across owners, so a bucket; trace line 2 lists comments within one document of posts sorted ts:asc
  1) Comments fifoStore on Post  - one seek per owner; a read of comments across owners is not possible
  2) bucket Comment  - both reads work; each owner's list is a scan: indexes are not available yet
  3) Comments fifoStore on Post, and a Comment bucket the application writes twice  - two writes per element; the application keeps them consistent
  [1/2/3/s/q]: 
```

The three answers:

| Answer | What you get | What it costs |
| --- | --- | --- |
| `1` store field | `Comments fifoStore` on `Post`: one seek per post | reading comments by author is not possible |
| `2` bucket | a `Comment` bucket | both reads work; a list by post is a scan, because there are no indexes ([Indexes](indexes.md)) |
| `3` both | the store field and a bucket | your application writes every comment twice and keeps them consistent |

`s` leaves the choice open and `q` stops asking. Save your answers, and the next run asks
nothing:

```sh
hoarddb plan blog.model.json --trace blog.trace.jsonl --save-answers blog.answers.jsonl
hoarddb plan blog.model.json --trace blog.trace.jsonl --answers blog.answers.jsonl
```

An answer is final. The plan prints your design first and keeps the evidence you overruled
beneath it, marked `overruled:`.

## Run what you picked

The plan's statements are for you to run. Pick a database and run one statement per bucket:

```text
create database app
use app
create bucket Post { Title string, AuthorId string, Comments fifoStore }
create bucket User { Name string }
```

The last step of the plan loads each file into its bucket, one command per file:

```sh
$ hoarddb ingest users.bson --into app.users
ingest: users.bson → app.users — 3 read, 3 written in <time>
  created database app
  created bucket app.users with 1 field: name string
```

The store fields the plan designs, such as `fifoStore` and `hashStore`, are filled
by your application or a driver, not by ingest.

Lines that start with `--` under a statement are notes, not part of it. `-- not available yet:` marks
something hoardDB does not accept today, such as a unique field or an index; keep the line
as a reminder.
`-- cannot be written on this build:` marks a store that a build cannot write to yet,
with the alternative that works today.

## The access trace

One JSON object per line. Every field except `op` and `coll` is optional.

| Field | Values | Meaning |
| --- | --- | --- |
| `op` | `get`, `list`, `range`, `insert`, `update`, `delete`, `take`, `put`, `join`, `aggregate`, `search` | what the application does; `take` reads one and removes it |
| `coll` | a kind name | which kind |
| `by` | field names | the fields the operation selects on; `["_id"]` is a read by key |
| `sort` | `field:asc` or `field:desc` | the order of the result |
| `order` | `oldest`, `newest`, `priority:<field>` | for `take`: which element comes out |
| `key` | a field | for `range`: the field whose range is read |
| `limit` | a number | the usual page size |
| `bytes_p50` | a number | for `put`: the usual size of the bytes |
| `weight` | a number, default 1 | how often, relative to a line with no weight |
| `surface` | `primary` or `admin` | admin lines can only earn a partial on the scorecard |
| `filters` | `[{"field": "status", "optional": true}]` | filters a list composes at run time |
| `sort_from` | `client`, `fixed`, `setting` | who chooses the sort |
| `visibility` | `owner`, `membership`, `container`, `object_scoped`, `object_global` | the permission predicate on the read |
| `atomic_with` | a name | writes that one transaction performs together; two kinds in one group fail rule 5 |
| `guard` | `{"field": "value"}` | an update that runs only if a field holds a value; a guarded group can be folded into one document |
| `inc` | field names | an update that increments fields by hand; the plan notes the counter |
| `join` | `{"to": "kind", "on": "field", "kind": "pk"}` | a join; `kind` is `pk`, `nonkey`, `visibility`, `aggregate` or `sort`. Only `pk` is a plain fetch by key |
| `dynamic` | `true` | the dimension of an aggregate, or the segment of a list, is chosen at run time |
| `substring` | `true` | a substring, regex or full-text match on a field. It fails rule 1 unless a `search` line for the same kind says a search engine serves it |

A line with an unknown `op`, an unknown `coll` or an unknown field is refused with its
line number and the accepted values.

## For an agent

An AI agent can read your code in any language and write the model file; `hoarddb plan`
then judges it the same way every time. Two commands give the agent what it needs:

```sh
hoarddb plan guide > plan-rules.md
hoarddb plan schema > plan-model.schema.json
```

`guide` prints the rules as Markdown sized for a system prompt: the placement rule, the
patterns, the five scorecard rules with their thresholds, and the loop below. `schema`
prints the JSON Schema of the model file. The loop:

1. Write a model file that validates against the schema.
2. Run `hoarddb plan model.json --json`.
3. Revise the design the model describes, or accept the fail. Do not edit trace lines, `surface`,
   counts or fans to move a cell: that changes the claim, not the application. A refusal under
   `--json` is `{"error": {"message": "...", "fix": "..."}}`, and `fix` is what to do.
4. Put the open choices to the person, with the plan's cost lines beside each option.

The agent does not pick the structure either: you do.

## Flags

| Flag | Meaning |
| --- | --- |
| `<file-or-dir>...` | BSON or JSON Lines files, a directory of `.bson` files, or one `model.json` |
| `--trace <file>` | the access trace |
| `--answers <file>`, `--save-answers <file>` | read, or write, the answers to the questions |
| `--sample <n>` or `--sample all` | documents read per file; default 10000 |
| `--out <file>` or `--out -` | where the plan goes; default `hoarddb-plan.md`. A file the command did not write is never replaced unless you add `--force` |
| `--json` | the plan as one JSON object on stdout: `inputs`, `scorecard` (with `decided_by` and `limits`), `reductions`, `suggestions`, `wont_port`, `cost`, `hql` |
| `--no-input` | ask nothing; leave every choice open. This is the default when stdin is not a terminal |
| `--max-buckets <n>` | the most files read from one directory; default 1000 |
| `--thresholds` | print every constant the detectors use |
| `guide`, `schema` | as the first word: print the rules for an agent, or the model file's JSON Schema |

The command is deterministic: the same files give the same plan, byte for byte. It issues
no network call.
