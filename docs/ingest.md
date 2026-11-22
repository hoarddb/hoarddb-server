# Load a file: `hoarddb ingest`

`hoarddb ingest` loads **one file into one bucket**:

```sh
$ hoarddb ingest users.bson --into app.users --insecure
```

It reads `.bson` (documents one after another, the way a BSON dump tool writes them) and
JSON Lines (`.jsonl`, `.ndjson` and `.json`, one JSON document per line; not a JSON array,
and not a compressed file). The file is read twice: once to work out its fields and their
types, once to write, so it must be a file and not standard input.

- The database and the bucket are created when they are absent, and the bucket's
  declaration is inferred from the file ([What ends up in the bucket](#what-ends-up-in-the-bucket)).
- Every document is written as an **upsert by its `_id`**, so loading the same file twice
  changes nothing: the second run replaces each document with itself.
- A bucket that already exists keeps its own declaration, and the file must fit it
  ([When it says no](#when-it-says-no)).

Loading a whole directory, and loading a file into a store field of other documents, are
not available: load each file with `--into`.

## Load your first file

Take a `users.bson` that holds three documents: `{_id: "ada", name: "Ada"}`,
`{_id: "grace", name: "Grace"}` and `{_id: "alan", name: "Alan"}`. A dry run first says
what the load would do ([A dry run](#a-dry-run)); then load the file:

```sh
$ hoarddb ingest users.bson --into app.users --insecure
ingest: users.bson → app.users — 3 read, 3 written in <time>
  created database app
  created bucket app.users with 1 field: name string
```

The command signs in as the root user: pass `--user` and `--password`, or set
`HOARDDB_ROOT_USER` and `HOARDDB_ROOT_PASSWORD` in the environment, which keeps the password
out of `ps`, as [Get started](getting-started.md#connect-the-cli) does. `--insecure` skips the
certificate check, so it is for a server you started yourself; against any other host, leave it
out and pass `--fingerprint SHA256:<hex>`.

The first line of each run is the summary, and the lines under it say what happened to the
database and the bucket. Both go to standard error, so `--json` on standard output stays
readable by a script.

Now open a session (`./hoarddb -insecure`, as
[Get started](getting-started.md#connect-the-cli) shows), select the database and read a
document back:

```sh
$ use app
Switched to database 'app'
$ db.users["ada"].get()
{"_id_": "ada", "name": "Ada"}
```

The `_id` of the file became the ring's text `_id_`, and the other top-level fields became
the bucket's fields.

## What ends up in the bucket

The declaration is inferred from the values the file holds. A new bucket gets the word in
the right column:

| What the file holds in a field | Declared word |
|---|---|
| only strings | `string` |
| only integers (32-bit or 64-bit) | `int` |
| only doubles | `double` |
| only decimals | `decimal` |
| integers, doubles or decimals together | `number` |
| only `true`/`false` | `bool` |
| only dates | `datetime` |
| only ObjectIds | `objectid` |
| only binaries | `binary` |
| only documents | `object` |
| only arrays | `array` |
| only regular expressions | `regex` |
| only BSON timestamps | `bsontimestamp` |
| values of two or more of those kinds (a string here and an integer there) | `any` |
| only nulls, or only one of the BSON kinds the ring has no word for (undefined, symbol, JavaScript, DBPointer, min key, max key, code with scope) | `any` |

Three rules turn those rows into a declaration:

- **`null` never changes the word.** A field that holds a string and a null is `string`.
- **A field that some documents leave out stays optional.** A bucket declares no required
  fields.
- **A file with more than 999 distinct top-level field names is refused before anything is
  created.** That is how many fields a bucket holds.

A field name is 1 to 255 bytes of printable UTF-8. A file whose field name breaks that is
refused before anything is written; the fix is to rename the field in the file, and there
is no flag for it.

**A bucket that already exists keeps its own declaration.** The file is checked against it
before a document is written, and the run is refused when a field is undeclared, when a
declared type cannot hold the data, or when the name belongs to a store field. The messages
in [When it says no](#when-it-says-no) name the statement that adds a field.

### The key of each document

The ring's `_id_` is text, so every `_id` is rendered by one fixed rule. A bare id is an
ObjectId's hex or a string; everything else starts with `~`, a type letter and a colon:

| `_id` in the file | `_id_` in the ring | Example |
|---|---|---|
| ObjectId | its 24 lowercase hex digits | `507f1f77bcf86cd799439011` |
| string | the string itself, unless it needs escaping (below) | `alice` |
| int32, int64 | `~i:` and the decimal digits | `~i:1` |
| double | `~i:` with the digits when the value is a whole number that fits a 64-bit integer, else `~d:` and the shortest text that reads back as the same double | `1.0` becomes `~i:1`, `2.5` becomes `~d:2.5` |
| bool | `~b:true` or `~b:false` | |
| datetime | `~t:` and the milliseconds since the Unix epoch, with `-` before the epoch | `~t:1700000000000` |
| null | `~z:` | |
| binary | `~y:`, the subtype as two hex digits, `:`, the bytes as hex | `~y:04:00112233` |
| every other type | `~x:`, the BSON type byte as two hex digits, `:`, the value's raw bytes as hex | `~x:03:0500000000` |

**Escaping.** A string `_id` that begins with `~` is written with the `~s:` prefix, so the
string `~i:1` becomes `~s:~i:1` and cannot be mistaken for the integer `1`. A string that is
exactly 24 lowercase hex digits is written the same way (`~s:` and the digits), so it stays a
different document from the ObjectId with those digits.

Because of that escape, **an integer and a whole double of the same value are one id**
(`~i:1` both), and the string `"1"` is another (`1`). **A document with no `_id` is
refused**, because a run without an id cannot be repeated safely.

The original numeric type of an `_id` is not kept: `~i:` records neither int32 nor int64
nor double ([What ingest does not carry over](#what-ingest-does-not-carry-over)).

## When it says no

Every refusal names the thing, then the command or the change that fixes it.

| What happened | What it says, and the fix |
|---|---|
| `--no-create` and the database is missing | `Database "app" not found. Run 'show databases' to list what exists, or 'create database app', or leave out --no-create.` |
| `--no-create` and the bucket is missing | `Bucket "users" not found in database "app". Run 'show buckets in app' to list what exists, or leave out --no-create and ingest will create it from the file (run with --dry-run first to see the declaration).` |
| an existing bucket does not declare a field of the file | `users.bson has a field "email" that bucket "users" does not declare. Nothing was written. Add it: 'alter bucket users in app { add email string }', or remove it from the file.` |
| a declared type cannot hold the data | `Field "age" is a string in 12 documents of users.bson and bucket "users" declares int. A declared type cannot be changed, so load into a new bucket: --into app.users2, or fix the 12 values (the first is document 9).` |
| the name belongs to a store field | `Field "Items" is a fifoStore in bucket "users", and a store is filled with its own commands, not from a field of a file. Remove "Items" from the file, or load into a new bucket: --into app.users2.` |
| more than 999 distinct field names | `users.bson has 1,204 distinct field names and a bucket holds at most 999. Nothing was created. Drop the fields you do not need from the file, or split it.` |
| a field name the ring cannot declare | `Field "…" in users.bson cannot be declared on the ring: it has a name of 0 bytes and a field name is 1 to 255. Send a name in that range. Nothing was written. Rename that field in the file and run ingest again.` |
| a document with no `_id` | `Document 17 of users.bson has no _id. hoarddb will not invent one, because running the file again would then add the document again. Give every document an _id. Nothing was written. Fix the document, or pass --on-error skip to load the others and list the skipped ones in a file (--rejects).` |
| a rendered `_id` the ring cannot take | `Document 17 of users.bson has an _id that renders to a text of 2,100 bytes, which encodes to 2,103 bytes, and a document id encodes to at most 1,024 bytes (a text of up to 1,021 bytes fits). Change the _id in the file. Nothing was written. Fix the document, or pass --on-error skip to load the others and list the skipped ones in a file (--rejects).` |
| a JSON array | `users.json starts with '[': ingest reads one JSON document per line, not a JSON array. Convert it with:  jq -c '.[]' users.json > users.jsonl   and ingest users.jsonl` |
| a compressed file | `users.bson.gz: this version reads uncompressed .bson and JSON Lines only. Run 'gunzip -k users.bson.gz' and ingest users.bson, or re-export without compression` |
| standard input (`-`) | `ingest reads the file twice (once to work out the fields, once to write), so it cannot read standard input. Save it to a file first, then run: hoarddb ingest <file> --into app.users` |
| a missing file | `cannot read users.bson: open users.bson: no such file or directory. Check the path.` |
| a directory instead of a file | `Loading a whole directory is not available. Load each file into its bucket: hoarddb ingest <dir>/<name>.bson --into <database>.<name>` |

These two refusals are per document, so `--on-error skip` applies to them: the run loads the
others, exits 3, and prints a line such as `1 skipped and not saved. Add --rejects <file> to
keep them.`, or `1 skipped; the ones that could be saved are in <file>` when `--rejects` is
given.

**Removed flags.** These are not flags of `ingest`, and none has an alias: `--type`, `--id-as`, `--write-concern`, `--allow-expiring`, `--state`, `--no-resume`, `--resume-anyway`, `--accept-duplicates`, `--from-offset`, `--again`, `--checkpoint-every`, `--allow-dots`, `--into-db` and `--max-buckets`. Each prints a sentence that names what to do instead — `--type is gone: a bucket has no type. Declare what it holds with 'create bucket <name> in <db> { <Field> <type> }', then load the file with --into <database>.<bucket>.` is one — and `hoarddb ingest --help` lists them.

### Skipping bad documents

A document the run cannot load stops it by default: `--on-error stop` is the default, and
the message names the document by its position in the file. Pass `--on-error skip` to load
the rest, and `--rejects <file>` to keep the documents that were skipped, so you can fix
them and load just those:

```sh
$ hoarddb ingest events.jsonl --into app.events --on-error skip --rejects events.rejects.jsonl --insecure
```

The rejects file is created with mode 0600 and is never overwritten; a run that skips
nothing leaves no file behind. With `--on-error skip` the run exits **3**, so a script can
tell a complete load from a partial one.

## A dry run

`--dry-run` reads the file and prints the declaration it would create and what that
declaration costs, and creates nothing: no database, no bucket, no document.

```sh
$ hoarddb ingest users.bson --into app.users --dry-run --insecure
ingest (dry run): users.bson → app.users — 3 read, would write 3
  would create database app
  would create bucket app.users with 1 field:
    name string
  cost: a bucket declaration is 8 keys and 436 bytes plus about 26 bytes per field (computed, not measured)
  it read the file only, and nothing was written
```

Against a bucket that already exists it prints that bucket's declaration instead, under the
line `bucket app.users exists and keeps its declaration: …`, which answers what a load would
do to a bucket you already made.

## Machine-readable output

`--json` prints one JSON object on standard output when the run ends. Everything else — the
progress line and the text summary — goes to standard error.

```json
{"ok": true, "partial": false, "dry_run": false, "input": "users.bson", "format": "bson",
 "database": "app", "bucket": "users", "documents_read": 3, "documents_written": 3,
 "documents_skipped": 0, "empty_input": false, "created_database": true,
 "created_bucket": true, "declaration": [{"name": "name", "type": "string"}],
 "warnings": [], "errors": [], "elapsed_ms": 42}
```

The object has these members, always present unless marked optional:

| Member | Meaning |
|---|---|
| `ok` | the run completed and wrote every document |
| `partial` | the run failed or skipped documents after writing some |
| `dry_run` | the run was `--dry-run` and wrote nothing |
| `input`, `format` | the file's name and `bson` or `jsonl` |
| `database`, `bucket` | the target |
| `documents_read`, `documents_written`, `documents_skipped` | counted from the file |
| `empty_input` | the file held no documents, so no bucket was created |
| `created_database`, `created_bucket` | what the run created |
| `declaration` | the bucket's fields, each `{name, type}` |
| `warnings` | sentences about the run, such as a field given the word `any` |
| `errors` | the skipped documents, each `{document, reason}`, up to 100 |
| `elapsed_ms` | the run's duration |
| `stopped_because` (optional) | why the run stopped early |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | every document was loaded |
| 1 | a bad flag, or a file that cannot be read at all: nothing was sent |
| 2 | refused before anything was written: a missing target under `--no-create`, a field the bucket does not declare, a type the declaration cannot hold, a document with no usable `_id`. Do not retry it unchanged |
| 3 | the run completed with documents skipped (`--on-error skip`) |
| 4 | Ctrl-C ended the run early |
| 6 | no session could be opened: nothing was sent |
| 7 | the server refused the login. Do not retry it unchanged |
| 8 | a request was sent and its outcome is unknown: run the same command again |

## Interrupted? Run it again

Every document is an upsert by its `_id`, so running the same command again finishes the
load and adds nothing twice. There is no resume file to find, and no run to restart; the
one thing an interruption costs is the time the file takes to read again.

## What ingest does not carry over

- **Indexes.** A bucket is declared with its fields; declare an index yourself.
- **The typed `_id`.** The ring's `_id_` is text, so the original type of an `_id` is not
  kept, and int32, int64 and a whole double of the same value share one id.
- **The bucket's store fields.** Ingest never declares a store field and never declares a
  link between buckets: inference produces scalar words only. Declare those yourself with
  `create bucket`, and let the application fill them.

## Every flag

| Flag | Meaning |
|---|---|
| `--into <database>.<bucket>` | the target; required. A three-part target names a store field and is refused |
| `--format bson\|jsonl` | the file's format, when its name does not say |
| `--no-create` | refuse a missing database or bucket instead of creating it |
| `--on-error stop\|skip` | stop at the first document that cannot be loaded (default), or skip it |
| `--rejects <file>` | with `--on-error skip`: write the skipped documents to this file |
| `--batch <n>` | documents per request, 1 to 255, default 100 |
| `--dry-run` | print what would be created and written, and write nothing |
| `--json` | print one JSON result object on standard output |
| `--address <host:port>` | the server to dial, default `localhost:7433` |
| `--user <name>`, `--password <text>` | the login |
| `--auth-token <token>` | resume a session with a token instead of a password |
| `--config <file>` | a YAML configuration file to read the default endpoint from |
| `--fingerprint SHA256:<hex>` | pin the server's certificate; the server prints it when it starts |
| `--insecure` | skip certificate verification, for a node you started yourself |

`hoarddb ingest --help` prints this list with the environment variable each flag reads.
