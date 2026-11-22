# Data types

A field of a bucket is either a **store** (a queue, a stack, a map, a file store; see [Store types](store-types.md)) or a **scalar**: one value per document, such as a name, a count or a date. This page is the scalar types. You declare one when you create the bucket:

```text
[h]oardDB [app]> create bucket Event { Title string, Seats int, Price decimal, Starts datetime, Ref uuid, Tags array, Venue object, Open bool }
OK  created bucket app.Event (8 fields)
Fields: Title string, Seats int, Price decimal, Starts datetime, Ref uuid, Tags array, Venue object, Open bool
```

Documents are stored as BSON, so the types are BSON's. `describe bucket Event` lists each field with its type.

## The types

| Type | Meant to hold | Write it as |
|------|-------|-------------|
| `string` | UTF-8 text | `"Launch"` |
| `int32` | a whole number that fits in 32 bits | `5`, or `{$numberInt: "5"}` |
| `int` | a whole number that fits in 64 bits | `120`, or `{$numberLong: "9007199254740993"}` |
| `double` | a 64-bit floating point number | `1.5`, or `{$numberDouble: "2.5"}` |
| `decimal` | an exact decimal, for money | `{$numberDecimal: "19.99"}` |
| `number` | any of the four numeric types above | any of the forms above |
| `bool` | true or false | `true` |
| `datetime` | a moment in time, to the millisecond | `{$date: "2026-12-01T18:30:00Z"}`, or milliseconds since 1970: `{$date: 1730628000000}` |
| `bsontimestamp` | BSON's internal timestamp: a count of seconds and an ordinal inside that second | `{$timestamp: {t: 1, i: 1}}` |
| `binary` | raw bytes | `{$binary: {base64: "aGk=", subType: "00"}}` |
| `uuid` | a 16-byte universally unique identifier | `{$uuid: "0192a3b4-c5d6-7890-abcd-ef1234567890"}` |
| `objectid` | a 12-byte object id | `{$oid: "507f1f77bcf86cd799439011"}` |
| `object` | a nested document | `{City: "Tirana", Room: 2}` |
| `array` | a list of values of any type | `["talk", "free"]` |
| `regex` | a regular expression | `{$regularExpression: {pattern: "a", options: "i"}}` |
| `null` | no value | `null` |
| `any` | any of the types above | any of the forms above |

The forms in braces with a `$` are Extended JSON: they are how a value that JSON has no word for is written. A date, a uuid, a decimal, an object id, bytes and a regular expression have no plain literal, so write them in these forms. A plain `"2026-12-01T18:30:00Z"` is a string, not a date.

A document reads back as you wrote it:

```text
[h]oardDB [app]> db.Event.insert({_id_: "e1", Title: "Launch", Seats: 120, Price: {$numberDecimal: "19.99"}, Starts: {$date: "2026-12-01T18:30:00Z"}, Ref: {$uuid: "0192a3b4-c5d6-7890-abcd-ef1234567890"}, Tags: ["talk", "free"], Venue: {City: "Tirana", Room: 2}, Open: true})
OK  id=e1
[h]oardDB [app]> db.Event["e1"].get()
{"Open": true, "Price": {"$numberDecimal":"19.99"}, "Ref": {"$uuid":"0192a3b4-c5d6-7890-abcd-ef1234567890"}, "Seats": 120, "Starts": {"$date":"2026-12-01T18:30:00Z"}, "Tags": ["talk","free"], "Title": "Launch", "Venue": {"City":"Tirana","Room":2}, "_id_": "e1"}
```

Keys print in sorted order. A whole number above 9223372036854775807 is stored as a `double`, so it loses precision; write it as a string, or as a `decimal`, if you need every digit.

## The type is recorded, not checked

The ring records a field's type and prints it, but it does **not** check a written value against it. A field declared `int` will hold a string if you write one:

```text
[h]oardDB [app]> db.Event.insert({_id_: "e2", Seats: "many"})
OK  id=e2
[h]oardDB [app]> db.Event["e2"].get()
{"Seats": "many", "_id_": "e2"}
```

Check a value in your own code before you write it, and do not rely on the declared type to keep a wrong one out. What the server does refuse is a field name the bucket does not declare, with the list of the names it does:

```text
[h]oardDB [app]> db.Event.insert({_id_: "e3", Capacity: 10})
CLI error: bucket app.Event declares no field "Capacity". Its scalar fields are: Title string, Seats int, Price decimal, Starts datetime, Ref uuid, Tags array, Venue object, Open bool. Check the spelling; the document id is the id argument, not a field
```

## Spelling

A type word is spelled exactly as in the table, in lower case. Other spellings are refused, and the refusal names the word to use:

```text
[h]oardDB [app]> create bucket Z { A integer }
CLI error: PARSE_ERROR: "integer" is not a type word here: the catalog records one spelling, and it is "int" ('A int') (character 21). ...
```

`integer` and `int64` are `int`, `float` is `double`, `boolean` is `bool`, and `date` and `timestamp` are `datetime`. A word that is not a type at all is refused with the full list. A field is declared once: naming it twice is refused.

## Reading the documents a list points at

A hash store or a B+Tree store often holds ids of another bucket: a room's `Members` are the ids of users. Reading them one by one is the loop every application writes. `expand` makes the node do it, in one call and one round trip.

Declare the reference once and `expand` needs no argument:

```text
[h]oardDB [app]> create bucket Room { Name string, Members hashStore of User }
[h]oardDB [app]> create bucket Message { Author string of User, Text string }
```

Then:

```text
[h]oardDB [app]> db.Room["general"].Members.list({expand: true})
[h]oardDB [app]> db.Message.list({expand: {Author: true}, limit: 20})
[h]oardDB [app]> db.Message["m1"].get({expand: {Author: true}})
```

Each row prints, and under it the far document, or `(not found)` when that document is gone. A reference you did not declare takes the bucket's name: `expand: {Author: "User"}` or `expand: "User"` on a store (and `"ref.User"` for another database). `{bucket: "User", fields: ["Name"]}` returns only the fields you name, which keeps wide profiles out of the page.

- **Cost.** One read per distinct id on the page, grouped by the node that holds each one, never a scan. A page of 100 messages by 7 authors reads 7 documents. A call expands at most 8 fields and one level: the far document's own references come back as plain ids.
- **Ids are text.** A field declared `of` holds the target's `_id_`, which is a string in this build, so only `string` fields and hash or B+Tree stores can be declared `of`. A write that sends a number to such a field is refused. Whether the document exists is not checked at write time: a dangling id reads back as `(not found)`.
- **`where`.** `where: {Name: "Ada"}` keeps only the rows whose far document matches, from the page already read. It is not an index: the page may come back short with `more` still true.
- **`local: true`.** Lets a copy of the far document on the node you are connected to answer, when that copy is ready. The reply says `served: local` or `served: owner`. A copy can lag the owner by the replication delay.
- **Down nodes.** If a node that holds one of the documents cannot be reached, the whole call fails and says which. A page with some rows silently unexpanded would look complete.

## Changing a type

A field's type never changes. `alter bucket` adds a field or drops one (`alter bucket Event { add Email string }`). To change a type, add a field of the type you want, copy the values across, and drop the old field. [Limitations](limitations.md#not-available-yet) has the rest of what is not available yet.

## Not written the way you might expect

- There is no `now()` and no `ISODate(...)`: a statement that uses either is refused as "not a value". Write the date as `{$date: ...}`.
- `_id_` is not a declared field. It is the document's id and is always there; see [Getting started](getting-started.md).
- A store is not a scalar. `Photos lifoStore` declares a stack that belongs to each document, and it holds elements, not a value: see [Store types](store-types.md).
