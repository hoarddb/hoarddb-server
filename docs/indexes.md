# Indexes

There are no indexes yet. `db.<bucket>.list({filter: …})` reads every document of
the bucket, and an `index` clause in `create bucket` or `alter bucket` is refused.
To look a document up without a scan, read it by `_id_`:
`db.User["ada"].get()`.

For a lookup by some other value that must not scan, give that value its own
bucket, keyed by the value, and keep the first bucket's `_id_` in it:

```text
create bucket UserByEmail { UserId string }
db.UserByEmail.insert({_id_: "ada@example.com", UserId: "ada"})
db.UserByEmail["ada@example.com"].get()
```

Your program writes both documents. hoardDB does not keep them in step: update
both when the email changes, and delete both when the user goes.

A document, or the elements of a queue field, can also leave at a time you
choose: [Store types](store-types.md#documents-and-queue-elements-that-expire)
has `expire at`.
