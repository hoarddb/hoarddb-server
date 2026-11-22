# Example: large file stores

Keep large files (video, audio, archives, images) on storage you already run,
such as a shared NFS mount, a SAN volume, or an object store's FUSE mount. Keep
one small document per file in hoardDB saying where the file is and what it
should look like. Your webservers look the document up, then read and serve the
file themselves. The bytes never pass through hoardDB.

This is a usage pattern, not a store type. It's an ordinary bucket: one document
per file, no store fields, nothing to configure.

## When to use it

- Many webservers or workers read the same files, and they can all mount the
  same storage.
- The files are large enough that you want the webserver's own `sendfile` or
  range handling, rather than the bytes travelling through a database.
- You already have backups, retention and permissions for that storage and want
  to keep them.

If you want hoardDB to own the bytes, use a `blobStore` field. You can declare
one, but writing to it is not available yet: a `blobStore` answers `count` and
nothing else ([Limitations](limitations.md#not-available-yet)). Until
it is, this pattern is how files are kept.

## Who owns what

| | hoardDB | You (the operator) |
| --- | --- | --- |
| The pointer document | Stores it | Decide its fields |
| The file | **Never opens, reads, checks or deletes it** | Write, back up, secure and remove it |
| A pointer to a missing file | Does not know | Detect and repair it |
| A file with no pointer | Does not know | Find and remove it |

hoardDB treats `path` as a string like any other field. It doesn't check that
the file exists. Back up the file storage yourself.

## Recording a file

Compute the size and checksum where the file is written:

```
$ stat -c %s /mnt/media/videos/intro.mp4
5000000
$ sha256sum /mnt/media/videos/intro.mp4 | cut -d' ' -f1
2e05c61949591b87293a6574c6d894249d0fabedffe756d24a1bd46438fdd875
```

Then declare a bucket for the pointers and insert one document per file. Use
the name your application asks for as the document's `_id_`, so a lookup is a
single `get`:

```
[h]oardDB> create database media
[h]oardDB> use media
[h]oardDB [media]> create bucket files { Path string, Size int, SHA256 string, ContentType string }
OK  created bucket media.files (4 fields)
Fields: Path string, Size int, SHA256 string, ContentType string
[h]oardDB [media]> db.files.insert({_id_: "videos/intro.mp4", Path: "/mnt/media/videos/intro.mp4", Size: 5000000, SHA256: "2e05c61949591b87293a6574c6d894249d0fabedffe756d24a1bd46438fdd875", ContentType: "video/mp4"})
OK  id=videos/intro.mp4
```

## Looking a file up

```
db.files["videos/intro.mp4"].get()
```

You can filter on any other declared field too. `filter` is an equality match
and reads every document of the bucket:

```
db.files.list({filter: {ContentType: "video/mp4"}, limit: 100})
```

To serve a request, a webserver:

1. Reads the pointer by `_id_`. `NOT_FOUND` means a 404.
2. Opens `Path` on its own mount and serves it with `ContentType`.
3. Uses `SHA256` as the `ETag`, and `Size` as the expected length.

hoardDB doesn't ship a client library yet, so a webserver talks to
hoardDB over its wire protocol.

## Removing a file

```
db.files.delete({_id_: "videos/intro.mp4"})
```

The file on `/mnt/media` is still there. Removing it is your job.

## Keeping the two in step

hoardDB can't keep the pointers and the files consistent for you. Order the
steps so that a failure halfway through leaves only a harmless leftover:

- **Adding:** write the file completely (to a temporary name, then rename it
  into place), *then* insert the pointer. A crash in between leaves a file with
  no pointer, which nobody can request.
- **Replacing:** write the new content under a **new** path, update the
  pointer, then remove the old file after a delay longer than your slowest
  download. Overwriting a file in place changes it under readers that are
  already streaming it, and it no longer matches its `SHA256`.
- **Removing:** delete the pointer first, then remove the file after the same
  delay. A crash in between leaves a file with no pointer, never a pointer to
  nothing.
- **Checking:** run a periodic job that compares the bucket against the
  directory to find both kinds of leftover. Re-running `sha256sum` and comparing
  against the stored `SHA256` finds files that changed or rotted on disk.

## Choices to make up front

- **Store paths, not URLs.** If your webservers mount the storage in different
  places, store paths relative to an agreed mount point:
  `media/videos/intro.mp4` under a per-host root moves more easily than an
  absolute path baked into every document.
- **Add any fields your application needs**, such as owner, duration, width and
  height. Declare them when you create the bucket, or later with
  `alter bucket files { add Owner string }`; documents written before read the
  new field as unset.
