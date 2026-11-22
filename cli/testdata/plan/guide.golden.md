# hoarddb plan: rules for an agent

You read the application and write a model file. `hoarddb plan` judges it. The verdict changes only when the design does: there is no model inside the tool.

## The Critical Rule

If you will look a thing up, list it or index it on its own, across its owners, it is a bucket. If it only ever exists as part of one document, it is a store field of that document.

## Patterns and their structure

- P1: look an instance up by its own key: a bucket, _id_ is the key
- P2: append to an owner's list, read oldest first or consume in order: fifoStore on the owner
- P3: append to an owner's list, read newest first: lifoStore on the owner
- P4: an owner's things looked up by a second key: hashStore on the owner
- P5: an owner's things read in key order or by range: btreeStore on the owner
- P6: an owner's things taken most important first: heapStore on the owner (no push yet)
- P7: bytes an owner keeps: blobStore on the owner, or the pointer layout (no put yet)
- P8: find instances by a field that is not the key, across owners: a bucket with the field declared and an index (a scan: indexes are not available yet)
- P9: one kind refers to another: a declared scalar field holding the other kind's _id_
- P10: a value read with its instance and never alone: a scalar field
- P11: a nested object read whole with its instance: an object field

## Query reductions

- join kind `aggregate`: join to aggregate
- join kind `nonkey`: join on a non-key column
- join kind `pk`: join to a row by its primary key
- join kind `sort`: join to sort by the far side
- join kind `visibility`: join to apply a visibility predicate
- an N+1 loop (a list, then a get per id) is a primary-key join: `expand`, a keyed batch
- child rows by parent key, ordered: an owned ordered store on the parent, one seek
- cost classes, cheapest first: seek, keyed batch, merge, scatter, scan

## Visibility and locality

Start at the principal: what may this principal see, then owned reads, then `expand`. Set `visibility` on the trace line:

- `owner`: a field, filtered after read: always fits
- `membership`: two-sided store with expand: always fits
- `container`: grants at the container, two-sided, owned ordered lists per container and a merge for the first page
- `object_scoped`: a Permissions hashStore on the object, checked per child, inheritance by walking the parent pointer: always fits
- `object_global`: Principal.Readable as a two-sided store, or a scan

For `container` and `object_global` the number that decides the fit is containers or grants per principal at p95 (put it in `fans`): up to 36 is seeks, up to 300 is a merge, above that a scan.
Small, read-everywhere kinds on the far side of primary-key joins belong in a reference database at full replication with local reads.

## The five disqualifiers

A rule fails (`F`) when the pattern is on a primary read or write, and is partial (`p`) on the admin surface only. Put `surface: admin` on secondary lines.

1. dynamic filters: a list with 3 or more optional filters composed at run time. Text search is a pass only when a `search` line delegates it to a search engine; a `substring` match with no `search` line for the kind fails.
2. client sorting: `sort_from: client` on a list. `setting` is partial.
3. non-reducible joins: a join on a non-key column, to sort by the far side, or to aggregate over a dimension chosen at run time.
4. permissions in the query: `object_global`, a `container` predicate above the merge bar, or a visibility join no rule above reduces. A join of kind `visibility` with no `visibility` word on it always counts: put the word on the join line.
5. atomic multi-entity writes: lines sharing `atomic_with` across two kinds with no `guard`.

Verdicts: `yes`, `yes with caveats`, `only with redesign of <rules>`, `not a good candidate for hoardDB`. The last needs, on one primary list, 15 or more optional filters AND a client-chosen sort AND either a `container` or `object_global` visibility predicate or a `dynamic` line. An `owner` predicate does not qualify. Say it as plainly as a yes.

## The loop

1. Run `hoarddb plan schema` and write a model file that validates against it.
2. Run `hoarddb plan model.json --json`.
3. Revise the DESIGN the model describes, or accept the fail. Do not edit trace lines, `surface`, counts or fans to move a cell: that changes the claim, not the application. A refusal carries a `fix` string you can act on.
4. Put the open choices to the human, with the cost lines of the plan beside each option.

You do not pick the structure either: the developer does.
