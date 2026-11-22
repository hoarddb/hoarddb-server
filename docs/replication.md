# Replication

A database names how many copies of its data the cluster keeps, when
you create it:

```text
create database app { replication_factor: 2 }
```

Leave the option out and the database has one copy (the node's default). There is
nothing to set per bucket: every bucket of a database follows the database's
factor, and a factor cannot be changed afterwards (`alter database` is not
available). A factor the cluster cannot hold is refused before anything is written:
a factor above 1 needs at least 3 voters (witnesses count) and as many data
nodes as copies, and the refusal names which is missing. `show databases` and `describe database app`
print each database's factor.

A server given `HOARDDB_REPLICATION_FACTOR` above 1, or `HOARDDB_SEED_NODES`, without
`HOARDDB_CLUSTER_ENABLED=true` refuses to start. Copies are set on the database as above,
and [Configuration](configuration.md#replication-factor) names the settings that replace it.

## What works today

- One node, one copy. This is the normal way to start.
- Forming a cluster. Start the members on fresh data directories, `join` each
  one, and only then `create database`. `promote <node>` gives a vote to a node
  that joined without one, and `nodes` and `status` show the members, the leader
  and the epoch. [Configuration](configuration.md#cluster-mode) has the steps.

## What does not work yet

A cluster cannot grow or shrink while it holds data, and a database with more
than one copy is not yet a promise that every copy is written and readable.

- `join` is refused once any database exists, so a node cannot be added to a
  cluster that holds data.
- `leave` and `remove-node` parse and are refused, so a member cannot be taken
  out. `show replication` and `show placement` are refused too.
- `replace-node <node> --with <node>@<addr>` brings back a member that lost its
  data, under its own id and at its own address, while the cluster holds data
  ([Add a node to a cluster](cluster-add-a-node.md#if-a-member-lost-its-data)).
- `status seeding`, `seeding retry` and `seeding forget` run on a node that
  belongs to a cluster.

[Limitations](limitations.md#cluster) lists what is missing in full.
