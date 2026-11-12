# cqlite with Squealx

Each directory is an independent runnable example. Every program runs its use case against both `:memory:` and a temporary file-backed database. Run from the repository's examples module with `go run ./cqlite/<name>`.

- `quickstart`: open, create, insert, select, and named arguments.
- `repository`: typed Squealx repository CRUD.
- `transactions`: commit and rollback.
- `master_replica`: route writes to a master and reads to a replica with `dbresolver`.
- `features`: prepared statements, JSON values, and full-text search SQL.
- `engine_native`: cqlite-specific engine options, engine statistics, explicit WAL checkpointing, and its packed `.db` file format through the embedded Go API.

The master/replica example demonstrates routing only. cqlite does not synchronize separate database files; provision and keep replicas current with your replication or backup process before routing reads to them.

## cqlite-specific engine APIs

cqlite targets SQLite-compatible SQL, so SQL features such as JSON and FTS are also available in SQLite. `engine_native` focuses on cqlite's engine-specific Go surface instead: `Options` controls its buffer pool, sync policy, and automatic checkpoint thresholds; `Stats` and `Checkpoint` are direct engine methods; and `.db` files use cqlite's packed storage format rather than SQLite's file format. These methods and file layout do not carry over to SQLite drivers.

This example runs once with a private in-memory database and once with a persistent file. The packed file and durability controls apply to the file-backed run; the in-memory run demonstrates the same engine API without a persistent artifact.
