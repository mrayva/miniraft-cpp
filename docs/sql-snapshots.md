# SQL application snapshots: v0.1.0-alpha.44

The [snapshot foundation](snapshots.md) and [compaction follow-up](compaction.md)
initially supported only the KV example. SQL now supplies the same application
snapshot callbacks, so a Linux SQL server can use the durable backend's
compaction and InstallSnapshot paths.

## Use

Start each SQL server with its unchanged fixed voter list and add
`--durable --snapshots N`:

```sh
./build/full/examples/sql --server --id 1 --node 127.0.0.1:8001:1 \
  --durable --snapshots 1000
```

`N` must be positive. The SQL CLI requires this option pair for periodic
compaction; snapshots remain off otherwise. Each node uses its own
`sql_state.durable.<id>` Raft store and `sql_file.db.<id>` SQLite database.
Every voter must run a binary that understands compacted durable images and
InstallSnapshot. Existing SQL and legacy Raft files remain in place, but old
binaries cannot read the compacted store. There is no automatic format
migration or mixed-version support.

## Image and recovery contract

The application image is captured with SQLite's backup API into an in-memory
database and then serialized. This includes committed pages still present in a
WAL file. Save requires a committed `LastAppliedIndex` marker and no open
transaction. The SQL payload is capped below 64 MiB to leave room for the Raft
snapshot header and maximum voter metadata. The decoder adds SQLite's recommended
read slack around malformed untrusted images. Before restore or installation,
SQL checks that the image can be opened,
passes `PRAGMA quick_check(1)`, and has exactly one valid progress marker equal
to the Raft snapshot boundary. WAL-mode backup bytes are normalized to SQLite's
standalone rollback-mode format before deserialization. The receiver also
requires the target database page size to match before Raft publishes the
snapshot durably. Invalid or incompatible images do not replace the live
database.

Restore uses SQLite's backup API to copy the validated in-memory image into the
existing database connection. The application progress marker in memory moves
only after the backup completes. A failed backup leaves the destination
unchanged according to SQLite's backup transaction contract. Snapshot
installation first publishes the Raft snapshot and retained suffix durably,
then replaces the application database, then acknowledges installation. If
the process stops between the first two steps, startup replaces any SQL
checkpoint behind the durable Raft snapshot before request processing begins.

This recovery only applies when the saved application index is below the
snapshot boundary. An application at or beyond the boundary is preserved; an
application behind the compacted boundary with no matching snapshot still
fails startup. The image does not make SQL files portable across arbitrary
SQLite versions or extensions. Use the same application schema, SQLite build
and database page size across a cluster.

Serialization runs synchronously on the event loop and can pause request and
replication processing for large databases. This feature does not provide
background or copy-on-write capture, application database quotas below the
64 MiB image cap, or live voter changes. Fixed voter IDs must still match on
every node.

## Validation

Deterministic tests cover round-trip capture and restore, malformed/corrupt
images, wrong progress boundaries, preservation of existing state after
validation or backup failure, and restart recovery when durable Raft state is
ahead of the SQL database. The regular release and ASan/UBSan CTest suites
also exercise the library and examples.

SQLite API details: [serialize](https://www.sqlite.org/c3ref/serialize.html),
[deserialize, including the WAL-mode restriction](https://www.sqlite.org/c3ref/deserialize.html), and
[backup completion](https://www.sqlite.org/c3ref/backup_finish.html).
