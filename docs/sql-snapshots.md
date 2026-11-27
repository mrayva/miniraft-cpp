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

The SQL example also stores immutable application snapshots in
`sql_file.db.<id>.generations`. It publishes the SQLite image before Raft
publishes the snapshot metadata that references its local generation ID, byte
length and SHA-256 digest. Both the durable and split-file Raft backends use
that resolver, so their snapshot sidecars contain metadata rather than another
copy of the SQLite payload. A receiving replica creates its own generation;
the sender's local ID is never transferred. After publication and activation,
SQL reclaims older generation files. Startup compares the generation directory
with the reference recovered from Raft and removes unselected images. A
generation published before a failed Raft publication remains an orphan until
the next activation or restart.

## Image and recovery contract

On Linux, the application image is captured with SQLite's backup API into a
private temporary database. Its page cache is limited to about 2 MiB, and Raft
reads the completed page image through an immutable file descriptor in bounded
ranges. This avoids a full-size serialized SQLite buffer and lets Raft charge
the final image before allocating it. The temporary file is removed after the
source is opened; its descriptor remains valid until capture finishes. This
includes committed pages still present in a WAL file. Non-Linux builds retain
the in-memory serialized source. Save requires a nonzero committed Raft applied
index and no open transaction. Raft no-ops and configuration entries advance
this index without changing SQL data. If the SQLite progress marker trails the
Raft index or is absent, capture writes the exact boundary into the backup copy.
The live database is unaffected. String-based `SaveSnapshot()` and validation
retain a payload cap below 128 MiB so complete-image compatibility snapshots
leave room for the Raft header and maximum voter metadata. Range-readable
capture and source-backed installation support SQL images above 128 MiB, up to
the 256 MiB streamed-image ceiling with conservative metadata headroom. The
actual encoded metadata and configured per-node limit still apply; the default
per-node limit remains 128 MiB. Before
SQLite opens the received image, SQL checks the format signature, rollback
mode, page size, page alignment and the in-header page count when SQLite's
header counters make it authoritative. On Linux, SQL writes the image to a
0600 file in a private temporary directory, then opens it read-only with a
2 MiB page cache and memory mapping disabled. The staged file remains alive
through validation and, for a prepared install, until the durable Raft snapshot
has been published and the application database has been replaced. This avoids
SQL's additional full-size padded heap copy; Raft still materializes the
received application image unless range-backed installation is enabled.
Non-Linux builds retain the in-memory deserialize
path, including its 20 zeroed guard bytes for SQLite's malformed-image checks.
Before restore or installation, SQL checks that the image can be opened,
passes `PRAGMA quick_check(1)`, and has exactly one valid progress marker equal
to the Raft snapshot boundary. Raft installation keeps this validated SQLite
database staged until the durable snapshot is published, then installs that
same database instead of reopening and checking the image again. WAL-mode
backup bytes are normalized to SQLite's standalone rollback-mode format before
validation. The receiver also
requires the target database page size to match before Raft publishes the
snapshot durably. Invalid or incompatible images do not replace the live
database.

When `--snapshot-staging-dir PATH --snapshot-staging-namespace GROUP` are
configured, a SQL server can install a received snapshot from a range-backed
view of the staged InstallSnapshot file. The options require `--server
--durable`, Linux, an existing directory, and a filesystem that supports
`O_TMPFILE`. Use a stable namespace unique to each Raft group sharing the
directory.
Raft validates the full image without copying its SQL payload into
`TSnapshot::Data`; SQL reads bounded ranges into its private SQLite staging
file. The active Raft snapshot keeps the anonymous receive file open so later
outgoing snapshot transfers can read it. Its disk use continues until that
snapshot and any transfer pins are released, but it no longer consumes the
in-flight staging quota after full-image decoding and checksum validation.
Configure the staging filesystem with room for these retained images as well
as active transfers. Legacy RSMs, receives without a staging directory, and
older embedded durable snapshots retain the materializing path. Current durable
sidecar snapshots remain range-backed through restart, and SQL stages them
through the same streaming installer used for received snapshots. Code that reads
`TSnapshot::Data` directly must use
`DataSize()`, `ReadDataRange()`, or `MaterializeData()` for a range-backed
snapshot.

The live database's page count gives an exact size bound when the marker
already matches. If capture needs to update the backup, SQLite can allocate
another page. Capture checks the SQLite size limit before backup and again
before publishing the source, then Raft checks the complete snapshot and its
metadata.

Restore uses SQLite's backup API to copy the validated staged database into
the existing database connection. The application progress marker in memory moves
only after the backup completes. A failed backup leaves the destination
unchanged according to SQLite's backup transaction contract. Snapshot
installation first publishes the application generation, then the Raft
snapshot and retained suffix durably, activates the referenced generation,
and acknowledges installation. If the process stops between Raft publication
and activation, startup verifies and restores the selected generation before
request processing begins.

This recovery only applies when the saved application index is below the
snapshot boundary. An application at or beyond the boundary is preserved; an
application behind the compacted boundary with no matching snapshot still
fails startup. The image does not make SQL files portable across arbitrary
SQLite versions or extensions. Use the same application schema, SQLite build
and database page size across a cluster.

Serialization runs synchronously on the event loop and can pause request and
replication processing for large databases. This feature does not provide
background or copy-on-write capture, application database quotas below the
128 MiB image cap, or live voter changes. Fixed voter IDs must still match on
every node.

## Validation

Deterministic tests cover round-trip capture and restore, malformed/corrupt and
truncated images, wrong progress boundaries, preservation of existing state after
validation or backup failure, and restart recovery when durable Raft state is
ahead of the SQL database. Compaction tests cover boundaries ending in an
election no-op or a committed membership change. The regular release and
ASan/UBSan CTest suites also exercise the library and examples.

SQLite details: [database file format](https://www.sqlite.org/fileformat.html),
[serialize](https://www.sqlite.org/c3ref/serialize.html),
[deserialize, including the WAL-mode restriction](https://www.sqlite.org/c3ref/deserialize.html), and
[backup completion](https://www.sqlite.org/c3ref/backup_finish.html).
