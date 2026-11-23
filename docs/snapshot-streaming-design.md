# Design: streamed application snapshots

This design describes the staged work to remove the current 64 MiB
complete-image limit. The application capture-source API and optional receive
staging files are implemented. Durable application generations and
crash-recoverable activation remain open. The current
snapshot format, storage behavior and transfer limits are described in
[compaction and InstallSnapshot](compaction.md).

## Current incremental step

`IRsm` now has an optional `SnapshotSizeUpperBound()` callback. Capture checks
the application's bound against the remaining Raft image budget, including
membership metadata, before calling `SaveSnapshot()`. The KV example reports
its encoded size and the SQL example reports its database page-size bound. An
application that does not implement the callback keeps the legacy post-capture
size check. This preflight still uses complete in-memory images and does not
change the 64 MiB limit.

`IStreamingSnapshotRsm::OpenSnapshotSource(index)` now optionally returns an immutable,
range-readable source for exactly the requested applied index. Capture copies
it in 64 KiB ranges into the final `TSnapshot::Data` buffer and does not call
`SaveSnapshot()`. Its exact size is checked against the metadata-adjusted image
limit before allocation and against `SnapshotSizeUpperBound()` when the
application also supplies that bound. Existing applications retain the
`SaveSnapshot()` path. The SQL example exposes its SQLite serialization buffer
as the source and releases the in-memory backup database before Raft allocates
the final image, reducing capture's peak full-image copies. The serialized
source and final Raft image still coexist during capture; capture still
materializes one complete image and remains capped at 64 MiB.

`IStagedSnapshotRsm` lets an application validate and prepare an installation
before Raft publishes the durable snapshot, then install that same prepared
image after publication. The SQL example keeps the validated SQLite database
open across those phases, avoiding a second full-image allocation and integrity
scan during activation. This is an in-memory staging step; it does not provide
durable application generations or change crash recovery ordering.

Full in-memory InstallSnapshot assemblies reserve their declared image size
against a shared 256 MiB process-wide heap budget by default. Raft instances can share a
custom `TSnapshotInstallBudget` through `TLogLimits`. Covered-boundary probes
reserve only the metadata prefix, even when a chunk also carries payload bytes.
Both the old and new buffers count while the prefix grows. A transfer that
cannot reserve space receives a
rejection with offset zero and can retry after capacity is released.
Full-image receive buffers allocate their declared length before accepting
chunks, so chunk appends do not grow the buffer geometrically.
Rejection, term change and idle expiry release temporary reservations. A
successful installation transfers its heap lease to the retained snapshot;
local compaction and startup recovery also charge retained image data. Outgoing
pins keep that same image lease alive. Capture-time buffers before reservation,
application memory, detached value copies and transport buffers remain outside
this budget.

On Linux, set `TRaft::TLogLimits::SnapshotStagingDirectory` to stage full
incoming images in a directory on a filesystem that supports anonymous
temporary files (`O_TMPFILE`). Durable KV servers expose the option as
`--snapshot-staging-dir PATH`. Use a disk-backed directory to avoid placing the
receive buffer in the process heap or on a RAM-backed filesystem. Chunks are
written at their validated offsets, and Raft reads the received bytes through
a read-only mapping for duplicate, metadata and checksum checks. Staged files
reserve their declared sizes against a separate 256 MiB process-wide quota.
Embedders can share a custom quota through `TLogLimits::SnapshotStagingBudget`.
Completion reserves space in the heap budget before decoding. If there is
temporary heap contention, the receiver keeps the staged file and leaves the
final chunk unacknowledged. The sender can retry that chunk once capacity is
free. Duplicate chunks do not extend the receiver's idle deadline. Staged data
is discarded when Raft clears the assembly, after decoding, or on process exit.

The decoder still materializes the final application image for validation
and installation. The staging file is temporary and partial transfers restart
at offset zero after process restart. The wire format, 64 MiB limit and
ordering of durable publication, application load and acknowledgement stay
the same.

After a full transfer's metadata is verified, intermediate payload chunks
reuse that result while the sender is still the recognized leader in the
same term and the receiver's configuration and recovery eligibility still
match. Configuration refresh invalidates the result. The receiver probes the
metadata again before decoding and installing the complete snapshot.

## Why this needs a storage contract

InstallSnapshot sends bounded chunks and can stage the complete encoded image
on disk before decoding it. `IRsm::SaveSnapshot()` returns
one `std::string` and `ValidateSnapshot()` receives a complete view. The durable
backend stores snapshot bytes in an immutable sidecar and still materializes
the application payload when opening it. Raising `TSnapshot::MaxBytes` would
raise peak memory without making the path streaming.

The SQL example independently copies the complete image into a zero-padded
buffer before SQLite deserialization. A Raft-only streaming decoder would
therefore leave a second full-image allocation in that application path. The
streaming application API must let SQL validate and stage a file-backed image
while preserving the current malformed-image bounds.

The replacement needs an immutable application-image reference that Raft can
publish atomically with the snapshot boundary, commit index, membership
metadata and retained suffix. Keeping the application payload in a separate
durable object lets the Raft image stay small while preserving crash recovery.

## Proposed lifecycle

### Capture and local compaction

1. The application opens a stable snapshot source for an exact applied index.
   The index must be committed. Applications with mutable state provide a
   copy-on-write view or briefly pause writes while the source is established.
2. The source reports a bounded length and digest, then reads requested byte
   ranges. Raft validates membership metadata and the source limits.
3. The application publishes the image under an immutable generation ID and
   makes it durable before Raft changes its own state.
4. Raft atomically publishes a small manifest containing that generation ID,
   image length and digest, snapshot index/term, membership metadata, commit
   index and retained suffix. Only after that publication may log-prefix
   compaction complete.
5. The application can delete an older image after startup and any pinned
   transfers no longer reference it. An image published before a failed Raft
   publication is an orphan and can be collected safely.

### InstallSnapshot

1. The receiver validates the bounded manifest metadata before accepting a
   higher term. It creates an application staging generation with a configured
   disk quota and idle deadline.
2. Chunks are written to the staging generation at the expected offset. Exact
   retransmissions are idempotent; conflicting duplicates and gaps are
   rejected. A rolling digest covers the complete image.
3. Once complete, the application validates and durably publishes an immutable
   generation. The receiver then atomically publishes the Raft manifest and
   retained suffix that point to it.
4. The application activates the referenced generation. If the process crashes
   after the Raft publication but before activation, startup loads that
   generation before serving requests. The receiver acknowledges installation
   only after these steps succeed.
5. Incomplete staging generations expire and are removed. A lost final
   acknowledgement can retry without activating or applying the image twice.

The ordering is deliberate: a Raft manifest must never point to an image that
is not already durable. An unreferenced application generation is recoverable
garbage; a referenced but missing image is a fatal storage error.

## API and compatibility direction

Add an optional streaming application interface alongside the existing
string-based callbacks. It should expose a snapshot source for capture and a
staging writer for installation, with explicit `Publish`, `Activate` and
`Abort` operations. Existing applications continue using the current API and
64 MiB format. Streaming applications opt in only when their storage can keep
immutable generations durable across restart.

The Raft storage interface needs a manifest plus generation reference instead
of embedding application bytes. Keep membership lineage, snapshot boundary and
retained-log publication in the same atomic Raft commit. Introduce a new
versioned snapshot format and negotiate support before sending it; never infer
streaming support from the peer's ability to read the existing chunk RPC.

Keep a configurable maximum image size, per-transfer temporary-disk quota,
aggregate in-flight quota, and a cap on retained generations. A digest detects
truncation or corruption but does not authenticate the sender or prove
commitment.

## Required deterministic validation

- Crash after staging, image publication, Raft manifest publication, and
  application activation; recovery must choose the last committed generation.
- Drop, duplicate, reorder, truncate and conflict chunks; verify offsets,
  digests, quotas and idle cleanup.
- Change leadership during transfer and compact a newer image while old
  transfers remain pinned.
- Install across stable and joint membership boundaries, including restart,
  suffix retention/discard, and stale-store history checks.
- Exhaust memory and disk quotas and inject I/O failures at each publication
  stage. No failure may acknowledge an uninstalled snapshot or expose partial
  application state.

The existing complete-image path remains the compatibility baseline until the
new storage and application contracts pass these schedules on every supported
backend.
