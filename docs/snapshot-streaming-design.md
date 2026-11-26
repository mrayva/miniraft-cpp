# Design: streamed application snapshots

This design describes the staged work to remove the current 128 MiB
complete-image limit. The application capture-source API, optional receive
staging files, an opt-in durable application-generation reference, and a
negotiated range-transfer wire path are implemented. `TDurableState` and
`TDiskState` can use a generation resolver to keep the application payload out
of their snapshot sidecars while preserving the payload as a range source after
restart. Each backend atomically stores the reference with its snapshot
boundary and retained suffix. Recovery requires the resolver for a nonempty
external payload and verifies its size and digest before exposing the snapshot.
The negotiated range path still assembles the complete encoded image at the
receiver, so it does not remove the image limit. The current snapshot format,
storage behavior and transfer limits are described in
[compaction and InstallSnapshot](compaction.md).

## Current incremental step

`IRsm` now has an optional `SnapshotSizeUpperBound()` callback. Capture checks
the application's bound against the remaining Raft image budget, including
membership metadata, before calling `SaveSnapshot()`. The KV example reports
its encoded size and the SQL example reports its database page-size bound. An
application that does not implement the callback keeps the legacy post-capture
size check. This preflight still uses complete in-memory images and does not
change the 128 MiB limit.

`IStreamingSnapshotRsm::OpenSnapshotSource(index)` now optionally returns an immutable,
range-readable source for exactly the requested applied index. Capture retains
it directly as the range-backed payload in `TSnapshot` and does not call
`SaveSnapshot()`. Its exact size is checked against the metadata-adjusted image
limit and `SnapshotSizeUpperBound()` when the application also supplies that
bound. Capture then verifies that the complete source is readable using a
fixed 64 KiB scratch buffer. The source must remain immutable and readable
until the snapshot and all transfer pins are released. Raft conservatively
charges its size against the retained snapshot heap budget by default. A source
that implements `ISnapshotSourceHeapUsage` can report a tighter upper bound,
including zero for a file-backed payload. The same charge is restored when a
source-backed snapshot is recovered; unmarked sources remain conservatively
charged by size.
Existing applications retain the `SaveSnapshot()` path. On Linux, the SQL
example backs up into a private temporary database with a bounded SQLite page
cache and serves ranges from a read-only file descriptor. This avoids a
full-size `sqlite3_serialize()` allocation and a second complete payload
allocation during capture. Without a generation resolver, both disk backends
store the complete snapshot payload in their snapshot generation. With a
resolver, either backend can publish only snapshot metadata there for a
generation-backed nonempty payload. Images remain capped at 128 MiB. Non-Linux
builds retain the in-memory serialized source.

`IStreamingSnapshotWriterRsm` is an alternative for applications that generate
snapshot bytes incrementally. Raft supplies a sink that checks the
metadata-adjusted image limit and extends the shared heap lease before each
append. This path does not require a size bound and avoids constructing a
separate `SaveSnapshot()` result, but still materializes one complete final
image in memory. The KV example writes its sorted records directly to this
sink, avoiding a second complete encoded buffer. Legacy `SaveSnapshot()`
callbacks without a bound reserve the maximum accepted payload before they run,
then shrink the lease to retained capacity. This conservative reservation can
reject capture when less than that amount is available; a declared size bound
allows a smaller reservation, while the writer interface charges incrementally.

`TSnapshotTransferManifest` represents snapshot metadata, application payload
length and the existing complete-image checksum separately. Its decoder
validates the checksum over bounded payload reads and retains the supplied
immutable source without joining metadata and application bytes into a
complete encoded image. Peers that negotiate `SnapshotTransferManifest` use
`INSTALL_SNAPSHOT_RANGE_REQUEST` for distinct metadata, payload and checksum
phases; peers without the feature retain the existing chunk RPC. The receiver
maps each validated range into the existing encoded-image installer, preserving
its membership checks, retry offsets, staging quotas and durable publication.
It still stages the complete encoded image and enforces the 128 MiB limit.

Applications can opt into `IApplicationSnapshotGenerationRsm`. The application
stages an immutable generation bound to the exact snapshot index, byte count and
SHA-256 digest, durably publishes it before Raft updates storage, and activates
it only after Raft's storage publication succeeds. On startup, request-processor
construction validates the referenced generation and restores it before the
node accepts requests. A generation published before a failed Raft publication
is an orphan; applications own its cleanup. InstallSnapshot creates a local
generation on the receiver rather than transferring the sender's local ID.
Applications that do not implement this optional interface keep the existing
snapshot path.

The durable backends persist this reference in MRFTD008 and MRFTL006 metadata;
MRFTD001 through MRFTD007 and MRFTL001 through MRFTL005 remain readable. Either
backend can be constructed with a generation resolver to write metadata-only
snapshot sidecars for nonempty referenced payloads. The resolver is checked
against the reference before publication and supplies the range source during
recovery, before Raft construction. Without a resolver, the existing
full-payload sidecar behavior remains. Every peer still receives the complete
image in the existing InstallSnapshot format. This reduces local durable
storage when applications opt in, but does not remove the 128 MiB ceiling or
introduce wire negotiation. Deterministic tests cover generation publication
and activation crashes for durable snapshot installation; split-backend crash
tests cover recovery before and after metadata selection.

`IStagedSnapshotRsm` lets an application validate and prepare an installation
before Raft publishes the durable snapshot, then install that same prepared
image after publication. On Linux, the SQL example writes the already
materialized application image to a private temporary file and keeps a
read-only SQLite connection open across those phases. It bounds SQLite's page
cache, disables memory mapping, and avoids a second full-size padded heap copy
and integrity scan during activation. Non-Linux builds retain in-memory
deserialization. This staging file is temporary; it does not provide durable
application generations or change crash recovery ordering.

Full in-memory InstallSnapshot assemblies reserve their declared image size
against the shared 256 MiB snapshot-heap category under the default process
memory root. Raft instances can share a custom `TSnapshotInstallBudget` through
`TLogLimits`. Covered-boundary probes
reserve only the metadata prefix, even when a chunk also carries payload bytes.
Both the old and new buffers count while the prefix grows. A transfer that
cannot reserve space receives a
rejection with offset zero and can retry after capacity is released.
Full-image receive buffers allocate their declared length before accepting
chunks, so chunk appends do not grow the buffer geometrically.
Rejection, term change and idle expiry release temporary reservations. A
successful installation transfers its heap lease to the retained snapshot;
local compaction and startup recovery also charge retained image data. Outgoing
pins keep that same image lease alive. Range-source capture reserves the
reported heap bound, or its exact size for an unmarked source, and retains the
source after checking it in bounded ranges. Internal file-backed snapshot
sources report zero heap payload bytes. Fallback installation paths that
materialize a source-backed image reserve an additional image-sized temporary
lease during validation and load. Application state and temporary allocations
made by validation/load callbacks remain outside this budget. For legacy
`SaveSnapshot()` capture, Raft reserves the declared upper bound before
invoking the callback, or the maximum accepted payload when no bound is
available, then shrinks the lease to retained string capacity. Incremental
snapshot writers extend the lease before each append even without a size bound.
Other application-owned state, detached value copies, callback allocations
beyond declared bounds and untracked allocations remain outside the snapshot
budget. `TRaftServer` accounts for inbound and outbound buffers in separate
categories under the same default process root as the snapshot heap budget, so
tracked usage competes under the shared total cap. Custom transports can bypass
those server accounting paths.

On Linux, set `TRaft::TLogLimits::SnapshotStagingDirectory` to stage full
incoming images in a directory on a filesystem that supports anonymous
temporary files (`O_TMPFILE`). Durable KV servers expose the option as
`--snapshot-staging-dir PATH`. Use a disk-backed directory to avoid placing the
receive buffer in the process heap or on a RAM-backed filesystem. Chunks are
written at their validated offsets, and Raft reads the received bytes through
a read-only mapping for duplicate, metadata and checksum checks. Staged files
reserve their declared sizes against a separate 256 MiB process-wide quota.
Embedders can share a custom quota through `TLogLimits::SnapshotStagingBudget`.
The staging quota lease covers an in-flight assembly. When a streaming install
RSM lets Raft retain the staged file as the snapshot's range source, the lease
moves to that source and remains charged until the snapshot is replaced and all
transfer pins release it. Materializing installs release the disk lease after
decode and checksum verification, and reserve the decoded image in the heap
budget. Duplicate chunks do not extend the receiver's idle deadline. If heap
capacity is temporarily unavailable, the receiver keeps the staged file and
leaves the final chunk unacknowledged for retry.

The decoder validates the full wire checksum and metadata through the mapping,
then retains a source for the application bytes. The SQL example validates and
stages directly from this source. The retained Raft snapshot uses it for
durable sidecar serialization and outgoing InstallSnapshot ranges, avoiding
the full payload's heap copy. Linux durable recovery also retains external
`TDurableState` sidecars and `TDiskState` snapshot generations as read-only
mapped range sources, so the active snapshot stays range-backed after restart.
The standalone `TSnapshotStore::Load()` path also returns a mapped source.
The SQL RSM uses its streaming installer for both received and recovered
snapshots. Legacy RSMs, receives without staging, older embedded durable images,
and non-Linux `TDiskState` recovery may still materialize the payload. The
active snapshot pins its anonymous receive file or durable snapshot mapping
until replaced and all transfer pins are released. Retained anonymous receive
files remain charged to the shared staging quota; durable storage's own snapshot
files are governed by its storage and filesystem capacity. Partial transfers
restart at offset zero after process restart. The wire format, 128 MiB limit and
publication, application-installation and acknowledgement ordering stay the same.

After a full transfer's metadata is verified, intermediate payload chunks
reuse that result while the sender is still the recognized leader in the
same term and the receiver's configuration and recovery eligibility still
match. Configuration refresh invalidates the result. The receiver probes the
metadata again before decoding and installing the complete snapshot.

## Why this needs a storage contract

InstallSnapshot sends bounded chunks and can stage the complete encoded image
on disk before decoding it. `IRsm::SaveSnapshot()` returns
one `std::string` and `ValidateSnapshot()` receives a complete view. The durable
backend stores current snapshot bytes in an immutable sidecar; Linux recovery
validates the mapped image and retains a range source for its application
payload. The versioned `TDiskState` backend does the same for snapshot-generation
files on Linux. Older embedded durable images and non-Linux generation recovery
still use the materializing decoder. Raising `TSnapshot::MaxBytes` would raise
peak memory without making the fallback path streaming.

The SQL adapter can validate and stage a range-backed application image from
the received mapping on Linux; non-Linux builds retain the padded deserialize
buffer. This avoids the Raft heap copy but still keeps the complete received
wire image on disk while it is the active in-memory snapshot. Current durable
sidecars and Linux snapshot generations remain mapped and range-backed after
restart; older embedded formats and RSMs without the streaming-install contract
can still materialize payloads. Generation-reference publication and sidecar
externalization are implemented for both durable backends. Their recovery
requires a configured generation resolver when the selected sidecar is
metadata-only. Payload bytes still traverse the same bounded Raft stream; the
new negotiated protocol separates them from the manifest on the wire. On Linux,
when `SnapshotStagingDirectory` is configured, the receiver now keeps manifest
metadata in a bounded heap buffer and writes payload ranges directly to a
separate anonymous file. It validates and decodes the manifest against that
payload source without rebuilding the encoded image. Legacy snapshot RPCs and
negotiated transfers without a staging directory retain the complete-image
compatibility path.

The generation reference is published atomically with the snapshot boundary,
commit index, membership metadata and retained suffix. Both durable backends
support source-backed local storage and require the startup resolver when the
selected sidecar omits a nonempty payload. The negotiated wire format now
transfers metadata, payload and checksum ranges separately. The remaining gap
is direct receiver staging of those parts as separate objects. Current
installation reconstructs the complete encoded image in heap or disk staging
and remains capped at 128 MiB. Removing that cap needs a versioned storage and
application-install contract that can publish the validated manifest and
payload source atomically without materializing their combined image.

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

The optional capture-source, streaming-writer, staged-install and durable
generation interfaces now coexist with the existing string callbacks.
Generation-aware applications publish immutable images before Raft publishes
the corresponding reference, and activate them after that publication.
Existing applications keep the current API; peers without size negotiation
remain limited to 64 MiB. The negotiated range RPC uses separate receiver
staging only when a Linux staging directory is configured; other receivers
keep the complete-image compatibility path. The remaining storage work is to
extend separate-object staging to the default in-memory receiver and define a
versioned durable payload-generation format whose size limit is independent of
the current 128 MiB encoded-image cap. Never infer streaming support from the
peer's ability to read the existing chunk RPC.

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

The existing complete-image receiver remains the compatibility baseline and
fallback. Deterministic tests cover negotiated phase selection and range
assembly in addition to pre-manifest orphan generations and post-manifest
recovery before activation. The broader transfer, membership and quota
schedules remain required before separate-object receiver publication replaces
the complete-image path on every supported backend.
