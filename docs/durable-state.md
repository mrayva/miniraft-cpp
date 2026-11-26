# Linux durable state: v0.1.0-alpha.3

## Commit recovery and application generations: MRFTD008

The current durable store persists `CommitIndex` alongside the current term,
vote, voter binding, snapshot and retained log. Recovery requires
`SnapshotIndex <= CommitIndex <= LastLogIndex`. Raft publishes a commit advance
before it acknowledges the corresponding AppendEntries request or exposes the
new boundary to application processing. Restart therefore replays committed
entries even when the application checkpoint trails them. Installed snapshots
advance the persisted commit floor to at least their included index, and storage
rejects truncation of committed entries.

MRFTD008 keeps the 20-byte file header: magic[8], payload length[8] and CRC[4].
Its payload is CurrentTerm[8], VotedFor[4], retained-log count[8],
CommitIndex[8], voter count[4], sorted voter IDs[4 each], SnapshotIndex[8],
SnapshotTerm[8], encoded snapshot length[8], SHA-256 digest[32], log
generation[8], log image length[8], log CRC[4], and an application-generation
marker[4]. When present, the marker is followed by the application snapshot
index[8], byte count[8], SHA-256 digest[32], ID length[4] and opaque ID bytes.
The manifest CRC covers the v8 magic and payload. Stores without an application
generation write a zero marker. The reference is local to this replica; it is
not included in the snapshot wire image. Without a generation resolver, the
complete snapshot remains in the Raft sidecar. When a resolver is supplied to
`TDurableState`, a referenced nonempty application payload is omitted from the
sidecar; the sidecar still contains the complete snapshot boundary and
membership metadata. Before publishing, the resolver's immutable source is
checked against the generation byte count and digest. Recovery verifies the
sidecar, resolves and checks the generation source, and fails closed if the
required resolver or generation is missing. Retained log records live in
`NAME.durable.ID.log.GENERATION`.
Each log image has a 20-byte header containing its magic, generation and header
CRC, followed by term/flags/length/data records; the manifest's log CRC covers
all records.

Snapshot sidecars are immutable and named
`NAME.durable.ID.snapshot.INDEX.TERM`; without a generation resolver they
contain the encoded snapshot, and with one they contain only snapshot metadata
when a nonempty application payload is generation-backed. Ordinary log, term,
vote and commit updates do not rewrite the sidecar. A changed snapshot is
written and synced to a temporary sidecar, renamed and directory-synced before
the manifest references it. Tail appends extend and fsync the selected log
generation before publishing the small manifest. Suffix replacement,
`RemoveLast`, and snapshot compaction
write a complete replacement log generation and sync its name before the
manifest switches to it. The manifest remains the atomic commit point. After it
is durable, storage removes superseded snapshot and log generations. A crash
can leave unreferenced sidecars or incomplete temp files; recovery follows only
the selected manifest and removes matching regular orphan files after
validating and syncing the selected images. If the manifest is absent but a
published sidecar remains, fresh initialization fails closed so an interrupted
migration cannot be mistaken for an empty store. Recovery verifies the length
and digest of the exact encoded snapshot sidecar before decoding it. A sidecar
from another store with the same snapshot index, term, and voters is rejected
if its bytes differ.

MRFTD007 remains readable with the bound-snapshot and external-log format.
MRFTD006 remains readable with inline log records; its next successful
publication migrates them into an external log generation. MRFTD005 remains
readable. It has no recorded digest, so the next successful publication binds
the sidecar that passed legacy recovery checks without rewriting it. MRFTD004
remains readable with its snapshot inline. MRFTD001 through MRFTD003 also
remain readable and initialize their commit floor to SnapshotIndex because
those formats did not record commit progress. All new publications write
MRFTD008, which older binaries cannot open.
Recovered unbound MRFTD002 snapshots with configuration metadata remain offline
until explicitly bound. `BindVotersFromTrustedHistory` accepts them only when
their complete retained membership chain starts from the independently trusted
original voter set; it also requires an independently supplied commit boundary.
Incomplete or truncated history remains unavailable for automatic recovery.

At alpha.3, this closed commit-boundary recovery for `TDurableState`; live
membership changes and joint consensus were added in later milestones. The
current bounded transition path is documented in
[live membership](configuration-entries.md). It accepts `TDurableState` and
versioned `TDiskState` metadata when the original voter binding and commit
boundary are durable.

TDurableState is an opt-in storage backend with synchronous durable publication.
It closes the missing file-sync and atomic-publication gap for new stores, under
the filesystem assumptions below. `TDiskState` syncs its split log/index files
before atomically replacing hard-state metadata. It supports snapshot install
and compaction through generation-specific snapshot, entries and retained-suffix
index files; metadata switches generations before the covered prefix is
reclaimed. Fresh stores use checksummed log/index records and MRFTL006 metadata
that includes CRC32, the commit boundary, original voter binding, log
generation and an optional application snapshot-generation reference. Like
`TDurableState`, `TDiskState` can be constructed with a resolver to keep a
referenced nonempty application payload out of the snapshot sidecar; recovery
requires that resolver and validates the exact size and digest before exposing
the payload. Without the resolver, snapshot bytes remain embedded in the
sidecar.
MRFTL001 through MRFTL005 and old 20-byte stores remain readable;
after binding an old store, its existing records keep the legacy encoding
selected by their metadata until a generation rewrite.
An old 20-byte metadata record has no commit boundary or voter binding; supply
both through the trusted-baseline API before Raft startup.

## Publication protocol

Each `Append`, `RemoveLast`, and `Commit` call publishes one complete manifest
containing the current term, vote, snapshot reference and selected log prefix.
A follower `AppendEntries` request that changes state is published once before the follower
acknowledges it. This combines a new entry batch, any conflicting uncommitted
suffix replacement, and a commit advance carried in that request. A commit
advance learned by a later matching request is a separate publication. Other
backends retain their existing truncation behavior:

Recovery and trusted migration validate the complete retained log. Online
append and suffix replacement validate new entries against the retained prefix;
snapshot installation keeps a suffix only when its boundary term matches, and
truncation only shortens the validated suffix. Since reads return owned clones,
publication checks the log boundary in constant time instead of rescanning all
retained entries on every append.

Before publishing the manifest, an ordinary tail append is appended and fsynced
in the selected log generation. If the retained suffix changes, storage first
writes a replacement log generation using write/fsync/rename/directory-fsync.
When the snapshot reference changes, it also publishes the snapshot sidecar
using the same sequence. It then publishes the manifest:

1. Serialize hard state, snapshot reference and log-generation reference into
   a temporary manifest in the same directory.
2. fsync the temporary file and check the result.
3. Rename the temporary file over the published image atomically.
4. fsync the containing directory and check the result.
5. Return to the caller, allowing Raft to acknowledge the update.

An older image is never overwritten in place. A crash before rename leaves the
previous published image; a process crash after rename exposes the complete new
image. An unacknowledged update may therefore appear after recovery, which is
allowed. No acknowledged operation returns before file and directory sync finish.
Startup syncs the selected image and directory again before serving requests.
If a crash/filesystem error leaves the published name missing, startup rejects
the store rather than inventing a new term or vote.

Live publication errors terminate the process, since the networking layer catches
ordinary exceptions and would otherwise keep an unsafe node running. An error
after rename has an uncertain outcome; clients must not assume it rolled back.
Startup errors are exceptions and do not fall back to older/temp images.

## Format and ownership

The manifest has version magic, payload length and CRC32. Recovery checks the
checksum before decoding metadata and validates bounds, commit metadata,
snapshot identity, sidecar contents, log-generation identity, log checksum and
term ordering. CRC32 detects accidental corruption but does not protect against
malicious modification or all collisions.

The files are NAME.durable.ID, its temporary image, NAME.durable.ID.lock, the
referenced snapshot sidecar, and the selected log generation. Snapshot
sidecars are identified by included index and term; log generations are
identified by a monotonically increasing generation number. Ordinary append
and manifest updates do not rewrite prior log records.
A nonblocking advisory flock holds the lock for the object's lifetime. Use one
object/thread to mutate a store, and do not delete/replace its lock while running.
Returned/appended entries are copied so external mutation cannot change the
selected state behind the publisher's back. Temporary files are never recovery
candidates; the next publication replaces a stale temp file while holding the lock.

An existing lock with no published image is rejected, including an interrupted
first initialization. This conservative choice can require operator recovery
even if the node had not acknowledged any updates. Do not delete an existing
store's files to force startup.

Legacy entries/index/state files with the same base name are rejected by the
runtime. The Linux `miniraft-migrate-disk-state` utility converts an offline
`TDiskState` store, including a compacted snapshot and retained suffix, to a
distinct fresh durable base when the operator supplies the complete trusted
original voter set and commit index. See
[legacy disk-state migration](legacy-disk-state-migration.md). The tool leaves
the source files intact and refuses snapshots whose membership history cannot
be anchored to the supplied voter set. Do not replace an existing node's state
with an empty store under the same identity. A corrupt main image is never
replaced by a seemingly valid temp image.

## Use

```cpp
#include <miniraft/durable.h>
auto state = std::make_shared<TDurableState>("state", nodeId);
```

On Linux, the KV example accepts --durable in place of --persist. Run it in a
dedicated directory without legacy state files. --persist and the SQL example
continue to use the legacy backend. Other platforms build the library but
TDurableState construction throws an explicit unsupported-platform exception.

## Validation and self-review

Tests cover publication ordering, process-crash boundaries across
append/truncation/vote/replacement/commit updates, immediate exits after returned
updates, interrupted initializations, a live injected failure, an actual
write error using a child-only file-size limit, corrupted/truncated images,
single-writer exclusion, missing images, legacy-store refusal, entry ownership and Raft election/application replay across
a storage restart.
There are 31 child-process scenarios; none relies on sleeps or random scheduling.

All 61 tests pass locally under ASan/UBSan, and the core suite passes in Debug
and Release builds. These process-crash tests do not simulate physical power
loss or prove a storage controller honors flush commands. Hosted CI executes the
same targets; its results are separate from local validation.

Self-review checked sync-before-rename-before-ack ordering, suffix replacement,
directory durability, lock lifetime, failure propagation, format parsing and
recovery's refusal to adopt temporary images. No independent approval is claimed.

## Limits and assumptions

Ordinary tail appends extend the selected log generation and publish a small
manifest; suffix replacement, `RemoveLast`, and compaction write a replacement
log generation. The in-memory state still retains the log, so memory use remains
proportional to the retained log. Publication blocks the calling thread during
sync. `AppendBatch` reduces publications for a follower request, including a
commit advance carried in that request. Individual client proposals and
commit-only advances still publish separately. There is no WAL or group commit
across requests; per-operation sync latency remains a throughput limit.

The durability contract assumes Linux, a pre-existing durable parent directory,
and a local filesystem/storage stack honoring fsync and same-directory atomic
rename, with exclusive application ownership of the files. Network filesystems,
arbitrary external file deletion, broken hardware/flush semantics and real power
cut testing are outside this milestone. CRC32 is not a substitute for backups.
Storage durability also does not establish end-to-end linearizability or exactly
once application behavior; those consensus/client audits remain open.
