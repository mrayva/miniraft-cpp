# Compaction and InstallSnapshot: v0.1.0-alpha.16

The [learner follow-up](learners.md) adds non-voting replication and durable
voter binding. Current MRFTD008 images persist commit progress. Without an
application-generation resolver, `TDurableState` keeps snapshot bytes in
immutable sidecars; with a resolver, it can externalize nonempty application
payloads and keep only snapshot metadata there. Readers continue to accept MRFTD001 through
MRFTD007 images. The current writer uses MRFTD008.

This milestone adds prefix deletion and follower state transfer to the alpha.15
KV checkpoint foundation. TState and Linux TDurableState support compacted logs;
legacy TDiskState does not. KV implements snapshot validation/restoration. SQL
support is described in [the SQL snapshot follow-up](sql-snapshots.md); the dummy
state machine remains unsupported for compaction and installation.

Current split-format `TDiskState` stores also support snapshot publication and
InstallSnapshot. MRFTL006 metadata atomically selects a snapshot sidecar, when
present, and matching entries/index generation. After publication, the previous
entries generation is reclaimed. MRFTL003 and MRFTL004 stores remain readable;
their old entries file moves into a versioned generation at the next suffix
replacement or snapshot installation. MRFTL001 through MRFTL005 and 20-byte
metadata remain readable. MRFTL006 can also persist an optional opaque
application-generation reference in the same metadata publication. When a
generation resolver is configured, either backend stores only snapshot
metadata in the Raft sidecar for a referenced nonempty payload. The resolver
must return the immutable source whose size and digest match the reference, and
must be configured again on recovery so the payload can be resolved before
constructing Raft. Without a resolver, the sidecar continues to contain the
complete snapshot payload.
The disk backend writes the encoded snapshot sidecar in 64 KiB ranges, avoiding
an additional full-size encoded string. Recovery decodes the owned sidecar
buffer in place, avoiding a second heap allocation for the application payload.
Streaming capture and the negotiated manifest InstallSnapshot path support
versioned images up to 256 MiB, subject to the configured per-node limit and
resource budgets. Original formats, `EncodeSnapshot()`, the non-owning
`DecodeSnapshot()` view, and legacy chunk RPCs remain capped at 128 MiB. Images
above 128 MiB require manifest-transfer support.
The default per-node limit remains 128 MiB; raising it is opt-in.
Peers that do not advertise the snapshot-size capability remain limited to
64 MiB; the leader includes the minimum active-voter limit in its configuration
barrier before allowing compaction. Learners without that all-voter certificate
also keep the 64 MiB local-compaction limit. Application snapshot restore is
negotiated separately from storage installation. Custom RSMs opt in through
`SupportsApplicationSnapshotInstall()` after implementing complete validation
and restore; the default is unsupported. A proposed voter must advertise this
capability, and compaction waits until the local application and every active
voter can restore installed images.

## Operation and compatibility

The existing Linux KV option `--durable --snapshots N` now compacts after N more
applied indices. Raft::Compact captures committed applied state, validates its
application image and atomically stores the snapshot plus retained suffix. The
separate `state.snapshot.<id>` remains a checkpoint export for compatibility; the
embedded snapshot in `state.durable.<id>` is authoritative after compaction.

Stop and upgrade every voter before enabling this release in an existing cluster.
Old binaries cannot read compacted durable images or process InstallSnapshot RPCs.
The meaning of `--snapshots` changes from checkpoint-only to actual compaction.
Mixed-version operation is unsupported. Existing v1 durable files and alpha.15
checkpoints can be read without destructive migration. The first compaction or
remote installation writes the current durable format; downgrading that store is
unsupported. Legacy snapshot voter IDs must match the original binding. Stable
configuration snapshots can describe a later active set while preserving that
binding. Static `--node` edits remain unsafe as a membership-change mechanism.

Startup restores an application behind the embedded snapshot before request
processing, including after a crash between durable publication and application
replacement. A newer standalone
checkpoint can restore progress within the retained suffix; an older one never
replaces embedded state. With periodic snapshots enabled, an existing corrupt
standalone file is still rejected conservatively. Snapshot recovery never resets
the persisted current term or vote.

## Boundaries and atomic persistence

Raft startup validates a recovered snapshot before binding voters: its image,
index and term must agree with the storage boundary, its last term must be
consistent with the boundary and current term, and its active set must be
supported by configured replication targets. Legacy images must match the
original voter binding. A stable configuration image may name a later active
set while the durable store retains that original binding; this path supports
restart after a committed stable membership transition. It scans retained
terms for nonzero, nondecreasing
values no newer than the current term and verifies the recorded last term. A
nonzero boundary without an image is rejected. Custom backends must report
accurate entry metadata, validate payloads, and supply a consistent, serialized
view; these checks are not a complete storage audit or proof of snapshot contents.
The scan uses `RetainedLogTerm`; backends relying on its default implementation
may read full entry payloads at startup.

SnapshotIndex/SnapshotTerm identify the last included entry. Get only returns
entries after SnapshotIndex, while LogTerm at the boundary returns SnapshotTerm.
LastLogIndex and all RPC/application indices remain absolute. Appends, truncation,
votes, current-term commitment and strong reads use these absolute boundaries.
Truncation cannot remove a snapshot. Election no-op creation also checks signed
index exhaustion, now reachable without allocating an enormous prefix. Memory
and durable storage reject a direct append once the last index is INT64_MAX,
before mutating the log or publishing an image. Appending the entry at INT64_MAX
itself remains valid when the previous index is INT64_MAX - 1.

An installation retains the suffix only when the local entry at the included
index has the same term. Otherwise its uncommitted log is discarded. A follower
does not install an image at or below known commitment, so stale snapshots cannot
rewind application state. Retained suffix entries are applied only after a leader
establishes their commitment.

MRFTD008 publishes hard state, the persisted commit boundary, voter binding,
snapshot reference and log-generation reference in one atomic manifest. Snapshot
bytes are published to a separate immutable sidecar before the manifest can
reference them. Tail appends extend and sync the selected log generation;
suffix replacement and compaction publish a new generation before the manifest
switch. The header remains magic[8], payload length[8], CRC[4]. The payload is
CurrentTerm[8], VotedFor[4], retained count[8], CommitIndex[8], voter count[4],
voter IDs, SnapshotIndex[8], SnapshotTerm[8], sidecar length[8], SHA-256
digest[32], log generation[8], log image length[8], and log CRC[4]. Log records
are stored in the referenced generation file. The CRC covers the v7 magic plus
payload. Recovery checks `SnapshotIndex <= CommitIndex <= LastLogIndex`, bounds
the retained count, checks the referenced sidecar's encoded size, identity and
checksum, and validates nondecreasing terms beginning at SnapshotTerm. MRFTD001
through MRFTD006 remain readable. V1 through v3 recover their commit floor as
SnapshotIndex, and a successful later publication writes v7. A crash can leave
an unreferenced sidecar, log generation or temp;
recovery ignores those files and removes matching orphans after validating the
selected manifest. Malformed recovery does not rewrite the manifest. Live
publication failure is fatal.

Followers publish a newly learned commit index before acknowledging
AppendEntries. Leaders publish quorum commit advancement before application
processing. Snapshot installation commits at least the included boundary, and
storage refuses to truncate a committed entry. Versioned `TDiskState` metadata
also persists CommitIndex, but that backend does not support compaction or
snapshot installation. Older 20-byte metadata recovers without a trusted commit
boundary until an operator supplies one through the legacy voter-baseline API.

An offline active voter deliberately blocks membership-bearing compaction until
it acknowledges the committed configuration. If a reachable quorum can satisfy
both sides of a joint transition, an operator can first remove that voter
through the normal joint-consensus API. Once the stable successor commits, the
compaction barrier covers only the remaining active voters. When the retired
node returns, it can install a snapshot whose bounded history is anchored to
its persisted original voter binding; it rejoins as a learner and must be
explicitly promoted again. This does not permit compaction while the node is
still an active voter.

`TRaft::CheckCompactionMembershipReadiness()` reports active voter IDs that
still need to acknowledge the committed configuration, remote active voters
whose storage or application restore support is unknown, and whether the local
RSM can restore snapshots. The leader blocks compaction until every active
voter advertises both installation capabilities; its barrier carries an
application-restore certificate as well as the size limit. The leader clamps
each voter's advertised receive budget to 128 MiB when that peer connection
lacks manifest-transfer support. A follower accepts
that certificate only in the Raft term in which it arrived; after a term
change, the current leader must establish it again. The barrier also covers the
fixed initial voter set with configuration index zero. A ready result still
does not guarantee that storage or the application can create and validate the
snapshot. Serialize the check with Raft work, as with `CompactShared()`.

## Transfer protocol

The previously reserved message types 6/7 now carry InstallSnapshot requests and
responses. A request identifies leader/term, boundary index/term, total image size,
offset and final-chunk flag. Responses carry the boundary, next offset, success
and an explicit Installed flag. Seqno correlates acknowledgements to the active
per-peer RPC. Frames are capped at a 64 KiB body before allocation; versioned
manifest transfers support images up to 256 MiB with at most 4,096 fixed
voters. Original formats, `EncodeSnapshot()`, the non-owning
`DecodeSnapshot()` view and legacy chunk RPCs remain capped at 128 MiB. The
existing AppendEntries response padding carries the receiver's limit
in 64 KiB units. A peer without the negotiation bit is treated as a 64 MiB
receiver; peers without manifest-transfer support cannot receive images above
128 MiB.

Leaders use stop-and-wait chunks when NextIndex is at or below their compacted
boundary. Offset and RPC context survive retries within the leadership term;
delayed/duplicate responses cannot confirm another image. Followers hold one
bounded partial image, validate duplicate bytes, reject gaps and bind assembly
to leader, term, index, boundary term and total length. A restarted follower asks
for offset zero; partial state is not persisted. Transfers restart on newer terms
or newer images, and valid chunks refresh the election timer.

Partial chunks never change application progress or MatchIndex. Completion first
checks CRC, metadata, fixed voters and the application payload without changing
live application state. The follower durably installs snapshot plus suffix, then
replaces application state, then acknowledges Installed. Failure after durable
installation terminates the process; startup restores the selected image. A lost
completion ACK can be retried without applying the image again.

Only Installed completion advances leader MatchIndex. Neither partial nor final
snapshot acknowledgements count as fresh-read confirmations: subsequent
AppendEntries must establish read barriers and replicate any suffix. A compacted
follower can return its boundary in a failed AppendEntries MatchIndex field as a
probe hint. That hint advances only NextIndex; a matching AppendEntries probe is
required before counting the index as replicated or acknowledging reads.

This implements the boundary/suffix rules of [Raft section 7 and Figure 13](https://raft.github.io/raft.pdf).
Transfer is deliberately serialized and bounded; it is not disk-streaming or a
parallel multi-image protocol.

## Validation and review

Fourteen new deterministic core/storage tests cover lagging follower catch-up and
tail replication; restart mid-transfer; delayed, duplicate and impossible ACKs;
leadership changes; strong-read gating; compacted-prefix probing; corrupt images
and invalid headers; voter and application mismatch; retained but uncommitted
suffixes; memory/durable boundary behavior; index exhaustion; v2 semantic
corruption; disk-space reclamation; and first/replacement image selection at all
five publication boundaries for retained/discarded suffixes.

One new transport test checks fragmented request/response round trips and rejects
empty/oversized chunks before body allocation. The existing CLI smoke now checks
v2 publication and restoration with periodic snapshotting disabled. A new real
three-process test compacts a two-voter majority, restarts both to clear queued
old log traffic, starts an empty third voter, verifies local reads after snapshot
installation, replicates a tail, then restarts that voter and checks recovery.
It uses bounded waits, separately from deterministic virtual-time fault schedules.
The disjoint membership test also takes a newly promoted voter offline after it
acknowledges the stable configuration, compacts on the new leader, restarts the
voter after a partial snapshot chunk, and verifies installation, tail replication,
and stable voter identity after recovery.

That integration test exposed a pre-existing election tie: all nodes used the
same RNG seed and campaigned in lockstep. Node-specific nonzero deterministic
seeds now provide distinct election schedules; a simultaneous-startup regression
verifies election progress after the initial split vote. This is scheduling
diversification, not a security/entropy source.

Local validation: 207 unit tests and two CLI tests pass under ASan/UBSan across
15 CTest targets; Release core checks pass. Self-review covered boundary arithmetic,
suffix selection, persistence/application/ACK ordering, parser allocation limits,
transfer context and retries, stale snapshots, fresh-read separation, recovery
ordering and backward v1 decoding. No independent reviewer approval is claimed.
Publication-stage process crashes do not replace hardware power-cut testing.

## Remaining work

Learners, catch-up gates, replicated joint-consensus configuration entries,
leader removal and corresponding quorum/read changes were added in later
follow-ups ([live membership](configuration-entries.md) and
[leadership transfer](leadership-transfer.md)). A leader rejects compaction
when an active voter has advertised that its storage cannot install snapshots.
An offline or not-yet-heard-from peer remains an operator-verified compatibility
requirement. AppendEntries feature bits negotiate lineage support and 32-, 256-
and 1,024-transition history encodings. A lineage-capable peer without a
history bit receives predecessor-only MRFTS004/MRFTS005 metadata; a peer without
lineage receives MRFTS003 stable or MRFTS002 joint configuration metadata
without predecessor history. MRFTS002 requires the exact committed joint
configuration. MRFTS003 can match a committed stable configuration or complete
the stable successor of the receiver's exact committed joint configuration
when the sender belongs to the new set. Fresh learners use a separate
constrained bootstrap path. These formats cannot recover a stale store across
unknown membership transitions. An unsupported learner cannot receive a
snapshot and must catch up before compaction or be removed and re-added after
its storage is upgraded. Application restore capability is negotiated, but
applications still own correct, atomic image validation and restore behavior.
Upgrade the voters before enabling the capability because older binaries
cannot distinguish storage support from application restore support.
Asynchronous/COW capture, disk-streamed transfer, allocator-wide heap
accounting, cluster/store authentication, and
independent review remain open. `TRaftServer` now applies configurable aggregate
message and byte budgets to its real peer-node outbound queues (defaults: 4096
messages and 256 MiB), in addition to the existing per-peer limits. It also
applies a configurable 256 MiB aggregate inbound message-storage budget across
decoded frame buffers and AppendEntries payload-holder arrays. Tracked network
buffers and snapshot heap reservations share the same process root by default,
but the budgets do not account for arbitrary state-machine allocations or the
entire process heap.
The conservative backend still rewrites the entire snapshot and retained suffix
on each update. TRaft::CompactShared() remains synchronous. Call it through
TRaftServer::RunSerializedRaftTask() from a poller coroutine to keep network
I/O and timers running; stateful Raft callbacks still wait behind the snapshot
work, and direct calls from the poller can block it. Asynchronous capture and
incremental suffix publication remain open.

A deterministic Linux subprocess test exits from the application `LoadSnapshot`
callback immediately after a remote snapshot has been durably installed. Restart
must restore the embedded image before request processing, covering the crash
window between Raft publication and application replacement.
