# Snapshot foundation: durable KV checkpoints, v0.1.0-alpha.15

The [alpha.16 follow-up](compaction.md) adds compaction and InstallSnapshot and
changes the behavior of the same `--snapshots` option. This document describes
the original checkpoint-only baseline.

This milestone adds local application checkpoint creation and restart recovery.
It retains the entire Raft log. InstallSnapshot RPCs and prefix compaction remain
unimplemented, so checkpoints improve application recovery but do not bound log
storage or bootstrap a node whose local log is missing.

## Use

On Linux, start a KV server with its existing fixed node list and add:

```sh
./build/full/examples/kv --server --id 1 --node 127.0.0.1:8001:1 \
  --durable --snapshots 1000
```

Use the complete, identical voter-ID configuration on each node as before.
`--snapshots N` requires a positive interval and `--server --durable`; it is off
by default. A one-second polling coroutine schedules a checkpoint after at
least N additional applied log indices, including internal no-ops, on the
serialized Raft worker. Network I/O and poller timers continue while the worker
captures and publishes; other stateful Raft work waits behind that callback.
Core compaction remains synchronous, so direct calls from a poller still block
it. Splitting capture and publication into asynchronous phases remains future
work.

The server writes `state.snapshot.<id>` beside `state.durable.<id>`, with separate
`.lock` and `.tmp` paths. Startup restores an existing checkpoint before creating
the request processor or accepting traffic. The recovered application marker
establishes the existing commit floor, and later commitment replays only the tail.
Without a checkpoint, the application rebuilds from the retained log. Disabling
the option also rebuilds from that log, leaving checkpoint files unused.

## Contracts and storage

IRsm gains optional SaveSnapshot/LoadSnapshot callbacks; the defaults throw an
unsupported-operation error. KV implements canonical, sorted, binary-safe state
encoding. Restore validates the entire image into a temporary map before swapping
state and progress. Key/value lengths retain the KV wire format's 65,535-byte
limits. SQL snapshot support is described in [its follow-up](sql-snapshots.md);
the dummy application still does not implement snapshots.

CaptureSnapshot requires a nonzero applied index no greater than the known commit
index or retained log end. Metadata records its index, term and sorted fixed voter
IDs. RestoreSnapshot requires an application behind the snapshot and an exact voter-set match;
the boundary index must exist in the local log with the same term, not newer than
CurrentTerm. Startup restore can recover a process stopped after durable Raft
snapshot publication but before application replacement. Restore does not
overwrite current term or votedFor. These APIs must run serialized, with restore
only during startup before request processing.

The versioned image contains little-endian fields in this order:

| Field | Bytes |
| --- | ---: |
| Magic/version `MRFTS001` | 8 |
| Last included index and term | 8 each |
| Voter count | 4 |
| Application byte length | 8 |
| Sorted unique positive voter IDs | 4 each |
| Application bytes | declared length |
| CRC32 covering every preceding byte | 4 |

Original formats MRFTS001 through MRFTS011, `EncodeSnapshot()`, and the
non-owning `DecodeSnapshot()` view are capped at 128 MiB; the voter count is
capped at 4,096. Versioned formats MRFTS012 through
MRFTS022 preserve the matching original metadata layouts and CRC, and support
larger images through bounded storage and manifest-transfer paths up to the
256 MiB hard limit. The default per-node limit remains 128 MiB; configured
limits and receive budgets may be lower.
Decoding checks lengths/counts against remaining input before allocation/copying,
rejects unknown versions and trailing bytes, and validates metadata after CRC
verification. AppendEntries capability acknowledgements advertise each peer's
image limit; older peers without that bit are treated as accepting at most 64
MiB. Images above 128 MiB also require manifest-transfer support. The leader's
current-term all-voter compaction certificate carries the minimum limit, so an
image above 64 MiB is compacted only when every active voter accepts it.
Learners without that certificate retain the 64 MiB local-compaction limit.
Older binaries cannot reopen a snapshot larger than 64 MiB, so do not downgrade
while any node's authoritative snapshot exceeds that legacy limit.
The KV payload has its own `MRKV0001` version, count and length-prefixed entries.

TSnapshotStore takes an exclusive advisory writer lock and publishes through temp
creation, complete write, file fsync, atomic rename and directory fsync. Startup
stabilizes the selected image/name with fsync. Temp files are never recovery
candidates. A missing published image is safe because the complete log remains;
an existing corrupt or mismatched image fails startup without speculative repair.
The directory and local writers are trusted; CRC32 detects damage, not forgery or
wrong-store copies with otherwise matching boundary metadata and voter IDs.

Snapshot-save failures are catchable and logged/retried by the example. No Raft
entry is discarded and the selected checkpoint is a complete committed image,
so consensus can continue using its retained log. A failure around rename can
leave either complete checkpoint selected after a crash. Never delete Raft term,
vote or log files to work around a checkpoint recovery error.

## Validation and review

Eleven new unit tests cover codec round trips, every-byte corruption/truncation,
semantic corruption with a valid checksum, bounded file reads, temp symlink and
concurrent-writer rejection, canonical binary KV encoding, atomic malformed KV
restore rejection, capture before commitment, log/configuration mismatch,
publication errors, crashes at all five publication stages on first/replacement
save, and durable Raft/KV restart with tail-only application. Crash tests use
explicit stage hooks and subprocess exit, with no timing-dependent schedules.

A separate real-process CLI smoke test covers option validation, publication,
restart and strong read, changed voter IDs, corrupt checkpoint rejection and
unchanged Raft storage. It uses bounded waits/sockets and is distinct from the
deterministic fault tests. Linux full CTest runs it when Python 3 is available.
All 192 unit tests and the CLI test pass locally across 13 CTest targets under
ASan/UBSan; the Release core suite passes too.

Self-review checked serialized capture boundaries, metadata and allocation limits,
checksum coverage, version rejection, atomic map replacement, restore before
commit-floor initialization, fsync/rename order, publication failure semantics,
and retention of all logs. No independent reviewer approval is claimed. Process
crash injection does not substitute for hardware power-cut/storage fault testing.

## Current snapshot and membership status

The original milestones for snapshot-boundary-aware log indexing and durable
publication, bounded chunked `InstallSnapshot`, non-voting learners, and
replicated joint-consensus membership are implemented. The current support
matrix and operational constraints are in [capability matrix](capability-matrix.md).

The remaining work includes disk-streamed or logical-object application
snapshots and allocator-wide accounting across application state and arbitrary
callback allocations. Tracked Raft snapshot buffers and `TRaftServer` inbound
and outbound buffers use separate categories under one shared process root by
default. Application-owned state and untracked allocations remain outside that
cap.
Range-source capture retains the immutable source and conservatively charges
its payload size; the incremental writer charges each append before growing its
in-memory image. Retained Raft snapshot images use the snapshot-heap category.
`TRaftServer` inbound and outbound buffers use separate categories under the
same process root by default, so tracked usage shares the total cap. Legacy
`SaveSnapshot()` callback allocations beyond declared bounds, application state
and other untracked allocations remain outside it. Partial incoming transfers are not persisted,
so a follower restart begins again at offset zero. Cluster-wide endpoint
discovery and rollout, plus automatic stale store recovery without an accepted
membership-history anchor, also remain open. See the [streaming
design](snapshot-streaming-design.md) for the proposed storage contract.
Snapshot capture can still block the event loop, and independent review remains
an open release gate.

These features follow the snapshot metadata/state-transfer requirements in
[Raft section 7](https://raft.github.io/raft.pdf) and the overlapping-quorum
configuration protocol in section 6. Editing the static `--node` list is not a
membership-change protocol; use the replicated joint-consensus API instead.
