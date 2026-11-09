# Snapshot foundation: durable KV checkpoints, v0.1.0-alpha.15

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
by default. A one-second polling coroutine captures a checkpoint after at least
N additional applied log indices, including internal no-ops. Creation, encoding
and publication execute synchronously on the serialized event loop. Large maps
and slow storage can pause processing; asynchronous/COW capture is future work.

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
limits. SQL and the dummy application do not implement snapshots in this release.

CaptureSnapshot requires a nonzero applied index no greater than the known commit
index or retained log end. Metadata records its index, term and sorted fixed voter
IDs. RestoreSnapshot requires a fresh application and an exact voter-set match;
the boundary index must exist in the local log with the same term, not newer than
CurrentTerm. Restore does not overwrite current term or votedFor. These APIs must
run serialized, with restore only during startup before request processing.

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

The complete image is capped at 64 MiB and the voter count at 4,096. Decoding
checks lengths/counts against remaining input before allocation/copying, rejects
unknown versions and trailing bytes, and validates metadata after CRC verification.
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

## Next snapshot and membership milestones

1. Introduce snapshot-boundary-aware log indexing and atomic durable publication
   of snapshot plus retained suffix. Validate votes, AppendEntries consistency,
   current-term commitment and strong reads at the compacted boundary.
2. Add bounded, resumable InstallSnapshot transfer. Test interrupted/reordered/
   duplicate chunks, leadership changes, concurrent tail replication, stale
   snapshots and follower restart. Enable prefix deletion only once lagging
   followers have a verified recovery path.
3. Add non-voting learners and catch-up gates, then replicated configuration
   entries with joint consensus. Elections, commits and strong-read confirmation
   must honor both old and new majorities during transition. Persist configuration
   state in logs/snapshots and test rollback, restart and leader removal.

These follow the snapshot metadata/state-transfer requirements in
[Raft section 7](https://raft.github.io/raft.pdf) and the overlapping-quorum
configuration protocol in section 6. Editing the static `--node` list is not a
membership-change protocol. Snapshots do not make such edits safe. This release
keeps fixed membership and leaves full compaction, transfer, dynamic membership,
background capture, SQL snapshots and independent review as open release gates.
