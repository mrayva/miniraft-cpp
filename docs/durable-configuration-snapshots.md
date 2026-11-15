# Durable configuration snapshot metadata: v0.1.0-alpha.45

Bound `TState` and `TDurableState` instances can install and recover MRFTS002
joint and MRFTS003 stable snapshots so offline tools can validate configuration
history after restart. A store must already have a voter binding. For a
configuration-bearing snapshot, `snapshot.Voters` describes the active set at
the snapshot boundary and may differ from that original binding. Durable
recovery preserves the original binding and exposes the active snapshot set
separately. A legacy snapshot without configuration metadata must still match
the binding. An unbound store cannot infer its original voter set from
configuration metadata. The [alpha.50 follow-up](trusted-legacy-voter-binding.md)
adds an explicit durable migration API that accepts a caller-supplied trusted
original set, validates it against the recovered vote, snapshot, and retained
log, then persists the binding before Raft startup. The API does not establish
that the caller's assertion is true; absent an independent trusted baseline, the
store still cannot infer its voter set.

The current MRFTD004 durable format persists the snapshot, commit boundary and
original voter binding in the same atomic image; it does not reinterpret that
binding from snapshot metadata. Older durable formats remain readable and use
their snapshot boundary as the conservative commit floor. The history scanner
checks configuration-bearing snapshot metadata against the active baseline;
the original binding may refer to an earlier stable set.

`ScanCommittedConfigurationHistory` uses the recovered commit index directly
and requires both durable commit-index support and a persisted original voter
binding. For configuration-bearing snapshots, the caller supplies the active
history baseline through the snapshot boundary; the scanner verifies it against
the snapshot metadata. Legacy snapshots still require the baseline voters to
match the original binding. This scanner is an offline validation API; snapshot
metadata is not proof of commitment or cluster identity. The separate live log
transition API uses the durable commit boundary and original binding. Since
alpha.71, the live runtime also recovers and installs MRFTS002 when the receiver
has committed the exact same joint voter sets and configuration identity.

In alpha.45 these images were persistence for offline validation only. The
alpha.46 follow-up enables runtime recovery and transfer of MRFTS003 stable
snapshots for the configured active voter set. Compaction carries forward the
stable configuration identity. The stable membership compaction follow-up
extends local compaction and restart recovery after a finalized voter change,
while preserving the original binding. Network installation requires the
receiver's committed configuration and both joint voter sets to match before
accepting a higher term. Configuration log entries are accepted through the
guarded joint-consensus path.
Since alpha.58, bound storage can preserve its original voter binding while
installing configuration-bearing snapshots whose active set changed. Raft
startup uses stable metadata to recover a changed active set after local
compaction. Since alpha.69, network installation also accepts stable snapshots
across a committed transition when the receiver's committed history proves the
same active set and configuration identity. Since alpha.71, joint snapshot
installation also works when the receiver has committed the same joint
configuration. Uncommitted transitions remain unsupported; the storage API
also serves offline history validation and trusted inputs.
The [alpha.59 follow-up](configuration-snapshot-continuity.md) checks that a
replacement configuration snapshot extends the previously stored phase,
identity, and active voter sets.

Tests cover in-memory installation, durable joint and stable round-trips,
rejection of unbound configuration snapshots and mismatched legacy snapshots,
offline scan baselines across multiple committed transitions, stable runtime
restore/transfer/compaction, and matching-history joint runtime recovery.
No independent reviewer approval is claimed.
