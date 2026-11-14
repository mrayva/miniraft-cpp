# Durable configuration snapshot metadata: v0.1.0-alpha.45

Bound `TState` and `TDurableState` instances can install and recover MRFTS002
joint and MRFTS003 stable snapshots so offline tools can validate configuration
history after restart. A store must already have a voter binding, and
`snapshot.Voters` must equal that binding. Durable recovery checks the same
invariant before exposing the image. An unbound store cannot infer its original
voter set from configuration metadata and continues to reject the snapshot.

The durable file layout is unchanged. The backend persists the snapshot together
with its existing voter binding; it does not reinterpret that binding from the
metadata. The history scanner also verifies that a joint baseline's old voters
match the bound set.

This is persistence for offline validation, not runtime configuration recovery.
`TRaft` still rejects configuration metadata snapshots on startup, the network
receiver rejects them, configuration log entries remain disabled, and compaction
continues to produce fixed-voter snapshots. A stable snapshot whose voter list
differs from the existing binding is rejected. Live transition semantics must
define how the binding changes before such snapshots can be used by Raft.

Tests cover in-memory installation, durable joint and stable round-trips, bound
voter mismatch rejection without rewriting storage, offline scan baseline checks,
and runtime startup rejection without rewriting the recovered image. Self-review
checked that recovery requires an existing binding and that no live Raft path
adopts the experimental metadata. No independent reviewer approval is claimed.
