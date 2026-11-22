# Trusted legacy voter binding: v0.1.0-alpha.50

Older durable MRFTD001/MRFTD002 files do not persist the complete fixed voter
set. Raft cannot infer that set safely from a vote, a legacy snapshot voter
list, or the current process's peer list alone.

`TDurableState::BindVotersFromTrustedBaseline(voters)` provides an explicit
migration path for an unbound, recovered durable store. The caller must supply
the complete original fixed voter set from an independently trusted source,
such as the cluster's verified deployment record. The operation checks that the
stored vote belongs to the set, any legacy snapshot names the same voters and
has no configuration metadata, retained terms are valid, and no reserved
configuration entries are present. It then atomically republishes the durable
state with its voter binding before the store is passed to `TRaft`.

Startup tooling can call `CanBindVotersFromTrustedBaseline(voters, &reason)`
first to check whether the recovered bytes are internally consistent with that
baseline. A typical sequence is:

```cpp
// At a controlled startup boundary, before constructing TRaft:
// 1. Stop this replica and preserve a backup of its durable files.
// 2. Obtain the complete original voter set from a separately verified record.
auto state = std::make_shared<TDurableState>(path, nodeId);
std::string reason;
if (!state->CanBindVotersFromTrustedBaseline(originalVoters, &reason))
    throw std::runtime_error("Legacy voter migration refused: " + reason);
state->BindVotersFromTrustedBaseline(originalVoters);
TRaft raft(state, nodeId, peers);
```

For an offline operation, `miniraft-migrate-disk-state` can validate and bind
the same recovered durable image. Stop the replica, preserve a backup, and
obtain the voter set from an independently verified deployment record first:

```sh
miniraft-migrate-disk-state --bind-durable --durable-base /var/lib/raft/node \
  --node-id 7 --voters 1,2,3 --dry-run
miniraft-migrate-disk-state --bind-durable --durable-base /var/lib/raft/node \
  --node-id 7 --voters 1,2,3
```

Use `--commit-index INDEX` when binding through the trusted-history path for a
configuration-bearing snapshot. That commit boundary must also come from an
independently verified record. The tool checks and reopens the published
binding, but cannot prove the completeness or historical correctness of either
operator-supplied value. Do not run it while the replica is serving traffic.

The dry run does not write state. It reports the same consistency checks that
the mutating call will apply, including vote, snapshot, and retained-log checks.
It cannot establish that `originalVoters` is complete or historically correct;
that proof remains an operator responsibility. Complete the migration before
serving traffic, and construct Raft only after the binding has been durably
published. Do not infer a baseline from current peer endpoints.

This API records the caller's assertion; it cannot prove the assertion is
correct, discover prior membership transitions hidden by a legacy snapshot, or
authorize a membership change. Existing `BindVoters` behavior is unchanged.
Do not call the migration API when the original complete voter set is unknown.

For an unbound MRFTD002 image whose snapshot contains configuration metadata,
use `CanBindVotersFromTrustedHistory(voters, commitIndex, &reason)` and
`BindVotersFromTrustedHistory(voters, commitIndex)`. This path requires the
snapshot's complete retained transition chain to begin at configuration index
zero with `voters` as its original set. It validates retained suffix entries
against that snapshot baseline, preserves the supplied commit boundary, and
publishes the binding before startup. Histories truncated before the original
transition remain unprovable and are rejected. The commit boundary and the
completeness of `voters` still require independent records.

New `TDiskState` metadata versions persist a CRC-protected commit boundary and
original voter binding, so fresh split-format stores can use live membership.
For a recovered old 20-byte metadata store, call
`TDiskState::CanBindVotersFromTrustedBaseline(voters, commitIndex, &reason)` to
preflight a simple baseline, then call
`TDiskState::BindVotersFromTrustedBaseline(voters, commitIndex)` before
constructing Raft. These methods require an independently verified voter set
and commit index, check the stored vote and term/log bounds, and refuse
compacted state, retained configuration entries, and unsupported log flags.
Binding atomically writes the new metadata before startup. The older
`ValidateTrustedLegacyVoterBaseline` name remains as a mutating compatibility
wrapper. Recovered unbound state otherwise fails Raft startup. For retained
membership entries or a compacted snapshot, use the full-history offline
`--bind-disk-state` path below. Split `TDiskState` supports snapshot installation
and compaction with versioned metadata. The separate Linux [offline
converter](legacy-disk-state-migration.md) remains the path to move a split-format
store to `TDurableState`.

Tests cover MRFTD001, MRFTD002 and MRFTD003 recovery, rejection of inconsistent
vote/snapshot baselines and unanchored configuration history without rewriting
the source file, and the current MRFTD006 binding after publication and restart.
Dry-run tests verify both successful and rejected checks preserve the source
bytes, and that diagnostics are cleared on success. Legacy formats recover
their commit floor at the snapshot boundary; the explicit history API accepts a
separately supplied commit boundary.

The durable-store CLI selects this path when `--commit-index` is present;
without that option it uses the simpler baseline path, which rejects retained
configuration entries and configuration-bearing snapshots.

For an existing split-format `TDiskState` store, bind its original voters using
the retained log and snapshot history validator:

```sh
miniraft-migrate-disk-state --bind-disk-state --disk-base /var/lib/raft/node \
  --node-id 7 --voters 1,2,3 --commit-index 42 --dry-run
miniraft-migrate-disk-state --bind-disk-state --disk-base /var/lib/raft/node \
  --node-id 7 --voters 1,2,3 --commit-index 42
```

This mode requires the independently verified commit boundary. It supports
retained membership entries and compacted snapshots only when the validated
history is anchored at configuration index zero and names the supplied original
voters. The dry run checks the same history without changing files, including
orphaned previous generations; it also refuses a missing store rather than
creating one. A successful bind publishes metadata, reopens the split store, and
verifies its voter set, Raft boundaries, and snapshot. As with the durable mode,
stop the replica and back up its files before running the command.

Self-review completed. No independent reviewer approval is claimed.
