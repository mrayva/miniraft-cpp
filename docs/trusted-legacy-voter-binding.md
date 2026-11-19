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
`TDiskState::ValidateTrustedLegacyVoterBaseline(voters, commitIndex)` before
constructing Raft. It requires an independently verified voter set and commit
index, checks the stored vote and term/log bounds, and refuses compacted state
or any retained configuration entry. A successful call atomically writes the
new metadata before startup. Recovered unbound state otherwise fails Raft
startup. The storage still does not support snapshots or compaction. The
separate Linux [offline converter](legacy-disk-state-migration.md) remains the
path to move a split-format store to `TDurableState`.

Tests cover MRFTD001, MRFTD002 and MRFTD003 recovery, rejection of inconsistent
vote/snapshot baselines and unanchored configuration history without rewriting
the source file, and the current MRFTD004 binding after publication and restart.
Dry-run tests verify both successful and rejected checks preserve the source
bytes, and that diagnostics are cleared on success. Legacy formats recover
their commit floor at the snapshot boundary; the explicit history API accepts a
separately supplied commit boundary.

Self-review completed. No independent reviewer approval is claimed.
