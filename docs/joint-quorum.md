# Joint quorum arithmetic: v0.1.0-alpha.24

This step prepares the election, commit and strong-read quorum calculations for
joint consensus. A joint quorum requires a strict majority of both the old and
new voter sets. Its replicated-index or read-ack watermark is the lower of the
two majority watermarks. Shared voters count in each set; outsiders and learners
do not contribute. Even-sized groups require more than half.

TQuorum validates positive, sorted, unique, nonempty voter sets and computes
stable or joint election results and watermarks. Its value callback must return
the same value for a voter throughout a call. TVolatileState can apply this
arithmetic to commit advancement, retaining the current-term commit restriction
and monotonic commit index. Existing fixed-membership election, commit and strong
read paths now use the same stable-quorum arithmetic; they do not enter joint mode.

Tests exhaust every pair of nonempty old/new subsets of five IDs and every vote
subset, including disjoint and overlapping sets, even-sized groups, absent self
and outsider votes. They independently enumerate thresholds for joint commit/read
watermarks and check current-term gating. Separate stable-set tests compare the
calculation with strict-majority order statistics for sizes 1–8. Existing Raft,
partition, learner, snapshot and read tests exercise the fixed-membership paths.

This is arithmetic foundation, not a reconfiguration API. It does not replicate
configuration entries, change roles, elect/remove leaders, bind joint membership
durably or encode configuration history in snapshots. A safe next step must define
configuration entry ordering and recovery, activation and rollback of uncommitted
configurations, promotion readiness, old/new quorum use at every protocol phase,
and snapshot installation across transitions. Until those pieces are implemented
and tested, stored voter bindings remain immutable and promotion stays disabled.

The change adds a public type and changes TRaft's in-memory layout; rebuild the
library and dependents together. Wire and durable formats are unchanged.
Self-review checked overlap, strict majorities, current-term commitment, stable
path equivalence and learner exclusion. No independent reviewer approval is
claimed. There is no live membership change to exercise or benchmark yet.
