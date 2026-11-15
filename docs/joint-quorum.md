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
and monotonic commit index. Validated retained configuration history now selects
the runtime quorum used by elections, commit advancement and strong-read
confirmation. An uncommitted joint entry uses both voter sets; after that entry
commits, the joint quorum remains active until its stable successor commits.
The later [joint snapshot follow-up](configuration-entries.md) allows snapshots
at a committed joint boundary when both voter sets and configuration identity
match the receiver's history.

Tests exhaust every pair of nonempty old/new subsets of five IDs and every vote
subset, including disjoint and overlapping sets, even-sized groups, absent self
and outsider votes. They independently enumerate thresholds for joint commit/read
watermarks and check current-term gating. Separate stable-set tests compare the
calculation with strict-majority order statistics for sizes 1–8. Existing Raft,
partition, learner, snapshot and read tests exercise the fixed-membership paths.

This arithmetic now backs the bounded live transition API described in
[configuration entries](configuration-entries.md). Recovered suffix transitions
and live changes require a bound original voter set, a persisted commit index and
endpoints for every active voter. Joint snapshot recovery requires the exact
committed joint identity; transfer that would introduce an unknown transition
or cross uncommitted history remains unsupported.
Leader removal and explicit leadership transfer were not implemented in this
milestone. Follow-up: [leadership transfer](leadership-transfer.md) adds a
bounded `TimeoutNow` path for removing the active leader after the joint entry
commits. Joint snapshots require matching committed history; transfer across
an uncommitted configuration remains open.

The change adds a public type and changes TRaft's in-memory layout; rebuild the
library and dependents together. Wire and durable formats are unchanged.
Self-review checked overlap, strict majorities, current-term commitment, stable
path equivalence and learner exclusion. Runtime tests verify that elections,
commit advancement and strong-read confirmation each wait for both majorities
under an in-progress joint configuration. No independent reviewer approval is
claimed. The live transition API remains disabled.
