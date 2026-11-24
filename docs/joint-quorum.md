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
endpoints for every active voter. At alpha.24, joint snapshot recovery required
the exact committed joint identity, and receivers with an uncommitted
configuration could not install snapshots. The later [joint snapshot
runtime](joint-snapshot-runtime.md) follow-up permits a matching snapshot before
that entry while retaining the suffix, or a snapshot covering it only when it
exactly matches the receiver's validated pending configuration and its sender
belongs to the governing quorum. Introducing an unknown transition remains
unsupported.
At alpha.24, leader removal and explicit leadership transfer were not yet
implemented. The later [leadership transfer](leadership-transfer.md) follow-up
adds a bounded `TimeoutNow` path for moving leadership to a caught-up voter in
the new set after the joint entry commits; the target must still win both joint
majorities. Joint snapshots require matching committed history. A matching
snapshot may cover committed progress before a pending configuration entry
while preserving that entry in the retained suffix.

The change adds a public type and changes TRaft's in-memory layout; rebuild the
library and dependents together. Wire and durable formats are unchanged.
Self-review checked overlap, strict majorities, current-term commitment, stable
path equivalence and learner exclusion. Runtime tests verify that elections,
commit advancement and strong-read confirmation each wait for both majorities
under an in-progress joint configuration. No independent reviewer approval is
claimed. At alpha.24, the live transition API was still disabled. It is now
available through the bounded durable path in
[live membership](configuration-entries.md); exact-history snapshot rules are
described in [joint snapshot runtime](joint-snapshot-runtime.md). Live
reconfiguration tests now exercise one-sided quorum schedules with both
overlapping `{1, 2, 3}` to `{3, 4, 5}` sets and disjoint `{1, 2, 3}` to
`{4, 5, 6}` sets. Restarted joint configurations also exercise elections and
current-term commits under both overlap patterns. An end-to-end disjoint
transition test now stages the new voters as learners, commits the joint
configuration, transfers leadership to a new-only voter, commits the stable
configuration, propagates its watermark, compacts the stable snapshot on each
active voter, and restarts replicas with endpoints only for the final voter
set. It also drops links to two new voters while the joint entry is pending,
confirms the old-side quorum plus one new-side voter cannot commit it, then
restores links and observes the joint commit.
A complementary network test isolates both old-side peers while all three new
voters acknowledge the joint entry; the new majority alone cannot commit it,
and the entry commits only after the old-side links return.
