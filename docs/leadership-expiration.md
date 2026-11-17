# Quorum-contact leadership expiration

A leader that stops hearing from a voting quorum now yields leadership after
`TTimeout::LeadershipExpiration` (20 seconds by default). The timer uses the
same monotonic timestamps supplied to `TRaft::Process` and
`TRaft::ProcessTimeout`.

Only correlated responses from active voters count. AppendEntries responses
count whether replication succeeds or reports a log conflict; correlated
InstallSnapshot and configuration-barrier responses also count. The expiry
point is the latest timestamp at which the active configuration's quorum could
have responded. During joint consensus, that requires a strict majority from
both voter sets. Learner responses do not count.

On expiry, the node becomes a follower, clears its claimed leader ID, releases
volatile replication-transfer state, and starts an ordinary randomized
election timeout. Expiration does not increment or persist the term. A
single-voter cluster remains leader because its local voter is available.

The timeout is currently a fixed constant, not a per-cluster option. It is a
liveness measure, not a leadership lease: it does not guarantee that an old
leader and a newly elected leader never overlap during a partition. The
implementation also has no pre-vote phase. Strong reads continue to require a
fresh quorum acknowledgement; default leader reads do not provide that
guarantee.

The deterministic regression keeps a healthy leader active beyond one expiry
window, then isolates it and checks that it yields exactly when the last known
quorum response expires.
