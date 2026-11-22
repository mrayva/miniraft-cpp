# Quorum-contact leadership expiration

A leader that stops hearing from a voting quorum yields leadership after
`TRaft::TLogLimits::LeadershipExpiration` (20 seconds by default). Configure
it on each `TRaft` instance before processing network traffic. The timer uses
the same monotonic timestamps supplied to `TRaft::Process` and
`TRaft::ProcessTimeout`. The initial deadline starts when the node becomes
leader. Each fresh quorum response can move the deadline forward.

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

The timeout is configured per Raft instance and must exceed the fixed heartbeat
interval (`TTimeout::Election / 2`, currently 2.5 seconds), so a healthy leader
can receive a response before expiration. It is a liveness measure, not a
leadership lease: it does not guarantee that an old leader and a newly elected
leader never overlap during a partition. Pre-vote reduces term disruption from
isolated candidates, but does not provide that lease guarantee. Strong reads
continue to require a fresh quorum acknowledgement; default leader reads do not
provide that guarantee.

Deterministic regressions keep a healthy leader active beyond the default
expiry window, then isolate it and check that it yields at the last known quorum
response deadline. Tests also verify the configured timeout after quorum contact
and immediately after election with no replication replies. Values at or below
the heartbeat interval are rejected.
