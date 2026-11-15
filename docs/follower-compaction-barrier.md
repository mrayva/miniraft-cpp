# Follower compaction barrier: v0.1.0-alpha.72

Membership-bearing compaction needs proof that every active voter acknowledged
the committed configuration. The leader has this evidence in correlated
replication acknowledgements; followers do not. Previously, a follower could
commit a joint or stable configuration and still be unable to compact locally.
The KV and SQL checkpoint loops run on every replica, so this left follower logs
growing after a membership change.

After the leader observes the configuration commit ACK from every active voter,
it sends a `CONFIGURATION_BARRIER_REQUEST` to each active follower. A follower
accepts only a same-term request from its known leader when its own committed
history has the exact configuration index and term and no configuration change
is uncommitted. It returns an explicit acceptance response. The leader retries
requests after lost messages and refreshes accepted certificates periodically,
so a follower that restarts can rebuild the volatile certificate without a new
election. A new leader recomputes the proof from its own replication ACKs.

The barrier cannot change voter sets or advance terms. It only permits local
compaction after the leader has already observed every active voter commit the
configuration. Learners cannot lead and may compact their own committed state
without a voter certificate. The barrier does not persist independently; after
restart a voter must receive a fresh certificate before compacting further.
The barrier is a new RPC type. Upgrade all active voters before a leader with a
committed membership configuration starts this protocol; older binaries reject
the unknown frame. Bootstrap-only fixed-membership clusters do not send barrier
messages.

Deterministic durable coverage drops a joint barrier request and verifies that
the follower refuses to compact until retry succeeds. It also simulates losing
the follower's volatile certificate, checks periodic re-advertisement, drops a
stable barrier, and verifies follower compaction after the stable certificate
arrives. RPC validation confirms that an unproven higher-term request cannot
advance hard state and that a same-term request without committed leader contact
is explicitly rejected.
