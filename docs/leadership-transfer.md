# Leadership transfer for leader removal: v0.1.0-alpha.70

`TRaft::ProposeJointConfiguration(newVoters)` may now exclude the current
leader. The leader remains active under the joint quorum until that entry
commits. To finish a transition that removes it, call
`TRaft::TransferLeadership(target, now)` with a voter from the new set, then let
the elected leader call `FinalizeConfiguration()`.

Transfer is intentionally limited to this membership-change case. It requires
a durable store with persisted commit and original-voter metadata, a committed
joint configuration, a current-term committed entry, and a target whose
correlated replication ACK covers the complete leader log and commit index. The
target must be a different configured voter in the joint configuration's new
set. This prevents a voter with a stale log or commit watermark from starting
the handoff election.

The old leader steps down before sending a `TIMEOUT_NOW_REQUEST`. The target
accepts it only from its recognized current leader, in the same term, while its
committed joint configuration excludes that leader and matches the request's
configuration, log, and commit boundaries. It then starts the ordinary
election path. The normal vote rules and both joint majorities still apply. If
elected, the new leader appends and commits its current-term no-op under the
joint quorum before it can append the stable successor.

The request has no acknowledgment. The old leader retries it every second while
it remains a follower and has not reached its election deadline. A higher-term
message or accepted leader contact cancels retries. If all requests are lost,
the old leader's ordinary election timeout remains the fallback. Deterministic
durable-store coverage drops the first request and confirms the retry starts the
new-set voter before timeout, then drops every request and confirms the old
leader can still win again with a vote from that voter. Another test rejects a
malformed higher-term request without changing the receiver's durable state.

`TIMEOUT_NOW_REQUEST` is a new Raft wire message. Upgrade all voters to a
compatible build before using a transition that removes its leader; older
builds reject the message. Joint snapshots require the receiver to have already
committed the exact same joint voter sets and configuration identity; see
[configuration entries](configuration-entries.md). Snapshots still cannot
introduce an uncommitted transition or cross an unknown configuration boundary.
