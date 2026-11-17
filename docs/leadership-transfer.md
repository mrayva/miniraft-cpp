# Leadership transfer for leader removal: v0.1.0-alpha.70

`TRaft::ProposeJointConfiguration(newVoters)` may now exclude the current
leader. The leader remains active under the joint quorum until that entry
commits. To finish a transition that removes it, call
`TRaft::TransferLeadership(target, now)` with a voter from the new set, then let
the elected leader call `FinalizeConfiguration()`.

Transfer is intentionally limited to this membership-change case. It requires
a durable store with persisted commit and original-voter metadata, a committed
joint configuration, and a target whose correlated replication ACK covers the
complete leader log and commit index. Normally, the leader must also have a
current-term committed entry. There is one exception: an old-set-only leader
may transfer while the stable successor to that committed joint entry is
pending. Committing a current-term entry after that successor would also commit
the successor and remove the old leader, so the transfer uses the already
committed joint boundary instead. No other uncommitted configuration is
allowed. The target must be a different configured voter in the joint
configuration's new set. These checks prevent a voter with a stale log or
commit watermark from starting the handoff election.

The old leader steps down before sending a `TIMEOUT_NOW_REQUEST`. The target
accepts it only from its recognized current leader, in the same term, while its
committed joint configuration excludes that leader and matches the request's
configuration, log, and commit boundaries. For the pending-successor case, it
must also have that stable successor in its log. It then starts the ordinary
election path. The normal vote rules and both joint majorities still apply. The
new leader commits a current-term entry under the joint quorum; that advances
commit through the pending stable successor and completes the change.

The request has no acknowledgment. The old leader retries it every second while
it remains a follower and has not reached its election deadline. A higher-term
message or accepted leader contact cancels retries. If all requests are lost,
the old leader's ordinary election timeout remains the fallback. Deterministic
durable-store coverage drops the first request and confirms the retry starts the
new-set voter before timeout, then drops every request and confirms the old
leader can still win again with a vote from that voter. Another test rejects a
malformed higher-term request without changing the receiver's durable state.
The transfer path also works after both peers restart from the same committed
joint snapshot, with only an uncommitted stable successor retained in the log;
the target campaigns under the recovered joint quorum and can complete that
successor after election. This recovery test uses disjoint old and new voter
sets, so neither the outgoing leader nor the target can supply both majorities
alone.

`TIMEOUT_NOW_REQUEST` is a new Raft wire message. Upgrade all voters to a
compatible build before using a transition that removes its leader; older
builds reject the message. Joint snapshots require the receiver to have already
committed the exact same joint voter sets and configuration identity; see
[configuration entries](configuration-entries.md). Snapshots still cannot
introduce an unknown transition or cross an unknown configuration boundary. A
snapshot matching a receiver's exact validated pending transition can now
confirm that transition at the snapshot boundary.
