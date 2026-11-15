# Live joint membership

The runtime accepts membership changes through a joint-consensus log sequence.
`TRaft::ProposeJointConfiguration(newVoters)` appends the joint entry, which
activates both the old and new voter majorities. After the joint entry commits,
`TRaft::FinalizeConfiguration()` appends its stable successor. The stable set
becomes active only after that successor commits. Elections, commit advancement
and strong-read confirmation use the active stable or joint quorum.

Both APIs require storage that persists the commit index and original voter
binding. `TDurableState` provides those guarantees. In-memory `TState` and legacy
stores cannot start a live transition. The leader must have a committed entry in
its current term. Every newly added voter must first be staged as a learner and
have a correlated replication acknowledgement through the leader's committed
boundary. If the leader remains in the new set, it can finalize the transition
after the joint entry commits. To remove the active leader, commit the joint
entry, transfer leadership to a caught-up voter in the new set, then let that
leader append the stable successor. The bounded transfer protocol and its
failure behavior are described in [leadership transfer](leadership-transfer.md).
Every node must have replication endpoints for all voters before it accepts the
transition. Endpoint discovery and configuration distribution remain
responsibilities of the embedding application.
On restart, the durable original voter binding and retained membership history
are distinct from the active replication endpoints. When a configuration entry
or configuration-bearing snapshot proves that membership changed, the binding
restores the original set while committed history restores active roles; only
active voters need replication endpoints. Without retained configuration
history, configured voters must still reproduce the original binding.

These are `TRaft` APIs. `TRaftServer` reconciles outbound client-response readers
after Raft messages and on its timer loop, so a promoted voter can receive
forwarded command replies without restarting. A reader cannot be safely stopped
while suspended in socket IO; after a peer has been a voter while the server is
running, keep its endpoint registered as a learner or restart the server before
removing that transport target.

Incoming AppendEntries configuration records are decoded and the complete
candidate suffix is scanned before a higher RPC term is persisted. The scan
checks predecessor identity, transition order, committed-prefix constraints,
voter binding and endpoint availability. Same-term configuration entries must
match byte for byte. Replacing an uncommitted configuration suffix recomputes
the active roles and quorum from the surviving log; committed entries cannot be
truncated. A malformed or semantically invalid candidate suffix is rejected
without changing the term, commit index, log or response state.

The record payload remains MRFTC001: magic[8], phase[4] (joint=1, stable=2),
entry term[8], previous configuration index[8], previous configuration
term[8], old count[4], new count[4], old voter IDs[4 each], new voter IDs[4
each], and CRC32[4] over the preceding payload. The outer entry term must match
the encoded term and its flags must equal `EConfiguration`. CRC checks accidental
corruption; it is not authentication. All voters must run a compatible binary
before a transition because older versions reject this entry type.

After the stable successor commits, local compaction records the active voter
set and stable configuration identity while preserving the original durable
voter binding ([details](stable-membership-compaction.md)). Compaction remains
blocked during uncommitted configuration state. A receiver can install a stable
snapshot across a transition only when its committed history has the same stable
voter set and configuration index/term. MRFTS002 snapshots can be installed
while the receiver has already committed the exact same joint configuration;
both voter sets and the configuration index/term must match before a higher
term is accepted. Joint snapshot compaction waits until every active voter has
acknowledged that committed joint boundary. Snapshots cannot introduce a
configuration the receiver has not committed or cross an uncommitted transition.
Do not remove or edit committed state to bypass these limits.

Deterministic durable-store tests cover learner catch-up, voter addition and
removal, leadership transfer and fallback when its request is dropped,
per-node activation, joint commit propagation, stable finalization, rejection
of invalid higher-term RPCs before term mutation, and rollback of an
uncommitted joint suffix. The test suite also checks recovery of retained joint
and stable histories, including post-restart role recovery after adding and
removing a voter. Joint snapshot tests cover compaction, exact-history
installation, higher-term rejection of forged voter metadata, restart recovery,
finalization from the snapshot baseline, and a dropped/retried all-voter
compaction barrier that enables follower-local snapshots. Restart coverage also
verifies that a compacted stable membership can recover with the removed voter
endpoint omitted while preserving the original binding.
The live transition path also tests both one-sided quorum schedules for the
overlapping sets `{1, 2, 3}` and `{3, 4, 5}`: neither majority alone commits the
joint entry, and the entry commits after both majorities acknowledge it.
