# Live joint membership

The runtime accepts membership changes through a joint-consensus log sequence.
Node IDs are nonzero 32-bit unsigned values across the local Raft API, voter
configuration records and RPC messages.
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
leader append the stable successor. If an old-set-only voter later becomes
leader while that stable successor is already present but uncommitted, it can
transfer to a caught-up new-set voter using the same bounded protocol; the
target still campaigns under both joint majorities and commits the successor
with a current-term entry. The bounded transfer protocol and its failure
behavior are described in [leadership transfer](leadership-transfer.md).
Every node must have replication endpoints for all voters before it accepts the
transition. The embedding application can provision them locally with
`RegisterPeerTarget` on each existing replica before the leader proposes the
joint entry; this endpoint registration does not itself change quorum
membership. Endpoint discovery and configuration distribution remain
responsibilities of the embedding application.
The leader advertises joint-consensus and joint-election-fence support in
AppendEntries responses. `CheckJointConfigurationReadiness()` and
`ProposeJointConfiguration()` require current-term support from every voter in
the proposed stable set. They also require fence-capable voters to form a strict
majority of the old set. Every old-side election quorum then intersects a
voter that rejects legacy vote and pre-vote requests while either the committed
or latest configuration is joint. This permits removal of an offline old voter
when the remaining fence-capable old voters still form a majority, while
preventing an old-format candidate from winning with an unsupported old-side
majority. The leader advances the joint entry's commit index only after a strict
old-side majority of fence-capable voters has acknowledged that entry. A
configuration-bearing AppendEntries and a snapshot that carries joint
membership metadata use a reserved leader marker; current receivers reject an
unmarked transition from an older leader. Older binaries leave these reserved
response and request bits zero.
Every voter in the proposed stable set must separately advertise application
snapshot restore support; newly added voters must advertise storage and
application snapshot installation support. This lets an operator remove a
legacy voter that blocks compaction while preventing another incapable voter
from replacing it.
`GetConfigurationState()` returns a copy of the validated committed and latest
configurations. During a pending joint entry, `Latest` is joint and `Committed`
remains stable. After the joint entry commits but before its stable successor
commits, `Committed` is joint and `Latest` is stable; `GetVoterIds()` continues
to report the joint voter union until finalization commits.
On restart, the durable original voter binding and retained membership history
are distinct from the active replication endpoints. When a configuration entry
or configuration-bearing snapshot proves that membership changed, the binding
restores the original set while committed history restores active roles; only
active voters need replication endpoints. Without retained configuration
history, configured voters must still reproduce the original binding. A stable
snapshot retains that membership proof after compaction, so restart accepts a
persisted vote for a voter removed by the compacted transition and preserves
the vote through the current term.

These are `TRaft` APIs. `TRaftServer` reconciles outbound client-response readers
after Raft messages and on its timer loop, so a promoted voter can receive
forwarded command replies without restarting. After the stable successor commits
a demotion, a peer endpoint can be unregistered. `TRaftServer` retires socket
types that expose `Shutdown() noexcept` or a live native `Fd()`, then retains the
endpoint until reader and writer coroutines unwind. This includes
`TSslSocket<THandle>`, whose native descriptor is shut down to wake pending TLS
I/O. Custom wrappers that hide the descriptor need to expose a shutdown hook or
use `NNet::TRetirableSslSocket<THandle>` from
`miniraft/net/retirable_ssl_socket.h`.

Incoming AppendEntries configuration records are decoded and the complete
candidate suffix is scanned before a higher RPC term is persisted. The scan
checks predecessor identity, transition order, committed-prefix constraints,
voter binding and endpoint availability. Same-term configuration entries must
match byte for byte. Replacing an uncommitted configuration suffix recomputes
the active roles and quorum from the surviving log; committed entries cannot be
truncated. A malformed or semantically invalid candidate suffix is rejected
without changing the term, commit index, log or response state.
The new-side-only leader forwarding exception is narrower: its prior-term joint
entry must extend the receiver's exact locally committed stable voter set and
configuration identity, even when the request's log predecessor is not yet
matched and the receiver needs to return a conflict hint.
An old-set leader originates each joint entry. After a later-term election under
the joint quorum, a new-set-only leader may forward that prior-term entry to a
lagging peer; it cannot originate a joint entry in its own term. A new-set
leader originates the stable successor. If leadership later moves to an
old-set-only voter while that successor is pending, it may forward the
prior-term successor to a transfer target, but cannot create one in its own
term.
After election, a new-set-only leader starts catch-up replication at the
retained joint entry when that entry is still in the log. Once the stable
successor is committed, it uses the retained predecessor joint identity for
lagging peers. Bounded append batches may send the joint and stable records in
separate rounds; this lets a peer validate the leader before it receives the
successor.

The record payload remains MRFTC001: magic[8], phase[4] (joint=1, stable=2),
entry term[8], previous configuration index[8], previous configuration
term[8], old count[4], new count[4], old voter IDs[4 each], new voter IDs[4
each], and CRC32[4] over the preceding payload. The outer entry term must match
the encoded term and its flags must equal `EConfiguration`. CRC checks accidental
corruption; it is not authentication. Every voter in the proposed stable set
must run a compatible binary because older versions reject this entry type.
Readiness checks both capability bits on a current-term AppendEntries response
from each remote voter in the proposed stable set, including newly added
voters. For the old set, it checks that fence-capable voters exceed half of the
set; each old-side election quorum must intersect that group. A removed old
voter may be offline or on an older binary if that condition holds. Its
endpoint remains configured during the transition, and the old-set majority
must still replicate and commit the joint entry. A removed voter that returns
before finalization may reject the entry and cannot help satisfy that majority;
after finalization it is no longer an active member and must be upgraded and
staged as a learner before being added again.

After the stable successor commits, local compaction records the active voter
set and stable configuration identity while preserving the original durable
voter binding ([details](stable-membership-compaction.md)). During an
uncommitted transition, compaction may include applied committed progress only
when its boundary is before the pending configuration entry; the pending entry
remains in the suffix. A receiver may install a snapshot under the same rule
when its metadata matches the locally committed configuration and its boundary
is before the receiver's pending entry. If it reaches or passes that entry, the
receiver accepts it only when the snapshot's phase, voter sets, and
configuration index/term exactly match the locally validated pending entry.
This lets a leader snapshot confirm the entry's commitment without allowing it
to invent a transition; a mismatch is rejected before a higher term is
persisted. The sender must be a voter in the quorum configuration that governs
the pending transition, including the committed joint quorum while a stable
successor is pending. Stable snapshots across completed transitions still
require matching stable voter set and configuration index/term. MRFTS002
snapshots for existing members require exact joint metadata before a higher
term is accepted.
If a receiver's exact committed baseline is a joint snapshot, it may also
install that joint configuration's stable successor: the stable image must name
the joint new voter set, carry a later configuration identity, and come from a
voter in the stable set. This path is validated before a higher term is
persisted and cannot bridge from an older stable baseline that lacks the joint
history.
Joint snapshot compaction waits until every active voter has acknowledged that
committed joint boundary. Do not remove or edit committed state to bypass these
limits.

When local history contains the predecessor identity, MRFTS005 joint snapshots
preserve the preceding stable voter set and joint entry identity; MRFTS004 stable
snapshots carry that link through the stable successor. MRFTS006 and MRFTS007
carry up to 32 contiguous transitions; MRFTS008 and MRFTS009 carry up to 256;
MRFTS010 and MRFTS011 carry up to 1,024.
A node with a nonempty stale store can recover across multiple compacted
transitions when a transition in the chain matches its exact committed stable
configuration. For a stable snapshot,
the sender must belong to both voter sets in every transition after that
anchor. For a joint snapshot, completed transitions have the same requirement;
the sender for the active joint phase may belong to either side, allowing the
old leader to finish that phase. The chain is checked before a higher term is
accepted. Disjoint transitions and senders absent from a required set remain
unsupported for online stale-store recovery; use a fresh learner or
independently verified history. Snapshot metadata does not prove commitment by
itself. AppendEntries responses advertise support for the 32-, 256- and
1,024-transition snapshot-history formats. The leader sends the highest
advertised format and caps its history to that format's limit. For a nonempty
transition chain, a peer advertising lineage support but no history feature
receives predecessor-only MRFTS004/MRFTS005 lineage and can recover from a
matching trusted predecessor. A peer without lineage support receives MRFTS003
stable or MRFTS002 joint configuration metadata without predecessor or
transition history. MRFTS002 installation requires the exact committed joint
configuration. MRFTS003 can match a committed stable configuration or complete
the stable successor of the receiver's exact committed joint configuration
when the sender belongs to the new set. Fresh learners use a separate
constrained bootstrap path. These formats cannot recover a stale store across
unknown membership transitions.

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
The live transition path also tests both one-sided quorum schedules for
overlapping sets `{1, 2, 3}` to `{3, 4, 5}` and disjoint sets `{1, 2, 3}` to
`{4, 5, 6}`: neither majority alone commits the joint entry, and the entry
commits only after both majorities acknowledge it. Persisted-joint election
recovery covers both shapes too; the candidate remains unelected with only one
side's votes, and its current-term entry commits only after both replication
majorities acknowledge it. A runtime strong-read regression on disjoint sets
also checks both response orders and keeps the read pending until both voter
majorities have acknowledged its barrier.
