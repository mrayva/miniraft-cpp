# Online learner replication staging

`TRaft::AddLearnerTarget(id, endpoint)` lets the current leader add a non-voting
replication target without restarting that Raft object. `RemoveLearnerTarget(id)`
removes a local target and returns its endpoint. The voter set and persisted
binding do not change. Staged learners receive the ordinary log or a snapshot
when needed, but never count toward election, commit, or strong-read quorums.
`IsLearnerCaughtUp` remains a point-in-time log watermark, not an authorization
to promote or a guarantee of application health.
`IsLearnerCommitAcknowledged` separately reports whether a successful reply to
the latest AppendEntries request confirms the leader's current Raft commit
boundary while the learner also matches the leader's log. An unanswered newer
request makes the prior acknowledgement stale. It does not report
application-applied progress and does not authorize promotion by itself.
Before promotion, the leader also requires an InstallSnapshot capability
advertisement from each added voter. A caught-up learner that cannot install
snapshots is not ready for promotion because it could later block safe log
compaction.

`TRaft::RegisterPeerTarget` and `TRaft::UnregisterPeerTarget` provide local
endpoint provisioning on any role. They add or remove this instance's transport
target and derived learner role, but do not change or persist the voter set.
Provision new voter endpoints on every existing replica before proposing the
joint entry. Registration on the leader also starts learner replication; use
`IsLearnerCommitAcknowledged` to check the current leader's catch-up proof.
Unregistration rejects active voters, including voters in a pending joint
configuration.

These are serialized embedding APIs, not a replicated cluster configuration or a
promotion API. The caller must ensure the destination is configured as a learner
with the same voter set, arrange and authenticate its transport, and keep its
endpoint alive while asynchronous operations may still use it. After removal,
use the returned endpoint to coordinate safe transport shutdown; queued traffic
can still exist. The caller must serialize registration/removal with Raft
processing and must not re-enter them from transport callbacks. Duplicate IDs,
voters, the local ID and null endpoints are rejected. The leader-only
`AddLearnerTarget` staging API also rejects nonleaders and requires storage with
an already-bound matching voter set; unbound and legacy stores cannot stage.

Registration is local to this Raft object and is not persisted or propagated to
other voters. The embedding application must re-register endpoints after
restart and make sure each new leader has the endpoint. Replication progress is
leader-local and catch-up must be checked again after leadership changes.
Removing a target drops replication progress and pending RPCs, and a subsequent
registration starts a fresh probe. Delayed responses cannot satisfy new RPCs.
The KV CLI loads transport endpoints at startup. `TRaftServer` runtime integration
is described in [network learner staging](online-learner-network.md). Older
binaries must not be mixed with learner configurations because they treat
unmarked peers as voters.

## Share startup endpoints across KV replicas

Create a peer config file with one role and endpoint per line. For example:

```text
# Original voters
voter 127.0.0.1:8001:1
voter 127.0.0.1:8002:2
learner 127.0.0.1:8003:3
```

Start each durable KV replica with its own `--id` and the same file contents.
Use a separate storage directory for each replica.

```sh
./kv --server --durable --id 1 --peer-config /path/to/peers.conf
```

`--peer-config` replaces `--node` and `--learner`. Blank lines and comments
starting with `#` are accepted. Use brackets around IPv6 addresses. The parser
rejects duplicate IDs, duplicate address and port pairs, a missing local ID,
and files without a voter. Startup provisions all listed endpoints again on
restart, so any elected voter with this file can replicate to the learner.

File roles supply the configured bootstrap baseline. Use the replicated joint
configuration API for voter changes. Raft validates the durable voter binding
and derives live roles from committed history.

On Linux, you can add learner endpoints or replace a remote address for the
same node ID while KV is serving. Publish a complete file using an atomic
rename, then send `SIGHUP` to each replica you want to update. Look for
`Reloaded peer config` in each process log before checking learner catch-up or
proposing a voter change. Reload rejects bootstrap role edits, endpoint
removals, address reassignment across node IDs, and local listener changes.
Restart a node to change its own listener. Existing cluster membership and
voter proofs still govern which changes can be committed.

Each process validates the file and creates replacement endpoints before
applying changes. The server applies endpoints individually. If an endpoint
operation fails during application, earlier successful changes can remain in
effect. Fix the cause and send another `SIGHUP` to reconcile the rest.
Distribution and signaling are handled by your deployment system.

Deterministic tests cover live add, snapshot catch-up, voter partitions and strong
reads, removal and re-addition with delayed append replies, removal during a
multi-chunk snapshot with delayed acknowledgements, invalid calls, and refusal
on unbound or legacy stores. Durable-store tests verify that staging does not
rewrite the binding and is absent on restart. A leader-change test verifies
that an embedding application must register the target on a new leader that
does not already have it.
A bounded durable-store promotion/finalization path now uses these catch-up
acknowledgements; see [live joint membership](configuration-entries.md).
Leader removal is supported after joint commit through
[leadership transfer](leadership-transfer.md). The leader-change test verifies
that a node without a local endpoint registration must register it before it
can replicate to that peer. Persistent endpoint discovery is still the embedding
application's responsibility. Snapshot transfer may
advance through the committed prefix before a pending configuration entry, but
may cover the entry only when its configuration identity exactly matches the
receiver's pending transition and its sender belongs to the governing quorum.

A newly staged learner can bootstrap from a configuration-bearing snapshot even
when its committed log has already been compacted past the membership entry. For
a stable snapshot, configure the empty learner with the exact active voter set.
For a joint snapshot, configure it with the union of the old and new voter sets.
The learner must have no log, commit, snapshot, vote, or applied application
progress, and the snapshot sender must be named in the snapshot's active voter
set. Installation advances the learner's configuration baseline from the
validated snapshot; it does not make the learner a voter. A voter or a node with
prior state still needs matching committed configuration history. Deterministic
coverage checks stable and joint bootstrap, rejects mismatched voter metadata
and nonempty learner state before accepting a higher term, and verifies the
learner role and voter binding after installation.
