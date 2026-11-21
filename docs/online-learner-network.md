# Network server learner staging: v0.1.0-alpha.40

`TRaftServer<TSocket>` exposes `RegisterPeerTarget(id, endpoint)`,
`ReplacePeerTarget(id, endpoint)`, and `UnregisterPeerTarget(id)` on any Raft role, using a
`shared_ptr<TNode<TSocket>>` endpoint. Register the endpoint on every existing
replica before a joint configuration introduces that voter. Each local
registration makes the endpoint available to configuration validation and the
request-routing layer without changing voters or quorums. Registering it on the
leader also starts ordinary learner replication, so the leader can check
`IsLearnerCommitAcknowledged` before proposing promotion. The leader-only
`AddLearnerTarget(id, endpoint)` API remains a convenience for adding and
staging an endpoint only on the leader; do not call it for an ID already
registered there. Both paths add the endpoint to the server's drain set, so the
normal server loop can send replication and snapshot traffic without restarting.
The request processor's peer map is updated at the same time, so an endpoint
added after server construction can receive forwarded commands if it later
becomes leader. Removal drops that forwarding route along with the Raft progress
and future server drains; the method returns the endpoint so the embedding
application can manage its transport lifetime. The voter list and quorum
calculations remain unchanged.

For an existing voter or learner, `ReplacePeerTarget` changes only this
replica's outbound transport and forwarding route. It does not change the
remote member's listener or distribute an address. If the old endpoint has a
response reader, its socket must support safe retirement; the server retains the
old node until its I/O coroutines unwind and starts a reader for the replacement.
Raft clears that peer's replication and capability evidence, so the replacement
must make fresh progress before it contributes acknowledgement or snapshot
capability proof. Apply the replacement on each replica through the embedding
application's rollout mechanism.

Construct the remote node with the right address and socket factory, and make
sure its local Raft instance is configured as a learner with the same voter set.
If only the leader has registered the endpoint, use `AddLearnerTarget` there to
start replication; if every replica already has it registered, replication
starts on the leader as part of that registration. In either case, wait for
`IsLearnerCommitAcknowledged` before promotion.
The storage on the current leader must have a bound voter list. The learner
still must have a configured return path to the voters. The server does not
discover, authenticate or configure the remote process.

These calls must run on the same event-loop thread as `TRaftServer` and be
serialized with Raft processing. They may be called before `Serve()` or from a
serialized callback after serving begins. Calls from other threads and
reentrant calls from transport or application callbacks are unsupported.
After removal, a write already in progress or data already queued in the
endpoint may remain. Keep the returned endpoint alive while that I/O can still
use it, then shut it down according to the embedding application's transport
lifecycle. Once `TRaftServer` starts a response reader for a peer promoted to
voter, `UnregisterPeerTarget` (or leader-only `RemoveLearnerTarget`) can remove
it after a stable configuration demotes it. The server shuts down the socket
and retains the endpoint until its reader and writer coroutines unwind. The
returned node is retired; use a fresh endpoint if that ID is registered again.
The raw coroio TLS wrapper does not expose the shutdown hook needed for live
removal after a response reader starts. Use
`NNet::TRetirableSslSocket<THandle>` from
`miniraft/net/retirable_ssl_socket.h` for both the server listener and peer
endpoints when live TLS peer retirement is needed. The deterministic TLS
lifecycle test verifies that shutdown wakes a pending TLS read and lets the
retired node clean up.

The KV CLI still discovers peers at startup. Endpoint registrations are
instance-local and are lost on restart; the application must provision them on
each replica before proposing a configuration change. Replication progress is
leader-local and must be checked again after failover. This adds network
transport support for non-voting catch-up. Voter promotion
and replicated joint consensus are now available through the bounded durable
transition path in [live membership](configuration-entries.md). Leader removal
is supported after joint commit through [leadership transfer](leadership-transfer.md).
Joint snapshot installation requires matching committed history; stable
snapshots require the receiver to have the matching committed membership
identity. The server now
starts response readers when peers become voters during a live transition,
without restarting `Serve()`.

The deterministic server test checks target registration, voter/quorum
invariance, removal and rollback after a nonleader rejection. The existing
learner tests cover snapshot catch-up and stale responses after removal. No
independent reviewer approved this milestone.
