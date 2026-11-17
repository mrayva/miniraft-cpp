# Server membership role reconciliation

`TRaftServer::Serve()` used to start outbound response readers only for peers
that were voters at startup. A learner promoted through a joint configuration
could then participate in Raft replication but could not reliably receive
forwarded client responses from that peer without restarting the server.

The server now reconciles peer readers after processing Raft messages and from
its timer loop. When a peer first becomes an active voter, the server starts its
response reader. It also handles learners staged through `AddLearnerTarget`
after `Serve()` has begun. Runtime add and remove operations also update the
request processor's forwarding routes. This lets a node that later becomes
leader receive forwarded client commands through an endpoint added after server
construction. Reader tracking follows each registered endpoint: joint promotion
starts one reader, stable demotion keeps it available for learner traffic, and
re-promotion does not start a duplicate. Removing the endpoint retires that
reader and releases its ID for a fresh endpoint.

When a peer with a response reader is removed after voter demotion,
`RemoveLearnerTarget` shuts down its socket to wake pending reads and writes.
The server retains the node until the reader and writer coroutines unwind, then
closes it. A reader in its reconnect delay exits on its next wakeup. The returned
node is retired and must not be re-added; register a fresh endpoint if the ID is
used again. The server can reuse that ID while the old endpoint's reader is
still unwinding: reader tracking is cleared for the removed ID, the old node is
retained by endpoint identity, and a promoted replacement starts its own reader.
Learner targets that never became voters can still be removed live, with the
embedding application responsible for any write already in progress.
The built-in TCP socket supports this shutdown path. Socket wrappers that do
not expose a nonthrowing shutdown or native descriptor, including the current
coroio TLS wrapper, still reject live removal after a response reader starts.

Before proposing a joint voter change, call
`TRaftServer::CheckJointConfigurationReadiness(newVoters)` (or the underlying
`TRaft` method) on each replica and compare the reports. `MissingPeerTargets`
is local: it lists active or proposed voters for which that replica has no
configured replication target. The report from the prospective leader also
lists `AddedVotersWithoutCommitAck`; only that
leader has the correlated replication evidence required to promote a learner.
`Ready()` is true only on a leader whose local prerequisites are satisfied.
This check does not distribute endpoints, so provision each replica's targets
through the server's normal endpoint registration path first.

This closes the server-side role-change gap for forwarded client responses.
Leadership transfer for removing the active leader is described in
[leadership transfer](leadership-transfer.md). Membership-aware compaction is
available after the active voters acknowledge the committed configuration.
Matching committed joint snapshots can also be compacted and installed; a
snapshot cannot introduce an unknown voter configuration. An exact match for a
receiver's validated pending transition can confirm it at the snapshot boundary.

A Linux regression starts a serving server with a durable voter configuration,
activates a staged peer through a committed joint entry, then commits its stable
demotion. It checks that the retirable TLS endpoint is shut down on removal,
which proves the server started and tracked the promoted peer's response reader.
The schedule then reuses the peer ID with a fresh endpoint, promotes it again,
and removes it to check that the replacement's reader is independently tracked
while the old endpoint retires. The test runs in a child process because the
server's serving coroutines are intentionally long-lived.
