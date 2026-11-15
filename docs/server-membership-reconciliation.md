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
used again. Learner targets that never became voters can still be removed live,
with the embedding application responsible for any write already in progress.
The built-in TCP socket supports this shutdown path. Socket wrappers that do
not expose a nonthrowing shutdown or native descriptor, including the current
coroio TLS wrapper, still reject live removal after a response reader starts.

This closes the server-side role-change gap for forwarded client responses.
Leadership transfer for removing the active leader is described in
[leadership transfer](leadership-transfer.md). Membership-aware compaction is
available after the active voters acknowledge the committed configuration.
Matching committed joint snapshots can also be compacted and installed; a
snapshot cannot introduce an unknown voter configuration. An exact match for a
receiver's validated pending transition can confirm it at the snapshot boundary.
