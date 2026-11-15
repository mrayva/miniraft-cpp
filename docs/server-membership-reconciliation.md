# Server membership role reconciliation

`TRaftServer::Serve()` used to start outbound response readers only for peers
that were voters at startup. A learner promoted through a joint configuration
could then participate in Raft replication but could not reliably receive
forwarded client responses from that peer without restarting the server.

The server now reconciles peer readers after processing Raft messages and from
its timer loop. When a peer first becomes an active voter, the server starts its
response reader. It also handles learners staged through `AddLearnerTarget`
after `Serve()` has begun. Reader IDs are monotonic: joint promotion starts the
reader once, and a later stable removal leaves the endpoint usable as a learner
target without restarting or duplicating the reader.

The coroutine can be suspended inside a socket read, and the networking layer
does not provide safe cancellation for that operation. Therefore
`RemoveLearnerTarget` rejects removal of a peer whose response reader has
started. After removing that peer from the voter set, keep it as a learner
replication target until server shutdown; restart the server before removing the
transport endpoint. Staged learners that have never been voters can still be
removed live.

This closes the server-side role-change gap for forwarded client responses. It
does not implement leadership transfer, membership-aware compaction, or joint
snapshot installation.
