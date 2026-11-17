# Bounded outbound peer queues

Each bundled `TNode` limits retained outbound messages while its peer is slow or
disconnected:

* Data traffic: at most 128 messages and 16 MiB.
* Raft control traffic: at most four messages and 64 MiB plus 4 KiB.

These byte counts include the root frame and any payload children. The Raft
budget accommodates a complete bounded AppendEntries batch and one maximum
snapshot image plus protocol metadata. Both budgets include messages in the
active writer batch as well as messages waiting in the queue. They count frame
bytes, not allocator overhead or kernel socket buffers.

Queued RequestVote, TimeoutNow and configuration-barrier requests replace an
older queued request of the same kind. AppendEntries and InstallSnapshot
requests share the replication class, because only the newest queued
replication attempt should be sent after a disconnected peer reconnects. A
request already being written cannot be replaced; it continues to consume its
budget until the write finishes or fails. A replacement is admitted atomically,
so a full budget does not discard the previous queued request. All Raft peer
messages use `TrySend`; when a request or response is rejected, Raft's existing
RPC retry path can regenerate it.

`TNode::TrySend` returns `false` when a message cannot be admitted. `Send` keeps
the existing `void` interface and throws `TNodeQueueFull` on that condition.
Forwarded client commands use `TrySend`: rejection removes the forwarding route
and replies with the existing error code. Direct `Send` calls throw
`TNodeQueueFull`; the server's periodic task catches that specific exception so
an outbound overload cannot terminate its loop.

The limits apply per peer. Cluster-wide memory still scales with the number of
configured peers, and this does not bound the Raft log, application request
queues, allocator overhead or kernel buffers. A client reply that cannot be
queued can have an unknown write outcome, as with a connection failure during
reply delivery.

The deterministic queue regression checks disconnected message-count and
byte-count rejection, repeated replication retry coalescing, and forwarding
failure cleanup. It does not simulate a slow but connected socket or sustained
disconnect/recovery under a live cluster workload.
