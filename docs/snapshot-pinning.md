# Snapshot transfer pinning: v0.1.0-alpha.19

Periodic compaction could starve snapshot catch-up even after alpha.18 fixed
retry correlation. Each send selected the leader's newest snapshot and reset the
peer's transfer if its index changed. When compaction outpaced a multi-chunk
transfer, the receiver repeatedly started new images without completing one.
This affected both voters and non-voting learners.

Each peer now pins the immutable image and encoded bytes selected at the start
of its transfer. New local snapshots do not change its index, offset, content or
outstanding chunk identity. A completed transfer releases that image. If the
leader compacted past the receiver's next index, the next transfer starts from
the newest available image; otherwise ordinary AppendEntries supplies the tail.
An ACK for the older image proves only that image's boundary, never the newer
prefix. Snapshot ACKs still cannot confirm strong-read barriers.

Failed chunks restart the pinned image from offset zero. Leadership changes still
discard outgoing transfer contexts. One image/encoding pair is retained per
active peer transfer, with sharing between transfers of the same image. The
[alpha.43 pin budget](snapshot-pin-budget.md) later capped active retention to two
distinct image generations and defers further compaction until a transfer
completes or becomes idle for 30 seconds without a successful chunk ACK. The
idle timeout releases stalled pins so a dead peer cannot block compaction
indefinitely. Incomplete incoming images share a 256 MiB process-wide budget by
default. The budget does not include retained Raft or application state,
outgoing images, capture buffers or transport memory, and no general transfer
cancellation API is provided.

A deterministic regression commits and compacts new data before every chunk ACK,
including the final ACK, for both voter and learner receivers. The old code fails
on the first replacement (snapshot 5 instead of 4). The fixed code completes the
original image without resetting offsets, releases its snapshot object, then
installs the newer image and replicates a committed tail. Existing tests cover
timeouts, read traffic, duplicate/stale replies, receiver restart, leader change,
corruption, atomic durable publication and strong-read isolation.

Sanitizer CI exposed a reconnect deadline race: a timed-out connection could
leave its write-readiness event queued, allowing a late readiness notification to
resume a connector after the retry path had begun. MiniRaft now uses a
five-second deadline for adapters that support `Connect(deadline)`. The updated
`coroio` connect awaiter cancels the pending connect on timeout, and the poller
removes the write event and clears already-queued readiness copies before the
connector resumes its retry sleep. A deterministic lifecycle regression makes
timeout win after readiness has been queued, then verifies draining that ready
queue cannot resume the expired connector. A separate socket regression checks
that a refused connection reports its actual socket error.

Adapters that expose only `Connect()` still rely on OS connection-failure timing.
This is a bounded timeout for connect, not a general asynchronous cancellation
or shutdown contract.

Validation: 228 unit tests and three CLI tests across 18 CTest targets under
ASan/UBSan, plus 12 Release core targets. Self-review checked image ownership,
encoding reuse, completed-boundary accounting, follow-up transfer selection and
term replacement, connection wakeup sources and deadline behavior. No independent
reviewer approval is claimed. Wire messages and
durable formats are unchanged; no additional migration is required.

This guarantees progress of the selected finite image under eventual chunk
delivery and ACKs. Reaching the live log end still requires enough replication
capacity relative to writes/compaction and sufficiently stable leadership.
The bounded promotion and joint-consensus path is described in
[live membership](configuration-entries.md). Matching-history joint snapshot
installation is supported; snapshots cannot establish an uncommitted
configuration. Leader removal is supported by
[leadership transfer](leadership-transfer.md).
