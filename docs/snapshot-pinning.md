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
active peer transfer, with sharing between transfers of the same image. A stalled
peer may keep an older image alive while the store compacts; total snapshot memory
is not independent of the configured peer count. This does not introduce a
resource quota or general transfer cancellation API.

A deterministic regression commits and compacts new data before every chunk ACK,
including the final ACK, for both voter and learner receivers. The old code fails
on the first replacement (snapshot 5 instead of 4). The fixed code completes the
original image without resetting offsets, releases its snapshot object, then
installs the newer image and replicates a committed tail. Existing tests cover
timeouts, read traffic, duplicate/stale replies, receiver restart, leader change,
corruption, atomic durable publication and strong-read isolation.

Validation: 227 unit tests and three CLI tests across 18 CTest targets under
ASan/UBSan, plus 12 Release core targets. Self-review checked image ownership,
encoding reuse, completed-boundary accounting, follow-up transfer selection and
term replacement. No independent reviewer approval is claimed. Wire messages and
durable formats are unchanged; no additional migration is required.

This guarantees progress of the selected finite image under eventual chunk
delivery and ACKs. Reaching the live log end still requires enough replication
capacity relative to writes/compaction and sufficiently stable leadership.
Dynamic promotion/removal and joint consensus remain open.
