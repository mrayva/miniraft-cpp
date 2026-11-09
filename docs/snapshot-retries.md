# Snapshot retry progress: v0.1.0-alpha.18

Timeout retries and strong-read requests could indefinitely stall InstallSnapshot
catch-up for a healthy follower or learner. Each send replaced the outstanding
chunk's sequence number. If an ACK arrived after another send, the leader rejected
it, retransmitted the same offset with another sequence number, and repeated the
cycle. This could happen without any corrupt messages or failed storage.

Retries now preserve the identity of the same immutable chunk. A valid partial
ACK consumes that identity; the next chunk receives a fresh sequence number. A
failure also consumes it before restarting from offset zero. Duplicate ACKs stay
stale both before and after the next send. A changed snapshot or leadership term
continues to discard the old transfer context. Invalid replies do not consume the
outstanding identity.

This reuse applies only to snapshot chunks. Snapshot ACKs still cannot confirm a
strong-read barrier; a subsequent fresh AppendEntries ACK is required. No wire
layout, durable format, role configuration or migration procedure changes.

Two deterministic regressions run against both voter and learner receivers. Each
ACK is held while the leader emits three retries, driven by either timeouts or
repeated ApproveRead calls; retry packets are dropped and the original ACK is
then delivered. Before the fix both tests failed at the first 64 KiB boundary:
the leader retransmitted offset zero instead of advancing to 65536. After the fix
the multi-chunk image completes, the application loads once, duplicate ACKs do
not consume subsequent chunks, and a new committed tail reaches the receiver.

The full validation set is 224 unit tests plus three CLI tests across 17 CTest
targets under ASan/UBSan, with 12 Release core targets. Existing tests cover
partial-transfer restart, stale replies after a reset, leader replacement,
duplicate installation, corruption, durable publication crashes and strong-read
isolation. Review is self-review; no independent approval is claimed.

Dynamic promotion/removal and replicated joint-consensus configurations remain
open. Snapshot catch-up is a prerequisite for promotion, not authorization to
change the fixed voter set.
