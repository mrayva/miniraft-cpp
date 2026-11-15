# Stable configuration snapshot runtime support: v0.1.0-alpha.46

This alpha.46 milestone accepted MRFTS003 snapshots for the node's configured
stable voter set. A later [joint snapshot follow-up](configuration-entries.md)
also accepts MRFTS002 when the receiver has committed the exact same joint
configuration.

With that same-set check, the existing snapshot lifecycle can restore a durable
snapshot into the application, receive and persist it over InstallSnapshot, and
compact later application progress. Compaction carries the prior stable
configuration entry index and term into the new image. A follower whose committed
prefix already covers a stable snapshot validates the RPC boundary, voter list,
configuration identity and encoded data length before acknowledging it.

Once a node has a stable configuration identity, installing a newer MRFTS001
snapshot carries that identity into the stored MRFTS003 image. An incoming
MRFTS003 image with a different identity is rejected, including on the
already-covered fast path. This prevents a legacy-format refresh from silently
erasing configuration history and prevents a peer from replacing it with a
conflicting baseline.

The original voter binding still identifies the trusted initial set. The live
joint-consensus log protocol can move the active set without rewriting that
binding. After a stable successor commits, local compaction records the new
active set and startup recovers it from the stable snapshot while the configured
bootstrap view still matches the original binding. Network snapshot handling
now allows transfer across a committed stable transition when the receiver has
the exact same stable identity and voter set. Joint installation requires the
receiver's committed joint history to match both voter sets and the
configuration index/term. Snapshot metadata cannot introduce an uncommitted
transition or establish membership on its own. Upgrade all voters before
sending configuration-bearing images because older binaries may acknowledge
an already-covered unknown snapshot format.

Tests cover durable application restore, identity preservation through
compaction, restart, and legacy-format refresh, chunked InstallSnapshot,
covered-prefix validation of stable metadata, and rejection of conflicting
identities. Joint snapshot runtime coverage is maintained with the live
membership tests. Self-review completed; no independent reviewer approval is
claimed.
