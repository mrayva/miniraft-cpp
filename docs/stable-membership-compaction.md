# Stable membership compaction: v0.1.0-alpha.68

Once a joint entry and its stable successor have both committed, `TRaft::Compact()`
can checkpoint application progress. The snapshot records the active stable voter
set together with the stable configuration entry index and term. The durable
store continues to retain its original voter binding as a separate identity.

Startup can recover this snapshot using the original bootstrap voter view and
the configured endpoints for every active voter. The snapshot's stable metadata
becomes the trusted configuration baseline; retained log entries after the
snapshot are scanned from that boundary. This allows a compacted node to restart
after a voter addition or removal without treating the active set as a new
original binding.

The metadata is not a cryptographic proof of a committed transition. Startup
trusts the persisted stable checkpoint produced by the validated local history.
Do not use the offline storage snapshot APIs to change membership; use the live
joint-consensus APIs, or independently verify a checkpoint before loading it.

Compaction rejects a committed joint configuration and any uncommitted
configuration change. Application progress must include the committed stable
configuration entry so the snapshot cannot claim a voter set newer than its
included log boundary. A lagging receiver can install an MRFTS003 image across
that transition after it has committed the same stable voter set and
configuration identity. The receiver rejects mismatched identities, images
that precede its committed transition, and snapshots while local history is
joint or uncommitted. MRFTS002 joint snapshot creation and installation remain
unsupported.

The deterministic durable tests commit an add transition, reject compaction
while joint, compact after the stable successor, and restart from the original
binding while recovering the active voter set. They also install a matching
stable snapshot at a lagging receiver, reject an unproven configuration
identity before a higher term is accepted, and append a later transition to
verify that snapshot installation advanced the receiver's history baseline.
