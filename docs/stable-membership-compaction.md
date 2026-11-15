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
included log boundary. Joint snapshot creation and network snapshot transfer
across membership transitions remain unsupported. A lagging peer still needs a
separately reconciled active configuration before it can accept a snapshot.

The deterministic durable test commits an add transition, rejects compaction
while joint, commits the stable successor, compacts, and restarts from the
original binding while recovering the active voter set from MRFTS003 metadata.
It also checks that the original binding remains unchanged.
