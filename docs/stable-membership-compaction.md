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

This milestone initially rejected compaction during joint consensus. Since
alpha.71, local compaction can create MRFTS002 while a joint configuration is
committed, and a receiver can install it after committing the same joint voter
sets and configuration index/term. Compaction waits for every active voter to
acknowledge the committed configuration, preventing a snapshot from removing
the only copy of a transition that an active voter has not learned. Uncommitted
configuration changes remain ineligible for compaction. A lagging receiver can
also install MRFTS003 across a stable transition after it has committed the
matching stable voter set and identity.

The deterministic durable tests wait for configuration ACKs before compaction,
compact committed joint and stable states, and restart from the original
binding while recovering active voter sets. They also install a matching
stable snapshot at a lagging receiver, reject an unproven configuration
identity before a higher term is accepted, and append a later transition to
verify that snapshot installation advanced the receiver's history baseline.
