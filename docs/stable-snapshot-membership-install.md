# Stable snapshot installation after a membership change: v0.1.0-alpha.69

An `InstallSnapshot` receiver can now accept an MRFTS003 stable snapshot after
a committed voter change when its own validated log history already identifies
the same stable voter set and configuration index/term. The bounded metadata
probe checks this before changing a higher term or acknowledging an already
covered boundary. A stable snapshot cannot introduce a configuration identity
that the receiver has not committed.

Legacy MRFTS001 images carry no configuration identity. The receiver can accept
one after a membership change only when its boundary is at or beyond the
committed stable configuration and its voter set matches; it then carries the
receiver's trusted identity into the stored image. After durable installation,
Raft advances the configuration scan baseline to the new snapshot boundary so
later entries continue from the correct history.

Snapshots remain rejected while the receiver's history is joint or has an
uncommitted transition. Joint MRFTS002 snapshot installation and transfer to a
receiver that has not committed the matching stable configuration remain
unsupported. Leader removal was added later through
[leadership transfer](leadership-transfer.md). Snapshot metadata is a
consistency assertion, not cryptographic proof of cluster identity; this path
trusts the receiver's previously validated durable configuration history.

Deterministic coverage includes rejecting a forged identity before accepting a
higher term, accepting a matching stable snapshot after a committed transition,
carrying that identity through a legacy refresh, and committing a later joint
entry after installation to prove the scan baseline advanced.
