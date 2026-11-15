# Stable snapshot installation after a membership change: v0.1.0-alpha.69

An `InstallSnapshot` receiver can now accept an MRFTS003 stable snapshot after
a committed voter change when its own validated log history already identifies
the same stable voter set and configuration index/term. The bounded metadata
probe checks this before changing a higher term or acknowledging an already
covered boundary. An existing member still cannot accept a stable snapshot that
introduces a configuration identity it has not committed. A completely fresh
learner is the bootstrap exception: its configured voter set must exactly match
the snapshot's stable voter set, it must have no log, commit, snapshot, vote, or
applied application progress, and the sender must be in that voter set.

Legacy MRFTS001 images carry no configuration identity. The receiver can accept
one after a membership change only when its boundary is at or beyond the
committed stable configuration and its voter set matches; it then carries the
receiver's trusted identity into the stored image. After durable installation,
Raft advances the configuration scan baseline to the new snapshot boundary so
later entries continue from the correct history.

At alpha.69, snapshots were rejected while the receiver's history was joint or
had an uncommitted transition. Since alpha.71, MRFTS002 installation is enabled
when the receiver has committed the same joint configuration, as described in
[joint snapshot runtime](joint-snapshot-runtime.md). A fresh learner can also
bootstrap from MRFTS002 when configured with the union of both voter sets. An
existing receiver may now install a matching snapshot before its uncommitted
configuration entry, retaining that suffix; a snapshot that covers the entry is
still rejected. A node with existing state still needs matching committed
history. Leader removal was added later through [leadership transfer](leadership-transfer.md).
Snapshot metadata is a consistency assertion, not cryptographic proof of
cluster identity; the learner bootstrap path relies on its configured voter set
and a sender named in the incoming active set.

Deterministic coverage includes rejecting a forged identity before accepting a
higher term, accepting a matching stable snapshot after a committed transition,
carrying that identity through a legacy refresh, and committing a later joint
entry after installation to prove the scan baseline advanced.
