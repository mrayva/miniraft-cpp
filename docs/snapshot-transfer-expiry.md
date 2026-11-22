# Idle InstallSnapshot transfer expiry: v0.1.0-alpha.42

InstallSnapshot is chunked and bounded to 64 MiB per image, but a receiver held
the accumulated partial image until a replacement transfer, a completed
transfer, or a voter election timeout. Learners do not campaign, so a learner
could retain that allocation indefinitely after an interrupted transfer. Normal
heartbeats could also keep a voter from reaching its election timeout while an
image remained abandoned.

Incoming snapshot state now records the time of the last forward-progressing
chunk. The receiver discards an incomplete image after 30 seconds without new
bytes, regardless of role or heartbeat traffic. Exact duplicate chunks are
acknowledged idempotently but do not extend the deadline; malformed, conflicting,
and out-of-order chunks do not extend it either. If an acknowledgement is lost
long enough for the receiver to expire, the next continuation receives a
failure with offset zero and the sender safely restarts the pinned image from
its first chunk. This may retransmit bytes, while ensuring repeated old chunks
cannot retain the receiver's memory indefinitely. Completed installations and
the covered-prefix metadata fast path release the partial state as before.

The sender also releases a pinned image after 30 seconds without a successful
chunk acknowledgement. It retries from the current local snapshot on a later
heartbeat, so a peer that stopped responding cannot retain an old generation
and block future compaction indefinitely. Only successful forward progress
refreshes this deadline; failed replies and repeated sends do not.

A deterministic learner test starts a partial transfer, continues ordinary
leader heartbeats, advances the injected clock beyond the idle limit, and
verifies that the old continuation is rejected and a fresh transfer succeeds.
The test also verifies the application and Raft snapshot boundary are unchanged
until the fresh image completes.

Self-review checked both receiving paths, duplicate chunk handling, role
independence, restart-from-zero behavior and release of a stalled sender pin.
This bounds the lifetime of one partial in-memory image; it does not impose a
cluster-wide memory quota or stream snapshot content to disk. At alpha.42, live
voter reconfiguration and joint snapshot adoption were still open. They are now
covered by [live membership](configuration-entries.md) and [matching-history
joint snapshot installation](joint-snapshot-runtime.md).
Snapshot installation still requires the receiver's committed or validated
pending history to match; an image cannot introduce an unknown transition.
