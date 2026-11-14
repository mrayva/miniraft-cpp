# Idle InstallSnapshot transfer expiry: v0.1.0-alpha.42

InstallSnapshot is chunked and bounded to 64 MiB per image, but a receiver held
the accumulated partial image until a replacement transfer, a completed
transfer, or a voter election timeout. Learners do not campaign, so a learner
could retain that allocation indefinitely after an interrupted transfer. Normal
heartbeats could also keep a voter from reaching its election timeout while an
image remained abandoned.

Incoming snapshot state now records the time of the last valid chunk. The
receiver discards an incomplete image after 30 seconds without a valid chunk,
regardless of role or heartbeat traffic. Matching retries count as activity so
lost acknowledgements remain safe; malformed, conflicting, and out-of-order
chunks do not extend the deadline. A continuation after expiry receives a
failure with offset zero, which tells the sender to restart the pinned image
from its first chunk. Completed installations and the covered-prefix metadata
fast path release the partial state as before.

A deterministic learner test starts a partial transfer, continues ordinary
leader heartbeats, advances the injected clock beyond the idle limit, and
verifies that the old continuation is rejected and a fresh transfer succeeds.
The test also verifies the application and Raft snapshot boundary are unchanged
until the fresh image completes.

Self-review checked both receiving paths, duplicate chunk handling, role
independence and restart-from-zero behavior. This bounds the lifetime of one
partial in-memory image; it does not impose a cluster-wide memory quota or stream
snapshot content to disk. Live voter reconfiguration and joint snapshot adoption
remain separate open features.
