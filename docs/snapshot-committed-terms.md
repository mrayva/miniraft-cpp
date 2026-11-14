# Snapshot boundary terms and committed prefixes

An InstallSnapshot request could previously advertise a snapshot at an index
after the follower's committed prefix but with an older boundary term. Such a
history cannot be a Raft log: entry terms do not decrease as indices increase.
The follower could accept that request, update its current term, replace its
snapshot and application state, and acknowledge the impossible boundary.

Incoming snapshot requests are now checked before term or role changes. A
snapshot after the committed prefix must have a boundary term at least as large
as the term at the follower's commit index. If the requested boundary itself is
committed and still known locally, its term must match. A boundary older than the
local snapshot cannot be checked for equality because its individual term has been
compacted, but its term must not exceed the local snapshot term. Invalid requests
are dropped, consistent with other malformed RPCs.
Each chunk is checked again, including when commitment advances mid-transfer.

Deterministic tests reproduce the prior mutation with both retained and compacted
committed prefixes, check higher-term invalid requests leave hard state and the
application unchanged, exercise a commit advancing during transfer, and confirm a
valid later snapshot still installs. This check detects these specific committed
term contradictions; it does not authenticate peers or prove snapshot contents
match the committed application state. Membership changes and joint snapshot
installation remain disabled. No wire or storage format changes.

The in-memory and durable storage APIs also reject a replacement snapshot whose
term is below the existing compacted boundary term. This protects direct storage
callers even when they bypass RPC validation. Rejection leaves the selected
snapshot, retained suffix and durable image unchanged.
