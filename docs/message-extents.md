# Message buffer extents: v0.1.0-alpha.23

Locally constructed message holders exposed a mutable frame length but did not
remember their allocated buffer size. A caller could enlarge an entry's declared
length while leaving it within the protocol maximum; proposal validation then
accepted it and the in-memory log copied beyond the allocation. A deterministic
ASan regression reproduced a heap-buffer-overflow.

Holders now carry the buffer extent and payload-array capacity through typed
conversions. Frame and batch validation reject lengths or entry counts beyond
those extents. Proposal and command request/reply admission validate the holder
before accessing or copying the data. The storage clone paths check the extent,
and the legacy disk write path fails closed for an invalid buffer. A malformed
forwarded reply is rejected before its client route is consumed.

NewHoldedMessage and InitPayload populate extents automatically. Callers that
construct holders directly around an external allocation must supply its actual
byte capacity and, for batches, the actual payload-array capacity to the holder
constructor; the default zero capacity fails validation. Holder layout changed,
so rebuild the library and all dependent binaries together. Wire formats and
durable formats are unchanged. The extents are trusted metadata owned by local
C++ code, not a sandbox against callers that forge pointers or mutate capacities.
Application code that bypasses Raft admission remains responsible for its own
validation.

Deterministic tests cover the original oversized proposal, forged batch count,
oversized child entry, typed conversion and a malformed forwarded reply followed
by a successful valid reply. Self-review checked framing, admission, clones and
forwarded-route ordering. No independent reviewer approval is claimed. Voting
membership changes and joint consensus remain open.
