# Durable client write deduplication design

Status: protocol foundation is present, but durable deduplication is not
implemented. The current client request path does not provide durable operation
identity. A timeout or lost response leaves the write outcome unknown; retrying
a non-idempotent write may apply it twice.

The new request and response frame types are strictly validated. An optional
`IIdempotentRsm` contract and bounded, versioned combined application/session
snapshot codec are also present. The request processor still returns
`Unsupported` before forwarding, appending, or applying because operation
identity is not yet carried through forwarding or replicated log entries. No
capability is advertised, and clients must not treat these interfaces as a
usable idempotency feature.

## Goal and boundary

An opt-in client write must execute at most once across leader changes,
restarts, retries and snapshot compaction. A retry of the most recently
completed operation in a client session returns its cached response. The
existing request type keeps its current at-least-once behavior.

The guarantee covers commands submitted through the Raft state machine. It does
not cover external side effects performed outside deterministic application
state, such as sending email or charging a separate payment service. Those
effects need an application outbox or an idempotent downstream API.

## Identity and client contract

An idempotent write carries a replicated client-session epoch, a stable
128-bit client session ID and a 64-bit sequence number. Clients persist all
three values and the next sequence number. A session starts at sequence 1,
permits one unacknowledged write at a time, and increments the sequence only
after receiving that write's response. A new process resuming a session retries
the same epoch, sequence and payload.

The identity is distinct from the response cookie and from sessionized
forwarding correlation IDs. Cookies correlate one transport exchange; the
operation identity survives retries, forwarding and process restarts.

The opt-in request must use a new, explicitly negotiated message type so
existing `TCommandRequest` layouts remain unchanged. A peer that cannot preserve
the operation identity through forwarding must reject the request; silently
falling back to a legacy envelope would remove the guarantee.

## Replication and duplicate handling

The operation identity and a digest of the command payload are stored with the
replicated log entry. They remain metadata owned by Raft; the application sees
the original command bytes and the stable operation identity through an
optional idempotent state-machine interface. Raft computes SHA-256 over the
exact request payload so callers cannot choose a digest that hides payload
changes.

The state machine maintains one committed record per open client session:
highest completed sequence, payload digest, log index and serialized response.
It updates that record atomically with the application mutation. On an exact
retry of the latest sequence and digest, the saved response is returned without
calling the application again. Reusing that sequence with a different digest is
a conflict. A sequence below the high-water mark is reported as already
completed and is never applied again. A sequence above `high-water + 1` is
rejected as a gap.

Different leaders can append the same operation before either copy commits.
Replicas therefore enforce deduplication in log order during application too:
the first committed occurrence updates the state machine; later occurrences
with the same identity and digest return the saved response without applying
again. A committed identity with a different digest is a deterministic
conflict, not a second application call.

## Snapshot and storage contract

The state machine's application image, current session epoch and deduplication
records must be captured and restored as one versioned snapshot. Validation
checks all three before either application state or Raft state is published.
This keeps the deduplication high-water marks and cached response aligned with
`LastAppliedIndex`; retaining the records only in volatile Raft memory would
make retries unsafe after restart or compaction. Epoch advance is a committed
control entry, so every request from an older epoch is rejected before append
and again while applying a retained log suffix.

Legacy snapshots remain readable, but they cannot prove that an earlier
operation was deduplicated. Enabling the opt-in request type requires an
idempotent state machine with the new snapshot contract on every voter. Mixed
clusters must reject opt-in requests until all active voters advertise support.

Each session consumes bounded state. Admission must account for session records,
closed-session tombstones and cached response bytes. Sessions are not evicted
silently. A replicated close-session operation retains a tombstone so delayed
requests cannot recreate a closed session or execute again. A replicated epoch
advance, permitted only after operators have resolved outstanding old-session
outcomes, invalidates every request from the old epoch and permits the bounded
session table and tombstones to be reclaimed. Configuration must bound active
sessions, tombstones, cached reply bytes and maximum request/response sizes;
when a bound is reached, new sessions or writes fail closed until safe cleanup.
The optional state-machine contract must provide a pre-append upper bound for
the cached response. Admission reserves that space before logging; an
application response that exceeds its declared bound is a contract violation.

## Required deterministic tests

- Commit a write, drop its reply, retry after leader change, and assert the
  application runs once and the original response is returned.
- Retry the same identity and digest while the first copy is uncommitted on a
  former leader; deliver both log copies and assert only one application call.
- Reuse the latest sequence with a different payload and verify conflict with
  no state mutation.
- Retry after restart and after snapshot compaction; verify the saved response
  and high-water mark survive both.
- Reject gaps, stale sequences, unsupported peers, and session-cache exhaustion
  before appending a new command.
- Close a session through replicated state, restart, and verify old IDs cannot
  execute again.

## Implementation order

1. Define versioned request and response types with strict validation. Keep
   legacy requests unchanged and reject the new type until the durable contract
   is available. (The frame types and fail-closed response have landed.)
2. Add the optional idempotent state-machine contract and versioned session
   snapshot codec with strict limits. (The contract and codec have landed.)
3. Carry identity through negotiated forwarding, wrap commands in replicated
   log metadata, and apply duplicate checks at the state-machine boundary. (The
   digest, strict log-record codec and committed replay hook are present;
   request routing and peer capability negotiation remain open.)
4. Add end-to-end loss, retry, leadership-change, restart, compaction and
   resource-limit tests before advertising the capability.

Until those steps land, clients must treat a nonzero error or timeout after
admission as an unknown write outcome and must not blindly retry a
non-idempotent operation.
