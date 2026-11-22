# Durable client write deduplication design

Status: the replicated protocol hooks, a reference KV idempotent state machine,
a client session/retry helper, and a Linux durable client-state-store adapter
are implemented. The KV example now exposes the idempotent request path over
its TCP client/server interface. MiniRaft does not provide a reusable network
client library. The sample CLI uses bounded reconnect/backoff for idempotent
writes. The opt-in
request type carries operation identity through negotiated forwarding and a
tagged replicated log record. `IIdempotentRsm` applies committed records and
captures application data, session records, epoch and progress in one bounded
snapshot. Replicated close-session and epoch-advance control records are also
available. Log-record support is negotiated; leaders reject opt-in writes until
all voters and learners that can receive the entry advertise support.

The application implementing `IIdempotentRsm` must enforce the session,
sequence, digest, tombstone and cached-response rules atomically. The current
test RSMs are protocol test doubles, not a ready-to-use implementation. A
timeout or lost response still leaves the write outcome unknown; retrying is
safe only when the application honors the operation identity contract. The
remaining end-to-end retry and recovery tests are listed below.

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

## Deterministic validation status

Focused tests now cover strict request and log-record validation, snapshot
round trips and limits, committed application replay, forwarding with
sessionized routes, timeout behavior, capability negotiation (including new
joint members), atomic control application, and control-record replay after a
durable restart. Duplicate committed idempotent records also replay after a
durable restart, applying the command once and returning the original cached
result. A deterministic three-voter scenario now commits a write while one
voter is isolated, drops the reply, compacts and installs a snapshot on that
voter, elects it in a later term, and retries the same identity. The retry
returns the saved response without another application mutation. These tests
validate the protocol boundary and storage integration with test RSMs; the
three-voter scenario uses in-memory Raft storage and does not simulate a process
restart. Another deterministic three-voter scenario leaves an operation
uncommitted on a former leader, elects the follower that holds that copy, and
retries the same identity before replicating the old suffix. Both copies then
commit, while each RSM applies the operation once and retains the original
result index. A separate replicated case reuses the committed sequence with a
different payload, confirms the conflict response, and checks that application
data and the saved session result remain unchanged. Its test RSM advances a
session through sequence 2 and rejects stale requests, skipped sequence numbers,
and a new session starting above sequence 1 before appending; the test verifies
the log and cached session remain unchanged. A request sent to a leader with a
voter that lacks idempotent-log support returns `Unsupported` without appending.
The test RSM also refuses a new session when its active-session bound is full,
before the request reaches the log. A durable single-voter case then compacts
the combined Raft/application snapshot, reconstructs the Raft and
RSM objects from disk, and retries the request. The new process returns the
cached result at its original index with the saved session high-water mark and
no second application mutation.

The durable multi-voter case also restarts all three durable stores, restores a
combined snapshot on the new leader, elects that voter in a later term, and
verifies the retry returns the original cached response. A third voter recovers
the committed application state from its durable log. The repeated request does
not cause another application mutation on either snapshot-restored replica.

A separate durable three-voter case commits a session close while one voter is
isolated, installs the resulting tombstone through snapshot transfer, restarts
all voters, and elects the snapshot-restored voter. Retrying the closed session
is rejected before append, with the tombstone and application state unchanged.

The reference `TIdempotentKvRsm` in `examples/idempotent_kv_rsm.cpp` applies
real KV writes together with session records and exercises combined snapshot
capture, restore, retry, close-session tombstones and epoch advance. Its focused
test verifies that a restored retry returns the original response without
reapplying the KV mutation, and that invalid application snapshot data leaves
the prior state intact. A Linux durable integration test stores the combined
KV/dedup snapshot, restarts the durable store, persists a duplicate committed
log record, restarts again, and verifies replay returns the cached result at its
original index. This closes the test-double-only application-contract gap for
the sample KV application. `TIdempotentClientSession` in
`miniraft/idempotent_client.h` persists each pending operation through a caller-
provided durable state-store interface before returning a request. It resends
the same identity and payload after restart and advances the sequence only
after a matching success response is durably acknowledged. `examples/kv.cpp`
can run an idempotent KV server with `--idempotent --durable` and a client with
`--idempotent --client-state <path>`. It retries an unresolved write when the
client process is launched again, including after the server applied the write
but its reply was lost. The sample also retries a lost reply within the running
process, using the same saved operation identity and a bounded reconnect/backoff
sequence. Exhausted retries leave the operation pending for a later command or
process restart. Integration with a deployment application's own durable state
and snapshot implementation remains open. The
`TDurableIdempotentClientStateStore` adapter stores one versioned, checksummed
session image per file and publishes updates through a synced temporary file,
atomic rename and directory sync. It holds an exclusive advisory lock for its
lifetime and rejects state or lock files with unexpected owners or broad
permissions. The file includes the pending payload and is created with private
permissions.
Deterministic tests inject failures at each publication stage and verify a
complete old or new state is recovered; they do not emulate hardware power loss.

## Implementation status

1. Versioned request and response types with strict validation; legacy request
   layouts remain unchanged. **Implemented.**
2. Optional idempotent state-machine contract and bounded, versioned combined
   application/session snapshots. **Implemented.**
3. Carry identity through forwarding, encode it in replicated entries, replay
   committed records through the state-machine contract, replicate session
   controls, and negotiate peer support. **Implemented.**
4. Validate lost-reply retries, duplicate log copies, leadership changes,
   restart, compaction and resource limits using a stateful deduplicating RSM.
  **Partially implemented:** lost-reply retry across snapshot installation and
  leadership change, duplicate uncommitted copies across leaders, and conflicting
  sequence reuse, stale/gap rejection, unsupported-peer rejection, active-session
  cache exhaustion and durable session close are covered with test RSMs. The
  sample KV application's real write/snapshot path is covered by
  `test_idempotent_kv`, and its disk-backed snapshot plus committed replay path
  is covered by `test_durable`. The client session/retry helper and its
  persist-before-send and restart behavior are covered by
  `test_idempotent_client`; durable client-session publication and restart are
  covered by `test_durable_idempotent_client`. The CLI network smoke test drops
  a committed response, verifies retry returns its cached result, and checks
  the combined snapshot after restart. Same-process reconnect/backoff for the
  sample CLI is covered by dropping a reply and forwarding the retry. A reusable
  client library and deployment-specific application storage integration remain
  open.

Clients using ordinary `TCommandRequest` writes retain at-least-once behavior.
For idempotent requests, a timeout after admission still has an unknown outcome;
the client must retry the same epoch, session ID, sequence and payload. This is
safe only when every receiving replica runs a conforming `IIdempotentRsm` and
the application atomically persists its deduplication state in snapshots.
