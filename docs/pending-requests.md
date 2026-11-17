# Pending request lifecycle: v0.1.0-alpha.5

This milestone prevents pending writes from receiving a replacement command's
reply after leadership changes. It also releases disconnected clients and
unused application replies, and makes pre-election requests progress when the
local node becomes leader.

## Reproduced failure

A leader accepted an uncommitted command at index 3. After a partition, another
leader replaced that index with a different command and committed it. When the
old leader rejoined, its request processor returned the replacement command's
successful reply to the original client. Requests and application replies were
matched only by index, with no leadership term or lifecycle validation. A
replacement no-op could instead leave no application reply and hit an assertion.

## Completion and cancellation contract

Local writes, default leader reads and strong reads now belong to the leadership
term in which they were admitted. Losing leadership or changing term rejects
pending operations with ErrorCode=1 and the original cookie. Re-election cannot
redeem an earlier term's request. Pruning runs before admission, application,
completion and leader-wait queue processing; unchanged terms/roles avoid rescans.

Write application replies are retained only while a matching local request is
pending. Both index and term must match before a success is returned. Replies
for follower/background application are released immediately. Completion,
leadership cancellation and disconnect cleanup remove cached replies and their
pending-write records.

## Bounded request admission

Each `TRequestProcessor` now admits at most 1024 pending client requests and
64 MiB of their request frames by default. This budget is shared across ordinary
and strong reads, writes, requests waiting for leader discovery, and forwarded
requests. `TRequestProcessor` and `TRaftServer` constructors accept smaller or
larger limits for deployments.
Requests over either limit receive ErrorCode=3 (`Overloaded`) before
application validation, append or forwarding, so that response means the
request was not admitted.

Cached local write replies have a separate 64 MiB default limit. If a committed
write's application reply cannot fit, the request processor drops that cached
result and returns a small ErrorCode=1 response when completing the waiter; the
command has already applied, so its outcome is unknown to the client.
Admissions are released on completion, forwarding response, role cancellation
and disconnect cleanup.

These limits bound request-processor memory. They do not bound the persistent
uncommitted Raft log: a write already appended remains in the log after its
client disconnects. A separate uncommitted-log budget is still needed to bound
storage growth during quorum loss.

Cancellation concerns the response, not the replicated command. Committed
commands still apply even if their client disconnected or their leader changed.
An error can therefore mean UNKNOWN OUTCOME: the write may already have
committed. It is not a rollback indication and must not trigger a blind retry of
a non-idempotent operation. Request IDs and replicated deduplication are not
implemented. The KV, SQL and debug clients now display errors rather than
printing a successful commit for nonzero ErrorCode responses.

CleanUp removes a disconnected endpoint from ordinary, strong-read and
leader-wait queues, plus forwarded-request routing. Disconnect cleanup sends no
reply and does not retract entries already appended to the log.

Explicit stale reads are independent of leadership terms and target the known
committed prefix, rather than an uncommitted tail. Default reads remain weaker
than strong reads but are cancelled if their admitting leadership ends. FIFO
queueing can still delay a stale read behind an earlier pending request.

## Leader discovery and timer progress

A newly elected leader records its own ID. Higher-term stepdown clears stale
leader routing until AppendEntries identifies the new leader. Requests waiting
for leader discovery can execute locally after election, including a single-node
election. Invalid or self-directed follower routes remain queued rather than
asserting or dereferencing a missing peer.

The server's idle timer now runs request-state checks, committed application and
completion processing after Raft timeouts, using the same ordering as inbound
events. Progress therefore does not require an unrelated incoming message.

## Validation and self-review

Twelve new deterministic tests cover replacement by another command or no-op,
stepdown before application, already-committed but unanswered writes, re-election
without intermediate application polls, default reads of lost tails, requests
queued before single/three-node elections, disconnects from all local queues,
cached reply release, unused follower replies and stale reads of a committed
prefix. Weak references verify ownership is actually released. The initial ten
tests all fail against alpha.4.

All 81 tests pass locally under ASan/UBSan, with core Debug/Release verification.
Hosted CI executes the same targets; its results are separate from local checks.
The wire format and public request-processor methods are unchanged.

The bounded-admission follow-up adds deterministic count and byte rejection,
completion/disconnect release, forwarded-route release and cached-reply budget
coverage. The focused correctness and forwarding targets pass with 40 and 16
tests respectively.

Self-review checked index-and-term matching, reply-cache bounds, cancellation
before log application, disconnect ownership, leader routing and timer/inbound
ordering. Access must remain serialized and callbacks must not re-enter the
processor. No independent reviewer approval is claimed.

## Remaining gaps

Exactly-once retries, durable request IDs, forwarded-request lifecycle across
leader failures, request deadlines, an uncommitted-log budget, transport
validation and broader generated history/linearizability testing remain open.
This change does not promise successful completion during quorum loss or
establish library-wide linearizability. Unanswered writes after process crashes
also have unknown outcomes.
