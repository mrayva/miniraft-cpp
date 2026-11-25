# Forwarded request lifecycle: v0.1.0-alpha.6

This milestone separates client cookies from forwarding correlation and gives
forwarded attempts an explicit leader/term/peer lifecycle.

## Reproduced failures

Followers previously interpreted every nonzero client cookie as an already
forwarded request and rejected it. For zero-cookie requests they replaced the
cookie with an internal routing token, then returned that token to the client
instead of restoring zero. They also mutated the caller's request buffer and
retained forwarding routes when terms/roles changed, allowing late replies to
complete obsolete attempts.

## Routing contract

EForwarded is a dedicated internal request flag. Clients may use any uint32_t
cookie, including zero and UINT32_MAX. A follower copies a request and sets
this one-hop flag. Its internal correlation IDs are monotonic uint64_t values
and are never reused within a processor. Peers that negotiated sessionized
forwarding use message types 19 and 20 for every correlation. Otherwise, IDs
through UINT32_MAX use the legacy command message and larger IDs use the
extended forwarded-command request and response types. A forwarded request
that reaches another follower fails instead of being relayed again.

The routing record holds the original cookie, client endpoint, target peer,
leader ID and origin term. Responses are accepted only from that configured
outbound peer. Before delivery, the route is removed and the response is copied
with the original cookie restored. Caller-owned request/response buffers are
not changed. Duplicate, unknown, wrong-peer and unattributed responses cannot
complete another attempt.

Routes are rejected with ErrorCode=1 and their original cookie when the origin
changes term, leaves follower state, observes a different leader, or receives a
peer-unavailable notification. Client disconnect removes only that client's
routes silently. Late responses for removed routes are ignored. Correlation IDs
are never reused within a processor, even after success, cancellation or peer
reconnection. Forwarding fails safely only after the full 64-bit sequence is
exhausted.

Cancellation is local bookkeeping. A write already sent or buffered by the
transport may still execute. Ordinary forwarded `TCommandRequest` writes have
no automatic retry, rollback or exactly-once execution guarantee. ErrorCode=1
can indicate an unknown write outcome, so non-idempotent writes must not be
blindly retried. The separate opt-in idempotent request path supports retrying
the same operation identity and payload under the requirements in
[durable client write deduplication](client-idempotency-design.md); it does not
change the semantics of ordinary forwarded requests.

## Transport integration and compatibility

Custom adapters must call OnCommandResponse(response, peer), passing the same
configured INode object used for forwarding, and call OnPeerDisconnected(peer)
when the outbound connection is reported unavailable. The old one-argument
response call still compiles but intentionally ignores unattributed responses.
The included server supplies peer context and reports outbound read/unavailable
errors. Repeated failure notifications are idempotent.

Configured-peer context is a routing check, not cryptographic authentication or
Byzantine fault tolerance. Untrusted inbound client sockets are not accepted as
the outbound peer that owns a response. Write-side failure notification timing
still depends on the transport detecting/reporting connection loss; blackholed
connections without such notification have no deadline here.

Existing client and Raft message layouts are unchanged. New outbound peer
connections negotiate forwarding features before message queues are drained.
Peers advertising sessionized-forwarding support use message types 19 and 20
from the first forwarded request; each reply carries a process-wide monotonic
processor-session ID as well as its correlation ID. This prevents delayed
sessionized and legacy-format replies from a previous processor in the same
process from matching a new request after its correlation counter restarts.
Peers advertising only the earlier extended-forwarding feature use message
types 15 and 16 after UINT32_MAX. Legacy peers use the original command
messages through UINT32_MAX.

A legacy peer rejects the initial capability probe; the connector retries once
without a probe and keeps using the legacy wire format for that connection. The
next reconnect probes again. Requests queued during the rejected probe
connection may receive Failed and must be retried; no forwarded command is
written on that probe connection. Correlations beyond UINT32_MAX fail safely
when the active leader has not advertised extended-forwarding support.

Custom transports must implement the peer-capability exchange and report the
negotiated forwarding features through INode. Sessionized generations are
process-local; built-in peer connections are recreated after process restart,
and custom transports must use fresh sessions across restarts. Sessionized
request and response headers add 16 bytes; the earlier extended headers add 8
bytes. Requests that cannot fit the selected envelope are rejected as
overloaded, and responses that cannot fit are returned as failed.
EForwarded reserves flag value 0x08.

## Validation and self-review

Twelve forwarding tests cover full-range client cookies, zero-cookie restoration,
request-copy/one-hop semantics, out-of-order and duplicate replies, term and
candidate transitions, stale replies arriving before an application poll,
selective client disconnect, leader discovery, wrong/missing peer context,
error propagation and peer failure/reconnection with late replies. Nine of the
initial ten tests fail against alpha.5; the selective-disconnect baseline passes.

An additional three-node deterministic test forwards a write and strong read
through actual Raft replication/application, including reversed and duplicate
consensus traffic. Together with existing tests, all 94 pass locally under
ASan/UBSan, with core Debug/Release verification. Hosted CI executes the same
targets; its results are separate from local checks. There are no sleeps or
wall-clock dependencies in the new tests.

Self-review checked original-cookie restoration, copy ownership, distinct token
allocation, route invalidation before response delivery, source-peer matching,
disconnect selectivity and adapter call sites. No independent approval is
claimed. Access remains serialized; callbacks must not re-enter the processor.

## Remaining gaps

Durable client operation IDs/deduplication, transport-session identity across
process restarts for custom adapters, broader transport validation and
generated history/linearizability testing remain open. This change does not
prove library-wide linearizability or guarantee completion during quorum loss.
