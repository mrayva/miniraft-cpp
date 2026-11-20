# Repository review, 2026-11-17

Reviewed baseline: `b519756` on `work/stable-snapshot-membership-install`.
This is a source review of the first-party core, storage, transport, examples,
build setup, tests and documentation, with targeted reproduction. It is not an
exhaustive proof of consensus safety or a review of every dependency. CodeGraph
failed twice with an internal module error; direct source inspection was used.
The full build and all 21 CTest targets passed at this same commit in the
preceding validation. Follow-up fixes below are recorded separately from that
baseline.

## Findings

### P1: endpoint parser truncates IDs and can form an invalid input range — fixed

`miniraft/net/server.h:102-110`: after consuming the port, `THost` reuses the
port delimiter offset as the length of the remaining ID. Extracting the current
struct unchanged and compiling a small C++ program confirmed that
`THost("localhost:80:12345").Id` is `12`, rather than `12345`.
If the ID is shorter than the port, the end pointer can extend beyond the
string. Missing delimiters also make the pointer calculation invalid.
Neither numeric parse checks its error or verifies full consumption.
This affected example endpoint configuration, including the advertised full
32-bit node-ID support. The working tree now parses the actual port and ID
fields, rejects malformed numeric input and invalid ranges, and accepts
bracketed IPv6 addresses. A regression covers unequal field lengths, full-width
IDs, missing fields, overflow, invalid ports and trailing garbage.

### P1: outbound queues grow without bounds while a peer is unavailable — fixed

Each `TNode` now bounds pending data traffic to 128 messages / 16 MiB and Raft
control traffic to four messages / 64 MiB plus 4 KiB. Queued requests are
coalesced by traffic class; AppendEntries and snapshot chunks share the
replication class. The accounting includes messages detached into the active
writer batch. Raft peer messages use `TrySend` and rely on their protocol retry
paths after rejection. Forwarding uses `TrySend` to reject the specific command
with the existing error response and remove its route. Direct `Send` reports
overflow with `TNodeQueueFull`, and the server's periodic task survives it.
Deterministic regressions cover disconnected queue count and byte caps, retry
coalescing, and rejected forwarded work. These are per-peer bounds; aggregate
memory still scales with the number of peers.

### P1: pending client work needs bounded lifetime and admission — partially fixed

`miniraft/raft.h:334-418` and `miniraft/raft.cpp:2540-2940`: waiting reads,
writes, requests awaiting a leader and forwarded requests now share default
limits of 1024 requests and 64 MiB of request frames per processor. A separate
64 MiB cap bounds cached local write replies. Requests rejected at admission
receive ErrorCode=3 before application validation, append or forwarding. The
limits are configurable at processor/server construction and are covered across
write, leader-wait and forwarded paths.

Pending operations now expire after a configurable 30 seconds by default with
ErrorCode=4, releasing local waiters, forwarded routes and cached replies. The
server uses its monotonic timer even when traffic is idle. Forwarded deadlines
are local because the wire request has no deadline field, so a remote leader
may still apply an already-forwarded write after the origin times out. A
disconnected client's already-appended write likewise remains in the durable
log. Follow-up work also bounds each node's uncommitted suffix to 256 MiB /
65,536 entries by default,
reserves one no-op entry for control progress, rejects over-budget local writes
as Overloaded, and makes followers reject oversized AppendEntries batches before
mutation. Repeated uncommitted terms can consume the finite control reserve;
recovered suffixes above the configured cap are retained and prevent further
proposals. This bounds storage but does not provide a recovery protocol for a
saturated log. A timed-out or cancelled write still has an unknown outcome.

### P2: readiness omitted exhausted log capacity — fixed

`TJointConfigurationReadiness` now reports `HasLogCapacity`, and `Ready()`
requires it. The check accounts for the joint entry, stable successor, control
reserve, configured log budget and available indices. While a joint transition
is pending, normal proposals and a leader no-op preserve the stable successor's
capacity; finalization consumes it. Deterministic tests cover exhausted index
space, insufficient transition budget and successful finalization under an
exact reservation. The report remains point-in-time; callers must still handle
capacity changes before proposing.

## Membership and snapshot limits still present

- Endpoints are provisioned separately on every replica. The readiness report
  exposes missing local targets and proposal capacity, but there is no
  cluster-wide endpoint rollout protocol or automated administrator workflow.
  For newly added voters, leader readiness also requires a successful
  AppendEntries reply to the latest replication request at the current commit
  boundary, plus InstallSnapshot capability.
- `TRaft::Compact` requires every active voter to acknowledge the committed
  configuration (`raft.cpp:2122-2132`). An offline active voter can block
  compaction although a quorum can continue committing; the retained log grows.
  A deterministic end-to-end regression now covers the safe operational path:
  remove the unreachable voter using joint consensus, compact under the new
  stable set, restart the stale durable voter before it reconnects, then install
  the anchored snapshot and restart it again as a learner. The guard remains
  necessary while that node is still an active voter.
- Snapshots are complete in-memory images, capped at 64 MiB including metadata
  (`snapshot.h`). Outbound transfer encodes each chunk directly from the pinned
  snapshot. Both durable-state and standalone-store publication encode through
  fixed 64 KiB buffers, so neither allocates a second serialized snapshot
  image. Durable state still rewrites the snapshot and retained log on each
  state update. KV and SQL can now preflight an application size bound against
  the metadata-adjusted limit before capture; legacy applications still receive
  the post-capture check. Application capture, receiver chunk accumulation,
  and restore retain complete images. The receiver
  reuses its completed wire buffer for decoded application data, avoiding an
  extra payload allocation during validation. Large databases still need an
  application and storage streaming contract with disk quotas and generation
  recovery.
- Recovery history is bounded to 1,024 transitions in MRFTS010/MRFTS011;
  MRFTS008/MRFTS009 cap at 256 and MRFTS006/MRFTS007 at 32. Automatic
  stale-store recovery still requires locally anchored history and the sender
  membership conditions documented in README and configuration-entries.md.
  Disjoint or out-of-window histories can require a fresh learner baseline or
  independently verified recovery history. Joint consensus itself is
  implemented, including disjoint transitions; unrestricted automatic stale
  recovery is a separate capability.
- Legacy migration requires a trusted complete original voter set and commit
  boundary. The new offline converter handles un-compacted split `TDiskState`
  logs and preserves the source files; its dry run checks consistency, not
  historical voter-set completeness or the application's commit boundary.
  Retained configuration entries are checked against the supplied baseline.
  An explicit durable-state binding path accepts configuration snapshots only
  when their complete transition chain starts from that baseline; truncated
  history and the caller-supplied commit boundary still require external proof.

## General production gaps

- Pre-vote elections are now implemented. Grants are non-persistent, use the
  active stable or joint quorum, and are suppressed until the recent leader
  contact deadline passes. The deterministic suite covers high-term isolated
  candidates and stale campaign responses. This reduces term disruption; it
  is not a lease, and the new RPC types require all voters to upgrade together.
- Quorum-contact leadership expiration is now implemented in LeaderTimeout.
  It uses the active stable or joint voter quorum and a configurable timeout
  that defaults to 20 seconds. This is not a lease and does not guarantee there
  is no overlap with a newly elected leader. Default leader reads are weaker
  than consistent reads.
- TDurableState synchronously serializes and republishes the snapshot and
  retained log on every update (`durable.cpp:245-298`). There is no efficient
  WAL/group-commit path; disk work blocks the event loop. Legacy TDiskState
  still lacks the same power-loss durability guarantees.
- Transport validates claimed source IDs, but incoming connections are not
  bound to authenticated Raft identities (`server.cpp:13-30`,
  `raft.cpp:1364-1368`). A reachable untrusted client can claim a configured
  peer ID. TLS configuration and peer authorization need a separate security
  design; enabling encryption alone does not establish that binding.
- No replicated client deduplication/session protocol. Retrying a write after
  an uncertain outcome can execute it again. Correlation cookies do not provide
  durable operation identity. Forwarding uses negotiated sessionized
  correlation IDs to reject delayed replies across processor instances within
  one process; these IDs do not deduplicate retried writes. Custom adapters
  still need fresh transport sessions across process restarts.
- Server lifecycle lacks a general stop/cancel/join API for its detached
  accept, timer and connection coroutines. Peer retirement addresses a narrower
  lifecycle. Reusable embedding needs explicit shutdown ownership.
- Wire messages use native C++ layouts without a general negotiated protocol
  version/endian encoding. Snapshot versions are explicit, but mixed-version
  upgrades need a compatibility policy and integration coverage.
- Existing deterministic regressions are useful; generated partition/crash/
  membership schedules, model/history checking, broad parser fuzzing, workload
  benchmarks and operational metrics remain gaps. Current CI includes Debug,
  Release and ASan/UBSan jobs; it does not establish those additional claims.
- Follow-up: CMake now installs and exports the core library as
  `miniraft::miniraft`, including public headers and its C++20 requirement. A
  Release install and independent downstream configure/build were verified.
  The optional built-in network server is not part of this package. Several
  milestone documents describe historical limits that later work superseded;
  a single current capability/recovery matrix would help.

## Comparison with NuRaft

NuRaft's current README documents core replication, snapshots, dynamic
membership, pluggable storage/state machines, group commit and pipelined writes.
It also lists pre-vote, leadership expiration, learner roles, logical snapshots,
parallel log appending, custom quorum/commit policies and streaming mode.
Miniraft now covers core guarded membership and snapshot functionality, but
has narrower recovery, resource management and operational interfaces.

NuRaft's streaming mode explicitly provides log-gap and in-flight byte caps;
its logical snapshot callbacks permit application-defined objects; and its
configurable leadership expiry can step down after missing quorum responses.
MiniRaft now has pre-vote and configurable leadership expiration. Custom
quorum policies, asynchronous replication and election priorities should follow
the correctness/resource priorities: they add semantics and are not
prerequisites for a robust standard Raft core.
NuRaft's user-defined log-store support is not itself a guarantee that any
particular storage implementation is durable.

Sources consulted:

- https://github.com/eBay/NuRaft
- https://github.com/eBay/NuRaft/blob/master/docs/prevote_protocol.md
- https://github.com/eBay/NuRaft/blob/master/docs/leadership_expiration.md
- https://github.com/eBay/NuRaft/blob/master/docs/snapshot_transmission.md
- https://github.com/eBay/NuRaft/blob/master/docs/streaming_mode.md

Recommended order: fix endpoint parsing; bound transport and client admission;
then address WAL performance, large snapshots, membership administration and
generated failure testing. Authentication and shutdown are necessary before
exposing this as a general production service. No new consensus safety
violation was established in this review; no independent reviewer approval is
claimed.
