# Repository review, 2026-11-17

Reviewed baseline: `b519756` on `work/stable-snapshot-membership-install`.
This is a source review of the first-party core, storage, transport, examples,
build setup, tests and documentation, with targeted reproduction. It is not an
exhaustive proof of consensus safety or a review of every dependency. CodeGraph
failed twice with an internal module error; direct source inspection was used.
The full build and all 21 CTest targets passed at this same commit in the
preceding validation. This review did not rerun that suite or change runtime code.

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

### P1: pending client work has no admission or deadline budget

`miniraft/raft.h:364-386` and `miniraft/raft.cpp:2568-2698`: waiting reads,
writes, requests awaiting a leader and forwarded requests have no aggregate
byte/count limit or request deadline. Role changes and disconnects clean up
some requests, but an open connection with an unchanged role can keep requests
alive indefinitely. Quorum loss plus continued admission can exhaust memory;
the leader may also keep growing its uncommitted durable log.
Add admission limits before append/forwarding, request deadlines, and explicit
overload/unknown-write-outcome responses. This is distinct from per-frame limits.

### P2: readiness omits exhausted log capacity

`miniraft/raft.cpp:1806-1838,1847`: the report has no capacity condition, while
the proposal rejects `LastLogIndex >= INT64_MAX`. At that boundary, a removal
with otherwise satisfied prerequisites can report Ready and then always fail.
Add a capacity field to the report or narrow its documented meaning. Test a
synthetic exhausted-index store. This is an API diagnostic inconsistency, not
evidence that an unsafe proposal is accepted.

## Membership and snapshot limits still present

- Endpoints are provisioned separately on every replica. The new report exposes
  missing local targets, but there is no cluster-wide endpoint rollout protocol
  or automated administrator workflow.
- `TRaft::Compact` requires every active voter to acknowledge the committed
  configuration (`raft.cpp:2122-2132`). An offline active voter can block
  compaction although a quorum can continue committing; the retained log grows.
  Relaxing this needs a safe recovery design, not simply removal of the guard.
- Snapshots are complete in-memory images, capped at 64 MiB including metadata
  (`snapshot.h`). Transfer is chunked, but capture, encoding and installation
  still materialize the whole image. Large databases need streaming or logical
  object snapshots and resource accounting.
- Recovery history is bounded to 32 transitions. Automatic stale-store recovery
  requires a locally anchored history and the sender membership conditions
  documented in README and configuration-entries.md. Disjoint or out-of-window
  histories can require a fresh learner baseline or independently verified
  recovery history. Joint consensus itself is implemented, including disjoint
  transitions; unrestricted automatic stale recovery is a separate capability.
- Legacy migration still requires a trusted complete original voter set. The
  dry run validates consistency, not historical completeness. Split legacy
  TDiskState files have no general offline conversion tool.

## General production gaps

- No pre-vote RPC phase: timeout increments the durable term directly
  (`raft.cpp:1690-1714`). Rejoining partitioned voters can disrupt a healthy
  leader through inflated terms.
- No quorum-contact leadership expiration in LeaderTimeout. An isolated leader
  can retain its role and keep accepting work. Consistent reads still require
  fresh quorum evidence, so this does not by itself establish a strong-read
  safety bug. Default leader reads are weaker than consistent reads.
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
  durable operation identity. Forwarding cookies also stop being allocated
  after UINT32_MAX during a processor's lifetime.
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
- CMake builds the library but has no first-party install/export package
  contract. Several milestone documents describe historical limits that later
  work superseded; a single current capability/recovery matrix would help.

## Comparison with NuRaft

NuRaft's current README documents core replication, snapshots, dynamic
membership, pluggable storage/state machines, group commit and pipelined writes.
It also lists pre-vote, leadership expiration, learner roles, logical snapshots,
parallel log appending, custom quorum/commit policies and streaming mode.
Miniraft now covers core guarded membership and snapshot functionality, but
has narrower recovery, resource management and operational interfaces.

NuRaft's streaming mode explicitly provides log-gap and in-flight byte caps;
its logical snapshot callbacks permit application-defined objects; and its
leadership expiry can step down after missing quorum responses. Those are
concrete comparison targets. Custom quorum policies, asynchronous replication
and election priorities should follow the correctness/resource priorities:
they add semantics and are not prerequisites for a robust standard Raft core.
NuRaft's user-defined log-store support is not itself a guarantee that any
particular storage implementation is durable.

Sources consulted:

- https://github.com/eBay/NuRaft
- https://github.com/eBay/NuRaft/blob/master/docs/prevote_protocol.md
- https://github.com/eBay/NuRaft/blob/master/docs/leadership_expiration.md
- https://github.com/eBay/NuRaft/blob/master/docs/snapshot_transmission.md
- https://github.com/eBay/NuRaft/blob/master/docs/streaming_mode.md

Recommended order: fix endpoint parsing; bound transport and client admission;
add deadlines and leadership expiration; implement pre-vote; then address WAL
performance, large snapshots, membership administration and generated failure
testing. Authentication and shutdown are necessary before exposing this as a
general production service. No new consensus safety violation was established
in this review; no independent reviewer approval is claimed.
