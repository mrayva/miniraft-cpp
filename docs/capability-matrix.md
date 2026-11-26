# Current membership and snapshot capabilities

This page summarizes the current implementation and its operational limits.
It supersedes the milestone-by-milestone status notes where those describe an
earlier stage. Detailed behavior and format compatibility remain in the linked
design notes. This is a capability summary, not a production-readiness claim.

## Membership

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Stable and joint quorums | Elections, commit advancement and strong-read confirmation use strict majorities of both voter sets while a joint configuration is active. | Every voter in the proposed stable set, including retained and added voters, must advertise joint-consensus support in a current-term AppendEntries response. Removed old voters still count toward the old-set majority, but need not be online or advertise features to start removal. Endpoints for both sets remain required until the stable successor commits. See [joint quorum](joint-quorum.md) and [live membership](configuration-entries.md). |
| Add or remove voters | `ProposeJointConfiguration()` starts a transition; after it commits, `FinalizeConfiguration()` commits the stable successor. Overlapping and disjoint voter sets are supported. | Live transitions require `TDurableState` or versioned `TDiskState` metadata with a persisted original voter binding and commit index, a current-term committed entry, and endpoints on every replica for the active and proposed voters. New voters must first be staged as learners and acknowledged through the leader's committed boundary. Existing split-format stores need trusted voter and commit baselines before startup. The full-history binding path accepts retained configuration entries and compacted snapshots only when their history proves continuity from the supplied original voter set. See [online learner staging](online-learner-staging.md) and [trusted legacy voter binding](trusted-legacy-voter-binding.md). |
| Remove the current leader | A bounded leadership transfer can move leadership to a caught-up voter in the new set after the joint entry commits. | The target must be in the new voter set and must still win the active joint quorum. See [leadership transfer](leadership-transfer.md). |
| Endpoint rollout and readiness | Readiness reports local missing targets, leader-observed learner acknowledgements, capability gaps for joint consensus, storage snapshot installation and application snapshot restore, and transition log capacity. Learner catch-up requires a successful response to the latest AppendEntries request. Server peer readers follow voter role changes. `TRaftServer::ReplacePeerTarget()` can replace one local outbound endpoint without changing its voter or learner role. | Endpoint registration and replacement are local to each process. Durable KV servers can load the same catalog via `--peer-config`. Linux KV servers can reload learner additions and remote endpoint replacements on `SIGHUP`. Replacement retires the old response reader and discards that peer's progress, acknowledgement, and capability evidence; fresh responses are required. Apply endpoint changes on every replica, check readiness on every replica, and check again on the prospective leader immediately before proposing. Proposed voters must advertise application snapshot restore support; newly added voters must advertise both storage and application InstallSnapshot support. A new leader needs a locally registered learner target and must reestablish its catch-up proof. There is no cluster-wide administrator or rollout API, and the server does not update its own listener address. See [server membership reconciliation](server-membership-reconciliation.md). |
| Retiring a removed peer | A target can be removed after the stable successor commits. Native TCP and sockets with a safe shutdown hook can be retired while I/O unwinds. | Some socket wrappers, including the current raw TLS wrapper, cannot be safely retired after a response reader starts. Keep those endpoints until server shutdown or use the retirable TLS wrapper. |

## Snapshots and recovery

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Application checkpoints | KV and SQL provide snapshot callbacks and size bounds. SQL implements `IStreamingSnapshotRsm` for range-readable capture and `IStagedSnapshotRsm` to reuse its validated SQLite image across durable publication and activation. Capture rejects a known over-limit image before retaining its source, and restore validates complete application images. | The core streaming capture and manifest path supports versioned images up to 256 MiB, subject to configured limits and resource budgets; the default per-node limit is 128 MiB. KV remains capped at or below 128 MiB; SQL source-backed capture and installation can exceed 128 MiB up to the streamed-image ceiling, while its string-based compatibility APIs remain capped below 128 MiB. Another RSM must implement and budget its larger-image validation and restore path. Raft retains a range source directly instead of copying it into a complete payload string; it charges source size by default, while `ISnapshotSourceHeapUsage` lets a source report its retained heap bound. Memory-backed snapshots charge retained string capacity. The same accounting applies to capture and startup recovery. Original formats, `EncodeSnapshot()`, the non-owning `DecodeSnapshot()` view, and legacy chunk RPCs remain capped at 128 MiB; peers without size negotiation retain the 64 MiB limit, and images above 128 MiB require manifest-transfer support. The all-voter compaction barrier certifies the active limit. Legacy applications without a size bound retain the post-capture check. Durable publication still copies ranges into its own snapshot generation. The dummy application has no snapshot implementation. Capture, storage encoding and restore can block the event loop. See [snapshot foundation](snapshots.md), [SQL snapshots](sql-snapshots.md), and [streamed snapshot design](snapshot-streaming-design.md). |
| Log compaction | `TState`, Linux `TDurableState` and `TDiskState` publish a snapshot with the retained suffix and hide the covered prefix from Raft. `CheckCompactionMembershipReadiness()` reports missing storage and application snapshot-install capabilities, committed-configuration acknowledgements, and follower barrier state, plus whether a follower has received the leader's barrier. | MRFTD008 binds the encoded snapshot length and SHA-256 digest to the manifest. By default its immutable sidecar contains the snapshot payload; with an application-generation resolver, `TDurableState` validates the referenced generation and stores only snapshot metadata in the sidecar. The reference is atomic with the snapshot boundary, membership metadata, commit index and retained suffix. `TDiskState` still stores the full payload in its sidecar. Ordinary durable tail appends write only new log records and the small manifest; suffix replacement and compaction publish a copy-on-write log generation. Recovery removes matching regular orphan sidecars, log generations and incomplete temps after validating the selected manifest. If a sidecar is present without its manifest, startup rejects fresh initialization rather than risk resetting migrated state. Fresh `TDiskState` snapshot generations compact the active entries file and reclaim the prior generation after metadata publication. MRFTL006 adds the same optional application-generation reference to its metadata; MRFTL001 through MRFTL005 remain readable. A leader blocks compaction until every remote active voter positively advertises storage and application snapshot-install support; a never-seen or explicitly unsupported voter blocks it. At membership-bearing boundaries, every active voter must also acknowledge the committed configuration. Followers, including on the zero-index initial voter baseline, require a current-term all-voter capability barrier. Storage or application snapshot failures can still reject `Compact()` after preflight. Safely remove an offline voter through joint consensus first. See [compaction](compaction.md). |
| InstallSnapshot | Leaders send bounded, stop-and-wait chunks after the target advertises backend and application restore support; followers validate the image, boundary and application payload before durable installation and acknowledgement. `TState`, Linux `TDurableState` and `TDiskState` support installation. | `TDiskState` atomically publishes a compacted entries generation, retained-suffix index and snapshot before switching metadata. The negotiated manifest path supports versioned images up to 256 MiB, subject to configured limits and resource budgets. Legacy complete-image transfer remains capped at 128 MiB; peers without size negotiation are limited to 64 MiB. Linux receivers can optionally stage full transfers in anonymous files before decoding them into memory; in-memory assemblies, covered-boundary prefixes, final decoding, fallback source materialization, retained snapshots and live outgoing InstallSnapshot frames share a 256 MiB snapshot-heap category by default. Outgoing frame leases follow each message holder through custom and built-in transports. Accounted `TRaftServer` inbound and outbound queue buffers use separate categories under the same 1 GiB process root by default, with category limits and reserved headroom. Memory-backed strings are charged by retained capacity. File-backed sources can report zero payload heap use; unmarked sources are charged by size. Staging files have a separate 256 MiB budget. Chunks are capped at 64 KiB. A voter with no positive capability acknowledgement blocks leader compaction until support is advertised or the voter is safely removed. Partial transfers are not persisted and restart from offset zero. A correlated rejection invalidates cached peer capabilities; the leader probes the compacted boundary with AppendEntries before retrying the image. Custom RSMs default to unsupported and must opt in through `SupportsApplicationSnapshotInstall()` after implementing correct atomic image validation and restore. See [snapshot pin and receive budgets](snapshot-pin-budget.md) and [compaction](compaction.md). |
| Membership-aware recovery | Stable and joint snapshots preserve configuration identity and bounded transition history. Recovery and installation validate the receiver's committed history before accepting membership metadata. | AppendEntries feature bits negotiate lineage support and the 32-, 256- and 1,024-transition MRFTS006/007, MRFTS008/009 and MRFTS010/011 formats. A lineage-capable peer without history support receives predecessor-only MRFTS004/MRFTS005. A peer without lineage receives MRFTS003 stable or MRFTS002 joint identity metadata without transition history. MRFTS002 requires the exact committed joint configuration. MRFTS003 can match a committed stable configuration or complete the stable successor of the receiver's exact committed joint configuration when the sender belongs to the new set. Fresh learners use a separate constrained bootstrap path. These formats cannot recover a stale store across unknown membership changes. A stale store needs an exact trusted anchor within transferred history, and the sender must satisfy voter-set lineage checks. Disjoint or out-of-window history may require a fresh learner baseline or independently verified history. Snapshot CRC and metadata do not authenticate a sender or prove commitment. See [configuration entries](configuration-entries.md) and [durable configuration snapshots](durable-configuration-snapshots.md). |
| Fresh learner bootstrap | An empty learner can install a stable or joint configuration-bearing snapshot and remains a learner afterward. | The learner must have no prior log, commit, snapshot, vote or application progress, and its configured voter set must exactly match the stable set or joint union. Existing voters and nonempty stores require matching committed history. See [online learner staging](online-learner-staging.md). |
| Legacy-store migration | An offline converter plus durable-store and split-store voter-binding CLI modes are available. Both binding modes support dry runs and trusted-history validation; split-store binding requires an explicit commit index. | Old durable and 20-byte `TDiskState` metadata cannot establish the complete original voter set or commit boundary; supply these from independently trusted records. Baseline binding cannot validate retained configuration entries or configuration-bearing snapshots; those need a complete history anchored to the original voters. The offline CLI can bind a split store directly when that history remains available. See [legacy disk-state migration](legacy-disk-state-migration.md) and [trusted legacy voter binding](trusted-legacy-voter-binding.md). |

## Operational sequence for a voter change

1. Upgrade every voter to a compatible build and snapshot format.
2. Register replication targets on every replica for all current and proposed
   voters. Stage new nodes as learners.
3. Check readiness on every replica. On the leader, wait for each added voter
   to acknowledge through the committed boundary and advertise InstallSnapshot
   support; recheck immediately before proposing.
4. Propose the joint configuration and wait for both majorities to commit it.
   If removing the leader, transfer to a caught-up voter in the new set.
5. Commit the stable successor. Only then retire removed endpoints.
6. Compact after the active voters have acknowledged the committed
   configuration. If an offline voter blocks the barrier, remove it safely
   before compacting; it can later rejoin as a learner using supported history.

## Remaining gaps

- Cluster-wide endpoint discovery, rollout and administrator workflow.
- End-to-end streaming or logical-object snapshots for large application state,
  plus allocator-wide accounting for application-owned state and untracked
  callback allocations. Tracked snapshot and server transport buffers now share
  a process root, but allocations outside those accounting paths remain outside
  the cap. Range-source
  capture retains the source and charges its declared heap bound, defaulting to
  payload size; incremental snapshot writers extend their lease before appending.
  Retained payloads and live InstallSnapshot frame buffers use the snapshot-heap
  category. The tracked snapshot and server transport categories share a process
  root. Legacy `SaveSnapshot()` callbacks without a bound reserve the maximum
  accepted payload before invocation and shrink the lease to retained capacity.
  Callback allocations beyond that bound, application-owned state and other
  untracked allocations remain outside it. The required
  staged-image and durable-manifest contract is outlined in [the streaming design](snapshot-streaming-design.md).
- Automatic recovery when a stale store lacks an allowed history anchor, or
  its transition chain is disjoint from the sender's voter lineage.
- An efficient WAL or cross-request group-commit storage path. `TDurableState`
  appends ordinary tail records to the selected log generation and publishes a
  small manifest. Suffix replacement and compaction publish a copy-on-write log
  generation. A state-changing follower `AppendEntries` request, including any
  commit advance it carries, is published synchronously as one operation, but
  individual proposals and commit-only advances still incur separate
  publications and syncs. There is no batching across requests.
  Legacy `TDiskState` writes append-only follower batches before one log/index
  sync and versioned metadata publication. Conflicting suffix replacements
  write a new entries/index generation and switch the retained prefix,
  replacement entries and commit boundary through one versioned metadata
  publication; the old generation remains selected until that switch. Fresh
  `TDiskState` stores checksum records; older split files and their record
  encoding remain readable after trusted-baseline migration.

The membership, compaction, transfer and legacy disk-publication paths have deterministic regression
coverage described in their design notes. Joint-election tests also enumerate
every reply ordering for overlapping and disjoint voter sets, checking each
delivery prefix before both majorities are present. This coverage does not
establish hardware power-loss behavior, authenticate peers, or replace
independent review.
