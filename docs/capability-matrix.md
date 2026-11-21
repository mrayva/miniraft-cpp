# Current membership and snapshot capabilities

This page summarizes the current implementation and its operational limits.
It supersedes the milestone-by-milestone status notes where those describe an
earlier stage. Detailed behavior and format compatibility remain in the linked
design notes. This is a capability summary, not a production-readiness claim.

## Membership

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Stable and joint quorums | Elections, commit advancement and strong-read confirmation use strict majorities of both voter sets while a joint configuration is active. | Every current and proposed voter must advertise joint-consensus support in a current-term AppendEntries response before a transition. Older binaries leave the capability bits zero. See [joint quorum](joint-quorum.md) and [live membership](configuration-entries.md). |
| Add or remove voters | `ProposeJointConfiguration()` starts a transition; after it commits, `FinalizeConfiguration()` commits the stable successor. Overlapping and disjoint voter sets are supported. | Live transitions require `TDurableState` or versioned `TDiskState` metadata with a persisted original voter binding and commit index, a current-term committed entry, and endpoints on every replica for the active and proposed voters. New voters must first be staged as learners and acknowledged through the leader's committed boundary. Existing split-format `TDiskState` stores need a trusted baseline before startup; they cannot bind if the retained log already contains configuration entries. See [online learner staging](online-learner-staging.md) and [trusted legacy voter binding](trusted-legacy-voter-binding.md). |
| Remove the current leader | A bounded leadership transfer can move leadership to a caught-up voter in the new set after the joint entry commits. | The target must be in the new voter set and must still win the active joint quorum. See [leadership transfer](leadership-transfer.md). |
| Endpoint rollout and readiness | Readiness reports local missing targets, leader-observed learner acknowledgements, capability gaps for joint consensus and InstallSnapshot, and transition log capacity. Learner catch-up requires a successful response to the latest AppendEntries request. Server peer readers follow voter role changes. `TRaftServer::ReplacePeerTarget()` can replace one local outbound endpoint without changing its voter or learner role. | Endpoint registration and replacement are local to each process; they are not replicated or distributed. Replacement retires the old response reader and discards that peer's progress, acknowledgement, and capability evidence; fresh responses are required. Apply endpoint changes on every replica, check readiness on every replica, and check again on the prospective leader immediately before proposing. Newly added voters must advertise InstallSnapshot support so they do not prevent later safe compaction. Learner replication targets must be registered again after leadership changes or restart. There is no cluster-wide administrator or rollout API, and the server does not update its own listener address. See [server membership reconciliation](server-membership-reconciliation.md). |
| Retiring a removed peer | A target can be removed after the stable successor commits. Native TCP and sockets with a safe shutdown hook can be retired while I/O unwinds. | Some socket wrappers, including the current raw TLS wrapper, cannot be safely retired after a response reader starts. Keep those endpoints until server shutdown or use the retirable TLS wrapper. |

## Snapshots and recovery

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Application checkpoints | KV and SQL provide snapshot callbacks and size bounds; capture rejects a known over-limit image before materialization and restore validates complete application images. | Images are still complete in-memory objects capped at 64 MiB, including metadata. Legacy applications without a size bound retain the post-capture check. The dummy application has no snapshot implementation. Capture, storage encoding and restore can block the event loop. See [snapshot foundation](snapshots.md) and [SQL snapshots](sql-snapshots.md). |
| Log compaction | `TState`, Linux `TDurableState` and `TDiskState` publish a snapshot with the retained suffix and hide the covered prefix from Raft. `CheckCompactionMembershipReadiness()` reports leader-side voters missing a positive `InstallSnapshot` capability or committed-configuration acknowledgement, plus whether a follower has received the leader's barrier. | Fresh `TDiskState` snapshot generations compact the active entries file and reclaim the prior generation after metadata publication. Stores using MRFTL003 keep their old entries file until their next snapshot install. A leader blocks compaction until every remote active voter positively advertises snapshot-install support; a never-seen or explicitly unsupported voter blocks it. At membership-bearing boundaries, every active voter must also acknowledge the committed configuration. Followers, including on the zero-index initial voter baseline, require a current-term all-voter capability barrier. Storage or application snapshot failures can still reject `Compact()` after preflight. Safely remove an offline voter through joint consensus first. See [compaction](compaction.md). |
| InstallSnapshot | Leaders send bounded, stop-and-wait chunks after the target advertises backend support; followers validate the image, boundary and application payload before durable installation and acknowledgement. `TState`, Linux `TDurableState` and `TDiskState` support installation. | `TDiskState` atomically publishes a compacted entries generation, retained-suffix index and snapshot before switching metadata. The full image is capped at 64 MiB and buffered by the receiver; incomplete full-image and covered-boundary metadata assemblies reserve their declared image size against a 256 MiB process-wide budget by default. Chunks are capped at 64 KiB. A voter with no positive capability acknowledgement blocks leader compaction until support is advertised or the voter is safely removed. Partial transfers are not persisted and restart from offset zero. A correlated rejection invalidates cached peer capabilities; the leader probes the compacted boundary with AppendEntries before retrying the image. Application capture and restore callbacks are not negotiated. This is not a disk-streaming or parallel transfer protocol. See [snapshot pin and receive budgets](snapshot-pin-budget.md) and [compaction](compaction.md). |
| Membership-aware recovery | Stable and joint snapshots preserve configuration identity and bounded transition history. Recovery and installation validate the receiver's committed history before accepting membership metadata. | AppendEntries feature bits negotiate lineage support and the 32-, 256- and 1,024-transition MRFTS006/007, MRFTS008/009 and MRFTS010/011 formats. A lineage-capable peer without history support receives predecessor-only MRFTS004/MRFTS005. A peer without lineage receives MRFTS003 stable or MRFTS002 joint identity metadata without transition history. MRFTS002 requires the exact committed joint configuration. MRFTS003 can match a committed stable configuration or complete the stable successor of the receiver's exact committed joint configuration when the sender belongs to the new set. Fresh learners use a separate constrained bootstrap path. These formats cannot recover a stale store across unknown membership changes. A stale store needs an exact trusted anchor within transferred history, and the sender must satisfy voter-set lineage checks. Disjoint or out-of-window history may require a fresh learner baseline or independently verified history. Snapshot CRC and metadata do not authenticate a sender or prove commitment. See [configuration entries](configuration-entries.md) and [durable configuration snapshots](durable-configuration-snapshots.md). |
| Fresh learner bootstrap | An empty learner can install a stable or joint configuration-bearing snapshot and remains a learner afterward. | The learner must have no prior log, commit, snapshot, vote or application progress, and its configured voter set must exactly match the stable set or joint union. Existing voters and nonempty stores require matching committed history. See [online learner staging](online-learner-staging.md). |
| Legacy-store migration | An offline converter, explicit durable binding path and a startup baseline API for split `TDiskState` are available. | Old 20-byte `TDiskState` metadata cannot establish the complete original voter set or commit boundary; supply both from independently trusted records. Existing configuration entries in that legacy log require the offline converter. See [legacy disk-state migration](legacy-disk-state-migration.md) and [trusted legacy voter binding](trusted-legacy-voter-binding.md). |

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
  plus one heap budget across capture, storage, application state and transport.
  The receive budget covers declared incoming image sizes only. The required
  staged-image and durable-manifest contract is outlined in [the streaming design](snapshot-streaming-design.md).
- Automatic recovery when a stale store lacks an allowed history anchor, or
  its transition chain is disjoint from the sender's voter lineage.
- An efficient WAL or cross-request group-commit storage path. `TDurableState`
  publishes one complete image for each follower `AppendEntries` batch, but
  republishes synchronously for individual proposals and commit-index advances.
  Legacy `TDiskState` syncs
  its log/index and atomically publishes versioned hard-state metadata. Fresh
  `TDiskState` stores checksum records; older split files and their record
  encoding remain readable after trusted-baseline migration.

The membership, compaction, transfer and legacy disk-publication paths have deterministic regression
coverage described in their design notes. Joint-election tests also enumerate
every reply ordering for overlapping and disjoint voter sets, checking each
delivery prefix before both majorities are present. This coverage does not
establish hardware power-loss behavior, authenticate peers, or replace
independent review.
