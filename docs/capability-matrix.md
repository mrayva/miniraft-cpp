# Current membership and snapshot capabilities

This page summarizes the current implementation and its operational limits.
It supersedes the milestone-by-milestone status notes where those describe an
earlier stage. Detailed behavior and format compatibility remain in the linked
design notes. This is a capability summary, not a production-readiness claim.

## Membership

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Stable and joint quorums | Elections, commit advancement and strong-read confirmation use strict majorities of both voter sets while a joint configuration is active. | Every voter must run a compatible build before a transition. Joint entries use the existing configuration wire format. See [joint quorum](joint-quorum.md) and [live membership](configuration-entries.md). |
| Add or remove voters | `ProposeJointConfiguration()` starts a transition; after it commits, `FinalizeConfiguration()` commits the stable successor. Overlapping and disjoint voter sets are supported. | Live transitions require `TDurableState`, a persisted original voter binding and commit index, a current-term committed entry, and endpoints on every replica for the active and proposed voters. New voters must first be staged as learners and acknowledged through the leader's committed boundary. See [online learner staging](online-learner-staging.md). |
| Remove the current leader | A bounded leadership transfer can move leadership to a caught-up voter in the new set after the joint entry commits. | The target must be in the new voter set and must still win the active joint quorum. See [leadership transfer](leadership-transfer.md). |
| Endpoint rollout and readiness | Readiness reports local missing targets, leader-observed learner acknowledgements, and transition log capacity. Server peer readers follow voter role changes. | Endpoint registration is local to each process; it is not replicated or distributed. Check readiness on every replica and again on the prospective leader immediately before proposing. Learner replication targets must be registered again after leadership changes or restart. There is no cluster-wide administrator or rollout API. See [server membership reconciliation](server-membership-reconciliation.md). |
| Retiring a removed peer | A target can be removed after the stable successor commits. Native TCP and sockets with a safe shutdown hook can be retired while I/O unwinds. | Some socket wrappers, including the current raw TLS wrapper, cannot be safely retired after a response reader starts. Keep those endpoints until server shutdown or use the retirable TLS wrapper. |

## Snapshots and recovery

| Capability | Available | Requirements and limits |
| --- | --- | --- |
| Application checkpoints | KV and SQL provide snapshot callbacks; capture and restore validate complete application images. | Images are complete in-memory objects capped at 64 MiB, including metadata. The dummy application has no snapshot implementation. Capture, storage encoding and restore can block the event loop. See [snapshot foundation](snapshots.md) and [SQL snapshots](sql-snapshots.md). |
| Log compaction | `TState` and Linux `TDurableState` can publish a snapshot with the retained suffix and delete the covered prefix. | `TDiskState` does not support compaction or InstallSnapshot. Compaction at a membership-bearing boundary waits for every active voter to acknowledge the committed configuration; one offline active voter can therefore block compaction. Safely remove it through joint consensus first. See [compaction](compaction.md). |
| InstallSnapshot | Leaders send bounded, stop-and-wait chunks; followers validate the image, boundary and application payload before durable installation and acknowledgement. | The full image is capped at 64 MiB and buffered by the receiver; chunks are capped at 64 KiB. Partial transfers are not persisted and restart from offset zero. This is not a disk-streaming or parallel transfer protocol. See [compaction](compaction.md). |
| Membership-aware recovery | Stable and joint snapshots preserve configuration identity and bounded transition history. Recovery and installation validate the receiver's committed history before accepting membership metadata. | Voters need compatible snapshot-format support. A nonempty stale store needs an exact trusted history anchor; retained lineage is bounded to 1,024 transitions in MRFTS010/MRFTS011 (MRFTS008/MRFTS009 cap at 256, older extended formats at 32). The sender must satisfy the documented voter-set lineage checks. Disjoint or out-of-window stale history may require a fresh learner baseline or independently verified history. Snapshot CRC and metadata do not authenticate a sender or prove commitment. See [configuration entries](configuration-entries.md) and [durable configuration snapshots](durable-configuration-snapshots.md). |
| Fresh learner bootstrap | An empty learner can install a stable or joint configuration-bearing snapshot and remains a learner afterward. | The learner must have no prior log, commit, snapshot, vote or application progress, and its configured voter set must exactly match the stable set or joint union. Existing voters and nonempty stores require matching committed history. See [online learner staging](online-learner-staging.md). |
| Legacy-store migration | An offline converter and explicit durable binding path are available. | Legacy storage cannot establish the complete original voter set or historical commit boundary by itself. Migration needs an independently trusted original voter set and commit boundary; a dry run cannot prove either. See [legacy disk-state migration](legacy-disk-state-migration.md) and [trusted legacy voter binding](trusted-legacy-voter-binding.md). |

## Operational sequence for a voter change

1. Upgrade every voter to a compatible build and snapshot format.
2. Register replication targets on every replica for all current and proposed
   voters. Stage new nodes as learners.
3. Check readiness on every replica. On the leader, wait for each added voter
   to acknowledge through the committed boundary; recheck immediately before
   proposing.
4. Propose the joint configuration and wait for both majorities to commit it.
   If removing the leader, transfer to a caught-up voter in the new set.
5. Commit the stable successor. Only then retire removed endpoints.
6. Compact after the active voters have acknowledged the committed
   configuration. If an offline voter blocks the barrier, remove it safely
   before compacting; it can later rejoin as a learner using supported history.

## Remaining gaps

- Cluster-wide endpoint discovery, rollout and administrator workflow.
- End-to-end streaming or logical-object snapshots with explicit aggregate
  resource accounting for large application state. The required staged-image
  and durable-manifest contract is outlined in [the streaming design](snapshot-streaming-design.md).
- Automatic recovery when a stale store lacks an allowed history anchor, or
  its transition chain is disjoint from the sender's voter lineage.
- An efficient WAL or group-commit storage path; `TDurableState` republishes a
  complete durable image synchronously on updates. Legacy `TDiskState` also
  lacks the durable backend's power-loss guarantees.

The membership, compaction and transfer paths have deterministic regression
coverage described in their design notes. Joint-election tests also enumerate
every reply ordering for overlapping and disjoint voter sets, checking each
delivery prefix before both majorities are present. This coverage does not
establish hardware power-loss behavior, authenticate peers, or replace
independent review.
