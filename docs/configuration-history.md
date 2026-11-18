# Offline configuration history validation: v0.1.0-alpha.27

TConfigurationHistory::MakeSnapshot projects the history at its through-index
and term into an offline snapshot. With up to 32 retained transitions, it emits
MRFTS007 for a joint configuration and MRFTS006 for a stable configuration.
With 33 through 256 transitions, it emits MRFTS009 for joint and MRFTS008 for
stable configurations.
Older snapshots without a chain retain the MRFTS002/MRFTS003 formats, and
MRFTS005/MRFTS004 remain supported for snapshots carrying only their bounded
predecessor link. The legacy MRFTS001 format is used when the trusted baseline
asserts no earlier configuration entry; it does not encode or prove that
zero-identity assertion.
The caller must independently establish that the history boundary is committed
and that the supplied application image corresponds exactly to that boundary.
The method validates snapshot metadata and size; it does not capture application
state, compact or mutate storage, establish commitment, or activate voters.
At this alpha.27 milestone joint snapshots were rejected by the live runtime.
Since alpha.71, MRFTS002 participates in runtime compaction and InstallSnapshot
when the receiver has committed the exact same joint voter sets and identity.
Bound storage preserves the original voter binding while the snapshot records
the active joint sets; that metadata alone does not prove commitment.

TConfigurationHistory checks a sequence of retained log entries against a trusted
configuration baseline. It joins the experimental configuration-entry codec with
the joint-snapshot metadata and checks that joint and stable records have the
expected predecessor index/term and voter sets. A stable configuration can advance
to a joint one; that joint configuration can advance to a stable record naming the
same old and new sets. A successful stable transition selects the new voters.

The caller visits every retained entry in order: Observe handles ordinary entries,
and Advance handles configuration entries. Both return a new history value and
leave the original unchanged on success or failure. They reject index gaps,
nonpositive or decreasing terms, malformed records, wrong predecessors, wrong voter
sets and invalid phase transitions. A caller can retain a previous value and scan
a replacement branch from that point. This models validation of rollback; it does
not truncate a live log or decide when a configuration becomes active.

FromJointSnapshot validates the snapshot metadata and starts at its included
index and term, using its joint configuration identity and preserving any
encoded predecessor lineage. Stable accepts an explicit
configuration identity and scanned-prefix boundary. The caller must establish that
baseline independently: a v1 fixed snapshot contains voter IDs but does not prove
the latest configuration entry's identity. Passing a zero identity asserts that no
earlier configuration entry exists. Decode and authenticate any persisted input as
appropriate before supplying it. The validator does not prove commitment, cluster
identity, promotion readiness, log completeness outside the supplied sequence or
the correctness of the caller's trusted baseline.

The [alpha.53 follow-up](configuration-scan-identity.md) ensures that the scan's
uncommitted-configuration result compares phase and voter sets as well as
configuration index and term.

Joint snapshot metadata now rejects a configuration term different from the
snapshot term when both indices are equal: one log index cannot have two terms.
Earlier alpha.25 tooling could encode such structurally inconsistent v2 images;
they are now rejected and are not rewritten automatically. The live runtime never
accepted joint snapshots. Fixed v1 snapshot encoding and existing durable formats
are unchanged.

Tests cover joint/stable chains, alternate branches, joint snapshot boundaries,
invalid baseline identities, skipped entries, stale terms, wrong sets and
predecessors, malformed entries, index exhaustion and CRC-valid snapshot metadata
corruption. A durable test scans configuration entries after a fixed snapshot and
restart, confirms the fixture's known bootstrap baseline, and verifies that Raft
still rejects the unsupported entries without rewriting storage. Runtime tests
install a maximum-length stable history from an anchored stale store and a
maximum-length joint history through fresh-learner bootstrap. Full tests also
cover existing snapshot installation and learner catch-up.

Snapshots retain at most 256 contiguous membership transitions. The legacy
MRFTS006/MRFTS007 formats remain limited to 32; MRFTS008/MRFTS009 carry longer
chains. The runtime can use this chain for online stale-store recovery when a
transition predecessor matches the receiver's trusted stable baseline. Stable
images require the sender in both voter sets for every transition after that
anchor. A joint image
requires this for completed transitions and accepts a sender from either side
of the active joint configuration. The chain cannot authorize a disjoint path
without an independent history source.

This validator is one component of live reconfiguration. Joint entry activation,
rollback and guarded transitions are described in
[configuration entries](configuration-entries.md); matching-history joint
snapshot compaction and installation are described in
[joint snapshot runtime](joint-snapshot-runtime.md). Existing members still
cannot use a snapshot to introduce a configuration absent from both committed
history and the locally validated pending transition; an exact pending match
can confirm that transition at its snapshot boundary. A fresh learner has a
narrow committed-snapshot bootstrap path. A snapshot may also stop before a
receiver's pending configuration entry and preserve it in the suffix.
Self-review checked contiguous scanning, term and predecessor relationships,
immutable branch results, snapshot boundary consistency and the unknown-v1-
identity case. No independent reviewer approval is claimed.
