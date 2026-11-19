# Matching-history joint snapshots: v0.1.0-alpha.71

Joint snapshots participate in local compaction, startup recovery, application
restore and InstallSnapshot. MRFTS002 remains the compact format when no
predecessor is known; MRFTS005 also records the immediately preceding stable
voter set, its configuration identity, and the joint entry identity. MRFTS006
stable and MRFTS007 joint snapshots retain up to 32 contiguous membership
transitions; MRFTS008 stable and MRFTS009 joint snapshots retain up to 256;
MRFTS010 stable and MRFTS011 joint snapshots retain up to 1,024.
Stable successors use MRFTS004, MRFTS006, MRFTS008, or MRFTS010 when lineage is
available. The persisted voter binding remains the original bootstrap set.

For an existing member, the runtime accepts a joint image only when the receiver
has already committed that exact joint configuration. The snapshot's old and
new voter sets and configuration index/term must all match local committed
history. A completely fresh learner can also bootstrap from a joint image when
its configured voter set is exactly the union of the snapshot's old and new
sets, the receiver is in neither set, and the sender belongs to the active set.
The learner must have no log, commit, snapshot, vote, or applied application
progress. These checks run on the bounded metadata prefix before a higher RPC
term is treated as leader contact or the image is installed. If the metadata
prefix exceeds one transport chunk, a configured voter with a supported
snapshot format can advance the receiver's term on the initial chunk so the
transfer can continue; full lineage is still required before leader contact or
installation. The full image, checksum and application payload are validated
before storage or application state changes. A node with existing state cannot
use snapshot metadata to skip its committed history or cross an uncommitted
transition.

A receiver whose durable snapshot is the exact committed joint boundary can
also install that configuration's stable successor directly. The stable image
must name the joint configuration's new voter set, carry a later configuration
index and nondecreasing configuration term, and come from a voter in that new
stable set. This covers a restart after joint compaction when the receiver missed
the stable successor and the leader has since compacted it.
The successor may retain only a matching suffix of its transition history. The
receiver checks that the final stable transition completes its committed joint
identity, completes its trusted local chain, and discards any older prefix that
extends beyond the receiver's trusted history.

Compaction may advance through applied committed progress before an
uncommitted configuration entry, while retaining that entry in the log suffix.
For a configuration-bearing snapshot, every active voter must first acknowledge
the committed configuration boundary. The leader verifies this from correlated
replication ACKs, then distributes a retryable certificate to followers so each
replica's local checkpoint loop can compact. A snapshot cannot remove the only
copy of a transition before every active voter has learned it. A receiver may
install a snapshot before its own uncommitted configuration entry only when the
snapshot matches its committed configuration. If the boundary reaches a
pending entry, the receiver requires exact phase, voter-set and configuration
identity matches with that validated entry before accepting a higher term. The
sender must belong to the quorum that governs the transition: the pending joint
quorum for an uncommitted joint entry, or the committed joint quorum while a
stable successor is pending. This lets a valid leader snapshot confirm
commitment without introducing unknown membership. A receiver that has already
committed the same joint configuration may install the image and retain its
committed matching log suffix. A fresh learner uses the validated image as its
initial configuration baseline and remains a learner. Startup reconstructs the
joint quorum from the snapshot baseline and can scan a later stable successor.

Deterministic durable coverage checks the compaction gate before the joint
commit watermark reaches a peer, joint capture and compaction after all active
voters acknowledge it, rejection of forged joint voter metadata before a higher
term, successful installation, restart recovery, and stable finalization from
the snapshot baseline. Pending-transition transfer rejects mismatched joint and
stable metadata before term mutation, then installs exact matching joint and
stable snapshots. Release and ASan/UBSan suites are run for this change.

A rejoin regression also keeps a removed member's nonempty stale store and
confirms it rejects a higher-term joint snapshot without mutation. A replacement
empty store receives the leader's normal AppendEntries probe followed by the
compacted joint snapshot, bootstraps from that image's active voter union, and
remains a learner while the existing transition finalizes. It catches up to the
stable boundary, then rejoins through a new joint entry and stable successor.

After that rejoin, a restart regression keeps the installed joint snapshot as
the application restore point while later committed stable, joint, and stable
membership records remain in the log. Snapshot restore scans only through the
snapshot boundary; it no longer treats those later committed transitions as
uncommitted relative to the earlier application image. Startup then recovers
the rejoined voter set and replays the application through the committed tail.

An older stable store can recover across multiple compacted joint-to-stable
transitions when its committed voter set and configuration identity exactly
match a transition predecessor anywhere in an MRFTS006 or MRFTS008 stable
image. The runtime validates the suffix from that anchor and joins it to the
receiver's trusted local history;
the snapshot's earlier, unanchored prefix is discarded. Each joint entry must
follow the preceding stable identity. Stable images require the sender in both
voter sets at every transition in the suffix. For MRFTS007 or MRFTS009,
completed transitions after the anchor have the same requirement; the active
joint tail accepts a leader from either its old or new voter set. This lets the
leader that proposed the active joint configuration finish replication while requiring the
sender to have survived every completed transition. The bounded metadata is
checked before a higher term is used to accept leader contact or install the
image. A current voter may
start buffering a supported snapshot whose bounded metadata spans chunks. A
sender absent from a required voter set, a disjoint
completed transition after the anchor, or a chain longer than 1,024 entries still
requires fresh learner provisioning or independently verified history
recovery.

Snapshot metadata does not authenticate a peer or prove commitment by itself.
The runtime relies on its trusted local predecessor and a sender that belonged
to that configuration, then checks the complete image and CRC before storage or
application replacement. Metadata does not authenticate a peer or prove each
historical transition committed; this path retains the Raft trusted-leader and
crash-fault assumptions. MRFTS006/MRFTS007 are limited to 32 transitions;
MRFTS008/MRFTS009 carry up to 256; MRFTS010/MRFTS011 carry up to 1,024. All
voters must understand MRFTS004 through MRFTS011 before using these lineage
snapshots; older versions reject the new formats.

Before publishing an installed image, the receiver also checks that it has a
local replication target for every voter in the snapshot's active configuration.
If one is missing, installation is rejected and the application and snapshot
image remain unchanged; provision the target and retry.
Snapshot transfer still cannot introduce an unknown joint configuration or
bridge an existing receiver whose committed and pending history identify a
different configuration.

A deterministic stale-store regression verifies multi-hop recovery through
overlapping transitions, rejects a sender that is missing from an intermediate
voter set before term mutation, and retains the original durable voter binding.
Same-configuration refresh and joint-to-stable installation also reject
unverified older prefixes while preserving the receiver's trusted lineage.
The accepted node installs the active stable snapshot as a learner, catches up,
and can later rejoin through a new joint transition. A disjoint chain, a chain
outside the 1,024-transition bound, or lineage that cannot be anchored to the
store's exact committed configuration still requires a fresh learner or
independently verified history.
For an existing durable store that must retain its original binding, the
verified-history path is to stop Raft, validate a full stable snapshot and its
committed configuration history out of band, install it with the offline
`TDurableState::InstallSnapshot` API, and restore the application to the same
snapshot index. Restart with endpoints for the active voter set and declare the
local removed ID as a learner. The runtime then uses the imported active
configuration as its baseline and can accept replication from its new voters.
Once caught up and commit-acknowledged, the learner can be re-added through the
normal joint-consensus API. The offline install API does not perform that
external history verification. A deterministic two-transition regression
rejects the direct peer image, imports the independently verified checkpoint,
then catches up and re-adds the stale voter through a joint configuration.
