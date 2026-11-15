# Matching-history joint snapshots: v0.1.0-alpha.71

MRFTS002 joint snapshots now participate in local compaction, startup recovery,
application restore and InstallSnapshot. The snapshot records the old and new
voter sets plus the joint configuration entry's index and term. The persisted
voter binding remains the original bootstrap set.

The runtime accepts a joint image only when the receiver has already committed
that exact joint configuration. The snapshot's old and new voter sets and
configuration index/term must all match local committed history. These checks
run on the bounded metadata prefix before a higher RPC term is persisted, and
the full image, checksum and application payload are validated before storage
or application state changes. Snapshot metadata cannot establish a new voter
configuration or cross an uncommitted transition.

Compaction remains disabled while a configuration is uncommitted. For a
configuration-bearing snapshot, every active voter must first acknowledge the
committed configuration boundary. The leader verifies this from correlated
replication ACKs, then distributes a retryable certificate to followers so each
replica's local checkpoint loop can compact. A snapshot cannot remove the only
copy of a transition before every active voter has learned it. A receiver that
has committed the same joint configuration may install the image and retain its
committed matching log suffix. Startup reconstructs the joint quorum from the
snapshot baseline and can scan a later stable successor.

Deterministic durable coverage checks the compaction gate before the joint
commit watermark reaches a peer, joint capture and compaction after all active
voters acknowledge it, rejection of forged joint voter metadata before a higher
term, successful installation, restart recovery, and stable finalization from
the snapshot baseline. Release and ASan/UBSan suites are run for this change.

All voters must understand MRFTS002 before using joint snapshot transfer;
older versions may mishandle a covered unknown image. Snapshot transfer still
cannot introduce an uncommitted joint configuration or bridge a receiver whose
committed history identifies a different configuration.
