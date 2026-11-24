# Joint snapshot metadata foundation: v0.1.0-alpha.25

The [alpha.27 follow-up](configuration-history.md) requires the configuration
term to equal the snapshot term when both refer to the same index.
The [alpha.48 follow-up](unsupported-snapshot-term-preflight.md) checks the
initial snapshot format before accepting a higher RPC term. The [alpha.49 follow-up](snapshot-metadata-term-preflight.md)
also checks the embedded boundary, fixed voters, and known stable configuration
identity.

This step adds an experimental snapshot representation for a joint configuration
effective at the included snapshot boundary. MRFTS002 records the old and new
sorted voter sets plus the configuration entry's index and term. Its index must
be positive and no greater than the snapshot index; its term must be positive and
no greater than the snapshot term. Both voter sets are nonempty, positive, sorted,
unique and individually bounded. The codec validates structure, not whether the
configuration was committed or belongs to an authenticated cluster.

The v2 byte layout is magic[8], snapshot index[8], snapshot term[8], old voter
count[4], application data length[8], configuration index[8], configuration
term[8], new voter count[4], old voter IDs[4 each], new voter IDs[4 each], opaque
application data, and CRC32[4] over all preceding bytes. Lengths and counts are
bounded before allocation. Fixed snapshots continue to encode as MRFTS001 with
exactly the previous bytes; a golden v1 image tests compatibility. The standalone
snapshot store can export and load v2 images for testing or offline tooling.

The [alpha.59 continuity follow-up](configuration-snapshot-continuity.md) checks
that a stored stable configuration advances to a joint image using the same old
voters, and that a joint image can only continue with the same joint identity or
move to a stable successor selecting its new voters.

This began as a format and offline persistence foundation. Since alpha.71, Raft
startup, local compaction, and InstallSnapshot support joint images when the
persisted original voter binding is present and the receiver's committed joint
history matches both voter sets and the configuration identity. The metadata
does not prove commitment or authenticate a sender; the runtime relies on the
receiver's previously validated history. Compaction waits for every active
voter to acknowledge the committed joint boundary. A covered
snapshot is acknowledged only after its prefix identifies a supported format,
including when the prefix arrives in several chunks. A higher-term install must
include the complete bounded metadata prefix and match the RPC boundary and
known configuration history before hard state advances.

Older binaries do not understand MRFTS002 and may acknowledge an already-covered
image without checking its version. Every voter in the proposed stable set must
be upgraded before it participates in a joint transition. A removed voter may
remain offline or on an older binary while its old-set majority is satisfied by
other voters; if it returns before finalization, it may reject joint snapshot
transfer and cannot help that majority. A standalone export is not proof that a
configuration is committed and is not an authoritative Raft checkpoint.

Tests cover a golden v1 image, joint round-trip, every-byte corruption and
truncation, semantic corruption with valid CRC, bounds and ordering, standalone
publication, bound durable round-trip, runtime rejection without rewriting the
image, and network rejection both above and below the commit boundary. They also
cover a supported v1 prefix split across chunks. The snapshot struct's
in-memory layout changes; rebuild the library and dependents together. Self-review
checked version dispatch, bounds, immutable voter binding, covered acknowledgments
and fail-closed recovery. No independent reviewer approval is claimed.
