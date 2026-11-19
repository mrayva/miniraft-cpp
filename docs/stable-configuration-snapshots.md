# Stable configuration snapshot metadata

The experimental MRFTS003 snapshot format preserves a stable voter set and the
index and term of the configuration entry that selected it. This fills a gap in
MRFTS001: its voter list does not establish the identity needed to validate a
later configuration entry. MRFTS003 stores magic[8], included index[8], included
term[8], voter count[4], application length[8], configuration index[8],
configuration term[8], voter IDs[4 each], application data, and CRC32[4]. The
configuration identity must be nonzero, no later than the snapshot boundary, and
have the same term when both indices coincide. Voters are sorted and bounded.
MRFTS001 and MRFTS002 encoding is unchanged.

When the validated history knows the immediately preceding transition, MRFTS004
extends the stable format with the predecessor's configuration index/term,
preceding joint entry index/term, and old voter set. MRFTS005 carries the same
lineage in a joint snapshot so a restart after joint compaction can preserve it
through the stable successor. The lineage is bounded to one transition; it is
not a quorum certificate or cryptographic proof.

MRFTS006 stable and MRFTS007 joint snapshots add a bounded chain of up to 32
transitions. MRFTS008 stable and MRFTS009 joint snapshots use the same chain
encoding with a 256-transition limit. MRFTS010 stable and MRFTS011 joint
snapshots extend the limit to 1,024. The separate magic makes older nodes
reject longer chains. Each entry records
the predecessor stable configuration identity,
joint entry identity, stable successor identity (zero only for the final active
joint phase), and both voter sets. The chain must be contiguous and its tail
must match the snapshot's active configuration. It enables stale-store recovery
across multiple compacted transitions when the receiver can anchor the first
entry to its trusted baseline and the sender belongs to every old and new voter
set. It remains lineage metadata, not a commitment certificate.

MRFTS004 places the lineage fields after the stable configuration index/term,
then stores the active and predecessor voter lists before application data.
MRFTS005 places them after the joint new-voter count, then stores the old,
new, and predecessor voter lists. Both formats retain the CRC32 trailer and the
64 MiB snapshot bound.

MRFTS006 through MRFTS011 keep the corresponding MRFTS004/MRFTS005 header and
voter lists, then append transition count[4] and transition records before
application data. A record stores six 64-bit index/term fields, old and new
voter counts[4 each], and sorted old and new voter IDs[4 each]. MRFTS006/007
accept at most 32 records; MRFTS008/009 accept at most 256; MRFTS010/011
accept at most 1,024. All retain the CRC32 trailer and the 64 MiB total
snapshot bound.

`TConfigurationHistory::FromStableSnapshot` initializes offline history from
this identity. The retained-log scanner checks the baseline identity against
the snapshot before checking subsequent joint/stable entries. These checks do
not prove commitment or authenticate the snapshot: the caller must establish a
trusted snapshot and commit boundary independently. MRFTS003 cannot assert that
there was no earlier configuration entry; an explicit trusted baseline remains
necessary for such histories.

The live runtime accepts MRFTS003 only when its voter list matches the node's
configured fixed voter set and persisted binding. It can recover the image at
startup, restore the application, install it from a peer, and preserve its
configuration identity through later compaction. The covered-snapshot path
checks the stable configuration identity and voter list before acknowledging an
already-covered boundary. These paths do not activate a changed voter set or
enable joint consensus.

Since alpha.58, bound in-memory and durable storage can also retain an
MRFTS003/MRFTS002 image whose active set differs from the original persisted
binding, for offline history validation. That storage support does not make the
image acceptable to live Raft or authenticate its membership claim; callers must
provide a trusted history baseline.

Older binaries may falsely acknowledge covered unknown formats; do not send
MRFTS004 or MRFTS005 images to a mixed-version cluster. Later follow-ups added
live configuration activation and matching-history joint snapshots. Snapshot metadata still cannot
invent an uncommitted configuration. A matching snapshot can cover committed
progress before a pending configuration entry while preserving that entry in
the suffix. A snapshot at or beyond the entry is accepted only when its phase,
voter sets and configuration index/term match the receiver's validated pending
transition and its sender belongs to the quorum governing that transition.
Rebuild dependents because the in-memory snapshot structure has changed.
