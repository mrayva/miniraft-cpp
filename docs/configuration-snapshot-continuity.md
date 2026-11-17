# Configuration snapshot continuity: v0.1.0-alpha.59

Alpha.58 separated a store's original voter binding from the active voter set
recorded in configuration-bearing snapshots. That exposed a continuity check
that previously relied on the original binding to catch conflicting snapshot
memberships.

When storage already has configuration metadata, a replacement snapshot must
extend that known state:

- Stable to stable snapshots keep both the configuration entry index/term and
  the active voter set unchanged.
- Stable to joint snapshots use the current active set as their old voters and
  place the new configuration entry after the current snapshot boundary without
  regressing its log term.
- Joint snapshots may advance their included log boundary only while keeping the
  same joint identity and both voter sets. Replacing joint metadata with a
  different joint record requires a stable successor first.
- Joint to stable snapshots select the joint snapshot's new voter set and use a
  stable entry after the joint snapshot boundary without regressing its log
  term.
- A legacy refresh of a stable snapshot must retain its active voter set; storage
  carries the stable identity forward. A legacy image cannot erase joint state.

These checks run in both storage backends and application snapshot recovery. They
check continuity with locally stored metadata, but they do not prove that a
configuration entry was committed or authenticate the sender. Callers that use
the offline install APIs must still provide a trusted snapshot and history
baseline. This alpha.59 milestone predates the live joint-consensus path and
matching-history network installation. Those are now available through the
[live membership](configuration-entries.md) and
[joint snapshot runtime](joint-snapshot-runtime.md) paths; snapshot metadata
still cannot introduce an uncommitted configuration.

Deterministic memory and durable tests cover conflicting same-identity voter
sets, stale legacy membership, stable-to-joint and joint-to-stable transitions,
rejected phase downgrades and durable recovery of the original binding.

The lineage follow-up extends continuity to MRFTS004 stable and MRFTS005 joint
snapshots. A known lineage cannot be replaced by a different predecessor claim;
an older image without lineage retains locally known lineage during refresh.
The live runtime can install a new lineage only after matching it against its
committed or validated pending configuration history. When a matching MRFTS002
joint image omits lineage, runtime fills it from that validated history before
storage; the lower-level storage API does not infer a missing configuration
history on its own. A same-configuration refresh must share its known history
tail, and the receiver retains only the lineage it already trusts; a longer
incoming chain cannot add unverified older transitions.
