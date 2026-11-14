# Bound pinned outgoing snapshot generations: v0.1.0-alpha.43

Per-peer transfer pinning guarantees that periodic compaction cannot continually
restart an in-flight InstallSnapshot. However, different peers can be transferring
different generations. A lagging peer can retain an older image while another
peer pins a newer one, and unbounded compaction can keep adding pinned generations.

The leader now allows at most two distinct snapshot generations to remain pinned
by outgoing transfers. `TRaft::Compact` checks this before capturing or changing
storage. At the cap it throws a transient `std::runtime_error` without changing
the application, log, snapshot boundary or active transfers. Callers can retry
after a transfer completes; the KV checkpoint loop already retries failed
compaction attempts while the applied-index threshold remains met. Term changes
and target removal continue to release their corresponding transfer contexts.

After a successful compaction, the leader drops its standalone encoded-snapshot
cache. Active transfers keep their own shared reference to the immutable bytes,
so they continue unchanged; a later transfer encodes the newer local image.
Transfers of the same image still share the same snapshot and encoding.

A deterministic test pins two different generations, verifies that compaction
fails without mutation, completes one transfer and verifies that compaction then
succeeds while the other peer still owns its older image. Existing tests continue
to verify catch-up progress under compaction during every chunk.

This caps the number of distinct active transfer generations, not total process
heap usage. Current application/log state, snapshot capture and installation,
transport buffers and other allocations remain outside this limit; a global byte
quota and disk-streamed snapshot transfer remain open. Voter membership changes
and joint snapshot installation are also still unsupported.

Self-review checked the two-generation accounting, exception ordering before
state mutation, cache ownership and retry path. No independent reviewer approval
is claimed.
