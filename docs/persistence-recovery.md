# Persistence recovery validation: v0.1.0-alpha.2

This follow-up closes unsafe recovery behavior in the original three-file
TDiskState format. At this alpha.2 milestone, the format was still experimental
and did not have a power-loss publication protocol.

## Fixed behavior

Previously an incorrectly sized state file silently initialized fresh metadata,
potentially forgetting the node's current term and vote. Missing files were
recreated, short indexes could crash startup, and entry lengths were trusted
before allocation. Recovery now rejects these conditions with std::runtime_error
without changing or recreating the stored files.

A new store is initialized only when all three paths are absent. Existing stores
must contain all three files and an exactly sized metadata record with a
nonzero current term. Recovery validates the complete selected log prefix:
index extent, contiguous offsets, entry types, minimum lengths and file bounds.
Unreferenced trailing bytes remain allowed because log truncation is logical.
The file format and public storage interface are unchanged.
The [log-term follow-up](legacy-log-terms.md) additionally validates nonzero,
nondecreasing selected entry terms bounded by CurrentTerm.

RemoveLast now updates LastLogTerm, including when the log becomes empty.
Append explicitly seeks the write position, so appending after truncation to
zero overwrites the beginning rather than writing into an obsolete suffix.
File-open failures no longer fall back to truncating an existing file. Stream
write/flush errors are detected, and buffered entries/index are flushed before metadata.

## Validation and review

Ten new persistence tests cover truncated/oversized metadata, each missing
storage file, short indexes/entries, invalid entry headers and offsets, zero
terms, excessive log counts and truncation/restart/replacement. Rejected recovery
is checked against a byte-for-byte snapshot of every file to verify no mutation.
An additional POSIX subprocess test verifies that a live storage read failure
terminates the node rather than escaping as an ordinary network exception.
The original implementation failed eight of the initial nine tests, including
a caught segmentation fault on a short index and stale LastLogTerm after removal.

Local Debug/Release core tests and the full ASan/UBSan suite pass: 27 existing
core tests, eight consensus correctness tests, eleven recovery tests and three
transport tests (49 total on POSIX; the subprocess test is excluded on Windows). CI runs the same targets on pushes and PRs.

Self-review checked recovery's non-mutating rejection paths, length arithmetic
before allocation, signed file positions, offset continuity, existing-format
compatibility, output positions after truncation and fail-stop behavior in the
presence of the network layer's generic exception handlers. No independent reviewer
approval is claimed.

## Operational contract and remaining gaps

Use one writer per storage set. Startup validation throws std::runtime_error.
Detected live storage read/write/flush errors call std::terminate: the network
layer catches ordinary exceptions, so throwing alone would allow a node with
unpersisted state to continue processing.
Recovery rejection requires restoring verified state or deliberately rebuilding
the node through a safe cluster procedure; never delete metadata alone to force
startup. Previously created empty or partial file sets are deliberately rejected.

Validation detects structural damage, not all corruption. Fresh `TDiskState`
stores use MRFTL002 with CRC32 for hard-state metadata, log
entries and index offsets. MRFTL001 and old split files remain readable; their
existing log/index records have no checksums or record generations, so a bit
flip that leaves valid fields can go undetected. A later durability follow-up
adds ordered log/index synchronization, temporary hard-state metadata, atomic
replacement and directory synchronization. Its
deterministic process-crash test checks recovery before and after the rename; it
does not substitute for hardware power-cut testing. This alpha.2 milestone
predates the later durable-image crash-boundary tests, snapshot/compaction
support, live joint membership and fresh-quorum read confirmation. Current
limits are documented in [durable state](durable-state.md),
[compaction](compaction.md), [live membership](configuration-entries.md) and
[strong reads](strong-reads.md). Versioned `TDiskState` metadata has a checksum,
persisted commit boundary and original voter binding. Its log and index records
remain unchecked, and it still lacks snapshot support; the strong-read protocol
still does not claim end-to-end application linearizability.
