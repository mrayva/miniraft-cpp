# Covered snapshot identity checks: v0.1.0-alpha.41

When a follower already has a committed prefix covering an InstallSnapshot
boundary, it does not need to install the image. Previously the fast path
recognized only the `MRFTS001` magic, then acknowledged the full snapshot as
installed. It skipped checking the snapshot's embedded index, term and voter
IDs against the RPC and the receiver's fixed voter set.

The fast path now waits for and parses the bounded fixed-format metadata prefix.
It acknowledges coverage only when the embedded boundary and sorted voter list
match. It also checks the encoded application length against the declared total
size. It does not read the application body or verify its CRC because it never
uses that image; the receiver's already committed prefix is the evidence for
coverage. Actual installations continue to validate the complete image, CRC,
metadata, voter set and application payload before replacing state.

Deterministic regression cases cover mismatched embedded voters, a snapshot
boundary that disagrees with the request, and a valid metadata prefix split
across chunks. Rejected images do not change durable Raft or application state.
Joint and stable configuration snapshots remain unsupported by the live
runtime, and live voter changes remain disabled. Self-review completed; no
independent reviewer approval is claimed. Network-enabled Release and ASan/UBSan
CTest suites both pass all 20 targets.
