# InstallSnapshot leader-contact validation

InstallSnapshot previously demoted a candidate and refreshed its election timer
as soon as a structurally valid RPC arrived. The runtime only discovered that a
same-term image used an unsupported format or conflicting metadata after those
state changes. A peer sending an unusable snapshot could therefore delay a new
election without providing a usable recovery image.

The receiver now accumulates the bounded metadata prefix before treating a
same-term InstallSnapshot as leader contact. Partial prefixes may be retained
and acknowledged so chunked transfer can continue, but they do not change the
Raft role, leader ID or election deadline. Unknown formats and invalid metadata
are rejected without those changes. Once the prefix matches a supported fixed
or stable image, normal transfer handling refreshes leader contact. Higher-term
requests may start buffering after a voter sends a recognized snapshot-format
prefix. Matching continuations are accepted only for that bounded in-progress
assembly. The receiver advances the term, demotes a candidate and refreshes its
election deadline only after the combined metadata prefix is complete and
supported. A full install keeps the assembly for the remaining payload chunks;
an already-covered boundary can be acknowledged after validating the metadata
prefix alone.

Deterministic regression coverage checks an unsupported joint image split across
chunks while the receiver is a candidate, both when its boundary is already
covered and when it would require full installation. The first partial prefix
does not demote it or move its deadline; the chunk that identifies the unsupported
format is rejected with the same state preserved. Existing valid split-prefix,
snapshot installation, stale-term and higher-term preflight tests continue to
cover the accepted paths. A separate higher-term regression splits a supported
metadata prefix across chunks, checks that the candidate remains in its prior
term until validation, then verifies the remaining image installs successfully.
No wire or storage format changed.

This validates format and boundary metadata before recognizing leader contact;
the complete image CRC and application payload are still checked only when an
image is installed. It does not authenticate peers or enable joint snapshot
installation or live membership changes.
