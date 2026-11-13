# Term-only storage lookup: v0.1.0-alpha.22

The in-memory ownership fix made Get return a defensive copy. LogTerm used Get,
so checking the term of a large entry also copied its entire payload. The durable
backend already copied full entries, so it had the same unnecessary cost. Raft
checks terms during log matching, elections, commitment and compaction.

IState now has a RetainedLogTerm hook. The in-memory and durable backends read
the term from their retained entry without copying its payload. LogTerm still
checks bounds and returns the snapshot boundary term as before. Full Get calls
continue to return independent copies. The default hook uses Get, preserving
source compatibility for external IState backends that do not override it.
An IState vtable changed: rebuild the library and dependent binaries together.
Backends subclassing TState or TDurableState and overriding Get with different
log semantics must also override RetainedLogTerm consistently.

Deterministic tests count full-entry reads while repeatedly checking 1 MiB log
entries, verify no Get call is needed in either optimized backend, and verify
mutating a full Get result cannot alter stored data. The durable test also checks
snapshot-boundary and out-of-range behavior. The memory test failed before the
change: 20 term checks invoked 20 full Get calls. No throughput benchmark is
claimed. Legacy and external backends can retain the fallback cost.

Self-review checked retained-suffix offsets, snapshot boundaries and fallback
behavior. There is no wire or durable-format migration. No independent reviewer
approval is claimed. Voting membership changes and joint consensus remain open.
