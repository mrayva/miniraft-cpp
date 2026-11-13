# In-memory log ownership: v0.1.0-alpha.21

The in-memory TState backend retained the mutable buffer supplied to Append and
returned the same buffer from Get. A caller or application could therefore change
an existing log entry, including its term or command, after Raft appended it.
That could break log matching and change a committed application command. The
durable backend already copies entries on append and read.

TState now copies entry bytes on append, construction and readback. Copies of a
TState also receive independent log entries. This preserves stored terms and data
when the original proposal or a returned entry is modified. Copies cost additional
memory and CPU, especially for large entries. TState is still volatile and does
not provide crash durability. Its public Log member and IState mutation methods
remain test/backend interfaces: modifying them while Raft runs bypasses its safety
rules, and serialized access is still required.

Deterministic tests cover mutation of the appended buffer, returned buffer,
constructor input and copied states, plus a three-node replication schedule that
changes the proposal after Append and verifies the original command commits on
all nodes. The first two ownership tests failed on the previous implementation.
Self-review covers snapshot suffix retention and the durable backend's existing
copy behavior; no independent reviewer approval is claimed. There is no wire or
durable format change, and existing corrupted state is not repaired.
