# Require voter proof before legacy durable startup: v0.1.0-alpha.52

The alpha.51 guard prevented a live Raft instance from being rebound after
startup, but it still allowed a recovered log-only MRFTD001/MRFTD002 store to
start unbound using only the process's peer list. That left the persisted node
identity unprotected across restarts.

Raft now rejects a recovered durable store with no voter binding and no
ordinary snapshot before it can bind peers or serve traffic. The operator must
first call `TDurableState::BindVotersFromTrustedBaseline` using an independently
verified complete original voter set. A recovered unbound store with an
ordinary snapshot retains the existing adoption path, which validates that the
snapshot voter list matches the configured peers before persisting the
binding. Newly initialized stores still bind their initial peer set during
startup.

Startup tooling may call `CanBindVotersFromTrustedBaseline(voters, &reason)`
before the migration to display a non-mutating consistency diagnostic. It runs
the same vote, snapshot, and retained-log checks as the binding operation. A
successful check does not prove the supplied voter set is historically
complete; the operator must still verify it independently.

The rejection does not rewrite the durable file. The alpha.51 startup marker
also keeps the explicit migration API startup-only. `TDiskState` and in-memory
state behavior are unchanged. This closes unbound durable startup; it does not
implement live membership changes or joint snapshot recovery at this milestone.
Later follow-ups add guarded live membership and joint snapshot recovery when
the original voter binding and matching committed configuration history exist.

Tests cover recovered log-only and idle stores failing without file changes,
successful startup after explicit trusted migration, ordinary-snapshot adoption,
and rejection of migration after a successful startup.

Self-review completed; no independent reviewer approval is claimed.
