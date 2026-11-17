# Pre-vote elections

Election timeouts start a pre-vote campaign before changing durable term or
vote state. A candidate asks each configured voter whether it would support
the next term. A voter grants only when the request is well formed, its log is
up to date, the prospective term is newer, and the voter has passed the
deadline of its most recent accepted leader contact. AppendEntries and
supported InstallSnapshot traffic refresh that deadline; malformed or
unsupported snapshot metadata does not.

Pre-vote grants are volatile and do not reset election timers. They use the
active stable or joint configuration quorum, with distinct voter IDs. Only
after that quorum grants the same campaign does the candidate persist its new
term and self-vote, then send ordinary RequestVote RPCs. Each campaign has a
unique sequence number, so delayed grants from an earlier retry cannot count.
The regular election and vote quorum remains required to become leader.

A validated TimeoutNow from a committed leadership transfer skips the probe
and starts the ordinary election immediately. It still requires real
RequestVote grants from the active quorum.

Pre-vote adds message types 13 and 14 to the native-layout wire protocol.
Older binaries do not recognize these types. Upgrade all voters together
before allowing a node to campaign with this version; mixed-version rolling
upgrades are not supported. The protocol still has no negotiated wire version
or authenticated peer identity.

Deterministic tests cover non-persistent grants, suppression after recent
leader contact, stale campaign replies, high-term isolated candidates, and
joint-quorum election paths. Pre-vote reduces term disruption from isolated
nodes; it is not a lease and does not replace leadership expiration or
strong-read quorum checks.
