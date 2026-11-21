#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <deque>
#include <string>
#include <string_view>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "messages.h"
#include "timesource.h"
#include "state.h"
#include "quorum.h"
#include "configuration.h"

struct INode {
    virtual ~INode() = default;
    virtual void Send(TMessageHolder<TMessage> message) = 0;
    // Bounded transports may reject best-effort protocol work without
    // throwing. Existing transports remain source compatible by default.
    virtual bool TrySend(TMessageHolder<TMessage> message) {
        Send(std::move(message));
        return true;
    }
    virtual void Drain() = 0;
    // False until the transport has positively negotiated the extended route.
    virtual bool SupportsExtendedForwarding() const { return false; }
    virtual bool SupportsSessionizedForwarding() const { return false; }
};

// CommandRequest -> Write? -> LogEntry -> Append -> Committed As Index -> Applied As Index (Same) -> Index -> CommandResponse
// CommandRequest -> Read? -> CurrentIndex (fixate) >= CommittedIndex -> CommandResponse
struct IRsm {
    virtual ~IRsm() = default;
    // Check application wire shape before queueing/forwarding; must not mutate state.
    virtual void Validate(TMessageHolder<TCommandRequest>) const {}
    virtual TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest> message, uint64_t index) = 0;
    virtual TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> message, uint64_t index) = 0;
    virtual TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest> message) = 0;
    virtual std::string SaveSnapshot() const { throw std::logic_error("Application snapshots are unsupported"); }
    // Optional upper bound for SaveSnapshot(). Raft checks it against the
    // metadata-adjusted image budget before asking the application to allocate
    // the image. An empty result preserves the legacy post-capture size check.
    virtual std::optional<uint64_t> SnapshotSizeUpperBound() const { return std::nullopt; }
    virtual void ValidateSnapshot(std::string_view, uint64_t) const { throw std::logic_error("Application snapshots are unsupported"); }
    // Implementations must validate completely before atomically replacing state
    // and LastAppliedIndex. Invoked at startup or during serialized installation;
    // a failure after durable installation is fatal and requires restart.
    virtual void LoadSnapshot(std::string_view, uint64_t) { throw std::logic_error("Application snapshots are unsupported"); }

    // Durable RSMs restore this atomically with application data before constructing
    // TRequestProcessor. It must identify committed progress in the same Raft log.
    uint64_t LastAppliedIndex = 0;
};

struct TDummyRsm: public IRsm {
    void Validate(TMessageHolder<TCommandRequest> message) const override;
    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest> message, uint64_t index) override;
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> message, uint64_t index) override;
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest> message) override;

private:
    std::vector<TMessageHolder<TLogEntry>> Log;
};

using TNodeDict = std::unordered_map<uint32_t, std::shared_ptr<INode>>;

struct TJointConfigurationReadiness {
    bool IsLeader = false;
    bool HasDurableMembershipStorage = false;
    bool HasStableConfiguration = false;
    bool HasCommittedCurrentTermEntry = false;
    bool ChangesVoterSet = false;
    // The joint entry, its stable successor and reserved control entries fit.
    bool HasLogCapacity = false;
    std::vector<uint32_t> MissingPeerTargets;
    std::vector<uint32_t> AddedVotersWithoutCommitAck;
    // Missing/unknown peer capability advertisements fail closed before promotion.
    std::vector<uint32_t> AddedVotersWithoutSnapshotInstallSupport;
    // Every current and proposed voter must understand joint-consensus entries.
    std::vector<uint32_t> VotersWithoutJointConsensusSupport;

    bool Ready() const {
        return IsLeader && HasDurableMembershipStorage && HasStableConfiguration &&
            HasCommittedCurrentTermEntry && ChangesVoterSet && HasLogCapacity && MissingPeerTargets.empty() &&
            AddedVotersWithoutCommitAck.empty() && AddedVotersWithoutSnapshotInstallSupport.empty() &&
            VotersWithoutJointConsensusSupport.empty();
    }
};

// Reports the membership and remote-install capability barriers used by
// CompactShared. A ready result does not guarantee that storage or the
// application can create and validate a snapshot.
struct TCompactionMembershipReadiness {
    // Zero identifies the fixed initial voter set; followers still require an
    // explicit capability barrier for that baseline.
    uint64_t ConfigurationIndex = 0;
    bool IsLeader = false;
    bool IsActiveVoter = false;
    bool RequiresLeaderAcknowledgementBarrier = false;
    bool HasLeaderAcknowledgementBarrier = false;
    // Populated on leaders with active voters still behind the committed config.
    std::vector<uint32_t> ActiveVotersWithoutConfigurationAck;
    // Remote active voters whose InstallSnapshot support is not positively known.
    std::vector<uint32_t> ActiveVotersWithoutSnapshotInstallSupport;

    bool Ready() const {
        return ActiveVotersWithoutConfigurationAck.empty() &&
            ActiveVotersWithoutSnapshotInstallSupport.empty() &&
            (!RequiresLeaderAcknowledgementBarrier || HasLeaderAcknowledgementBarrier);
    }
};

class TRaftLogCapacityExceeded : public std::runtime_error {
public:
    TRaftLogCapacityExceeded() : std::runtime_error("Uncommitted Raft log capacity exceeded") {}
};

struct TVolatileState {
    struct TAppendRpc { uint64_t Seqno, LastIndex, LeaderCommit; };
    static constexpr size_t MaxPendingAppendRpcs = 256;
    // Distinct snapshot images retained by active peer transfers. New
    // compaction is deferred when this many generations are already pinned.
    static constexpr size_t MaxPinnedSnapshotGenerations = 2;
    uint64_t CommitIndex = 0;
    // Successful current-term RPC acknowledgements, separately from log indices.
    std::unordered_map<uint32_t, uint64_t> ReadAckSeqno;
    std::unordered_map<uint32_t, uint64_t> LastSentSeqno;
    std::unordered_map<uint32_t, std::deque<TAppendRpc>> PendingAppendRpcs;
    struct TSnapshotRpc {
        // Pinned for this transfer even if the local store compacts again.
        std::shared_ptr<const TSnapshot> Snapshot;
        std::shared_ptr<const TSnapshotEncoding> Encoding;
        // Seqno identifies the outstanding chunk across retries; zero means
        // its reply was consumed and the next send needs a fresh identity.
        uint64_t Offset = 0, Seqno = 0, SentEnd = 0;
        // Lack of forward progress releases this pin so stalled peers cannot
        // retain old snapshot generations indefinitely.
        std::optional<ITimeSource::Time> LastProgress;
    };
    std::unordered_map<uint32_t, TSnapshotRpc> SnapshotRpcs;
    struct TConfigurationBarrierRpc {
        uint64_t Seqno = 0, ConfigurationIndex = 0, ConfigurationTerm = 0;
        ITimeSource::Time Due{};
    };
    std::unordered_map<uint32_t, TConfigurationBarrierRpc> ConfigurationBarrierRpcs;
    std::unordered_map<uint32_t, uint64_t> ConfigurationBarrierAckIndex;
    std::unordered_map<uint32_t, ITimeSource::Time> ConfigurationBarrierAckDue;
    struct TPeerCapabilities {
        uint64_t AppendAckSeqno = 0, Term = 0;
        bool ConfigurationBarrier = false, SnapshotLineage = false;
        bool SnapshotHistory32 = false, SnapshotHistory256 = false;
        bool SnapshotHistory1024 = false;
        bool InstallSnapshot = false;
        bool JointConsensus = false;
    };
    std::unordered_map<uint32_t, TPeerCapabilities> PeerCapabilities;
    struct TLeadershipTransfer {
        uint32_t TargetId = 0;
        uint64_t LastLogIndex = 0, LastLogTerm = 0;
        uint64_t CommitIndex = 0, CommitTerm = 0;
        uint64_t ConfigurationIndex = 0, ConfigurationTerm = 0;
        ITimeSource::Time RetryDue{};
    };
    std::optional<TLeadershipTransfer> LeadershipTransfer;
    // A follower may compact only after the current leader certifies that all
    // active voters support snapshot installation. The zero-index baseline
    // needs a separate marker because its configuration index cannot encode it.
    uint64_t CompactionSafeConfigurationIndex = 0;
    // A certificate expires for readiness purposes when the Raft term changes.
    uint64_t CompactionSafeConfigurationTerm = 0;
    bool HasCompactionSafeInitialConfigurationBarrier = false;
    uint32_t LeaderId = 0;
    std::unordered_map<uint32_t, uint64_t> NextIndex;
    std::unordered_map<uint32_t, uint64_t> MatchIndex;
    // Highest leader commit boundary confirmed by a correlated replication ACK.
    std::unordered_map<uint32_t, uint64_t> CommitAckIndex;
    std::unordered_set<uint32_t> Votes;
    // Pre-votes use the same distinct-voter set as real votes, but never
    // persist a term or vote until a joint-aware quorum has granted them.
    bool PreVoting = false;
    uint64_t PreVoteTerm = 0;
    uint64_t PreVoteSeqno = 0;
    std::unordered_map<uint32_t, ITimeSource::Time> HeartbeatDue;
    std::unordered_map<uint32_t, ITimeSource::Time> RpcDue;
    std::unordered_map<uint32_t, int> BatchSize;
    std::unordered_map<uint32_t, int> BackOff;
    ITimeSource::Time ElectionDue;
    // Tracks only valid leader contact. Campaign and vote retry timers must
    // not make a voter appear to have recently heard from a leader.
    ITimeSource::Time LeaderContactDue{};
    ITimeSource::Time LeadershipExpiryDue = ITimeSource::Max;
    std::unordered_map<uint32_t, ITimeSource::Time> QuorumResponseTime;

    std::vector<uint64_t> Indices;

    TVolatileState& Vote(uint32_t id);
    // Compatibility helper for all-voter fixtures; mixed roles require IDs below.
    TVolatileState& CommitAdvance(int nservers, const IState& state);
    TVolatileState& CommitAdvance(const std::vector<uint32_t>& voters, uint32_t self, const IState& state);
    TVolatileState& CommitAdvance(const TQuorum& quorum, uint32_t self, const IState& state);
    TVolatileState& SetCommitIndex(uint64_t index);
    TVolatileState& SetElectionDue(ITimeSource::Time);
    TVolatileState& SetNextIndex(uint32_t id, uint64_t nextIndex);
    TVolatileState& SetMatchIndex(uint32_t id, uint64_t matchIndex);
    TVolatileState& SetHearbeatDue(uint32_t id, ITimeSource::Time heartbeatDue);
    TVolatileState& SetRpcDue(uint32_t id, ITimeSource::Time rpcDue);
    TVolatileState& SetBatchSize(uint32_t id, int size);
    TVolatileState& SetBackOff(uint32_t id, int size);
    TVolatileState& SetLeaderId(uint32_t id);

    bool operator==(const TVolatileState& other) const {
        return CommitIndex == other.CommitIndex &&
            NextIndex == other.NextIndex &&
            MatchIndex == other.MatchIndex;
    }
};

enum class EState: int {
    NONE = 0,
    CANDIDATE = 1,
    FOLLOWER = 2,
    LEADER = 3,
};

class TRaft {
public:
    // Per-node proposal and failure-detection settings. The log limits reserve
    // room for a current-term leader no-op and, during joint membership, its
    // stable successor. Repeated uncommitted terms may consume the finite
    // control reserve and require operator recovery to restore progress.
    struct TLogLimits {
        uint64_t MaxUncommittedBytes = 256ull * 1024 * 1024;
        size_t MaxUncommittedEntries = 65536;
        // Keep one entry's worth of room for a leader's current-term no-op.
        // Repeated failed terms can still consume this finite reserve.
        uint64_t ReservedControlBytes = sizeof(TLogEntry);
        size_t ReservedControlEntries = 1;
        // Yield leadership after this long without a fresh voter quorum.
        std::chrono::milliseconds LeadershipExpiration = TTimeout::LeadershipExpiration;
        // Shared by all Raft instances in a process by default. Supply one
        // shared budget here to override the process-wide InstallSnapshot cap.
        std::shared_ptr<TSnapshotInstallBudget> SnapshotInstallBudget;
    };

    // Nodes contains every configured replication peer; learners may include
    // the local ID. Retained validated configuration history determines active
    // voter roles and joint quorum arithmetic.
    TRaft(std::shared_ptr<IState> state, int node, const TNodeDict& nodes, std::vector<uint32_t> learners = {});
    TRaft(std::shared_ptr<IState> state, int node, const TNodeDict& nodes,
          std::vector<uint32_t> learners, TLogLimits logLimits);
    // Node IDs are uint32_t on the wire and in membership records. Keep the int
    // overload for source compatibility while allowing the full voter ID range.
    TRaft(std::shared_ptr<IState> state, uint32_t node, const TNodeDict& nodes, std::vector<uint32_t> learners = {});
    TRaft(std::shared_ptr<IState> state, uint32_t node, const TNodeDict& nodes,
          std::vector<uint32_t> learners, TLogLimits logLimits);

    void Process(ITimeSource::Time now, TMessageHolder<TMessage> message, const std::shared_ptr<INode>& replyTo = {});
    void ProcessTimeout(ITimeSource::Time now);

    // Local proposals require leader state. Role rejection leaves storage and
    // the supplied entry unchanged; followers receive entries through Process.
    uint64_t Append(TMessageHolder<TLogEntry> entry);
    // Start a joint-consensus transition. Every newly added voter must already
    // be staged as a learner and have acknowledged this leader's committed
    // boundary. A transition may exclude the leader; transfer leadership to a
    // caught-up voter in the new set after the joint entry commits.
    // Readiness is local to this replica: collect reports from every replica
    // before proposing to verify the configured target set is complete.
    TJointConfigurationReadiness CheckJointConfigurationReadiness(
        const std::vector<uint32_t>& newVoters) const;
    uint64_t ProposeJointConfiguration(std::vector<uint32_t> newVoters);
    // Append the stable successor after the joint entry has committed.
    uint64_t FinalizeConfiguration();
    // Step down and ask a caught-up voter in the committed new set to campaign
    // immediately. If that election fails, this node can campaign again after
    // its election timeout.
    void TransferLeadership(uint32_t target, ITimeSource::Time now);
    uint32_t GetLeaderId() const;
    uint64_t GetLastIndex() const;
    std::vector<uint32_t> GetVoterIds() const;
    // Snapshot of validated committed and latest log configurations. This
    // distinguishes the active joint quorum from an uncommitted transition.
    TConfigurationScan GetConfigurationState() const;
    // Point-in-time report of the membership-specific compaction barrier.
    // Serialize this call with Raft work, like CompactShared().
    TCompactionMembershipReadiness CheckCompactionMembershipReadiness() const;
    std::vector<uint32_t> GetLearnerIds() const { return Learners; }
    bool IsVoter(uint32_t id) const;
    // Leader-only, point-in-time log catch-up; not permission to promote.
    bool IsLearnerCaughtUp(uint32_t id) const;
    // True when a correlated ACK confirms the learner's Raft commit boundary
    // through this leader's current commit index, and its log matches the leader.
    // This does not attest to application progress or authorize promotion.
    bool IsLearnerCommitAcknowledged(uint32_t id) const;
    // Register or remove a local peer endpoint on any Raft role. This changes
    // only this instance's transport targets; it does not change voter
    // membership or persist/distribute the endpoint. A registered nonvoter
    // is treated as a learner locally. Calls must be serialized with Raft work.
    void RegisterPeerTarget(uint32_t id, std::shared_ptr<INode> node);
    std::shared_ptr<INode> UnregisterPeerTarget(uint32_t id);
    // Leader-local replication staging; requires bound voter storage. Caller
    // owns endpoint lifecycle and reconciles targets after failover/restart.
    void AddLearnerTarget(uint32_t id, std::shared_ptr<INode> node);
    std::shared_ptr<INode> RemoveLearnerTarget(uint32_t id);
    // Capture applied state and atomically replace its prefix with a snapshot.
    // Membership-bearing voter snapshots require an all-voter commit ACK
    // proof: leaders derive it locally, followers receive a retryable barrier.
    // Throws without mutation while two distinct generations remain pinned by
    // outgoing peer transfers; callers may retry after a transfer completes.
    TSnapshot Compact();
    // Shared variant for callers that can consume the immutable image by
    // reference. Backends that support shared snapshot installation can keep
    // one application-data image; legacy backends may copy it.
    std::shared_ptr<const TSnapshot> CompactShared();
    uint64_t ApproveRead();
    // Highest barrier confirmed by a fresh majority in this leadership term.
    // Zero means no confirmation (including before a current-term commit).
    uint64_t CommitSeqno() const;

// ut
    const auto& GetState() const {
        return State;
    }

    EState CurrentStateName() const {
        return StateName;
    }

    void Become(EState newStateName);

    const TVolatileState* GetVolatileState() const {
        return VolatileState.get();
    }

    const uint32_t GetId() const {
        return Id;
    }

    int GetMinVotes() const {
        // Stable configurations return their strict-majority threshold. Joint
        // configurations return the larger of the two thresholds; callers
        // must use protocol outcomes, not this scalar, to decide a quorum.
        return MinVotes;
    }

    int GetNpeers() const {
        return Npeers;
    }

    int GetNservers() const {
        // Voting members only; GetNpeers counts all remote replication targets.
        return Nservers;
    }

private:
    friend class TRequestProcessor;
    friend TSnapshot CaptureSnapshot(const TRaft&, const IRsm&);
    friend void RestoreSnapshot(const TSnapshot&, const TRaft&, IRsm&);
    void RestoreAppliedIndex(uint64_t index);
    void AdvanceCommitIndex(uint64_t index);
    void AdvanceLeaderCommit();
    void OnInstallSnapshot(ITimeSource::Time now, TMessageHolder<TInstallSnapshotRequest> message);
    void OnInstallSnapshot(ITimeSource::Time now, TMessageHolder<TInstallSnapshotResponse> message);
    void OnTimeoutNow(ITimeSource::Time now, TMessageHolder<TTimeoutNowRequest> message);
    void OnConfigurationBarrier(TMessageHolder<TConfigurationBarrierRequest> message);
    void OnConfigurationBarrier(ITimeSource::Time now, TMessageHolder<TConfigurationBarrierResponse> message);
    void MaybeDistributeConfigurationBarrier(ITimeSource::Time now);
    TMessageHolder<TInstallSnapshotRequest> CreateInstallSnapshot(uint32_t nodeId);
    void Candidate(ITimeSource::Time now, TMessageHolder<TMessage> message);
    void Follower(ITimeSource::Time now, TMessageHolder<TMessage> message);
    void Leader(ITimeSource::Time now, TMessageHolder<TMessage> message);

    void OnRequestVote(ITimeSource::Time now, TMessageHolder<TRequestVoteRequest> message);
    void OnRequestVote(TMessageHolder<TRequestVoteResponse> message);
    void OnPreVote(ITimeSource::Time now, TMessageHolder<TPreVoteRequest> message);
    void OnPreVote(TMessageHolder<TPreVoteResponse> message);
    void OnAppendEntries(ITimeSource::Time now, TMessageHolder<TAppendEntriesRequest> message);
    void OnAppendEntries(ITimeSource::Time now, TMessageHolder<TAppendEntriesResponse> message);

    void LeaderTimeout(ITimeSource::Time now);
    void CandidateTimeout(ITimeSource::Time now);
    void FollowerTimeout(ITimeSource::Time now);
    std::optional<ITimeSource::Time> LatestLeadershipQuorumResponse(ITimeSource::Time now) const;
    bool HasRecentLeadershipQuorum(ITimeSource::Time now) const;
    void RecordLeadershipResponse(ITimeSource::Time now, uint32_t nodeId);
    ITimeSource::Time LeadershipExpirationDeadline(ITimeSource::Time latestResponse) const;
    void ExpireLeadership(ITimeSource::Time now);

    TMessageHolder<TRequestVoteRequest> CreateVote(uint32_t nodeId);
    TMessageHolder<TPreVoteRequest> CreatePreVote(uint32_t nodeId);
    void StartPreVote(ITimeSource::Time now);
    void StartElection(ITimeSource::Time now);
    TMessageHolder<TAppendEntriesRequest> CreateAppendEntries(
        uint32_t nodeId, bool snapshotBoundaryProbe = false);
    ITimeSource::Time MakeElection(ITimeSource::Time now);
    uint64_t NextSeqno();
    bool ValidateRpc(TMessageHolder<TMessage> message) const;
    bool ValidateAppendConfiguration(TMessageHolder<TAppendEntriesRequest> message) const;
    void RequireLiveConfigurationStorage() const;
    void RefreshConfiguration(uint64_t commitIndex);
    void RebuildUncommittedLogUsage();
    void PruneCommittedLogUsage(uint64_t commitIndex);
    void RemoveUncommittedLogSuffix(uint64_t firstIndex);
    void RecordUncommittedLogEntry(uint64_t index, uint32_t bytes);
    bool FitsUncommittedLog(uint64_t bytes, size_t entries) const;
    bool FitsProposalLog(uint64_t bytes, size_t entries,
        bool preserveStableSuccessor = true) const;
    void EnsureProposalLogCapacity(uint64_t bytes, size_t entries,
        bool preserveStableSuccessor = true) const;

    uint32_t Id;
    TNodeDict Nodes;
    // InitialVoters is the immutable storage binding. Voters is the active
    // voting union (the union of both sets while a joint configuration is in
    // force); Learners is derived from that active membership.
    std::vector<uint32_t> InitialVoters, Voters, Learners;
    std::optional<TConfigurationHistory> ConfigurationBaseline;
    std::optional<TConfigurationScan> Configuration;
    bool ConfigurationScanRequired = false;
    std::optional<TQuorum> VotingQuorum;
    int MinVotes;
    int Npeers;
    int Nservers;
    std::shared_ptr<IState> State;
    TLogLimits LogLimits;
    std::shared_ptr<TSnapshotInstallBudget> SnapshotInstallBudget;
    struct TUncommittedLogEntry { uint64_t Index; uint32_t Bytes; };
    std::deque<TUncommittedLogEntry> UncommittedLog;
    uint64_t UncommittedLogBytes = 0;
    std::unique_ptr<TVolatileState> VolatileState;
    uint64_t Seqno = 1; // zero is reserved for an unconfirmed read
    std::weak_ptr<IRsm> SnapshotApplication;
    struct TIncomingSnapshot {
        uint64_t Term, Index, BoundaryTerm, Total;
        uint32_t Leader;
        ITimeSource::Time LastProgress;
        std::string Bytes;
        uint64_t ReceivedBytes = 0;
        bool Preallocated = false;
        std::shared_ptr<void> MemoryReservation;
    };
    std::optional<TIncomingSnapshot> IncomingSnapshot;
    std::shared_ptr<const TSnapshotEncoding> EncodedSnapshot;
    uint64_t EncodedSnapshotIndex = 0;
    std::shared_ptr<const TSnapshotEncoding> EncodedSnapshotExtendedHistory;
    uint64_t EncodedSnapshotExtendedHistoryIndex = 0;
    std::shared_ptr<const TSnapshotEncoding> EncodedSnapshotLegacyHistory;
    uint64_t EncodedSnapshotLegacyHistoryIndex = 0;
    std::shared_ptr<const TSnapshotEncoding> EncodedSnapshotLineage;
    uint64_t EncodedSnapshotLineageIndex = 0;
    std::shared_ptr<const TSnapshotEncoding> EncodedSnapshotBaseFormat;
    uint64_t EncodedSnapshotBaseFormatIndex = 0;
    ITimeSource::Time LastRaftTime{};

    EState StateName;
    uint32_t Seed = 31337;
};

struct TRequestProcessorTestAccess;

class TRequestProcessor {
public:
    struct TAdmissionLimits {
        size_t MaxPendingRequests = 1024;
        uint64_t MaxPendingRequestBytes = 64 * 1024 * 1024;
        uint64_t MaxPendingReplyBytes = 64 * 1024 * 1024;
        // End-to-end lifetime from admission, including leader discovery and forwarding.
        ITimeSource::Time::duration RequestTimeout = std::chrono::seconds(30);
    };

    // Construct after restoring the RSM and before accepting network traffic.
    TRequestProcessor(std::shared_ptr<TRaft> raft, std::shared_ptr<IRsm> rsm, const TNodeDict& nodes);
    TRequestProcessor(std::shared_ptr<TRaft> raft, std::shared_ptr<IRsm> rsm,
                      const TNodeDict& nodes, TAdmissionLimits admissionLimits);
    TRequestProcessor(std::shared_ptr<TRaft> raft, std::shared_ptr<IRsm> rsm,
                      const TNodeDict& nodes, TAdmissionLimits admissionLimits,
                      std::shared_ptr<ITimeSource> timeSource);

    void CheckStateChange();
    void OnCommandRequest(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo);
    void OnForwardedCommandRequest(TMessageHolder<TForwardedCommandRequest> message,
                                   const std::shared_ptr<INode>& peer);
    void OnSessionForwardedCommandRequest(TMessageHolder<TSessionForwardedCommandRequest> message,
                                          const std::shared_ptr<INode>& peer);
    // Unattributed responses are ignored; pass the configured outbound peer.
    void OnCommandResponse(TMessageHolder<TCommandResponse> message, const std::shared_ptr<INode>& peer = {});
    void OnForwardedCommandResponse(TMessageHolder<TForwardedCommandResponse> message,
                                    const std::shared_ptr<INode>& peer);
    void OnSessionForwardedCommandResponse(TMessageHolder<TSessionForwardedCommandResponse> message,
                                          const std::shared_ptr<INode>& peer);
    void OnPeerDisconnected(const std::shared_ptr<INode>& peer);
    // Keep command forwarding aligned with runtime peer endpoint changes.
    void AddPeerEndpoint(uint32_t id, std::shared_ptr<INode> peer);
    void RemovePeerEndpoint(uint32_t id, const std::shared_ptr<INode>& peer) noexcept;
    void ProcessCommitted();
    void ProcessWaiting();
    void CleanUp(const std::shared_ptr<INode>& replyTo);

private:
    // Test seam for exercising the wire-cookie boundary without billions of requests.
    friend struct TRequestProcessorTestAccess;
    bool TryAdmit(const TMessageHolder<TCommandRequest>& message, const std::shared_ptr<INode>& replyTo,
                  uint64_t& admissionId);
    bool AdmissionExpired(uint64_t admissionId, ITimeSource::Time now) const;
    void ReleaseAdmission(uint64_t admissionId) noexcept;
    void EraseWriteAnswer(uint64_t index) noexcept;
    void DispatchCommandRequest(TMessageHolder<TCommandRequest> message,
                                const std::shared_ptr<INode>& replyTo, uint64_t admissionId);
    void Forward(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                 uint64_t admissionId);
    void OnReadRequest(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                       uint64_t admissionId);
    void OnWriteRequest(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                        uint64_t admissionId);
    void PruneRequests(const std::shared_ptr<INode>& disconnected = {});
    void ExpireRequests(ITimeSource::Time now, const std::shared_ptr<INode>& disconnected = {});
    void CancelForwarded(const std::shared_ptr<INode>& disconnected = {}, bool peerFailure = false);

    std::shared_ptr<TRaft> Raft;
    std::shared_ptr<IRsm> Rsm;
    TNodeDict Nodes;
    TAdmissionLimits AdmissionLimits;
    std::shared_ptr<ITimeSource> TimeSource;
    struct TAdmission { uint64_t Bytes; ITimeSource::Time Deadline; };
    std::unordered_map<uint64_t, TAdmission> Admissions;
    uint64_t PendingRequestBytes = 0;
    uint64_t PendingReplyBytes = 0;
    uint64_t NextAdmissionId = 1;

    struct TWaiting {
        uint64_t Index;
        uint64_t Seqno = 0;
        TMessageHolder<TCommandRequest> Command;
        std::shared_ptr<INode> ReplyTo;
        // Zero is reserved for requests not owned by a leadership term.
        uint64_t Term = 0;
        uint64_t AdmissionId = 0;
    };
    std::queue<TWaiting> Waiting;
    std::queue<TWaiting> StrongWaiting;
    std::queue<TWaiting> WaitingStateChange;

    struct TAnswer {
        uint64_t Term;
        TMessageHolder<TMessage> Reply;
    };
    // Retain application replies only for live local writes, matched by index
    // and term. An overwritten index must never redeem an older request.
    std::unordered_map<uint64_t, uint64_t> PendingWriteTerms;
    std::unordered_map<uint64_t, TAnswer> WriteAnswers;
    uint64_t RequestsTerm = 0;
    EState RequestsState = EState::NONE;
    uint32_t RequestsLeaderId = 0;
    struct TForwarded {
        std::shared_ptr<INode> Client, Peer;
        uint32_t OriginalCookie, LeaderId;
        uint64_t Term;
        uint64_t AdmissionId;
        bool Sessionized;
    };
    // Never reuse a wire correlation ID within this processor, including after
    // failure. IDs above uint32 use the extended forwarded-command envelope.
    uint64_t NextForwardId = 1;
    uint64_t ForwardingSession = 0;
    std::unordered_map<uint64_t, TForwarded> Forwarded;
};

