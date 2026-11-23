#pragma once

#include <memory>
#include <optional>
#include <queue>
#include <deque>
#include <span>
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
#include "idempotency.h"

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

// Immutable, range-readable application snapshot data. A source must remain
// stable until released and represent the requested applied index. Read returns
// the number of bytes copied; zero before Size() is reached is an error.
struct ISnapshotSource {
    virtual ~ISnapshotSource() = default;
    virtual uint64_t Size() const = 0;
    virtual size_t Read(uint64_t offset, std::span<char> destination) const = 0;
};

// Optional application contract for range-readable snapshot capture. Keeping
// this separate from IRsm preserves the existing application's virtual ABI.
struct IStreamingSnapshotRsm {
    virtual ~IStreamingSnapshotRsm() = default;
    virtual std::shared_ptr<const ISnapshotSource> OpenSnapshotSource(uint64_t index) const = 0;
};

// Optional incremental capture contract. When present, Raft prefers this writer
// over SaveSnapshot(); an available range-readable source takes precedence.
// Writes are consumed synchronously; the sink applies the Raft image limit and
// charges each growth before it allocates the corresponding bytes. Implementors
// should propagate Write failures and emit one immutable image for the requested
// applied index.
struct ISnapshotSink {
    virtual ~ISnapshotSink() = default;
    virtual void Write(std::span<const char> bytes) = 0;
};

struct IStreamingSnapshotWriterRsm {
    virtual ~IStreamingSnapshotWriterRsm() = default;
    virtual void WriteSnapshot(uint64_t index, ISnapshotSink& sink) const = 0;
};

// Optional two-phase application snapshot installation. Preparation validates
// and stages an image without changing application state. Install publishes
// that same staged image after Raft has durably published its snapshot. The
// installer must be installed or destroyed before its RSM is destroyed.
struct IStagedSnapshotInstallation {
    virtual ~IStagedSnapshotInstallation() = default;
    virtual void Install() = 0;
};

struct IStagedSnapshotRsm {
    virtual ~IStagedSnapshotRsm() = default;
    virtual std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
        std::string_view bytes, uint64_t index) = 0;
};

// Optional range-based counterpart for RSMs that can validate and stage a
// snapshot without first materializing its application payload in memory.
struct IStreamingSnapshotInstallRsm {
    virtual ~IStreamingSnapshotInstallRsm() = default;
    virtual std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
        const TSnapshotDataSource& source, uint64_t index) = 0;
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
    // Optional upper bound for SaveSnapshot(), IStreamingSnapshotRsm source data,
    // or IStreamingSnapshotWriterRsm output. Raft checks it against the
    // metadata-adjusted image limit. The writer sink can charge output
    // incrementally when no bound is available; legacy SaveSnapshot() capture
    // without a bound can only be charged after the callback returns.
    virtual std::optional<uint64_t> SnapshotSizeUpperBound() const { return std::nullopt; }
    virtual void ValidateSnapshot(std::string_view, uint64_t) const { throw std::logic_error("Application snapshots are unsupported"); }
    // Implementations must validate completely before atomically replacing state
    // and LastAppliedIndex. Invoked at startup or during serialized installation;
    // a failure after durable installation is fatal and requires restart.
    virtual void LoadSnapshot(std::string_view, uint64_t) { throw std::logic_error("Application snapshots are unsupported"); }
    // True only when this application can validate and atomically restore the
    // snapshots it emits. Storage install support alone is not sufficient for a
    // peer that may receive a compacted prefix.
    virtual bool SupportsApplicationSnapshotInstall() const { return false; }

    // Durable RSMs restore this atomically with application data before constructing
    // TRequestProcessor. It must identify committed progress in the same Raft log.
    uint64_t LastAppliedIndex = 0;
};

inline std::unique_ptr<IStagedSnapshotInstallation> PrepareApplicationSnapshotInstallation(
        IRsm& rsm, std::string_view bytes, uint64_t index) {
    if (auto* staged = dynamic_cast<IStagedSnapshotRsm*>(&rsm)) {
        auto installation = staged->PrepareSnapshotInstallation(bytes, index);
        if (!installation) throw std::logic_error("Staged snapshot preparation returned no installation");
        return installation;
    }
    rsm.ValidateSnapshot(bytes, index);
    return {};
}

inline void InstallApplicationSnapshot(IRsm& rsm, std::string_view bytes, uint64_t index,
        std::unique_ptr<IStagedSnapshotInstallation> installation) {
    if (installation) installation->Install();
    else rsm.LoadSnapshot(bytes, index);
}

inline std::unique_ptr<IStagedSnapshotInstallation> PrepareApplicationSnapshotInstallation(
        IRsm& rsm, const TSnapshot& snapshot) {
    if (snapshot.DataSource) {
        if (auto* streaming = dynamic_cast<IStreamingSnapshotInstallRsm*>(&rsm)) {
            auto installation = streaming->PrepareSnapshotInstallation(*snapshot.DataSource, snapshot.Index);
            if (!installation) throw std::logic_error("Streaming snapshot preparation returned no installation");
            return installation;
        }
        const auto bytes = snapshot.MaterializeData();
        return PrepareApplicationSnapshotInstallation(rsm, bytes, snapshot.Index);
    }
    return PrepareApplicationSnapshotInstallation(rsm, snapshot.Data, snapshot.Index);
}

inline void InstallApplicationSnapshot(IRsm& rsm, const TSnapshot& snapshot,
        std::unique_ptr<IStagedSnapshotInstallation> installation) {
    if (installation) {
        installation->Install();
    } else if (snapshot.DataSource) {
        const auto bytes = snapshot.MaterializeData();
        rsm.LoadSnapshot(bytes, snapshot.Index);
    } else {
        rsm.LoadSnapshot(snapshot.Data, snapshot.Index);
    }
}

struct TIdempotentWriteResult {
    uint64_t Index = 0;
    uint32_t ErrorCode = TCommandResponse::Success;
    std::string Data;
};

// Optional application contract for durable write deduplication. Implementors
// must apply a new operation and update its session record atomically. On log
// replay, an exact latest-operation duplicate returns the stored result without
// reapplying; identity reuse with another digest and invalid sequence/epoch
// transitions return deterministic error results without mutating application
// state. Snapshot capture and restore cover application data, epoch, all active
// sessions/tombstones, cached results and LastAppliedIndex as one state image.
struct IIdempotentRsm : IRsm {
    explicit IIdempotentRsm(TIdempotencySnapshotLimits limits = {})
        : SnapshotLimits(std::move(limits)) {
        ValidateIdempotencySnapshotLimits(SnapshotLimits);
    }

    virtual void ValidateIdempotent(TMessageHolder<TIdempotentCommandRequest>) const = 0;
    // Bound for cached response data, excluding the fixed response frame.
    virtual uint64_t CachedResponseDataUpperBound(
        TMessageHolder<TIdempotentCommandRequest>) const = 0;
    uint64_t IdempotentResponseSizeUpperBound(
            TMessageHolder<TIdempotentCommandRequest> request) const {
        const auto bound = CachedResponseDataUpperBound(std::move(request));
        if (bound > SnapshotLimits.MaxResponseBytes)
            throw TMessageError("Idempotent response bound exceeds the configured limit");
        return bound;
    }
    virtual TMessageHolder<TLogEntry> PrepareIdempotent(
        TMessageHolder<TIdempotentCommandRequest>) = 0;
    // Atomically apply/deduplicate the command, update its session record, and
    // advance inherited LastAppliedIndex to index before returning.
    virtual TIdempotentWriteResult WriteIdempotent(
        TMessageHolder<TLogEntry>, uint64_t index,
        const TIdempotentOperationIdentity& identity) = 0;
    TIdempotentWriteResult ApplyIdempotentLogRecord(TMessageHolder<TLogEntry> entry, uint64_t index) {
        if (!index) throw std::invalid_argument("Idempotent log application requires a positive index");
        auto decoded = DecodeIdempotentLogEntry(std::move(entry));
        auto result = WriteIdempotent(std::move(decoded.ApplicationEntry), index, decoded.Identity);
        if (LastAppliedIndex != index || result.ErrorCode > TCommandResponse::AlreadyCompleted || result.Index > index ||
            (result.ErrorCode == TCommandResponse::Success && !result.Index) ||
            result.Data.size() > SnapshotLimits.MaxResponseBytes)
            throw std::logic_error("Idempotent RSM violated its atomic apply/result contract");
        return result;
    }
    // Validation is read-only and checks the operation against the state
    // preceding its log index. Preparation returns a deterministic command
    // without publishing application or session state before commit.
    virtual void ValidateIdempotencyControl(
        const TIdempotencyControlOperation&) const = 0;
    virtual TMessageHolder<TLogEntry> PrepareIdempotencyControl(
        const TIdempotencyControlOperation&) = 0;
    // Committed replay validates the operation again, then atomically applies
    // both the prepared entry and session-control transition and advances
    // inherited LastAppliedIndex to index.
    virtual void ApplyIdempotencyControl(
        const TIdempotencyControlOperation&, TMessageHolder<TLogEntry>, uint64_t index) = 0;

    // Capture returns application data and dedup state together. Validation must
    // not mutate state; restore must publish the whole image and LastAppliedIndex
    // only after the complete image has passed validation.
    virtual TIdempotencySnapshot CaptureIdempotencySnapshot() const = 0;
    virtual void ValidateIdempotencySnapshot(
        const TIdempotencySnapshot&, uint64_t index) const = 0;
    virtual void RestoreIdempotencySnapshot(
        TIdempotencySnapshot, uint64_t index) = 0;
    bool SupportsApplicationSnapshotInstall() const final { return true; }
    // Upper bound for the complete encoded wrapper returned by SaveSnapshot or
    // emitted by the disk-staged writer path.
    virtual std::optional<uint64_t> IdempotencySnapshotSizeUpperBound() const {
        return std::nullopt;
    }

    std::string SaveSnapshot() const final {
        auto snapshot = CaptureIdempotencySnapshot();
        if (snapshot.LastAppliedIndex != LastAppliedIndex)
            throw std::logic_error("Idempotency snapshot progress does not match the RSM");
        auto bytes = EncodeIdempotencySnapshot(snapshot, SnapshotLimits);
        const auto bound = IdempotencySnapshotSizeUpperBound();
        if (bound && bytes.size() > *bound)
            throw std::logic_error("Idempotency snapshot exceeded its declared size bound");
        return bytes;
    }
    // Disk-staged capture can emit the wrapper directly without constructing a
    // second full encoded string. This is deliberately non-virtual so the
    // existing IIdempotentRsm vtable and application ABI remain unchanged.
    void WriteSnapshot(uint64_t index, ISnapshotSink& sink) const {
        auto snapshot = CaptureIdempotencySnapshot();
        if (index != LastAppliedIndex || snapshot.LastAppliedIndex != LastAppliedIndex)
            throw std::logic_error("Idempotency snapshot progress does not match the RSM");
        const auto bytes = ::WriteIdempotencySnapshot(snapshot, SnapshotLimits, sink);
        const auto bound = IdempotencySnapshotSizeUpperBound();
        if (bound && bytes > *bound)
            throw std::logic_error("Idempotency snapshot exceeded its declared size bound");
    }
    std::optional<uint64_t> SnapshotSizeUpperBound() const final {
        auto bound = IdempotencySnapshotSizeUpperBound();
        if (bound && *bound > SnapshotLimits.MaxEncodedBytes)
            throw std::runtime_error("Idempotency snapshot size bound exceeds its configured limit");
        return bound;
    }
    void ValidateSnapshot(std::string_view bytes, uint64_t index) const final {
        auto snapshot = DecodeIdempotencySnapshot(bytes, SnapshotLimits);
        if (snapshot.LastAppliedIndex != index)
            throw std::runtime_error("Idempotency snapshot progress does not match its Raft boundary");
        ValidateIdempotencySnapshot(snapshot, index);
    }
    void LoadSnapshot(std::string_view bytes, uint64_t index) final {
        auto snapshot = DecodeIdempotencySnapshot(bytes, SnapshotLimits);
        if (snapshot.LastAppliedIndex != index)
            throw std::runtime_error("Idempotency snapshot progress does not match its Raft boundary");
        ValidateIdempotencySnapshot(snapshot, index);
        RestoreIdempotencySnapshot(std::move(snapshot), index);
        if (LastAppliedIndex != index)
            throw std::runtime_error("Idempotent RSM failed to restore snapshot progress");
    }

protected:
    const TIdempotencySnapshotLimits SnapshotLimits;
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
    // Every voter in the proposed stable set must be able to restore application
    // data from an installed Raft snapshot.
    std::vector<uint32_t> ProposedVotersWithoutApplicationSnapshotInstallSupport;
    // Every current and proposed voter must understand joint-consensus entries.
    std::vector<uint32_t> VotersWithoutJointConsensusSupport;
    // When this RSM enables idempotent writes, every replication target must
    // understand tagged idempotent log records before membership can change.
    std::vector<uint32_t> MembersWithoutIdempotentLogSupport;
    // Session controls have a separate capability bit for safe mixed-version
    // operation, even when the peer supports idempotent command records.
    std::vector<uint32_t> MembersWithoutIdempotencyControlSupport;

    bool Ready() const {
        return IsLeader && HasDurableMembershipStorage && HasStableConfiguration &&
            HasCommittedCurrentTermEntry && ChangesVoterSet && HasLogCapacity && MissingPeerTargets.empty() &&
            AddedVotersWithoutCommitAck.empty() && AddedVotersWithoutSnapshotInstallSupport.empty() &&
            ProposedVotersWithoutApplicationSnapshotInstallSupport.empty() &&
            VotersWithoutJointConsensusSupport.empty() && MembersWithoutIdempotentLogSupport.empty() &&
            MembersWithoutIdempotencyControlSupport.empty();
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
    bool LocalApplicationSnapshotInstallSupported = false;
    // Populated on leaders with active voters still behind the committed config.
    std::vector<uint32_t> ActiveVotersWithoutConfigurationAck;
    // Remote active voters whose InstallSnapshot support is not positively known.
    std::vector<uint32_t> ActiveVotersWithoutSnapshotInstallSupport;
    // Remote active voters whose RSM cannot restore an installed snapshot.
    std::vector<uint32_t> ActiveVotersWithoutApplicationSnapshotInstallSupport;

    bool Ready() const {
        return LocalApplicationSnapshotInstallSupported &&
            ActiveVotersWithoutConfigurationAck.empty() &&
            ActiveVotersWithoutSnapshotInstallSupport.empty() &&
            ActiveVotersWithoutApplicationSnapshotInstallSupport.empty() &&
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
        uint64_t MaxSnapshotBytes = TSnapshot::LegacyMaxBytes;
        bool ConfigurationBarrier = false, SnapshotLineage = false;
        bool SnapshotHistory32 = false, SnapshotHistory256 = false;
        bool SnapshotHistory1024 = false;
        bool InstallSnapshot = false;
        bool ApplicationSnapshotInstall = false;
        bool JointConsensus = false;
        bool IdempotentLogEntries = false;
        bool IdempotencyControlEntries = false;
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
    // Minimum image size accepted by all active voters, certified by the leader.
    uint64_t CompactionSafeMaxSnapshotBytes = TSnapshot::LegacyMaxBytes;
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
        // shared budget here to override the process-wide snapshot receive
        // heap cap, including decoding of completed staging files.
        std::shared_ptr<TSnapshotInstallBudget> SnapshotInstallBudget;
        // On Linux, stage streamed application captures and full incoming
        // snapshot images in anonymous files in this directory.
        std::string SnapshotStagingDirectory;
        // Share this separately from SnapshotInstallBudget to limit staged
        // file bytes. Defaults to a separate 256 MiB process-wide budget.
        std::shared_ptr<TSnapshotInstallBudget> SnapshotStagingBudget;
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
    // Set from the request processor's actual RSM contract before serving
    // traffic. Peers advertise the capability only when their RSM can apply it.
    void SetLocalIdempotentLogSupport(bool supported) noexcept {
        LocalIdempotentLogSupport = supported;
    }
    // Set from the request processor's actual application contract before the
    // node serves traffic; peers advertise this independently of storage support.
    void SetLocalApplicationSnapshotInstallSupport(bool supported) noexcept {
        LocalApplicationSnapshotInstallSupport = supported;
    }
    bool CanAppendIdempotentLogEntries() const;
    bool CanAppendIdempotencyControlEntries() const;
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
    // Replace a configured endpoint without changing its local voter role.
    // All replication and capability evidence for that peer is discarded.
    void ReplacePeerTarget(uint32_t id, std::shared_ptr<INode> node);
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
    friend TSnapshot CaptureSnapshot(const TRaft&, const IRsm&,
        std::shared_ptr<TSnapshotInstallBudget>);
    friend TSnapshot CaptureSnapshot(const TRaft&, const IRsm&,
        std::shared_ptr<TSnapshotInstallBudget>,
        std::shared_ptr<TSnapshotInstallBudget>, std::string_view);
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
    TMessageHolder<TMessage> CreateInstallSnapshot(uint32_t nodeId);
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
    void AdvanceTerm(ITimeSource::Time now, uint64_t term, bool preserveIncomingSnapshot = false);
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
    bool LocalIdempotentLogSupport = false;
    bool LocalApplicationSnapshotInstallSupport = false;
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
    class TIncomingSnapshotFile;
    struct TIncomingSnapshot {
        struct TMetadataContext {
            bool AllowFreshLearnerBootstrap;
            bool AllowStaleStoreRecovery;
            bool operator==(const TMetadataContext&) const = default;
        };
        uint64_t Term, Index, BoundaryTerm, Total;
        uint32_t Leader;
        ITimeSource::Time LastProgress;
        std::string Bytes;
        uint64_t ReceivedBytes = 0;
        bool Preallocated = false;
        std::shared_ptr<void> ReceiveReservation;
        std::unique_ptr<char[]> CoveredPrefix;
        size_t CoveredPrefixCapacity = 0;
        std::shared_ptr<TIncomingSnapshotFile> File;
        std::optional<TMetadataContext> ValidatedMetadata;
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
    // Idempotent writes are forwarded from followers only over a negotiated
    // sessionized route; the leader also gates on RSM and replica support.
    void OnIdempotentCommandRequest(TMessageHolder<TIdempotentCommandRequest> message,
                                    const std::shared_ptr<INode>& replyTo);
    // Replicate a session close or epoch advance. The returned index identifies
    // the control record; application state changes only when it is applied.
    uint64_t ProposeIdempotencyControl(TIdempotencyControlOperation operation);
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
    bool ReplacePeerEndpoint(uint32_t id, const std::shared_ptr<INode>& oldPeer,
                             const std::shared_ptr<INode>& newPeer) noexcept;
    void RemovePeerEndpoint(uint32_t id, const std::shared_ptr<INode>& peer) noexcept;
    void ProcessCommitted();
    void ProcessWaiting();
    void CleanUp(const std::shared_ptr<INode>& replyTo);

private:
    // Test seam for exercising the wire-cookie boundary without billions of requests.
    friend struct TRequestProcessorTestAccess;
    bool TryAdmit(const TMessageHolder<TCommandRequest>& message, const std::shared_ptr<INode>& replyTo,
                  uint64_t& admissionId);
    bool TryAdmit(uint64_t bytes, const std::shared_ptr<INode>& replyTo, uint64_t& admissionId);
    bool AdmissionExpired(uint64_t admissionId, ITimeSource::Time now) const;
    void ReleaseAdmission(uint64_t admissionId) noexcept;
    void ReleaseIdempotentPending(uint64_t index) noexcept;
    void EraseWriteAnswer(uint64_t index) noexcept;
    void DispatchCommandRequest(TMessageHolder<TCommandRequest> message,
                                const std::shared_ptr<INode>& replyTo, uint64_t admissionId);
    void Forward(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                 uint64_t admissionId);
    void ForwardIdempotent(TMessageHolder<TIdempotentCommandRequest> message,
                           const TIdempotentOperationIdentity& identity,
                           const std::shared_ptr<INode>& replyTo, uint64_t admissionId,
                           uint64_t& reservedReplyBytes);
    void OnReadRequest(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                       uint64_t admissionId);
    void OnWriteRequest(TMessageHolder<TCommandRequest> message, const std::shared_ptr<INode>& replyTo,
                        uint64_t admissionId);
    void OnIdempotentCommandRequestInternal(TMessageHolder<TIdempotentCommandRequest> message,
                                            const std::shared_ptr<INode>& replyTo, bool forwarded);
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
    struct TIdempotentPending {
        uint64_t Term = 0;
        TIdempotentOperationIdentity Identity;
        uint32_t Cookie = 0;
        std::shared_ptr<INode> ReplyTo;
        uint64_t AdmissionId = 0;
        uint64_t ReservedReplyBytes = 0;
    };
    std::unordered_map<uint64_t, TIdempotentPending> PendingIdempotentWrites;
    uint64_t RequestsTerm = 0;
    EState RequestsState = EState::NONE;
    uint32_t RequestsLeaderId = 0;
    struct TForwarded {
        std::shared_ptr<INode> Client, Peer;
        uint32_t OriginalCookie, LeaderId;
        uint64_t Term;
        uint64_t AdmissionId;
        bool Sessionized;
        bool Idempotent = false;
        TIdempotentOperationIdentity IdempotentIdentity;
        uint64_t ReservedReplyBytes = 0;
    };
    // Never reuse a wire correlation ID within this processor, including after
    // failure. IDs above uint32 use the extended forwarded-command envelope.
    uint64_t NextForwardId = 1;
    uint64_t ForwardingSession = 0;
    std::unordered_map<uint64_t, TForwarded> Forwarded;
};

