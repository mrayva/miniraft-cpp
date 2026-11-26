#pragma once

#include "messages.h"
#include "snapshot.h"
#include <algorithm>

struct TStateFields {
    uint64_t CurrentTerm = 1;
    uint32_t VotedFor = 0;
    uint64_t LastLogIndex = 0;
    uint64_t LastLogTerm = 0;
    uint64_t SnapshotIndex = 0;
    uint64_t SnapshotTerm = 0;
    // Persisted commit boundary where the storage backend supports it. Older
    // formats conservatively recover this as SnapshotIndex.
    uint64_t CommitIndex = 0;

    bool operator==(const TStateFields& other) const = default;
};

struct IState: TStateFields {
    virtual void RemoveLast() = 0;
    virtual void Append(TMessageHolder<TLogEntry>) = 0;
    // Backends may persist one replicated AppendEntries batch with a single
    // storage update. The default preserves the existing per-entry behavior.
    virtual void AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries) {
        for (const auto& entry : entries) Append(entry);
    }
    // Publish a replicated batch and its newly learned commit boundary as one
    // storage update when the backend supports it. The default preserves the
    // existing append-then-commit behavior.
    virtual void AppendBatchAndCommit(
            const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) {
        if (LastLogIndex > uint64_t(INT64_MAX) ||
            entries.size() > uint64_t(INT64_MAX) - LastLogIndex ||
            commitIndex < std::max(CommitIndex, SnapshotIndex) ||
            commitIndex > LastLogIndex + entries.size())
            throw std::logic_error("Invalid batched Raft commit boundary");
        AppendBatch(entries);
        if (CommitIndex < commitIndex) {
            CommitIndex = commitIndex;
            Commit();
        }
    }
    // Replace the retained suffix beginning at firstIndex, append the new
    // entries there, and publish the commit boundary with the resulting log.
    // The default preserves existing backend behavior; durable backends can
    // override it to publish the rewrite atomically.
    virtual void ReplaceSuffixAndAppendBatch(uint64_t firstIndex,
            const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) {
        if (LastLogIndex > uint64_t(INT64_MAX))
            throw std::overflow_error("Raft log index exhausted");
        const auto commitFloor = std::max(CommitIndex, SnapshotIndex);
        if (firstIndex <= commitFloor || firstIndex > LastLogIndex + 1)
            throw std::logic_error("Invalid Raft suffix replacement boundary");
        const auto retainedLastIndex = firstIndex - 1;
        if (entries.size() > uint64_t(INT64_MAX) - retainedLastIndex)
            throw std::overflow_error("Raft log index exhausted");
        const auto newLastLogIndex = retainedLastIndex + entries.size();
        if (commitIndex < commitFloor || commitIndex > newLastLogIndex)
            throw std::logic_error("Invalid Raft suffix replacement commit boundary");
        while (LastLogIndex >= firstIndex) RemoveLast();
        AppendBatchAndCommit(entries, commitIndex);
    }
    // Absolute indices; only the suffix after SnapshotIndex has log entries.
    // The boundary term remains available through LogTerm(SnapshotIndex).
    virtual TMessageHolder<TLogEntry> Get(int64_t index) const = 0;
    // Backends may read a retained entry's term without copying its payload.
    // The default keeps existing external backends source compatible.
    virtual uint64_t RetainedLogTerm(int64_t index) const {
        auto entry = Get(index);
        return entry ? entry->Term : 0;
    }
    // Used during startup to reject unsupported retained entry kinds.
    virtual uint64_t RetainedLogFlags(int64_t index) const {
        auto entry = Get(index);
        if (!entry) throw std::runtime_error("Missing retained log entry");
        return entry->Flags;
    }
    virtual void Commit() = 0;
    // True only when Commit() durably records CommitIndex and recovery restores
    // it. Membership protocols can require this before trusting recovered history.
    virtual bool SupportsCommitIndexPersistence() const { return false; }
    virtual bool HasTrustedCommitIndex() const { return SupportsCommitIndexPersistence(); }
    virtual bool SupportsSnapshots() const { return false; }
    // True when this backend can durably adopt an InstallSnapshot image.
    virtual bool SupportsInstallSnapshot() const { return false; }
    virtual std::shared_ptr<const TSnapshot> GetSnapshot() const { return {}; }
    // Optional application-owned immutable generation committed with the
    // Raft snapshot boundary. Backends that do not implement this must reject
    // a nonempty reference rather than silently dropping it.
    virtual std::optional<TApplicationSnapshotGenerationReference>
    GetApplicationSnapshotGenerationReference() const { return {}; }
    virtual void InstallSnapshot(const TSnapshot&) { throw std::logic_error("Storage snapshots are unsupported"); }
    // Raft calls this after validating configuration lineage, either from its
    // local scan during compaction or from the sender during snapshot transfer.
    // Backends may rely on that proof for a stable-set transition that
    // snapshot-only continuity cannot establish.
    virtual void InstallRaftSnapshot(const TSnapshot& snapshot,
            const class TRaftInstallSnapshotPermit&) { InstallSnapshot(snapshot); }
    // Raft may transfer a validated immutable snapshot into storage without
    // copying the application image. Existing backends keep the copy-based path.
    virtual void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
            const class TRaftInstallSnapshotPermit& permit) {
        if (!snapshot) throw std::invalid_argument("Missing Raft snapshot");
        InstallRaftSnapshot(*snapshot, permit);
    }
    virtual void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
            const class TRaftInstallSnapshotPermit& permit,
            std::optional<TApplicationSnapshotGenerationReference> generation) {
        if (generation)
            throw std::logic_error("Storage cannot atomically publish application snapshot generations");
        InstallRaftSnapshot(std::move(snapshot), permit);
    }
    virtual bool SupportsVoterBinding() const { return false; }
    // True when this backend recovered a persistent image without a stored
    // voter identity; Raft must obtain proof before serving this state.
    virtual bool HasUnboundRecoveredState() const { return false; }
    // Original trusted voter set persisted with this store. It may differ from
    // the active set in a configuration-bearing offline snapshot.
    virtual std::vector<uint32_t> GetBoundVoters() const { return {}; }
    virtual void BindVoters(const std::vector<uint32_t>&) { throw std::logic_error("Storage voter binding is unsupported"); }
    // Storage backends can close startup-only migration paths once a Raft
    // instance has finished validating and attached to this state.
    virtual void MarkRaftStarted() {}
    virtual ~IState() = default;

    uint64_t LogTerm(int64_t index = -1) const {
        if (index < 0) {
            index = LastLogIndex;
        }
        if (index > 0 && uint64_t(index) == SnapshotIndex) return SnapshotTerm;
        if (index < 1 || uint64_t(index) < SnapshotIndex || index > LastLogIndex) {
            return 0;
        } else {
            return RetainedLogTerm(index);
        }
    }
};

class TRaftInstallSnapshotPermit {
private:
    TRaftInstallSnapshotPermit() = default;
    friend class TRaft;
};

inline void ValidateFixedVoters(const std::vector<uint32_t>& voters) {
    if (voters.empty() || voters.size() > TSnapshot::MaxVoters) throw std::invalid_argument("Invalid voter count");
    uint32_t previous = 0;
    for (auto id : voters) {
        if (id <= previous) throw std::invalid_argument("Voters must be positive, sorted and unique");
        previous = id;
    }
}
inline void ValidateVoterBinding(const IState& state, const std::vector<uint32_t>& old, const std::vector<uint32_t>& voters) {
    ValidateFixedVoters(voters);
    if (state.VotedFor && !std::binary_search(voters.begin(), voters.end(), state.VotedFor))
        throw std::invalid_argument("Stored vote does not name a configured voter");
    if (!old.empty()) {
        if (old != voters) throw std::invalid_argument("Persisted voter configuration cannot be changed by restart");
    } else if (auto snapshot = state.GetSnapshot()) {
        if (snapshot->HasConfigurationMetadata())
            throw std::invalid_argument("Configuration snapshot recovery requires an existing voter binding");
        if (snapshot->Voters != voters) throw std::invalid_argument("Snapshot voter configuration mismatch");
    } else if (state.CurrentTerm != 1 || state.VotedFor || state.LastLogIndex) {
        throw std::invalid_argument("Unbound nonfresh store needs a verified snapshot before enabling learners");
    }
}

// A persisted voter binding identifies the original, trusted membership. For
// offline history storage, explicit configuration metadata allows the snapshot
// Voters field to describe a later active set while that original binding stays
// put. Metadata still requires an existing binding; it is not proof by itself
// and does not authorize runtime membership changes.
inline bool SnapshotCompatibleWithVoterBinding(const TSnapshot& snapshot,
    const std::vector<uint32_t>& voters) {
    if (snapshot.HasConfigurationMetadata()) return !voters.empty();
    return voters.empty() || snapshot.Voters == voters;
}

struct TState: IState {
    static TMessageHolder<TLogEntry> CloneEntry(const TMessageHolder<TLogEntry>& entry) {
        if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) || entry->Len < sizeof(TLogEntry))
            throw std::invalid_argument("Invalid in-memory log entry");
        auto copy = NewHoldedMessage<TLogEntry>(entry->Len);
        memcpy(copy.Mes, entry.Mes, entry->Len);
        return copy;
    }
    static void ValidateLogTerm(uint64_t term, uint64_t previousTerm, uint64_t currentTerm) {
        if (!term || term < previousTerm || term > currentTerm)
            throw std::invalid_argument("Raft log entry term outside current log range");
    }
    std::vector<TMessageHolder<TLogEntry>> CloneEntries(
            const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t previousTerm) const {
        std::vector<TMessageHolder<TLogEntry>> cloned;
        cloned.reserve(entries.size());
        for (const auto& entry : entries) {
            auto copy = CloneEntry(entry);
            ValidateLogTerm(copy->Term, previousTerm, CurrentTerm);
            previousTerm = copy->Term;
            cloned.push_back(std::move(copy));
        }
        return cloned;
    }
    std::vector<TMessageHolder<TLogEntry>> Log;
    std::shared_ptr<const TSnapshot> Snapshot;
    std::optional<TApplicationSnapshotGenerationReference> SnapshotGeneration;
    std::vector<uint32_t> BoundVoters;

    TState() = default;
    TState(uint64_t currentTerm, uint32_t votedFor, const std::vector<TMessageHolder<TLogEntry>>& log)
    {
        CurrentTerm = currentTerm;
        VotedFor = votedFor;
        Log = CloneEntries(log, SnapshotTerm);
        if (!log.empty()) {
            LastLogIndex = log.size();
            LastLogTerm = Log.back()->Term;
        }
    }
    TState(const TState& other) : IState(other), Snapshot(other.Snapshot),
        SnapshotGeneration(other.SnapshotGeneration), BoundVoters(other.BoundVoters) {
        for (const auto& entry : other.Log) Log.push_back(CloneEntry(entry));
    }
    TState& operator=(const TState& other) {
        if (this != &other) {
            TState copy(other);
            static_cast<TStateFields&>(*this) = copy;
            Log.swap(copy.Log); Snapshot.swap(copy.Snapshot);
            SnapshotGeneration.swap(copy.SnapshotGeneration); BoundVoters.swap(copy.BoundVoters);
        }
        return *this;
    }

    void RemoveLast() override {
        if (Log.empty()) return;
        if (LastLogIndex <= CommitIndex)
            throw std::logic_error("Cannot remove a committed log entry");
        Log.pop_back();
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.empty() ? SnapshotTerm : Log.back()->Term;
    }

    void Append(TMessageHolder<TLogEntry> entry) override {
        if (LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
        auto copy = CloneEntry(std::move(entry));
        ValidateLogTerm(copy->Term, LastLogTerm, CurrentTerm);
        Log.emplace_back(std::move(copy));
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.back()->Term;
    }

    void AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries) override {
        if (LastLogIndex > uint64_t(INT64_MAX) ||
            entries.size() > uint64_t(INT64_MAX) - LastLogIndex)
            throw std::overflow_error("Raft log index exhausted");
        if (entries.size() > Log.max_size() - Log.size())
            throw std::length_error("In-memory log is too large for this platform");
        auto cloned = CloneEntries(entries, LastLogTerm);
        Log.reserve(Log.size() + cloned.size());
        for (auto& entry : cloned) Log.push_back(std::move(entry));
        LastLogIndex = SnapshotIndex + Log.size();
        if (!cloned.empty()) LastLogTerm = Log.back()->Term;
    }

    void ReplaceSuffixAndAppendBatch(uint64_t firstIndex,
            const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) override {
        if (LastLogIndex > uint64_t(INT64_MAX))
            throw std::overflow_error("Raft log index exhausted");
        const auto commitFloor = std::max(CommitIndex, SnapshotIndex);
        if (firstIndex <= commitFloor || firstIndex > LastLogIndex + 1)
            throw std::logic_error("Invalid Raft suffix replacement boundary");
        const auto retainedLastIndex = firstIndex - 1;
        if (entries.size() > uint64_t(INT64_MAX) - retainedLastIndex)
            throw std::overflow_error("Raft log index exhausted");
        const auto newLastLogIndex = retainedLastIndex + entries.size();
        if (commitIndex < commitFloor || commitIndex > newLastLogIndex)
            throw std::logic_error("Invalid Raft suffix replacement commit boundary");

        const auto prefixSize = size_t(firstIndex - SnapshotIndex - 1);
        if (prefixSize > Log.size())
            throw std::logic_error("Invalid in-memory Raft suffix replacement index");
        if (entries.size() > Log.max_size() - prefixSize)
            throw std::length_error("In-memory log is too large for this platform");
        const auto previousTerm = prefixSize ? Log[prefixSize - 1]->Term : SnapshotTerm;
        auto cloned = CloneEntries(entries, previousTerm);
        Log.reserve(prefixSize + cloned.size());
        Log.resize(prefixSize);
        for (auto& entry : cloned) Log.push_back(std::move(entry));
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.empty() ? SnapshotTerm : Log.back()->Term;
        const bool advancesCommit = CommitIndex < commitIndex;
        CommitIndex = commitIndex;
        if (advancesCommit) Commit();
    }

    TMessageHolder<TLogEntry> Get(int64_t index) const override {
        if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex) return {};
        return CloneEntry(Log[index - SnapshotIndex - 1]);
    }
    uint64_t RetainedLogTerm(int64_t index) const override {
        if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex) return 0;
        return Log[index - SnapshotIndex - 1]->Term;
    }
    uint64_t RetainedLogFlags(int64_t index) const override {
        if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex)
            throw std::out_of_range("Missing retained log entry");
        return Log[index - SnapshotIndex - 1]->Flags;
    }

    bool SupportsSnapshots() const override { return true; }
    bool SupportsInstallSnapshot() const override { return true; }
    bool SupportsVoterBinding() const override { return true; }
    std::vector<uint32_t> GetBoundVoters() const override { return BoundVoters; }
    void BindVoters(const std::vector<uint32_t>& voters) override {
        ValidateVoterBinding(*this, BoundVoters, voters);
        BoundVoters = voters;
    }
    std::shared_ptr<const TSnapshot> GetSnapshot() const override { return Snapshot; }
    std::optional<TApplicationSnapshotGenerationReference>
    GetApplicationSnapshotGenerationReference() const override { return SnapshotGeneration; }
    void InstallSnapshot(const TSnapshot& snapshot) override { InstallSnapshotImpl(snapshot, false); }
    void InstallRaftSnapshot(const TSnapshot& snapshot,
            const TRaftInstallSnapshotPermit&) override { InstallSnapshotImpl(snapshot, true); }
    void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
            const TRaftInstallSnapshotPermit&) override { InstallSnapshotImpl(std::move(snapshot)); }
    void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
            const TRaftInstallSnapshotPermit& permit,
            std::optional<TApplicationSnapshotGenerationReference> generation) override {
        if (generation) {
            if (!snapshot) throw std::invalid_argument("Missing Raft snapshot");
            ValidateApplicationSnapshotGenerationReference(*generation, *snapshot);
        }
        InstallRaftSnapshot(std::move(snapshot), permit);
        SnapshotGeneration = std::move(generation);
    }
private:
    void InstallSnapshotImpl(const TSnapshot& snapshot, bool configurationHistoryValidated) {
        ValidateSnapshot(snapshot);
        TSnapshot image = snapshot;
        if (!configurationHistoryValidated) PreserveStableSnapshotIdentity(Snapshot.get(), image);
        ValidateSnapshot(image); // Validate before changing any state.
        InstallValidatedSnapshot(std::make_shared<const TSnapshot>(std::move(image)));
    }
    void InstallSnapshotImpl(std::shared_ptr<const TSnapshot> saved) {
        if (!saved) throw std::invalid_argument("Missing Raft snapshot");
        ValidateSnapshot(*saved);
        InstallValidatedSnapshot(std::move(saved));
    }
    void InstallValidatedSnapshot(std::shared_ptr<const TSnapshot> saved) {
        if (!SnapshotCompatibleWithVoterBinding(*saved, BoundVoters))
            throw std::invalid_argument("Snapshot is incompatible with the persisted voter binding");
        if (saved->Index <= SnapshotIndex || saved->Term < SnapshotTerm || saved->Term > CurrentTerm)
            throw std::invalid_argument("Obsolete or future snapshot");
        if (saved->Index <= CommitIndex &&
            (saved->Index > LastLogIndex || LogTerm(saved->Index) != saved->Term))
            throw std::invalid_argument("Snapshot would discard committed log entries");
        std::vector<TMessageHolder<TLogEntry>> suffix;
        if (saved->Index <= LastLogIndex && LogTerm(saved->Index) == saved->Term)
            suffix.assign(Log.begin() + (saved->Index - SnapshotIndex), Log.end());
        Log.swap(suffix);
        Snapshot = std::move(saved);
        SnapshotGeneration.reset();
        SnapshotIndex = Snapshot->Index;
        SnapshotTerm = Snapshot->Term;
        LastLogIndex = SnapshotIndex + Log.size();
        LastLogTerm = Log.empty() ? SnapshotTerm : Log.back()->Term;
        CommitIndex = std::max(CommitIndex, SnapshotIndex);
    }
public:
    void Commit() override { }
};

