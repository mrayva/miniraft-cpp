#pragma once
#include "state.h"
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct TLegacyDiskStateMigrationInfo {
    uint64_t CurrentTerm = 0;
    uint32_t VotedFor = 0;
    uint64_t LastLogIndex = 0;
    uint64_t LastLogTerm = 0;
    uint64_t CommitIndex = 0;
};

// Check or migrate an offline, un-compacted TDiskState store. The caller must
// supply the independently verified original voter set and commit boundary.
// Retained configuration entries are validated from that voter-set baseline;
// compacted stores still require a separate snapshot/history migration.
TLegacyDiskStateMigrationInfo CheckLegacyDiskStateMigration(
    const std::string& legacyBase, uint32_t nodeId,
    const std::vector<uint32_t>& trustedVoters, uint64_t commitIndex);
TLegacyDiskStateMigrationInfo MigrateLegacyDiskState(
    const std::string& legacyBase, const std::string& durableBase, uint32_t nodeId,
    const std::vector<uint32_t>& trustedVoters, uint64_t commitIndex);

// Linux: atomic hard-state + snapshot + suffix replacement, with file/directory
// fsync. O(snapshot + retained log size) per update: a durability baseline.
class TDurableState : public IState {
public:
    enum class EStage { TempCreated, PayloadStarted, Written, FileSynced, Renamed, DirectorySynced };
    // Diagnostics/fault injection. Throwing from this hook during a live
    // operation is fatal, just like an I/O failure.
    using THook = std::function<void(EStage)>;
    TDurableState(const std::string& name, uint32_t id, THook hook = {});
    ~TDurableState() override;
    TDurableState(const TDurableState&) = delete;
    TDurableState& operator=(const TDurableState&) = delete;
    void RemoveLast() override;
    void Append(TMessageHolder<TLogEntry> entry) override;
    void AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries) override;
    void AppendBatchAndCommit(
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) override;
    void ReplaceSuffixAndAppendBatch(uint64_t firstIndex,
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) override;
    TMessageHolder<TLogEntry> Get(int64_t index) const override;
    uint64_t RetainedLogTerm(int64_t index) const override;
    uint64_t RetainedLogFlags(int64_t index) const override;
    void Commit() override;
    bool SupportsCommitIndexPersistence() const override { return true; }
    bool SupportsSnapshots() const override { return true; }
    bool SupportsInstallSnapshot() const override { return true; }
    std::shared_ptr<const TSnapshot> GetSnapshot() const override;
    void InstallSnapshot(const TSnapshot& snapshot) override;
    void InstallRaftSnapshot(const TSnapshot& snapshot,
        const TRaftInstallSnapshotPermit& permit) override;
    void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        const TRaftInstallSnapshotPermit& permit) override;
    bool SupportsVoterBinding() const override { return true; }
    bool HasUnboundRecoveredState() const override;
    std::vector<uint32_t> GetBoundVoters() const override;
    void BindVoters(const std::vector<uint32_t>& voters) override;
    void MarkRaftStarted() override;
    // Explicit legacy migration. Call before constructing TRaft. The caller
    // asserts that voters is the complete original fixed set, verified outside
    // this store.
    // The dry run reports on-disk consistency only; it cannot prove the
    // operator-supplied voter set is complete. It returns false with a
    // diagnostic in reason on failure, clears reason on success, and never
    // publishes state.
    bool CanBindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters,
        std::string* reason = nullptr) const;
    void BindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters);
    // Migrates a recovered unbound image when its snapshot carries complete
    // membership lineage from the trusted original set. commitIndex must come
    // from an independent application/deployment record.
    bool CanBindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, std::string* reason = nullptr) const;
    void BindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex);
private:
    friend TLegacyDiskStateMigrationInfo MigrateLegacyDiskState(
        const std::string&, const std::string&, uint32_t,
        const std::vector<uint32_t>&, uint64_t);
    void ValidateTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters) const;
    void ValidateTrustedLegacyVoterHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) const;
    void InstallSnapshotImpl(const TSnapshot& snapshot, bool configurationHistoryValidated);
    void InstallSnapshotImpl(std::shared_ptr<const TSnapshot> snapshot);
    void InstallValidatedSnapshot(std::shared_ptr<const TSnapshot> snapshot);
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
    void Publish();
};
