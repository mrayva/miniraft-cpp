#pragma once

#include "state.h"

#include <fstream>
#include <functional>
#include <string>

class TConfigurationHistory;

struct TDiskState : IState {
    enum class ECommitStage {
        EntriesPublished,
        SnapshotIndexPublished,
        SnapshotImagePublished,
        LogFilesSynced,
        MetadataFileSynced,
        MetadataRenamed,
        DirectorySynced,
    };
    // Exposes publication boundaries for deterministic crash/failure tests.
    using TCommitHook = std::function<void(ECommitStage)>;

    // Recovery normally removes previous and interrupted generations after
    // the metadata-selected generation is validated. Offline validation can
    // disable cleanup to keep dry runs from mutating the store.
    TDiskState(const std::string& name, uint32_t id, TCommitHook commitHook = {});
    TDiskState(const std::string& name, uint32_t id, TCommitHook commitHook,
        bool reclaimUnselectedGenerationsOnRecovery);
    
    void RemoveLast() override;
    void Append(TMessageHolder<TLogEntry> entry) override;
    void AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries) override;
    void AppendBatchAndCommit(
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) override;
    void ReplaceSuffixAndAppendBatch(uint64_t firstIndex,
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex) override;
    TMessageHolder<TLogEntry> Get(int64_t index) const override;
    void Commit() override;
    bool SupportsCommitIndexPersistence() const override { return true; }
    bool HasTrustedCommitIndex() const override { return CommitIndexTrusted; }
    bool SupportsSnapshots() const override { return true; }
    bool SupportsInstallSnapshot() const override { return true; }
    std::shared_ptr<const TSnapshot> GetSnapshot() const override { return Snapshot; }
    std::optional<TApplicationSnapshotGenerationReference>
    GetApplicationSnapshotGenerationReference() const override { return SnapshotGenerationReference; }
    void InstallSnapshot(const TSnapshot& snapshot) override;
    void InstallRaftSnapshot(const TSnapshot& snapshot,
        const TRaftInstallSnapshotPermit&) override;
    void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        const TRaftInstallSnapshotPermit&) override;
    void InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        const TRaftInstallSnapshotPermit&,
        std::optional<TApplicationSnapshotGenerationReference> generation) override;
    bool SupportsVoterBinding() const override { return true; }
    bool HasUnboundRecoveredState() const override;
    std::vector<uint32_t> GetBoundVoters() const override;
    void BindVoters(const std::vector<uint32_t>& voters) override;
    // Read-only preflight for a recovered split-format store with no saved
    // commit boundary or voter binding. This simple baseline path rejects
    // compacted state and logs that already contain configuration entries.
    bool CanBindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, std::string* reason = nullptr) const;
    void BindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex);
    // Compatibility spelling retained for callers that used this mutating
    // operation. New code should call CanBind... and Bind... explicitly.
    void ValidateTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex);
    // Full-history variant for operators migrating recovered stores that have
    // membership records or a configuration-bearing compacted snapshot. The
    // supplied original voter set and commit boundary are trusted inputs; all
    // retained history must validate against them before metadata is published.
    bool CanBindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, std::string* reason = nullptr) const;
    void BindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex);
    // Variant for a compacted snapshot whose bounded embedded history no longer
    // reaches the original voter set. The caller supplies independently
    // verified complete history through the snapshot boundary.
    bool CanBindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, const TConfigurationHistory& history,
        std::string* reason = nullptr) const;
    void BindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, const TConfigurationHistory& history);
    void MarkRaftStarted() override { RaftStarted = true; }

private:
    void CheckTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) const;
    void ValidateTrustedLegacyVoterHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) const;
    void ValidateTrustedLegacyVoterHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, const TConfigurationHistory* history) const;
    void ValidateStoredVoteMembershipHistory() const;
    bool Recovering = true;
    bool RecoveredStore = false;
    bool CommitIndexTrusted = true;
    bool ChecksummedRecords = true;
    bool RaftStarted = false;
    bool ReclaimUnselectedGenerationsOnRecovery = true;
    [[noreturn]] void Fail(const char* message) const;
    std::fstream Open(const std::string& name, bool create);
    TMessageHolder<TMessage> Read() const;
    void Write(TMessageHolder<TMessage>);
    uint64_t ReadOffset(uint64_t index) const;
    void WriteOffset(uint64_t index, uint64_t offset);
    uint64_t IndexRecordSize() const { return ChecksummedRecords ? 12 : sizeof(uint64_t); }
    std::string GenerationPath(uint64_t generation) const;
    std::string EntryGenerationPath(uint64_t generation) const;
    std::string SnapshotPath(uint64_t generation) const;
    void ReclaimUncommittedGeneration();
    void InstallSnapshotImpl(const TSnapshot& snapshot, bool configurationHistoryValidated);
    void InstallSnapshotImpl(std::shared_ptr<const TSnapshot> snapshot);
    void InstallValidatedSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        bool allowSameBoundary = false,
        std::optional<TApplicationSnapshotGenerationReference> generation = {});
    void WriteSnapshotGenerationFile(const std::string& path, const TSnapshot& snapshot);
    uint64_t WriteCompactedLogGeneration(const std::string& entriesPath, const std::string& indexPath,
        uint64_t firstIndex, uint64_t lastIndex, uint64_t& lastTerm,
        const std::vector<TMessageHolder<TLogEntry>>& appended = {});
    void ReclaimPreviousGeneration(uint64_t previousEntriesGeneration, uint64_t previousIndexGeneration);

    std::string EntriesPath;
    std::string EntriesBasePath;
    std::string IndexPath;
    std::string IndexBasePath;
    std::string StatePath;
    std::string SnapshotFilePath;
    TCommitHook CommitHook;
    std::vector<uint32_t> Voters;
    std::shared_ptr<const TSnapshot> Snapshot;
    std::optional<TApplicationSnapshotGenerationReference> SnapshotGenerationReference;
    uint64_t EntriesGeneration = 0;
    uint64_t IndexGeneration = 0;
    uint64_t IndexBaseIndex = 1;
    mutable std::fstream Entries;
    mutable std::fstream Index;
    std::fstream State;
};
