#pragma once

#include "state.h"

#include <fstream>
#include <functional>
#include <string>

struct TDiskState : IState {
    enum class ECommitStage {
        LogFilesSynced,
        MetadataFileSynced,
        MetadataRenamed,
        DirectorySynced,
    };
    // Exposes publication boundaries for deterministic crash/failure tests.
    using TCommitHook = std::function<void(ECommitStage)>;

    TDiskState(const std::string& name, uint32_t id, TCommitHook commitHook = {});
    
    void RemoveLast() override;
    void Append(TMessageHolder<TLogEntry> entry) override;
    TMessageHolder<TLogEntry> Get(int64_t index) const override;
    void Commit() override;
    bool SupportsCommitIndexPersistence() const override { return true; }
    bool HasTrustedCommitIndex() const override { return CommitIndexTrusted; }
    bool SupportsVoterBinding() const override { return true; }
    bool HasUnboundRecoveredState() const override;
    std::vector<uint32_t> GetBoundVoters() const override;
    void BindVoters(const std::vector<uint32_t>& voters) override;
    // Recovered split-format stores have no saved commit boundary or voter
    // binding. This startup-only path requires a trusted operator baseline and
    // rejects logs that already contain configuration entries.
    void ValidateTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex);
    void MarkRaftStarted() override { RaftStarted = true; }

private:
    bool Recovering = true;
    bool RecoveredStore = false;
    bool CommitIndexTrusted = true;
    bool ChecksummedRecords = true;
    bool RaftStarted = false;
    [[noreturn]] void Fail(const char* message) const;
    std::fstream Open(const std::string& name, bool create);
    TMessageHolder<TMessage> Read() const;
    void Write(TMessageHolder<TMessage>);
    uint64_t ReadOffset(uint64_t index) const;
    void WriteOffset(uint64_t index, uint64_t offset);
    uint64_t IndexRecordSize() const { return ChecksummedRecords ? 12 : sizeof(uint64_t); }

    std::string EntriesPath;
    std::string IndexPath;
    std::string StatePath;
    TCommitHook CommitHook;
    std::vector<uint32_t> Voters;
    mutable std::fstream Entries;
    mutable std::fstream Index;
    std::fstream State;
};
