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

private:
    bool Recovering = true;
    [[noreturn]] void Fail(const char* message) const;
    std::fstream Open(const std::string& name, bool create);
    TMessageHolder<TMessage> Read() const;
    void Write(TMessageHolder<TMessage>);

    std::string EntriesPath;
    std::string IndexPath;
    std::string StatePath;
    TCommitHook CommitHook;
    mutable std::fstream Entries;
    mutable std::fstream Index;
    std::fstream State;
};
