#pragma once

#include <unordered_map>
#include <string>

#include <miniraft/raft.h>

class TKv: public IRsm, public IStreamingSnapshotWriterRsm {
public:
    void Validate(TMessageHolder<TCommandRequest> message) const override;
    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest> message, uint64_t index) override;
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> message, uint64_t index) override;
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest> message) override;
    std::string SaveSnapshot() const override;
    void WriteSnapshot(uint64_t index, ISnapshotSink& sink) const override;
    std::optional<uint64_t> SnapshotSizeUpperBound() const override;
    void ValidateSnapshot(std::string_view bytes, uint64_t index) const override;
    void LoadSnapshot(std::string_view bytes, uint64_t index) override;
    bool SupportsApplicationSnapshotInstall() const override { return true; }

protected:
    using TEntries = std::unordered_map<std::string, std::string>;
    void InstallSnapshotEntries(TEntries&& entries, uint64_t index);

private:
    TEntries H;
};

// Range-based installer variant for deployments that enable disk-staged
// InstallSnapshot. It validates into a temporary map, then swaps it in after
// Raft has durably published the snapshot.
class TStreamingKv final : public TKv, public IStreamingSnapshotInstallRsm {
public:
    std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
        const TSnapshotDataSource& source, uint64_t index) override;

private:
    struct TInstallation;
};
