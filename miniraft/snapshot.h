#pragma once
#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

class TRaft;
struct IRsm;

// Versioned application snapshot, shared by local compaction and state transfer.
struct TSnapshot {
    static constexpr size_t MaxBytes = 64 * 1024 * 1024;
    static constexpr uint32_t MaxVoters = 4096;
    uint64_t Index = 0, Term = 0;
    std::vector<uint32_t> Voters;
    std::string Data;
    bool operator==(const TSnapshot&) const = default;
};
std::string EncodeSnapshot(const TSnapshot& snapshot);
TSnapshot DecodeSnapshot(std::string_view bytes);
TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm);
// Startup only, with a fresh application and a matching local boundary/log.
void RestoreSnapshot(const TSnapshot& snapshot, const TRaft& raft, IRsm& rsm);

class TSnapshotStore {
public:
    enum class EStage { TempCreated, Written, FileSynced, Renamed, DirectorySynced };
    using THook = std::function<void(EStage)>;
    // Linux atomic publication, exclusive writer lock, bounded reads and CRC32.
    TSnapshotStore(const std::string& name, uint32_t id, THook hook = {});
    ~TSnapshotStore();
    TSnapshotStore(const TSnapshotStore&) = delete;
    TSnapshotStore& operator=(const TSnapshotStore&) = delete;
    std::optional<TSnapshot> Load();
    // Standalone checkpoint export, not the compaction commit point. Failure is
    // catchable when the authoritative snapshot/log remains in the Raft state.
    void Save(const TSnapshot& snapshot);
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};
