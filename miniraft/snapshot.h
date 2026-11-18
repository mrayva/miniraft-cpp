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

// One committed stable -> joint -> stable voter transition. A zero stable
// successor identifies the final, currently active joint phase.
struct TSnapshotConfigurationTransition {
    static constexpr size_t MaxVoters = 4096;
    std::vector<uint32_t> OldVoters, NewVoters;
    uint64_t PreviousConfigurationIndex = 0, PreviousConfigurationTerm = 0;
    uint64_t JointIndex = 0, JointTerm = 0;
    uint64_t StableIndex = 0, StableTerm = 0;
    bool operator==(const TSnapshotConfigurationTransition&) const = default;
};

// Versioned application snapshot, shared by local compaction and state transfer.
struct TSnapshot {
    static constexpr size_t MaxBytes = 64 * 1024 * 1024;
    static constexpr uint32_t MaxVoters = 4096;
    static constexpr size_t MaxLegacyConfigurationTransitions = 32;
    static constexpr size_t MaxConfigurationTransitions = 256;
    uint64_t Index = 0, Term = 0;
    std::vector<uint32_t> Voters;
    std::string Data;
    // Optional joint configuration effective at this snapshot boundary.
    std::vector<uint32_t> JointVoters;
    uint64_t ConfigurationIndex = 0, ConfigurationTerm = 0;
    // Stable-configuration identity. Offline storage retains the original
    // persisted voter binding while this field records the active voter set.
    bool StableConfiguration = false;
    // Optional bounded membership lineage. For a stable snapshot these identify
    // its preceding stable voter set and joint entry. For a joint snapshot they
    // preserve that predecessor and identify the current joint entry.
    std::vector<uint32_t> PreviousVoters;
    uint64_t PreviousConfigurationIndex = 0, PreviousConfigurationTerm = 0;
    uint64_t PreviousJointIndex = 0, PreviousJointTerm = 0;
    // Bounded, contiguous committed history for online recovery of stores that
    // missed more than one compaction. Legacy snapshots retain only the tail.
    std::vector<TSnapshotConfigurationTransition> ConfigurationTransitions;
    bool HasConfigurationLineage() const { return !PreviousVoters.empty(); }
    bool HasStablePredecessor() const { return StableConfiguration && !PreviousVoters.empty(); }
    bool IsJoint() const { return !JointVoters.empty() || (!StableConfiguration && (ConfigurationIndex || ConfigurationTerm)); }
    bool HasConfigurationMetadata() const { return StableConfiguration || IsJoint(); }
    bool operator==(const TSnapshot&) const = default;
};
std::string EncodeSnapshot(const TSnapshot& snapshot);
TSnapshot DecodeSnapshot(std::string_view bytes);
// Validate metadata and size without copying the application image.
void ValidateSnapshot(const TSnapshot& snapshot);
// Preserve a trusted stable identity on legacy refresh and validate snapshot
// configuration continuity: stable -> joint -> stable, with voter-set checks.
void PreserveStableSnapshotIdentity(const TSnapshot* current, TSnapshot& incoming);
TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm);
// Startup only, with an application behind the snapshot and
// a matching local boundary/log. This can recover an app left behind by a crash
// after durable Raft snapshot publication.
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
