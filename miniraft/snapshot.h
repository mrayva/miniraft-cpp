#pragma once
#include <cstdint>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <string>
#include <string_view>
#include <vector>

class TRaft;
class TSnapshotInstallBudget;
struct IRsm;

// Immutable, range-readable snapshot payload. Sources must remain stable while
// a snapshot retains them. Reads may be partial but cannot return zero before
// the requested range reaches the end of the source.
struct TSnapshotDataSource {
    virtual ~TSnapshotDataSource() = default;
    virtual uint64_t Size() const = 0;
    virtual size_t Read(uint64_t offset, std::span<char> destination) const = 0;
};

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
    // The former limit remains the safe fallback for peers that predate
    // snapshot-size capability advertisement.
    static constexpr size_t LegacyMaxBytes = 64 * 1024 * 1024;
    static constexpr size_t MaxBytes = 128 * 1024 * 1024;
    static constexpr uint32_t MaxVoters = 4096;
    static constexpr size_t MaxLegacyConfigurationTransitions = 32;
    static constexpr size_t MaxExtendedConfigurationTransitions = 256;
    static constexpr size_t MaxConfigurationTransitions = 1024;
    uint64_t Index = 0, Term = 0;
    std::vector<uint32_t> Voters;
    std::string Data;
    // Optional file-backed payload used by disk-staged InstallSnapshot,
    // durable sidecar recovery, and snapshot-store loading. When set, Data is
    // empty; consumers should use DataSize/ReadDataRange or MaterializeData.
    // Ordinary captures and legacy embedded restores keep using Data.
    std::shared_ptr<const TSnapshotDataSource> DataSource;
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
    // Runtime-only quota lease for the retained application image. These
    // fields are neither serialized nor part of snapshot identity.
    mutable std::shared_ptr<void> HeapReservation;
    mutable std::shared_ptr<TSnapshotInstallBudget> HeapReservationBudget;
    size_t DataSize() const;
    size_t ReadDataRange(uint64_t offset, std::span<char> destination) const;
    std::string MaterializeData() const;
    bool HasConfigurationLineage() const { return !PreviousVoters.empty(); }
    bool HasStablePredecessor() const { return StableConfiguration && !PreviousVoters.empty(); }
    bool IsJoint() const { return !JointVoters.empty() || (!StableConfiguration && (ConfigurationIndex || ConfigurationTerm)); }
    bool HasConfigurationMetadata() const { return StableConfiguration || IsJoint(); }
    bool operator==(const TSnapshot& other) const;
};

// Shares an aggregate heap limit across snapshot transfers and retained images.
// Temporary file staging uses a separate budget. Callers can supply shared
// custom budgets through Raft limits.
class TSnapshotInstallBudget : public std::enable_shared_from_this<TSnapshotInstallBudget> {
public:
    class TReservation {
    public:
        ~TReservation() { Budget->Release(Bytes); }
    private:
        friend class TSnapshotInstallBudget;
        TReservation(std::shared_ptr<TSnapshotInstallBudget> budget, uint64_t bytes)
            : Budget(std::move(budget)), Bytes(bytes) { }
        std::shared_ptr<TSnapshotInstallBudget> Budget;
        uint64_t Bytes;
    };

    static constexpr uint64_t DefaultMaxBytes = 256ULL * 1024 * 1024;

    explicit TSnapshotInstallBudget(uint64_t maxBytes = DefaultMaxBytes)
        : MaxBytes(maxBytes) {
        if (!MaxBytes) throw std::invalid_argument("Snapshot install budget must be positive");
    }

    static std::shared_ptr<TSnapshotInstallBudget> DefaultProcessBudget() {
        static auto budget = std::make_shared<TSnapshotInstallBudget>();
        return budget;
    }

    std::shared_ptr<TReservation> TryReserveLease(uint64_t bytes) {
        if (!bytes) return {};
        {
            std::lock_guard lock(Mutex);
            if (Used > MaxBytes || bytes > MaxBytes - Used) return {};
            Used += bytes;
        }
        TReservation* lease = nullptr;
        try {
            lease = new TReservation(shared_from_this(), bytes);
        } catch (...) {
            Release(bytes);
            throw;
        }
        // If allocating the shared_ptr control block throws, its constructor
        // deletes lease, which releases the charge exactly once.
        return std::shared_ptr<TReservation>(lease);
    }

    std::shared_ptr<void> TryReserve(uint64_t bytes) { return TryReserveLease(bytes); }

    // Adjust a lease created by TryReserveLease. Shrinking never fails; growing
    // atomically checks the shared limit before changing the charged byte count.
    bool TryResize(const std::shared_ptr<TReservation>& reservation, uint64_t bytes) {
        if (!reservation) return bytes == 0;
        if (reservation->Budget.get() != this)
            throw std::invalid_argument("Snapshot reservation belongs to a different budget");
        std::lock_guard lock(Mutex);
        const auto previous = reservation->Bytes;
        if (bytes > previous) {
            const auto additional = bytes - previous;
            if (Used > MaxBytes || additional > MaxBytes - Used) return false;
            Used += additional;
        } else {
            Used -= previous - bytes;
        }
        reservation->Bytes = bytes;
        return true;
    }

    uint64_t UsedBytes() const {
        std::lock_guard lock(Mutex);
        return Used;
    }

    uint64_t LimitBytes() const { return MaxBytes; }

private:
    void Release(uint64_t bytes) noexcept {
        std::lock_guard lock(Mutex);
        if (bytes > Used) std::terminate();
        Used -= bytes;
    }

    const uint64_t MaxBytes;
    mutable std::mutex Mutex;
    uint64_t Used = 0;
};

std::string EncodeSnapshot(const TSnapshot& snapshot);
TSnapshot DecodeSnapshot(std::string_view bytes);
// Decodes metadata from a complete range-readable image while retaining its
// application-payload range instead of allocating another full image buffer.
TSnapshot DecodeSnapshotBacked(std::string_view bytes,
    std::shared_ptr<const TSnapshotDataSource> image);
// Reuses the input allocation for application data after the image checksum
// and metadata validate, avoiding another full-size copy during installation.
TSnapshot DecodeSnapshotOwned(std::string&& bytes);
struct TSnapshotEncoding;
// Keeps only serialized metadata and the image checksum. The snapshot must
// remain immutable while ranges are copied from this prepared encoding.
std::shared_ptr<const TSnapshotEncoding> PrepareSnapshotEncoding(const TSnapshot& snapshot);
std::shared_ptr<const TSnapshotEncoding> PrepareSnapshotEncoding(
    const TSnapshot& snapshot, size_t maxConfigurationTransitions);
// Omitting membership history emits only the active stable/joint identity.
std::shared_ptr<const TSnapshotEncoding> PrepareSnapshotEncoding(
    const TSnapshot& snapshot, size_t maxConfigurationTransitions, bool includeMembershipHistory);
uint64_t SnapshotEncodingSize(const TSnapshotEncoding& encoding);
void CopySnapshotEncodingRange(const TSnapshot& snapshot, const TSnapshotEncoding& encoding,
    uint64_t offset, char* destination, size_t length);
// Validate metadata and size without copying the application image.
void ValidateSnapshot(const TSnapshot& snapshot);
// Preserve a trusted stable identity on legacy refresh and validate snapshot
// configuration continuity: stable -> joint -> stable, with voter-set checks.
void PreserveStableSnapshotIdentity(const TSnapshot* current, TSnapshot& incoming);
TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm);
TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm,
    std::shared_ptr<TSnapshotInstallBudget> heapBudget);
// When a staging directory is supplied, incremental application writers emit
// into an anonymous file and retain its quota lease with the range-backed image.
TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm,
    std::shared_ptr<TSnapshotInstallBudget> heapBudget,
    std::shared_ptr<TSnapshotInstallBudget> stagingBudget,
    std::string_view stagingDirectory);
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
