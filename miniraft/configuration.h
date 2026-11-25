#pragma once

#include "messages.h"
#include "snapshot.h"
#include <cstddef>
#include <cstdint>
#include <vector>

// Configuration record encoding for the live joint-consensus log protocol.
// Configuration-bearing snapshots carry the same identity and voter sets used
// by the runtime's validated history scan.
enum class EConfigurationPhase : uint32_t { Joint = 1, Stable = 2 };
struct TConfigurationRecord {
    EConfigurationPhase Phase = EConfigurationPhase::Joint;
    uint64_t Term = 0, PreviousIndex = 0, PreviousTerm = 0;
    std::vector<uint32_t> OldVoters, NewVoters;
    bool operator==(const TConfigurationRecord&) const = default;
};

TMessageHolder<TLogEntry> EncodeConfigurationEntry(const TConfigurationRecord& record);
size_t ConfigurationEntrySize(size_t oldVoterCount, size_t newVoterCount);
TConfigurationRecord DecodeConfigurationEntry(TMessageHolder<TLogEntry> entry);

// Validates transition ordering and derives configuration identity from a
// trusted baseline and commit boundary. Runtime callers use this before
// activating roles or accepting a candidate AppendEntries suffix.
class TConfigurationHistory {
public:
    TConfigurationHistory(const TConfigurationHistory&) = default;
    TConfigurationHistory& operator=(const TConfigurationHistory&) = default;
    // The caller must supply a trusted identity and scanned-prefix boundary.
    // A zero configuration identity asserts that no configuration entry exists
    // in that prefix; a v1 snapshot alone does not prove this assertion.
    static TConfigurationHistory Stable(std::vector<uint32_t> voters,
        uint64_t configurationIndex, uint64_t configurationTerm,
        uint64_t throughIndex, uint64_t throughTerm);
    static TConfigurationHistory FromJointSnapshot(const TSnapshot& snapshot);
    static TConfigurationHistory FromStableSnapshot(const TSnapshot& snapshot);
    // Visit every retained entry in index order; neither method changes *this.
    TConfigurationHistory Observe(uint64_t index, TMessageHolder<TLogEntry> entry) const;
    // Metadata must come from a trusted, already validated storage backend.
    TConfigurationHistory ObserveMetadata(uint64_t index, uint64_t term, uint64_t flags) const;
    TConfigurationHistory Advance(uint64_t index, TMessageHolder<TLogEntry> entry) const;
    // Offline projection only. The caller must establish that ThroughIndex is
    // committed and that data is the application image at exactly that index.
    TSnapshot MakeSnapshot(std::string data) const;

    bool IsJoint() const { return !New.empty(); }
    const std::vector<uint32_t>& OldVoters() const { return Old; }
    const std::vector<uint32_t>& NewVoters() const { return New; }
    uint64_t ConfigurationIndex() const { return ConfigIndex; }
    uint64_t ConfigurationTerm() const { return ConfigTerm; }
    const std::vector<uint32_t>& PreviousVoters() const { return Previous; }
    uint64_t PreviousConfigurationIndex() const { return PreviousConfigIndex; }
    uint64_t PreviousConfigurationTerm() const { return PreviousConfigTerm; }
    uint64_t PreviousJointIndex() const { return PreviousJointConfigIndex; }
    uint64_t PreviousJointTerm() const { return PreviousJointConfigTerm; }
    const std::vector<TSnapshotConfigurationTransition>& ConfigurationTransitions() const { return Transitions; }
    uint64_t ThroughIndex() const { return Through; }
    uint64_t ThroughTerm() const { return ThroughLogTerm; }
private:
    TConfigurationHistory() = default;
    std::vector<uint32_t> Old, New;
    uint64_t ConfigIndex = 0, ConfigTerm = 0, Through = 0, ThroughLogTerm = 0;
    std::vector<uint32_t> Previous;
    uint64_t PreviousConfigIndex = 0, PreviousConfigTerm = 0;
    uint64_t PreviousJointConfigIndex = 0, PreviousJointConfigTerm = 0;
    std::vector<TSnapshotConfigurationTransition> Transitions;
};

// Validate an offline snapshot against a complete history supplied from a
// trusted external record, then return the same image with that history
// attached. The supplied history must start at the original voter baseline
// and end exactly at the snapshot boundary.
TSnapshot SnapshotWithTrustedConfigurationHistory(const TSnapshot& snapshot,
    const std::vector<uint32_t>& originalVoters, const TConfigurationHistory& history);
void ValidateSnapshotAgainstTrustedConfigurationHistory(const TSnapshot& snapshot,
    const std::vector<uint32_t>& originalVoters, const TConfigurationHistory& history);

struct IState;
struct TConfigurationScan {
    TConfigurationHistory Committed;
    TConfigurationHistory Latest;
    bool HasUncommittedConfiguration() const {
        return Committed.ConfigurationIndex() != Latest.ConfigurationIndex() ||
            Committed.ConfigurationTerm() != Latest.ConfigurationTerm() ||
            Committed.IsJoint() != Latest.IsJoint() ||
            Committed.OldVoters() != Latest.OldVoters() ||
            Committed.NewVoters() != Latest.NewVoters() ||
            Committed.PreviousVoters() != Latest.PreviousVoters() ||
            Committed.PreviousConfigurationIndex() != Latest.PreviousConfigurationIndex() ||
            Committed.PreviousConfigurationTerm() != Latest.PreviousConfigurationTerm() ||
            Committed.PreviousJointIndex() != Latest.PreviousJointIndex() ||
            Committed.PreviousJointTerm() != Latest.PreviousJointTerm() ||
            Committed.ConfigurationTransitions() != Latest.ConfigurationTransitions();
    }
};

// Read-only validation. The caller supplies and must substantiate both the
// baseline and commit boundary; neither is proven by this scan.
TConfigurationScan ScanConfigurationHistory(const IState& state,
    TConfigurationHistory baseline, uint64_t commitIndex);
// Offline migration-only scan. Callers must first validate the supplied
// boundary snapshot against independently trusted history; the stored image's
// metadata is then checked against this validated boundary instead.
TConfigurationScan ScanConfigurationHistoryWithTrustedSnapshotBoundary(const IState& state,
    TConfigurationHistory baseline, uint64_t commitIndex, const TSnapshot& trustedSnapshot);

// Recover configuration identity at a particular committed index without
// interpreting later retained entries as part of that boundary.
TConfigurationHistory ScanConfigurationHistoryPrefix(const IState& state,
    TConfigurationHistory baseline, uint64_t throughIndex);

// Read the committed boundary from storage instead of accepting a caller's
// guess. Requires voter binding and a backend that persists CommitIndex; the
// caller must still supply a trusted baseline through SnapshotIndex.
TConfigurationScan ScanCommittedConfigurationHistory(const IState& state,
    TConfigurationHistory baseline);
