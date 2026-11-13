#pragma once

#include "messages.h"
#include "snapshot.h"
#include <cstdint>
#include <vector>

// Experimental record encoding only. Current Raft rejects these entries.
enum class EConfigurationPhase : uint32_t { Joint = 1, Stable = 2 };
struct TConfigurationRecord {
    EConfigurationPhase Phase = EConfigurationPhase::Joint;
    uint64_t Term = 0, PreviousIndex = 0, PreviousTerm = 0;
    std::vector<uint32_t> OldVoters, NewVoters;
    bool operator==(const TConfigurationRecord&) const = default;
};

TMessageHolder<TLogEntry> EncodeConfigurationEntry(const TConfigurationRecord& record);
TConfigurationRecord DecodeConfigurationEntry(TMessageHolder<TLogEntry> entry);

// Offline transition validation only; does not establish commitment, activate
// roles, or make a fixed-membership Raft node accept configuration entries.
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
    // Visit every retained entry in index order; neither method changes *this.
    TConfigurationHistory Observe(uint64_t index, TMessageHolder<TLogEntry> entry) const;
    TConfigurationHistory Advance(uint64_t index, TMessageHolder<TLogEntry> entry) const;

    bool IsJoint() const { return !New.empty(); }
    const std::vector<uint32_t>& OldVoters() const { return Old; }
    const std::vector<uint32_t>& NewVoters() const { return New; }
    uint64_t ConfigurationIndex() const { return ConfigIndex; }
    uint64_t ConfigurationTerm() const { return ConfigTerm; }
    uint64_t ThroughIndex() const { return Through; }
    uint64_t ThroughTerm() const { return ThroughLogTerm; }
private:
    TConfigurationHistory() = default;
    std::vector<uint32_t> Old, New;
    uint64_t ConfigIndex = 0, ConfigTerm = 0, Through = 0, ThroughLogTerm = 0;
};
