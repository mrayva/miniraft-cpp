#pragma once

#include "messages.h"
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
