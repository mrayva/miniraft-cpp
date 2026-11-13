#pragma once

#include "state.h"
#include <algorithm>
#include <cstdint>
#include <unordered_set>
#include <utility>
#include <vector>

// Quorum arithmetic only. This does not activate or persist a membership change.
class TQuorum {
public:
    explicit TQuorum(std::vector<uint32_t> voters) : Old(std::move(voters)) {
        ValidateFixedVoters(Old);
    }
    TQuorum(std::vector<uint32_t> oldVoters, std::vector<uint32_t> newVoters)
        : Old(std::move(oldVoters)), New(std::move(newVoters)) {
        ValidateFixedVoters(Old);
        ValidateFixedVoters(New);
    }

    // Highest threshold acknowledged by a strict majority of each voter set.
    // Missing voters should return zero. Overlapping voters contribute once to
    // each set; members outside both sets cannot affect the result.
    // The callback must return a stable value for an ID during the call.
    template<class F> uint64_t Value(F value, std::vector<uint64_t>* scratch = nullptr) const {
        std::vector<uint64_t> local;
        auto& values = scratch ? *scratch : local;
        const auto majority = [&](const auto& voters) {
            values.clear();
            values.reserve(voters.size());
            for (auto id : voters) values.push_back(value(id));
            auto middle = values.begin() + (values.size() - 1) / 2;
            std::nth_element(values.begin(), middle, values.end());
            return *middle;
        };
        const auto old = majority(Old);
        return New.empty() ? old : std::min(old, majority(New));
    }

    bool HasQuorum(const std::unordered_set<uint32_t>& votes, uint32_t self = 0) const {
        return Value([&](uint32_t id) -> uint64_t {
            return (self != 0 && id == self) || votes.contains(id);
        }) != 0;
    }
private:
    std::vector<uint32_t> Old, New;
};
