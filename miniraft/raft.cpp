#include <chrono>
#include <climits>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <iostream>
#include <algorithm>
#include <iterator>

#include <string.h>
#include <assert.h>

#include "raft.h"
#include "messages.h"
#include "timesource.h"
#include "configuration.h"

namespace {

static uint32_t rand_(uint32_t* seed) {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 17;
    *seed ^= *seed << 5;
    return *seed;
}

struct TSnapshotMetadataProbe {
    bool Complete = false;
    bool Supported = false;
    size_t Need = 8;
    bool FreshLearnerBootstrap = false;
};

bool IsSupportedSnapshotMagicPrefix(std::string_view prefix) {
    constexpr std::string_view legacy = "MRFTS001";
    constexpr std::string_view joint = "MRFTS002";
    constexpr std::string_view stable = "MRFTS003";
    constexpr std::string_view stableLineage = "MRFTS004";
    constexpr std::string_view jointLineage = "MRFTS005";
    constexpr std::string_view stableHistory = "MRFTS006";
    constexpr std::string_view jointHistory = "MRFTS007";
    const auto bytes = prefix.substr(0, std::min(prefix.size(), legacy.size()));
    return legacy.substr(0, bytes.size()) == bytes || joint.substr(0, bytes.size()) == bytes ||
        stable.substr(0, bytes.size()) == bytes || stableLineage.substr(0, bytes.size()) == bytes ||
        jointLineage.substr(0, bytes.size()) == bytes ||
        stableHistory.substr(0, bytes.size()) == bytes || jointHistory.substr(0, bytes.size()) == bytes;
}

bool ReadSnapshotInteger(std::string_view bytes, size_t offset, unsigned width, uint64_t& value) {
    if (offset > bytes.size() || width > bytes.size() - offset) return false;
    value = 0;
    for (unsigned i = 0; i < width; ++i)
        value |= uint64_t(uint8_t(bytes[offset + i])) << (8 * i);
    return true;
}

bool SnapshotHistorySharesKnownTail(const std::vector<TSnapshotConfigurationTransition>& incoming,
        const std::vector<TSnapshotConfigurationTransition>& known) {
    if (incoming.empty() || known.empty()) return true;
    const auto count = std::min(incoming.size(), known.size());
    for (size_t i = 0; i < count; ++i) {
        if (incoming[incoming.size() - count + i] != known[known.size() - count + i]) return false;
    }
    return true;
}

bool SnapshotHistoryCompletesKnownJoint(const std::vector<TSnapshotConfigurationTransition>& incoming,
        const std::vector<TSnapshotConfigurationTransition>& known) {
    if (incoming.empty() || known.empty()) return true;
    const auto& joint = known.back();
    const auto& stable = incoming.back();
    if (joint.StableIndex || joint.StableTerm ||
        stable.StableIndex <= joint.JointIndex || stable.StableTerm < joint.JointTerm ||
        stable.OldVoters != joint.OldVoters || stable.NewVoters != joint.NewVoters ||
        stable.PreviousConfigurationIndex != joint.PreviousConfigurationIndex ||
        stable.PreviousConfigurationTerm != joint.PreviousConfigurationTerm ||
        stable.JointIndex != joint.JointIndex || stable.JointTerm != joint.JointTerm)
        return false;
    // Compare the completed prefix as a suffix: the receiver may know more
    // history than the incoming image, while the image may retain an older
    // prefix beyond the receiver's bounded window.
    const size_t incomingCompleted = incoming.size() - 1;
    const size_t knownCompleted = known.size() - 1;
    const size_t count = std::min(incomingCompleted, knownCompleted);
    for (size_t i = 0; i < count; ++i) {
        if (incoming[incomingCompleted - count + i] != known[knownCompleted - count + i])
            return false;
    }
    return true;
}

bool SnapshotMatchesCommittedConfiguration(const TSnapshot& snapshot,
        const TConfigurationHistory& committed) {
    if (committed.IsJoint()) {
        return snapshot.IsJoint() && !snapshot.StableConfiguration &&
            snapshot.Voters == committed.OldVoters() && snapshot.JointVoters == committed.NewVoters() &&
            snapshot.ConfigurationIndex == committed.ConfigurationIndex() &&
            snapshot.ConfigurationTerm == committed.ConfigurationTerm() &&
            SnapshotHistorySharesKnownTail(snapshot.ConfigurationTransitions,
                committed.ConfigurationTransitions()) &&
            (!snapshot.HasConfigurationLineage() ||
                (snapshot.PreviousVoters == committed.PreviousVoters() &&
                 snapshot.PreviousConfigurationIndex == committed.PreviousConfigurationIndex() &&
                 snapshot.PreviousConfigurationTerm == committed.PreviousConfigurationTerm() &&
                 snapshot.PreviousJointIndex == committed.PreviousJointIndex() &&
                 snapshot.PreviousJointTerm == committed.PreviousJointTerm()));
    }
    if (snapshot.IsJoint() || snapshot.Voters != committed.OldVoters()) return false;
    if (snapshot.StableConfiguration)
        return snapshot.ConfigurationIndex == committed.ConfigurationIndex() &&
            snapshot.ConfigurationTerm == committed.ConfigurationTerm() &&
            SnapshotHistorySharesKnownTail(snapshot.ConfigurationTransitions,
                committed.ConfigurationTransitions()) &&
            (!snapshot.HasStablePredecessor() ||
                (snapshot.PreviousVoters == committed.PreviousVoters() &&
                 snapshot.PreviousConfigurationIndex == committed.PreviousConfigurationIndex() &&
                 snapshot.PreviousConfigurationTerm == committed.PreviousConfigurationTerm() &&
                 snapshot.PreviousJointIndex == committed.PreviousJointIndex() &&
                 snapshot.PreviousJointTerm == committed.PreviousJointTerm()));
    return snapshot.Index >= committed.ConfigurationIndex();
}

std::vector<uint32_t> ConfigurationVoterUnion(const TConfigurationHistory& configuration) {
    auto voters = configuration.OldVoters();
    voters.insert(voters.end(), configuration.NewVoters().begin(), configuration.NewVoters().end());
    std::sort(voters.begin(), voters.end());
    voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
    return voters;
}

bool MayForwardPriorTermJointRecord(const TConfigurationRecord& record, uint64_t requestTerm,
        uint32_t leaderId, const TConfigurationScan* configuration) {
    if (record.Phase != EConfigurationPhase::Joint || record.Term >= requestTerm ||
        !std::binary_search(record.NewVoters.begin(), record.NewVoters.end(), leaderId) ||
        !configuration || configuration->Committed.IsJoint())
        return false;
    // The receiver's uncommitted suffix may be replaced by the elected
    // leader. Only the committed stable configuration anchors this exception.
    const auto& committed = configuration->Committed;
    return record.OldVoters == committed.OldVoters() &&
        record.PreviousIndex == committed.ConfigurationIndex() &&
        record.PreviousTerm == committed.ConfigurationTerm();
}

bool MayForwardPriorTermJointEntry(TMessageHolder<TAppendEntriesRequest> request,
        const TConfigurationScan* configuration) {
    if (request->Nentries != request.PayloadSize) return false;
    for (uint32_t i = 0; i < request.PayloadSize; ++i) {
        const auto entry = request.Payload[i].Cast<TLogEntry>();
        if (!(entry->Flags & TLogEntry::EConfiguration)) continue;
        try {
            const auto record = DecodeConfigurationEntry(entry);
            if (MayForwardPriorTermJointRecord(record, request->Term, request->LeaderId, configuration))
                return true;
        } catch (const std::exception&) {
            return false;
        }
    }
    return false;
}

const TConfigurationHistory& SnapshotLeaderConfiguration(
        const TConfigurationHistory& committed, const TConfigurationHistory& pending) {
    // An uncommitted joint transition already uses both sets for elections and
    // commitment. During an uncommitted stable successor, the committed joint
    // configuration remains authoritative until that successor commits.
    if (committed.IsJoint()) return committed;
    if (pending.IsJoint()) return pending;
    return committed;
}

bool HasPendingStableSuccessor(const TConfigurationScan& configuration) {
    return configuration.HasUncommittedConfiguration() && configuration.Committed.IsJoint() &&
        !configuration.Latest.IsJoint() &&
        configuration.Latest.ConfigurationIndex() > configuration.Committed.ConfigurationIndex();
}

bool SnapshotMatchesPendingConfiguration(const TSnapshot& snapshot,
        const TConfigurationHistory& committed, const TConfigurationHistory& pending,
        uint32_t leaderId) {
    if (!snapshot.HasConfigurationMetadata() || snapshot.Index < pending.ConfigurationIndex() ||
        snapshot.ConfigurationIndex != pending.ConfigurationIndex() ||
        snapshot.ConfigurationTerm != pending.ConfigurationTerm() ||
        !SnapshotHistorySharesKnownTail(snapshot.ConfigurationTransitions,
            pending.ConfigurationTransitions()))
        return false;
    if (pending.IsJoint()) {
        if (!snapshot.IsJoint() || snapshot.StableConfiguration ||
            snapshot.Voters != pending.OldVoters() || snapshot.JointVoters != pending.NewVoters())
            return false;
    } else if (snapshot.IsJoint() || !snapshot.StableConfiguration ||
        snapshot.Voters != pending.OldVoters()) {
        return false;
    }
    if (snapshot.HasConfigurationLineage() &&
        (snapshot.PreviousVoters != pending.PreviousVoters() ||
         snapshot.PreviousConfigurationIndex != pending.PreviousConfigurationIndex() ||
         snapshot.PreviousConfigurationTerm != pending.PreviousConfigurationTerm() ||
         snapshot.PreviousJointIndex != pending.PreviousJointIndex() ||
         snapshot.PreviousJointTerm != pending.PreviousJointTerm()))
        return false;
    const auto activeVoters = ConfigurationVoterUnion(SnapshotLeaderConfiguration(committed, pending));
    return std::binary_search(activeVoters.begin(), activeVoters.end(), leaderId);
}

bool SnapshotMetadataContinuesKnown(const TSnapshot* known, bool joint, bool stable,
        const std::vector<uint32_t>& oldVoters,
        const std::vector<uint32_t>& newVoters, uint64_t configurationIndex,
        uint64_t configurationTerm) {
    if (!known || !known->HasConfigurationMetadata()) return true;
    if (known->StableConfiguration) {
        if (joint) return oldVoters == known->Voters && configurationIndex > known->Index &&
            configurationTerm >= known->Term;
        if (stable) return oldVoters == known->Voters &&
            configurationIndex == known->ConfigurationIndex && configurationTerm == known->ConfigurationTerm;
        return oldVoters == known->Voters;
    }
    if (joint) return oldVoters == known->Voters && newVoters == known->JointVoters &&
        configurationIndex == known->ConfigurationIndex && configurationTerm == known->ConfigurationTerm;
    if (stable) return oldVoters == known->JointVoters && configurationIndex > known->Index &&
        configurationTerm >= known->Term;
    return false;
}

bool SnapshotMetadataCompletesKnownJoint(const TSnapshot* known,
        const TConfigurationHistory* committed, bool stable, bool stableLineage,
        const std::vector<uint32_t>& oldVoters, const std::vector<uint32_t>& newVoters,
        const std::vector<uint32_t>& previousVoters, uint64_t previousConfigurationIndex,
        uint64_t previousConfigurationTerm, uint64_t previousJointIndex,
        uint64_t previousJointTerm,
        uint64_t configurationIndex, uint64_t configurationTerm, uint32_t leaderId) {
    if (!known || !known->IsJoint() || known->StableConfiguration || !committed ||
        !committed->IsJoint() || known->Voters != committed->OldVoters() ||
        known->JointVoters != committed->NewVoters() ||
        known->ConfigurationIndex != committed->ConfigurationIndex() ||
        known->ConfigurationTerm != committed->ConfigurationTerm() || !stable ||
        !newVoters.empty() || oldVoters != committed->NewVoters() ||
        configurationIndex <= known->Index ||
        configurationIndex <= committed->ConfigurationIndex() ||
        configurationTerm < known->Term ||
        configurationTerm < committed->ConfigurationTerm())
        return false;
    if (stableLineage && (previousVoters != known->Voters ||
        previousJointIndex != committed->ConfigurationIndex() ||
        previousJointTerm != committed->ConfigurationTerm() ||
        (known->HasConfigurationLineage() &&
            (previousVoters != known->PreviousVoters ||
             previousConfigurationIndex != known->PreviousConfigurationIndex ||
             previousConfigurationTerm != known->PreviousConfigurationTerm ||
             previousJointIndex != known->PreviousJointIndex ||
             previousJointTerm != known->PreviousJointTerm))))
        return false;
    // The image claims the stable successor has committed, so its sender must
    // still be a voter after the transition, not only on the old joint side.
    return std::binary_search(committed->NewVoters().begin(), committed->NewVoters().end(), leaderId);
}

bool HasPersistedRecoveryState(const IState& state) {
    return state.LastLogIndex || state.SnapshotIndex || state.CommitIndex || state.VotedFor || state.GetSnapshot();
}

bool SnapshotMetadataExtendsKnownStable(const TConfigurationHistory* committed,
        bool stableLineage, bool staleState, const std::vector<uint32_t>& incomingVoters,
        const std::vector<uint32_t>& previousVoters, uint64_t previousConfigurationIndex,
        uint64_t previousConfigurationTerm, uint64_t previousJointIndex,
        uint64_t previousJointTerm, uint64_t configurationIndex, uint64_t configurationTerm,
        uint32_t leaderId) {
    if (!stableLineage || !staleState || !committed || committed->IsJoint() ||
        incomingVoters == committed->OldVoters() || previousVoters != committed->OldVoters() ||
        previousConfigurationIndex != committed->ConfigurationIndex() ||
        previousConfigurationTerm != committed->ConfigurationTerm() ||
        previousJointIndex <= committed->ThroughIndex() ||
        previousJointTerm < committed->ThroughTerm() ||
        configurationIndex <= previousJointIndex || configurationTerm < previousJointTerm)
        return false;
    return std::binary_search(previousVoters.begin(), previousVoters.end(), leaderId) &&
        std::binary_search(incomingVoters.begin(), incomingVoters.end(), leaderId);
}

bool SnapshotHistoryExtendsKnownStable(const TConfigurationHistory* committed,
        bool staleState, const std::vector<TSnapshotConfigurationTransition>& transitions,
        const std::vector<uint32_t>& incomingVoters, uint64_t configurationIndex,
        uint64_t configurationTerm, uint32_t leaderId, size_t* anchorIndex = nullptr) {
    if (!staleState || !committed || committed->IsJoint() || transitions.empty() ||
        transitions.size() > TSnapshot::MaxConfigurationTransitions)
        return false;
    size_t anchor = transitions.size();
    for (size_t i = 0; i < transitions.size(); ++i) {
        const auto& transition = transitions[i];
        if (transition.OldVoters == committed->OldVoters() &&
            transition.PreviousConfigurationIndex == committed->ConfigurationIndex() &&
            transition.PreviousConfigurationTerm == committed->ConfigurationTerm() &&
            transition.JointIndex > committed->ThroughIndex() &&
            transition.JointTerm >= committed->ThroughTerm()) {
            anchor = i;
            break;
        }
    }
    if (anchor == transitions.size())
        return false;
    for (size_t i = anchor; i < transitions.size(); ++i) {
        const auto& transition = transitions[i];
        if (!std::binary_search(transition.OldVoters.begin(), transition.OldVoters.end(), leaderId) ||
            !std::binary_search(transition.NewVoters.begin(), transition.NewVoters.end(), leaderId))
            return false;
    }
    const auto& last = transitions.back();
    if (last.StableIndex != configurationIndex || last.StableTerm != configurationTerm ||
        last.NewVoters != incomingVoters)
        return false;
    if (anchorIndex) *anchorIndex = anchor;
    return true;
}

bool SnapshotHistoryExtendsKnownStableToJoint(const TConfigurationHistory* committed,
        bool staleState, const std::vector<TSnapshotConfigurationTransition>& transitions,
        const std::vector<uint32_t>& incomingOld, const std::vector<uint32_t>& incomingNew,
        uint64_t configurationIndex, uint64_t configurationTerm, uint32_t leaderId,
        size_t* anchorIndex = nullptr) {
    if (!staleState || !committed || committed->IsJoint() || transitions.empty() ||
        transitions.size() > TSnapshot::MaxConfigurationTransitions)
        return false;
    size_t anchor = transitions.size();
    for (size_t i = 0; i < transitions.size(); ++i) {
        const auto& transition = transitions[i];
        if (transition.OldVoters == committed->OldVoters() &&
            transition.PreviousConfigurationIndex == committed->ConfigurationIndex() &&
            transition.PreviousConfigurationTerm == committed->ConfigurationTerm() &&
            transition.JointIndex > committed->ThroughIndex() &&
            transition.JointTerm >= committed->ThroughTerm()) {
            anchor = i;
            break;
        }
    }
    if (anchor == transitions.size()) return false;
    for (size_t i = anchor; i + 1 < transitions.size(); ++i) {
        const auto& transition = transitions[i];
        if (!std::binary_search(transition.OldVoters.begin(), transition.OldVoters.end(), leaderId) ||
            !std::binary_search(transition.NewVoters.begin(), transition.NewVoters.end(), leaderId))
            return false;
    }
    const auto& activeJoint = transitions.back();
    const bool extends = !activeJoint.StableIndex && !activeJoint.StableTerm &&
        activeJoint.JointIndex == configurationIndex && activeJoint.JointTerm == configurationTerm &&
        activeJoint.OldVoters == incomingOld && activeJoint.NewVoters == incomingNew &&
        (std::binary_search(incomingOld.begin(), incomingOld.end(), leaderId) ||
         std::binary_search(incomingNew.begin(), incomingNew.end(), leaderId));
    if (extends && anchorIndex) *anchorIndex = anchor;
    return extends;
}

std::vector<TSnapshotConfigurationTransition> ExtendTrustedTransitionHistory(
        const std::vector<TSnapshotConfigurationTransition>& trusted,
        const std::vector<TSnapshotConfigurationTransition>& incoming, size_t anchor) {
    if (anchor >= incoming.size()) throw std::logic_error("Missing anchored snapshot transition");
    auto transitions = trusted;
    const auto& first = incoming[anchor];
    if (!transitions.empty()) {
        const auto& previous = transitions.back();
        if (!previous.StableIndex || previous.StableIndex != first.PreviousConfigurationIndex ||
            previous.StableTerm != first.PreviousConfigurationTerm || previous.NewVoters != first.OldVoters)
            throw std::logic_error("Snapshot transition does not extend trusted local history");
    }
    transitions.insert(transitions.end(), incoming.begin() + anchor, incoming.end());
    if (transitions.size() > TSnapshot::MaxConfigurationTransitions)
        transitions.erase(transitions.begin(), transitions.begin() +
            (transitions.size() - TSnapshot::MaxConfigurationTransitions));
    return transitions;
}

bool CanBootstrapFreshLearnerSnapshot(uint32_t receiverId, const IState& state,
        const std::vector<uint32_t>& localVoters, const IRsm* application,
        const TConfigurationHistory* committedConfiguration, bool configurationBoundaryKnown) {
    return application && application->LastAppliedIndex == 0 &&
        !std::binary_search(localVoters.begin(), localVoters.end(), receiverId) &&
        state.VotedFor == 0 && state.LastLogIndex == 0 && state.LastLogTerm == 0 &&
        state.CommitIndex == 0 && state.SnapshotIndex == 0 && state.SnapshotTerm == 0 &&
        !state.GetSnapshot() && configurationBoundaryKnown && committedConfiguration &&
        !committedConfiguration->IsJoint() && committedConfiguration->ConfigurationIndex() == 0 &&
        committedConfiguration->ConfigurationTerm() == 0 && committedConfiguration->ThroughIndex() == 0 &&
        committedConfiguration->OldVoters() == localVoters;
}

bool SnapshotMatchesFreshLearnerBootstrap(const TSnapshot& snapshot,
        const std::vector<uint32_t>& localVoters, const TConfigurationHistory& committedConfiguration,
        uint32_t leaderId, uint32_t receiverId) {
    if (!snapshot.HasConfigurationMetadata() ||
        snapshot.ConfigurationIndex <= committedConfiguration.ConfigurationIndex() ||
        snapshot.ConfigurationTerm < committedConfiguration.ConfigurationTerm())
        return false;

    std::vector<uint32_t> activeVoters = snapshot.Voters;
    if (snapshot.IsJoint()) {
        if (snapshot.StableConfiguration || snapshot.Voters == snapshot.JointVoters) return false;
        activeVoters.insert(activeVoters.end(), snapshot.JointVoters.begin(), snapshot.JointVoters.end());
        std::sort(activeVoters.begin(), activeVoters.end());
        activeVoters.erase(std::unique(activeVoters.begin(), activeVoters.end()), activeVoters.end());
    }
    return activeVoters == localVoters &&
        std::binary_search(activeVoters.begin(), activeVoters.end(), leaderId) &&
        !std::binary_search(activeVoters.begin(), activeVoters.end(), receiverId);
}

TSnapshotMetadataProbe ProbeSnapshotMetadata(std::string_view prefix, uint64_t totalBytes,
        uint64_t index, uint64_t term, const std::vector<uint32_t>& voters,
        const TSnapshot* knownSnapshot, const TConfigurationHistory* committedConfiguration,
        const TConfigurationHistory* pendingConfiguration, bool allowFreshLearnerBootstrap,
        bool allowStaleStoreMembershipRecovery, uint32_t leaderId, uint32_t receiverId) {
    if (prefix.size() < 8) return {false, false, 8};
    const auto magic = prefix.substr(0, 8);
    const bool stable = magic == "MRFTS003";
    const bool stableHistory = magic == "MRFTS006";
    const bool stableLineage = magic == "MRFTS004" || stableHistory;
    const bool stableImage = stable || stableLineage;
    const bool jointHistory = magic == "MRFTS007";
    const bool jointLineage = magic == "MRFTS005" || jointHistory;
    const bool joint = magic == "MRFTS002" || jointLineage;
    const bool configurationHistory = stableHistory || jointHistory;
    if (!stableImage && !joint && magic != "MRFTS001") return {true, false, 0};
    const bool freshLearnerBootstrap = allowFreshLearnerBootstrap && (stableImage || joint);
    const bool pendingConfigurationBoundary = pendingConfiguration && committedConfiguration &&
        pendingConfiguration->ConfigurationIndex() > committedConfiguration->ConfigurationIndex() &&
        index >= pendingConfiguration->ConfigurationIndex();
    const bool committedConfigurationBoundary = committedConfiguration &&
        (!pendingConfiguration || index < pendingConfiguration->ConfigurationIndex());
    const bool knownJointStableSuccessor = stableImage && committedConfiguration &&
        committedConfiguration->IsJoint() && !pendingConfiguration && knownSnapshot;
    const bool staleJointHistoryCandidate = jointHistory && allowStaleStoreMembershipRecovery &&
        committedConfiguration && !committedConfiguration->IsJoint() && !pendingConfiguration;
    // The active configuration phase is already trusted from local committed
    // history. A boundary that reaches a locally pending entry must identify
    // that exact validated entry; all other unknown boundaries are rejected.
    if ((!committedConfigurationBoundary && !pendingConfigurationBoundary) || !committedConfiguration ||
        (joint != committedConfiguration->IsJoint() && !freshLearnerBootstrap &&
            !pendingConfigurationBoundary && !knownJointStableSuccessor && !staleJointHistoryCandidate))
        return {true, false, 0};
    if (pendingConfigurationBoundary && !stableImage && !joint) return {true, false, 0};

    // Inspect only the bounded format header and voter list. Callers use this
    // before granting replication progress or accepting a higher term; payload
    // bytes and CRC are validated by the full install path when needed.
    const size_t fixedHeaderBytes = stableLineage ? 88 : stable ? 52 : jointLineage ? 92 : joint ? 56 : 36;
    if (prefix.size() < fixedHeaderBytes) return {false, false, fixedHeaderBytes};
    uint64_t embeddedIndex = 0, embeddedTerm = 0, voterCount = 0, dataBytes = 0;
    if (!ReadSnapshotInteger(prefix, 8, 8, embeddedIndex) ||
        !ReadSnapshotInteger(prefix, 16, 8, embeddedTerm) ||
        !ReadSnapshotInteger(prefix, 24, 4, voterCount) ||
        !ReadSnapshotInteger(prefix, 28, 8, dataBytes) ||
        embeddedIndex != index || embeddedTerm != term || voterCount > TSnapshot::MaxVoters)
        return {true, false, 0};
    uint64_t configurationIndex = 0, configurationTerm = 0, newCount = 0;
    uint64_t previousConfigurationIndex = 0, previousConfigurationTerm = 0;
    uint64_t previousJointIndex = 0, previousJointTerm = 0, previousVoterCount = 0;
    if ((stableImage || joint) && (!ReadSnapshotInteger(prefix, 36, 8, configurationIndex) ||
        !ReadSnapshotInteger(prefix, 44, 8, configurationTerm) || !configurationIndex ||
        configurationIndex > embeddedIndex || !configurationTerm || configurationTerm > embeddedTerm ||
        (configurationIndex == embeddedIndex && configurationTerm != embeddedTerm)))
        return {true, false, 0};
    if (joint && (!ReadSnapshotInteger(prefix, 52, 4, newCount) || newCount > TSnapshot::MaxVoters))
        return {true, false, 0};
    const size_t lineageOffset = jointLineage ? 56 : 52;
    if ((stableLineage || jointLineage) &&
        (!ReadSnapshotInteger(prefix, lineageOffset, 8, previousConfigurationIndex) ||
        !ReadSnapshotInteger(prefix, lineageOffset + 8, 8, previousConfigurationTerm) ||
        !ReadSnapshotInteger(prefix, lineageOffset + 16, 8, previousJointIndex) ||
        !ReadSnapshotInteger(prefix, lineageOffset + 24, 8, previousJointTerm) ||
        !ReadSnapshotInteger(prefix, lineageOffset + 32, 4, previousVoterCount) ||
        previousVoterCount > TSnapshot::MaxVoters || !previousJointIndex ||
        previousJointIndex <= previousConfigurationIndex ||
        (jointLineage ? configurationIndex != previousJointIndex : configurationIndex <= previousJointIndex) ||
        !previousJointTerm || previousJointTerm < previousConfigurationTerm ||
        (jointLineage ? configurationTerm != previousJointTerm : configurationTerm < previousJointTerm) ||
        ((previousConfigurationIndex == 0) != (previousConfigurationTerm == 0))))
        return {true, false, 0};
    if (pendingConfigurationBoundary &&
        ((joint != pendingConfiguration->IsJoint()) ||
         configurationIndex != pendingConfiguration->ConfigurationIndex() ||
         configurationTerm != pendingConfiguration->ConfigurationTerm() ||
         voterCount != pendingConfiguration->OldVoters().size() ||
         newCount != (joint ? pendingConfiguration->NewVoters().size() : 0)))
        return {true, false, 0};
    const bool stableSuccessorMetadata = !pendingConfiguration && !pendingConfigurationBoundary && stableImage &&
        committedConfiguration->IsJoint() && knownSnapshot &&
        configurationIndex > knownSnapshot->Index &&
        configurationIndex > committedConfiguration->ConfigurationIndex() &&
        configurationTerm >= knownSnapshot->Term &&
        configurationTerm >= committedConfiguration->ConfigurationTerm() &&
        voterCount == committedConfiguration->NewVoters().size() && newCount == 0;
    const bool staleStableLineageMetadata = !pendingConfiguration && !pendingConfigurationBoundary &&
        stableLineage && allowStaleStoreMembershipRecovery && !committedConfiguration->IsJoint() &&
        newCount == 0 && (configurationHistory ||
            (previousVoterCount == committedConfiguration->OldVoters().size() &&
             previousConfigurationIndex == committedConfiguration->ConfigurationIndex() &&
             previousConfigurationTerm == committedConfiguration->ConfigurationTerm() &&
             previousJointIndex > committedConfiguration->ThroughIndex() &&
             previousJointTerm >= committedConfiguration->ThroughTerm() &&
             configurationIndex > previousJointIndex && configurationTerm >= previousJointTerm));
    const bool staleJointHistoryMetadata = staleJointHistoryCandidate && configurationHistory &&
        jointHistory;
    if (freshLearnerBootstrap) {
        if (configurationIndex <= committedConfiguration->ConfigurationIndex() ||
            configurationTerm < committedConfiguration->ConfigurationTerm())
            return {true, false, 0};
    } else if (!pendingConfigurationBoundary && !stableSuccessorMetadata && !staleStableLineageMetadata &&
        !staleJointHistoryMetadata) {
        std::vector<uint32_t> activeVoters = committedConfiguration->OldVoters();
        if (committedConfiguration->IsJoint()) {
            activeVoters.insert(activeVoters.end(), committedConfiguration->NewVoters().begin(),
                committedConfiguration->NewVoters().end());
            std::sort(activeVoters.begin(), activeVoters.end());
            activeVoters.erase(std::unique(activeVoters.begin(), activeVoters.end()), activeVoters.end());
        }
        if (voters != activeVoters || voterCount != committedConfiguration->OldVoters().size() ||
            newCount != (joint ? committedConfiguration->NewVoters().size() : 0))
            return {true, false, 0};
        if (joint) {
            if (!committedConfiguration->IsJoint() ||
                configurationIndex != committedConfiguration->ConfigurationIndex() ||
                configurationTerm != committedConfiguration->ConfigurationTerm())
                return {true, false, 0};
        } else {
            if (committedConfiguration->IsJoint() || embeddedIndex < committedConfiguration->ConfigurationIndex())
                return {true, false, 0};
            if (stableImage && (configurationIndex != committedConfiguration->ConfigurationIndex() ||
                configurationTerm != committedConfiguration->ConfigurationTerm()))
                return {true, false, 0};
        }
    }
    const uint64_t voterBytes = voterCount * 4;
    uint64_t headerBytes = fixedHeaderBytes + voterBytes + newCount * 4 + previousVoterCount * 4;
    if (headerBytes > totalBytes || totalBytes - headerBytes < 4)
        return {true, false, 0};
    if (headerBytes > prefix.size()) return {false, false, size_t(headerBytes)};
    std::vector<uint32_t> incomingOld, incomingNew, incomingPrevious;
    auto readVoterList = [&](size_t offset, uint64_t count, std::vector<uint32_t>& result) {
        uint32_t previous = 0;
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t voter = 0;
            if (!ReadSnapshotInteger(prefix, offset + i * 4, 4, voter) || !voter || voter <= previous)
                return false;
            previous = uint32_t(voter);
            result.push_back(previous);
        }
        return true;
    };
    if (!readVoterList(fixedHeaderBytes, voterCount, incomingOld) ||
        !readVoterList(fixedHeaderBytes + voterBytes, newCount, incomingNew) ||
        !readVoterList(fixedHeaderBytes + voterBytes + newCount * 4, previousVoterCount, incomingPrevious) ||
        (joint && incomingOld == incomingNew) || (jointLineage && incomingPrevious != incomingOld))
        return {true, false, 0};

    std::vector<TSnapshotConfigurationTransition> transitions;
    if (configurationHistory) {
        if (headerBytes + 4 > totalBytes - 4) return {true, false, 0};
        uint64_t transitionCount = 0;
        if (!ReadSnapshotInteger(prefix, size_t(headerBytes), 4, transitionCount))
            return {false, false, size_t(headerBytes + 4)};
        if (!transitionCount || transitionCount > TSnapshot::MaxConfigurationTransitions)
            return {true, false, 0};
        headerBytes += 4;
        for (uint64_t i = 0; i < transitionCount; ++i) {
            if (headerBytes + 56 > totalBytes - 4) return {true, false, 0};
            if (headerBytes + 56 > prefix.size()) return {false, false, size_t(headerBytes + 56)};
            TSnapshotConfigurationTransition transition;
            const size_t offset = size_t(headerBytes);
            uint64_t oldCount = 0, nextCount = 0;
            if (!ReadSnapshotInteger(prefix, offset, 8, transition.PreviousConfigurationIndex) ||
                !ReadSnapshotInteger(prefix, offset + 8, 8, transition.PreviousConfigurationTerm) ||
                !ReadSnapshotInteger(prefix, offset + 16, 8, transition.JointIndex) ||
                !ReadSnapshotInteger(prefix, offset + 24, 8, transition.JointTerm) ||
                !ReadSnapshotInteger(prefix, offset + 32, 8, transition.StableIndex) ||
                !ReadSnapshotInteger(prefix, offset + 40, 8, transition.StableTerm) ||
                !ReadSnapshotInteger(prefix, offset + 48, 4, oldCount) ||
                !ReadSnapshotInteger(prefix, offset + 52, 4, nextCount) ||
                oldCount > TSnapshot::MaxVoters || nextCount > TSnapshot::MaxVoters)
                return {true, false, 0};
            const uint64_t transitionBytes = 56 + 4 * (oldCount + nextCount);
            if (transitionBytes > totalBytes - 4 - headerBytes)
                return {true, false, 0};
            if (headerBytes + transitionBytes > prefix.size())
                return {false, false, size_t(headerBytes + transitionBytes)};
            auto readTransitionVoters = [&](size_t listOffset, uint64_t count,
                    std::vector<uint32_t>& output) {
                uint32_t previous = 0;
                for (uint64_t j = 0; j < count; ++j) {
                    uint64_t voter = 0;
                    if (!ReadSnapshotInteger(prefix, listOffset + j * 4, 4, voter) || !voter || voter <= previous)
                        return false;
                    previous = uint32_t(voter);
                    output.push_back(previous);
                }
                return true;
            };
            if (!readTransitionVoters(offset + 56, oldCount, transition.OldVoters) ||
                !readTransitionVoters(offset + 56 + oldCount * 4, nextCount, transition.NewVoters) ||
                transition.OldVoters.empty() || transition.NewVoters.empty() ||
                transition.OldVoters == transition.NewVoters || !transition.JointIndex || !transition.JointTerm ||
                ((transition.PreviousConfigurationIndex == 0) != (transition.PreviousConfigurationTerm == 0)) ||
                transition.JointIndex <= transition.PreviousConfigurationIndex ||
                transition.JointTerm < transition.PreviousConfigurationTerm ||
                ((transition.StableIndex == 0) != (transition.StableTerm == 0)) ||
                (transition.StableIndex && (transition.StableIndex <= transition.JointIndex ||
                    transition.StableTerm < transition.JointTerm)))
                return {true, false, 0};
            if (!transitions.empty()) {
                const auto& previous = transitions.back();
                if (!previous.StableIndex || previous.NewVoters != transition.OldVoters ||
                    previous.StableIndex != transition.PreviousConfigurationIndex ||
                    previous.StableTerm != transition.PreviousConfigurationTerm ||
                    transition.JointIndex <= previous.StableIndex || transition.JointTerm < previous.StableTerm)
                    return {true, false, 0};
            }
            transitions.push_back(std::move(transition));
            headerBytes += transitionBytes;
        }
        const auto& tail = transitions.back();
        if (tail.OldVoters != incomingPrevious ||
            tail.PreviousConfigurationIndex != previousConfigurationIndex ||
            tail.PreviousConfigurationTerm != previousConfigurationTerm ||
            tail.JointIndex != previousJointIndex || tail.JointTerm != previousJointTerm ||
            (stableImage && (tail.StableIndex != configurationIndex || tail.StableTerm != configurationTerm ||
                tail.NewVoters != incomingOld)) ||
            (joint && (tail.StableIndex || tail.JointIndex != configurationIndex ||
                tail.JointTerm != configurationTerm || tail.OldVoters != incomingOld ||
                tail.NewVoters != incomingNew)))
            return {true, false, 0};
    }
    if (headerBytes + 4 > totalBytes || dataBytes != totalBytes - headerBytes - 4)
        return {true, false, 0};
    if (configurationHistory && !knownJointStableSuccessor && !staleStableLineageMetadata &&
        !staleJointHistoryMetadata) {
        const auto& trustedTransitions = pendingConfiguration
            ? pendingConfiguration->ConfigurationTransitions()
            : committedConfiguration->ConfigurationTransitions();
        if (!SnapshotHistorySharesKnownTail(transitions, trustedTransitions))
            return {true, false, 0};
    }
    if (configurationHistory && knownJointStableSuccessor &&
        !SnapshotHistoryCompletesKnownJoint(transitions, committedConfiguration->ConfigurationTransitions()))
        return {true, false, 0};

    if (freshLearnerBootstrap) {
        std::vector<uint32_t> activeVoters = incomingOld;
        if (joint) {
            activeVoters.insert(activeVoters.end(), incomingNew.begin(), incomingNew.end());
            std::sort(activeVoters.begin(), activeVoters.end());
            activeVoters.erase(std::unique(activeVoters.begin(), activeVoters.end()), activeVoters.end());
        }
        if (activeVoters != voters ||
            !std::binary_search(activeVoters.begin(), activeVoters.end(), leaderId) ||
            std::binary_search(activeVoters.begin(), activeVoters.end(), receiverId) ||
            configurationIndex <= committedConfiguration->ConfigurationIndex() ||
            configurationTerm < committedConfiguration->ConfigurationTerm())
            return {true, false, 0};
        return {true, true, size_t(headerBytes), true};
    }

    if (!pendingConfiguration && !pendingConfigurationBoundary && stableImage && committedConfiguration->IsJoint() &&
        SnapshotMetadataCompletesKnownJoint(knownSnapshot, committedConfiguration, stableImage,
            stableLineage, incomingOld, incomingNew, incomingPrevious,
            previousConfigurationIndex, previousConfigurationTerm, previousJointIndex,
            previousJointTerm, configurationIndex, configurationTerm, leaderId))
        return {true, true, size_t(headerBytes)};

    if (!configurationHistory && SnapshotMetadataExtendsKnownStable(committedConfiguration, stableLineage,
            allowStaleStoreMembershipRecovery, incomingOld, incomingPrevious,
            previousConfigurationIndex, previousConfigurationTerm, previousJointIndex,
            previousJointTerm, configurationIndex, configurationTerm, leaderId))
        return {true, true, size_t(headerBytes)};

    if (stableImage && configurationHistory && SnapshotHistoryExtendsKnownStable(
            committedConfiguration, allowStaleStoreMembershipRecovery, transitions,
            incomingOld, configurationIndex, configurationTerm, leaderId))
        return {true, true, size_t(headerBytes)};

    if (joint && configurationHistory && SnapshotHistoryExtendsKnownStableToJoint(
            committedConfiguration, allowStaleStoreMembershipRecovery, transitions,
            incomingOld, incomingNew, configurationIndex, configurationTerm, leaderId))
        return {true, true, size_t(headerBytes)};

    if (pendingConfigurationBoundary) {
        if (incomingOld != pendingConfiguration->OldVoters() ||
            incomingNew != pendingConfiguration->NewVoters())
            return {true, false, 0};
        if ((stableLineage || jointLineage) &&
            (incomingPrevious != pendingConfiguration->PreviousVoters() ||
             previousConfigurationIndex != pendingConfiguration->PreviousConfigurationIndex() ||
             previousConfigurationTerm != pendingConfiguration->PreviousConfigurationTerm() ||
             previousJointIndex != pendingConfiguration->PreviousJointIndex() ||
             previousJointTerm != pendingConfiguration->PreviousJointTerm()))
            return {true, false, 0};
        const auto activeVoters = ConfigurationVoterUnion(
            SnapshotLeaderConfiguration(*committedConfiguration, *pendingConfiguration));
        if (!std::binary_search(activeVoters.begin(), activeVoters.end(), leaderId) ||
            !SnapshotMetadataContinuesKnown(knownSnapshot, joint, stableImage,
                pendingConfiguration->OldVoters(), pendingConfiguration->NewVoters(),
                configurationIndex, configurationTerm))
            return {true, false, 0};
        return {true, true, size_t(headerBytes)};
    }

    const auto& expectedOld = committedConfiguration->OldVoters();
    const auto& expectedNew = committedConfiguration->NewVoters();
    if (incomingOld != expectedOld || incomingNew != expectedNew) return {true, false, 0};
    if ((stableLineage || jointLineage) &&
        (incomingPrevious != committedConfiguration->PreviousVoters() ||
         previousConfigurationIndex != committedConfiguration->PreviousConfigurationIndex() ||
         previousConfigurationTerm != committedConfiguration->PreviousConfigurationTerm() ||
         previousJointIndex != committedConfiguration->PreviousJointIndex() ||
         previousJointTerm != committedConfiguration->PreviousJointTerm()))
        return {true, false, 0};
    const auto activeVoters = ConfigurationVoterUnion(*committedConfiguration);
    if (!std::binary_search(activeVoters.begin(), activeVoters.end(), leaderId))
        return {true, false, 0};
    if (!SnapshotMetadataContinuesKnown(knownSnapshot, joint, stableImage,
        expectedOld, expectedNew, configurationIndex, configurationTerm))
        return {true, false, 0};
    return {true, true, size_t(headerBytes)};
}

} // namespace

void TDummyRsm::Validate(TMessageHolder<TCommandRequest> message) const {
    if (!message || message->Len < sizeof(TCommandRequest) ||
        (!(message->Flags & TCommandRequest::EWrite) && message->Len < sizeof(TCommandRequest) + sizeof(int64_t)))
        throw TMessageError("Invalid dummy RSM request");
}

TMessageHolder<TMessage> TDummyRsm::Read(TMessageHolder<TCommandRequest> message, uint64_t index)
{
    if (!message || message->Len < sizeof(TCommandRequest) + sizeof(int64_t))
        throw TMessageError("Missing log read index");
    int64_t readIndex;
    memcpy(&readIndex, message->Data, sizeof(readIndex));
    if (readIndex > 0 && readIndex <= Log.size()) {
        auto log = Log[readIndex-1];
        auto size = log->Len - sizeof(TLogEntry);
        auto mes = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse) + size);
        mes->Index = index;
        memcpy(mes->Data, log->Data, size);
        return mes;
    } else {
        return NewHoldedMessage<TCommandResponse>(TCommandResponse {.Index = index});
    }
}

TMessageHolder<TMessage> TDummyRsm::Write(TMessageHolder<TLogEntry> message, uint64_t index)
{
    if (LastAppliedIndex < index) {
        Log.emplace_back(std::move(message));
        LastAppliedIndex = index;
    }
    return {};
}

TMessageHolder<TLogEntry> TDummyRsm::Prepare(TMessageHolder<TCommandRequest> command)
{
    if (!command || command->Len < sizeof(TCommandRequest)) throw TMessageError("Short command");
    auto dataSize = command->Len - sizeof(TCommandRequest);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry)+dataSize);
    memcpy(entry->Data, command->Data, dataSize);
    return entry;
}

TVolatileState& TVolatileState::SetElectionDue(ITimeSource::Time due) {
    ElectionDue = due;
    return *this;
}

TVolatileState& TVolatileState::Vote(uint32_t nodeId)
{
    Votes.insert(nodeId);
    return *this;
}

TVolatileState& TVolatileState::CommitAdvance(int nservers, const IState& state)
{
    auto lastIndex = state.LastLogIndex;
    Indices.clear(); Indices.reserve(nservers);
    for (auto [_, index] : MatchIndex) {
        Indices.push_back(index);
    }
    Indices.push_back(lastIndex);
    while (Indices.size() < nservers) {
        Indices.push_back(0);
    }
    std::sort(Indices.begin(), Indices.end());
    // Ascending order: a strict majority must have replicated this index.
    // For even cluster sizes, the upper median would accept only half.
    auto commitIndex = std::max(CommitIndex, Indices[(nservers - 1) / 2]);
    if (state.LogTerm(commitIndex) == state.CurrentTerm) {
        CommitIndex = commitIndex;
    }
    return *this;
}

TVolatileState& TVolatileState::CommitAdvance(const std::vector<uint32_t>& voters, uint32_t self, const IState& state) {
    return CommitAdvance(TQuorum(voters), self, state);
}

TVolatileState& TVolatileState::CommitAdvance(const TQuorum& quorum, uint32_t self, const IState& state) {
    const auto quorumIndex = quorum.Value([&](uint32_t id) {
        auto match = MatchIndex.find(id);
        return id == self ? state.LastLogIndex : match == MatchIndex.end() ? uint64_t(0) : match->second;
    }, &Indices);
    const auto index = std::max(CommitIndex, quorumIndex);
    if (state.LogTerm(index) == state.CurrentTerm) CommitIndex = index;
    return *this;
}

TVolatileState& TVolatileState::SetNextIndex(uint32_t id, uint64_t nextIndex)
{
    NextIndex[id] = nextIndex;
    return *this;
}

TVolatileState& TVolatileState::SetMatchIndex(uint32_t id, uint64_t matchIndex)
{
    MatchIndex[id] = matchIndex;
    return *this;
}

TVolatileState& TVolatileState::SetHearbeatDue(uint32_t id, ITimeSource::Time heartbeatDue)
{
    HeartbeatDue[id] = heartbeatDue;
    return *this;
}

TVolatileState& TVolatileState::SetRpcDue(uint32_t id, ITimeSource::Time rpcDue)
{
    RpcDue[id] = rpcDue;
    return *this;
}

TVolatileState& TVolatileState::SetBatchSize(uint32_t id, int size)
{
    BatchSize[id] = size;
    return *this;
}

TVolatileState& TVolatileState::SetBackOff(uint32_t id, int size) {
    BackOff[id] = size;
    return *this;
}

TVolatileState& TVolatileState::SetLeaderId(uint32_t id) {
    LeaderId = id;
    return *this;
}

TVolatileState& TVolatileState::SetCommitIndex(uint64_t index)
{
    CommitIndex = index;
    return *this;
}

TRaft::TRaft(std::shared_ptr<IState> state, int node, const TNodeDict& nodes,
        std::vector<uint32_t> learners)
    : TRaft(std::move(state), node, nodes, std::move(learners), TLogLimits{})
{}

TRaft::TRaft(std::shared_ptr<IState> state, int node, const TNodeDict& nodes,
        std::vector<uint32_t> learners, TLogLimits logLimits)
    : TRaft(std::move(state), node > 0 ? static_cast<uint32_t>(node) : uint32_t{0},
        nodes, std::move(learners), logLimits)
{}

TRaft::TRaft(std::shared_ptr<IState> state, uint32_t node, const TNodeDict& nodes,
        std::vector<uint32_t> learners)
    : TRaft(std::move(state), node, nodes, std::move(learners), TLogLimits{})
{}

TRaft::TRaft(std::shared_ptr<IState> state, uint32_t node, const TNodeDict& nodes,
        std::vector<uint32_t> learners, TLogLimits logLimits)
    : Id(node)
    , Nodes(nodes)
    , MinVotes(0) // derived from voting IDs after role validation
    , Npeers(nodes.size())
    , Nservers(0)
    , State(std::move(state))
    , LogLimits(logLimits)
    , VolatileState(std::make_unique<TVolatileState>())
    , StateName(EState::FOLLOWER)
{
    if (!node || !State) throw std::invalid_argument("Raft requires a positive ID and state");
    if (!LogLimits.MaxUncommittedBytes || !LogLimits.MaxUncommittedEntries ||
        LogLimits.ReservedControlBytes < sizeof(TLogEntry) || !LogLimits.ReservedControlEntries ||
        LogLimits.ReservedControlBytes > LogLimits.MaxUncommittedBytes ||
        LogLimits.ReservedControlEntries > LogLimits.MaxUncommittedEntries)
        throw std::invalid_argument("Raft uncommitted-log limits must be positive");
    // Equal seeds make simultaneously started voters campaign in lockstep.
    // An odd multiplier gives every nonzero node ID a distinct stream.
    Seed = Id * 0x9e3779b9u;
    for (const auto& [id, peer] : Nodes) {
        if (id == 0 || id == Id || !peer)
            throw std::invalid_argument("Invalid Raft peer configuration");
        VolatileState->NextIndex[id] = 1;
    }
    Learners = std::move(learners);
    std::sort(Learners.begin(), Learners.end());
    uint32_t previous = 0;
    for (auto id : Learners) {
        if (id <= previous || (id != Id && !Nodes.contains(id)))
            throw std::invalid_argument("Invalid or duplicate learner ID");
        previous = id;
    }
    if (!std::binary_search(Learners.begin(), Learners.end(), Id)) InitialVoters.push_back(Id);
    for (const auto& [id, _] : Nodes)
        if (!std::binary_search(Learners.begin(), Learners.end(), id)) InitialVoters.push_back(id);
    std::sort(InitialVoters.begin(), InitialVoters.end());
    if (!State->CurrentTerm) throw std::invalid_argument("Stored Raft term must be positive");
    if (!Learners.empty() && !State->SupportsVoterBinding())
        throw std::invalid_argument("Learners require voter-binding-capable storage");
    const auto bound = State->GetBoundVoters();
    if (State->SnapshotIndex > State->LastLogIndex || State->LastLogIndex > uint64_t(INT64_MAX) ||
        State->CommitIndex > State->LastLogIndex)
        throw std::invalid_argument("Invalid retained log bounds");
    const bool commitFloorAdded = State->CommitIndex < State->SnapshotIndex;
    const auto snapshot = State->GetSnapshot();
    if (snapshot) {
        ValidateSnapshot(*snapshot);
    }
    bool hasConfigurationHistory = snapshot && snapshot->HasConfigurationMetadata();
    const bool durableBinding = State->SupportsVoterBinding() && State->SupportsCommitIndexPersistence();
    if (durableBinding && !bound.empty() && !hasConfigurationHistory) {
        for (uint64_t index = State->SnapshotIndex + 1; index <= State->LastLogIndex; ++index) {
            if (State->RetainedLogFlags(index) & TLogEntry::EConfiguration) {
                hasConfigurationHistory = true;
                break;
            }
        }
    }
    if (durableBinding && !bound.empty() && hasConfigurationHistory) {
        // The durable binding is the trusted original voter set. After a
        // committed membership change, removed original voters need not remain
        // configured as replication endpoints on restart.
        ValidateFixedVoters(bound);
        InitialVoters = bound;
    }
    ValidateFixedVoters(InitialVoters);
    Voters = InitialVoters;
    VotingQuorum.emplace(Voters);
    Nservers = Voters.size(); MinVotes = Nservers / 2 + 1;
    if (snapshot) {
        if (snapshot->HasConfigurationMetadata() && bound != InitialVoters)
            throw std::invalid_argument("Configuration snapshot requires the existing voter binding");
        if (snapshot->Index != State->SnapshotIndex || snapshot->Term != State->SnapshotTerm ||
            snapshot->Term > State->CurrentTerm ||
            (snapshot->Voters != InitialVoters && !snapshot->HasConfigurationMetadata()) ||
            State->LastLogTerm < State->SnapshotTerm || State->LastLogTerm > State->CurrentTerm ||
            (State->LastLogIndex == State->SnapshotIndex && State->LastLogTerm != State->SnapshotTerm))
            throw std::invalid_argument("Compacted state snapshot boundary mismatch");
    } else if (State->SnapshotIndex || State->SnapshotTerm) {
        throw std::invalid_argument("Compacted state is missing its snapshot");
    }
    if (State->HasUnboundRecoveredState() && !snapshot)
        throw std::invalid_argument("Recovered durable state requires voter proof before Raft startup");
    auto previousTerm = State->SnapshotTerm;
    for (uint64_t index = State->SnapshotIndex + 1; index <= State->LastLogIndex; ++index) {
        if (snapshot) {
            const auto term = State->RetainedLogTerm(index);
            if (!term || term < previousTerm || term > State->CurrentTerm)
                throw std::invalid_argument("Invalid compacted log term sequence");
            previousTerm = term;
        }
    }
    if (snapshot && previousTerm != State->LastLogTerm)
        throw std::invalid_argument("Compacted log last term mismatch");
    const bool fresh = State->CurrentTerm == 1 && !State->VotedFor && !State->LastLogIndex;
    if (!bound.empty()) {
        if (bound != InitialVoters)
            throw std::invalid_argument("Persisted voter configuration differs from the configured peers");
    }
    const bool bindInitialVoters = bound.empty() && State->SupportsVoterBinding() &&
        (fresh || snapshot || !Learners.empty());
    const auto baseline = snapshot && snapshot->StableConfiguration
        ? TConfigurationHistory::FromStableSnapshot(*snapshot)
        : snapshot && snapshot->IsJoint()
            ? TConfigurationHistory::FromJointSnapshot(*snapshot)
            : TConfigurationHistory::Stable(InitialVoters, 0, 0, State->SnapshotIndex, State->SnapshotTerm);
    ConfigurationBaseline = baseline;
    RefreshConfiguration(std::max(State->CommitIndex, State->SnapshotIndex));
    const bool hasRetainedConfiguration = Configuration->Latest.ConfigurationIndex() > baseline.ConfigurationIndex();
    if (hasRetainedConfiguration && (!State->SupportsVoterBinding() || bound.empty() ||
        !State->SupportsCommitIndexPersistence()))
        throw std::invalid_argument("Retained membership changes require bound voters and a persisted commit index");
    // A configuration-bearing snapshot may have compacted the entry that
    // changed voter roles. Its identity still proves that a persisted vote
    // from the preceding configuration can name a now-removed voter.
    const bool hasMembershipHistory = hasRetainedConfiguration || baseline.ConfigurationIndex() != 0;
    if (State->VotedFor && !IsVoter(State->VotedFor) && !hasMembershipHistory)
        throw std::invalid_argument("Stored vote does not name a configured voter");
    if (!IsVoter(Id) && State->VotedFor && !hasMembershipHistory)
        throw std::invalid_argument("A learner store must not contain a vote");
    if (bindInitialVoters) State->BindVoters(InitialVoters);
    if (commitFloorAdded) {
        State->CommitIndex = State->SnapshotIndex;
        State->Commit();
    }
    VolatileState->CommitIndex = State->CommitIndex;
    RebuildUncommittedLogUsage();
    State->MarkRaftStarted();
}

void TRaft::RefreshConfiguration(uint64_t commitIndex) {
    if (!ConfigurationBaseline)
        throw std::logic_error("Missing trusted configuration baseline");
    auto scan = ScanConfigurationHistory(*State, *ConfigurationBaseline, commitIndex);

    // An uncommitted joint entry already requires both majorities to commit.
    // Once the joint entry commits, keep joint consensus until its stable
    // successor commits as well. This makes truncating either suffix restore
    // the quorum implied by the surviving log.
    const TConfigurationHistory* quorumConfiguration = nullptr;
    if (scan.Committed.IsJoint()) quorumConfiguration = &scan.Committed;
    else if (scan.Latest.IsJoint()) quorumConfiguration = &scan.Latest;
    else quorumConfiguration = &scan.Committed;

    std::vector<uint32_t> voters = quorumConfiguration->OldVoters();
    std::optional<TQuorum> quorum;
    int minVotes = 0;
    if (quorumConfiguration->IsJoint()) {
        voters.insert(voters.end(), quorumConfiguration->NewVoters().begin(),
            quorumConfiguration->NewVoters().end());
        std::sort(voters.begin(), voters.end());
        voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
        quorum.emplace(quorumConfiguration->OldVoters(), quorumConfiguration->NewVoters());
        minVotes = std::max(int(quorumConfiguration->OldVoters().size() / 2 + 1),
            int(quorumConfiguration->NewVoters().size() / 2 + 1));
    } else {
        quorum.emplace(quorumConfiguration->OldVoters());
        minVotes = int(quorumConfiguration->OldVoters().size() / 2 + 1);
    }
    for (auto id : voters)
        if (id != Id && !Nodes.contains(id))
            throw std::invalid_argument("Active configuration has no replication target for a voter");

    std::vector<uint32_t> learners;
    if (!std::binary_search(voters.begin(), voters.end(), Id)) learners.push_back(Id);
    for (const auto& [id, _] : Nodes)
        if (!std::binary_search(voters.begin(), voters.end(), id)) learners.push_back(id);
    std::sort(learners.begin(), learners.end());

    Voters.swap(voters);
    Learners.swap(learners);
    VotingQuorum.swap(quorum);
    MinVotes = minVotes;
    Nservers = int(Voters.size());
    Configuration = std::move(scan);
    const auto& baseline = *ConfigurationBaseline;
    const auto& latest = Configuration->Latest;
    ConfigurationScanRequired = baseline.ConfigurationIndex() != latest.ConfigurationIndex() ||
        baseline.ConfigurationTerm() != latest.ConfigurationTerm() || baseline.IsJoint() != latest.IsJoint() ||
        baseline.OldVoters() != latest.OldVoters() || baseline.NewVoters() != latest.NewVoters();

    if (!IsVoter(Id)) {
        VolatileState->Votes.clear();
        if (StateName == EState::CANDIDATE || StateName == EState::LEADER) {
            StateName = EState::FOLLOWER;
            VolatileState->LeaderId = 0;
        }
    }
}

void TRaft::OnRequestVote(ITimeSource::Time now, TMessageHolder<TRequestVoteRequest> message) {
    if (!IsVoter(Id)) {
        (void)Nodes.at(message->Src)->TrySend(NewHoldedMessage(
            TMessageEx{.Src = Id, .Dst = message->Src, .Term = State->CurrentTerm}, TRequestVoteResponse{.VoteGranted = false}));
        return;
    }
    if (message->Term < State->CurrentTerm) {
        auto reply = NewHoldedMessage(
            TMessageEx {.Src = Id, .Dst = message->Src, .Term = State->CurrentTerm},
            TRequestVoteResponse {.VoteGranted = false});
        (void)Nodes[reply->Dst]->TrySend(std::move(reply));
    } else if (message->Term == State->CurrentTerm) {
        bool accept = false;
        if (State->VotedFor == 0 || State->VotedFor == message->CandidateId) {
            if (message->LastLogTerm > State->LogTerm()) {
                accept = true;
            } else if (message->LastLogTerm == State->LogTerm() && message->LastLogIndex >= State->LastLogIndex) {
                accept = true;
            }
        }

        auto reply = NewHoldedMessage(
            TMessageEx {.Src = Id, .Dst = message->Src, .Term = State->CurrentTerm},
            TRequestVoteResponse {.VoteGranted = accept});

        if (accept) {
            VolatileState->ElectionDue = MakeElection(now);
            State->VotedFor = message->CandidateId;
            State->Commit();
        }

        (void)Nodes[reply->Dst]->TrySend(std::move(reply));
    }
}

void TRaft::OnRequestVote(TMessageHolder<TRequestVoteResponse> message) {
    if (message->VoteGranted && message->Term == State->CurrentTerm) {
        (*VolatileState)
            .Vote(message->Src)
            .SetRpcDue(message->Src, ITimeSource::Max);
    }
}

void TRaft::OnAppendEntries(ITimeSource::Time now, TMessageHolder<TAppendEntriesRequest> message) {
    if (message->Term < State->CurrentTerm) {
        auto reply = NewHoldedMessage(
            TMessageEx {
                .Src = Id,
                .Dst = message->Src,
                .Term = State->CurrentTerm,
                .Seqno = message->Seqno,
            },
            TAppendEntriesResponse {
                .MatchIndex = 0,
                .Success = false,
                .Padding = TAppendEntriesResponse::SupportsConfigurationBarrier,
            });
        (void)Nodes[reply->Dst]->TrySend(std::move(reply));
        return;
    }

    assert(message->Term == State->CurrentTerm);
    VolatileState->LeadershipTransfer.reset();

    uint64_t matchIndex = 0;
    uint64_t commitIndex = VolatileState->CommitIndex;
    bool success = false;
    bool configurationTouched = false;
    if (message->PrevLogIndex >= State->SnapshotIndex && (message->PrevLogIndex == 0 ||
        (message->PrevLogIndex <= State->LastLogIndex
            && State->LogTerm(message->PrevLogIndex) == message->PrevLogTerm)))
    {
        uint64_t conflictIndex = 0;
        uint32_t conflictOffset = 0;
        auto index = message->PrevLogIndex;
        for (uint32_t i = 0; i < message.PayloadSize; ++i) {
            ++index;
            const auto entry = message.Payload[i].Cast<TLogEntry>();
            if (State->LogTerm(index) != entry->Term) {
                conflictIndex = index;
                conflictOffset = i;
                break;
            }
        }

        bool fits = true;
        if (conflictIndex) {
            // Refuse a conflicting leader suffix at or below a commit boundary
            // before touching storage. Storage removal methods also guard this,
            // but a protocol rejection is the correct response to stale input.
            const auto commitFloor = std::max(State->CommitIndex, VolatileState->CommitIndex);
            if (conflictIndex <= commitFloor) {
                fits = false;
            } else {
                uint64_t replacedBytes = 0;
                size_t replacedEntries = 0;
                for (auto it = UncommittedLog.rbegin(); it != UncommittedLog.rend() &&
                        it->Index >= conflictIndex; ++it) {
                    replacedBytes += it->Bytes;
                    ++replacedEntries;
                }
                uint64_t incomingBytes = 0;
                for (uint32_t i = conflictOffset; i < message.PayloadSize; ++i)
                    incomingBytes += message.Payload[i].Cast<TLogEntry>()->Len;
                const auto retainedBytes = UncommittedLogBytes - replacedBytes;
                const auto retainedEntries = UncommittedLog.size() - replacedEntries;
                const auto incomingEntries = size_t(message.PayloadSize - conflictOffset);
                fits = retainedBytes <= LogLimits.MaxUncommittedBytes &&
                    incomingBytes <= LogLimits.MaxUncommittedBytes - retainedBytes &&
                    retainedEntries <= LogLimits.MaxUncommittedEntries &&
                    incomingEntries <= LogLimits.MaxUncommittedEntries - retainedEntries;
            }
        }

        if (fits) {
            success = true;
            index = message->PrevLogIndex;
            for (uint32_t i = 0; i < message.PayloadSize; ++i) {
                auto& data = message.Payload[i];
                auto entry = data.Cast<TLogEntry>();
                ++index;
                // replace or append log entries
                if (State->LogTerm(index) != entry->Term) {
                    if (Configuration && index <= Configuration->Latest.ConfigurationIndex())
                        configurationTouched = true;
                    while (State->LastLogIndex > index - 1) {
                        const auto removedIndex = State->LastLogIndex;
                        State->RemoveLast();
                        RemoveUncommittedLogSuffix(removedIndex);
                    }
                    const auto bytes = entry->Len;
                    State->Append(entry);
                    RecordUncommittedLogEntry(index, bytes);
                    if (entry->Flags & TLogEntry::EConfiguration) configurationTouched = true;
                }
            }

            matchIndex = index;
            commitIndex = std::max(commitIndex, std::min(message->LeaderCommit, matchIndex));
        }
    }

    if (!success && message->PrevLogIndex < State->SnapshotIndex) matchIndex = State->SnapshotIndex;
    auto reply = NewHoldedMessage(
        TMessageEx {.Src = Id, .Dst = message->Src, .Term = State->CurrentTerm, .Seqno = message->Seqno},
        TAppendEntriesResponse {
            .MatchIndex = matchIndex,
            .Success = success,
            .Padding = TAppendEntriesResponse::SupportsConfigurationBarrier,
        });

    AdvanceCommitIndex(commitIndex);
    if (configurationTouched) RefreshConfiguration(VolatileState->CommitIndex);
    (*VolatileState)
        .SetLeaderId(message->LeaderId)
        .SetElectionDue(MakeElection(now));

    Become(EState::FOLLOWER);
    (void)Nodes[reply->Dst]->TrySend(std::move(reply));
}

void TRaft::OnAppendEntries(ITimeSource::Time now, TMessageHolder<TAppendEntriesResponse> message) {
    if (message->Term != State->CurrentTerm) {
        return;
    }

    auto nodeId = message->Src;
    if (!Nodes.contains(nodeId)) return;
    auto pending = VolatileState->PendingAppendRpcs.find(nodeId);
    if (pending == VolatileState->PendingAppendRpcs.end()) return;
    auto& requests = pending->second;
    auto request = std::find_if(requests.begin(), requests.end(), [&](const auto& rpc) {
        return rpc.Seqno == message->Seqno;
    });
    if (request == requests.end() ||
        (message->Success && message->MatchIndex != request->LastIndex)) return;
    auto& capabilities = VolatileState->PeerCapabilities[nodeId];
    if (message->Seqno >= capabilities.AppendAckSeqno) {
        capabilities.AppendAckSeqno = message->Seqno;
        capabilities.ConfigurationBarrier =
            message->Padding & TAppendEntriesResponse::SupportsConfigurationBarrier;
    }
    const auto acknowledgedLeaderCommit = request->LeaderCommit;
    // A newer response supersedes older RPCs. Reordered/duplicate failures
    // must not undo progress or repeatedly increase backoff.
    requests.erase(requests.begin(), std::next(request));
    if (message->Success) {
        if (message->Seqno > 0 && message->Seqno <= VolatileState->LastSentSeqno[nodeId]) {
            auto& acknowledged = VolatileState->ReadAckSeqno[nodeId];
            acknowledged = std::max(acknowledged, message->Seqno);
        }
        auto matchIndex = std::max(VolatileState->MatchIndex[nodeId], message->MatchIndex);
        auto commitAckIndex = std::max(VolatileState->CommitAckIndex[nodeId],
            std::min(acknowledgedLeaderCommit, message->MatchIndex));
        (*VolatileState)
            .SetMatchIndex(nodeId, matchIndex)
            .SetNextIndex(nodeId, matchIndex+1)
            .SetRpcDue(nodeId, ITimeSource::Time{})
            .SetBatchSize(nodeId, 1024)
            .SetBackOff(nodeId, 1);
        VolatileState->CommitAckIndex[nodeId] = commitAckIndex;
        AdvanceLeaderCommit();
    } else if (message->MatchIndex > 0) {
        // A compacted-prefix hint is not replication evidence. Probe its boundary
        // with AppendEntries before advancing MatchIndex or confirming reads.
        (*VolatileState).SetNextIndex(nodeId, std::max(VolatileState->MatchIndex[nodeId] + 1, message->MatchIndex + 1))
            .SetRpcDue(nodeId, ITimeSource::Time{}).SetBatchSize(nodeId, 1).SetBackOff(nodeId, 1);
    } else {
        auto backOff = std::max(VolatileState->BackOff[nodeId], 1);
        auto nextIndex = VolatileState->NextIndex[nodeId] > backOff
            ? VolatileState->NextIndex[nodeId] - backOff
            : 0;
        (*VolatileState)
            .SetNextIndex(nodeId, std::max(VolatileState->MatchIndex[nodeId] + 1, nextIndex))
            .SetRpcDue(nodeId, ITimeSource::Time{})
            .SetBatchSize(nodeId, 1)
            .SetBackOff(nodeId, std::min(32768, backOff << 1));
    }
    MaybeDistributeConfigurationBarrier(now);
}

TMessageHolder<TRequestVoteRequest> TRaft::CreateVote(uint32_t nodeId) {
    auto mes = NewHoldedMessage(
        TMessageEx {.Src = Id, .Dst = nodeId, .Term = State->CurrentTerm},
        TRequestVoteRequest {
            .LastLogIndex = State->LastLogIndex,
            .LastLogTerm = State->LastLogTerm,
            .CandidateId = Id,
        });
    return mes;
}

TMessageHolder<TAppendEntriesRequest> TRaft::CreateAppendEntries(uint32_t nodeId) {
    int batchSize = std::max(1, VolatileState->BatchSize[nodeId]);
    auto prevIndex = VolatileState->NextIndex[nodeId] - 1;
    auto lastIndex = std::min<uint64_t>(prevIndex+batchSize, State->LastLogIndex);
    if (VolatileState->MatchIndex[nodeId]+1 < VolatileState->NextIndex[nodeId]) {
        lastIndex = prevIndex;
    }

    const TMessageLimits limits;
    uint64_t bytes = sizeof(TAppendEntriesRequest);
    std::vector<TMessageHolder<TLogEntry>> entries;
    for (auto i = prevIndex + 1; i <= lastIndex && entries.size() < limits.MaxEntries; ++i) {
        auto entry = State->Get(i);
        ValidateMessageHeader(entry->Type, entry->Len, limits);
        if (entry->Len > limits.MaxBatchBytes - bytes) break;
        bytes += entry->Len;
        entries.push_back(std::move(entry));
    }
    lastIndex = prevIndex + entries.size();

    const auto leaderCommit = std::min(VolatileState->CommitIndex, lastIndex);
    auto mes = NewHoldedMessage(
        TMessageEx {.Src = Id, .Dst = nodeId, .Term = State->CurrentTerm, .Seqno = NextSeqno()},
        TAppendEntriesRequest {
            .PrevLogIndex = prevIndex,
            .PrevLogTerm = State->LogTerm(prevIndex),
            .LeaderCommit = leaderCommit,
            .LeaderId = Id,
            .Nentries = static_cast<uint32_t>(lastIndex - prevIndex),
        });

    if (!entries.empty()) {
        mes.InitPayload(entries.size());
        for (uint32_t i = 0; i < entries.size(); ++i) mes.Payload[i] = entries[i];
    }
    VolatileState->LastSentSeqno[nodeId] = mes->Seqno;
    auto& pending = VolatileState->PendingAppendRpcs[nodeId];
    pending.push_back({mes->Seqno, lastIndex, leaderCommit});
    if (pending.size() > TVolatileState::MaxPendingAppendRpcs) pending.pop_front();
    return mes;
}

void TRaft::Follower(ITimeSource::Time now, TMessageHolder<TMessage> message) {
    if (auto maybeRequestVote = message.Maybe<TRequestVoteRequest>()) {
        OnRequestVote(now, std::move(maybeRequestVote.Cast()));
    } else if (auto maybeAppendEntries = message.Maybe<TAppendEntriesRequest>()) {
        OnAppendEntries(now, std::move(maybeAppendEntries.Cast()));
    } else if (auto snapshot = message.Maybe<TInstallSnapshotRequest>()) {
        OnInstallSnapshot(now, snapshot.Cast());
    } else if (auto transfer = message.Maybe<TTimeoutNowRequest>()) {
        OnTimeoutNow(now, transfer.Cast());
    } else if (auto barrier = message.Maybe<TConfigurationBarrierRequest>()) {
        OnConfigurationBarrier(barrier.Cast());
    }
}

void TRaft::Candidate(ITimeSource::Time now, TMessageHolder<TMessage> message) {
    if (auto maybeResponseVote = message.Maybe<TRequestVoteResponse>()) {
        OnRequestVote(std::move(maybeResponseVote.Cast()));
    } else if (auto maybeRequestVote = message.Maybe<TRequestVoteRequest>()) {
        OnRequestVote(now, std::move(maybeRequestVote.Cast()));
    } else if (auto maybeAppendEntries = message.Maybe<TAppendEntriesRequest>()) {
        OnAppendEntries(now, std::move(maybeAppendEntries.Cast()));
    } else if (auto snapshot = message.Maybe<TInstallSnapshotRequest>()) {
        OnInstallSnapshot(now, snapshot.Cast());
    } else if (auto barrier = message.Maybe<TConfigurationBarrierRequest>()) {
        OnConfigurationBarrier(barrier.Cast());
    }
}

void TRaft::Leader(ITimeSource::Time now, TMessageHolder<TMessage> message) {
    if (auto snapshot = message.Maybe<TInstallSnapshotResponse>()) {
        OnInstallSnapshot(snapshot.Cast());
    } else if (auto maybeAppendEntries = message.Maybe<TAppendEntriesResponse>()) {
        OnAppendEntries(now, std::move(maybeAppendEntries.Cast()));
    } else if (auto maybeVoteRequest = message.Maybe<TRequestVoteRequest>()) {
        OnRequestVote(now, std::move(maybeVoteRequest.Cast()));
    } else if (auto maybeAppendEntries = message.Maybe<TAppendEntriesRequest>()) {
        OnAppendEntries(now, std::move(maybeAppendEntries.Cast()));
    } else if (auto snapshot = message.Maybe<TInstallSnapshotRequest>()) {
        OnInstallSnapshot(now, snapshot.Cast());
    } else if (auto barrier = message.Maybe<TConfigurationBarrierRequest>()) {
        OnConfigurationBarrier(barrier.Cast());
    } else if (auto barrier = message.Maybe<TConfigurationBarrierResponse>()) {
        OnConfigurationBarrier(now, barrier.Cast());
    }
}

void TRaft::Become(EState newStateName) {
    if (!IsVoter(Id) && (newStateName == EState::CANDIDATE || newStateName == EState::LEADER))
        throw std::logic_error("A learner cannot campaign or lead");
    if (StateName != newStateName) {
        StateName = newStateName;
    }
}

void TRaft::Process(ITimeSource::Time now, TMessageHolder<TMessage> message, const std::shared_ptr<INode>& replyTo) {
    if (!ValidateRpc(message)) return;
    if (message.IsEx()) {
        auto messageEx = message.Cast<TMessageEx>();
        if (messageEx->Term > State->CurrentTerm) {
            State->CurrentTerm = messageEx->Term;
            State->VotedFor = 0;
            State->Commit();
            StateName = EState::FOLLOWER;
            VolatileState->LeadershipTransfer.reset();
            IncomingSnapshot.reset();
            VolatileState->SnapshotRpcs.clear();
            EncodedSnapshot.reset();
            EncodedSnapshotIndex = 0;
            VolatileState->LeaderId = 0;
            if (VolatileState->ElectionDue <= now || VolatileState->ElectionDue == ITimeSource::Max) {
                VolatileState->ElectionDue = MakeElection(now);
            }
        }
    }

    switch (StateName) {
    case EState::FOLLOWER:
        Follower(now, std::move(message));
        break;
    case EState::CANDIDATE:
        Candidate(now, std::move(message));
        break;
    case EState::LEADER:
        Leader(now, std::move(message));
        break;
    default:
        throw std::logic_error("Unknown state");
    }
}

bool TRaft::ValidateRpc(TMessageHolder<TMessage> message) const {
    if (!message || !message.IsEx()) return false;
    try { ValidateMessageTree(message); } catch (const TMessageError&) { return false; }
    auto header = message.Cast<TMessageEx>();
    if (header->Dst != Id || !Nodes.contains(header->Src)) return false;
    if (!IsVoter(header->Src)) {
        const bool forwardingJoint = header->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST) &&
            MayForwardPriorTermJointEntry(message.Cast<TAppendEntriesRequest>(),
                Configuration ? &*Configuration : nullptr);
        if (!forwardingJoint &&
            ((header->Type != uint32_t(EMessageType::APPEND_ENTRIES_RESPONSE) &&
             header->Type != uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE) &&
             header->Type != uint32_t(EMessageType::CONFIGURATION_BARRIER_RESPONSE) &&
             header->Type != uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST)) ||
            (header->Type != uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST) &&
             header->Term != State->CurrentTerm)))
            return false; // Learners cannot campaign or lead; the joint-forward exception is checked below.
    }
    constexpr uint64_t maxIndex = INT64_MAX;
    switch (EMessageType(message->Type)) {
    case EMessageType::REQUEST_VOTE_REQUEST: {
        auto vote = message.Cast<TRequestVoteRequest>();
        return vote->CandidateId == header->Src && vote->LastLogIndex <= maxIndex &&
            vote->LastLogTerm <= header->Term &&
            ((vote->LastLogIndex == 0) == (vote->LastLogTerm == 0));
    }
    case EMessageType::REQUEST_VOTE_RESPONSE:
        return message.Cast<TRequestVoteResponse>()->VoteGranted <= 1;
    case EMessageType::APPEND_ENTRIES_RESPONSE: {
        auto reply = message.Cast<TAppendEntriesResponse>();
        return reply->Success <= 1 && reply->MatchIndex <= maxIndex &&
            reply->MatchIndex <= State->LastLogIndex;
    }
    case EMessageType::INSTALL_SNAPSHOT_REQUEST: {
        auto request = message.Cast<TInstallSnapshotRequest>();
        const auto length = request->Len - sizeof(TInstallSnapshotRequest);
        // A later boundary cannot have an older term than the committed prefix.
        // For an already committed, retained boundary, the terms must match;
        // an older compacted boundary cannot have a newer term than the snapshot.
        const bool valid = request->LeaderId == header->Src && request->SnapshotIndex > 0 && request->SnapshotIndex <= maxIndex &&
            request->SnapshotTerm > 0 && request->SnapshotTerm <= header->Term && request->Done <= 1 &&
            (request->SnapshotIndex <= VolatileState->CommitIndex ||
                request->SnapshotTerm >= State->LogTerm(VolatileState->CommitIndex)) &&
            (request->SnapshotIndex > VolatileState->CommitIndex || request->SnapshotIndex < State->SnapshotIndex ||
                request->SnapshotTerm == State->LogTerm(request->SnapshotIndex)) &&
            (request->SnapshotIndex >= State->SnapshotIndex || request->SnapshotTerm <= State->SnapshotTerm) &&
            request->TotalBytes >= 44 && request->TotalBytes <= TSnapshot::MaxBytes &&
            request->Offset <= request->TotalBytes && length > 0 && length <= TInstallSnapshotRequest::MaxChunkBytes &&
            length <= request->TotalBytes - request->Offset &&
            bool(request->Done) == (request->Offset + length == request->TotalBytes);
        if (!valid) return false;
        if (header->Term > State->CurrentTerm || !IsVoter(header->Src)) {
            // Do not persist a higher term based on an InstallSnapshot request
            // whose format cannot be accepted by this runtime. A continuation
            // cannot be valid across a term change because term changes discard
            // the partial transfer; an initial chunk must identify a supported
            // fixed/stable format and complete metadata prefix before it can
            // establish leader contact.
            if (header->Term > State->CurrentTerm && request->Offset != 0) return false;
            const auto knownSnapshot = State->GetSnapshot();
            const auto* committedConfiguration = Configuration ? &Configuration->Committed : nullptr;
            const auto* pendingConfiguration = Configuration && Configuration->HasUncommittedConfiguration()
                ? &Configuration->Latest : nullptr;
            const auto application = SnapshotApplication.lock();
            const auto snapshotVoters = committedConfiguration
                ? ConfigurationVoterUnion(*committedConfiguration) : std::vector<uint32_t>{};
            const auto probe = ProbeSnapshotMetadata(std::string_view(request->Data, length),
                request->TotalBytes, request->SnapshotIndex, request->SnapshotTerm,
                snapshotVoters, knownSnapshot.get(), committedConfiguration,
                pendingConfiguration,
                CanBootstrapFreshLearnerSnapshot(Id, *State, GetVoterIds(), application.get(),
                    committedConfiguration, !pendingConfiguration),
                HasPersistedRecoveryState(*State),
                request->LeaderId, Id);
            if (!probe.Complete) {
                // Large, valid membership histories can exceed one transport
                // chunk. Let a recognized voter start buffering a supported
                // initial snapshot chunk so the sender can continue; the full
                // bounded metadata is still checked before leader contact or
                // snapshot installation.
                const bool supportedInitialChunk = header->Term > State->CurrentTerm &&
                    IsVoter(header->Src) && request->Offset == 0 && length >= 8 &&
                    IsSupportedSnapshotMagicPrefix(std::string_view(request->Data, length));
                return header->Term == State->CurrentTerm || supportedInitialChunk;
            }
            if (!probe.Supported) return false;
        }
        return true;
    }
    case EMessageType::INSTALL_SNAPSHOT_RESPONSE: {
        auto response = message.Cast<TInstallSnapshotResponse>();
        return response->SnapshotIndex > 0 && response->SnapshotIndex <= maxIndex &&
            response->NextOffset <= TSnapshot::MaxBytes && response->Success <= 1 && response->Installed <= 1 &&
            (!response->Installed || response->Success) && (response->Success || response->NextOffset == 0);
    }
    case EMessageType::TIMEOUT_NOW_REQUEST: {
        auto transfer = message.Cast<TTimeoutNowRequest>();
        const bool pendingStableSuccessor = Configuration && HasPendingStableSuccessor(*Configuration);
        if (StateName != EState::FOLLOWER || header->Term != State->CurrentTerm ||
            State->CurrentTerm == UINT64_MAX || !Configuration ||
            (Configuration->HasUncommittedConfiguration() && !pendingStableSuccessor) ||
            !Configuration->Committed.IsJoint())
            return false;
        const auto& joint = Configuration->Committed;
        return transfer->LeaderId == header->Src && transfer->TargetId == Id &&
            VolatileState->LeaderId == header->Src &&
            std::binary_search(joint.OldVoters().begin(), joint.OldVoters().end(), header->Src) &&
            !std::binary_search(joint.NewVoters().begin(), joint.NewVoters().end(), header->Src) &&
            std::binary_search(joint.NewVoters().begin(), joint.NewVoters().end(), Id) &&
            transfer->ConfigurationIndex == joint.ConfigurationIndex() &&
            transfer->ConfigurationTerm == joint.ConfigurationTerm() &&
            transfer->LastLogIndex == State->LastLogIndex && transfer->LastLogTerm == State->LastLogTerm &&
            transfer->CommitIndex >= joint.ConfigurationIndex() &&
            transfer->CommitIndex <= VolatileState->CommitIndex && transfer->CommitIndex <= State->CommitIndex &&
            transfer->CommitTerm > 0 && State->LogTerm(transfer->CommitIndex) == transfer->CommitTerm;
    }
    case EMessageType::CONFIGURATION_BARRIER_REQUEST: {
        auto request = message.Cast<TConfigurationBarrierRequest>();
        return header->Term == State->CurrentTerm && request->LeaderId == header->Src &&
            request->Padding == 0 &&
            request->ConfigurationIndex > 0 && request->ConfigurationIndex <= maxIndex &&
            request->ConfigurationTerm > 0 && request->ConfigurationTerm <= header->Term;
    }
    case EMessageType::CONFIGURATION_BARRIER_RESPONSE: {
        auto response = message.Cast<TConfigurationBarrierResponse>();
        return response->ConfigurationIndex > 0 && response->ConfigurationIndex <= maxIndex &&
            response->ConfigurationTerm > 0 && response->ConfigurationTerm <= header->Term &&
            response->Accepted <= 1 && response->Padding == 0;
    }
    case EMessageType::APPEND_ENTRIES_REQUEST: {
        auto append = message.Cast<TAppendEntriesRequest>();
        if (append->LeaderId != header->Src || append->PrevLogIndex > maxIndex ||
            append->LeaderCommit > maxIndex || append->Nentries > maxIndex - append->PrevLogIndex ||
            append->PrevLogTerm > header->Term ||
            ((append->PrevLogIndex == 0) != (append->PrevLogTerm == 0))) return false;
        auto previousTerm = append->PrevLogTerm;
        for (uint32_t i = 0; i < message.PayloadSize; ++i) {
            auto entry = message.Payload[i].Cast<TLogEntry>();
            if (!entry->Term || entry->Term < previousTerm || entry->Term > header->Term) return false;
            previousTerm = entry->Term;
            const auto index = append->PrevLogIndex + i + 1;
            if (index >= State->SnapshotIndex && index <= VolatileState->CommitIndex && State->LogTerm(index) != entry->Term) return false;
        }
        return ValidateAppendConfiguration(append);
    }
    default: return false;
    }
}

bool TRaft::ValidateAppendConfiguration(TMessageHolder<TAppendEntriesRequest> message) const {
    if (message->Nentries != message.PayloadSize) return false;
    bool hasConfigurationEntry = false;
    for (uint32_t i = 0; i < message.PayloadSize; ++i) {
        auto entry = message.Payload[i].Cast<TLogEntry>();
        if (!(entry->Flags & TLogEntry::EConfiguration)) continue;
        try {
            const auto record = DecodeConfigurationEntry(entry);
            const bool leaderInOld = std::binary_search(record.OldVoters.begin(), record.OldVoters.end(), message->LeaderId);
            const bool leaderInNew = std::binary_search(record.NewVoters.begin(), record.NewVoters.end(), message->LeaderId);
            // An old-set leader originates a joint entry. A new-set leader can
            // forward that prior-term entry after election under the joint
            // quorum; it cannot introduce one in its own term.
            const bool forwardingJoint = record.Phase == EConfigurationPhase::Joint &&
                leaderInNew && entry->Term < message->Term &&
                (IsVoter(message->LeaderId) || MayForwardPriorTermJointRecord(record, message->Term,
                    message->LeaderId, Configuration ? &*Configuration : nullptr));
            // A new-set leader originates the stable successor. An old-set
            // leader elected under the still-active joint quorum may forward
            // that prior-term entry so a new-set target can catch up before
            // leadership transfer.
            const bool forwardingStable = record.Phase == EConfigurationPhase::Stable &&
                leaderInOld && entry->Term < message->Term;
            if ((record.Phase == EConfigurationPhase::Joint && !leaderInOld && !forwardingJoint) ||
                (record.Phase == EConfigurationPhase::Stable && !leaderInNew && !forwardingStable))
                return false;
        } catch (const std::exception&) {
            return false;
        }
        hasConfigurationEntry = true;
    }
    if (hasConfigurationEntry && (!ConfigurationBaseline || !State->SupportsVoterBinding() ||
        !State->SupportsCommitIndexPersistence() || State->GetBoundVoters() != InitialVoters))
        return false;

    // A mismatched previous boundary cannot change this log, so after checking
    // the entry frames we can safely return the normal AppendEntries conflict.
    if (message->Term < State->CurrentTerm || message->PrevLogIndex < State->SnapshotIndex ||
        (message->PrevLogIndex && (message->PrevLogIndex > State->LastLogIndex ||
            State->LogTerm(message->PrevLogIndex) != message->PrevLogTerm)))
        return true;

    bool scanRequired = hasConfigurationEntry;
    for (uint32_t i = 0; i < message.PayloadSize; ++i) {
        const auto index = message->PrevLogIndex + i + 1;
        auto incoming = message.Payload[i].Cast<TLogEntry>();
        const auto localTerm = State->LogTerm(index);
        const bool incomingConfiguration = incoming->Flags & TLogEntry::EConfiguration;
        const bool localConfiguration = index <= State->LastLogIndex &&
            (State->RetainedLogFlags(index) & TLogEntry::EConfiguration);
        if (localTerm == incoming->Term && (incomingConfiguration || localConfiguration)) {
            auto local = State->Get(index);
            if (!local || local->Flags != incoming->Flags || local->Len != incoming->Len ||
                memcmp(local.Mes, incoming.Mes, incoming->Len) != 0)
                return false;
        }
        if (localTerm != incoming->Term && Configuration &&
            index <= Configuration->Latest.ConfigurationIndex())
            scanRequired = true;
    }
    if (!scanRequired) return true;
    if (!ConfigurationBaseline || !State->SupportsVoterBinding() ||
        !State->SupportsCommitIndexPersistence() || State->GetBoundVoters() != InitialVoters)
        return false;

    try {
        TState shadow;
        static_cast<TStateFields&>(shadow) = static_cast<const TStateFields&>(*State);
        shadow.Snapshot = State->GetSnapshot();
        shadow.BoundVoters = State->GetBoundVoters();
        shadow.LastLogIndex = State->SnapshotIndex;
        shadow.LastLogTerm = State->SnapshotTerm;
        for (uint64_t index = State->SnapshotIndex + 1; index <= State->LastLogIndex; ++index) {
            auto entry = State->Get(index);
            if (!entry) return false;
            shadow.Append(std::move(entry));
        }
        shadow.CurrentTerm = message->Term;
        auto index = message->PrevLogIndex;
        for (uint32_t i = 0; i < message.PayloadSize; ++i) {
            auto entry = message.Payload[i].Cast<TLogEntry>();
            ++index;
            if (shadow.LogTerm(index) == entry->Term) continue;
            while (shadow.LastLogIndex > index - 1) shadow.RemoveLast();
            shadow.Append(std::move(entry));
        }
        const auto commitIndex = std::max(VolatileState->CommitIndex,
            std::min(message->LeaderCommit, index));
        const auto scan = ScanConfigurationHistory(shadow, *ConfigurationBaseline, commitIndex);
        const auto* active = scan.Committed.IsJoint() ? &scan.Committed :
            scan.Latest.IsJoint() ? &scan.Latest : &scan.Committed;
        for (auto id : active->OldVoters())
            if (id != Id && !Nodes.contains(id)) return false;
        for (auto id : active->NewVoters())
            if (id != Id && !Nodes.contains(id)) return false;
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

void TRaft::FollowerTimeout(ITimeSource::Time now) {
    if (VolatileState->LeadershipTransfer && now >= VolatileState->LeadershipTransfer->RetryDue &&
        now < VolatileState->ElectionDue) {
        auto& transfer = *VolatileState->LeadershipTransfer;
        transfer.RetryDue = now + TTimeout::LeadershipTransferRetry;
        (void)Nodes.at(transfer.TargetId)->TrySend(NewHoldedMessage(
            TMessageEx{.Src = Id, .Dst = transfer.TargetId, .Term = State->CurrentTerm},
            TTimeoutNowRequest{
                .LeaderId = Id, .TargetId = transfer.TargetId,
                .LastLogIndex = transfer.LastLogIndex, .LastLogTerm = transfer.LastLogTerm,
                .CommitIndex = transfer.CommitIndex, .CommitTerm = transfer.CommitTerm,
                .ConfigurationIndex = transfer.ConfigurationIndex,
                .ConfigurationTerm = transfer.ConfigurationTerm,
            }));
    }
    if (VolatileState->ElectionDue <= now) {
        Become(EState::CANDIDATE);
    }
}

void TRaft::CandidateTimeout(ITimeSource::Time now) {
    for (auto& [id, node] : Nodes) {
        if (!IsVoter(id)) continue;
        if (VolatileState->RpcDue[id] <= now) {
            VolatileState->RpcDue[id] = now + TTimeout::Rpc;
            (void)node->TrySend(CreateVote(id));
        }
    }
}

void TRaft::LeaderTimeout(ITimeSource::Time now) {
    for (auto& [id, node] : Nodes) {
        if (VolatileState->HeartbeatDue[id] <= now
            || (VolatileState->NextIndex[id] <= State->LastLogIndex &&
            VolatileState->RpcDue[id] <= now))
        {
            VolatileState->HeartbeatDue[id] = now + TTimeout::Election / 2;
            VolatileState->RpcDue[id] = now + TTimeout::Rpc;
            if (VolatileState->NextIndex[id] <= State->SnapshotIndex) (void)node->TrySend(CreateInstallSnapshot(id));
            else (void)node->TrySend(CreateAppendEntries(id));
        }
    }

    if (Nservers == 1) {
        AdvanceLeaderCommit();
    }
    MaybeDistributeConfigurationBarrier(now);
}

uint64_t TRaft::ApproveRead() {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can approve a read");
    // Reserve a unique barrier even for a single-node cluster. Every request
    // emitted below is newer than this barrier; old replies cannot satisfy it.
    const uint64_t seqno = NextSeqno();
    for (auto& [id, node] : Nodes) {
        if (VolatileState->NextIndex[id] <= State->SnapshotIndex) (void)node->TrySend(CreateInstallSnapshot(id));
        else (void)node->TrySend(CreateAppendEntries(id));
    }
    return seqno;
}

uint64_t TRaft::CommitSeqno() const {
    if (StateName != EState::LEADER || VolatileState->CommitIndex == 0 ||
        State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm) return 0;
    return VotingQuorum->Value([&](uint32_t id) -> uint64_t {
        if (id == Id) return Seqno - 1; // leader's own confirmation
        auto it = VolatileState->ReadAckSeqno.find(id);
        return it == VolatileState->ReadAckSeqno.end() ? 0 : it->second;
    });
}

uint64_t TRaft::NextSeqno() {
    if (Seqno == UINT64_MAX) throw std::overflow_error("Raft RPC sequence exhausted");
    return Seqno++;
}

void TRaft::ProcessTimeout(ITimeSource::Time now) {
    // Learners do not campaign, so their election timeout cannot be used to
    // reclaim an interrupted InstallSnapshot transfer. Expire partial images
    // independently of role and ordinary heartbeat activity.
    if (IncomingSnapshot && now >= IncomingSnapshot->LastProgress &&
        now - IncomingSnapshot->LastProgress >= TTimeout::SnapshotTransferIdle)
        IncomingSnapshot.reset();
    if (!IsVoter(Id)) return;
    if (StateName == EState::CANDIDATE || StateName == EState::FOLLOWER) {
        if (VolatileState->ElectionDue <= now) {
            if (State->CurrentTerm == UINT64_MAX) throw std::overflow_error("Raft term exhausted");
            IncomingSnapshot.reset();
            auto nextVolatileState = std::make_unique<TVolatileState>();
            for (auto [id, _] : Nodes) {
                nextVolatileState->NextIndex[id] = 1;
            }
            nextVolatileState->ElectionDue = MakeElection(now);
            nextVolatileState->CommitIndex = VolatileState->CommitIndex;
            VolatileState = std::move(nextVolatileState);
            State->VotedFor = Id;
            State->CurrentTerm ++;
            State->Commit();
            Become(EState::CANDIDATE);
        }
    }

    if (StateName == EState::CANDIDATE) {
        if (VotingQuorum->HasQuorum(VolatileState->Votes, Id)) {
            if (State->LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
            auto value = State->LastLogIndex+1;
            const std::vector<uint32_t>* previousVoters = nullptr;
            uint64_t transitionJointIndex = 0;
            if (Configuration) {
                const TConfigurationHistory* transition = nullptr;
                if (Configuration->Latest.IsJoint()) transition = &Configuration->Latest;
                else if (Configuration->Committed.IsJoint()) transition = &Configuration->Committed;
                if (transition) {
                    previousVoters = &transition->OldVoters();
                    transitionJointIndex = transition->ConfigurationIndex();
                } else if (Configuration->Committed.PreviousJointIndex()) {
                    previousVoters = &Configuration->Committed.PreviousVoters();
                    transitionJointIndex = Configuration->Committed.PreviousJointIndex();
                }
            }
            // A new-side-only leader is unknown to peers that have not yet
            // learned the transition. Start replication at the retained joint
            // entry so its first AppendEntries can establish that membership.
            if (previousVoters && transitionJointIndex > State->SnapshotIndex &&
                !std::binary_search(previousVoters->begin(), previousVoters->end(), Id))
                value = std::min(value, transitionJointIndex);
            decltype(VolatileState->NextIndex) nextIndex;
            decltype(VolatileState->RpcDue) rpcDue;
            for (auto [id, _] : Nodes) {
                nextIndex.emplace(id, value);
                rpcDue.emplace(id, ITimeSource::Max);
            }

            auto nextVolatileState = std::make_unique<TVolatileState>(TVolatileState {
                .CommitIndex = VolatileState->CommitIndex,
                .LeaderId = Id,
                .NextIndex = nextIndex,
                .RpcDue = rpcDue,
                .ElectionDue = ITimeSource::Max,
            });

            VolatileState = std::move(nextVolatileState);
            StateName = EState::LEADER;

            // Add fake empty entry for faster commit
            //
            {
                auto empty = NewHoldedMessage<TLogEntry>();
                empty->Term = State->CurrentTerm;
                empty->Flags = TLogEntry::EStub;
                // A full log must not prevent election itself. The hard cap
                // may prevent this current-term entry; in that case proposals
                // also fail until a conflicting leader suffix replaces entries
                // or the cluster is otherwise recovered.
                if (FitsUncommittedLog(empty->Len, 1)) {
                    const auto bytes = empty->Len;
                    State->Append(std::move(empty));
                    RecordUncommittedLogEntry(State->LastLogIndex, bytes);
                }
            }
        }
    }

    switch (StateName) {
    case EState::FOLLOWER:
        FollowerTimeout(now); break;
    case EState::CANDIDATE:
        CandidateTimeout(now); break;
    case EState::LEADER:
        LeaderTimeout(now); break;
    default:
        throw std::logic_error("Unknown state");
    }
}

ITimeSource::Time TRaft::MakeElection(ITimeSource::Time now) {
    uint64_t delta = (uint64_t)((1.0 + (double)rand_(&Seed) / (double)UINT_MAX) * TTimeout::Election.count());
    return now + std::chrono::milliseconds(delta);
}

uint64_t TRaft::Append(TMessageHolder<TLogEntry> entry) {
    if (!IsVoter(Id)) throw std::logic_error("A learner cannot propose log entries");
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can propose log entries");
    if (State->LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
    if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY))
        throw TMessageError("Invalid proposed log entry");
    ValidateMessageTree(entry);
    if (entry->Flags & TLogEntry::EConfiguration)
        throw TMessageError("Configuration proposals are unsupported");
    EnsureProposalLogCapacity(entry->Len, 1);
    entry->Term = State->CurrentTerm;
    const auto bytes = entry->Len;
    State->Append(std::move(entry));
    RecordUncommittedLogEntry(State->LastLogIndex, bytes);
    return State->LastLogIndex;
}

void TRaft::RequireLiveConfigurationStorage() const {
    if (!State->SupportsVoterBinding() || !State->SupportsCommitIndexPersistence() ||
        InitialVoters.empty() || State->GetBoundVoters() != InitialVoters)
        throw std::logic_error("Live membership changes require persisted commit and voter metadata");
}

TJointConfigurationReadiness TRaft::CheckJointConfigurationReadiness(
        const std::vector<uint32_t>& newVoters) const {
    ValidateFixedVoters(newVoters);
    TJointConfigurationReadiness readiness;
    readiness.IsLeader = StateName == EState::LEADER;
    readiness.HasDurableMembershipStorage = State->SupportsVoterBinding() &&
        State->SupportsCommitIndexPersistence() && !InitialVoters.empty() &&
        State->GetBoundVoters() == InitialVoters;
    readiness.HasStableConfiguration = Configuration &&
        !Configuration->HasUncommittedConfiguration() && !Configuration->Latest.IsJoint() &&
        !Configuration->Committed.IsJoint();
    readiness.HasCommittedCurrentTermEntry = VolatileState->CommitIndex &&
        State->LogTerm(VolatileState->CommitIndex) == State->CurrentTerm;
    readiness.HasLogCapacity = State->LastLogIndex < uint64_t(INT64_MAX);

    if (readiness.HasStableConfiguration) {
        const auto& oldVoters = Configuration->Committed.OldVoters();
        readiness.ChangesVoterSet = newVoters != oldVoters;
        if (readiness.HasLogCapacity && readiness.ChangesVoterSet) {
            const auto& latest = Configuration->Latest;
            auto proposed = EncodeConfigurationEntry(TConfigurationRecord{
                EConfigurationPhase::Joint, State->CurrentTerm,
                latest.ConfigurationIndex(), latest.ConfigurationTerm(), oldVoters, newVoters});
            readiness.HasLogCapacity = FitsProposalLog(proposed->Len, 1);
        }
        if (readiness.IsLeader) {
            for (auto id : newVoters) {
                if (!std::binary_search(oldVoters.begin(), oldVoters.end(), id) &&
                    !IsLearnerCommitAcknowledged(id))
                    readiness.AddedVotersWithoutCommitAck.push_back(id);
            }
        }
    }

    std::vector<uint32_t> requiredVoters = Voters;
    requiredVoters.insert(requiredVoters.end(), newVoters.begin(), newVoters.end());
    std::sort(requiredVoters.begin(), requiredVoters.end());
    requiredVoters.erase(std::unique(requiredVoters.begin(), requiredVoters.end()), requiredVoters.end());
    for (auto id : requiredVoters)
        if (id != Id && !Nodes.contains(id)) readiness.MissingPeerTargets.push_back(id);
    return readiness;
}

uint64_t TRaft::ProposeJointConfiguration(std::vector<uint32_t> newVoters) {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can change membership");
    RequireLiveConfigurationStorage();
    ValidateFixedVoters(newVoters);
    if (!Configuration || Configuration->HasUncommittedConfiguration() || Configuration->Latest.IsJoint())
        throw std::logic_error("A stable committed configuration is required to start a transition");
    if (State->LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
    if (!VolatileState->CommitIndex || State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm)
        throw std::logic_error("A current-term entry must be committed before changing membership");

    const auto& oldVoters = Configuration->Committed.OldVoters();
    const auto readiness = CheckJointConfigurationReadiness(newVoters);
    if (!readiness.ChangesVoterSet)
        throw std::invalid_argument("The new voter set must change membership");
    if (!readiness.MissingPeerTargets.empty())
        throw std::invalid_argument("Every active and proposed voter must have a configured replication target");
    if (!readiness.AddedVotersWithoutCommitAck.empty())
        throw std::logic_error("Every added voter must be a staged learner with a committed catch-up ACK");

    const auto& latest = Configuration->Latest;
    TConfigurationRecord record{EConfigurationPhase::Joint, State->CurrentTerm,
        latest.ConfigurationIndex(), latest.ConfigurationTerm(), oldVoters, std::move(newVoters)};
    auto entry = EncodeConfigurationEntry(record);
    EnsureProposalLogCapacity(entry->Len, 1);
    const auto bytes = entry->Len;
    State->Append(std::move(entry));
    RecordUncommittedLogEntry(State->LastLogIndex, bytes);
    RefreshConfiguration(VolatileState->CommitIndex);
    return State->LastLogIndex;
}

uint64_t TRaft::FinalizeConfiguration() {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can change membership");
    RequireLiveConfigurationStorage();
    if (!Configuration || Configuration->HasUncommittedConfiguration() || !Configuration->Committed.IsJoint())
        throw std::logic_error("The joint configuration must commit before finalization");
    if (State->LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
    if (!VolatileState->CommitIndex || State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm)
        throw std::logic_error("A current-term entry must be committed before finalizing membership");

    const auto& joint = Configuration->Committed;
    if (!std::binary_search(joint.NewVoters().begin(), joint.NewVoters().end(), Id))
        throw std::logic_error("Leadership must transfer to a voter in the new set before finalization");
    TConfigurationRecord record{EConfigurationPhase::Stable, State->CurrentTerm,
        joint.ConfigurationIndex(), joint.ConfigurationTerm(), joint.OldVoters(), joint.NewVoters()};
    auto entry = EncodeConfigurationEntry(record);
    EnsureProposalLogCapacity(entry->Len, 1);
    const auto bytes = entry->Len;
    State->Append(std::move(entry));
    RecordUncommittedLogEntry(State->LastLogIndex, bytes);
    RefreshConfiguration(VolatileState->CommitIndex);
    return State->LastLogIndex;
}

void TRaft::TransferLeadership(uint32_t target, ITimeSource::Time now) {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can transfer leadership");
    RequireLiveConfigurationStorage();
    if (State->CurrentTerm == UINT64_MAX) throw std::overflow_error("Raft term exhausted");
    const bool pendingStableSuccessor = Configuration && HasPendingStableSuccessor(*Configuration);
    if (!Configuration || (Configuration->HasUncommittedConfiguration() && !pendingStableSuccessor) ||
        !Configuration->Committed.IsJoint())
        throw std::logic_error("Leadership transfer requires a committed joint configuration");
    const auto& joint = Configuration->Committed;
    if (std::binary_search(joint.NewVoters().begin(), joint.NewVoters().end(), Id) ||
        !std::binary_search(joint.NewVoters().begin(), joint.NewVoters().end(), target) ||
        target == Id || !Nodes.contains(target))
        throw std::invalid_argument("Transfer target must be a different voter in the joint configuration's new set");
    if (!VolatileState->CommitIndex || (!pendingStableSuccessor &&
        State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm))
        throw std::logic_error("A current-term entry must be committed before transferring leadership");
    const auto match = VolatileState->MatchIndex.find(target);
    const auto commitAck = VolatileState->CommitAckIndex.find(target);
    if (match == VolatileState->MatchIndex.end() || match->second < State->LastLogIndex ||
        commitAck == VolatileState->CommitAckIndex.end() || commitAck->second < VolatileState->CommitIndex)
        throw std::logic_error("Transfer target must acknowledge the leader's complete committed log");

    auto request = NewHoldedMessage(
        TMessageEx{.Src = Id, .Dst = target, .Term = State->CurrentTerm},
        TTimeoutNowRequest{
            .LeaderId = Id, .TargetId = target,
            .LastLogIndex = State->LastLogIndex, .LastLogTerm = State->LastLogTerm,
            .CommitIndex = VolatileState->CommitIndex, .CommitTerm = State->LogTerm(VolatileState->CommitIndex),
            .ConfigurationIndex = joint.ConfigurationIndex(), .ConfigurationTerm = joint.ConfigurationTerm(),
        });

    // Stop issuing writes before asking the target to campaign. Retry while
    // this node remains a follower; the ordinary election timeout is fallback.
    Become(EState::FOLLOWER);
    VolatileState->LeaderId = 0;
    VolatileState->ElectionDue = MakeElection(now);
    VolatileState->LeadershipTransfer = TVolatileState::TLeadershipTransfer{
        .TargetId = target,
        .LastLogIndex = State->LastLogIndex,
        .LastLogTerm = State->LastLogTerm,
        .CommitIndex = VolatileState->CommitIndex,
        .CommitTerm = State->LogTerm(VolatileState->CommitIndex),
        .ConfigurationIndex = joint.ConfigurationIndex(),
        .ConfigurationTerm = joint.ConfigurationTerm(),
        .RetryDue = now + TTimeout::LeadershipTransferRetry,
    };
    VolatileState->SnapshotRpcs.clear();
    VolatileState->PendingAppendRpcs.clear();
    VolatileState->ConfigurationBarrierRpcs.clear();
    IncomingSnapshot.reset();
    EncodedSnapshot.reset();
    EncodedSnapshotIndex = 0;
    (void)Nodes.at(target)->TrySend(std::move(request));
}

void TRaft::OnTimeoutNow(ITimeSource::Time now, TMessageHolder<TTimeoutNowRequest>) {
    // ValidateRpc has established that the recognized old leader is excluded
    // from the committed new set and that this node has the exact transfer
    // boundary. Reuse the normal election path so term persistence, vote
    // clearing and joint-quorum arithmetic stay identical to timeout elections.
    VolatileState->ElectionDue = now;
    ProcessTimeout(now);
}

void TRaft::OnConfigurationBarrier(TMessageHolder<TConfigurationBarrierRequest> request) {
    const bool accepted = StateName == EState::FOLLOWER && IsVoter(request->LeaderId) &&
        VolatileState->LeaderId == request->LeaderId && Configuration &&
        !Configuration->HasUncommittedConfiguration() &&
        Configuration->Committed.ConfigurationIndex() == request->ConfigurationIndex &&
        Configuration->Committed.ConfigurationTerm() == request->ConfigurationTerm;
    if (accepted) {
        VolatileState->CompactionSafeConfigurationIndex = std::max(
            VolatileState->CompactionSafeConfigurationIndex, request->ConfigurationIndex);
    }
    auto reply = NewHoldedMessage(
        TMessageEx{.Src = Id, .Dst = request->Src, .Term = State->CurrentTerm, .Seqno = request->Seqno},
        TConfigurationBarrierResponse{
            .ConfigurationIndex = request->ConfigurationIndex,
            .ConfigurationTerm = request->ConfigurationTerm,
            .Accepted = uint32_t(accepted),
        });
    (void)Nodes.at(reply->Dst)->TrySend(std::move(reply));
}

void TRaft::OnConfigurationBarrier(ITimeSource::Time now,
        TMessageHolder<TConfigurationBarrierResponse> response) {
    auto found = VolatileState->ConfigurationBarrierRpcs.find(response->Src);
    if (found == VolatileState->ConfigurationBarrierRpcs.end()) return;
    const auto pending = found->second;
    if (pending.Seqno != response->Seqno ||
        pending.ConfigurationIndex != response->ConfigurationIndex ||
        pending.ConfigurationTerm != response->ConfigurationTerm) return;
    VolatileState->ConfigurationBarrierRpcs.erase(found);
    if (response->Accepted) {
        VolatileState->ConfigurationBarrierAckIndex[response->Src] = std::max(
            VolatileState->ConfigurationBarrierAckIndex[response->Src], response->ConfigurationIndex);
        VolatileState->ConfigurationBarrierAckDue[response->Src] =
            now + TTimeout::ConfigurationBarrierRefresh;
    } else {
        VolatileState->ConfigurationBarrierAckIndex.erase(response->Src);
        VolatileState->ConfigurationBarrierAckDue.erase(response->Src);
    }
}

void TRaft::MaybeDistributeConfigurationBarrier(ITimeSource::Time now) {
    if (StateName != EState::LEADER || !Configuration ||
        Configuration->HasUncommittedConfiguration()) return;
    const auto index = Configuration->Committed.ConfigurationIndex();
    const auto term = Configuration->Committed.ConfigurationTerm();
    if (!index || !term) return;
    for (auto voter : Voters) {
        if (voter == Id) continue;
        const auto ack = VolatileState->CommitAckIndex.find(voter);
        if (ack == VolatileState->CommitAckIndex.end() || ack->second < index) return;
        const auto capability = VolatileState->PeerCapabilities.find(voter);
        const auto lastSent = VolatileState->LastSentSeqno.find(voter);
        if (capability == VolatileState->PeerCapabilities.end() ||
            lastSent == VolatileState->LastSentSeqno.end() ||
            !capability->second.ConfigurationBarrier ||
            capability->second.AppendAckSeqno != lastSent->second) return;
    }

    VolatileState->CompactionSafeConfigurationIndex = std::max(
        VolatileState->CompactionSafeConfigurationIndex, index);
    for (auto voter : Voters) {
        if (voter == Id || !Nodes.contains(voter)) continue;
        const auto acked = VolatileState->ConfigurationBarrierAckIndex.find(voter);
        const auto ackDue = VolatileState->ConfigurationBarrierAckDue.find(voter);
        if (acked != VolatileState->ConfigurationBarrierAckIndex.end() && acked->second >= index &&
            ackDue != VolatileState->ConfigurationBarrierAckDue.end() && now < ackDue->second) continue;
        const auto pending = VolatileState->ConfigurationBarrierRpcs.find(voter);
        if (pending != VolatileState->ConfigurationBarrierRpcs.end() &&
            pending->second.ConfigurationIndex == index && pending->second.ConfigurationTerm == term &&
            now < pending->second.Due) continue;

        const auto seqno = NextSeqno();
        auto request = NewHoldedMessage(
            TMessageEx{.Src = Id, .Dst = voter, .Term = State->CurrentTerm, .Seqno = seqno},
            TConfigurationBarrierRequest{
                .LeaderId = Id,
                .ConfigurationIndex = index,
                .ConfigurationTerm = term,
            });
        VolatileState->ConfigurationBarrierRpcs[voter] = {
            seqno, index, term, now + TTimeout::Rpc};
        (void)Nodes.at(voter)->TrySend(std::move(request));
    }
}

uint32_t TRaft::GetLeaderId() const {
    return VolatileState->LeaderId;
}

uint64_t TRaft::GetLastIndex() const {
    return State->LastLogIndex;
}

std::vector<uint32_t> TRaft::GetVoterIds() const { return Voters; }

TConfigurationScan TRaft::GetConfigurationState() const {
    if (!Configuration) throw std::logic_error("Missing validated configuration state");
    return *Configuration;
}

bool TRaft::IsVoter(uint32_t id) const { return std::binary_search(Voters.begin(), Voters.end(), id); }

void TRaft::AddLearnerTarget(uint32_t id, std::shared_ptr<INode> node) {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can stage a learner");
    if (!State->SupportsVoterBinding() || State->GetBoundVoters() != InitialVoters)
        throw std::logic_error("Learner staging requires a bound voter configuration");
    RegisterPeerTarget(id, std::move(node));
}

void TRaft::RegisterPeerTarget(uint32_t id, std::shared_ptr<INode> node) {
    if (!id || id == Id || !node || Nodes.contains(id) || IsVoter(id) ||
        std::binary_search(Learners.begin(), Learners.end(), id))
        throw std::invalid_argument("Invalid or duplicate peer target");
    if (Nodes.size() >= size_t(INT_MAX)) throw std::overflow_error("Too many replication targets");
    // Build all allocating changes before publishing any part of the target.
    auto nodes = Nodes;
    auto learners = Learners;
    auto nextIndex = VolatileState->NextIndex;
    nodes.emplace(id, std::move(node));
    learners.insert(std::lower_bound(learners.begin(), learners.end(), id), id);
    nextIndex.emplace(id, State->LastLogIndex + 1);
    Nodes.swap(nodes); Learners.swap(learners); VolatileState->NextIndex.swap(nextIndex);
    Npeers = int(Nodes.size());
}

std::shared_ptr<INode> TRaft::RemoveLearnerTarget(uint32_t id) {
    if (StateName != EState::LEADER) throw std::logic_error("Only a leader can remove a learner target");
    return UnregisterPeerTarget(id);
}

std::shared_ptr<INode> TRaft::UnregisterPeerTarget(uint32_t id) {
    auto learner = std::lower_bound(Learners.begin(), Learners.end(), id);
    auto peer = Nodes.find(id);
    if (id == Id || learner == Learners.end() || *learner != id || peer == Nodes.end() || IsVoter(id))
        throw std::invalid_argument("Unknown or active-voter peer target");
    auto node = peer->second;
    Nodes.erase(peer); Learners.erase(learner);
    VolatileState->NextIndex.erase(id); VolatileState->MatchIndex.erase(id);
    VolatileState->CommitAckIndex.erase(id);
    VolatileState->ReadAckSeqno.erase(id); VolatileState->LastSentSeqno.erase(id);
    VolatileState->PendingAppendRpcs.erase(id); VolatileState->SnapshotRpcs.erase(id);
    VolatileState->ConfigurationBarrierRpcs.erase(id);
    VolatileState->ConfigurationBarrierAckIndex.erase(id);
    VolatileState->ConfigurationBarrierAckDue.erase(id);
    VolatileState->PeerCapabilities.erase(id);
    VolatileState->HeartbeatDue.erase(id); VolatileState->RpcDue.erase(id);
    VolatileState->BatchSize.erase(id); VolatileState->BackOff.erase(id);
    VolatileState->Votes.erase(id);
    Npeers = int(Nodes.size());
    return node;
}

bool TRaft::IsLearnerCaughtUp(uint32_t id) const {
    if (StateName != EState::LEADER || !std::binary_search(Learners.begin(), Learners.end(), id) ||
        VolatileState->CommitIndex == 0 || State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm) return false;
    auto match = VolatileState->MatchIndex.find(id);
    return match != VolatileState->MatchIndex.end() && match->second >= State->LastLogIndex;
}

bool TRaft::IsLearnerCommitAcknowledged(uint32_t id) const {
    if (StateName != EState::LEADER || !std::binary_search(Learners.begin(), Learners.end(), id) ||
        VolatileState->CommitIndex == 0 || State->LogTerm(VolatileState->CommitIndex) != State->CurrentTerm)
        return false;
    const auto match = VolatileState->MatchIndex.find(id);
    const auto commitAck = VolatileState->CommitAckIndex.find(id);
    return match != VolatileState->MatchIndex.end() && match->second >= State->LastLogIndex &&
        commitAck != VolatileState->CommitAckIndex.end() && commitAck->second >= VolatileState->CommitIndex;
}

TSnapshot TRaft::Compact() {
    if (!State->SupportsSnapshots()) throw std::logic_error("Storage cannot compact snapshots");
    if (Configuration && Configuration->Committed.ConfigurationIndex()) {
        const auto configurationIndex = Configuration->Committed.ConfigurationIndex();
        if (IsVoter(Id) && StateName == EState::LEADER) {
            for (auto voter : Voters) {
                if (voter == Id) continue;
                const auto ack = VolatileState->CommitAckIndex.find(voter);
                if (ack == VolatileState->CommitAckIndex.end() || ack->second < configurationIndex)
                    throw std::logic_error("Compaction requires every active voter to acknowledge the committed configuration");
            }
        } else if (IsVoter(Id) && VolatileState->CompactionSafeConfigurationIndex < configurationIndex) {
            throw std::logic_error("Compaction requires the leader's all-voter configuration acknowledgement barrier");
        }
    }
    auto application = SnapshotApplication.lock();
    if (!application) throw std::logic_error("Compaction requires a bound application");
    if (application->LastAppliedIndex == State->SnapshotIndex && State->GetSnapshot()) return *State->GetSnapshot();
    std::unordered_set<uint64_t> pinnedGenerations;
    for (const auto& [_, rpc] : VolatileState->SnapshotRpcs)
        if (rpc.Snapshot) pinnedGenerations.insert(rpc.Snapshot->Index);
    if (pinnedGenerations.size() >= TVolatileState::MaxPinnedSnapshotGenerations)
        throw std::runtime_error("Compaction deferred while snapshot transfers pin multiple generations");
    auto snapshot = CaptureSnapshot(*this, *application);
    application->ValidateSnapshot(snapshot.Data, snapshot.Index);
    AdvanceCommitIndex(VolatileState->CommitIndex);
    State->InstallSnapshot(snapshot);
    RebuildUncommittedLogUsage();
    ConfigurationBaseline = snapshot.StableConfiguration
        ? TConfigurationHistory::FromStableSnapshot(snapshot)
        : snapshot.IsJoint()
            ? TConfigurationHistory::FromJointSnapshot(snapshot)
            : TConfigurationHistory::Stable(Voters, 0, 0, snapshot.Index, snapshot.Term);
    RefreshConfiguration(VolatileState->CommitIndex);
    // Outstanding transfers own their immutable encoded bytes; discard any
    // otherwise-retained encoding of the old local snapshot.
    EncodedSnapshot.reset();
    EncodedSnapshotIndex = 0;
    return snapshot;
}

TMessageHolder<TInstallSnapshotRequest> TRaft::CreateInstallSnapshot(uint32_t nodeId) {
    if (auto capability = VolatileState->PeerCapabilities.find(nodeId);
        capability != VolatileState->PeerCapabilities.end())
        capability->second.AppendAckSeqno = 0; // Require a fresh AppendEntries capability ACK afterward.
    auto& rpc = VolatileState->SnapshotRpcs[nodeId];
    if (!rpc.Snapshot) {
        auto snapshot = State->GetSnapshot();
        if (!snapshot) throw std::logic_error("Missing snapshot for compacted log");
        if (EncodedSnapshotIndex != snapshot->Index) {
            EncodedSnapshot = std::make_shared<const std::string>(EncodeSnapshot(*snapshot));
            EncodedSnapshotIndex = snapshot->Index;
        }
        rpc = TVolatileState::TSnapshotRpc{snapshot, EncodedSnapshot};
        VolatileState->PendingAppendRpcs[nodeId].clear();
    }
    // Pin one immutable image until this peer finishes. Periodic compaction
    // must not restart every chunk; after completion a still-lagging peer can
    // start a separate transfer of the newest image.
    const auto& snapshot = rpc.Snapshot;
    const auto count = std::min<uint64_t>(TInstallSnapshotRequest::MaxChunkBytes, rpc.Bytes->size() - rpc.Offset);
    auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
    request->Src = Id; request->Dst = nodeId; request->Term = State->CurrentTerm;
    // A retry sends the same immutable chunk. Keep its identity until a reply
    // consumes it, otherwise traffic faster than the RTT invalidates every ACK.
    // Snapshot ACKs never confirm read barriers, so reuse cannot confirm a read
    // that arrived after the first transmission.
    if (!rpc.Seqno) rpc.Seqno = NextSeqno();
    request->Seqno = rpc.Seqno;
    request->SnapshotIndex = snapshot->Index; request->SnapshotTerm = snapshot->Term;
    request->Offset = rpc.Offset; request->TotalBytes = rpc.Bytes->size();
    rpc.SentEnd = rpc.Offset + count;
    request->Done = rpc.SentEnd == rpc.Bytes->size(); request->LeaderId = Id;
    memcpy(request->Data, rpc.Bytes->data() + rpc.Offset, count);
    return request;
}

void TRaft::OnInstallSnapshot(TMessageHolder<TInstallSnapshotResponse> response) {
    if (response->Term != State->CurrentTerm) return;
    auto found = VolatileState->SnapshotRpcs.find(response->Src);
    if (found == VolatileState->SnapshotRpcs.end()) return;
    auto& rpc = found->second;
    if (!rpc.Seqno || response->Seqno != rpc.Seqno || response->SnapshotIndex != rpc.Snapshot->Index) return;
    if (!response->Success) {
        rpc.Offset = 0;
        rpc.Seqno = 0;
    } else if (response->Installed) {
        if (response->NextOffset != rpc.Bytes->size()) return;
        const auto index = std::max(VolatileState->MatchIndex[response->Src], response->SnapshotIndex);
        (*VolatileState).SetMatchIndex(response->Src, index).SetNextIndex(response->Src, index + 1)
            .SetBatchSize(response->Src, 1024).SetBackOff(response->Src, 1);
        VolatileState->CommitAckIndex[response->Src] = std::max(
            VolatileState->CommitAckIndex[response->Src],
            std::min(VolatileState->CommitIndex, response->SnapshotIndex));
        AdvanceLeaderCommit();
        VolatileState->SnapshotRpcs.erase(found);
        // Snapshot completion is not a fresh-read acknowledgement. AppendEntries
        // must still establish the next read barrier and replicate the suffix.
    } else {
        if (response->NextOffset != rpc.SentEnd || response->NextOffset >= rpc.Bytes->size()) return;
        rpc.Offset = response->NextOffset;
        rpc.Seqno = 0; // Next chunk gets a new identity; old replies stay stale.
    }
    VolatileState->RpcDue[response->Src] = ITimeSource::Time{};
}

void TRaft::OnInstallSnapshot(ITimeSource::Time now, TMessageHolder<TInstallSnapshotRequest> request) {
    auto reply = [&](bool success, bool installed, uint64_t offset) {
        (void)Nodes.at(request->Src)->TrySend(NewHoldedMessage(
            TMessageEx{.Src = Id, .Dst = request->Src, .Term = State->CurrentTerm, .Seqno = request->Seqno},
            TInstallSnapshotResponse{.SnapshotIndex = request->SnapshotIndex, .NextOffset = offset,
                .Success = uint32_t(success), .Installed = uint32_t(installed)}));
    };
    if (request->Term < State->CurrentTerm) { reply(false, false, 0); return; }
    // Partial or unsupported image headers are not evidence of leader contact.
    // In particular, do not demote a candidate or refresh its election timer
    // until the complete bounded metadata prefix is supported.
    auto acceptLeaderContact = [&] {
        Become(EState::FOLLOWER);
        VolatileState->LeadershipTransfer.reset();
        VolatileState->LeaderId = request->LeaderId;
        VolatileState->ElectionDue = MakeElection(now);
    };
    if (request->SnapshotIndex <= VolatileState->CommitIndex) {
        const auto term = State->LogTerm(request->SnapshotIndex);
        const bool covered = request->SnapshotIndex < State->SnapshotIndex || term == request->SnapshotTerm;
        if (!covered) { reply(false, false, 0); return; }
        // A covered image needs no application replacement, but its snapshot
        // boundary and fixed voter set must match before it can count as
        // replicated. Accumulate only the bounded metadata prefix.
        const bool same = IncomingSnapshot && IncomingSnapshot->Term == request->Term &&
            IncomingSnapshot->Leader == request->LeaderId && IncomingSnapshot->Index == request->SnapshotIndex &&
            IncomingSnapshot->BoundaryTerm == request->SnapshotTerm && IncomingSnapshot->Total == request->TotalBytes;
        if (!same) {
            if (request->Offset != 0 || (IncomingSnapshot && IncomingSnapshot->Term == request->Term &&
                IncomingSnapshot->Index > request->SnapshotIndex)) { reply(false, false, 0); return; }
            IncomingSnapshot = TIncomingSnapshot{request->Term, request->SnapshotIndex, request->SnapshotTerm,
                request->TotalBytes, request->LeaderId, now, {}};
        }
        auto& prefix = IncomingSnapshot->Bytes;
        const auto count = request->Len - sizeof(TInstallSnapshotRequest);
        if (request->Offset > prefix.size()) { reply(false, false, 0); return; }
        if (request->Offset < prefix.size()) {
            if (count > prefix.size() - request->Offset ||
                std::string_view(prefix).substr(request->Offset, count) != std::string_view(request->Data, count)) {
                IncomingSnapshot.reset(); reply(false, false, 0); return;
            }
        } else prefix.append(request->Data, count);
        IncomingSnapshot->LastProgress = now;
        if (!IsSupportedSnapshotMagicPrefix(prefix)) {
            IncomingSnapshot.reset(); reply(false, false, 0); return;
        }
        const auto knownSnapshot = State->GetSnapshot();
        const auto* committedConfiguration = Configuration ? &Configuration->Committed : nullptr;
        const auto* pendingConfiguration = Configuration && Configuration->HasUncommittedConfiguration()
            ? &Configuration->Latest : nullptr;
        const auto snapshotVoters = committedConfiguration
            ? ConfigurationVoterUnion(*committedConfiguration) : std::vector<uint32_t>{};
        const auto probe = ProbeSnapshotMetadata(prefix, request->TotalBytes,
            request->SnapshotIndex, request->SnapshotTerm, snapshotVoters, knownSnapshot.get(),
            committedConfiguration, pendingConfiguration,
            false, HasPersistedRecoveryState(*State), request->LeaderId, Id);
        if (!probe.Complete) { reply(true, false, prefix.size()); return; }
        IncomingSnapshot.reset();
        if (probe.Supported) acceptLeaderContact();
        reply(probe.Supported, probe.Supported, probe.Supported ? request->TotalBytes : 0);
        return;
    }
    auto application = SnapshotApplication.lock();
    if (!State->SupportsSnapshots() || !application) { reply(false, false, 0); return; }
    const bool same = IncomingSnapshot && IncomingSnapshot->Term == request->Term &&
        IncomingSnapshot->Leader == request->LeaderId && IncomingSnapshot->Index == request->SnapshotIndex &&
        IncomingSnapshot->BoundaryTerm == request->SnapshotTerm && IncomingSnapshot->Total == request->TotalBytes;
    if (!same) {
        if (request->Offset != 0 || (IncomingSnapshot && IncomingSnapshot->Term == request->Term &&
            IncomingSnapshot->Index > request->SnapshotIndex)) { reply(false, false, 0); return; }
        IncomingSnapshot = TIncomingSnapshot{request->Term, request->SnapshotIndex, request->SnapshotTerm,
            request->TotalBytes, request->LeaderId, now, {}};
    }
    auto& bytes = IncomingSnapshot->Bytes;
    const auto count = request->Len - sizeof(TInstallSnapshotRequest);
    if (request->Offset > bytes.size()) { reply(false, false, 0); return; }
    if (request->Offset < bytes.size()) {
        if (count > bytes.size() - request->Offset ||
            std::string_view(bytes).substr(request->Offset, count) != std::string_view(request->Data, count)) {
            IncomingSnapshot.reset(); reply(false, false, 0); return;
        }
    } else bytes.append(request->Data, count);
    IncomingSnapshot->LastProgress = now;
    if (!IsSupportedSnapshotMagicPrefix(bytes)) {
        IncomingSnapshot.reset(); reply(false, false, 0); return;
    }
    const auto knownSnapshot = State->GetSnapshot();
    const auto* committedConfiguration = Configuration ? &Configuration->Committed : nullptr;
    const auto* pendingConfiguration = Configuration && Configuration->HasUncommittedConfiguration()
        ? &Configuration->Latest : nullptr;
    const auto snapshotVoters = committedConfiguration
        ? ConfigurationVoterUnion(*committedConfiguration) : std::vector<uint32_t>{};
    const auto probe = ProbeSnapshotMetadata(bytes, request->TotalBytes,
        request->SnapshotIndex, request->SnapshotTerm, snapshotVoters, knownSnapshot.get(),
        committedConfiguration, pendingConfiguration,
        CanBootstrapFreshLearnerSnapshot(Id, *State, GetVoterIds(), application.get(),
            committedConfiguration, !pendingConfiguration),
        HasPersistedRecoveryState(*State),
        request->LeaderId, Id);
    if (!probe.Complete) { reply(true, false, bytes.size()); return; }
    if (!probe.Supported) { IncomingSnapshot.reset(); reply(false, false, 0); return; }
    acceptLeaderContact();
    if (bytes.size() < request->TotalBytes) { reply(true, false, bytes.size()); return; }
    TSnapshot snapshot;
    try {
        snapshot = DecodeSnapshot(bytes);
        auto inheritTrustedLineage = [&](const TConfigurationHistory& history) {
            if (snapshot.HasConfigurationMetadata() && history.IsJoint() &&
                !history.PreviousVoters().empty() &&
                snapshot.StableConfiguration && !snapshot.IsJoint() &&
                snapshot.Voters == history.NewVoters() &&
                snapshot.ConfigurationIndex > history.ConfigurationIndex() &&
                snapshot.ConfigurationTerm >= history.ConfigurationTerm()) {
                if (snapshot.HasConfigurationLineage()) {
                    if (snapshot.PreviousVoters != history.OldVoters() ||
                        snapshot.PreviousConfigurationIndex != history.PreviousConfigurationIndex() ||
                        snapshot.PreviousConfigurationTerm != history.PreviousConfigurationTerm() ||
                        snapshot.PreviousJointIndex != history.ConfigurationIndex() ||
                        snapshot.PreviousJointTerm != history.ConfigurationTerm())
                        return;
                } else {
                    snapshot.PreviousVoters = history.OldVoters();
                    snapshot.PreviousConfigurationIndex = history.PreviousConfigurationIndex();
                    snapshot.PreviousConfigurationTerm = history.PreviousConfigurationTerm();
                    snapshot.PreviousJointIndex = history.ConfigurationIndex();
                    snapshot.PreviousJointTerm = history.ConfigurationTerm();
                }
                const auto& trustedTransitions = history.ConfigurationTransitions();
                if (!trustedTransitions.empty()) {
                    auto transitions = trustedTransitions;
                    auto& tail = transitions.back();
                    if (!tail.StableIndex && tail.JointIndex == history.ConfigurationIndex() &&
                        tail.JointTerm == history.ConfigurationTerm() && tail.OldVoters == history.OldVoters() &&
                        tail.NewVoters == history.NewVoters()) {
                        tail.StableIndex = snapshot.ConfigurationIndex;
                        tail.StableTerm = snapshot.ConfigurationTerm;
                        if (SnapshotHistoryCompletesKnownJoint(
                                snapshot.ConfigurationTransitions, trustedTransitions))
                            snapshot.ConfigurationTransitions = std::move(transitions);
                    }
                }
                return;
            }
            if (!snapshot.HasConfigurationMetadata() || history.PreviousVoters().empty() ||
                snapshot.IsJoint() != history.IsJoint() ||
                snapshot.Voters != history.OldVoters() ||
                snapshot.ConfigurationIndex != history.ConfigurationIndex() ||
                snapshot.ConfigurationTerm != history.ConfigurationTerm() ||
                (snapshot.IsJoint() && snapshot.JointVoters != history.NewVoters()))
                return;
            if (snapshot.HasConfigurationLineage()) {
                if (snapshot.PreviousVoters != history.PreviousVoters() ||
                    snapshot.PreviousConfigurationIndex != history.PreviousConfigurationIndex() ||
                    snapshot.PreviousConfigurationTerm != history.PreviousConfigurationTerm() ||
                    snapshot.PreviousJointIndex != history.PreviousJointIndex() ||
                    snapshot.PreviousJointTerm != history.PreviousJointTerm())
                    return;
            } else {
                snapshot.PreviousVoters = history.PreviousVoters();
                snapshot.PreviousConfigurationIndex = history.PreviousConfigurationIndex();
                snapshot.PreviousConfigurationTerm = history.PreviousConfigurationTerm();
                snapshot.PreviousJointIndex = history.PreviousJointIndex();
                snapshot.PreviousJointTerm = history.PreviousJointTerm();
            }
            const auto& trustedTransitions = history.ConfigurationTransitions();
            if (SnapshotHistorySharesKnownTail(snapshot.ConfigurationTransitions, trustedTransitions))
                snapshot.ConfigurationTransitions = trustedTransitions;
            else
                snapshot.ConfigurationTransitions.clear();
        };
        if (pendingConfiguration) inheritTrustedLineage(*pendingConfiguration);
        inheritTrustedLineage(Configuration->Committed);
        const bool freshLearnerBootstrap = probe.FreshLearnerBootstrap &&
            CanBootstrapFreshLearnerSnapshot(Id, *State, GetVoterIds(), application.get(),
                committedConfiguration, !pendingConfiguration);
        const bool pendingConfigurationSnapshot = pendingConfiguration &&
            SnapshotMatchesPendingConfiguration(snapshot, Configuration->Committed,
                *pendingConfiguration, request->LeaderId);
        const bool committedJointStableSuccessor = !pendingConfiguration &&
            SnapshotMetadataCompletesKnownJoint(knownSnapshot.get(), &Configuration->Committed,
                snapshot.StableConfiguration, snapshot.HasStablePredecessor(),
                snapshot.Voters, snapshot.JointVoters, snapshot.PreviousVoters,
                snapshot.PreviousConfigurationIndex, snapshot.PreviousConfigurationTerm,
                snapshot.PreviousJointIndex, snapshot.PreviousJointTerm,
                snapshot.ConfigurationIndex, snapshot.ConfigurationTerm, request->LeaderId) &&
            SnapshotHistoryCompletesKnownJoint(snapshot.ConfigurationTransitions,
                Configuration->Committed.ConfigurationTransitions());
        size_t staleStableHistoryAnchor = 0;
        const bool staleStableLineage = !pendingConfiguration &&
            (snapshot.ConfigurationTransitions.empty() && SnapshotMetadataExtendsKnownStable(&Configuration->Committed,
                snapshot.HasStablePredecessor(), HasPersistedRecoveryState(*State),
                snapshot.Voters, snapshot.PreviousVoters,
                snapshot.PreviousConfigurationIndex, snapshot.PreviousConfigurationTerm,
                snapshot.PreviousJointIndex, snapshot.PreviousJointTerm,
                snapshot.ConfigurationIndex, snapshot.ConfigurationTerm, request->LeaderId) ||
             SnapshotHistoryExtendsKnownStable(&Configuration->Committed,
                HasPersistedRecoveryState(*State), snapshot.ConfigurationTransitions,
                snapshot.Voters, snapshot.ConfigurationIndex, snapshot.ConfigurationTerm,
                request->LeaderId, &staleStableHistoryAnchor));
        size_t staleJointHistoryAnchor = 0;
        const bool staleJointLineage = !pendingConfiguration && snapshot.IsJoint() &&
            SnapshotHistoryExtendsKnownStableToJoint(&Configuration->Committed,
                HasPersistedRecoveryState(*State), snapshot.ConfigurationTransitions,
                snapshot.Voters, snapshot.JointVoters, snapshot.ConfigurationIndex,
                snapshot.ConfigurationTerm, request->LeaderId, &staleJointHistoryAnchor);
        if (staleStableLineage && snapshot.ConfigurationTransitions.empty() &&
            snapshot.HasStablePredecessor()) {
            const std::vector<TSnapshotConfigurationTransition> successor{{
                snapshot.PreviousVoters, snapshot.Voters,
                snapshot.PreviousConfigurationIndex, snapshot.PreviousConfigurationTerm,
                snapshot.PreviousJointIndex, snapshot.PreviousJointTerm,
                snapshot.ConfigurationIndex, snapshot.ConfigurationTerm}};
            snapshot.ConfigurationTransitions = ExtendTrustedTransitionHistory(
                Configuration->Committed.ConfigurationTransitions(), successor, 0);
        } else if (staleStableLineage && !snapshot.ConfigurationTransitions.empty()) {
            snapshot.ConfigurationTransitions = ExtendTrustedTransitionHistory(
                Configuration->Committed.ConfigurationTransitions(), snapshot.ConfigurationTransitions,
                staleStableHistoryAnchor);
        }
        if (staleJointLineage)
            snapshot.ConfigurationTransitions = ExtendTrustedTransitionHistory(
                Configuration->Committed.ConfigurationTransitions(), snapshot.ConfigurationTransitions,
                staleJointHistoryAnchor);
        const bool committedConfigurationSnapshot =
            SnapshotMatchesCommittedConfiguration(snapshot, Configuration->Committed);
        const bool freshSnapshotMatches = freshLearnerBootstrap &&
            SnapshotMatchesFreshLearnerBootstrap(snapshot, GetVoterIds(), Configuration->Committed,
                request->LeaderId, Id);
        if (snapshot.Index != request->SnapshotIndex || snapshot.Term != request->SnapshotTerm ||
            (!committedConfigurationSnapshot && !pendingConfigurationSnapshot &&
             !committedJointStableSuccessor && !staleStableLineage && !staleJointLineage &&
             !freshSnapshotMatches))
            throw TMessageError("Snapshot metadata mismatch");
        if (!snapshot.HasConfigurationMetadata() && Configuration->Committed.ConfigurationIndex()) {
            // Legacy snapshots have no configuration identity. The bounded
            // probe established that this image is at or beyond the receiver's
            // committed stable configuration and names its exact voter set.
            snapshot.StableConfiguration = true;
            snapshot.ConfigurationIndex = Configuration->Committed.ConfigurationIndex();
            snapshot.ConfigurationTerm = Configuration->Committed.ConfigurationTerm();
        }
        const bool trustedConfigurationLineage = snapshot.HasConfigurationLineage() &&
            (committedConfigurationSnapshot || pendingConfigurationSnapshot ||
             committedJointStableSuccessor || staleStableLineage || staleJointLineage || freshSnapshotMatches);
        if (snapshot.HasConfigurationLineage() && !trustedConfigurationLineage)
            throw TMessageError("Snapshot predecessor lineage is not locally validated");
        if (!trustedConfigurationLineage) PreserveStableSnapshotIdentity(knownSnapshot.get(), snapshot);
        ValidateSnapshot(snapshot);
        // RefreshConfiguration requires a local replication target for every
        // active voter. Check that before publishing either the Raft snapshot
        // or its application image; otherwise a valid stale-store recovery can
        // install successfully and then throw while activating its new voter set.
        for (auto voter : snapshot.Voters)
            if (voter != Id && !Nodes.contains(voter))
                throw TMessageError("Snapshot configuration is missing a voter peer target");
        for (auto voter : snapshot.JointVoters)
            if (voter != Id && !Nodes.contains(voter))
                throw TMessageError("Snapshot configuration is missing a voter peer target");
        application->ValidateSnapshot(snapshot.Data, snapshot.Index);
    } catch (const std::exception&) {
        IncomingSnapshot.reset(); reply(false, false, 0); return;
    }
    try {
        TRaftInstallSnapshotPermit permit;
        State->InstallRaftSnapshot(snapshot, permit); // publish Raft state before app replacement or ACK
        application->LoadSnapshot(snapshot.Data, snapshot.Index);
        if (application->LastAppliedIndex != snapshot.Index) std::terminate();
    } catch (...) { std::terminate(); }
    VolatileState->CommitIndex = State->CommitIndex;
    RebuildUncommittedLogUsage();
    ConfigurationBaseline = snapshot.StableConfiguration
        ? TConfigurationHistory::FromStableSnapshot(snapshot)
        : snapshot.IsJoint()
            ? TConfigurationHistory::FromJointSnapshot(snapshot)
            : TConfigurationHistory::Stable(snapshot.Voters, 0, 0, snapshot.Index, snapshot.Term);
    RefreshConfiguration(VolatileState->CommitIndex);
    IncomingSnapshot.reset();
    reply(true, true, request->TotalBytes);
}

void TRaft::RestoreAppliedIndex(uint64_t index) {
    if (index > uint64_t(INT64_MAX) || index > State->LastLogIndex)
        throw std::invalid_argument("Application progress exceeds recovered Raft log");
    // A correctly recovered application marker is evidence of commitment.
    // Protect its prefix immediately, before any AppendEntries can be handled.
    AdvanceCommitIndex(std::max(VolatileState->CommitIndex, index));
}

void TRaft::RebuildUncommittedLogUsage() {
    UncommittedLog.clear();
    UncommittedLogBytes = 0;
    const auto committed = std::max({State->CommitIndex, State->SnapshotIndex,
        VolatileState ? VolatileState->CommitIndex : uint64_t(0)});
    for (uint64_t index = committed + 1; index <= State->LastLogIndex; ++index) {
        auto entry = State->Get(static_cast<int64_t>(index));
        if (!entry) throw std::logic_error("Missing uncommitted Raft log entry");
        RecordUncommittedLogEntry(index, entry->Len);
    }
}

void TRaft::PruneCommittedLogUsage(uint64_t commitIndex) {
    while (!UncommittedLog.empty() && UncommittedLog.front().Index <= commitIndex) {
        UncommittedLogBytes -= UncommittedLog.front().Bytes;
        UncommittedLog.pop_front();
    }
}

void TRaft::RemoveUncommittedLogSuffix(uint64_t firstIndex) {
    while (!UncommittedLog.empty() && UncommittedLog.back().Index >= firstIndex) {
        UncommittedLogBytes -= UncommittedLog.back().Bytes;
        UncommittedLog.pop_back();
    }
}

void TRaft::RecordUncommittedLogEntry(uint64_t index, uint32_t bytes) {
    if ((!UncommittedLog.empty() && index != UncommittedLog.back().Index + 1) ||
        (UncommittedLog.empty() && index <= std::max(State->CommitIndex, State->SnapshotIndex)))
        throw std::logic_error("Noncontiguous uncommitted Raft log accounting");
    if (bytes > UINT64_MAX - UncommittedLogBytes)
        throw std::overflow_error("Uncommitted Raft log byte accounting overflow");
    UncommittedLog.push_back({index, bytes});
    UncommittedLogBytes += bytes;
}

bool TRaft::FitsUncommittedLog(uint64_t bytes, size_t entries) const {
    return UncommittedLogBytes <= LogLimits.MaxUncommittedBytes &&
        bytes <= LogLimits.MaxUncommittedBytes - UncommittedLogBytes &&
        UncommittedLog.size() <= LogLimits.MaxUncommittedEntries &&
        entries <= LogLimits.MaxUncommittedEntries - UncommittedLog.size();
}

bool TRaft::FitsProposalLog(uint64_t bytes, size_t entries) const {
    const auto proposalBytes = LogLimits.MaxUncommittedBytes - LogLimits.ReservedControlBytes;
    const auto proposalEntries = LogLimits.MaxUncommittedEntries - LogLimits.ReservedControlEntries;
    return UncommittedLogBytes <= proposalBytes && bytes <= proposalBytes - UncommittedLogBytes &&
        UncommittedLog.size() <= proposalEntries && entries <= proposalEntries - UncommittedLog.size();
}

void TRaft::EnsureProposalLogCapacity(uint64_t bytes, size_t entries) const {
    if (!FitsProposalLog(bytes, entries)) throw TRaftLogCapacityExceeded();
}

void TRaft::AdvanceCommitIndex(uint64_t index) {
    if (index > State->LastLogIndex)
        throw std::logic_error("Invalid Raft commit index advance");
    const auto target = std::max({index, VolatileState->CommitIndex, State->CommitIndex});
    if (target > State->LastLogIndex)
        throw std::logic_error("Invalid persisted Raft commit index");
    if (State->CommitIndex < target) {
        State->CommitIndex = target;
        State->Commit();
    }
    VolatileState->CommitIndex = target;
    PruneCommittedLogUsage(target);
    if (ConfigurationScanRequired) RefreshConfiguration(target);
}

void TRaft::AdvanceLeaderCommit() {
    const auto previous = VolatileState->CommitIndex;
    VolatileState->CommitAdvance(*VotingQuorum, Id, *State);
    const auto updated = VolatileState->CommitIndex;
    VolatileState->CommitIndex = previous;
    AdvanceCommitIndex(updated);
}

TRequestProcessor::TRequestProcessor(std::shared_ptr<TRaft> raft, std::shared_ptr<IRsm> rsm, const TNodeDict& nodes)
    : TRequestProcessor(std::move(raft), std::move(rsm), nodes, TAdmissionLimits{})
{ }

TRequestProcessor::TRequestProcessor(std::shared_ptr<TRaft> raft, std::shared_ptr<IRsm> rsm,
        const TNodeDict& nodes, TAdmissionLimits admissionLimits)
    : Raft(std::move(raft)), Rsm(std::move(rsm)), Nodes(nodes), AdmissionLimits(admissionLimits)
{
    if (!Raft || !Rsm) throw std::invalid_argument("Request processor requires Raft and application state");
    if (auto snapshot = Raft->GetState()->GetSnapshot(); snapshot && Rsm->LastAppliedIndex < snapshot->Index)
        RestoreSnapshot(*snapshot, *Raft, *Rsm);
    if (Rsm->LastAppliedIndex < Raft->GetState()->SnapshotIndex)
        throw std::invalid_argument("Application progress precedes compacted log");
    Raft->RestoreAppliedIndex(Rsm->LastAppliedIndex);
    Raft->SnapshotApplication = Rsm;
}

bool TRequestProcessor::TryAdmit(const TMessageHolder<TCommandRequest>& message,
        const std::shared_ptr<INode>& replyTo, uint64_t& admissionId) {
    admissionId = 0;
    if (!replyTo) return true;
    const uint64_t bytes = message->Len;
    if (Admissions.size() >= AdmissionLimits.MaxPendingRequests ||
        PendingRequestBytes > AdmissionLimits.MaxPendingRequestBytes ||
        bytes > AdmissionLimits.MaxPendingRequestBytes - PendingRequestBytes)
        return false;

    auto id = NextAdmissionId ? NextAdmissionId : 1;
    while (Admissions.contains(id)) {
        ++id;
        if (!id) id = 1;
    }
    const bool inserted = Admissions.emplace(id, bytes).second;
    if (!inserted) throw std::logic_error("Duplicate pending request admission ID");
    PendingRequestBytes += bytes;
    NextAdmissionId = id + 1;
    admissionId = id;
    return true;
}

void TRequestProcessor::ReleaseAdmission(uint64_t admissionId) noexcept {
    if (!admissionId) return;
    auto it = Admissions.find(admissionId);
    if (it == Admissions.end()) return;
    if (it->second > PendingRequestBytes) std::terminate();
    PendingRequestBytes -= it->second;
    Admissions.erase(it);
}

void TRequestProcessor::EraseWriteAnswer(uint64_t index) noexcept {
    auto it = WriteAnswers.find(index);
    if (it == WriteAnswers.end()) return;
    if (it->second.Reply->Len > PendingReplyBytes) std::terminate();
    PendingReplyBytes -= it->second.Reply->Len;
    WriteAnswers.erase(it);
}

void TRequestProcessor::DispatchCommandRequest(TMessageHolder<TCommandRequest> message,
        const std::shared_ptr<INode>& replyTo, uint64_t admissionId) {
    if (!(message->Flags & TCommandRequest::EWrite))
        return OnReadRequest(std::move(message), replyTo, admissionId);
    return OnWriteRequest(std::move(message), replyTo, admissionId);
}

void TRequestProcessor::CheckStateChange() {
    PruneRequests();
    if (WaitingStateChange.empty()) {
        return;
    }

    auto stateName = Raft->CurrentStateName();
    auto leaderId = Raft->GetLeaderId();

    if (stateName == EState::CANDIDATE || (stateName != EState::LEADER &&
        (leaderId == 0 || leaderId == Raft->GetId() || !Nodes.contains(leaderId)))) {
        return;
    }

    std::queue<TWaiting> apply;
    WaitingStateChange.swap(apply);
    while (!apply.empty()) {
        auto w = std::move(apply.front()); apply.pop();
        try {
            DispatchCommandRequest(std::move(w.Command), w.ReplyTo, w.AdmissionId);
        } catch (...) {
            ReleaseAdmission(w.AdmissionId);
            while (!apply.empty()) {
                ReleaseAdmission(apply.front().AdmissionId);
                apply.pop();
            }
            throw;
        }
    }
}

void TRequestProcessor::Forward(TMessageHolder<TCommandRequest> command,
        const std::shared_ptr<INode>& replyTo, uint64_t admissionId)
{
    if (!replyTo) {
        ReleaseAdmission(admissionId);
        return;
    }

    if (command->Flags & TCommandRequest::EForwarded) {
        // already forwarded
        ReleaseAdmission(admissionId);
        replyTo->Send(NewHoldedMessage(TCommandResponse{.Cookie = command->Cookie, .ErrorCode = TCommandResponse::Failed}));
        return;
    }

    auto stateName = Raft->CurrentStateName();
    auto leaderId = Raft->GetLeaderId();
    if (stateName == EState::CANDIDATE || leaderId == 0 ||
        leaderId == Raft->GetId() || !Nodes.contains(leaderId)) {
        WaitingStateChange.emplace(TWaiting{0, 0, std::move(command), replyTo, 0, admissionId});
        return;
    }

    if (stateName == EState::FOLLOWER) {
        if (NextForwardCookie > UINT32_MAX) {
            ReleaseAdmission(admissionId);
            replyTo->Send(NewHoldedMessage(TCommandResponse{.Cookie = command->Cookie,
                .ErrorCode = TCommandResponse::Failed}));
            return;
        }
        const auto cookie = uint32_t(NextForwardCookie++);
        auto forwarded = NewHoldedMessage<TCommandRequest>(command->Len);
        memcpy(forwarded.Mes, command.Mes, command->Len);
        forwarded->Cookie = cookie;
        forwarded->Flags |= TCommandRequest::EForwarded;
        auto peer = Nodes.at(leaderId);
        Forwarded.emplace(cookie, TForwarded{replyTo, peer, command->Cookie,
                                            leaderId, Raft->GetState()->CurrentTerm, admissionId});
        bool accepted = false;
        try {
            accepted = peer->TrySend(std::move(forwarded));
        } catch (...) {
            Forwarded.erase(cookie);
            throw;
        }
        if (!accepted) {
            Forwarded.erase(cookie);
            ReleaseAdmission(admissionId);
            replyTo->Send(NewHoldedMessage(TCommandResponse{.Cookie = command->Cookie,
                .ErrorCode = TCommandResponse::Overloaded}));
        }
        return;
    }

    assert(false && "Wrong state");
}

void TRequestProcessor::OnReadRequest(TMessageHolder<TCommandRequest> command,
        const std::shared_ptr<INode>& replyTo, uint64_t admissionId)
{
    auto stateName = Raft->CurrentStateName();
    auto flags = command->Flags;
    assert(!(flags & TCommandRequest::EWrite));

    // stale read, default read (from leader)
    if (!(flags & TCommandRequest::EConsistent) &&
        ((flags & TCommandRequest::EStale) || stateName == EState::LEADER)) {
        const bool stale = flags & TCommandRequest::EStale;
        const auto index = stale ? Raft->GetVolatileState()->CommitIndex : Raft->GetLastIndex();
        const auto term = stale ? 0 : Raft->GetState()->CurrentTerm;
        Waiting.emplace(TWaiting{index, 0, std::move(command), replyTo, term, admissionId});
        return;
    }

    if (stateName != EState::LEADER) {
        return Forward(std::move(command), replyTo, admissionId);
    }

    // Consistent read
    const auto term = Raft->GetState()->CurrentTerm;
    const auto index = Raft->GetLastIndex();
    auto seqno = Raft->ApproveRead();
    assert(StrongWaiting.empty() || (StrongWaiting.back().Index <= index && StrongWaiting.back().Seqno <= seqno));
    StrongWaiting.emplace(TWaiting{index, seqno, std::move(command), replyTo, term, admissionId});
}

void TRequestProcessor::PruneRequests(const std::shared_ptr<INode>& disconnected) {
    const auto term = Raft->GetState()->CurrentTerm;
    const auto state = Raft->CurrentStateName();
    const bool leader = state == EState::LEADER;
    const auto leaderId = Raft->GetLeaderId();
    if (!disconnected) {
        if (RequestsTerm == term && RequestsState == state && RequestsLeaderId == leaderId) return;
        RequestsTerm = term;
        RequestsState = state;
        RequestsLeaderId = leaderId;
    }
    std::vector<std::pair<std::shared_ptr<INode>, uint32_t>> rejected;
    for (auto* queue : {&Waiting, &StrongWaiting, &WaitingStateChange}) {
        std::queue<TWaiting> retained;
        while (!queue->empty()) {
            auto waiting = std::move(queue->front());
            queue->pop();
            const bool remove = disconnected ? waiting.ReplyTo == disconnected :
                waiting.Term != 0 && (!leader || waiting.Term != term);
            if (!remove) {
                retained.push(std::move(waiting));
                continue;
            }
            if (waiting.Command->Flags & TCommandRequest::EWrite) {
                PendingWriteTerms.erase(waiting.Index);
                EraseWriteAnswer(waiting.Index);
            }
            ReleaseAdmission(waiting.AdmissionId);
            if (!disconnected && waiting.ReplyTo)
                rejected.emplace_back(waiting.ReplyTo, waiting.Command->Cookie);
        }
        queue->swap(retained);
    }
    CancelForwarded(disconnected);
    for (auto& [replyTo, cookie] : rejected)
        replyTo->Send(NewHoldedMessage(TCommandResponse{
            .Cookie = cookie, .ErrorCode = TCommandResponse::Failed}));
}

void TRequestProcessor::CancelForwarded(const std::shared_ptr<INode>& disconnected, bool peerFailure) {
    for (auto it = Forwarded.begin(); it != Forwarded.end();) {
        const auto& route = it->second;
        const bool remove = disconnected ?
            (peerFailure ? route.Peer == disconnected : route.Client == disconnected) :
            Raft->CurrentStateName() != EState::FOLLOWER ||
            route.Term != Raft->GetState()->CurrentTerm || route.LeaderId != Raft->GetLeaderId();
        if (!remove) { ++it; continue; }
        auto pending = std::move(it->second);
        it = Forwarded.erase(it);
        ReleaseAdmission(pending.AdmissionId);
        if (!disconnected || peerFailure) pending.Client->Send(NewHoldedMessage(
            TCommandResponse{.Cookie = pending.OriginalCookie, .ErrorCode = TCommandResponse::Failed}));
    }
}

void TRequestProcessor::OnWriteRequest(TMessageHolder<TCommandRequest> command,
        const std::shared_ptr<INode>& replyTo, uint64_t admissionId) {
    auto stateName = Raft->CurrentStateName();
    auto flags = command->Flags;
    assert((flags & TCommandRequest::EWrite));

    if (stateName == EState::LEADER) {
        const auto term = Raft->GetState()->CurrentTerm;
        const auto cookie = command->Cookie;
        try {
            uint64_t index = Raft->Append(std::move(Rsm->Prepare(command)));
            if (replyTo) {
                PendingWriteTerms[index] = term;
                Waiting.emplace(TWaiting{index, 0, std::move(command), replyTo, term, admissionId});
            }
        } catch (const TRaftLogCapacityExceeded&) {
            ReleaseAdmission(admissionId);
            if (replyTo) replyTo->Send(NewHoldedMessage(TCommandResponse{
                .Cookie = cookie, .ErrorCode = TCommandResponse::Overloaded}));
        }
    } else {
        Forward(std::move(command), replyTo, admissionId);
    }
}

void TRequestProcessor::OnCommandRequest(TMessageHolder<TCommandRequest> command, const std::shared_ptr<INode>& replyTo) {
    if (!command.HasValidExtent() || command->Type != uint32_t(EMessageType::COMMAND_REQUEST))
        throw TMessageError("Invalid command request");
    ValidateMessageTree(command);
    PruneRequests();
    uint64_t admissionId = 0;
    if (!TryAdmit(command, replyTo, admissionId)) {
        if (replyTo) replyTo->Send(NewHoldedMessage(TCommandResponse{
            .Cookie = command->Cookie, .ErrorCode = TCommandResponse::Overloaded}));
        return;
    }
    try {
        Rsm->Validate(command);
        DispatchCommandRequest(std::move(command), replyTo, admissionId);
    } catch (...) {
        ReleaseAdmission(admissionId);
        throw;
    }
}

void TRequestProcessor::OnCommandResponse(TMessageHolder<TCommandResponse> command,
                                         const std::shared_ptr<INode>& peer) {
    if (!command.HasValidExtent() || command->Type != uint32_t(EMessageType::COMMAND_RESPONSE))
        throw TMessageError("Invalid command response");
    ValidateMessageTree(command);
    PruneRequests();
    auto it = Forwarded.find(command->Cookie);
    if (it == Forwarded.end() || !peer || it->second.Peer != peer) return;
    auto pending = std::move(it->second);
    Forwarded.erase(it);
    ReleaseAdmission(pending.AdmissionId);
    auto reply = NewHoldedMessage<TCommandResponse>(command->Len);
    memcpy(reply.Mes, command.Mes, command->Len);
    reply->Cookie = pending.OriginalCookie;
    pending.Client->Send(std::move(reply));
}

void TRequestProcessor::OnPeerDisconnected(const std::shared_ptr<INode>& peer) {
    if (peer) CancelForwarded(peer, true);
}

void TRequestProcessor::AddPeerEndpoint(uint32_t id, std::shared_ptr<INode> peer) {
    if (!id || id == Raft->GetId() || !peer)
        throw std::invalid_argument("Invalid request-routing peer endpoint");
    if (!Nodes.emplace(id, std::move(peer)).second)
        throw std::invalid_argument("Duplicate request-routing peer ID");
}

void TRequestProcessor::RemovePeerEndpoint(uint32_t id, const std::shared_ptr<INode>& peer) noexcept {
    auto it = Nodes.find(id);
    if (it != Nodes.end() && it->second == peer) Nodes.erase(it);
}

void TRequestProcessor::CleanUp(const std::shared_ptr<INode>& replyTo) {
    if (!replyTo) return;
    PruneRequests(replyTo);
}

void TRequestProcessor::ProcessCommitted() {
    PruneRequests();
    auto commitIndex = Raft->GetVolatileState()->CommitIndex;
    // Never rewind application progress, or overflow LastAppliedIndex + 1.
    if (commitIndex <= Rsm->LastAppliedIndex) return;
    auto& state = Raft->GetState();
    for (auto i = Rsm->LastAppliedIndex+1; i <= commitIndex; i++) {
        auto entry = state->Get(i);
        if (entry->Flags & TLogEntry::EConfiguration) {
            // Configuration is Raft metadata, never an application command.
            // Validate its control record and advance the application boundary
            // without exposing it to the state machine.
            DecodeConfigurationEntry(entry);
            continue;
        }
        if (entry->Flags == TLogEntry::EStub) {
            continue;
        }
        TMessageHolder<TMessage> reply;
        try { reply = Rsm->Write(entry, i); }
        catch (...) { std::terminate(); }
        auto pending = PendingWriteTerms.find(i);
        if (pending != PendingWriteTerms.end() && pending->second == entry->Term) {
            auto heldReply = reply ? reply : NewHoldedMessage(TCommandResponse {.Index = i});
            const auto answerBytes = heldReply->Len;
            if (PendingReplyBytes <= AdmissionLimits.MaxPendingReplyBytes &&
                answerBytes <= AdmissionLimits.MaxPendingReplyBytes - PendingReplyBytes) {
                auto [answer, inserted] = WriteAnswers.emplace(i, TAnswer {
                    .Term = entry->Term, .Reply = std::move(heldReply)
                });
                if (inserted) PendingReplyBytes += answer->second.Reply->Len;
            }
            // If it does not fit, ProcessWaiting returns a small unknown-outcome
            // error for this already-applied write instead of caching the result.
        }
    }
    Rsm->LastAppliedIndex = commitIndex;
}

void TRequestProcessor::ProcessWaiting() {
    PruneRequests();
    auto lastApplied = Rsm->LastAppliedIndex;
    while (!Waiting.empty()) {
        if (Waiting.front().Term != 0 && (Raft->CurrentStateName() != EState::LEADER ||
            Waiting.front().Term != Raft->GetState()->CurrentTerm)) {
            PruneRequests();
            continue;
        }
        if (Waiting.front().Index > lastApplied) break;
        auto w = Waiting.front(); Waiting.pop();
        TMessageHolder<TCommandResponse> reply;
        auto cookie = w.Command->Cookie;
        if (w.Command->Flags & TCommandRequest::EWrite) {
            auto answer = WriteAnswers.find(w.Index);
            if (answer != WriteAnswers.end() && answer->second.Term == w.Term) {
                reply = answer->second.Reply.Cast<TCommandResponse>();
            } else {
                reply = NewHoldedMessage(TCommandResponse{.ErrorCode = 1});
            }
            PendingWriteTerms.erase(w.Index);
            EraseWriteAnswer(w.Index);
        } else {
            try { reply = Rsm->Read(std::move(w.Command), w.Index).Cast<TCommandResponse>(); }
            catch (const std::exception&) {
                reply = NewHoldedMessage(TCommandResponse{.Index = w.Index, .ErrorCode = 2});
            }
        }
        reply->Cookie = cookie;
        ReleaseAdmission(w.AdmissionId);
        w.ReplyTo->Send(std::move(reply));
    }

    while (!StrongWaiting.empty()) {
        if (Raft->CurrentStateName() != EState::LEADER ||
            StrongWaiting.front().Term != Raft->GetState()->CurrentTerm) {
            PruneRequests();
            continue;
        }
        if (StrongWaiting.front().Index > lastApplied ||
            StrongWaiting.front().Seqno > Raft->CommitSeqno()) break;
        auto w = StrongWaiting.front(); StrongWaiting.pop();
        TMessageHolder<TCommandResponse> reply;
        assert (!(w.Command->Flags & TCommandRequest::EWrite));
        auto cookie = w.Command->Cookie;
        try { reply = Rsm->Read(std::move(w.Command), w.Index).Cast<TCommandResponse>(); }
        catch (const std::exception&) {
            reply = NewHoldedMessage(TCommandResponse{.Index = w.Index, .ErrorCode = 2});
        }
        reply->Cookie = cookie;
        ReleaseAdmission(w.AdmissionId);
        w.ReplyTo->Send(std::move(reply));
    }
}

