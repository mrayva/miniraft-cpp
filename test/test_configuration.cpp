#include <miniraft/configuration.h>
#include <miniraft/durable.h>
#include <miniraft/raft.h>
#include <miniraft/persist.h>
#include <miniraft/snapshot_codec.h>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <string>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}
#include <algorithm>
namespace {
template<class F> void Reject(F f) {
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}
TConfigurationRecord Joint() {
    return {EConfigurationPhase::Joint, 3, 2, 2, {1, 2, 3}, {3, 4, 5}};
}
TMessageHolder<TLogEntry> Copy(TMessageHolder<TLogEntry> entry) {
    auto copy = NewHoldedMessage<TLogEntry>(entry->Len);
    memcpy(copy.Mes, entry.Mes, entry->Len); return copy;
}
void FixChecksum(TMessageHolder<TLogEntry> entry) {
    const auto length = entry->Len - sizeof(TLogEntry);
    const auto crc = NSnapshotCodec::Checksum(std::string_view(entry->Data, length - 4));
    for (int i = 0; i < 4; ++i) entry->Data[length - 4 + i] = char(crc >> (8 * i));
}
struct TEndpoint : INode {
    unsigned Sent = 0;
    std::vector<TMessageHolder<TMessage>> Messages;
    std::function<void(TMessageHolder<TMessage>)> Route;
    void Send(TMessageHolder<TMessage> message) override {
        ++Sent;
        if (Route) Route(std::move(message));
        else Messages.push_back(std::move(message));
    }
    void Drain() override {}
};
struct TCountingRsm : TDummyRsm {
    unsigned Writes = 0;
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> entry, uint64_t index) override {
        ++Writes;
        return TDummyRsm::Write(std::move(entry), index);
    }
};
struct TSnapshotRsm : TCountingRsm {
    size_t SnapshotPaddingBytes = 0;
    std::string SaveSnapshot() const override {
        return std::to_string(LastAppliedIndex) + std::string(SnapshotPaddingBytes, 's');
    }
    void ValidateSnapshot(std::string_view data, uint64_t index) const override {
        const auto prefix = std::to_string(index);
        if (data.size() != prefix.size() + SnapshotPaddingBytes ||
            data.substr(0, prefix.size()) != prefix ||
            data.substr(prefix.size()) != std::string(SnapshotPaddingBytes, 's'))
            throw std::runtime_error("Snapshot index or payload mismatch");
    }
    void LoadSnapshot(std::string_view data, uint64_t index) override {
        ValidateSnapshot(data, index);
        LastAppliedIndex = index;
    }
    bool SupportsApplicationSnapshotInstall() const override { return true; }
};
TMessageHolder<TLogEntry> Ordinary(uint64_t term) {
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = term; return entry;
}
void test_raft_accepts_full_width_voter_ids(void**) {
    constexpr uint32_t highId = 0x80000001u;
    auto state = std::make_shared<TState>();
    TNodeDict peers{{1, std::make_shared<TEndpoint>()}};
    TRaft raft(state, highId, peers);
    assert_true(raft.GetId() == highId);
    assert_true(raft.IsVoter(highId));
    assert_true(raft.GetVoterIds() == (std::vector<uint32_t>{1, highId}));
    Reject([&] { TRaft invalid(std::make_shared<TState>(), -1, peers); });
}
void test_configuration_snapshot_projection_and_history_resume(void**) {
    auto initial = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    Reject([&] { initial.MakeSnapshot("empty boundary"); });
    auto prefix = initial.Observe(1, Ordinary(1));
    const auto bootstrap = prefix.MakeSnapshot("bootstrap");
    assert_true(EncodeSnapshot(bootstrap).substr(0, 8) == "MRFTS001");
    assert_int_equal(bootstrap.Index, 1); assert_int_equal(bootstrap.Term, 1);
    assert_false(bootstrap.HasConfigurationMetadata());
    auto joint = Joint(); joint.Term = 2; joint.PreviousIndex = joint.PreviousTerm = 0;
    auto pending = prefix.Advance(2, EncodeConfigurationEntry(joint)).Observe(3, Ordinary(2));
    const auto pendingSnapshot = pending.MakeSnapshot(std::string("j\0x", 3));
    const auto jointBytes = EncodeSnapshot(pendingSnapshot);
    assert_true(jointBytes.substr(0, 8) == "MRFTS007");
    assert_true(pendingSnapshot.HasConfigurationLineage());
    assert_true(pendingSnapshot.PreviousVoters == joint.OldVoters);
    assert_int_equal(pendingSnapshot.PreviousConfigurationIndex, 0);
    assert_int_equal(pendingSnapshot.PreviousConfigurationTerm, 0);
    assert_int_equal(pendingSnapshot.PreviousJointIndex, 2);
    assert_int_equal(pendingSnapshot.PreviousJointTerm, 2);
    auto restoredJoint = TConfigurationHistory::FromJointSnapshot(DecodeSnapshot(jointBytes));
    assert_int_equal(restoredJoint.ThroughIndex(), 3); assert_int_equal(restoredJoint.ConfigurationIndex(), 2);
    assert_true(restoredJoint.OldVoters() == joint.OldVoters); assert_true(restoredJoint.NewVoters() == joint.NewVoters);
    assert_true(restoredJoint.PreviousVoters() == joint.OldVoters);
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    auto stable = restoredJoint.Advance(4, EncodeConfigurationEntry(final));
    const auto stableSnapshot = stable.MakeSnapshot("stable");
    const auto stableBytes = EncodeSnapshot(stableSnapshot);
    assert_true(stableBytes.substr(0, 8) == "MRFTS006");
    assert_true(stableSnapshot.ConfigurationTransitions.size() == 1);
    assert_true(DecodeSnapshot(stableBytes) == stableSnapshot);
    auto truncatedHistory = stableSnapshot;
    truncatedHistory.ConfigurationTransitions.back().StableIndex = 0;
    truncatedHistory.ConfigurationTransitions.back().StableTerm = 0;
    Reject([&] { EncodeSnapshot(truncatedHistory); });
    assert_true(stableSnapshot.HasStablePredecessor());
    assert_true(stableSnapshot.PreviousVoters == joint.OldVoters);
    assert_int_equal(stableSnapshot.PreviousJointIndex, 2);
    assert_int_equal(stableSnapshot.PreviousJointTerm, joint.Term);
    auto restoredStable = TConfigurationHistory::FromStableSnapshot(DecodeSnapshot(stableBytes));
    assert_int_equal(restoredStable.ConfigurationIndex(), 4); assert_int_equal(restoredStable.ConfigurationTerm(), 3);
    assert_true(restoredStable.OldVoters() == joint.NewVoters); assert_false(restoredStable.IsJoint());
    TConfigurationRecord next{EConfigurationPhase::Joint, 3, 4, 3, joint.NewVoters, {3, 4, 6}};
    auto resumed = restoredStable.Advance(5, EncodeConfigurationEntry(next));
    assert_true(resumed.IsJoint()); assert_true(resumed.NewVoters() == next.NewVoters);
    const auto resumedJoint = resumed.MakeSnapshot("second transition");
    assert_true(EncodeSnapshot(resumedJoint).substr(0, 8) == "MRFTS007");
    assert_true(DecodeSnapshot(EncodeSnapshot(resumedJoint)) == resumedJoint);
    assert_true(pending.NewVoters() == joint.NewVoters); assert_false(stable.IsJoint());
    TState jointState; jointState.BindVoters(pendingSnapshot.Voters);
    jointState.CurrentTerm = pendingSnapshot.Term;
    jointState.InstallSnapshot(pendingSnapshot);
    assert_true(*jointState.GetSnapshot() == pendingSnapshot);
    TState stableState; stableState.BindVoters(stableSnapshot.Voters);
    stableState.CurrentTerm = stableSnapshot.Term;
    stableState.InstallSnapshot(stableSnapshot);
    assert_true(*stableState.GetSnapshot() == stableSnapshot);
    TState unbound; unbound.CurrentTerm = pendingSnapshot.Term;
    Reject([&] { unbound.InstallSnapshot(pendingSnapshot); });
    unbound.Snapshot = std::make_shared<TSnapshot>(pendingSnapshot);
    Reject([&] { unbound.BindVoters(pendingSnapshot.Voters); });
}
void test_generated_membership_sets_roundtrip_and_bootstrap_from_snapshots(void**) {
    uint32_t random = UINT32_C(0x6d2b79f5);
    auto nextRandom = [&] {
        random ^= random << 13;
        random ^= random >> 17;
        random ^= random << 5;
        return random;
    };

    auto bootstrapFreshLearner = [](const TSnapshot& snapshot,
            std::vector<uint32_t> peerVoters, std::vector<uint32_t> configuredVoters,
            uint32_t leaderId, bool restartPartial) {
        constexpr uint32_t LearnerId = 1000;
        constexpr size_t ChunkBytes = 32;
        TNodeDict peers;
        std::shared_ptr<TEndpoint> leaderEndpoint;
        for (const auto voter : peerVoters) {
            auto endpoint = std::make_shared<TEndpoint>();
            if (voter == leaderId) leaderEndpoint = endpoint;
            peers.emplace(voter, std::move(endpoint));
        }
        assert_true(bool(leaderEndpoint));

        std::vector<uint32_t> learners{LearnerId};
        for (const auto peer : peerVoters)
            if (!std::binary_search(configuredVoters.begin(), configuredVoters.end(), peer))
                learners.push_back(peer);
        std::sort(learners.begin(), learners.end());

        auto state = std::make_shared<TState>();
        state->BindVoters(configuredVoters);
        state->CurrentTerm = snapshot.Term;
        auto raft = std::make_shared<TRaft>(state, LearnerId, peers, learners);
        auto application = std::make_shared<TSnapshotRsm>();
        auto processor = std::make_unique<TRequestProcessor>(raft, application, peers);
        const auto bytes = EncodeSnapshot(snapshot);
        const bool jointElectionFenceLeader = snapshot.IsJoint() || snapshot.HasConfigurationLineage() ||
            !snapshot.ConfigurationTransitions.empty();
        auto sendChunk = [&](uint64_t offset, uint32_t count, uint32_t seqno) {
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
            request->Src = leaderId; request->Dst = LearnerId;
            request->Term = std::max<uint64_t>(snapshot.Term, state->CurrentTerm);
            request->Seqno = seqno;
            request->SnapshotIndex = snapshot.Index; request->SnapshotTerm = snapshot.Term;
            request->Offset = offset; request->TotalBytes = bytes.size();
            request->Done = (offset + count == bytes.size() ? TInstallSnapshotRequest::DoneFlag : 0u) |
                (jointElectionFenceLeader ? TInstallSnapshotRequest::JointElectionFenceLeader : 0u);
            request->LeaderId = leaderId;
            memcpy(request->Data, bytes.data() + offset, count);
            raft->Process(ITimeSource::Time{}, request);
            assert_false(leaderEndpoint->Messages.empty());
            return leaderEndpoint->Messages.back().Cast<TInstallSnapshotResponse>();
        };

        const auto firstCount = uint32_t(std::min<size_t>(ChunkBytes, bytes.size()));
        if (bytes.size() > firstCount) {
            const auto gap = sendChunk(firstCount, 1, 1);
            assert_false(gap->Success);
            assert_int_equal(gap->NextOffset, 0);
            leaderEndpoint->Messages.clear();
        }
        const auto accepted = sendChunk(0, firstCount, 2);
        assert_true(accepted->Success);
        assert_false(accepted->Installed);
        assert_int_equal(accepted->NextOffset, firstCount);
        const auto duplicate = sendChunk(0, firstCount, 2);
        assert_true(duplicate->Success);
        assert_false(duplicate->Installed);
        assert_int_equal(duplicate->NextOffset, firstCount);

        uint64_t offset = firstCount;
        uint32_t seqno = 3;
        if (restartPartial) {
            // Only the durable Raft/application state survives; an incomplete
            // receive must restart from byte zero after volatile state is lost.
            processor.reset();
            raft.reset();
            application.reset();
            raft = std::make_shared<TRaft>(state, LearnerId, peers, learners);
            application = std::make_shared<TSnapshotRsm>();
            processor = std::make_unique<TRequestProcessor>(raft, application, peers);
            assert_int_equal(application->LastAppliedIndex, 0);
            leaderEndpoint->Messages.clear();
            offset = 0;
            seqno = 1;
        }

        while (offset < bytes.size()) {
            const auto count = uint32_t(std::min<uint64_t>(ChunkBytes, bytes.size() - offset));
            const auto response = sendChunk(offset, count, seqno++);
            assert_true(response->Success);
            assert_int_equal(response->NextOffset, offset + count);
            assert_int_equal(response->Installed, offset + count == bytes.size());
            offset += count;
        }
        assert_int_equal(state->SnapshotIndex, snapshot.Index);
        assert_int_equal(state->CommitIndex, snapshot.Index);
        assert_int_equal(application->LastAppliedIndex, snapshot.Index);
        assert_true(state->GetSnapshot() && *state->GetSnapshot() == snapshot);
        assert_false(raft->IsVoter(LearnerId));

        processor.reset();
        raft.reset();
        application.reset();
        raft = std::make_shared<TRaft>(state, LearnerId, peers, learners);
        application = std::make_shared<TSnapshotRsm>();
        processor = std::make_unique<TRequestProcessor>(raft, application, peers);
        processor->ProcessCommitted();
        auto expectedActiveVoters = snapshot.Voters;
        if (snapshot.IsJoint()) {
            expectedActiveVoters.insert(expectedActiveVoters.end(), snapshot.JointVoters.begin(),
                snapshot.JointVoters.end());
            std::sort(expectedActiveVoters.begin(), expectedActiveVoters.end());
            expectedActiveVoters.erase(std::unique(expectedActiveVoters.begin(), expectedActiveVoters.end()),
                expectedActiveVoters.end());
        }
        assert_true(raft->GetVoterIds() == expectedActiveVoters);
        assert_false(raft->IsVoter(LearnerId));
        assert_int_equal(application->LastAppliedIndex, snapshot.Index);
    };

    auto history = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0)
        .Observe(1, Ordinary(1));
    for (uint64_t transition = 0; transition < 96; ++transition) {
        // Alternate disjoint ID ranges with overlapping ranges so the generated
        // chain exercises both kinds of membership transition.
        const uint32_t firstId = transition % 4 == 0 ? 13 : 1;
        const uint32_t lastId = firstId + 11;
        std::vector<uint32_t> nextVoters;
        for (uint32_t id = firstId; id <= lastId; ++id)
            if (nextRandom() & 1) nextVoters.push_back(id);
        if (nextVoters.empty()) nextVoters.push_back(firstId + nextRandom() % 12);
        if (nextVoters == history.OldVoters()) {
            if (nextVoters.size() > 1) {
                nextVoters.pop_back();
            } else {
                const auto additional = nextVoters.front() == lastId
                    ? firstId : nextVoters.front() + 1;
                nextVoters.push_back(additional);
                std::sort(nextVoters.begin(), nextVoters.end());
            }
        }

        const auto jointIndex = history.ThroughIndex() + 1;
        TConfigurationRecord joint{EConfigurationPhase::Joint, jointIndex,
            history.ConfigurationIndex(), history.ConfigurationTerm(),
            history.OldVoters(), nextVoters};
        history = history.Advance(jointIndex, EncodeConfigurationEntry(joint));
        const auto jointImage = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
        const auto decodedJoint = DecodeSnapshot(EncodeSnapshot(jointImage));
        assert_true(decodedJoint == jointImage);
        history = TConfigurationHistory::FromJointSnapshot(decodedJoint);
        assert_true(history.IsJoint());
        assert_true(history.NewVoters() == nextVoters);
        if (transition < 12) {
            auto peerVoters = decodedJoint.Voters;
            peerVoters.insert(peerVoters.end(), decodedJoint.JointVoters.begin(),
                decodedJoint.JointVoters.end());
            std::sort(peerVoters.begin(), peerVoters.end());
            peerVoters.erase(std::unique(peerVoters.begin(), peerVoters.end()), peerVoters.end());
            bootstrapFreshLearner(decodedJoint, peerVoters, peerVoters,
                decodedJoint.Voters.front(),
                transition % 2 == 0);
        }

        const auto stableIndex = history.ThroughIndex() + 1;
        TConfigurationRecord stable{EConfigurationPhase::Stable, stableIndex,
            history.ConfigurationIndex(), history.ConfigurationTerm(),
            history.OldVoters(), history.NewVoters()};
        history = history.Advance(stableIndex, EncodeConfigurationEntry(stable));
        const auto stableImage = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
        const auto decodedStable = DecodeSnapshot(EncodeSnapshot(stableImage));
        assert_true(decodedStable == stableImage);
        history = TConfigurationHistory::FromStableSnapshot(decodedStable);
        assert_false(history.IsJoint());
        assert_true(history.OldVoters() == nextVoters);
        assert_int_equal(history.ConfigurationIndex(), stableIndex);
        assert_int_equal(history.ConfigurationTransitions().size(), transition + 1);
        if (transition < 12)
            bootstrapFreshLearner(decodedStable, decodedStable.Voters, decodedStable.Voters,
                decodedStable.Voters.front(),
                transition % 2 != 0);
    }
}
void test_configuration_history_rolls_bounded_transition_suffix_and_resumes(void**) {
    const std::vector<uint32_t> firstSet{1, 2, 3};
    const std::vector<uint32_t> secondSet{2, 3, 4};
    auto history = TConfigurationHistory::Stable(firstSet, 0, 0, 0, 0)
        .Observe(1, Ordinary(1));
    TSnapshot oldestRetainedAnchor;
    std::optional<TSnapshot> stableAt255, stableAt256, stableAt257;

    for (uint64_t transition = 1; transition <= TSnapshot::MaxConfigurationTransitions + 1; ++transition) {
        const auto jointIndex = history.ThroughIndex() + 1;
        const auto stableIndex = jointIndex + 1;
        const auto nextVoters = transition % 2 ? secondSet : firstSet;
        TConfigurationRecord joint{EConfigurationPhase::Joint, jointIndex,
            history.ConfigurationIndex(), history.ConfigurationTerm(), history.OldVoters(), nextVoters};
        history = history.Advance(jointIndex, EncodeConfigurationEntry(joint));
        joint.Phase = EConfigurationPhase::Stable;
        joint.Term = stableIndex;
        joint.PreviousIndex = jointIndex;
        joint.PreviousTerm = jointIndex;
        history = history.Advance(stableIndex, EncodeConfigurationEntry(joint));
        if (transition == 1)
            oldestRetainedAnchor = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
        assert_true(history.ConfigurationTransitions().size() ==
            std::min<size_t>(transition, TSnapshot::MaxConfigurationTransitions));
        if (transition == 255) stableAt255 = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
        if (transition == 256) stableAt256 = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
        if (transition == 257) stableAt257 = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
    }

    const auto& retained = history.ConfigurationTransitions();
    assert_int_equal(retained.size(), TSnapshot::MaxConfigurationTransitions);
    // The oldest retained edge is a valid suffix anchor at the stable boundary
    // immediately after the evicted transition.
    assert_true(retained.front().PreviousConfigurationIndex == 3);
    assert_true(retained.front().PreviousConfigurationTerm == 3);
    assert_true(retained.back().StableIndex == history.ConfigurationIndex());
    assert_true(retained.back().StableTerm == history.ConfigurationTerm());
    const auto encodedAt256 = EncodeSnapshot(*stableAt256);
    assert_true(encodedAt256.substr(0, 8) == "MRFTS008");
    assert_true(DecodeSnapshot(encodedAt256) == *stableAt256);
    const auto encodedAt257 = EncodeSnapshot(*stableAt257);
    assert_true(encodedAt257.substr(0, 8) == "MRFTS010");
    assert_true(DecodeSnapshot(encodedAt257) == *stableAt257);

    auto boundaryJointHistory = TConfigurationHistory::FromStableSnapshot(*stableAt255);
    const auto boundaryJointIndex = boundaryJointHistory.ThroughIndex() + 1;
    const auto boundaryNewVoters = boundaryJointHistory.OldVoters() == firstSet ? secondSet : firstSet;
    TConfigurationRecord boundaryJoint{EConfigurationPhase::Joint, boundaryJointIndex,
        boundaryJointHistory.ConfigurationIndex(), boundaryJointHistory.ConfigurationTerm(),
        boundaryJointHistory.OldVoters(), boundaryNewVoters};
    boundaryJointHistory = boundaryJointHistory.Advance(boundaryJointIndex,
        EncodeConfigurationEntry(boundaryJoint));
    const auto jointAt256 = boundaryJointHistory.MakeSnapshot("joint boundary");
    const auto encodedJointAt256 = EncodeSnapshot(jointAt256);
    assert_true(encodedJointAt256.substr(0, 8) == "MRFTS009");
    assert_true(DecodeSnapshot(encodedJointAt256) == jointAt256);
    auto extendedJointHistory = TConfigurationHistory::FromStableSnapshot(*stableAt256);
    const auto extendedJointIndex = extendedJointHistory.ThroughIndex() + 1;
    const auto extendedJointVoters = extendedJointHistory.OldVoters() == firstSet ? secondSet : firstSet;
    TConfigurationRecord extendedJoint{EConfigurationPhase::Joint, extendedJointIndex,
        extendedJointHistory.ConfigurationIndex(), extendedJointHistory.ConfigurationTerm(),
        extendedJointHistory.OldVoters(), extendedJointVoters};
    extendedJointHistory = extendedJointHistory.Advance(extendedJointIndex,
        EncodeConfigurationEntry(extendedJoint));
    const auto jointAt257 = extendedJointHistory.MakeSnapshot("joint extended boundary");
    const auto encodedJointAt257 = EncodeSnapshot(jointAt257);
    assert_true(encodedJointAt257.substr(0, 8) == "MRFTS011");
    assert_true(DecodeSnapshot(encodedJointAt257) == jointAt257);

    const auto stableImage = history.MakeSnapshot(std::to_string(history.ThroughIndex()));
    const auto encodedStableImage = EncodeSnapshot(stableImage);
    assert_true(encodedStableImage.substr(0, 8) == "MRFTS010");
    assert_true(DecodeSnapshot(encodedStableImage) == stableImage);
    const auto legacyPeerStableEncoding = PrepareSnapshotEncoding(
        stableImage, TSnapshot::MaxExtendedConfigurationTransitions);
    std::string legacyPeerStableImage(SnapshotEncodingSize(*legacyPeerStableEncoding), '\0');
    CopySnapshotEncodingRange(stableImage, *legacyPeerStableEncoding, 0,
        legacyPeerStableImage.data(), legacyPeerStableImage.size());
    assert_true(legacyPeerStableImage.substr(0, 8) == "MRFTS008");
    assert_int_equal(DecodeSnapshot(legacyPeerStableImage).ConfigurationTransitions.size(), 256);
    const auto olderPeerStableEncoding = PrepareSnapshotEncoding(
        stableImage, TSnapshot::MaxLegacyConfigurationTransitions);
    std::string olderPeerStableImage(SnapshotEncodingSize(*olderPeerStableEncoding), '\0');
    CopySnapshotEncodingRange(stableImage, *olderPeerStableEncoding, 0,
        olderPeerStableImage.data(), olderPeerStableImage.size());
    const auto decodedOlderStable = DecodeSnapshot(olderPeerStableImage);
    assert_true(olderPeerStableImage.substr(0, 8) == "MRFTS006");
    assert_int_equal(decodedOlderStable.ConfigurationTransitions.size(), 32);
    assert_true(std::equal(decodedOlderStable.ConfigurationTransitions.begin(),
        decodedOlderStable.ConfigurationTransitions.end(), stableImage.ConfigurationTransitions.end() - 32));
    const auto lineageOnlyStableEncoding = PrepareSnapshotEncoding(stableImage, 0);
    std::string lineageOnlyStableImage(SnapshotEncodingSize(*lineageOnlyStableEncoding), '\0');
    CopySnapshotEncodingRange(stableImage, *lineageOnlyStableEncoding, 0,
        lineageOnlyStableImage.data(), lineageOnlyStableImage.size());
    const auto decodedLineageOnlyStable = DecodeSnapshot(lineageOnlyStableImage);
    assert_true(lineageOnlyStableImage.substr(0, 8) == "MRFTS004");
    assert_true(decodedLineageOnlyStable.ConfigurationTransitions.empty());
    assert_true(decodedLineageOnlyStable.HasStablePredecessor());
    assert_true(decodedLineageOnlyStable.PreviousVoters == stableImage.PreviousVoters);
    const auto baseStableEncoding = PrepareSnapshotEncoding(stableImage, 0, false);
    std::string baseStableImage(SnapshotEncodingSize(*baseStableEncoding), '\0');
    CopySnapshotEncodingRange(stableImage, *baseStableEncoding, 0,
        baseStableImage.data(), baseStableImage.size());
    const auto decodedBaseStable = DecodeSnapshot(baseStableImage);
    assert_true(baseStableImage.substr(0, 8) == "MRFTS003");
    assert_true(decodedBaseStable.StableConfiguration);
    assert_true(decodedBaseStable.ConfigurationTransitions.empty());
    assert_true(!decodedBaseStable.HasConfigurationLineage());
    assert_int_equal(decodedBaseStable.ConfigurationIndex, stableImage.ConfigurationIndex);
    assert_int_equal(decodedBaseStable.ConfigurationTerm, stableImage.ConfigurationTerm);
    auto legacyHistoryImage = encodedStableImage;
    memcpy(legacyHistoryImage.data(), "MRFTS006", 8);
    const auto legacyChecksum = NSnapshotCodec::Checksum(
        std::string_view(legacyHistoryImage.data(), legacyHistoryImage.size() - 4));
    for (size_t i = 0; i < 4; ++i)
        legacyHistoryImage[legacyHistoryImage.size() - 4 + i] = char(legacyChecksum >> (8 * i));
    Reject([&] { DecodeSnapshot(legacyHistoryImage); });
    auto priorExtendedImage = encodedStableImage;
    memcpy(priorExtendedImage.data(), "MRFTS008", 8);
    const auto priorExtendedChecksum = NSnapshotCodec::Checksum(
        std::string_view(priorExtendedImage.data(), priorExtendedImage.size() - 4));
    for (size_t i = 0; i < 4; ++i)
        priorExtendedImage[priorExtendedImage.size() - 4 + i] = char(priorExtendedChecksum >> (8 * i));
    Reject([&] { DecodeSnapshot(priorExtendedImage); });

#ifdef __linux__
    // Once the trusted baseline falls outside the retained suffix, a current
    // voter cannot use the snapshot to move this stale store to an unknown
    // configuration or raise its term.
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-history-window-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto staleState = std::make_shared<TDurableState>((dir / "stale").string(), 1);
    staleState->BindVoters(firstSet);
    staleState->CurrentTerm = 2;
    staleState->Append(Ordinary(1));
    staleState->CommitIndex = 1;
    staleState->Commit();
    TNodeDict peers;
    std::array<std::shared_ptr<TEndpoint>, 5> endpoints{};
    for (auto peer : {2u, 3u, 4u}) {
        endpoints[peer] = std::make_shared<TEndpoint>();
        peers.emplace(peer, endpoints[peer]);
    }
    auto stale = std::make_shared<TRaft>(staleState, 1, peers, std::vector<uint32_t>{4});
    auto application = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(stale, application, peers);
    const auto bytes = EncodeSnapshot(stableImage);
    auto sendToStale = [&](std::string_view image) {
        for (size_t offset = 0; offset < image.size();) {
            const auto count = std::min<size_t>(TInstallSnapshotRequest::MaxChunkBytes, image.size() - offset);
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
            request->Seqno = offset + 1;
            request->Src = 4; request->Dst = 1; request->Term = stableImage.Term + 1;
            request->SnapshotIndex = stableImage.Index; request->SnapshotTerm = stableImage.Term;
            request->Offset = offset; request->TotalBytes = image.size();
            request->Done = uint32_t(offset + count == image.size()) |
                TInstallSnapshotRequest::JointElectionFenceLeader;
            request->LeaderId = 4;
            memcpy(request->Data, image.data() + offset, count);
            stale->Process({}, request);
            offset += count;
        }
    };

    // The stable identity at the oldest retained suffix boundary still
    // authorizes recovery through the full 1024-transition window.
    auto retainedState = std::make_shared<TDurableState>((dir / "retained-stale").string(), 4);
    retainedState->BindVoters(firstSet);
    retainedState->CurrentTerm = oldestRetainedAnchor.Term;
    retainedState->Commit();
    retainedState->InstallSnapshot(oldestRetainedAnchor);
    TNodeDict retainedPeers{{2, std::make_shared<TEndpoint>()}, {3, std::make_shared<TEndpoint>()}};
    auto retainedRaft = std::make_shared<TRaft>(retainedState, 4, retainedPeers);
    auto retainedApplication = std::make_shared<TSnapshotRsm>();
    retainedApplication->LastAppliedIndex = oldestRetainedAnchor.Index;
    TRequestProcessor retainedProcessor(retainedRaft, retainedApplication, retainedPeers);
    for (size_t offset = 0; offset < bytes.size();) {
        const auto count = std::min<size_t>(TInstallSnapshotRequest::MaxChunkBytes, bytes.size() - offset);
        auto retainedRequest = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
        retainedRequest->Seqno = offset + 1;
        retainedRequest->Src = 2; retainedRequest->Dst = 4; retainedRequest->Term = stableImage.Term + 1;
        retainedRequest->SnapshotIndex = stableImage.Index; retainedRequest->SnapshotTerm = stableImage.Term;
        retainedRequest->Offset = offset; retainedRequest->TotalBytes = bytes.size();
        retainedRequest->Done = uint32_t(offset + count == bytes.size()) |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        retainedRequest->LeaderId = 2;
        memcpy(retainedRequest->Data, bytes.data() + offset, count);
        retainedRaft->Process({}, retainedRequest);
        offset += count;
    }
    assert_int_equal(retainedState->CurrentTerm, stableImage.Term + 1);
    assert_true(*retainedState->GetSnapshot() == stableImage);
    assert_true(retainedState->GetBoundVoters() == firstSet);
    assert_true(retainedRaft->GetVoterIds() == secondSet);
    assert_int_equal(retainedApplication->LastAppliedIndex, stableImage.Index);

    sendToStale(bytes);
    assert_int_equal(staleState->CurrentTerm, 2);
    assert_true(!staleState->GetSnapshot());
    assert_int_equal(application->LastAppliedIndex, 0);
    assert_true(endpoints[4]->Messages.empty());

    // A base-format image without lineage must not bypass the same proof
    // requirement when it omits the transition history entirely.
    sendToStale(baseStableImage);
    assert_int_equal(staleState->CurrentTerm, 2);
    assert_true(!staleState->GetSnapshot());
    assert_int_equal(application->LastAppliedIndex, 0);
    assert_true(endpoints[4]->Messages.empty());
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif

    auto resumed = TConfigurationHistory::FromStableSnapshot(DecodeSnapshot(EncodeSnapshot(stableImage)));
    const auto nextJointIndex = resumed.ThroughIndex() + 1;
    TConfigurationRecord nextJoint{EConfigurationPhase::Joint, nextJointIndex,
        resumed.ConfigurationIndex(), resumed.ConfigurationTerm(), resumed.OldVoters(),
        resumed.OldVoters() == firstSet ? secondSet : firstSet};
    resumed = resumed.Advance(nextJointIndex, EncodeConfigurationEntry(nextJoint));
    const auto jointImage = resumed.MakeSnapshot(std::to_string(resumed.ThroughIndex()));
    const auto encodedJointImage = EncodeSnapshot(jointImage);
    assert_true(encodedJointImage.substr(0, 8) == "MRFTS011");
    assert_int_equal(jointImage.ConfigurationTransitions.size(), TSnapshot::MaxConfigurationTransitions);
    assert_true(DecodeSnapshot(encodedJointImage) == jointImage);
    const auto legacyPeerJointEncoding = PrepareSnapshotEncoding(
        jointImage, TSnapshot::MaxExtendedConfigurationTransitions);
    std::string legacyPeerJointImage(SnapshotEncodingSize(*legacyPeerJointEncoding), '\0');
    CopySnapshotEncodingRange(jointImage, *legacyPeerJointEncoding, 0,
        legacyPeerJointImage.data(), legacyPeerJointImage.size());
    assert_true(legacyPeerJointImage.substr(0, 8) == "MRFTS009");
    assert_int_equal(DecodeSnapshot(legacyPeerJointImage).ConfigurationTransitions.size(), 256);
    const auto olderPeerJointEncoding = PrepareSnapshotEncoding(
        jointImage, TSnapshot::MaxLegacyConfigurationTransitions);
    std::string olderPeerJointImage(SnapshotEncodingSize(*olderPeerJointEncoding), '\0');
    CopySnapshotEncodingRange(jointImage, *olderPeerJointEncoding, 0,
        olderPeerJointImage.data(), olderPeerJointImage.size());
    const auto decodedOlderJoint = DecodeSnapshot(olderPeerJointImage);
    assert_true(olderPeerJointImage.substr(0, 8) == "MRFTS007");
    assert_int_equal(decodedOlderJoint.ConfigurationTransitions.size(), 32);
    assert_true(std::equal(decodedOlderJoint.ConfigurationTransitions.begin(),
        decodedOlderJoint.ConfigurationTransitions.end(), jointImage.ConfigurationTransitions.end() - 32));
    const auto lineageOnlyJointEncoding = PrepareSnapshotEncoding(jointImage, 0);
    std::string lineageOnlyJointImage(SnapshotEncodingSize(*lineageOnlyJointEncoding), '\0');
    CopySnapshotEncodingRange(jointImage, *lineageOnlyJointEncoding, 0,
        lineageOnlyJointImage.data(), lineageOnlyJointImage.size());
    const auto decodedLineageOnlyJoint = DecodeSnapshot(lineageOnlyJointImage);
    assert_true(lineageOnlyJointImage.substr(0, 8) == "MRFTS005");
    assert_true(decodedLineageOnlyJoint.ConfigurationTransitions.empty());
    assert_true(decodedLineageOnlyJoint.HasConfigurationLineage());
    assert_true(decodedLineageOnlyJoint.PreviousVoters == jointImage.PreviousVoters);
    const auto baseJointEncoding = PrepareSnapshotEncoding(jointImage, 0, false);
    std::string baseJointImage(SnapshotEncodingSize(*baseJointEncoding), '\0');
    CopySnapshotEncodingRange(jointImage, *baseJointEncoding, 0,
        baseJointImage.data(), baseJointImage.size());
    const auto decodedBaseJoint = DecodeSnapshot(baseJointImage);
    assert_true(baseJointImage.substr(0, 8) == "MRFTS002");
    assert_true(decodedBaseJoint.IsJoint());
    assert_true(decodedBaseJoint.ConfigurationTransitions.empty());
    assert_true(!decodedBaseJoint.HasConfigurationLineage());
    assert_int_equal(decodedBaseJoint.ConfigurationIndex, jointImage.ConfigurationIndex);
    assert_int_equal(decodedBaseJoint.ConfigurationTerm, jointImage.ConfigurationTerm);

    // Exercise MRFTS011 through the runtime metadata probe and fresh-learner
    // install path, not only through the standalone codec.
    auto bootstrapVoters = jointImage.Voters;
    bootstrapVoters.insert(bootstrapVoters.end(), jointImage.JointVoters.begin(), jointImage.JointVoters.end());
    std::sort(bootstrapVoters.begin(), bootstrapVoters.end());
    bootstrapVoters.erase(std::unique(bootstrapVoters.begin(), bootstrapVoters.end()), bootstrapVoters.end());
    constexpr uint32_t learnerId = 5;
    const auto leaderId = jointImage.Voters.front();
    std::vector<TMessageHolder<TMessage>> bootstrapResponses;
    TNodeDict bootstrapPeers;
    for (auto voter : bootstrapVoters) {
        auto peer = std::make_shared<TEndpoint>();
        peer->Route = [&](auto message) { bootstrapResponses.push_back(std::move(message)); };
        bootstrapPeers.emplace(voter, std::move(peer));
    }
    auto learnerState = std::make_shared<TState>();
    auto learnerRaft = std::make_shared<TRaft>(learnerState, learnerId, bootstrapPeers,
        std::vector<uint32_t>{learnerId});
    auto learnerApplication = std::make_shared<TSnapshotRsm>();
    TRequestProcessor learnerProcessor(learnerRaft, learnerApplication, bootstrapPeers);
    for (size_t offset = 0; offset < encodedJointImage.size();) {
        const auto count = std::min<size_t>(TInstallSnapshotRequest::MaxChunkBytes,
            encodedJointImage.size() - offset);
        auto install = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
        install->Seqno = offset + 1;
        install->Src = leaderId; install->Dst = learnerId; install->Term = jointImage.Term + 1;
        install->SnapshotIndex = jointImage.Index; install->SnapshotTerm = jointImage.Term;
        install->Offset = offset; install->TotalBytes = encodedJointImage.size();
        install->Done = uint32_t(offset + count == encodedJointImage.size()) |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        install->LeaderId = leaderId;
        memcpy(install->Data, encodedJointImage.data() + offset, count);
        learnerRaft->Process({}, install);
        offset += count;
    }
    assert_true(!bootstrapResponses.empty());
    auto response = bootstrapResponses.back().Cast<TInstallSnapshotResponse>();
    assert_true(response->Success); assert_true(response->Installed);
    assert_true(*learnerState->GetSnapshot() == jointImage);
    assert_int_equal(learnerState->CurrentTerm, jointImage.Term + 1);
    assert_true(learnerRaft->GetVoterIds() == bootstrapVoters);
    assert_true(learnerRaft->GetLearnerIds() == (std::vector<uint32_t>{learnerId}));
    assert_true(learnerState->GetBoundVoters() == bootstrapVoters);
    assert_int_equal(learnerApplication->LastAppliedIndex, jointImage.Index);
}
void test_history_validates_joint_stable_chain_and_independent_rollback(void**) {
    auto initial = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    auto prefix = initial.Observe(1, Ordinary(1));
    auto joint = Joint(); joint.Term = 2; joint.PreviousIndex = joint.PreviousTerm = 0;
    auto pending = prefix.Advance(2, EncodeConfigurationEntry(joint));
    assert_true(pending.IsJoint());
    assert_true(pending.OldVoters() == joint.OldVoters); assert_true(pending.NewVoters() == joint.NewVoters);
    assert_int_equal(pending.ConfigurationIndex(), 2); assert_int_equal(pending.ConfigurationTerm(), 2);
    auto scanned = pending.Observe(3, Ordinary(2));
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    auto stable = scanned.Advance(4, EncodeConfigurationEntry(final));
    assert_false(stable.IsJoint()); assert_true(stable.OldVoters() == joint.NewVoters);
    assert_int_equal(stable.ConfigurationIndex(), 4); assert_int_equal(stable.ThroughTerm(), 3);
    // A replacement branch starts from the unchanged prior prefix.
    auto replacement = joint; replacement.Term = 3; replacement.NewVoters = {3, 5, 6};
    auto alternate = prefix.Advance(2, EncodeConfigurationEntry(replacement));
    assert_true(alternate.NewVoters() == replacement.NewVoters);
    assert_true(pending.NewVoters() == joint.NewVoters);
    assert_false(prefix.IsJoint()); assert_int_equal(prefix.ThroughIndex(), 1);
}
void test_configuration_scan_compares_full_configuration_state(void**) {
    auto committed = TConfigurationHistory::Stable({1, 2, 3}, 7, 3, 9, 4);
    auto differentVoters = TConfigurationHistory::Stable({1, 2, 4}, 7, 3, 9, 4);
    TConfigurationScan voterMismatch{committed, differentVoters};
    assert_true(voterMismatch.HasUncommittedConfiguration());

    TSnapshot jointSnapshot{9, 4, {1, 2, 3}, "opaque"};
    jointSnapshot.JointVoters = {3, 4, 5};
    jointSnapshot.ConfigurationIndex = 7; jointSnapshot.ConfigurationTerm = 3;
    auto joint = TConfigurationHistory::FromJointSnapshot(jointSnapshot);
    TConfigurationScan phaseMismatch{committed, joint};
    assert_true(phaseMismatch.HasUncommittedConfiguration());

    TConfigurationScan unchanged{committed, committed};
    assert_false(unchanged.HasUncommittedConfiguration());
}
void test_history_from_joint_snapshot_requires_a_consistent_identity(void**) {
    TSnapshot snapshot{8, 4, {1, 2, 3}, "opaque"};
    snapshot.JointVoters = {3, 4, 5}; snapshot.ConfigurationIndex = 5; snapshot.ConfigurationTerm = 3;
    auto history = TConfigurationHistory::FromJointSnapshot(snapshot);
    assert_true(history.IsJoint()); assert_int_equal(history.ConfigurationIndex(), 5);
    assert_int_equal(history.ConfigurationTerm(), 3); assert_int_equal(history.ThroughIndex(), 8);
    assert_int_equal(history.ThroughTerm(), 4);
    auto final = Joint(); final.Phase = EConfigurationPhase::Stable; final.Term = 4;
    final.PreviousIndex = 5; final.PreviousTerm = 3;
    auto stable = history.Advance(9, EncodeConfigurationEntry(final));
    assert_false(stable.IsJoint()); assert_true(stable.OldVoters() == final.NewVoters);
    auto fixed = snapshot; fixed.JointVoters.clear(); fixed.ConfigurationIndex = fixed.ConfigurationTerm = 0;
    Reject([&] { TConfigurationHistory::FromJointSnapshot(fixed); });
    auto inconsistent = snapshot; inconsistent.ConfigurationIndex = inconsistent.Index;
    Reject([&] { TConfigurationHistory::FromJointSnapshot(inconsistent); });
    Reject([&] { TConfigurationHistory::Stable({1}, 4, 2, 4, 3); });
    Reject([&] { TConfigurationHistory::Stable({1}, 5, 1, 4, 3); });
    Reject([&] { TConfigurationHistory::Stable({1}, 0, 0, 1, 0); });
}
void test_history_rejects_skips_wrong_predecessors_and_term_regression(void**) {
    auto base = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    auto joint = Joint(); joint.Term = 2; joint.PreviousIndex = joint.PreviousTerm = 0;
    auto entry = EncodeConfigurationEntry(joint);
    Reject([&] { base.Advance(2, entry); });
    Reject([&] { base.Observe(2, Ordinary(1)); });
    Reject([&] { base.Observe(1, entry); });
    Reject([&] { base.Advance(1, Ordinary(1)); });
    auto wrong = joint; wrong.OldVoters = {1, 2, 4};
    Reject([&] { base.Advance(1, EncodeConfigurationEntry(wrong)); });
    auto noOp = joint; noOp.NewVoters = noOp.OldVoters;
    Reject([&] { base.Advance(1, EncodeConfigurationEntry(noOp)); });
    auto prefix = base.Observe(1, Ordinary(3));
    Reject([&] { prefix.Advance(2, entry); });
    Reject([&] { prefix.Observe(2, Ordinary(2)); });
    auto valid = joint; valid.Term = 3;
    auto pending = prefix.Advance(2, EncodeConfigurationEntry(valid));
    wrong = valid; wrong.Phase = EConfigurationPhase::Stable; wrong.PreviousIndex = 2; wrong.PreviousTerm = 3;
    wrong.NewVoters = {3, 4, 6};
    Reject([&] { pending.Advance(3, EncodeConfigurationEntry(wrong)); });
    wrong = valid; wrong.PreviousIndex = 2; wrong.PreviousTerm = 3;
    Reject([&] { pending.Advance(3, EncodeConfigurationEntry(wrong)); });
    auto corrupt = EncodeConfigurationEntry(valid); corrupt->Data[0] ^= 1;
    Reject([&] { prefix.Advance(2, corrupt); });
    auto exhausted = TConfigurationHistory::Stable({1}, 0, 0, uint64_t(INT64_MAX), 1);
    Reject([&] { exhausted.Observe(UINT64_MAX, Ordinary(1)); });
    Reject([&] { exhausted.Advance(UINT64_MAX, entry); });
    assert_false(prefix.IsJoint()); assert_int_equal(prefix.ThroughIndex(), 1);
    assert_true(pending.IsJoint()); assert_int_equal(pending.ThroughIndex(), 2);
}
void test_scan_requires_each_configuration_predecessor_to_be_committed(void**) {
    TState state; state.CurrentTerm = 4;
    state.Append(Ordinary(1));
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    state.Append(EncodeConfigurationEntry(joint));
    const TConfigurationRecord stable{EConfigurationPhase::Stable, 3, 2, 2, {1}, {1, 2}};
    state.Append(EncodeConfigurationEntry(stable));
    const TConfigurationRecord nextJoint{EConfigurationPhase::Joint, 4, 3, 3, {1, 2}, {2, 3}};
    state.Append(EncodeConfigurationEntry(nextJoint));

    const auto baseline = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
    // The first joint entry is committed, but its stable successor is not.
    // The next joint transition must wait for that stable entry to commit.
    Reject([&] { ScanConfigurationHistory(state, baseline, 2); });

    const auto scan = ScanConfigurationHistory(state, baseline, 3);
    assert_false(scan.Committed.IsJoint());
    assert_true(scan.Committed.OldVoters() == stable.NewVoters);
    assert_true(scan.Latest.IsJoint());
    assert_true(scan.Latest.OldVoters() == nextJoint.OldVoters);
    assert_true(scan.Latest.NewVoters() == nextJoint.NewVoters);
    assert_true(scan.HasUncommittedConfiguration());
}
struct TCountingState : TState {
    mutable unsigned Gets = 0;
    TMessageHolder<TLogEntry> Get(int64_t index) const override { ++Gets; return TState::Get(index); }
};
struct TMismatchedMetadataState : TState {
    uint64_t RetainedLogTerm(int64_t index) const override { return TState::RetainedLogTerm(index) + 1; }
};
void test_scan_separates_commit_and_replaced_suffix(void**) {
    TCountingState state; state.CurrentTerm = 3;
    auto large = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1024 * 1024); large->Term = 1;
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    state.Append(large); state.Append(EncodeConfigurationEntry(joint));
    state.Append(Ordinary(2)); state.Append(EncodeConfigurationEntry(final));
    auto base = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
    auto scan = ScanConfigurationHistory(state, base, 2);
    assert_true(scan.Committed.IsJoint()); assert_int_equal(scan.Committed.ThroughIndex(), 2);
    assert_false(scan.Latest.IsJoint()); assert_true(scan.Latest.OldVoters() == final.NewVoters);
    assert_true(scan.HasUncommittedConfiguration()); assert_int_equal(state.Gets, 2);
    const auto committedImage = scan.Committed.MakeSnapshot("committed image");
    assert_true(committedImage.IsJoint()); assert_int_equal(committedImage.Index, 2);
    assert_int_equal(committedImage.ConfigurationIndex, 2);
    scan = ScanConfigurationHistory(state, base, 4);
    assert_false(scan.HasUncommittedConfiguration()); assert_int_equal(scan.Committed.ThroughIndex(), 4);
    state.RemoveLast();
    scan = ScanConfigurationHistory(state, base, 2);
    assert_true(scan.Latest.IsJoint()); assert_false(scan.HasUncommittedConfiguration());
    state.Append(EncodeConfigurationEntry(final));
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    Reject([&] { base.ObserveMetadata(1, 1, TLogEntry::EConfiguration); });
    Reject([&] { base.ObserveMetadata(2, 1, 0); });
    assert_int_equal(base.ObserveMetadata(1, 1, TLogEntry::EStub).ThroughIndex(), 1);

    TState unknownFlags;
    unknownFlags.CurrentTerm = 2;
    auto unknown = Ordinary(1);
    unknown->Flags = uint64_t(1) << 40;
    unknownFlags.Append(std::move(unknown));
    Reject([&] { ScanConfigurationHistory(unknownFlags, base, 0); });
}
void test_scan_from_joint_snapshot_and_rejects_mismatched_baselines(void**) {
    TSnapshot snapshot{2, 2, {1, 2}, "image"};
    snapshot.JointVoters = {2, 3}; snapshot.ConfigurationIndex = 2; snapshot.ConfigurationTerm = 2;
    // The original binding is retained while the snapshot describes an active
    // configuration reached by an earlier committed transition.
    TState state; state.BindVoters({7, 8}); state.CurrentTerm = 2;
    state.InstallSnapshot(snapshot); state.CurrentTerm = 3; state.Append(Ordinary(2));
    TConfigurationRecord final{EConfigurationPhase::Stable, 3, 2, 2, {1, 2}, {2, 3}};
    state.Append(EncodeConfigurationEntry(final));
    auto base = TConfigurationHistory::FromJointSnapshot(snapshot);
    auto scan = ScanConfigurationHistory(state, base, 2);
    assert_true(scan.Committed.IsJoint()); assert_false(scan.Latest.IsJoint());
    assert_true(scan.Latest.OldVoters() == final.NewVoters); assert_true(scan.HasUncommittedConfiguration());
    assert_false(ScanConfigurationHistory(state, base, 4).HasUncommittedConfiguration());
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    Reject([&] { ScanConfigurationHistory(state, base, 5); });
    Reject([&] { ScanConfigurationHistory(state, TConfigurationHistory::Stable({1, 2}, 0, 0, 2, 2), 2); });
    auto wrong = snapshot; wrong.JointVoters = {2, 4};
    Reject([&] { ScanConfigurationHistory(state, TConfigurationHistory::FromJointSnapshot(wrong), 2); });
    state.BoundVoters = {1, 2, 4};
    assert_true(ScanConfigurationHistory(state, base, 2).Committed.IsJoint());
    state.BoundVoters = {7, 8};
    state.LastLogTerm = 2;
    Reject([&] { ScanConfigurationHistory(state, base, 2); });
}
void test_scan_from_stable_configuration_snapshot(void**) {
    TSnapshot snapshot{5, 2, {1, 2}, "image"}; snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 4; snapshot.ConfigurationTerm = 2;
    auto decoded = DecodeSnapshot(EncodeSnapshot(snapshot));
    auto baseline = TConfigurationHistory::FromStableSnapshot(decoded);
    assert_false(baseline.IsJoint()); assert_int_equal(baseline.ConfigurationIndex(), 4);
    TState state; state.BindVoters({7, 8});
    state.CurrentTerm = 2; state.InstallSnapshot(decoded);
    state.CurrentTerm = 3;
    TConfigurationRecord joint{EConfigurationPhase::Joint, 3, 4, 2, {1, 2}, {2, 3}};
    state.Append(EncodeConfigurationEntry(joint));
    auto scan = ScanConfigurationHistory(state, baseline, 5);
    assert_false(scan.Committed.IsJoint()); assert_true(scan.Latest.IsJoint());
    assert_true(scan.HasUncommittedConfiguration());
    auto wrong = TConfigurationHistory::Stable({1, 2}, 0, 0, 5, 2);
    Reject([&] { ScanConfigurationHistory(state, wrong, 5); });
    wrong = TConfigurationHistory::Stable({1, 2}, 4, 1, 5, 2);
    Reject([&] { ScanConfigurationHistory(state, wrong, 5); });
    Reject([&] { TConfigurationHistory::FromJointSnapshot(decoded); });
    Reject([&] { TConfigurationHistory::FromStableSnapshot(TSnapshot{5, 2, {1, 2}, "legacy"}); });
    Reject([&] { state.InstallSnapshot(snapshot); });
    auto wrongPredecessor = joint; wrongPredecessor.PreviousIndex = 3;
    state.RemoveLast(); state.Append(EncodeConfigurationEntry(wrongPredecessor));
    Reject([&] { ScanConfigurationHistory(state, baseline, 5); });
}
void test_scan_fails_closed_on_invalid_suffix(void**) {
    TState state; state.CurrentTerm = 3;
    auto base = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
    state.Append(Ordinary(2));
    TConfigurationRecord wrong{EConfigurationPhase::Joint, 3, 1, 2, {1}, {1, 2}};
    state.Append(EncodeConfigurationEntry(wrong));
    const auto fields = static_cast<TStateFields>(state);
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    assert_true(static_cast<TStateFields>(state) == fields); assert_int_equal(base.ThroughIndex(), 0);
    state.Log.back() = Ordinary(1); state.LastLogTerm = 1;
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    state.Log.back() = Ordinary(3); state.LastLogTerm = 3;
    state.BoundVoters = {2};
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    state.BoundVoters.clear(); state.CurrentTerm = 2;
    Reject([&] { ScanConfigurationHistory(state, base, 1); });
    TState corrupt; corrupt.CurrentTerm = 3;
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    auto damaged = EncodeConfigurationEntry(joint); damaged->Data[0] ^= 1;
    corrupt.Append(damaged);
    Reject([&] { ScanConfigurationHistory(corrupt, base, 0); });
    TMismatchedMetadataState mismatch; mismatch.CurrentTerm = 3;
    mismatch.Append(EncodeConfigurationEntry(joint));
    Reject([&] { ScanConfigurationHistory(mismatch, base, 0); });
}
void test_configuration_roundtrips_both_phases_and_bounded_maximum(void**) {
    auto record = Joint();
    for (auto phase : {EConfigurationPhase::Joint, EConfigurationPhase::Stable}) {
        record.Phase = phase;
        auto entry = EncodeConfigurationEntry(record);
        assert_int_equal(entry->Flags, TLogEntry::EConfiguration);
        assert_true(DecodeConfigurationEntry(entry) == record);
    }
    record = {EConfigurationPhase::Joint, 1, 0, 0, {1}, {1, 2}};
    assert_true(DecodeConfigurationEntry(EncodeConfigurationEntry(record)) == record);
    record.OldVoters.clear(); record.NewVoters.clear();
    for (uint32_t id = 1; id <= TSnapshot::MaxVoters; ++id) {
        record.OldVoters.push_back(id); record.NewVoters.push_back(id + TSnapshot::MaxVoters);
    }
    auto max = EncodeConfigurationEntry(record);
    assert_int_equal(max->Len, sizeof(TLogEntry) + 48 + 8 * TSnapshot::MaxVoters);
    assert_true(DecodeConfigurationEntry(max) == record);
}
void test_configuration_rejects_invalid_fields_and_valid_crc_corruption(void**) {
    for (int mode = 0; mode < 12; ++mode) {
        auto record = Joint();
        if (mode == 0) record.Phase = EConfigurationPhase(0);
        if (mode == 1) record.Term = 0;
        if (mode == 2) record.PreviousIndex = UINT64_MAX;
        if (mode == 3) record.PreviousIndex = 0;
        if (mode == 4) record.PreviousTerm = 0;
        if (mode == 5) record.PreviousTerm = 4;
        if (mode == 6) record.OldVoters.clear();
        if (mode == 7) record.NewVoters = {0, 1};
        if (mode == 8) record.NewVoters = {1, 1};
        if (mode == 9) record.NewVoters = {3, 2};
        if (mode == 10) { record.Phase = EConfigurationPhase::Stable; record.PreviousIndex = record.PreviousTerm = 0; }
        if (mode == 11) record.OldVoters.resize(TSnapshot::MaxVoters + 1, 1);
        Reject([&] { EncodeConfigurationEntry(record); });
    }
    auto original = EncodeConfigurationEntry(Joint());
    const auto payload = original->Len - sizeof(TLogEntry);
    for (size_t i = 0; i < payload; ++i) {
        auto bad = Copy(original); bad->Data[i] ^= 1;
        Reject([&] { DecodeConfigurationEntry(bad); });
        bad = Copy(original); bad->Len = sizeof(TLogEntry) + i;
        Reject([&] { DecodeConfigurationEntry(bad); });
    }
    for (size_t offset : {size_t(8), size_t(12), size_t(20), size_t(28), size_t(36), size_t(40), size_t(44), size_t(56)}) {
        auto bad = Copy(original);
        const auto width = offset == 12 || offset == 20 || offset == 28 ? 8 : 4;
        for (size_t i = offset; i < offset + width; ++i) bad->Data[i] = offset >= 44 ? 0 : char(0xff);
        FixChecksum(bad); Reject([&] { DecodeConfigurationEntry(bad); });
    }
    auto bad = Copy(original); bad->Term++; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad->Flags |= TLogEntry::EStub; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad->Len += 1; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad.InitPayload(1); Reject([&] { DecodeConfigurationEntry(bad); });
}
void test_configuration_rpc_and_proposal_are_rejected_before_mutation(void**) {
    auto peer = std::make_shared<TEndpoint>(); auto state = std::make_shared<TState>();
    TRaft follower(state, 2, {{1, peer}});
    auto record = Joint(); record.Term = 2; record.PreviousIndex = record.PreviousTerm = 0;
    auto config = EncodeConfigurationEntry(record);
    auto request = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 2},
        TAppendEntriesRequest{.LeaderId = 1, .Nentries = 1});
    request.InitPayload(1); request.Payload[0] = config;
    const auto before = static_cast<TStateFields>(*state);
    follower.Process({}, request);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(peer->Sent, 0);
    config->Flags |= TLogEntry::EStub; follower.Process({}, request);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(peer->Sent, 0);
    auto normal = NewHoldedMessage<TLogEntry>(); normal->Term = 2; request.Payload[0] = normal;
    follower.Process({}, request); assert_int_equal(state->CurrentTerm, 2); assert_int_equal(state->LastLogIndex, 1);
    assert_int_equal(peer->Sent, 1);
    auto leaderState = std::make_shared<TState>(); TRaft leader(leaderState, 1, {});
    leader.ProcessTimeout({}); assert_int_equal(leader.CurrentStateName(), EState::LEADER);
    const auto leaderBefore = static_cast<TStateFields>(*leaderState);
    auto proposed = EncodeConfigurationEntry(record);
    Reject([&] { leader.Append(proposed); });
    assert_true(static_cast<TStateFields>(*leaderState) == leaderBefore); assert_int_equal(proposed->Term, 2);
    Reject([&] { leader.ProposeJointConfiguration({1, 2}); }); // Volatile stores cannot authorize a live change.
    assert_true(static_cast<TStateFields>(*leaderState) == leaderBefore);
}
void test_follower_can_register_future_voter_endpoint_before_joint_entry(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-peer-provisioning-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto leaderEndpoint = std::make_shared<TEndpoint>();
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 2);
        TRaft follower(state, 2, {{1, leaderEndpoint}});
        TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2}, {1, 2, 3}};
        auto makeRequest = [&] {
            auto request = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 2},
                TAppendEntriesRequest{.LeaderCommit = TAppendEntriesRequest::JointElectionFenceLeader,
                    .LeaderId = 1, .Nentries = 1});
            request.InitPayload(1);
            request.Payload[0] = EncodeConfigurationEntry(joint);
            return request;
        };

        const auto beforeProvisioning = follower.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(beforeProvisioning.MissingPeerTargets == (std::vector<uint32_t>{3}));
        assert_false(beforeProvisioning.Ready());

        const auto beforeMissingEndpoint = static_cast<TStateFields>(*state);
        follower.Process({}, makeRequest());
        assert_true(static_cast<TStateFields>(*state) == beforeMissingEndpoint);
        assert_int_equal(leaderEndpoint->Sent, 0);
        assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2}));

        follower.RegisterPeerTarget(3, std::make_shared<TEndpoint>());
        assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_true(follower.GetLearnerIds() == (std::vector<uint32_t>{3}));
        const auto afterProvisioning = follower.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(afterProvisioning.MissingPeerTargets.empty());
        assert_false(afterProvisioning.Ready()); // A follower cannot propose the transition.
        follower.Process({}, makeRequest());
        assert_int_equal(state->CurrentTerm, 2);
        assert_int_equal(state->LastLogIndex, 1);
        assert_true(follower.IsVoter(3)); // The received joint entry activates both sets.
        assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
        Reject([&] { follower.UnregisterPeerTarget(3); }); // Active joint voters cannot be detached.
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_configuration_readiness_reports_log_and_index_capacity(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-readiness-capacity-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "capacity").string(), 1);
        state->BindVoters({1});
        state->Commit();
        state->Append(Ordinary(1));
        state->CommitIndex = 1;
        state->Commit();
        state->Append(Ordinary(1)); // Fill the proposal slot left by the control reserve.
        TRaft raft(state, 1, {{2, std::make_shared<TEndpoint>()}}, {2},
            TRaft::TLogLimits{.MaxUncommittedBytes = 1024, .MaxUncommittedEntries = 2});
        raft.Become(EState::LEADER);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->MatchIndex[2] = state->LastLogIndex;
        volatileState->CommitAckIndex[2] = state->CommitIndex;
        volatileState->LastSentSeqno[2] = 1;
        volatileState->ReadAckSeqno[2] = 1;

        const auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.IsLeader);
        assert_true(readiness.HasDurableMembershipStorage);
        assert_true(readiness.HasStableConfiguration);
        assert_true(readiness.HasCommittedCurrentTermEntry);
        assert_true(readiness.ChangesVoterSet);
        assert_true(readiness.MissingPeerTargets.empty());
        assert_true(readiness.AddedVotersWithoutCommitAck.empty());
        assert_false(readiness.HasLogCapacity);
        assert_false(readiness.Ready());
    }
    {
        const auto jointEntry = EncodeConfigurationEntry(TConfigurationRecord{
            EConfigurationPhase::Joint, 1, 0, 0, {1}, {1, 2}});
        auto state = std::make_shared<TDurableState>((dir / "byte-capacity").string(), 1);
        state->BindVoters({1});
        state->Commit();
        state->Append(Ordinary(1));
        state->CommitIndex = 1;
        state->Commit();
        const TRaft::TLogLimits limits{
            .MaxUncommittedBytes = sizeof(TLogEntry) + uint64_t(jointEntry->Len) * 2 - 1,
            .MaxUncommittedEntries = 2,
        };
        TRaft raft(state, 1, {{2, std::make_shared<TEndpoint>()}}, {2}, limits);
        raft.Become(EState::LEADER);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->MatchIndex[2] = state->LastLogIndex;
        volatileState->CommitAckIndex[2] = state->CommitIndex;
        volatileState->LastSentSeqno[2] = 1;
        volatileState->ReadAckSeqno[2] = 1;

        const auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.IsLeader);
        assert_true(readiness.HasDurableMembershipStorage);
        assert_true(readiness.HasStableConfiguration);
        assert_true(readiness.HasCommittedCurrentTermEntry);
        assert_true(readiness.ChangesVoterSet);
        assert_true(readiness.MissingPeerTargets.empty());
        assert_true(readiness.AddedVotersWithoutCommitAck.empty());
        assert_false(readiness.HasLogCapacity);
        assert_false(readiness.Ready());
        const auto before = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2}); });
        assert_true(static_cast<TStateFields>(*state) == before);
    }
    {
        auto state = std::make_shared<TDurableState>((dir / "exhausted").string(), 1);
        state->BindVoters({1});
        state->Commit();
        TSnapshot snapshot;
        snapshot.Index = uint64_t(INT64_MAX) - 2;
        snapshot.Term = 1;
        snapshot.Voters = {1};
        state->InstallSnapshot(snapshot);

        TRaft raft(state, 1, {{2, std::make_shared<TEndpoint>()}}, {2});
        raft.Become(EState::LEADER);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->MatchIndex[2] = state->LastLogIndex;
        volatileState->CommitAckIndex[2] = state->CommitIndex;
        volatileState->LastSentSeqno[2] = 1;
        volatileState->ReadAckSeqno[2] = 1;

        const auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.IsLeader);
        assert_true(readiness.HasDurableMembershipStorage);
        assert_true(readiness.HasStableConfiguration);
        assert_true(readiness.HasCommittedCurrentTermEntry);
        assert_true(readiness.ChangesVoterSet);
        assert_true(readiness.MissingPeerTargets.empty());
        assert_true(readiness.AddedVotersWithoutCommitAck.empty());
        assert_false(readiness.HasLogCapacity);
        assert_false(readiness.Ready());
    }
    {
        auto state = std::make_shared<TDurableState>((dir / "reserve-stable").string(), 1);
        state->BindVoters({1});
        state->Commit();
        const auto joint = EncodeConfigurationEntry(TConfigurationRecord{
            EConfigurationPhase::Joint, 1, 0, 0, {1}, {1, 2}});
        const auto stable = EncodeConfigurationEntry(TConfigurationRecord{
            EConfigurationPhase::Stable, 1, 1, 1, {1}, {1, 2}});
        assert_int_equal(joint->Len, stable->Len);
        state->Append(joint);
        state->CommitIndex = state->LastLogIndex;
        state->Commit();

        TRaft raft(state, 1, {{2, std::make_shared<TEndpoint>()}}, {},
            TRaft::TLogLimits{
                .MaxUncommittedBytes = sizeof(TLogEntry) + stable->Len,
                .MaxUncommittedEntries = 2,
            });
        raft.Become(EState::LEADER);

        auto applicationEntry = NewHoldedMessage<TLogEntry>(stable->Len);
        Reject([&] { raft.Append(applicationEntry); }); // Preserve room for the stable successor.
        assert_int_equal(state->LastLogIndex, 1);
        const auto stableIndex = raft.FinalizeConfiguration();
        assert_int_equal(stableIndex, 2);
        assert_true(DecodeConfigurationEntry(state->Get(2)).Phase == EConfigurationPhase::Stable);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_compaction_membership_readiness_reports_acknowledgement_blockers(void**) {
#ifdef __linux__
    const std::vector<uint32_t> originalVoters{1, 2, 3};
    const std::vector<uint32_t> activeVoters{1, 2};
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-compaction-readiness-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
        state->BindVoters(originalVoters);
        state->Commit();
        state->CurrentTerm = 2;
        state->Append(Ordinary(1));
        state->Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 1, 0, 0, originalVoters, activeVoters}));
        state->Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 1, 2, 1, originalVoters, activeVoters}));
        state->CommitIndex = state->LastLogIndex;
        state->Commit();

        TNodeDict peers{{2, std::make_shared<TEndpoint>()}, {3, std::make_shared<TEndpoint>()}};
        TRaft raft(state, 1, peers);
        raft.Become(EState::LEADER);
        raft.SetLocalApplicationSnapshotInstallSupport(true);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->CommitAckIndex[2] = 2;
        volatileState->PeerCapabilities[2].Term = state->CurrentTerm;

        auto readiness = raft.CheckCompactionMembershipReadiness();
        assert_int_equal(readiness.ConfigurationIndex, 3);
        assert_true(readiness.IsLeader);
        assert_true(readiness.IsActiveVoter);
        assert_false(readiness.RequiresLeaderAcknowledgementBarrier);
        assert_true(readiness.ActiveVotersWithoutConfigurationAck == (std::vector<uint32_t>{2}));
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_true(readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());
        std::string compactionError;
        try { (void)raft.Compact(); }
        catch (const std::logic_error& error) { compactionError = error.what(); }
        assert_string_equal(compactionError.c_str(),
            "Compaction requires every active voter to advertise InstallSnapshot support");

        volatileState->PeerCapabilities[2].InstallSnapshot = false;
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        volatileState->PeerCapabilities[2].InstallSnapshot = true;
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport.empty());
        assert_true(readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());
        compactionError.clear();
        try { (void)raft.Compact(); }
        catch (const std::logic_error& error) { compactionError = error.what(); }
        assert_string_equal(compactionError.c_str(),
            "Compaction requires every active voter to support application snapshot installation");
        volatileState->PeerCapabilities[2].ApplicationSnapshotInstall = true;
        volatileState->CommitAckIndex[2] = 3;
        volatileState->PeerCapabilities[2].Term = state->CurrentTerm - 1;
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_true(readiness.ActiveVotersWithoutConfigurationAck.empty());
        assert_false(readiness.Ready());
        compactionError.clear();
        try { (void)raft.Compact(); }
        catch (const std::logic_error& error) { compactionError = error.what(); }
        assert_string_equal(compactionError.c_str(),
            "Compaction requires every active voter to advertise InstallSnapshot support");
        volatileState->PeerCapabilities[2].Term = state->CurrentTerm;
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport.empty());
        assert_true(readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport.empty());
        assert_true(readiness.Ready());

        raft.Become(EState::FOLLOWER);
        volatileState->PeerCapabilities.clear();
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport.empty());
        assert_true(readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport.empty());
        assert_true(readiness.RequiresLeaderAcknowledgementBarrier);
        assert_false(readiness.HasLeaderAcknowledgementBarrier);
        assert_false(readiness.Ready());
        volatileState->CompactionSafeConfigurationIndex = 3;
        volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
        readiness = raft.CheckCompactionMembershipReadiness();
        assert_true(readiness.HasLeaderAcknowledgementBarrier);
        assert_true(readiness.Ready());
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_new_voter_requires_snapshot_install_capability(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-readiness-snapshot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
        state->BindVoters({1});
        state->Commit();
        state->Append(Ordinary(1));
        state->CommitIndex = 1;
        state->Commit();
        auto endpoint = std::make_shared<TEndpoint>();
        TRaft raft(state, 1, {{2, endpoint}}, {2});
        raft.Become(EState::LEADER);
        raft.SetLocalApplicationSnapshotInstallSupport(true);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->MatchIndex[2] = state->LastLogIndex;
        volatileState->CommitAckIndex[2] = state->CommitIndex;
        volatileState->LastSentSeqno[2] = 5;
        volatileState->ReadAckSeqno[2] = 5;

        auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.AddedVotersWithoutCommitAck.empty());
        assert_true(readiness.AddedVotersWithoutSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_true(readiness.ProposedVotersWithoutApplicationSnapshotInstallSupport ==
            (std::vector<uint32_t>{2}));
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());
        const auto before = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2}); });
        assert_true(static_cast<TStateFields>(*state) == before);

        volatileState->PeerCapabilities[2].AppendAckSeqno = 5;
        volatileState->PeerCapabilities[2].Term = state->CurrentTerm;
        volatileState->PeerCapabilities[2].InstallSnapshot = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.AddedVotersWithoutSnapshotInstallSupport.empty());
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2}));
        assert_true(readiness.ProposedVotersWithoutApplicationSnapshotInstallSupport ==
            (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());

        volatileState->PeerCapabilities[2].JointConsensus = true;
        volatileState->PeerCapabilities[2].Term = state->CurrentTerm - 1;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());

        volatileState->PeerCapabilities[2].Term = state->CurrentTerm;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());
        volatileState->PeerCapabilities[2].JointElectionFence = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.VotersWithoutJointConsensusSupport.empty());
        assert_true(readiness.ProposedVotersWithoutApplicationSnapshotInstallSupport == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());

        volatileState->PeerCapabilities[2].ApplicationSnapshotInstall = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.ProposedVotersWithoutApplicationSnapshotInstallSupport.empty());
        assert_true(readiness.Ready());

        // A newer unacknowledged append makes the previous catch-up and
        // capability response stale for promotion readiness.
        volatileState->LastSentSeqno[2] = 6;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.AddedVotersWithoutCommitAck == (std::vector<uint32_t>{2}));
        assert_false(readiness.Ready());
        const auto beforeStaleProposal = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2}); });
        assert_true(static_cast<TStateFields>(*state) == beforeStaleProposal);

        volatileState->ReadAckSeqno[2] = 6;
        volatileState->PeerCapabilities[2].AppendAckSeqno = 6;
        readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.AddedVotersWithoutCommitAck.empty());
        assert_true(readiness.Ready());
        assert_int_equal(raft.ProposeJointConfiguration({1, 2}), 2);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_configuration_requires_every_member_capability(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-readiness-existing-voter-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
        state->BindVoters({1, 3});
        state->Commit();
        state->Append(Ordinary(1));
        state->CommitIndex = 1;
        state->Commit();

        auto joining = std::make_shared<TEndpoint>();
        auto existing = std::make_shared<TEndpoint>();
        TRaft raft(state, 1, {{2, joining}, {3, existing}}, {2});
        raft.Become(EState::LEADER);
        raft.SetLocalIdempotentLogSupport(true);
        raft.SetLocalApplicationSnapshotInstallSupport(true);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->MatchIndex[2] = state->LastLogIndex;
        volatileState->CommitAckIndex[2] = state->CommitIndex;
        volatileState->LastSentSeqno[2] = 1;
        volatileState->ReadAckSeqno[2] = 1;
        volatileState->PeerCapabilities[2] = {
            .AppendAckSeqno = 1,
            .Term = state->CurrentTerm,
            .InstallSnapshot = true,
            .ApplicationSnapshotInstall = true,
            .JointConsensus = true,
            .IdempotentLogEntries = true,
            .IdempotencyControlEntries = true,
        };

        auto readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2, 3}));
        assert_false(readiness.Ready());
        const auto before = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2, 3}); });
        assert_true(static_cast<TStateFields>(*state) == before);

        volatileState->PeerCapabilities[3] = {
            .AppendAckSeqno = 1,
            .Term = state->CurrentTerm,
            .JointConsensus = true,
        };
        readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{2, 3}));
        volatileState->PeerCapabilities[2].JointElectionFence = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.VotersWithoutJointConsensusSupport == (std::vector<uint32_t>{3}));
        volatileState->PeerCapabilities[3].JointElectionFence = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.VotersWithoutJointConsensusSupport.empty());
        assert_true(readiness.MembersWithoutIdempotentLogSupport == (std::vector<uint32_t>{3}));
        assert_true(readiness.MembersWithoutIdempotencyControlSupport == (std::vector<uint32_t>{3}));
        assert_false(readiness.Ready());
        const auto beforeMissingLogSupport = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2, 3}); });
        assert_true(static_cast<TStateFields>(*state) == beforeMissingLogSupport);

        volatileState->PeerCapabilities[3].IdempotentLogEntries = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.MembersWithoutIdempotentLogSupport.empty());
        assert_true(readiness.MembersWithoutIdempotencyControlSupport == (std::vector<uint32_t>{3}));
        assert_false(readiness.Ready());
        const auto beforeMissingControlSupport = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1, 2, 3}); });
        assert_true(static_cast<TStateFields>(*state) == beforeMissingControlSupport);

        volatileState->PeerCapabilities[3].IdempotencyControlEntries = true;
        volatileState->PeerCapabilities[3].ApplicationSnapshotInstall = true;
        readiness = raft.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(readiness.Ready());
        const auto jointIndex = raft.ProposeJointConfiguration({1, 2, 3});
        assert_int_equal(jointIndex, state->LastLogIndex);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_removal_can_progress_without_removed_voter_capability(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-remove-offline-voter-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
        state->BindVoters({1, 2, 3});
        state->CurrentTerm = 2;
        state->Append(Ordinary(2));
        state->CommitIndex = 1;
        state->Commit();

        auto retainedPeer = std::make_shared<TEndpoint>();
        auto removedPeer = std::make_shared<TEndpoint>();
        TRaft raft(state, 1, {{2, retainedPeer}, {3, removedPeer}});
        raft.Become(EState::LEADER);
        raft.SetLocalApplicationSnapshotInstallSupport(true);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->PeerCapabilities[2] = {
            .Term = state->CurrentTerm,
            .ApplicationSnapshotInstall = true,
            .JointConsensus = true,
            .JointElectionFence = true,
        };
        volatileState->MatchIndex[2] = 1;
        volatileState->NextIndex[2] = 2;
        volatileState->CommitAckIndex[2] = 1;

        volatileState->PeerCapabilities[2].JointElectionFence = false;
        const auto insufficientFenceQuorum = raft.CheckJointConfigurationReadiness({1});
        assert_false(insufficientFenceQuorum.HasOldSideJointElectionFenceQuorum);
        assert_true(insufficientFenceQuorum.OldVotersWithoutJointElectionFence ==
            (std::vector<uint32_t>{2, 3}));
        assert_false(insufficientFenceQuorum.Ready());
        const auto beforeInsufficientFenceProposal = static_cast<TStateFields>(*state);
        Reject([&] { raft.ProposeJointConfiguration({1}); });
        assert_true(static_cast<TStateFields>(*state) == beforeInsufficientFenceProposal);

        volatileState->PeerCapabilities[2].JointElectionFence = true;

        const auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.MissingPeerTargets.empty());
        assert_true(readiness.VotersWithoutJointConsensusSupport.empty());
        assert_true(readiness.HasOldSideJointElectionFenceQuorum);
        assert_true(readiness.OldVotersWithoutJointElectionFence == (std::vector<uint32_t>{3}));
        assert_true(readiness.Ready());

        const auto jointIndex = raft.ProposeJointConfiguration({1, 2});
        assert_int_equal(jointIndex, 2);
        raft.ProcessTimeout(ITimeSource::Time{});
        assert_false(retainedPeer->Messages.empty());
        auto request = retainedPeer->Messages.back().Cast<TAppendEntriesRequest>();
        const auto matchIndex = request->PrevLogIndex + request->Nentries;
        assert_int_equal(matchIndex, jointIndex);
        assert_true(request->LeaderCommit & TAppendEntriesRequest::JointElectionFenceLeader);
        raft.Process(ITimeSource::Time{} + std::chrono::milliseconds(1), NewHoldedMessage(
            TMessageEx{.Src = 2, .Dst = 1, .Term = state->CurrentTerm, .Seqno = request->Seqno},
            TAppendEntriesResponse{.MatchIndex = matchIndex, .Success = 1,
                .Padding = TAppendEntriesResponse::SupportsJointConsensus |
                    TAppendEntriesResponse::SupportsApplicationSnapshotInstall}));
        assert_int_equal(raft.GetVolatileState()->CommitIndex, 1);
        assert_int_equal(state->CommitIndex, 1);

        const auto firstAckSeqno = request->Seqno;
        const auto fenceAckTime = ITimeSource::Time{} + std::chrono::seconds(3);
        raft.ProcessTimeout(fenceAckTime);
        request = retainedPeer->Messages.back().Cast<TAppendEntriesRequest>();
        assert_int_equal(request->PrevLogIndex + request->Nentries, jointIndex);
        assert_true(request->Seqno > firstAckSeqno);
        raft.Process(fenceAckTime + std::chrono::milliseconds(1), NewHoldedMessage(
            TMessageEx{.Src = 2, .Dst = 1, .Term = state->CurrentTerm, .Seqno = request->Seqno},
            TAppendEntriesResponse{.MatchIndex = jointIndex, .Success = 1,
                .Padding = TAppendEntriesResponse::SupportsJointConsensus |
                    TAppendEntriesResponse::SupportsJointElectionFence |
                    TAppendEntriesResponse::SupportsApplicationSnapshotInstall}));
        assert_int_equal(raft.GetVolatileState()->MatchIndex.at(2), jointIndex);
        assert_true(raft.GetVolatileState()->PeerCapabilities.at(2).JointElectionFence);
        assert_int_equal(raft.GetVolatileState()->CommitIndex, jointIndex);
        assert_int_equal(state->CommitIndex, jointIndex);
        assert_false(removedPeer->Messages.empty()); // The peer gets the entry but never acknowledges it.
        assert_int_equal(removedPeer->Messages.back().Cast<TAppendEntriesRequest>()->Dst, 3);

        const auto stableIndex = raft.FinalizeConfiguration();
        assert_int_equal(stableIndex, 3);
        const auto later = ITimeSource::Time{} + std::chrono::seconds(10);
        raft.ProcessTimeout(later);
        request = retainedPeer->Messages.back().Cast<TAppendEntriesRequest>();
        const auto stableMatchIndex = request->PrevLogIndex + request->Nentries;
        assert_int_equal(stableMatchIndex, stableIndex);
        raft.Process(later + std::chrono::milliseconds(1), NewHoldedMessage(
            TMessageEx{.Src = 2, .Dst = 1, .Term = state->CurrentTerm, .Seqno = request->Seqno},
            TAppendEntriesResponse{.MatchIndex = stableMatchIndex, .Success = 1,
                .Padding = TAppendEntriesResponse::SupportsJointConsensus |
                    TAppendEntriesResponse::SupportsJointElectionFence |
                    TAppendEntriesResponse::SupportsApplicationSnapshotInstall}));
        assert_int_equal(raft.GetVolatileState()->CommitIndex, stableIndex);
        assert_int_equal(state->CommitIndex, stableIndex);
        assert_false(raft.IsVoter(3));
        assert_true(raft.GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_configuration_rejects_legacy_election_requests(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-vote-fence-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDurableState>((dir / "raft").string(), 4);
        state->BindVoters({1, 2, 3});
        auto joint = Joint();
        joint.Term = 1;
        joint.PreviousIndex = joint.PreviousTerm = 0;
        state->Append(EncodeConfigurationEntry(joint));
        state->CommitIndex = 1;
        state->Commit();

        auto oldCandidate = std::make_shared<TEndpoint>();
        TNodeDict peers{{1, oldCandidate}, {2, std::make_shared<TEndpoint>()},
            {3, std::make_shared<TEndpoint>()}, {5, std::make_shared<TEndpoint>()}};
        TRaft raft(state, 4, peers, {4});
        assert_true(raft.GetConfigurationState().Committed.IsJoint());

        // A voter from the old side may campaign during joint consensus only
        // if it understands that elections need both majorities. Old binaries
        // send zero in the reserved padding word.
        auto legacyPreVote = NewHoldedMessage(TMessageEx{
            .Src = 1, .Dst = 4, .Term = 1, .Seqno = 1},
            TPreVoteRequest{.ProspectiveTerm = 2, .LastLogIndex = 1,
                .LastLogTerm = 1, .CandidateId = 1});
        raft.Process({}, legacyPreVote);
        auto legacyVote = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 4, .Term = 2},
            TRequestVoteRequest{.LastLogIndex = 1, .LastLogTerm = 1, .CandidateId = 1});
        raft.Process({}, legacyVote);
        assert_int_equal(state->CurrentTerm, 1);
        assert_int_equal(state->VotedFor, 0);
        assert_true(oldCandidate->Messages.empty());

        auto capableVote = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 4, .Term = 2},
            TRequestVoteRequest{.LastLogIndex = 1, .LastLogTerm = 1, .CandidateId = 1,
                .Padding = TRequestVoteRequest::JointConsensusCapable});
        raft.Process({}, capableVote);
        assert_int_equal(state->CurrentTerm, 2);
        assert_int_equal(state->VotedFor, 1);
        assert_false(oldCandidate->Messages.empty());
        assert_true(oldCandidate->Messages.back().Cast<TRequestVoteResponse>()->VoteGranted);

        auto legacyStable = TConfigurationRecord{EConfigurationPhase::Stable, 2, 1, 1,
            {1, 2, 3}, {3, 4, 5}};
        auto legacyStableEntry = EncodeConfigurationEntry(legacyStable);
        auto legacyAppend = NewHoldedMessage(
            TMessageEx{.Src = 3, .Dst = 4, .Term = 2, .Seqno = 1},
            TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 1, .LeaderCommit = 1,
                .LeaderId = 3, .Nentries = 1});
        legacyAppend.InitPayload(1);
        legacyAppend.Payload[0] = legacyStableEntry;
        raft.Process({}, legacyAppend);
        assert_int_equal(state->LastLogIndex, 1);

        auto capableAppend = NewHoldedMessage(
            TMessageEx{.Src = 3, .Dst = 4, .Term = 2, .Seqno = 2},
            TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 1,
                .LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
                .LeaderId = 3, .Nentries = 1});
        capableAppend.InitPayload(1);
        capableAppend.Payload[0] = legacyStableEntry;
        raft.Process({}, capableAppend);
        assert_int_equal(state->LastLogIndex, 2);

        auto legacySnapshot = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + 1);
        legacySnapshot->Src = 3;
        legacySnapshot->Dst = 4;
        legacySnapshot->Term = 3;
        legacySnapshot->Seqno = 3;
        legacySnapshot->SnapshotIndex = 1;
        legacySnapshot->SnapshotTerm = 1;
        legacySnapshot->TotalBytes = 44;
        legacySnapshot->LeaderId = 3;
        legacySnapshot->Done = TInstallSnapshotRequest::DoneFlag;
        legacySnapshot->Data[0] = 'x';
        raft.Process({}, legacySnapshot);
        assert_int_equal(state->CurrentTerm, 2);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}

void test_versioned_disk_state_can_start_voter_removal(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-disk-membership-removal-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto state = std::make_shared<TDiskState>((dir / "raft").string(), 1);
        state->BindVoters({1, 2, 3});
        state->CurrentTerm = 2;
        state->Append(Ordinary(2));
        state->CommitIndex = 1;
        state->Commit();

        auto peer2 = std::make_shared<TEndpoint>();
        auto peer3 = std::make_shared<TEndpoint>();
        TRaft raft(state, 1, {{2, peer2}, {3, peer3}});
        raft.Become(EState::LEADER);
        raft.SetLocalApplicationSnapshotInstallSupport(true);
        auto* volatileState = const_cast<TVolatileState*>(raft.GetVolatileState());
        volatileState->PeerCapabilities[2] = {
            .Term = state->CurrentTerm,
            .ApplicationSnapshotInstall = true,
            .JointConsensus = true,
            .JointElectionFence = true,
        };
        volatileState->PeerCapabilities[3] = {
            .Term = state->CurrentTerm,
            .JointConsensus = true,
            .JointElectionFence = true,
        };

        const auto readiness = raft.CheckJointConfigurationReadiness({1, 2});
        assert_true(readiness.HasDurableMembershipStorage);
        assert_true(readiness.Ready());
        const auto jointIndex = raft.ProposeJointConfiguration({1, 2});
        assert_int_equal(jointIndex, 2);
        assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}
void test_new_voter_leader_can_forward_prior_term_joint_entry(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-new-side-joint-forward-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto oldLeader = std::make_shared<TEndpoint>();
    auto sharedVoter = std::make_shared<TEndpoint>();
    auto newLeader = std::make_shared<TEndpoint>();
    auto otherNewVoter = std::make_shared<TEndpoint>();
    auto outsider = std::make_shared<TEndpoint>();
    auto state = std::make_shared<TDurableState>((dir / "raft").string(), 2);
    state->BindVoters({1, 2, 3});
    state->CurrentTerm = 2;
    state->Commit();
    TRaft follower(state, 2, {{1, oldLeader}, {3, sharedVoter}});
    follower.RegisterPeerTarget(4, newLeader);
    follower.RegisterPeerTarget(5, otherNewVoter);
    follower.RegisterPeerTarget(6, outsider);

    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0,
        {1, 2, 3}, {3, 4, 5}};
    auto makeRequest = [&](uint64_t term) {
        auto request = NewHoldedMessage(TMessageEx{.Src = 4, .Dst = 2, .Term = term},
            TAppendEntriesRequest{.LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
                .LeaderId = 4, .Nentries = 1});
        request.InitPayload(1);
        request.Payload[0] = EncodeConfigurationEntry(joint);
        return request;
    };

    // A new-side voter cannot originate this joint entry in its own term.
    const auto beforeSameTerm = static_cast<TStateFields>(*state);
    follower.Process({}, makeRequest(2));
    assert_true(static_cast<TStateFields>(*state) == beforeSameTerm);
    assert_int_equal(newLeader->Sent, 0);

    auto outsiderRequest = NewHoldedMessage(TMessageEx{.Src = 6, .Dst = 2, .Term = 4},
        TAppendEntriesRequest{.LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 6, .Nentries = 1});
    outsiderRequest.InitPayload(1);
    outsiderRequest.Payload[0] = EncodeConfigurationEntry(joint);
    follower.Process({}, std::move(outsiderRequest));
    assert_true(static_cast<TStateFields>(*state) == beforeSameTerm);
    assert_int_equal(outsider->Sent, 0);

    // A new-side learner cannot use a joint entry with the correct old voters
    // but a false predecessor identity to raise the receiver's term.
    const TConfigurationRecord unrelatedJoint{EConfigurationPhase::Joint, 2, 1, 1,
        {1, 2, 3}, {3, 6, 7}};
    auto unrelatedRequest = NewHoldedMessage(TMessageEx{.Src = 6, .Dst = 2, .Term = 4},
        TAppendEntriesRequest{.PrevLogIndex = 99, .PrevLogTerm = 2,
            .LeaderCommit = 99 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 6, .Nentries = 1});
    unrelatedRequest.InitPayload(1);
    unrelatedRequest.Payload[0] = EncodeConfigurationEntry(unrelatedJoint);
    follower.Process({}, std::move(unrelatedRequest));
    assert_true(static_cast<TStateFields>(*state) == beforeSameTerm);
    assert_int_equal(outsider->Sent, 0);

    // A real new-side leader may replace a conflicting, uncommitted joint
    // suffix. The committed stable predecessor remains the trust anchor.
    const std::vector<uint32_t> replacementNewVoters{3, 6, 7};
    auto pendingState = std::make_shared<TDurableState>((dir / "pending-raft").string(), 2);
    pendingState->BindVoters({1, 2, 3});
    pendingState->CurrentTerm = 2;
    pendingState->Append(EncodeConfigurationEntry(TConfigurationRecord{
        EConfigurationPhase::Joint, 2, 0, 0, {1, 2, 3}, {3, 4, 5}}));
    pendingState->Commit();
    TNodeDict pendingPeers{{1, std::make_shared<TEndpoint>()}, {3, std::make_shared<TEndpoint>()},
        {4, std::make_shared<TEndpoint>()}, {5, std::make_shared<TEndpoint>()}};
    TRaft pendingFollower(pendingState, 2, pendingPeers);
    for (auto id : {6u, 7u})
        pendingFollower.RegisterPeerTarget(id, std::make_shared<TEndpoint>());
    const TConfigurationRecord replacementJoint{EConfigurationPhase::Joint, 3, 0, 0,
        {1, 2, 3}, replacementNewVoters};
    auto replacementRequest = NewHoldedMessage(TMessageEx{.Src = 6, .Dst = 2, .Term = 4},
        TAppendEntriesRequest{.LeaderCommit = TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 6, .Nentries = 1});
    replacementRequest.InitPayload(1);
    replacementRequest.Payload[0] = EncodeConfigurationEntry(replacementJoint);
    pendingFollower.Process({}, replacementRequest);
    assert_int_equal(pendingState->CurrentTerm, 4);
    assert_int_equal(pendingState->LastLogIndex, 1);
    assert_true(DecodeConfigurationEntry(pendingState->Get(1)) == replacementJoint);
    assert_true(pendingFollower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 6, 7}));

    // After election in a later term, that voter may forward the prior-term
    // entry so a lagging new voter can learn the joint configuration.
    follower.Process({}, makeRequest(3));
    assert_int_equal(state->CurrentTerm, 3);
    assert_int_equal(state->LastLogIndex, 1);
    assert_int_equal(state->CommitIndex, 1);
    assert_true(follower.IsVoter(4));
    assert_true(follower.IsVoter(5));
    assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));

    // A leader still in the old set can relay a stable successor created by a
    // new-set leader in an earlier term, letting the transfer target catch up.
    const TConfigurationRecord stable{EConfigurationPhase::Stable, 2, 1, 2,
        {1, 2, 3}, {3, 4, 5}};
    auto stableForward = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 2,
            .LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 1, .Nentries = 1});
    stableForward.InitPayload(1);
    stableForward.Payload[0] = EncodeConfigurationEntry(stable);
    follower.Process({}, std::move(stableForward));
    assert_int_equal(state->LastLogIndex, 2);
    assert_int_equal(state->CommitIndex, 1); // The successor remains pending until the joint quorum commits it.
    assert_true(DecodeConfigurationEntry(state->Get(2)) == stable);
    assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));

    // But an old-side leader still cannot originate a stable successor.
    auto sameTermStable = stable;
    sameTermStable.Term = 3;
    auto invalidStable = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 2,
            .LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 1, .Nentries = 1});
    invalidStable.InitPayload(1);
    invalidStable.Payload[0] = EncodeConfigurationEntry(sameTermStable);
    const auto beforeInvalidStable = static_cast<TStateFields>(*state);
    const auto oldLeaderReplies = oldLeader->Sent;
    follower.Process({}, std::move(invalidStable));
    assert_true(static_cast<TStateFields>(*state) == beforeInvalidStable);
    assert_true(DecodeConfigurationEntry(state->Get(2)) == stable);
    assert_int_equal(oldLeader->Sent, oldLeaderReplies);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_new_side_election_replays_joint_entry_to_old_peer(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-election-replay-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> original{1, 2, 3};
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0,
        original, {3, 4, 5}};
    constexpr uint64_t jointIndex = 1;

    auto leaderState = std::make_shared<TDurableState>((dir / "leader").string(), 4);
    leaderState->BindVoters(original);
    leaderState->CurrentTerm = 2;
    leaderState->Append(EncodeConfigurationEntry(joint));
    leaderState->CommitIndex = 1;
    leaderState->Commit();

    auto toOne = std::make_shared<TEndpoint>();
    auto toTwo = std::make_shared<TEndpoint>();
    auto toThree = std::make_shared<TEndpoint>();
    auto toFive = std::make_shared<TEndpoint>();
    auto leader = std::make_shared<TRaft>(leaderState, 4,
        TNodeDict{{1, toOne}, {2, toTwo}, {3, toThree}, {5, toFive}});

    auto followerState = std::make_shared<TDurableState>((dir / "follower").string(), 2);
    followerState->BindVoters(original);
    followerState->CurrentTerm = 2;
    auto followerToOne = std::make_shared<TEndpoint>();
    auto followerToThree = std::make_shared<TEndpoint>();
    auto followerToLeader = std::make_shared<TEndpoint>();
    TRaft follower(followerState, 2, {{1, followerToOne}, {3, followerToThree}});
    follower.RegisterPeerTarget(4, followerToLeader);
    follower.RegisterPeerTarget(5, std::make_shared<TEndpoint>());

    ITimeSource::Time now = ITimeSource::Time{} + std::chrono::seconds(20);
    unsigned appendRequests = 0;
    uint64_t appendPrevIndex = UINT64_MAX;
    uint64_t appendTerm = 0;
    uint32_t appendPayloadSize = 0;
    uint64_t appendLeaderCommit = 0;
    uint32_t appendLeaderId = 0;
    uint32_t appendEntryCount = 0;
    uint64_t forwardedConfigurationTerm = 0;
    bool replayedJoint = false;
    toTwo->Route = [&](auto message) {
        if (message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
            const auto append = message.template Cast<TAppendEntriesRequest>();
            ++appendRequests;
            appendPrevIndex = append->PrevLogIndex;
            appendTerm = append->Term;
            appendPayloadSize = append.PayloadSize;
            appendLeaderCommit = append->LeaderCommit;
            appendLeaderId = append->LeaderId;
            appendEntryCount = append->Nentries;
            for (uint32_t i = 0; i < append.PayloadSize; ++i) {
                const auto entry = append.Payload[i].template Cast<TLogEntry>();
                if (entry->Flags & TLogEntry::EConfiguration)
                    forwardedConfigurationTerm = DecodeConfigurationEntry(entry).Term;
                replayedJoint = replayedJoint ||
                    bool(entry->Flags & TLogEntry::EConfiguration);
            }
        }
        follower.Process(now, std::move(message));
    };
    unsigned followerReplies = 0;
    followerToLeader->Route = [&](auto message) {
        ++followerReplies;
        leader->Process(now, std::move(message));
    };

    const_cast<TVolatileState*>(leader->GetVolatileState())->SetElectionDue(now);
    leader->ProcessTimeout(now);
    assert_true(leader->CurrentStateName() == EState::CANDIDATE);
    auto grantPreVote = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto request = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::PRE_VOTE_REQUEST);
        });
        assert_true(request != endpoint->Messages.rend());
        const auto preVote = request->Cast<TPreVoteRequest>();
        leader->Process(now,
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 4, .Term = preVote->Term,
                    .Seqno = preVote->Seqno},
                TPreVoteResponse{.ProspectiveTerm = preVote->ProspectiveTerm, .VoteGranted = 1}));
    };
    grantPreVote(1, toOne);
    grantPreVote(3, toThree); // Together with self 4, this forms both joint pre-vote majorities.
    leader->ProcessTimeout(now); // Pre-vote quorum starts the durable election term.
    auto grantVote = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto request = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::REQUEST_VOTE_REQUEST);
        });
        assert_true(request != endpoint->Messages.rend());
        const auto ex = request->Cast<TMessageEx>();
        leader->Process(now + std::chrono::milliseconds(1),
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 4, .Term = ex->Term, .Seqno = ex->Seqno},
                TRequestVoteResponse{.VoteGranted = 1}));
    };
    grantVote(1, toOne);
    grantVote(3, toThree); // Together with self 4, this forms both joint majorities.
    now += std::chrono::milliseconds(2);
    leader->ProcessTimeout(now);
    assert_true(leader->CurrentStateName() == EState::LEADER);

    // The first replication request must include the prior-term joint record;
    // an empty/no-op-only probe would be rejected by this old-config peer.
    assert_int_equal(appendRequests, 1);
    assert_true(replayedJoint);
    assert_int_equal(appendPrevIndex, 0);
    assert_int_equal(appendTerm, 3);
    assert_int_equal(appendPayloadSize, appendEntryCount);
    assert_int_equal(appendLeaderCommit & ~TAppendEntriesRequest::JointElectionFenceLeader, 1);
    assert_true(appendLeaderCommit & TAppendEntriesRequest::JointElectionFenceLeader);
    assert_int_equal(appendLeaderId, 4);
    assert_int_equal(forwardedConfigurationTerm, 2);
    assert_true(appendEntryCount >= 1);
    assert_int_equal(followerReplies, 1);
    assert_true(followerState->LastLogIndex >= jointIndex);
    assert_true(follower.IsVoter(4));
    assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));
    assert_int_equal(followerState->CurrentTerm, leaderState->CurrentTerm);
    assert_int_equal(followerState->CommitIndex, jointIndex);
    assert_true(leader->GetVolatileState()->MatchIndex.at(2) >= jointIndex);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_new_stable_leader_replays_transition_to_lagging_joiner(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-stable-election-replay-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> active{3, 4, 5};
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, original, active};
    const TConfigurationRecord stable{EConfigurationPhase::Stable, 3, 1, 2, original, active};

    auto leaderState = std::make_shared<TDurableState>((dir / "leader").string(), 4);
    leaderState->BindVoters(original);
    leaderState->CurrentTerm = 3;
    leaderState->Append(EncodeConfigurationEntry(joint));
    leaderState->Append(EncodeConfigurationEntry(stable));
    leaderState->CommitIndex = 2;
    leaderState->Commit();
    auto toThree = std::make_shared<TEndpoint>();
    auto toFive = std::make_shared<TEndpoint>();
    auto leader = std::make_shared<TRaft>(leaderState, 4, TNodeDict{{3, toThree}, {5, toFive}});

    auto followerState = std::make_shared<TDurableState>((dir / "follower").string(), 5);
    followerState->BindVoters(original);
    followerState->CurrentTerm = 3;
    auto followerToOne = std::make_shared<TEndpoint>();
    auto followerToTwo = std::make_shared<TEndpoint>();
    auto followerToThree = std::make_shared<TEndpoint>();
    auto followerToLeader = std::make_shared<TEndpoint>();
    TRaft follower(followerState, 5,
        {{1, followerToOne}, {2, followerToTwo}, {3, followerToThree}}, {5});
    follower.RegisterPeerTarget(4, followerToLeader);

    ITimeSource::Time now = ITimeSource::Time{} + std::chrono::seconds(30);
    bool replayedJoint = false;
    bool replayedStable = false;
    toFive->Route = [&](auto message) {
        if (message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
            const auto append = message.template Cast<TAppendEntriesRequest>();
            for (uint32_t i = 0; i < append.PayloadSize; ++i) {
                const auto entry = append.Payload[i].template Cast<TLogEntry>();
                if (!(entry->Flags & TLogEntry::EConfiguration)) continue;
                const auto record = DecodeConfigurationEntry(entry);
                replayedJoint = replayedJoint || record.Phase == EConfigurationPhase::Joint;
                replayedStable = replayedStable || record.Phase == EConfigurationPhase::Stable;
            }
        }
        follower.Process(now, std::move(message));
    };
    followerToLeader->Route = [&](auto message) { leader->Process(now, std::move(message)); };

    const_cast<TVolatileState*>(leader->GetVolatileState())->SetElectionDue(now);
    leader->ProcessTimeout(now);
    assert_true(leader->CurrentStateName() == EState::CANDIDATE);
    const auto preVoteRequest = std::find_if(toThree->Messages.rbegin(), toThree->Messages.rend(), [](const auto& item) {
        return item->Type == uint32_t(EMessageType::PRE_VOTE_REQUEST);
    });
    assert_true(preVoteRequest != toThree->Messages.rend());
    const auto preVoteHeader = preVoteRequest->Cast<TPreVoteRequest>();
    leader->Process(now, NewHoldedMessage(
        TMessageEx{.Src = 3, .Dst = 4, .Term = preVoteHeader->Term, .Seqno = preVoteHeader->Seqno},
        TPreVoteResponse{.ProspectiveTerm = preVoteHeader->ProspectiveTerm, .VoteGranted = 1}));
    leader->ProcessTimeout(now); // The stable set's quorum starts the durable election term.
    const auto voteRequest = std::find_if(toThree->Messages.rbegin(), toThree->Messages.rend(), [](const auto& item) {
        return item->Type == uint32_t(EMessageType::REQUEST_VOTE_REQUEST);
    });
    assert_true(voteRequest != toThree->Messages.rend());
    const auto voteHeader = voteRequest->Cast<TMessageEx>();
    leader->Process(now + std::chrono::milliseconds(1),
        NewHoldedMessage(TMessageEx{.Src = 3, .Dst = 4, .Term = voteHeader->Term,
            .Seqno = voteHeader->Seqno}, TRequestVoteResponse{.VoteGranted = 1}));
    now += std::chrono::milliseconds(2);
    leader->ProcessTimeout(now);
    assert_true(leader->CurrentStateName() == EState::LEADER);

    // The stable-only cursor must include its retained predecessor joint entry,
    // otherwise this staged learner cannot recognize the new-side leader. The
    // bounded append batch sends the stable successor in a later round.
    assert_true(replayedJoint);
    assert_false(replayedStable);
    now += std::chrono::seconds(1);
    leader->ProcessTimeout(now);
    assert_true(replayedStable);
    assert_true(follower.IsVoter(5));
    assert_true(follower.GetVoterIds() == active);
    assert_int_equal(followerState->CurrentTerm, leaderState->CurrentTerm);
    assert_int_equal(followerState->CommitIndex, 2);
    assert_true(leader->GetVolatileState()->MatchIndex.at(5) >= 2);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_runtime_peer_provisioning_commits_joint_membership_across_replicas(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-runtime-joint-provisioning-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        auto leaderToJoining = std::make_shared<TEndpoint>();
        auto leaderToFollower = std::make_shared<TEndpoint>();
        auto followerToLeader = std::make_shared<TEndpoint>();
        auto followerToJoining = std::make_shared<TEndpoint>();
        auto joiningToLeader = std::make_shared<TEndpoint>();
        auto joiningToFollower = std::make_shared<TEndpoint>();

        auto leaderState = std::make_shared<TDurableState>((dir / "raft").string(), 1);
        auto followerState = std::make_shared<TDurableState>((dir / "raft").string(), 3);
        auto joiningState = std::make_shared<TDurableState>((dir / "raft").string(), 2);
        TRaft leader(leaderState, 1, {{2, leaderToJoining}, {3, leaderToFollower}}, {2});
        TRaft follower(followerState, 3, {{1, followerToLeader}});
        TRaft joining(joiningState, 2, {{1, joiningToLeader}, {3, joiningToFollower}}, {2});
        leader.SetLocalApplicationSnapshotInstallSupport(true);
        follower.SetLocalApplicationSnapshotInstallSupport(true);
        joining.SetLocalApplicationSnapshotInstallSupport(true);

        ITimeSource::Time rpcNow{};
        auto routeLeaderToJoining = [&](auto message) { joining.Process(rpcNow, std::move(message)); };
        leaderToFollower->Route = [&](auto message) { follower.Process(rpcNow, std::move(message)); };
        followerToLeader->Route = [&](auto message) { leader.Process(rpcNow, std::move(message)); };
        followerToJoining->Route = [&](auto message) { joining.Process(rpcNow, std::move(message)); };
        joiningToLeader->Route = [&](auto message) { leader.Process(rpcNow, std::move(message)); };
        joiningToFollower->Route = [&](auto message) { follower.Process(rpcNow, std::move(message)); };

        const auto initialVoters = follower.GetVoterIds();
        follower.RegisterPeerTarget(2, followerToJoining);
        assert_true(follower.GetVoterIds() == initialVoters);
        assert_true(follower.GetLearnerIds() == (std::vector<uint32_t>{2}));
        assert_int_equal(follower.GetNpeers(), 2);

        const auto start = ITimeSource::Time{};
        auto timeout = [&](std::chrono::seconds offset) {
            rpcNow = start + offset;
            leader.ProcessTimeout(rpcNow);
        };
        rpcNow = start + std::chrono::seconds(20);
        leader.ProcessTimeout(rpcNow); // Start pre-vote for old voters {1, 3}.
        leader.ProcessTimeout(rpcNow); // Pre-vote quorum starts the durable election.
        leader.ProcessTimeout(rpcNow); // Process the synchronous vote and become leader.
        assert_true(leader.CurrentStateName() == EState::LEADER);
        const auto beforeLearnerAck = leader.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(beforeLearnerAck.IsLeader);
        assert_true(beforeLearnerAck.AddedVotersWithoutCommitAck == (std::vector<uint32_t>{2}));
        assert_false(beforeLearnerAck.Ready());
        leaderToJoining->Route = routeLeaderToJoining;
        timeout(std::chrono::seconds(30)); // Replicate and commit the current-term leader entry.
        timeout(std::chrono::seconds(40)); // The joining learner acknowledges the commit boundary.
        assert_true(leader.IsLearnerCommitAcknowledged(2));

        const auto ready = leader.CheckJointConfigurationReadiness({1, 2, 3});
        assert_true(ready.Ready());
        assert_true(ready.HasLogCapacity);
        assert_true(ready.MissingPeerTargets.empty());
        assert_true(ready.AddedVotersWithoutCommitAck.empty());
        assert_true(ready.AddedVotersWithoutSnapshotInstallSupport.empty());

        const auto jointIndex = leader.ProposeJointConfiguration({1, 2, 3});
        timeout(std::chrono::seconds(50));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, jointIndex);
        assert_true(follower.IsVoter(2));
        assert_true(joining.IsVoter(2));
        assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
        assert_true(joining.GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));

        timeout(std::chrono::seconds(60)); // Propagate the committed joint watermark.
        assert_int_equal(followerState->CommitIndex, jointIndex);
        assert_int_equal(joiningState->CommitIndex, jointIndex);
        const auto stableIndex = leader.FinalizeConfiguration();
        timeout(std::chrono::seconds(70));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, stableIndex);
        timeout(std::chrono::seconds(80));
        assert_int_equal(followerState->CommitIndex, stableIndex);
        assert_int_equal(joiningState->CommitIndex, stableIndex);
        for (const auto* raft : {&leader, &follower, &joining})
            assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_removed_member_rejoins_from_fresh_snapshot_after_compaction(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-rejoin-after-compaction-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));

    const std::vector<uint32_t> originalVoters{1, 2, 3};
    auto leaderState = std::make_shared<TDurableState>((dir / "raft").string(), 1);
    leaderState->BindVoters(originalVoters);
    leaderState->CurrentTerm = 2;
    auto history = TConfigurationHistory::Stable(originalVoters, 0, 0, 0, 0);
    leaderState->Append(Ordinary(1));
    history = history.Observe(1, Ordinary(1));
    TConfigurationRecord joint{EConfigurationPhase::Joint, 1, 0, 0, originalVoters, {1}};
    leaderState->Append(EncodeConfigurationEntry(joint));
    history = history.Advance(2, EncodeConfigurationEntry(joint));
    auto stable = joint;
    stable.Phase = EConfigurationPhase::Stable;
    stable.PreviousIndex = 2;
    stable.PreviousTerm = 1;
    leaderState->Append(EncodeConfigurationEntry(stable));
    history = history.Advance(3, EncodeConfigurationEntry(stable));
    leaderState->CommitIndex = 3;
    leaderState->Commit();

    // Model the active one-voter cluster after removing node 2 and compacting
    // the complete membership history into its committed stable snapshot.
    const auto snapshot = history.MakeSnapshot("3");
    leaderState->InstallSnapshot(snapshot);
    auto leader = std::make_shared<TRaft>(leaderState, 1, TNodeDict{});
    auto leaderApp = std::make_shared<TSnapshotRsm>();
    TRequestProcessor leaderProcessor(leader, leaderApp, {});
    auto now = ITimeSource::Time{} + std::chrono::seconds(20);
    leader->ProcessTimeout(now); // The remaining single voter becomes leader.
    assert_true(leader->CurrentStateName() == EState::LEADER);
    leaderProcessor.ProcessCommitted();
    assert_true(leaderState->CommitIndex > snapshot.Index);

    auto sendSnapshot = [&](TRaft& receiver, uint32_t receiverId, uint64_t term,
            const TSnapshot& image, uint32_t senderId) {
        const auto bytes = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
        request->Seqno = 1;
        request->Src = senderId; request->Dst = receiverId; request->Term = term;
        request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
        request->Offset = 0; request->TotalBytes = bytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = senderId;
        memcpy(request->Data, bytes.data(), bytes.size());
        receiver.Process(now, request);
    };

    // A nonempty old disk can accept one stable transition when the snapshot
    // carries the exact prior voter set and joint-entry identity. The removed
    // node becomes a learner while its original durable voter binding remains.
    auto staleState = std::make_shared<TDurableState>((dir / "stale").string(), 2);
    staleState->BindVoters(originalVoters);
    staleState->CurrentTerm = 1;
    staleState->Append(Ordinary(1));
    staleState->CommitIndex = 1;
    staleState->Commit();
    auto staleToLeader = std::make_shared<TEndpoint>();
    auto staleToOther = std::make_shared<TEndpoint>();
    auto staleToUntrusted = std::make_shared<TEndpoint>();
    auto stale = std::make_shared<TRaft>(staleState, 2,
        TNodeDict{{1, staleToLeader}, {3, staleToOther}, {4, staleToUntrusted}}, std::vector<uint32_t>{4});
    auto staleApp = std::make_shared<TSnapshotRsm>();
    TRequestProcessor staleProcessor(stale, staleApp,
        {{1, staleToLeader}, {3, staleToOther}, {4, staleToUntrusted}});
    auto mismatchedLineage = snapshot;
    mismatchedLineage.PreviousVoters = {1, 3};
    mismatchedLineage.ConfigurationTransitions.back().OldVoters = {1, 3};
    sendSnapshot(*stale, 2, leaderState->CurrentTerm, mismatchedLineage, 1);
    assert_int_equal(staleState->CurrentTerm, 1);
    assert_true(staleToLeader->Messages.empty());
    // An endpoint outside both the receiver's trusted predecessor and the incoming active set cannot raise the term.
    sendSnapshot(*stale, 2, leaderState->CurrentTerm, snapshot, 4);
    assert_int_equal(staleState->CurrentTerm, 1);
    assert_true(staleToUntrusted->Messages.empty());
    sendSnapshot(*stale, 2, leaderState->CurrentTerm, snapshot, 1);
    assert_true(*staleState->GetSnapshot() == snapshot);
    assert_true(staleState->GetBoundVoters() == originalVoters);
    assert_true(staleToLeader->Messages.size() == 1);
    assert_true(staleToLeader->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);
    assert_false(stale->IsVoter(2));
    assert_true(stale->GetVoterIds() == (std::vector<uint32_t>{1}));

    // Preserve that old store and replace the retired process with a fresh
    // learner configured from the current {1} voter set. It can bootstrap from
    // the stable snapshot, catch up, and rejoin through a new joint transition.
    auto joiningState = std::make_shared<TDurableState>((dir / "fresh").string(), 2);
    auto joiningToLeader = std::make_shared<TEndpoint>();
    auto joining = std::make_shared<TRaft>(joiningState, 2, TNodeDict{{1, joiningToLeader}},
        std::vector<uint32_t>{2});
    auto joiningApp = std::make_shared<TSnapshotRsm>();
    TRequestProcessor joiningProcessor(joining, joiningApp, {{1, joiningToLeader}});
    sendSnapshot(*joining, 2, leaderState->CurrentTerm, snapshot, 1);
    assert_true(!joiningToLeader->Messages.empty());
    auto installed = joiningToLeader->Messages.back().Cast<TInstallSnapshotResponse>();
    assert_true(installed->Success); assert_true(installed->Installed);
    assert_true(joining->GetVoterIds() == (std::vector<uint32_t>{1}));
    assert_false(joining->IsVoter(2));
    assert_true(joiningState->GetBoundVoters() == (std::vector<uint32_t>{1}));
    assert_true(*joiningState->GetSnapshot() == snapshot);

    auto leaderToJoining = std::make_shared<TEndpoint>();
    leaderToJoining->Route = [&](auto message) {
        joining->Process(now, std::move(message));
        joiningProcessor.ProcessCommitted();
    };
    joiningToLeader->Route = [&](auto message) {
        leader->Process(now, std::move(message));
        leaderProcessor.ProcessCommitted();
    };
    leader->AddLearnerTarget(2, leaderToJoining);
    auto tick = [&] {
        now += std::chrono::seconds(1);
        leader->ProcessTimeout(now);
        leaderProcessor.ProcessCommitted();
        joiningProcessor.ProcessCommitted();
    };
    for (unsigned i = 0; i < 8 && !leader->IsLearnerCommitAcknowledged(2); ++i) tick();
    assert_true(leader->IsLearnerCommitAcknowledged(2));

    const auto jointIndex = leader->ProposeJointConfiguration({1, 2});
    for (unsigned i = 0; i < 8 && leader->GetVolatileState()->CommitIndex < jointIndex; ++i) tick();
    assert_int_equal(leader->GetVolatileState()->CommitIndex, jointIndex);
    for (unsigned i = 0; i < 4 && joiningState->CommitIndex < jointIndex; ++i) tick();
    assert_int_equal(joiningState->CommitIndex, jointIndex);
    assert_true(joining->IsVoter(2));

    const auto stableIndex = leader->FinalizeConfiguration();
    for (unsigned i = 0; i < 8 && leader->GetVolatileState()->CommitIndex < stableIndex; ++i) tick();
    assert_int_equal(leader->GetVolatileState()->CommitIndex, stableIndex);
    for (unsigned i = 0; i < 4 && joiningState->CommitIndex < stableIndex; ++i) tick();
    assert_int_equal(joiningState->CommitIndex, stableIndex);
    assert_true(leader->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    assert_true(joining->GetVoterIds() == (std::vector<uint32_t>{1, 2}));

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_quorum_retires_offline_voter_before_compaction_and_snapshot_rejoin(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-offline-voter-compaction-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> originalVoters{1, 2, 3};
    std::array<std::shared_ptr<TDurableState>, 4> states{};
    std::array<std::shared_ptr<TRaft>, 4> rafts{};
    std::array<std::shared_ptr<TSnapshotRsm>, 4> applications{};
    std::array<std::shared_ptr<TRequestProcessor>, 4> processors{};
    std::array<std::array<std::shared_ptr<TEndpoint>, 4>, 4> endpoints{};
    ITimeSource::Time now{};
    bool offline = false;
    unsigned snapshotRequestsToRetired = 0;
    unsigned snapshotRepliesFromRetired = 0, successfulSnapshotReplies = 0;
    uint64_t lastSnapshotOffset = 0;

    for (uint32_t id = 1; id <= 3; ++id) {
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
        states[id]->BindVoters(originalVoters);
        states[id]->Commit();
        for (uint32_t peer = 1; peer <= 3; ++peer)
            if (peer != id) endpoints[id][peer] = std::make_shared<TEndpoint>();
    }
    for (uint32_t id = 1; id <= 3; ++id) {
        TNodeDict peers;
        for (uint32_t peer = 1; peer <= 3; ++peer)
            if (peer != id) peers.emplace(peer, endpoints[id][peer]);
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers);
        applications[id] = std::make_shared<TSnapshotRsm>();
        processors[id] = std::make_shared<TRequestProcessor>(rafts[id], applications[id], peers);
    }
    for (uint32_t from = 1; from <= 3; ++from) {
        for (uint32_t to = 1; to <= 3; ++to) {
            if (from == to) continue;
            endpoints[from][to]->Route = [&, from, to](auto message) {
                if (offline && (from == 3 || to == 3)) return;
                if (from == 1 && to == 3 && message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST))
                    ++snapshotRequestsToRetired;
                if (from == 3 && to == 1 && message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE)) {
                    ++snapshotRepliesFromRetired;
                    const auto response = message.template Cast<TInstallSnapshotResponse>();
                    if (response->Success) ++successfulSnapshotReplies;
                    lastSnapshotOffset = response->NextOffset;
                }
                rafts[to]->Process(now, std::move(message));
                processors[to]->ProcessCommitted();
            };
        }
    }
    auto tick = [&] {
        now += std::chrono::seconds(1);
        rafts[1]->ProcessTimeout(now);
        for (uint32_t id = 1; id <= 3; ++id) processors[id]->ProcessCommitted();
    };
    auto restartRetired = [&] {
        processors[3].reset();
        rafts[3].reset();
        applications[3].reset();
        states[3].reset();
        states[3] = std::make_shared<TDurableState>((dir / "raft").string(), 3);
        TNodeDict peers{{1, endpoints[3][1]}, {2, endpoints[3][2]}};
        rafts[3] = std::make_shared<TRaft>(states[3], 3, peers);
        applications[3] = std::make_shared<TSnapshotRsm>();
        processors[3] = std::make_shared<TRequestProcessor>(rafts[3], applications[3], peers);
    };

    now += std::chrono::seconds(20);
    rafts[1]->ProcessTimeout(now); // Pre-vote campaign reaches the old voters.
    rafts[1]->ProcessTimeout(now + std::chrono::milliseconds(1)); // Start real election.
    now += std::chrono::milliseconds(2);
    rafts[1]->ProcessTimeout(now); // Voter 2's real grant elects node 1.
    assert_true(rafts[1]->CurrentStateName() == EState::LEADER);
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < 1; ++i) tick();
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, 1);
    offline = true; // Voter 3 advertised compatible joint-consensus support before isolation.

    const auto jointIndex = rafts[1]->ProposeJointConfiguration({1, 2});
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick();
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, jointIndex);
    assert_true(rafts[1]->GetVoterIds() == originalVoters); // Both sets still govern the joint phase.
    assert_true(states[3]->CommitIndex < jointIndex);
    const auto beforeBlockedCompaction = states[1]->SnapshotIndex;
    Reject([&] { rafts[1]->Compact(); }); // The offline active voter cannot be skipped safely.
    assert_int_equal(states[1]->SnapshotIndex, beforeBlockedCompaction);
    restartRetired(); // Recover the stale durable voter before it receives the transition snapshot.
    assert_true(rafts[3]->IsVoter(3));
    assert_true(rafts[3]->GetVoterIds() == originalVoters);

    const auto stableIndex = rafts[1]->FinalizeConfiguration();
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < stableIndex; ++i) tick();
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, stableIndex);
    for (unsigned i = 0; i < 8 && states[2]->CommitIndex < stableIndex; ++i) tick();
    assert_int_equal(states[2]->CommitIndex, stableIndex);
    assert_true(rafts[1]->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    for (unsigned i = 0; i < 8 &&
        rafts[2]->GetVolatileState()->CompactionSafeConfigurationIndex < stableIndex; ++i) tick();
    assert_true(rafts[2]->GetVolatileState()->CompactionSafeConfigurationIndex >= stableIndex);
    assert_true(rafts[1]->GetVolatileState()->CommitAckIndex.at(2) >= stableIndex);

    const auto snapshot = rafts[1]->Compact();
    assert_true(snapshot.StableConfiguration);
    assert_true(snapshot.Voters == (std::vector<uint32_t>{1, 2}));
    assert_int_equal(snapshot.ConfigurationIndex, stableIndex);
    assert_int_equal(states[1]->SnapshotIndex, stableIndex);
    assert_true(rafts[1]->GetVolatileState()->NextIndex.at(3) <= snapshot.Index);

    // The retired process comes back only after compaction. The proven stable
    // transition installs it as a learner while preserving its original
    // durable voter binding; it does not silently restore its old vote.
    offline = false;
    for (unsigned i = 0; i < 12 && states[3]->SnapshotIndex < snapshot.Index; ++i) tick();
    assert_true(snapshotRequestsToRetired > 0);
    assert_true(snapshotRepliesFromRetired > 0);
    assert_true(successfulSnapshotReplies > 0);
    assert_true(lastSnapshotOffset > 0);
    assert_int_equal(states[3]->SnapshotIndex, snapshot.Index);
    assert_false(rafts[3]->IsVoter(3));
    assert_true(rafts[3]->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    assert_true(states[3]->GetBoundVoters() == originalVoters);
    assert_true(*states[3]->GetSnapshot() == snapshot);

    restartRetired(); // The installed membership and learner role must also survive a crash.
    assert_false(rafts[3]->IsVoter(3));
    assert_true(rafts[3]->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    assert_true(states[3]->GetBoundVoters() == originalVoters);
    assert_true(*states[3]->GetSnapshot() == snapshot);
    assert_int_equal(applications[3]->LastAppliedIndex, snapshot.Index);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_removed_member_rejoins_through_fresh_joint_snapshot_bootstrap(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-snapshot-rejoin-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        const std::vector<uint32_t> originalVoters{1, 3};
        TSnapshot jointSnapshot{5, 3, {1}, "5"};
        jointSnapshot.JointVoters = {1, 2};
        jointSnapshot.ConfigurationIndex = 4;
        jointSnapshot.ConfigurationTerm = 2;
        ValidateSnapshot(jointSnapshot);

        // Node 3 was removed before the active {1} -> {1, 2} transition. Its
        // nonempty old store cannot skip the committed configuration history.
        auto staleState = std::make_shared<TDurableState>((dir / "stale").string(), 3);
        staleState->BindVoters(originalVoters);
        staleState->Append(Ordinary(1));
        staleState->CommitIndex = 1;
        staleState->Commit();
        auto staleToLeader = std::make_shared<TEndpoint>();
        auto stale = std::make_shared<TRaft>(staleState, 3, TNodeDict{{1, staleToLeader}});
        auto staleApp = std::make_shared<TSnapshotRsm>();
        TRequestProcessor staleProcessor(stale, staleApp, {{1, staleToLeader}});
        const auto beforeStaleInstall = static_cast<TStateFields>(*staleState);
        const auto jointBytes = EncodeSnapshot(jointSnapshot);
        auto makeInstallRequest = [&](uint32_t receiverId) {
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + jointBytes.size());
            request->Seqno = 1;
            request->Src = 1; request->Dst = receiverId; request->Term = 3;
            request->SnapshotIndex = jointSnapshot.Index; request->SnapshotTerm = jointSnapshot.Term;
            request->Offset = 0; request->TotalBytes = jointBytes.size();
            request->Done = TInstallSnapshotRequest::DoneFlag |
                TInstallSnapshotRequest::JointElectionFenceLeader;
            request->LeaderId = 1;
            memcpy(request->Data, jointBytes.data(), jointBytes.size());
            return request;
        };
        stale->Process({}, makeInstallRequest(3));
        assert_true(static_cast<TStateFields>(*staleState) == beforeStaleInstall);
        assert_true(staleState->GetBoundVoters() == originalVoters);
        assert_true(staleToLeader->Messages.empty());

        auto leaderState = std::make_shared<TDurableState>((dir / "leader").string(), 1);
        leaderState->BindVoters(originalVoters);
        leaderState->CurrentTerm = jointSnapshot.Term;
        leaderState->Commit();
        leaderState->InstallSnapshot(jointSnapshot);
        auto followerState = std::make_shared<TDurableState>((dir / "follower").string(), 2);
        followerState->BindVoters(originalVoters);
        followerState->CurrentTerm = jointSnapshot.Term;
        followerState->Commit();
        followerState->InstallSnapshot(jointSnapshot);

        auto leaderToFollower = std::make_shared<TEndpoint>();
        auto followerToLeader = std::make_shared<TEndpoint>();
        TNodeDict leaderPeers{{2, leaderToFollower}};
        TNodeDict followerPeers{{1, followerToLeader}};
        auto leader = std::make_shared<TRaft>(leaderState, 1, leaderPeers);
        auto follower = std::make_shared<TRaft>(followerState, 2, followerPeers);
        auto leaderApp = std::make_shared<TSnapshotRsm>();
        auto followerApp = std::make_shared<TSnapshotRsm>();
        TRequestProcessor leaderProcessor(leader, leaderApp, leaderPeers);
        TRequestProcessor followerProcessor(follower, followerApp, followerPeers);

        auto freshState = std::make_shared<TDurableState>((dir / "fresh").string(), 3);
        auto freshToLeader = std::make_shared<TEndpoint>();
        auto freshToFollower = std::make_shared<TEndpoint>();
        TNodeDict freshPeers{{1, freshToLeader}, {2, freshToFollower}};
        auto fresh = std::make_shared<TRaft>(freshState, 3, freshPeers, std::vector<uint32_t>{3});
        auto freshApp = std::make_shared<TSnapshotRsm>();
        TRequestProcessor freshProcessor(fresh, freshApp, freshPeers);
        ITimeSource::Time now{};
        leader->Become(EState::LEADER);
        auto leaderToFresh = std::make_shared<TEndpoint>();
        leaderToFresh->Route = [&](auto message) {
            fresh->Process(now, std::move(message));
            freshProcessor.ProcessCommitted();
        };
        leaderToFollower->Route = [&](auto message) {
            follower->Process(now, std::move(message));
            followerProcessor.ProcessCommitted();
        };
        followerToLeader->Route = [&](auto message) {
            leader->Process(now, std::move(message));
            leaderProcessor.ProcessCommitted();
        };
        // Start with the leader's normal AppendEntries probe. The empty learner
        // rejects the compacted prefix; the resulting retry enters InstallSnapshot.
        freshToLeader->Route = [&](auto message) {
            leader->Process(now, std::move(message));
            leaderProcessor.ProcessCommitted();
        };
        leader->AddLearnerTarget(3, leaderToFresh);

        auto tick = [&] {
            now += std::chrono::seconds(10);
            leader->ProcessTimeout(now);
            leaderProcessor.ProcessCommitted();
            followerProcessor.ProcessCommitted();
            freshProcessor.ProcessCommitted();
        };
        for (unsigned i = 0; i < 8 && !freshState->GetSnapshot(); ++i) tick();
        assert_true(bool(freshState->GetSnapshot()));
        assert_true(*freshState->GetSnapshot() == jointSnapshot);
        assert_true(freshState->GetBoundVoters() == (std::vector<uint32_t>{1, 2}));
        assert_true(fresh->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_false(fresh->IsVoter(3));
        assert_int_equal(freshApp->LastAppliedIndex, jointSnapshot.Index);
        for (unsigned i = 0; i < 8 && !leader->IsLearnerCommitAcknowledged(3); ++i) tick();
        assert_true(leader->IsLearnerCommitAcknowledged(3));

        // Complete the in-progress {1} -> {1, 2} transition while the returning
        // node remains a learner, then catch it up to the stable boundary.
        const auto transitionStable = leader->FinalizeConfiguration();
        for (unsigned i = 0; i < 8 && leader->GetVolatileState()->CommitIndex < transitionStable; ++i) tick();
        assert_int_equal(leader->GetVolatileState()->CommitIndex, transitionStable);
        for (unsigned i = 0; i < 4 && (followerState->CommitIndex < transitionStable ||
                freshState->CommitIndex < transitionStable); ++i) tick();
        assert_int_equal(followerState->CommitIndex, transitionStable);
        assert_int_equal(freshState->CommitIndex, transitionStable);
        assert_true(leader->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_true(fresh->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_false(fresh->IsVoter(3));

        auto followerToFresh = std::make_shared<TEndpoint>();
        followerToFresh->Route = [&](auto message) {
            fresh->Process(now, std::move(message));
            freshProcessor.ProcessCommitted();
        };
        follower->RegisterPeerTarget(3, followerToFresh);
        for (unsigned i = 0; i < 8 && !leader->IsLearnerCommitAcknowledged(3); ++i) tick();
        assert_true(leader->IsLearnerCommitAcknowledged(3));

        // Re-add the removed ID only after snapshot bootstrap and stable catch-up.
        const auto rejoinJoint = leader->ProposeJointConfiguration({1, 2, 3});
        for (unsigned i = 0; i < 8 && leader->GetVolatileState()->CommitIndex < rejoinJoint; ++i) tick();
        assert_int_equal(leader->GetVolatileState()->CommitIndex, rejoinJoint);
        for (unsigned i = 0; i < 4 && (followerState->CommitIndex < rejoinJoint ||
                freshState->CommitIndex < rejoinJoint); ++i) tick();
        assert_int_equal(followerState->CommitIndex, rejoinJoint);
        assert_int_equal(freshState->CommitIndex, rejoinJoint);
        assert_true(fresh->IsVoter(3));

        const auto rejoinStable = leader->FinalizeConfiguration();
        for (unsigned i = 0; i < 8 && leader->GetVolatileState()->CommitIndex < rejoinStable; ++i) tick();
        assert_int_equal(leader->GetVolatileState()->CommitIndex, rejoinStable);
        for (unsigned i = 0; i < 4 && (followerState->CommitIndex < rejoinStable ||
                freshState->CommitIndex < rejoinStable); ++i) tick();
        assert_int_equal(followerState->CommitIndex, rejoinStable);
        assert_int_equal(freshState->CommitIndex, rejoinStable);
        for (const auto* raft : {leader.get(), follower.get(), fresh.get()})
            assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
        assert_true(fresh->IsVoter(3));
        assert_true(fresh->GetLearnerIds().empty());
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_fresh_joint_snapshot_rejoin_recovers_voter_role_after_restart(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-rejoin-recovery-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto path = (dir / "raft").string();
    TSnapshot jointSnapshot{5, 3, {1}, "5"};
    jointSnapshot.JointVoters = {1, 2};
    jointSnapshot.ConfigurationIndex = 4;
    jointSnapshot.ConfigurationTerm = 2;

    {
        TDurableState state(path, 3);
        state.BindVoters({1, 2}); // Fresh-store bootstrap binding, before node 3 rejoins.
        state.CurrentTerm = 3;
        state.Commit();
        state.InstallSnapshot(jointSnapshot);

        const TConfigurationRecord transitionStable{
            EConfigurationPhase::Stable, 3, 4, 2, {1}, {1, 2}};
        state.Append(EncodeConfigurationEntry(transitionStable));
        state.CommitIndex = 6;
        state.Commit();

        state.CurrentTerm = 4;
        state.Commit();
        const TConfigurationRecord rejoinJoint{
            EConfigurationPhase::Joint, 4, 6, 3, {1, 2}, {1, 2, 3}};
        state.Append(EncodeConfigurationEntry(rejoinJoint));
        state.CommitIndex = 7;
        state.Commit();

        const TConfigurationRecord rejoinStable{
            EConfigurationPhase::Stable, 4, 7, 4, {1, 2}, {1, 2, 3}};
        state.Append(EncodeConfigurationEntry(rejoinStable));
        state.CommitIndex = 8;
        state.Commit();
    }

    {
        auto state = std::make_shared<TDurableState>(path, 3);
        assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1, 2}));
        assert_true(*state->GetSnapshot() == jointSnapshot);
        assert_int_equal(state->CommitIndex, 8);
        TNodeDict peers{{1, std::make_shared<TEndpoint>()}, {2, std::make_shared<TEndpoint>()}};
        auto baseline = TConfigurationHistory::FromJointSnapshot(*state->GetSnapshot());
        auto scan = ScanCommittedConfigurationHistory(*state, baseline);
        assert_false(scan.HasUncommittedConfiguration());
        assert_true(scan.Committed.OldVoters() == (std::vector<uint32_t>{1, 2, 3}));
        auto raft = std::make_shared<TRaft>(state, 3, peers);
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
        assert_true(raft->IsVoter(3));
        auto application = std::make_shared<TSnapshotRsm>();
        TRequestProcessor processor(raft, application, peers);
        processor.ProcessCommitted();
        assert_int_equal(application->LastAppliedIndex, 8);
        assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1, 2}));
    }

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}

void test_joint_membership_addition_and_finalization_replicate_deterministically(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-live-config-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    {
        auto toLearner = std::make_shared<TEndpoint>();
        auto toLeader = std::make_shared<TEndpoint>();
        auto leaderState = std::make_shared<TDurableState>(name, 1);
        auto learnerState = std::make_shared<TDurableState>(name, 2);
        TRaft leader(leaderState, 1, {{2, toLearner}}, {2});
        TRaft learner(learnerState, 2, {{1, toLeader}}, {2});
        leader.SetLocalApplicationSnapshotInstallSupport(true);
        learner.SetLocalApplicationSnapshotInstallSupport(true);
        const auto now = ITimeSource::Time{};
        auto rpcNow = now;
        toLearner->Route = [&](auto message) { learner.Process(rpcNow, std::move(message)); };
        toLeader->Route = [&](auto message) { leader.Process(rpcNow, std::move(message)); };
        auto leaderTimeout = [&](std::chrono::milliseconds offset) {
            rpcNow = now + offset;
            leader.ProcessTimeout(rpcNow);
        };
        auto learnerTimeout = [&](ITimeSource::Time when) {
            rpcNow = when;
            learner.ProcessTimeout(rpcNow);
        };

        // A well-formed but wrong predecessor must not make the higher term
        // durable or earn an AppendEntries response.
        TConfigurationRecord invalid{EConfigurationPhase::Joint, 2, 0, 0, {2}, {1, 2}};
        auto invalidRequest = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 2},
            TAppendEntriesRequest{.LeaderId = 1, .Nentries = 1});
        invalidRequest.InitPayload(1); invalidRequest.Payload[0] = EncodeConfigurationEntry(invalid);
        const auto beforeInvalidRequest = static_cast<TStateFields>(*learnerState);
        learner.Process(now, invalidRequest);
        assert_true(static_cast<TStateFields>(*learnerState) == beforeInvalidRequest);
        assert_int_equal(toLeader->Sent, 0);

        leader.ProcessTimeout(now);
        assert_true(leader.CurrentStateName() == EState::LEADER);
        leaderTimeout(std::chrono::seconds(10));
        assert_true(leader.IsLearnerCommitAcknowledged(2));

        const auto beforeRejectedProposal = static_cast<TStateFields>(*leaderState);
        Reject([&] { leader.ProposeJointConfiguration({1}); }); // A transition must change the voter set.
        assert_true(static_cast<TStateFields>(*leaderState) == beforeRejectedProposal);
        const auto jointIndex = leader.ProposeJointConfiguration({1, 2});
        assert_int_equal(jointIndex, 2);
        assert_true(leader.IsVoter(2));
        assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{1, 2}));

        leaderTimeout(std::chrono::seconds(20));
        assert_true(learner.IsVoter(2));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, jointIndex);
        assert_int_equal(learnerState->CommitIndex, 1); // The commit watermark needs another RPC.
        leaderTimeout(std::chrono::seconds(30));
        assert_int_equal(learnerState->CommitIndex, jointIndex);

        const auto stableIndex = leader.FinalizeConfiguration();
        assert_int_equal(stableIndex, 3);
        leaderTimeout(std::chrono::seconds(40));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, stableIndex);
        assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        leaderTimeout(std::chrono::seconds(50));
        assert_int_equal(learnerState->CommitIndex, stableIndex);
        assert_true(leader.GetLearnerIds().empty());
        assert_true(learner.IsVoter(2));
        assert_true(learner.GetVoterIds() == (std::vector<uint32_t>{1, 2}));

        // Removing a different voter still requires both old and new
        // majorities until the stable successor commits.
        const auto removalJoint = leader.ProposeJointConfiguration({1});
        assert_int_equal(removalJoint, 4);
        auto membership = leader.GetConfigurationState();
        assert_false(membership.Committed.IsJoint());
        assert_true(membership.Committed.OldVoters() == (std::vector<uint32_t>{1, 2}));
        assert_true(membership.Latest.IsJoint());
        assert_true(membership.Latest.NewVoters() == (std::vector<uint32_t>{1}));
        assert_true(membership.HasUncommittedConfiguration());
        assert_true(leader.IsVoter(2));
        leaderTimeout(std::chrono::seconds(60));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, removalJoint);
        membership = leader.GetConfigurationState();
        assert_true(membership.Committed.IsJoint());
        assert_false(membership.HasUncommittedConfiguration());
        leaderTimeout(std::chrono::seconds(70));
        assert_int_equal(learnerState->CommitIndex, removalJoint);
        const auto removalStable = leader.FinalizeConfiguration();
        assert_int_equal(removalStable, 5);
        membership = leader.GetConfigurationState();
        assert_true(membership.Committed.IsJoint());
        assert_false(membership.Latest.IsJoint());
        assert_true(membership.Latest.OldVoters() == (std::vector<uint32_t>{1}));
        assert_true(membership.HasUncommittedConfiguration());
        assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        leaderTimeout(std::chrono::seconds(80));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, removalStable);
        membership = leader.GetConfigurationState();
        assert_false(membership.Committed.IsJoint());
        assert_false(membership.HasUncommittedConfiguration());
        assert_true(membership.Committed.OldVoters() == (std::vector<uint32_t>{1}));
        assert_false(leader.IsVoter(2));
        leaderTimeout(std::chrono::seconds(90));
        assert_int_equal(learnerState->CommitIndex, removalStable);
        assert_false(leader.IsVoter(2));
        assert_false(learner.IsVoter(2));
        assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{1}));
        assert_true(learner.GetVoterIds() == (std::vector<uint32_t>{1}));

        // Promote the caught-up learner while jointly retaining the old leader,
        // then transfer leadership so the stable successor can remove it.
        const auto leaderRemovalJoint = leader.ProposeJointConfiguration({2});
        assert_int_equal(leaderRemovalJoint, 6);
        leaderTimeout(std::chrono::seconds(100));
        assert_int_equal(leader.GetVolatileState()->CommitIndex, leaderRemovalJoint);
        rpcNow = now + std::chrono::seconds(100);
        Reject([&] { leader.TransferLeadership(2, rpcNow); });
        assert_true(leader.CurrentStateName() == EState::LEADER); // Commit watermark has not reached node 2 yet.
        leaderTimeout(std::chrono::seconds(104));
        assert_int_equal(learnerState->CommitIndex, leaderRemovalJoint);
        Reject([&] { learner.UnregisterPeerTarget(1); }); // Old-set voters remain active throughout joint consensus.
        const auto beforeOldLeaderFinalization = static_cast<TStateFields>(*learnerState);
        const auto jointTerm = leaderState->LogTerm(leaderRemovalJoint);
        auto oldLeaderStable = EncodeConfigurationEntry(TConfigurationRecord{
            EConfigurationPhase::Stable, leaderState->CurrentTerm, leaderRemovalJoint, jointTerm, {1}, {2}});
        auto invalidFinalization = NewHoldedMessage(
            TMessageEx{.Src = 1, .Dst = 2, .Term = leaderState->CurrentTerm, .Seqno = 0},
            TAppendEntriesRequest{
                .PrevLogIndex = leaderRemovalJoint, .PrevLogTerm = jointTerm,
                .LeaderCommit = leaderRemovalJoint, .LeaderId = 1, .Nentries = 1,
            });
        invalidFinalization.InitPayload(1);
        invalidFinalization.Payload[0] = oldLeaderStable;
        learner.Process(rpcNow, invalidFinalization);
        assert_true(static_cast<TStateFields>(*learnerState) == beforeOldLeaderFinalization);

        const auto beforeInvalidTransfer = static_cast<TStateFields>(*learnerState);
        auto invalidTransfer = NewHoldedMessage(
            TMessageEx{.Src = 1, .Dst = 2, .Term = leaderState->CurrentTerm + 1},
            TTimeoutNowRequest{
                .LeaderId = 1, .TargetId = 1, // A transfer to another node cannot trigger this follower.
                .LastLogIndex = leaderState->LastLogIndex, .LastLogTerm = leaderState->LastLogTerm,
                .CommitIndex = leaderState->CommitIndex, .CommitTerm = leaderState->LogTerm(leaderState->CommitIndex),
                .ConfigurationIndex = leaderRemovalJoint, .ConfigurationTerm = leaderState->LogTerm(leaderRemovalJoint),
            });
        learner.Process(rpcNow, invalidTransfer);
        assert_true(static_cast<TStateFields>(*learnerState) == beforeInvalidTransfer);
        assert_true(learner.CurrentStateName() == EState::FOLLOWER);
        rpcNow = now + std::chrono::seconds(105);
        leader.TransferLeadership(2, rpcNow);
        assert_true(leader.CurrentStateName() == EState::FOLLOWER);
        assert_true(learner.CurrentStateName() == EState::CANDIDATE);

        learnerTimeout(now + std::chrono::seconds(105) + std::chrono::milliseconds(1));
        assert_true(learner.CurrentStateName() == EState::LEADER);
        assert_true(leader.CurrentStateName() == EState::FOLLOWER);
        learnerTimeout(now + std::chrono::seconds(106)); // Commit the new leader's current-term entry.
        assert_int_equal(learner.GetVolatileState()->CommitIndex, learnerState->LastLogIndex);
        const auto leaderRemovalStable = learner.FinalizeConfiguration();
        assert_int_equal(leaderRemovalStable, 8);
        learnerTimeout(now + std::chrono::seconds(107));
        learnerTimeout(now + std::chrono::seconds(110));
        assert_false(learner.IsVoter(1)); assert_true(learner.IsVoter(2));
        assert_false(leader.IsVoter(1)); assert_true(leader.IsVoter(2));
        assert_true(learner.GetVoterIds() == (std::vector<uint32_t>{2}));
        assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{2}));
        assert_true(learner.UnregisterPeerTarget(1) == toLeader);
        assert_int_equal(learner.GetNpeers(), 0);
        assert_true(learner.GetVoterIds() == (std::vector<uint32_t>{2}));
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_live_joint_transition_waits_for_both_nontrivial_majorities(void**) {
#ifdef __linux__
    auto run = [](bool newMajorityFirst, bool disjoint) {
        auto dir = std::filesystem::temp_directory_path() / ("miniraft-live-joint-quorum-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            (disjoint ? "-disjoint" : "-overlap") + (newMajorityFirst ? "-new" : "-old"));
        assert_true(std::filesystem::create_directory(dir));
        {
            auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
            std::unordered_map<uint32_t, std::shared_ptr<TEndpoint>> endpoints;
            TNodeDict peers;
            const std::vector<uint32_t> addedVoters = disjoint
                ? std::vector<uint32_t>{4, 5, 6} : std::vector<uint32_t>{4, 5};
            std::vector<uint32_t> peerIds{2, 3};
            peerIds.insert(peerIds.end(), addedVoters.begin(), addedVoters.end());
            for (uint32_t id : peerIds) {
                auto endpoint = std::make_shared<TEndpoint>();
                endpoints.emplace(id, endpoint);
                peers.emplace(id, endpoint);
            }
            TRaft leader(state, 1, peers, addedVoters);
            const auto start = ITimeSource::Time{};

            leader.ProcessTimeout(start);
            const auto prospectiveTerm = state->CurrentTerm + 1;
            leader.Process(start + std::chrono::milliseconds(1),
                    NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = state->CurrentTerm,
                            .Seqno = leader.GetVolatileState()->PreVoteSeqno},
                    TPreVoteResponse{.ProspectiveTerm = prospectiveTerm, .VoteGranted = 1}));
            leader.ProcessTimeout(start + std::chrono::milliseconds(2));
            const auto electionTerm = state->CurrentTerm;
            leader.Process(start + std::chrono::milliseconds(3),
                NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = electionTerm},
                    TRequestVoteResponse{.VoteGranted = 1}));
            leader.ProcessTimeout(start + std::chrono::milliseconds(4));
            assert_true(leader.CurrentStateName() == EState::LEADER);

            auto ackLatestAppend = [&](uint32_t source, std::chrono::milliseconds offset) {
                auto& messages = endpoints.at(source)->Messages;
                const auto request = std::find_if(messages.rbegin(), messages.rend(), [](const auto& message) {
                    return message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
                });
                assert_true(request != messages.rend());
                auto append = request->Cast<TAppendEntriesRequest>();
                const auto matchIndex = append->PrevLogIndex + append->Nentries;
                leader.Process(start + offset,
                    NewHoldedMessage(TMessageEx{.Src = source, .Dst = 1, .Term = append->Term,
                        .Seqno = append->Seqno},
                        TAppendEntriesResponse{.MatchIndex = matchIndex, .Success = 1,
                            .Padding = TAppendEntriesResponse::SupportsInstallSnapshot |
                                TAppendEntriesResponse::SupportsApplicationSnapshotInstall |
                                TAppendEntriesResponse::SupportsJointConsensus |
                                TAppendEntriesResponse::SupportsJointElectionFence}));
                return matchIndex;
            };
            auto latestAppendMatchIndex = [&](uint32_t source) {
                auto& messages = endpoints.at(source)->Messages;
                const auto request = std::find_if(messages.rbegin(), messages.rend(), [](const auto& message) {
                    return message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
                });
                assert_true(request != messages.rend());
                auto append = request->Cast<TAppendEntriesRequest>();
                return append->PrevLogIndex + append->Nentries;
            };

            assert_int_equal(ackLatestAppend(2, std::chrono::milliseconds(3)), 1);
            assert_int_equal(leader.GetVolatileState()->CommitIndex, 1);
            leader.ApproveRead(); // Send the learners a fresh committed watermark.
            for (auto id : addedVoters) {
                assert_int_equal(ackLatestAppend(id, std::chrono::milliseconds(4)), 1);
                assert_true(leader.IsLearnerCommitAcknowledged(id));
            }
            assert_int_equal(ackLatestAppend(3, std::chrono::milliseconds(5)), 1);

            const auto newVoters = disjoint
                ? std::vector<uint32_t>{4, 5, 6} : std::vector<uint32_t>{3, 4, 5};
            const auto jointIndex = leader.ProposeJointConfiguration(newVoters);
            assert_int_equal(jointIndex, 2);
            // Send the joint entry to voter 3 but withhold its ACK. It is
            // shared only in the overlapping case.
            leader.ProcessTimeout(start + std::chrono::seconds(3));
            leader.ProcessTimeout(start + std::chrono::seconds(3) + std::chrono::milliseconds(2));
            assert_int_equal(latestAppendMatchIndex(3), jointIndex);
            for (auto id : std::vector<uint32_t>{2, 4, 5})
                assert_int_equal(latestAppendMatchIndex(id), jointIndex);
            if (disjoint) assert_int_equal(latestAppendMatchIndex(6), jointIndex);

            if (newMajorityFirst) {
                // The new-set majority cannot commit without an old majority.
                ackLatestAppend(4, std::chrono::seconds(3) + std::chrono::milliseconds(4));
                ackLatestAppend(5, std::chrono::seconds(3) + std::chrono::milliseconds(4));
                assert_int_equal(leader.GetVolatileState()->CommitIndex, 1);
                ackLatestAppend(2, std::chrono::seconds(3) + std::chrono::milliseconds(5));
            } else {
                // The old-set majority {1, 2} cannot commit without a new majority.
                ackLatestAppend(2, std::chrono::seconds(3) + std::chrono::milliseconds(4));
                assert_int_equal(leader.GetVolatileState()->CommitIndex, 1);
                ackLatestAppend(4, std::chrono::seconds(3) + std::chrono::milliseconds(4));
                assert_int_equal(leader.GetVolatileState()->CommitIndex, 1);
                ackLatestAppend(5, std::chrono::seconds(3) + std::chrono::milliseconds(5));
            }
            assert_int_equal(leader.GetVolatileState()->CommitIndex, jointIndex);
            assert_true(leader.IsVoter(1));
            assert_true(leader.IsVoter(5));
            assert_true(leader.GetVoterIds() == (disjoint
                ? std::vector<uint32_t>{1, 2, 3, 4, 5, 6}
                : std::vector<uint32_t>{1, 2, 3, 4, 5}));
        }
        std::error_code ignored; std::filesystem::remove_all(dir, ignored);
    };
    run(false, false);
    run(true, false);
    run(false, true);
    run(true, true);
#endif
}
#ifdef __linux__
void run_live_membership_failure_scenario(uint32_t scenario,
        const std::vector<uint32_t>& newVoters,
        const std::vector<uint32_t>& blockedJointReplicas) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-live-membership-faults-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
        std::to_string(scenario));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> oldVoters{1, 2, 3};
    const std::vector<uint32_t> initialLearners{4, 5, 6};
    std::vector<uint32_t> addedVoters;
    for (auto voter : newVoters)
        if (!std::binary_search(oldVoters.begin(), oldVoters.end(), voter))
            addedVoters.push_back(voter);
    std::array<std::shared_ptr<TDurableState>, 7> states{};
    std::array<std::shared_ptr<TRaft>, 7> rafts{};
    std::array<std::shared_ptr<TSnapshotRsm>, 7> applications{};
    std::array<std::shared_ptr<TRequestProcessor>, 7> processors{};
    std::array<std::array<std::shared_ptr<TEndpoint>, 7>, 7> endpoints{};
    ITimeSource::Time now{};
    bool isolateNewMajority = false;
    bool isolateSnapshotReceiver = false;
    bool holdSnapshotChunks = false;
    uint32_t snapshotSenderId = 4;
    bool reorderStableAppendTo6 = false;
    TMessageHolder<TMessage> delayedStableAppend;
    std::vector<TMessageHolder<TMessage>> heldSnapshotChunks;
    bool expectSnapshotRestartRetry = false;
    bool awaitingOutOfOrderSnapshotReply = false;
    uint64_t outOfOrderSnapshotSeqno = 0;
    bool sawOutOfOrderSnapshotRejection = false;
    bool duplicateSnapshotChunkTo6 = false;
    bool holdDuplicateSnapshotReplies = false;
    bool duplicatedSnapshotChunk = false;
    std::vector<TMessageHolder<TMessage>> heldDuplicateSnapshotReplies;

    for (uint32_t id = 1; id <= 6; ++id) {
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
        states[id]->BindVoters(oldVoters);
        states[id]->Commit();
        for (uint32_t peer = 1; peer <= 6; ++peer)
            if (peer != id) endpoints[id][peer] = std::make_shared<TEndpoint>();
    }
    for (uint32_t id = 1; id <= 6; ++id) {
        TNodeDict peers;
        for (uint32_t peer = 1; peer <= 6; ++peer)
            if (peer != id) peers.emplace(peer, endpoints[id][peer]);
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers, initialLearners);
        applications[id] = std::make_shared<TSnapshotRsm>();
        applications[id]->SnapshotPaddingBytes = 2 * TInstallSnapshotRequest::MaxChunkBytes + 17;
        processors[id] = std::make_shared<TRequestProcessor>(rafts[id], applications[id], peers);
    }
    for (uint32_t from = 1; from <= 6; ++from) {
        for (uint32_t to = 1; to <= 6; ++to) {
            if (from == to) continue;
            endpoints[from][to]->Route = [&, from, to](auto message) {
                if (isolateNewMajority && from == 1 &&
                    std::binary_search(blockedJointReplicas.begin(), blockedJointReplicas.end(), to)) return;
                if (isolateSnapshotReceiver && (from == 6 || to == 6)) return;
                if (reorderStableAppendTo6 && from == 4 && to == 6 &&
                    message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
                    reorderStableAppendTo6 = false;
                    delayedStableAppend = std::move(message);
                    return;
                }
                if (expectSnapshotRestartRetry && from == snapshotSenderId && to == 6 &&
                    message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST)) {
                    const auto request = message.template Cast<TInstallSnapshotRequest>();
                    if (request->Offset > 0) {
                        awaitingOutOfOrderSnapshotReply = true;
                        outOfOrderSnapshotSeqno = request->Seqno;
                    }
                }
                if (holdDuplicateSnapshotReplies && from == 6 && to == snapshotSenderId &&
                    message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE)) {
                    heldDuplicateSnapshotReplies.push_back(std::move(message));
                    return;
                }
                if (duplicateSnapshotChunkTo6 && from == snapshotSenderId && to == 6 &&
                    message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST) &&
                    message.template Cast<TInstallSnapshotRequest>()->Offset == 0) {
                    duplicateSnapshotChunkTo6 = false;
                    auto duplicate = message;
                    holdDuplicateSnapshotReplies = true;
                    rafts[to]->Process(now, std::move(message));
                    processors[to]->ProcessCommitted();
                    rafts[to]->Process(now, std::move(duplicate));
                    processors[to]->ProcessCommitted();
                    holdDuplicateSnapshotReplies = false;
                    assert_int_equal(heldDuplicateSnapshotReplies.size(), size_t(2));
                    duplicatedSnapshotChunk = true;
                    while (!heldDuplicateSnapshotReplies.empty()) {
                        auto reply = std::move(heldDuplicateSnapshotReplies.back());
                        heldDuplicateSnapshotReplies.pop_back();
                        endpoints[6][snapshotSenderId]->Route(std::move(reply));
                    }
                    return;
                }
                if (awaitingOutOfOrderSnapshotReply && from == 6 && to == snapshotSenderId &&
                    message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE)) {
                    const auto response = message.template Cast<TInstallSnapshotResponse>();
                    if (response->Seqno == outOfOrderSnapshotSeqno) {
                        sawOutOfOrderSnapshotRejection = !response->Success && response->NextOffset == 0;
                        awaitingOutOfOrderSnapshotReply = false;
                        expectSnapshotRestartRetry = false;
                    }
                }
                if (holdSnapshotChunks && from == snapshotSenderId && to == 6 &&
                    message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST)) {
                    heldSnapshotChunks.push_back(std::move(message));
                    return;
                }
                rafts[to]->Process(now, std::move(message));
                processors[to]->ProcessCommitted();
            };
        }
    }
    auto tick = [&](uint32_t leaderId) {
        now += std::chrono::seconds(1);
        rafts[leaderId]->ProcessTimeout(now);
        processors[leaderId]->ProcessCommitted();
    };

    rafts[1]->ProcessTimeout(now);
    tick(1);
    tick(1);
    assert_true(rafts[1]->CurrentStateName() == EState::LEADER);
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < 1; ++i) tick(1);
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, 1);
    for (unsigned i = 0; i < 8; ++i) {
        bool ready = true;
        for (auto id : addedVoters) ready = ready && rafts[1]->IsLearnerCommitAcknowledged(id);
        if (ready) break;
        tick(1);
    }
    for (auto id : addedVoters) assert_true(rafts[1]->IsLearnerCommitAcknowledged(id));

    isolateNewMajority = true;
    const auto jointIndex = rafts[1]->ProposeJointConfiguration(newVoters);
    for (unsigned i = 0; i < 4 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick(1);
    assert_true(rafts[1]->GetVolatileState()->CommitIndex < jointIndex);
    assert_int_equal(states[2]->CommitIndex, jointIndex - 1);
    assert_int_equal(states[4]->CommitIndex, jointIndex - 1);
    for (uint32_t id = 2; id <= 6; ++id)
        assert_true((rafts[1]->GetVolatileState()->MatchIndex.at(id) >= jointIndex) ==
            !std::binary_search(blockedJointReplicas.begin(), blockedJointReplicas.end(), id));
    assert_true((states[4]->LastLogIndex >= jointIndex) ==
        !std::binary_search(blockedJointReplicas.begin(), blockedJointReplicas.end(), 4));
    isolateNewMajority = false;
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick(1);
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, jointIndex);
    for (unsigned i = 0; i < 8; ++i) {
        bool propagated = true;
        for (uint32_t id = 2; id <= 6; ++id) propagated = propagated && states[id]->CommitIndex >= jointIndex;
        if (propagated) break;
        tick(1);
    }
    auto jointVoters = oldVoters;
    jointVoters.insert(jointVoters.end(), newVoters.begin(), newVoters.end());
    std::sort(jointVoters.begin(), jointVoters.end());
    jointVoters.erase(std::unique(jointVoters.begin(), jointVoters.end()), jointVoters.end());
    for (uint32_t id = 1; id <= 6; ++id) {
        assert_true(rafts[id]->IsVoter(id) ==
            std::binary_search(jointVoters.begin(), jointVoters.end(), id));
        assert_true(rafts[id]->GetVoterIds() == jointVoters);
    }

    // Compact while the committed configuration is joint, then interrupt
    // installation on voter 6 and restart it with endpoints for both voter sets.
    for (unsigned i = 0; i < 8 && !rafts[1]->CheckCompactionMembershipReadiness().Ready(); ++i)
        tick(1);
    assert_true(rafts[1]->CheckCompactionMembershipReadiness().Ready());
    isolateSnapshotReceiver = true;
    const auto jointSnapshotTail = rafts[1]->Append(Ordinary(states[1]->CurrentTerm));
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < jointSnapshotTail; ++i)
        tick(1);
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, jointSnapshotTail);
    assert_int_equal(states[6]->CommitIndex, jointIndex);
    const auto jointSnapshot = rafts[1]->Compact();
    const auto jointSnapshotIndex = jointSnapshot.Index;
    assert_true(jointSnapshot.IsJoint());
    assert_false(jointSnapshot.StableConfiguration);
    assert_true(jointSnapshot.Voters == oldVoters);
    assert_true(jointSnapshot.JointVoters == newVoters);
    assert_int_equal(jointSnapshot.ConfigurationIndex, jointIndex);
    assert_true(jointSnapshotIndex > jointIndex);

    // Give the replacement leader its own copy of the same joint snapshot.
    for (unsigned i = 0; i < 8 && states[4]->CommitIndex < jointSnapshotTail; ++i) tick(1);
    assert_int_equal(states[4]->CommitIndex, jointSnapshotTail);
    for (unsigned i = 0; i < 8 && !rafts[4]->CheckCompactionMembershipReadiness().Ready(); ++i)
        tick(1);
    assert_true(rafts[4]->CheckCompactionMembershipReadiness().Ready());
    const auto followerJointSnapshot = rafts[4]->Compact();
    assert_int_equal(followerJointSnapshot.Index, jointSnapshotIndex);
    assert_true(followerJointSnapshot.IsJoint());
    assert_true(followerJointSnapshot.Voters == oldVoters);
    assert_true(followerJointSnapshot.JointVoters == newVoters);

    snapshotSenderId = 1;
    holdSnapshotChunks = true;
    isolateSnapshotReceiver = false;
    for (unsigned i = 0; i < 8 && heldSnapshotChunks.empty(); ++i) tick(1);
    assert_false(heldSnapshotChunks.empty());
    auto partialJointChunk = std::move(heldSnapshotChunks.front());
    heldSnapshotChunks.erase(heldSnapshotChunks.begin());
    assert_int_equal(partialJointChunk.Cast<TInstallSnapshotRequest>()->Offset, 0);
    assert_false(partialJointChunk.Cast<TInstallSnapshotRequest>()->Done & TInstallSnapshotRequest::DoneFlag);
    rafts[6]->Process(now, std::move(partialJointChunk));
    processors[6]->ProcessCommitted();
    assert_int_equal(states[6]->SnapshotIndex, 0);
    assert_int_equal(applications[6]->LastAppliedIndex, jointIndex);

    // Preserve a later chunk from the old leader. The restarted follower must
    // reject it after leadership changes, then accept a clean transfer from 4.
    for (unsigned i = 0; i < 8 && heldSnapshotChunks.empty(); ++i) tick(1);
    assert_false(heldSnapshotChunks.empty());
    auto staleJointChunk = std::move(heldSnapshotChunks.front());
    heldSnapshotChunks.erase(heldSnapshotChunks.begin());
    assert_int_equal(staleJointChunk.Cast<TInstallSnapshotRequest>()->Offset,
        TInstallSnapshotRequest::MaxChunkBytes);
    assert_false(staleJointChunk.Cast<TInstallSnapshotRequest>()->Done & TInstallSnapshotRequest::DoneFlag);

    processors[6].reset();
    applications[6].reset();
    rafts[6].reset();
    states[6].reset();
    states[6] = std::make_shared<TDurableState>((dir / "raft").string(), 6);
    TNodeDict jointPeers;
    for (auto voter : jointVoters)
        if (voter != 6) jointPeers.emplace(voter, endpoints[6][voter]);
    rafts[6] = std::make_shared<TRaft>(states[6], 6, jointPeers);
    applications[6] = std::make_shared<TSnapshotRsm>();
    applications[6]->SnapshotPaddingBytes = 2 * TInstallSnapshotRequest::MaxChunkBytes + 17;
    processors[6] = std::make_shared<TRequestProcessor>(rafts[6], applications[6], jointPeers);
    processors[6]->ProcessCommitted();
    assert_int_equal(states[6]->SnapshotIndex, 0);
    assert_int_equal(applications[6]->LastAppliedIndex, jointIndex);

    for (unsigned i = 0; i < 8 &&
        (rafts[1]->GetVolatileState()->MatchIndex.at(4) < states[1]->LastLogIndex ||
         rafts[1]->GetVolatileState()->CommitAckIndex.at(4) < rafts[1]->GetVolatileState()->CommitIndex); ++i)
        tick(1);
    rafts[1]->TransferLeadership(4, now);
    assert_true(rafts[1]->CurrentStateName() == EState::FOLLOWER);
    snapshotSenderId = 4;
    for (unsigned i = 0; i < 4 && rafts[4]->CurrentStateName() != EState::LEADER; ++i) tick(4);
    assert_true(rafts[4]->CurrentStateName() == EState::LEADER);
    for (unsigned i = 0; i < 8 && rafts[4]->GetVolatileState()->CommitIndex < states[4]->LastLogIndex; ++i)
        tick(4); // Commit the new leader's current-term entry under both majorities.
    assert_true(rafts[4]->GetVolatileState()->CommitIndex > jointIndex);

    snapshotSenderId = 1;
    expectSnapshotRestartRetry = true;
    holdSnapshotChunks = false;
    endpoints[1][6]->Route(std::move(staleJointChunk));
    assert_true(sawOutOfOrderSnapshotRejection);
    assert_int_equal(states[6]->SnapshotIndex, 0);

    snapshotSenderId = 4;
    expectSnapshotRestartRetry = false;
    awaitingOutOfOrderSnapshotReply = false;
    heldSnapshotChunks.clear(); // Discard undelivered chunks from the new leader's first attempt.
    duplicateSnapshotChunkTo6 = scenario == 1;
    for (unsigned i = 0; i < 48 && states[6]->SnapshotIndex < jointSnapshotIndex; ++i) tick(4);
    assert_int_equal(states[6]->SnapshotIndex, jointSnapshotIndex);
    assert_true(rafts[6]->GetVoterIds() == jointVoters);
    assert_true(rafts[6]->IsVoter(6));
    assert_true(states[6]->GetBoundVoters() == oldVoters);
    assert_true(states[6]->GetSnapshot()->IsJoint());
    assert_true(states[6]->GetSnapshot()->JointVoters == newVoters);
    assert_int_equal(states[6]->GetSnapshot()->ConfigurationIndex, jointIndex);
    assert_int_equal(applications[6]->LastAppliedIndex, jointSnapshotIndex);
    if (scenario == 1) {
        assert_true(duplicatedSnapshotChunk);
        assert_true(heldDuplicateSnapshotReplies.empty());
    }

    reorderStableAppendTo6 = scenario == 1;
    const auto stableIndex = rafts[4]->FinalizeConfiguration();
    for (unsigned i = 0; i < 8 && rafts[4]->GetVolatileState()->CommitIndex < stableIndex; ++i) tick(4);
    assert_int_equal(rafts[4]->GetVolatileState()->CommitIndex, stableIndex);
    for (unsigned i = 0; i < 8; ++i) {
        bool propagated = true;
        for (uint32_t id = 1; id <= 6; ++id) propagated = propagated && states[id]->CommitIndex >= stableIndex;
        if (propagated) break;
        tick(4);
    }
    for (uint32_t id = 1; id <= 6; ++id) {
        assert_true(rafts[id]->GetVoterIds() == newVoters);
        assert_true(rafts[id]->IsVoter(id) ==
            std::binary_search(newVoters.begin(), newVoters.end(), id));
        assert_true(states[id]->GetBoundVoters() == oldVoters);
    }
    if (scenario == 1) {
        assert_true(bool(delayedStableAppend));
        const auto delayed = delayedStableAppend.Cast<TAppendEntriesRequest>();
        const auto delayedCommit = delayed->LeaderCommit &
            ~TAppendEntriesRequest::JointElectionFenceLeader;
        assert_true(delayedCommit < stableIndex);
        assert_true(states[6]->CommitIndex >= stableIndex);

        // Deliver an older stable-entry replication after a newer one reached
        // this follower, then replay the same RPC. Neither stale commit metadata
        // nor a duplicate append may roll back the committed voter set or log.
        const auto committedBeforeReplay = states[6]->CommitIndex;
        const auto lastLogBeforeReplay = states[6]->LastLogIndex;
        auto duplicate = delayedStableAppend;
        endpoints[4][6]->Route(std::move(delayedStableAppend));
        endpoints[4][6]->Route(std::move(duplicate));
        assert_int_equal(states[6]->CommitIndex, committedBeforeReplay);
        assert_int_equal(states[6]->LastLogIndex, lastLogBeforeReplay);
        assert_true(rafts[6]->GetVoterIds() == newVoters);
    }

    for (unsigned i = 0; i < 8; ++i) {
        bool safe = true;
        for (auto voter : newVoters)
            safe = safe && rafts[voter]->GetVolatileState()->CompactionSafeConfigurationIndex >= stableIndex;
        if (safe) break;
        tick(4);
    }
    for (auto voter : newVoters)
        assert_true(rafts[voter]->GetVolatileState()->CompactionSafeConfigurationIndex >= stableIndex);

    // Keep a newly promoted voter behind the log, then compact on the new
    // leader. Its application image spans multiple chunks so restart loses a
    // real partial transfer rather than a complete one-chunk snapshot.
    isolateSnapshotReceiver = true;
    const auto tailBeforeSnapshot = rafts[4]->Append(Ordinary(states[4]->CurrentTerm));
    for (unsigned i = 0; i < 8 && rafts[4]->GetVolatileState()->CommitIndex < tailBeforeSnapshot; ++i)
        tick(4);
    assert_int_equal(rafts[4]->GetVolatileState()->CommitIndex, tailBeforeSnapshot);
    for (unsigned i = 0; i < 8 && states[5]->CommitIndex < tailBeforeSnapshot; ++i) tick(4);
    assert_int_equal(states[5]->CommitIndex, tailBeforeSnapshot);
    assert_int_equal(states[6]->CommitIndex, stableIndex);
    const auto snapshot = rafts[4]->Compact();
    const auto snapshotIndex = snapshot.Index;
    assert_true(snapshot.StableConfiguration);
    assert_true(snapshot.Voters == newVoters);
    assert_int_equal(snapshot.ConfigurationIndex, stableIndex);
    assert_true(snapshotIndex > stableIndex);
    assert_true(states[4]->GetBoundVoters() == oldVoters);
    const auto followerSnapshot = rafts[5]->Compact();
    assert_int_equal(followerSnapshot.Index, snapshotIndex);
    assert_true(followerSnapshot.StableConfiguration);

    const auto replacementSnapshotLeaderId = newVoters.front() == 4
        ? newVoters[1] : newVoters.front();
    for (unsigned i = 0; i < 8 &&
        states[replacementSnapshotLeaderId]->CommitIndex < tailBeforeSnapshot; ++i) tick(4);
    assert_int_equal(states[replacementSnapshotLeaderId]->CommitIndex, tailBeforeSnapshot);
    for (unsigned i = 0; i < 8 &&
        !rafts[replacementSnapshotLeaderId]->CheckCompactionMembershipReadiness().Ready(); ++i)
        tick(4);
    assert_true(rafts[replacementSnapshotLeaderId]->CheckCompactionMembershipReadiness().Ready());
    const auto replacementSnapshot = rafts[replacementSnapshotLeaderId]->Compact();
    assert_int_equal(replacementSnapshot.Index, snapshotIndex);
    assert_true(replacementSnapshot.StableConfiguration);
    assert_true(replacementSnapshot.Voters == newVoters);

    holdSnapshotChunks = true;
    isolateSnapshotReceiver = false;
    for (unsigned i = 0; i < 8 && heldSnapshotChunks.empty(); ++i) tick(4);
    assert_false(heldSnapshotChunks.empty());
    auto partialChunk = std::move(heldSnapshotChunks.front());
    heldSnapshotChunks.erase(heldSnapshotChunks.begin());
    assert_false(partialChunk.Cast<TInstallSnapshotRequest>()->Done & TInstallSnapshotRequest::DoneFlag);
    rafts[6]->Process(now, std::move(partialChunk));
    processors[6]->ProcessCommitted();
    assert_int_equal(states[6]->SnapshotIndex, jointSnapshotIndex);
    assert_int_equal(applications[6]->LastAppliedIndex, stableIndex);

    // Hold a later chunk from the old leader so it can be replayed after the
    // replacement leader wins an election and the restarted follower resets.
    for (unsigned i = 0; i < 8 && heldSnapshotChunks.empty(); ++i) tick(4);
    assert_false(heldSnapshotChunks.empty());
    auto staleStableChunk = std::move(heldSnapshotChunks.front());
    heldSnapshotChunks.erase(heldSnapshotChunks.begin());
    assert_int_equal(staleStableChunk.Cast<TInstallSnapshotRequest>()->Offset,
        TInstallSnapshotRequest::MaxChunkBytes);
    assert_false(staleStableChunk.Cast<TInstallSnapshotRequest>()->Done & TInstallSnapshotRequest::DoneFlag);

    auto restartVoter = [&](uint32_t id) {
        processors[id].reset();
        applications[id].reset();
        rafts[id].reset();
        states[id].reset();
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
        TNodeDict peers;
        for (auto voter : newVoters)
            if (voter != id) peers.emplace(voter, endpoints[id][voter]);
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers);
        applications[id] = std::make_shared<TSnapshotRsm>();
        applications[id]->SnapshotPaddingBytes = 2 * TInstallSnapshotRequest::MaxChunkBytes + 17;
        processors[id] = std::make_shared<TRequestProcessor>(rafts[id], applications[id], peers);
        processors[id]->ProcessCommitted();
    };
    restartVoter(6); // Partial transfer bytes are volatile and disappear here.
    assert_int_equal(states[6]->SnapshotIndex, jointSnapshotIndex);
    assert_int_equal(applications[6]->LastAppliedIndex, stableIndex);
    sawOutOfOrderSnapshotRejection = false;
    duplicatedSnapshotChunk = false;
    const auto oldSnapshotLeaderTerm = states[4]->CurrentTerm;
    snapshotSenderId = replacementSnapshotLeaderId;
    for (unsigned i = 0; i < 16 &&
        rafts[replacementSnapshotLeaderId]->CurrentStateName() != EState::LEADER; ++i)
        tick(replacementSnapshotLeaderId);
    assert_true(rafts[replacementSnapshotLeaderId]->CurrentStateName() == EState::LEADER);
    assert_true(rafts[4]->CurrentStateName() == EState::FOLLOWER);
    assert_true(states[replacementSnapshotLeaderId]->CurrentTerm > oldSnapshotLeaderTerm);
    for (unsigned i = 0; i < 8 &&
        rafts[replacementSnapshotLeaderId]->GetVolatileState()->CommitIndex <
            states[replacementSnapshotLeaderId]->LastLogIndex; ++i)
        tick(replacementSnapshotLeaderId);
    assert_true(rafts[replacementSnapshotLeaderId]->GetVolatileState()->CommitIndex > stableIndex);

    snapshotSenderId = 4;
    expectSnapshotRestartRetry = true;
    holdSnapshotChunks = false;
    endpoints[4][6]->Route(std::move(staleStableChunk));
    assert_true(sawOutOfOrderSnapshotRejection);
    assert_int_equal(states[6]->SnapshotIndex, jointSnapshotIndex);

    snapshotSenderId = replacementSnapshotLeaderId;
    expectSnapshotRestartRetry = false;
    awaitingOutOfOrderSnapshotReply = false;
    heldSnapshotChunks.clear(); // Drop undelivered chunks from the new leader's first attempt.
    duplicateSnapshotChunkTo6 = scenario == 1;
    for (unsigned i = 0; i < 48 && states[6]->SnapshotIndex < snapshotIndex; ++i)
        tick(replacementSnapshotLeaderId);
    assert_int_equal(states[6]->SnapshotIndex, snapshotIndex);
    if (scenario == 1) {
        assert_true(duplicatedSnapshotChunk);
        assert_true(heldDuplicateSnapshotReplies.empty());
    }
    assert_true(rafts[6]->IsVoter(6));
    assert_true(rafts[6]->GetVoterIds() == newVoters);
    assert_true(states[6]->GetBoundVoters() == oldVoters);
    assert_true(states[6]->GetSnapshot()->StableConfiguration);
    assert_true(states[6]->GetSnapshot()->Voters == newVoters);
    assert_int_equal(states[6]->GetSnapshot()->ConfigurationIndex, stableIndex);
    assert_int_equal(applications[6]->LastAppliedIndex, snapshotIndex);
    assert_true(rafts[replacementSnapshotLeaderId]->GetVolatileState()->MatchIndex.at(6) >= snapshotIndex);

    // Replicate a tail after installation, then restart again to prove the
    // remote snapshot and final voter identity recover together.
    const auto tailAfterSnapshot = rafts[replacementSnapshotLeaderId]->Append(
        Ordinary(states[replacementSnapshotLeaderId]->CurrentTerm));
    for (unsigned i = 0; i < 8 &&
        rafts[replacementSnapshotLeaderId]->GetVolatileState()->CommitIndex < tailAfterSnapshot; ++i)
        tick(replacementSnapshotLeaderId);
    for (unsigned i = 0; i < 8 && states[6]->CommitIndex < tailAfterSnapshot; ++i)
        tick(replacementSnapshotLeaderId);
    assert_int_equal(states[6]->CommitIndex, tailAfterSnapshot);
    restartVoter(6);
    assert_true(rafts[6]->GetVoterIds() == newVoters);
    assert_true(rafts[6]->IsVoter(6));
    assert_true(states[6]->GetBoundVoters() == oldVoters);
    assert_int_equal(states[6]->SnapshotIndex, snapshotIndex);
    assert_int_equal(applications[6]->LastAppliedIndex, tailAfterSnapshot);

    // Restart every replica with endpoints only for the final voter set.
    processors = {};
    applications = {};
    rafts = {};
    states = {};
    for (uint32_t id = 1; id <= 6; ++id)
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
    for (uint32_t id = 1; id <= 6; ++id) {
        TNodeDict peers;
        for (auto voter : newVoters)
            if (voter != id) peers.emplace(voter, std::make_shared<TEndpoint>());
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers);
        assert_true(rafts[id]->GetVoterIds() == newVoters);
        assert_true(rafts[id]->IsVoter(id) ==
            std::binary_search(newVoters.begin(), newVoters.end(), id));
        assert_true(rafts[id]->GetVolatileState()->CommitIndex >= stableIndex);
        if (std::binary_search(newVoters.begin(), newVoters.end(), id))
            assert_true(rafts[id]->GetVolatileState()->CommitIndex >= snapshotIndex);
        if (states[id]->SnapshotIndex >= snapshotIndex) {
            const auto snapshot = states[id]->GetSnapshot();
            assert_true(snapshot && snapshot->StableConfiguration);
            assert_true(snapshot->Voters == newVoters);
            assert_int_equal(snapshot->ConfigurationIndex, stableIndex);
        }
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}
#endif
void test_live_membership_fault_scenarios_transfer_finalize_and_restart(void**) {
#ifdef __linux__
    run_live_membership_failure_scenario(0, {4, 5, 6}, {5, 6});
    run_live_membership_failure_scenario(1, {2, 4, 6}, {4, 6});
    // Keep a majority of the new disjoint voter set reachable while the old
    // set has only its leader. Joint commitment must still wait for the old
    // majority before the transition can take effect.
    run_live_membership_failure_scenario(2, {4, 5, 6}, {2, 3});
#endif
}
void test_live_disjoint_joint_entry_waits_for_old_majority_over_network(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-old-partition-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> oldVoters{1, 2, 3};
    const std::vector<uint32_t> newVoters{4, 5, 6};
    std::array<std::shared_ptr<TDurableState>, 7> states{};
    std::array<std::shared_ptr<TRaft>, 7> rafts{};
    std::array<std::array<std::shared_ptr<TEndpoint>, 7>, 7> endpoints{};
    ITimeSource::Time now{};
    bool isolateOldPeers = false;

    for (uint32_t id = 1; id <= 6; ++id) {
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
        states[id]->BindVoters(oldVoters);
        states[id]->Commit();
        for (uint32_t peer = 1; peer <= 6; ++peer)
            if (peer != id) endpoints[id][peer] = std::make_shared<TEndpoint>();
    }
    for (uint32_t id = 1; id <= 6; ++id) {
        TNodeDict peers;
        for (uint32_t peer = 1; peer <= 6; ++peer)
            if (peer != id) peers.emplace(peer, endpoints[id][peer]);
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers, newVoters);
    }
    for (uint32_t id = 1; id <= 6; ++id)
        rafts[id]->SetLocalApplicationSnapshotInstallSupport(true);
    for (uint32_t from = 1; from <= 6; ++from) {
        for (uint32_t to = 1; to <= 6; ++to) {
            if (from == to) continue;
            endpoints[from][to]->Route = [&, from, to](auto message) {
                if (isolateOldPeers && from == 1 && (to == 2 || to == 3)) return;
                rafts[to]->Process(now, std::move(message));
            };
        }
    }
    auto tick = [&] {
        now += std::chrono::seconds(1);
        rafts[1]->ProcessTimeout(now);
    };
    rafts[1]->ProcessTimeout(now);
    tick();
    tick();
    assert_true(rafts[1]->CurrentStateName() == EState::LEADER);
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < 1; ++i) tick();
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, 1);
    for (unsigned i = 0; i < 8; ++i) {
        bool ready = true;
        for (auto id : newVoters) ready = ready && rafts[1]->IsLearnerCommitAcknowledged(id);
        if (ready) break;
        tick();
    }
    for (auto id : newVoters) assert_true(rafts[1]->IsLearnerCommitAcknowledged(id));

    isolateOldPeers = true;
    const auto jointIndex = rafts[1]->ProposeJointConfiguration(newVoters);
    for (unsigned i = 0; i < 4 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick();
    assert_true(rafts[1]->GetVolatileState()->CommitIndex < jointIndex);
    for (auto id : newVoters) {
        assert_true(rafts[1]->GetVolatileState()->MatchIndex.at(id) >= jointIndex);
        assert_true(rafts[id]->IsVoter(id));
        assert_int_equal(states[id]->CommitIndex, jointIndex - 1);
    }
    assert_true(rafts[1]->GetVolatileState()->MatchIndex.at(2) < jointIndex);
    assert_true(rafts[1]->GetVolatileState()->MatchIndex.at(3) < jointIndex);

    isolateOldPeers = false;
    for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick();
    assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, jointIndex);
    for (unsigned i = 0; i < 8; ++i) {
        bool propagated = true;
        for (uint32_t id = 1; id <= 6; ++id) propagated = propagated && states[id]->CommitIndex >= jointIndex;
        if (propagated) break;
        tick();
    }
    for (uint32_t id = 1; id <= 6; ++id) {
        assert_int_equal(states[id]->CommitIndex, jointIndex);
        assert_true(rafts[id]->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5, 6}));
    }

    rafts = {};
    states = {};
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_strong_read_waits_for_both_disjoint_majorities(void**) {
#ifdef __linux__
    auto run = [](bool newMajorityFirst) {
        auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-read-quorum-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            (newMajorityFirst ? "-new" : "-old"));
        assert_true(std::filesystem::create_directory(dir));
        {
            auto state = std::make_shared<TDurableState>((dir / "raft").string(), 4);
            state->BindVoters({1, 2, 3});
            state->CurrentTerm = 2;
            state->Commit();
            const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0,
                {1, 2, 3}, {4, 5, 6}};
            state->Append(EncodeConfigurationEntry(joint));
            state->CommitIndex = 1;
            state->Commit();

            std::unordered_map<uint32_t, std::shared_ptr<TEndpoint>> endpoints;
            TNodeDict peers;
            for (uint32_t id : {1, 2, 3, 5, 6}) {
                auto endpoint = std::make_shared<TEndpoint>();
                endpoints.emplace(id, endpoint);
                peers.emplace(id, endpoint);
            }
            auto raft = std::make_shared<TRaft>(state, 4, peers);
            auto application = std::make_shared<TDummyRsm>();
            TRequestProcessor processor(raft, application, {});
            processor.ProcessCommitted();

            const auto now = ITimeSource::Time{};
            raft->ProcessTimeout(now);
            assert_true(raft->CurrentStateName() == EState::CANDIDATE);
            const auto prospectiveTerm = state->CurrentTerm + 1;
            for (uint32_t voter : {1, 2, 5})
                raft->Process(now + std::chrono::milliseconds(1),
                    NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 4, .Term = state->CurrentTerm,
                            .Seqno = raft->GetVolatileState()->PreVoteSeqno},
                        TPreVoteResponse{.ProspectiveTerm = prospectiveTerm, .VoteGranted = 1}));
            raft->ProcessTimeout(now + std::chrono::milliseconds(2));
            for (uint32_t voter : {1, 2, 5})
                raft->Process(now + std::chrono::milliseconds(3),
                    NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 4, .Term = state->CurrentTerm},
                        TRequestVoteResponse{.VoteGranted = 1}));
            raft->ProcessTimeout(now + std::chrono::milliseconds(4));
            assert_true(raft->CurrentStateName() == EState::LEADER);
            assert_int_equal(raft->GetLastIndex(), 2);

            auto acknowledgeLatestAppend = [&](uint32_t voter, int responseMillis) {
                auto& messages = endpoints.at(voter)->Messages;
                const auto request = std::find_if(messages.rbegin(), messages.rend(), [](const auto& item) {
                    return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
                });
                assert_true(request != messages.rend());
                const auto append = request->Cast<TAppendEntriesRequest>();
                const auto matchIndex = append->PrevLogIndex + append->Nentries;
                raft->Process(now + std::chrono::milliseconds(responseMillis),
                    NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 4, .Term = append->Term,
                        .Seqno = append->Seqno},
                        TAppendEntriesResponse{.MatchIndex = matchIndex, .Success = 1,
                            .Padding = TAppendEntriesResponse::SupportsInstallSnapshot |
                                TAppendEntriesResponse::SupportsApplicationSnapshotInstall}));
                return matchIndex;
            };

            raft->ProcessTimeout(now + std::chrono::milliseconds(3));
            // The new leader first probes the known joint prefix. ACK those
            // probes, then let it replicate its current-term no-op.
            assert_int_equal(acknowledgeLatestAppend(1, 4), 1);
            assert_int_equal(acknowledgeLatestAppend(2, 4), 1);
            assert_int_equal(acknowledgeLatestAppend(5, 4), 1);
            raft->ProcessTimeout(now + std::chrono::milliseconds(5));
            assert_int_equal(acknowledgeLatestAppend(1, 6), 2);
            assert_int_equal(acknowledgeLatestAppend(2, 6), 2);
            assert_int_equal(acknowledgeLatestAppend(5, 6), 2);
            assert_int_equal(raft->GetVolatileState()->CommitIndex, 2);
            processor.ProcessCommitted();
            processor.ProcessWaiting();
            assert_int_equal(application->LastAppliedIndex, 2);

            auto replyTo = std::make_shared<TEndpoint>();
            auto read = NewHoldedMessage<TCommandRequest>(sizeof(TCommandRequest) + sizeof(int64_t));
            read->Cookie = 41;
            read->Flags = TCommandRequest::EConsistent;
            const int64_t readKey = 0;
            memcpy(read->Data, &readKey, sizeof(readKey));
            processor.OnCommandRequest(read, replyTo);

            if (newMajorityFirst) {
                acknowledgeLatestAppend(5, 5); // New majority alone cannot confirm the read.
                processor.ProcessWaiting();
                assert_true(replyTo->Messages.empty());
                acknowledgeLatestAppend(1, 6); // One old vote is still short of its majority.
                processor.ProcessWaiting();
                assert_true(replyTo->Messages.empty());
                acknowledgeLatestAppend(2, 7);
            } else {
                acknowledgeLatestAppend(1, 5);
                acknowledgeLatestAppend(2, 5); // Old majority alone cannot confirm the read.
                processor.ProcessWaiting();
                assert_true(replyTo->Messages.empty());
                acknowledgeLatestAppend(5, 6);
            }
            processor.ProcessCommitted();
            processor.ProcessWaiting();
            assert_int_equal(replyTo->Messages.size(), 1);
            assert_int_equal(replyTo->Messages.front().Cast<TCommandResponse>()->Cookie, 41);
        }
        std::error_code ignored;
        std::filesystem::remove_all(dir, ignored);
    };
    run(false);
    run(true);
#endif
}
void test_timeout_now_retries_and_falls_back_under_joint_quorum(void**) {
#ifdef __linux__
    for (const bool deliverRetry : {false, true}) {
        auto dir = std::filesystem::temp_directory_path() / ("miniraft-transfer-failure-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            (deliverRetry ? "-retry" : "-fallback"));
        assert_true(std::filesystem::create_directory(dir));
        const auto name = (dir / "raft").string();
        {
            auto toLearner = std::make_shared<TEndpoint>();
            auto toLeader = std::make_shared<TEndpoint>();
            auto leaderState = std::make_shared<TDurableState>(name, 1);
            auto learnerState = std::make_shared<TDurableState>(name, 2);
            TRaft leader(leaderState, 1, {{2, toLearner}}, {2});
            TRaft learner(learnerState, 2, {{1, toLeader}}, {2});
            leader.SetLocalApplicationSnapshotInstallSupport(true);
            learner.SetLocalApplicationSnapshotInstallSupport(true);
            const auto start = ITimeSource::Time{};
            auto rpcNow = start;
            unsigned timeoutNowRequests = 0;
            bool timeoutNowDropped = false;
            toLearner->Route = [&](auto message) {
                if (message->Type == uint32_t(EMessageType::TIMEOUT_NOW_REQUEST)) {
                    ++timeoutNowRequests;
                    if (timeoutNowRequests == 1 || !deliverRetry) {
                        timeoutNowDropped = true;
                        return;
                    }
                }
                learner.Process(rpcNow, std::move(message));
            };
            toLeader->Route = [&](auto message) { leader.Process(rpcNow, std::move(message)); };
            auto leaderTimeout = [&](std::chrono::milliseconds offset) {
                rpcNow = start + offset;
                leader.ProcessTimeout(rpcNow);
            };
            auto learnerTimeout = [&](std::chrono::milliseconds offset) {
                rpcNow = start + offset;
                learner.ProcessTimeout(rpcNow);
            };

            leader.ProcessTimeout(start);
            leaderTimeout(std::chrono::seconds(10)); // Commit the leader's current-term no-op.
            assert_true(leader.IsLearnerCommitAcknowledged(2));
            const auto jointIndex = leader.ProposeJointConfiguration({2});
            leaderTimeout(std::chrono::seconds(20));
            leaderTimeout(std::chrono::seconds(30));
            leaderTimeout(std::chrono::seconds(40)); // Propagate the committed joint watermark.
            assert_int_equal(learnerState->CommitIndex, jointIndex);

            rpcNow = start + std::chrono::seconds(41);
            leader.TransferLeadership(2, rpcNow);
            assert_true(timeoutNowDropped);
            assert_int_equal(timeoutNowRequests, 1);
            assert_true(leader.CurrentStateName() == EState::FOLLOWER);
            assert_true(learner.CurrentStateName() == EState::FOLLOWER);

            if (deliverRetry) {
                leaderTimeout(std::chrono::seconds(41) + std::chrono::milliseconds(999));
                assert_int_equal(timeoutNowRequests, 1);
                leaderTimeout(std::chrono::seconds(42)); // Retry before the election timeout.
                assert_int_equal(timeoutNowRequests, 2);
                assert_true(learner.CurrentStateName() == EState::CANDIDATE);
                assert_int_equal(leader.GetState()->CurrentTerm, learner.GetState()->CurrentTerm);
                assert_true(leader.CurrentStateName() == EState::FOLLOWER);
                learnerTimeout(std::chrono::seconds(42) + std::chrono::milliseconds(1));
                assert_true(learner.CurrentStateName() == EState::LEADER);
                leaderTimeout(std::chrono::seconds(43));
                assert_int_equal(timeoutNowRequests, 2); // The higher term cancels transfer retries.
            } else {
                // If every request is lost, the old leader's ordinary election
                // timer still lets it recover under the active joint quorum.
                leaderTimeout(std::chrono::seconds(55));
                leaderTimeout(std::chrono::seconds(55) + std::chrono::milliseconds(1));
                leaderTimeout(std::chrono::seconds(55) + std::chrono::milliseconds(2));
                assert_true(leader.CurrentStateName() == EState::LEADER);
                assert_true(learner.CurrentStateName() == EState::FOLLOWER);
            }
            assert_true(leader.GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        }
        std::error_code ignored; std::filesystem::remove_all(dir, ignored);
    }
#endif
}
void test_old_side_leader_can_transfer_after_joint_snapshot_with_pending_stable_successor(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-pending-stable-transfer-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    const std::vector<uint32_t> originalVoters{1, 2, 3};
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0,
        {1, 2, 3}, {4, 5, 6}};
    TSnapshot jointSnapshot{1, 2, joint.OldVoters, "1"};
    jointSnapshot.JointVoters = joint.NewVoters;
    jointSnapshot.ConfigurationIndex = 1;
    jointSnapshot.ConfigurationTerm = 2;
    const TConfigurationRecord stable{EConfigurationPhase::Stable, 3, 1, 2,
        joint.OldVoters, joint.NewVoters};
    auto makeState = [&](uint32_t id, bool includePendingStable) {
        auto state = std::make_shared<TDurableState>(name, id);
        state->BindVoters(originalVoters);
        state->CurrentTerm = 4;
        state->InstallSnapshot(jointSnapshot); // Both nodes restart from the committed joint boundary.
        if (includePendingStable)
            state->Append(EncodeConfigurationEntry(stable)); // The old-side leader inherits the successor.
        state->Commit();
        return state;
    };
    auto leaderState = makeState(3, true);
    auto targetState = makeState(5, false);
    assert_true(leaderState->GetSnapshot() && leaderState->GetSnapshot()->IsJoint());
    assert_true(targetState->GetSnapshot() && targetState->GetSnapshot()->IsJoint());
    assert_int_equal(leaderState->CommitIndex, jointSnapshot.Index);
    assert_int_equal(targetState->CommitIndex, jointSnapshot.Index);
    assert_int_equal(leaderState->LastLogIndex, stable.PreviousIndex + 1);
    assert_int_equal(targetState->LastLogIndex, jointSnapshot.Index);

    auto toOne = std::make_shared<TEndpoint>();
    auto toTwo = std::make_shared<TEndpoint>();
    auto toFour = std::make_shared<TEndpoint>();
    auto toTarget = std::make_shared<TEndpoint>();
    auto toSix = std::make_shared<TEndpoint>();
    TNodeDict leaderPeers{{1, toOne}, {2, toTwo}, {4, toFour}, {5, toTarget}, {6, toSix}};
    auto leader = std::make_shared<TRaft>(leaderState, 3, leaderPeers);

    auto targetToOne = std::make_shared<TEndpoint>();
    auto targetToTwo = std::make_shared<TEndpoint>();
    auto targetToLeader = std::make_shared<TEndpoint>();
    auto targetToFour = std::make_shared<TEndpoint>();
    auto targetToSix = std::make_shared<TEndpoint>();
    TNodeDict targetPeers{{1, targetToOne}, {2, targetToTwo}, {3, targetToLeader},
        {4, targetToFour}, {6, targetToSix}};
    auto target = std::make_shared<TRaft>(targetState, 5, targetPeers);

    const ITimeSource::Time start{};
    leader->ProcessTimeout(start);
    assert_true(leader->CurrentStateName() == EState::CANDIDATE);
    auto grantPreVote = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto request = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::PRE_VOTE_REQUEST);
        });
        assert_true(request != endpoint->Messages.rend());
        const auto preVote = request->Cast<TPreVoteRequest>();
        assert_true(preVote->Padding & TPreVoteRequest::JointConsensusCapable);
        leader->Process(start + std::chrono::milliseconds(1),
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 3, .Term = preVote->Term,
                    .Seqno = preVote->Seqno},
                TPreVoteResponse{.ProspectiveTerm = preVote->ProspectiveTerm, .VoteGranted = 1}));
    };
    grantPreVote(1, toOne);
    grantPreVote(4, toFour);
    grantPreVote(5, toTarget);
    leader->ProcessTimeout(start + std::chrono::milliseconds(2));
    auto grantVote = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto request = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::REQUEST_VOTE_REQUEST);
        });
        assert_true(request != endpoint->Messages.rend());
        const auto vote = request->Cast<TRequestVoteRequest>();
        assert_true(vote->Padding & TRequestVoteRequest::JointConsensusCapable);
        leader->Process(start + std::chrono::milliseconds(3),
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 3, .Term = vote->Term,
                .Seqno = request->Cast<TMessageEx>()->Seqno},
                TRequestVoteResponse{.VoteGranted = 1}));
        assert_true(leader->GetVolatileState()->Votes.contains(voter));
    };
    grantVote(1, toOne);
    grantVote(4, toFour);
    grantVote(5, toTarget); // Old-side 3 leads with votes from both disjoint majorities.
    leader->ProcessTimeout(start + std::chrono::milliseconds(4));
    assert_true(leader->CurrentStateName() == EState::LEADER);
    assert_true(leaderState->CurrentTerm == 5);
    assert_int_equal(leaderState->CommitIndex, 1); // Current-term no-op remains uncommitted.

    auto replicateLatestTargetAppend = [&](ITimeSource::Time now) {
        const auto append = std::find_if(toTarget->Messages.rbegin(), toTarget->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
        });
        assert_true(append != toTarget->Messages.rend());
        target->Process(now, *append);
        const auto appendReply = std::find_if(targetToLeader->Messages.rbegin(), targetToLeader->Messages.rend(),
            [](const auto& item) { return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_RESPONSE); });
        assert_true(appendReply != targetToLeader->Messages.rend());
        leader->Process(now, *appendReply);
    };
    auto replicationNow = start + std::chrono::milliseconds(3);
    for (unsigned attempt = 0; attempt < 4 &&
        leader->GetVolatileState()->MatchIndex.at(5) < leaderState->LastLogIndex; ++attempt) {
        if (attempt) {
            replicationNow += std::chrono::seconds(1);
            leader->ProcessTimeout(replicationNow);
        }
        replicationNow += std::chrono::milliseconds(1);
        replicateLatestTargetAppend(replicationNow);
    }
    assert_int_equal(leader->GetVolatileState()->MatchIndex.at(5), leaderState->LastLogIndex);
    assert_int_equal(leader->GetVolatileState()->CommitAckIndex.at(5), 1);
    assert_true(DecodeConfigurationEntry(targetState->Get(stable.PreviousIndex + 1)) == stable);

    // The old-side leader first forwards the pending stable successor and its
    // current-term no-op. Once the target has the complete suffix, it can
    // transfer leadership before that successor commits.
    leader->TransferLeadership(5, start + std::chrono::seconds(1) + std::chrono::milliseconds(2));
    assert_true(leader->CurrentStateName() == EState::FOLLOWER);
    const auto timeoutNow = std::find_if(toTarget->Messages.rbegin(), toTarget->Messages.rend(), [](const auto& item) {
        return item->Type == uint32_t(EMessageType::TIMEOUT_NOW_REQUEST);
    });
    assert_true(timeoutNow != toTarget->Messages.rend());
    target->Process(start + std::chrono::seconds(1) + std::chrono::milliseconds(2), *timeoutNow);
    assert_true(target->CurrentStateName() == EState::CANDIDATE);
    assert_int_equal(targetState->CurrentTerm, 6);
    // The former leader has transferred and now crashes before the new leader
    // commits the inherited stable successor.
    leader.reset();
    leaderState.reset();

    auto grantTargetVote = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto request = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::REQUEST_VOTE_REQUEST);
        });
        assert_true(request != endpoint->Messages.rend());
        const auto vote = request->Cast<TRequestVoteRequest>();
        target->Process(start + std::chrono::seconds(1) + std::chrono::milliseconds(3),
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 5, .Term = vote->Term,
                .Seqno = request->Cast<TMessageEx>()->Seqno}, TRequestVoteResponse{.VoteGranted = 1}));
    };
    grantTargetVote(1, targetToOne);
    grantTargetVote(2, targetToTwo);
    grantTargetVote(4, targetToFour);
    target->ProcessTimeout(start + std::chrono::seconds(1) + std::chrono::milliseconds(4));
    assert_true(target->CurrentStateName() == EState::LEADER);
    assert_int_equal(targetState->CommitIndex, 1); // The stable successor still needs joint replication.

    auto acknowledgeTargetAppend = [&](uint32_t voter, const std::shared_ptr<TEndpoint>& endpoint) {
        const auto append = std::find_if(endpoint->Messages.rbegin(), endpoint->Messages.rend(), [](const auto& item) {
            return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
        });
        assert_true(append != endpoint->Messages.rend());
        const auto request = append->Cast<TAppendEntriesRequest>();
        const auto matchIndex = request->PrevLogIndex + request->Nentries;
        target->Process(start + std::chrono::seconds(1) + std::chrono::milliseconds(5),
            NewHoldedMessage(TMessageEx{.Src = voter, .Dst = 5, .Term = targetState->CurrentTerm,
                .Seqno = append->Cast<TMessageEx>()->Seqno},
                TAppendEntriesResponse{.MatchIndex = matchIndex, .Success = 1,
                    .Padding = TAppendEntriesResponse::SupportsConfigurationBarrier |
                        TAppendEntriesResponse::SupportsInstallSnapshot |
                        TAppendEntriesResponse::SupportsApplicationSnapshotInstall}));
        return matchIndex;
    };
    const auto firstProbe = acknowledgeTargetAppend(1, targetToOne);
    assert_int_equal(firstProbe, targetState->LastLogIndex - 1);
    assert_int_equal(acknowledgeTargetAppend(2, targetToTwo), firstProbe);
    assert_int_equal(acknowledgeTargetAppend(4, targetToFour), firstProbe);
    // The new leader first probes the followers' known prefix, then retries
    // with its current-term no-op after those match ACKs advance NextIndex.
    target->ProcessTimeout(start + std::chrono::seconds(1) + std::chrono::milliseconds(6));
    assert_int_equal(acknowledgeTargetAppend(1, targetToOne), targetState->LastLogIndex);
    assert_int_equal(acknowledgeTargetAppend(2, targetToTwo), targetState->LastLogIndex);
    assert_int_equal(acknowledgeTargetAppend(4, targetToFour), targetState->LastLogIndex);
    assert_int_equal(targetState->CommitIndex, targetState->LastLogIndex);
    assert_true(targetState->CommitIndex > jointSnapshot.Index);
    assert_true(target->GetVoterIds() == joint.NewVoters);

    // Restart the crashed old-side voter from its durable joint snapshot and
    // pending stable entry. Catch it up from the new leader, including the
    // commit metadata that finalizes the stable voter set.
    auto recoveredState = std::make_shared<TDurableState>(name, 3);
    assert_int_equal(recoveredState->CommitIndex, jointSnapshot.Index);
    assert_int_equal(recoveredState->LastLogIndex, stable.PreviousIndex + 2);
    auto recovered = std::make_shared<TRaft>(recoveredState, 3, leaderPeers);
    assert_true(recovered->GetConfigurationState().Committed.IsJoint());
    auto recoveryNow = start + std::chrono::seconds(2);
    for (unsigned attempt = 0; attempt < 8 && recoveredState->CommitIndex < targetState->CommitIndex; ++attempt) {
        recoveryNow += std::chrono::seconds(1);
        target->ProcessTimeout(recoveryNow);
        const auto append = std::find_if(targetToLeader->Messages.rbegin(), targetToLeader->Messages.rend(),
            [](const auto& item) { return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST); });
        assert_true(append != targetToLeader->Messages.rend());
        const auto seqno = append->Cast<TMessageEx>()->Seqno;
        recovered->Process(recoveryNow, *append);
        const auto response = std::find_if(toTarget->Messages.rbegin(), toTarget->Messages.rend(),
            [&](auto& item) {
                return item->Type == uint32_t(EMessageType::APPEND_ENTRIES_RESPONSE) &&
                    item.template Cast<TMessageEx>()->Seqno == seqno;
            });
        assert_true(response != toTarget->Messages.rend());
        target->Process(recoveryNow, *response);
    }
    assert_true(recoveredState->CommitIndex >= targetState->CommitIndex);
    assert_true(recovered->GetVoterIds() == joint.NewVoters);
    assert_false(recovered->GetConfigurationState().Committed.IsJoint());
    assert_true(recoveredState->GetBoundVoters() == originalVoters);
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_stable_membership_compaction_recovers_active_voters(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-membership-snapshot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    TSnapshot snapshot;
    uint64_t stableIndex = 0;
    {
        auto leaderState = std::make_shared<TDurableState>(name, 1);
        auto addedState = std::make_shared<TDurableState>(name, 2);
        auto toAdded = std::make_shared<TEndpoint>();
        auto toLeader = std::make_shared<TEndpoint>();
        auto leader = std::make_shared<TRaft>(leaderState, 1, TNodeDict{{2, toAdded}}, std::vector<uint32_t>{2});
        auto added = std::make_shared<TRaft>(addedState, 2, TNodeDict{{1, toLeader}}, std::vector<uint32_t>{2});
        auto leaderApp = std::make_shared<TSnapshotRsm>();
        auto addedApp = std::make_shared<TSnapshotRsm>();
        TRequestProcessor leaderProcessor(leader, leaderApp, {{2, toAdded}});
        TRequestProcessor addedProcessor(added, addedApp, {{1, toLeader}});
        ITimeSource::Time now{};
        bool dropConfigurationBarrier = false;
        bool dropConfigurationBarrierResponse = false;
        bool maskConfigurationBarrierCapability = false;
        unsigned configurationBarrierRequests = 0, droppedConfigurationBarriers = 0;
        unsigned configurationBarrierResponses = 0, acceptedConfigurationBarriers = 0;
        unsigned droppedConfigurationBarrierResponses = 0;
        toAdded->Route = [&](auto message) {
            if (message->Type == uint32_t(EMessageType::CONFIGURATION_BARRIER_REQUEST)) {
                ++configurationBarrierRequests;
                if (dropConfigurationBarrier) { ++droppedConfigurationBarriers; return; }
            }
            added->Process(now, std::move(message));
            addedProcessor.ProcessCommitted();
        };
        toLeader->Route = [&](auto message) {
            if (message->Type == uint32_t(EMessageType::CONFIGURATION_BARRIER_RESPONSE)) {
                ++configurationBarrierResponses;
                if (message.template Cast<TConfigurationBarrierResponse>()->Accepted)
                    ++acceptedConfigurationBarriers;
                if (dropConfigurationBarrierResponse) {
                    ++droppedConfigurationBarrierResponses;
                    return;
                }
            }
            if (maskConfigurationBarrierCapability &&
                message->Type == uint32_t(EMessageType::APPEND_ENTRIES_RESPONSE)) {
                auto response = message.template Cast<TAppendEntriesResponse>();
                response->Padding &= ~TAppendEntriesResponse::SupportsConfigurationBarrier;
            }
            leader->Process(now, std::move(message));
            leaderProcessor.ProcessCommitted();
        };
        auto pump = [&] {
            now += std::chrono::seconds(10);
            leader->ProcessTimeout(now);
            leaderProcessor.ProcessCommitted();
            addedProcessor.ProcessCommitted();
        };

        leader->ProcessTimeout(now);
        assert_int_equal(leader->CurrentStateName(), EState::LEADER);
        leaderProcessor.ProcessCommitted();
        pump(); // Learner acknowledges the leader's committed bootstrap boundary.
        assert_true(leader->GetVolatileState()->MatchIndex.contains(2));
        assert_int_equal(leader->GetVolatileState()->MatchIndex.at(2), leaderState->LastLogIndex);
        assert_int_equal(leader->GetVolatileState()->CommitAckIndex.at(2),
            leader->GetVolatileState()->CommitIndex);
        assert_true(leader->IsLearnerCommitAcknowledged(2));

        const auto jointIndex = leader->ProposeJointConfiguration({1, 2});
        pump(); // Commit locally; the peer has not received the commit watermark yet.
        assert_int_equal(leader->GetVolatileState()->CommitIndex, jointIndex);
        assert_true(addedState->CommitIndex < jointIndex);
        const auto beforeUngossipedJointCompact = static_cast<TStateFields>(*leaderState);
        Reject([&] { leader->Compact(); });
        assert_true(static_cast<TStateFields>(*leaderState) == beforeUngossipedJointCompact);
        maskConfigurationBarrierCapability = true;
        pump(); // A legacy peer can acknowledge the commit without advertising the new RPC.
        assert_int_equal(addedState->CommitIndex, jointIndex);
        assert_int_equal(configurationBarrierRequests, 0);
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, 0);
        Reject([&] { added->Compact(); });

        maskConfigurationBarrierCapability = false;
        dropConfigurationBarrier = true;
        pump(); // The fresh capability ACK permits the leader to send the barrier.
        assert_int_equal(droppedConfigurationBarriers, 1);
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, 0);
        Reject([&] { added->Compact(); }); // A local commit alone cannot prove peer ACKs.
        dropConfigurationBarrier = false;
        pump(); // Retry the dropped leader certificate and acknowledge it.
        assert_int_equal(configurationBarrierRequests, 2);
        assert_int_equal(configurationBarrierResponses, 1);
        assert_int_equal(acceptedConfigurationBarriers, 1);
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, jointIndex);
        // The receiver's certificate is volatile; periodic re-advertisement
        // reconstructs it after a process restart without a new election.
        const auto requestsBeforeRefresh = configurationBarrierRequests;
        auto* addedVolatileState = const_cast<TVolatileState*>(added->GetVolatileState());
        addedVolatileState->CompactionSafeConfigurationIndex = 0;
        addedVolatileState->CompactionSafeConfigurationTerm = 0;
        pump(); pump();
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, 0);
        pump();
        assert_int_equal(configurationBarrierRequests, requestsBeforeRefresh + 1);
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, jointIndex);
        assert_true(leader->IsVoter(1)); assert_true(leader->IsVoter(2));
        const auto jointImage = CaptureSnapshot(*leader, *leaderApp);
        assert_true(jointImage.IsJoint());
        assert_true(jointImage.Voters == (std::vector<uint32_t>{1}));
        assert_true(jointImage.JointVoters == (std::vector<uint32_t>{1, 2}));
        assert_int_equal(jointImage.ConfigurationIndex, jointIndex);
        snapshot = leader->Compact();
        assert_true(snapshot == jointImage);
        assert_int_equal(leaderState->SnapshotIndex, jointIndex);
        const auto addedJointImage = added->Compact();
        assert_true(addedJointImage.IsJoint());
        assert_int_equal(addedState->SnapshotIndex, jointIndex);

        stableIndex = leader->FinalizeConfiguration();
        dropConfigurationBarrier = true;
        pump(); pump();
        assert_int_equal(leader->GetVolatileState()->CommitIndex, stableIndex);
        assert_true(leader->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_int_equal(leaderApp->LastAppliedIndex, stableIndex);
        assert_int_equal(addedApp->LastAppliedIndex, stableIndex);

        assert_true(added->GetVolatileState()->CompactionSafeConfigurationIndex < stableIndex);
        dropConfigurationBarrier = false;
        dropConfigurationBarrierResponse = true;
        pump(); // Retry the stable barrier, then lose its acknowledgement.
        assert_int_equal(added->GetVolatileState()->CompactionSafeConfigurationIndex, stableIndex);
        assert_true(leader->GetVolatileState()->ConfigurationBarrierAckIndex.at(2) < stableIndex);
        assert_int_equal(droppedConfigurationBarrierResponses, 1);
        dropConfigurationBarrierResponse = false;
        pump(); // Retry after the lost response and record follower acceptance.
        assert_int_equal(leader->GetVolatileState()->ConfigurationBarrierAckIndex.at(2), stableIndex);
        const auto addedStableImage = added->Compact();
        assert_true(addedStableImage.StableConfiguration);
        assert_int_equal(addedStableImage.ConfigurationIndex, stableIndex);

        snapshot = leader->Compact();
        assert_true(snapshot.StableConfiguration);
        assert_true(snapshot.Voters == (std::vector<uint32_t>{1, 2}));
        assert_true(leaderState->GetBoundVoters() == (std::vector<uint32_t>{1}));
        assert_int_equal(snapshot.ConfigurationIndex, stableIndex);
        assert_int_equal(snapshot.ConfigurationTerm, leaderState->LogTerm(stableIndex));

        // A committed application prefix before a pending transition can be
        // compacted while retaining the uncommitted configuration entry.
        const auto appliedIndex = leader->Append(Ordinary(leaderState->CurrentTerm));
        pump(); pump(); // First commit locally, then propagate the new commit boundary.
        assert_int_equal(leader->GetVolatileState()->CommitIndex, appliedIndex);
        assert_int_equal(addedState->CommitIndex, appliedIndex);
        assert_int_equal(leaderApp->LastAppliedIndex, appliedIndex);
        const auto pendingJointIndex = leader->ProposeJointConfiguration({1});
        assert_int_equal(pendingJointIndex, appliedIndex + 1);

        snapshot = leader->Compact();
        assert_int_equal(snapshot.Index, appliedIndex);
        assert_true(snapshot.StableConfiguration);
        assert_int_equal(leaderState->SnapshotIndex, appliedIndex);
        assert_int_equal(leaderState->LastLogIndex, pendingJointIndex);
        assert_int_equal(leaderState->RetainedLogFlags(pendingJointIndex), TLogEntry::EConfiguration);
        assert_int_equal(leaderState->CommitIndex, appliedIndex);
        assert_true(DecodeConfigurationEntry(leaderState->Get(pendingJointIndex)).Phase == EConfigurationPhase::Joint);
    }

    {
        auto recoveredState = std::make_shared<TDurableState>(name, 1);
        assert_true(recoveredState->GetBoundVoters() == (std::vector<uint32_t>{1}));
        assert_true(*recoveredState->GetSnapshot() == snapshot);
        auto peer = std::make_shared<TEndpoint>();
        auto recovered = std::make_shared<TRaft>(recoveredState, 1, TNodeDict{{2, peer}},
            std::vector<uint32_t>{2}); // Bootstrap from original binding; snapshot restores active voters.
        assert_true(recovered->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_true(recovered->IsVoter(2));
        assert_int_equal(recoveredState->CommitIndex, snapshot.Index);
        assert_int_equal(recoveredState->LastLogIndex, snapshot.Index + 1);
        assert_true(DecodeConfigurationEntry(recoveredState->Get(snapshot.Index + 1)).Phase == EConfigurationPhase::Joint);
        auto recoveredApp = std::make_shared<TSnapshotRsm>();
        TRequestProcessor recoveredProcessor(recovered, recoveredApp, {{2, peer}});
        assert_int_equal(recoveredApp->LastAppliedIndex, snapshot.Index);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_snapshot_install_validates_and_advances_committed_membership_history(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-snapshot-install-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        state->BindVoters({1}); // The durable binding is the original set.
        state->CurrentTerm = 3;
        state->Append(Ordinary(1));
        auto joint = TConfigurationRecord{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
        state->Append(EncodeConfigurationEntry(joint));
        auto stable = joint; stable.Phase = EConfigurationPhase::Stable;
        stable.PreviousIndex = 2; stable.PreviousTerm = 2;
        state->Append(EncodeConfigurationEntry(stable));
        state->CommitIndex = 3; state->Commit();

        std::vector<TMessageHolder<TMessage>> replies;
        auto peer = std::make_shared<TEndpoint>();
        peer->Route = [&](auto message) { replies.push_back(std::move(message)); };
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{2});
        auto app = std::make_shared<TSnapshotRsm>(); TRequestProcessor processor(raft, app, {{1, peer}});
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));

        auto sendSnapshot = [&](const TSnapshot& snapshot, uint64_t rpcTerm) {
            const auto bytes = EncodeSnapshot(snapshot);
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
            request->Src = 1; request->Dst = 2; request->Term = rpcTerm; request->Seqno = replies.size() + 1;
            request->SnapshotIndex = snapshot.Index; request->SnapshotTerm = snapshot.Term;
            request->Offset = 0; request->TotalBytes = bytes.size();
            request->Done = TInstallSnapshotRequest::DoneFlag |
                TInstallSnapshotRequest::JointElectionFenceLeader;
            request->LeaderId = 1;
            memcpy(request->Data, bytes.data(), bytes.size());
            raft->Process(ITimeSource::Time{}, request);
        };

        // The joint entry's identity is not the committed stable identity.
        // Reject this claim before accepting even a higher leader term.
        TSnapshot wrongIdentity{4, 3, {1, 2}, "4"};
        wrongIdentity.StableConfiguration = true;
        wrongIdentity.ConfigurationIndex = 2; wrongIdentity.ConfigurationTerm = 2;
        const auto before = static_cast<TStateFields>(*state);
        sendSnapshot(wrongIdentity, 4);
        assert_true(static_cast<TStateFields>(*state) == before);
        assert_true(replies.empty());
        sendSnapshot(wrongIdentity, 3);
        assert_int_equal(replies.size(), 1);
        assert_false(replies.back().Cast<TInstallSnapshotResponse>()->Success);
        assert_int_equal(state->SnapshotIndex, 0); assert_int_equal(app->LastAppliedIndex, 0);

        // A stable snapshot that matches the committed successor may cross the
        // membership boundary. The receiver advances its baseline to index 4.
        TSnapshot valid{4, 3, {1, 2}, "4"};
        valid.StableConfiguration = true;
        valid.ConfigurationIndex = 3; valid.ConfigurationTerm = 2;
        sendSnapshot(valid, 3);
        assert_int_equal(replies.size(), 2);
        assert_true(replies.back().Cast<TInstallSnapshotResponse>()->Installed);
        assert_int_equal(state->SnapshotIndex, 4); assert_int_equal(state->CommitIndex, 4);
        auto validWithLineage = valid;
        validWithLineage.PreviousVoters = {1};
        validWithLineage.PreviousJointIndex = 2;
        validWithLineage.PreviousJointTerm = 2;
        validWithLineage.ConfigurationTransitions.push_back({{1}, {1, 2}, 0, 0, 2, 2, 3, 2});
        assert_true(*state->GetSnapshot() == validWithLineage);
        assert_int_equal(app->LastAppliedIndex, 4);

        // A legacy image beyond this committed transition inherits its identity
        // from the receiver's proven stable history.
        TSnapshot legacy{5, 3, {1, 2}, "5"};
        sendSnapshot(legacy, 3);
        assert_int_equal(replies.size(), 3);
        assert_true(replies.back().Cast<TInstallSnapshotResponse>()->Installed);
        assert_true(state->GetSnapshot()->StableConfiguration);
        assert_int_equal(state->GetSnapshot()->ConfigurationIndex, 3);
        assert_int_equal(state->GetSnapshot()->ConfigurationTerm, 2);
        assert_int_equal(app->LastAppliedIndex, 5);

        // A subsequent transition extends the committed identity. This append
        // is rejected if snapshot installation left the old baseline at index 0.
        auto next = TConfigurationRecord{EConfigurationPhase::Joint, 3, 3, 2, {1, 2}, {1}};
        auto append = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3, .Seqno = 20},
            TAppendEntriesRequest{.PrevLogIndex = 5, .PrevLogTerm = 3,
                .LeaderCommit = 6 | TAppendEntriesRequest::JointElectionFenceLeader,
                .LeaderId = 1, .Nentries = 1});
        append.InitPayload(1); append.Payload[0] = EncodeConfigurationEntry(next);
        raft->Process(ITimeSource::Time{}, append);
        assert_int_equal(replies.size(), 4);
        assert_true(replies.back().Cast<TAppendEntriesResponse>()->Success);
        assert_int_equal(state->LastLogIndex, 6); assert_int_equal(state->CommitIndex, 6);
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    }

    // A stale voter can catch up from a joint snapshot after missing a stable
    // membership change. The old-side leader remains valid during joint
    // consensus, but a chain with a different trusted anchor must be rejected.
    {
        const std::vector<uint32_t> originalVoters{1, 2, 3};
        const std::vector<uint32_t> intermediateVoters{1, 2, 4};
        const std::vector<uint32_t> jointNewVoters{2, 4, 5};
        auto history = TConfigurationHistory::Stable(originalVoters, 0, 0, 0, 0)
            .Observe(1, Ordinary(1));
        const TConfigurationRecord firstJoint{
            EConfigurationPhase::Joint, 2, 0, 0, originalVoters, intermediateVoters};
        history = history.Advance(2, EncodeConfigurationEntry(firstJoint));
        auto firstStable = firstJoint;
        firstStable.Phase = EConfigurationPhase::Stable;
        firstStable.PreviousIndex = 2;
        firstStable.PreviousTerm = 2;
        history = history.Advance(3, EncodeConfigurationEntry(firstStable));
        history = history.Observe(4, Ordinary(3));
        const TConfigurationRecord activeJoint{
            EConfigurationPhase::Joint, 3, 3, 2, intermediateVoters, jointNewVoters};
        history = history.Advance(5, EncodeConfigurationEntry(activeJoint));
        const auto snapshot = history.MakeSnapshot("5");
        auto wrongAnchor = snapshot;
        wrongAnchor.ConfigurationTransitions.front().OldVoters = {1, 2, 7};

        auto state = std::make_shared<TDurableState>((dir / "stale-history").string(), 3);
        state->BindVoters(originalVoters);
        state->CurrentTerm = 4;
        state->Append(Ordinary(1));
        state->CommitIndex = 1;
        state->Commit();
        auto toLeader = std::make_shared<TEndpoint>();
        TNodeDict peers{{1, toLeader}, {2, std::make_shared<TEndpoint>()},
            {4, std::make_shared<TEndpoint>()}, {5, std::make_shared<TEndpoint>()}};
        auto raft = std::make_shared<TRaft>(state, 3, peers, std::vector<uint32_t>{4, 5});
        auto application = std::make_shared<TSnapshotRsm>();
        TRequestProcessor processor(raft, application, peers);

        auto install = [&](const TSnapshot& incoming, uint64_t sequence) {
            const auto bytes = EncodeSnapshot(incoming);
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(
                sizeof(TInstallSnapshotRequest) + bytes.size());
            request->Src = 1;
            request->Dst = 3;
            request->Term = 4;
            request->Seqno = sequence;
            request->SnapshotIndex = incoming.Index;
            request->SnapshotTerm = incoming.Term;
            request->Offset = 0;
            request->TotalBytes = bytes.size();
            request->Done = TInstallSnapshotRequest::DoneFlag |
                TInstallSnapshotRequest::JointElectionFenceLeader;
            request->LeaderId = 1;
            memcpy(request->Data, bytes.data(), bytes.size());
            raft->Process(ITimeSource::Time{}, std::move(request));
        };

        install(wrongAnchor, 40);
        assert_int_equal(state->SnapshotIndex, 0);
        assert_int_equal(state->LastLogIndex, 1);
        assert_int_equal(application->LastAppliedIndex, 0);
        auto rejected = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
        assert_false(rejected->Success);
        assert_false(rejected->Installed);

        install(snapshot, 41);
        assert_int_equal(state->SnapshotIndex, snapshot.Index);
        assert_int_equal(state->CommitIndex, snapshot.Index);
        assert_int_equal(application->LastAppliedIndex, snapshot.Index);
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 4, 5}));
        assert_true(raft->IsVoter(1)); // The valid sender belongs to the old side only.
        assert_false(raft->IsVoter(3));
        assert_true(state->GetSnapshot()->IsJoint());
        assert_true(state->GetSnapshot()->JointVoters == jointNewVoters);
        auto installed = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
        assert_true(installed->Success);
        assert_true(installed->Installed);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_snapshot_transfer_before_uncommitted_configuration_preserves_suffix(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-pending-config-snapshot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    auto state = std::make_shared<TDurableState>(name, 2);
    state->BindVoters({1, 2, 3});
    state->CurrentTerm = 3;
    auto addJoint = TConfigurationRecord{EConfigurationPhase::Joint, 2, 0, 0,
        {1, 2, 3}, {1, 2, 3, 4}};
    state->Append(EncodeConfigurationEntry(addJoint));
    auto addStable = addJoint; addStable.Phase = EConfigurationPhase::Stable;
    addStable.PreviousIndex = 1; addStable.PreviousTerm = 2;
    state->Append(EncodeConfigurationEntry(addStable));
    state->Append(Ordinary(2)); // The leader's committed snapshot boundary.
    auto pendingJoint = TConfigurationRecord{EConfigurationPhase::Joint, 3, 2, 2,
        {1, 2, 3, 4}, {1, 2, 4, 5}};
    state->Append(EncodeConfigurationEntry(pendingJoint));
    state->CommitIndex = 2; state->Commit();

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto source = std::make_shared<TEndpoint>();
    source->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    auto jointOnlySource = std::make_shared<TEndpoint>();
    jointOnlySource->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    auto newOnlySource = std::make_shared<TEndpoint>();
    newOnlySource->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    TNodeDict peers{{1, source}, {3, jointOnlySource},
        {4, std::make_shared<TEndpoint>()}, {5, newOnlySource},
        {6, std::make_shared<TEndpoint>()}};
    auto raft = std::make_shared<TRaft>(state, 2, peers);
    auto app = std::make_shared<TSnapshotRsm>(); app->LastAppliedIndex = 2;
    TRequestProcessor processor(raft, app, peers);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));
    assert_int_equal(state->CommitIndex, 2);
    assert_true(DecodeConfigurationEntry(state->Get(4)) == pendingJoint);

    auto sendSnapshot = [&](const TSnapshot& image, uint64_t rpcTerm, uint32_t leaderId = 1) {
        const auto bytes = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
        request->Seqno = 1;
        request->Src = leaderId; request->Dst = 2; request->Term = rpcTerm;
        request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
        request->Offset = 0; request->TotalBytes = bytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = leaderId;
        memcpy(request->Data, bytes.data(), bytes.size());
        raft->Process(ITimeSource::Time{}, request);
    };

    TSnapshot safe{3, 2, {1, 2, 3, 4}, "3"};
    safe.StableConfiguration = true;
    safe.ConfigurationIndex = 2; safe.ConfigurationTerm = 2;
    sendSnapshot(safe, 4);
    assert_int_equal(replies.size(), 1);
    assert_true(replies.back()->Installed); assert_true(replies.back()->Success);
    auto safeWithLineage = safe;
    safeWithLineage.PreviousVoters = {1, 2, 3};
    safeWithLineage.PreviousJointIndex = 1;
    safeWithLineage.PreviousJointTerm = 2;
    safeWithLineage.ConfigurationTransitions.push_back({{1, 2, 3}, {1, 2, 3, 4}, 0, 0, 1, 2, 2, 2});
    assert_true(*state->GetSnapshot() == safeWithLineage);
    assert_int_equal(state->SnapshotIndex, 3); assert_int_equal(state->CommitIndex, 3);
    assert_int_equal(state->LastLogIndex, 4);
    assert_int_equal(state->RetainedLogFlags(4), TLogEntry::EConfiguration);
    assert_int_equal(app->LastAppliedIndex, 3);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));
    assert_true(DecodeConfigurationEntry(state->Get(4)) == pendingJoint);

    // A snapshot that reaches the pending entry must identify the exact
    // transition already validated in the receiver's log. Reject a different
    // joint voter set, and reject the former stable configuration phase, before
    // either higher term can change the state.
    TSnapshot mismatchedJoint{4, 3, {1, 2, 3, 4}, "4"};
    mismatchedJoint.JointVoters = {1, 2, 4, 6};
    mismatchedJoint.ConfigurationIndex = 4; mismatchedJoint.ConfigurationTerm = 3;
    const auto beforeMismatchedJoint = static_cast<TStateFields>(*state);
    sendSnapshot(mismatchedJoint, 5);
    assert_true(static_cast<TStateFields>(*state) == beforeMismatchedJoint);
    assert_int_equal(replies.size(), 1);

    TSnapshot mismatchedIdentity{5, 4, {1, 2, 3, 4}, "5"};
    mismatchedIdentity.JointVoters = {1, 2, 4, 5};
    mismatchedIdentity.ConfigurationIndex = 4; mismatchedIdentity.ConfigurationTerm = 2;
    const auto beforeMismatchedIdentity = static_cast<TStateFields>(*state);
    sendSnapshot(mismatchedIdentity, 5);
    assert_true(static_cast<TStateFields>(*state) == beforeMismatchedIdentity);
    assert_int_equal(replies.size(), 1);

    TSnapshot crosses{4, 3, {1, 2, 3, 4}, "4"};
    crosses.StableConfiguration = true;
    crosses.ConfigurationIndex = 2; crosses.ConfigurationTerm = 2;
    const auto beforeCrossing = static_cast<TStateFields>(*state);
    sendSnapshot(crosses, 5);
    assert_true(static_cast<TStateFields>(*state) == beforeCrossing);
    assert_int_equal(replies.size(), 1);
    assert_int_equal(state->LastLogIndex, 4);
    assert_int_equal(state->RetainedLogFlags(4), TLogEntry::EConfiguration);

    // The same leader term can now install the exact pending transition as a
    // committed snapshot boundary. This consumes the matching log entry and
    // recovers the joint quorum from snapshot metadata.
    TSnapshot matchingJoint{4, 3, {1, 2, 3, 4}, "4"};
    matchingJoint.JointVoters = {1, 2, 4, 5};
    matchingJoint.ConfigurationIndex = 4; matchingJoint.ConfigurationTerm = 3;
    matchingJoint.PreviousVoters = {1, 2, 3, 4};
    matchingJoint.PreviousConfigurationIndex = 2; matchingJoint.PreviousConfigurationTerm = 2;
    matchingJoint.PreviousJointIndex = 4; matchingJoint.PreviousJointTerm = 3;
    matchingJoint.ConfigurationTransitions = {
        {{1, 2, 3}, {1, 2, 3, 4}, 0, 0, 1, 2, 2, 2},
        {{1, 2, 3, 4}, {1, 2, 4, 5}, 2, 2, 4, 3, 0, 0},
    };
    // A newly added voter can lead once the pending joint configuration is
    // active and both majorities authorize its term.
    sendSnapshot(matchingJoint, 5, 5);
    assert_int_equal(replies.size(), 2);
    assert_true(replies.back()->Installed); assert_true(replies.back()->Success);
    assert_int_equal(state->CurrentTerm, 5);
    assert_int_equal(state->SnapshotIndex, 4); assert_int_equal(state->CommitIndex, 4);
    assert_int_equal(state->LastLogIndex, 4);
    assert_true(*state->GetSnapshot() == matchingJoint);
    assert_int_equal(app->LastAppliedIndex, 4);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3, 4, 5}));

    // Stable finalization is also safe to install when it exactly matches the
    // pending successor. The committed joint quorum still authorizes an old-
    // only leader while the stable entry is uncommitted.
    auto pendingStable = TConfigurationRecord{EConfigurationPhase::Stable, 5, 4, 3,
        {1, 2, 3, 4}, {1, 2, 4, 5}};
    state->Append(EncodeConfigurationEntry(pendingStable));
    state->Commit();
    auto finalizedRaft = std::make_shared<TRaft>(state, 2, peers);
    auto finalizedApp = std::make_shared<TSnapshotRsm>(); finalizedApp->LastAppliedIndex = 4;
    TRequestProcessor finalizedProcessor(finalizedRaft, finalizedApp, peers);
    raft = std::move(finalizedRaft);
    app = std::move(finalizedApp);

    TSnapshot mismatchedStable{5, 5, {1, 2, 3, 4, 5}, "5"};
    mismatchedStable.StableConfiguration = true;
    mismatchedStable.ConfigurationIndex = 5; mismatchedStable.ConfigurationTerm = 5;
    const auto beforeMismatchedStable = static_cast<TStateFields>(*state);
    sendSnapshot(mismatchedStable, 6, 3);
    assert_true(static_cast<TStateFields>(*state) == beforeMismatchedStable);
    assert_int_equal(replies.size(), 2);

    TSnapshot matchingStable{5, 5, {1, 2, 4, 5}, "5"};
    matchingStable.StableConfiguration = true;
    matchingStable.ConfigurationIndex = 5; matchingStable.ConfigurationTerm = 5;
    matchingStable.PreviousVoters = {1, 2, 3, 4};
    matchingStable.PreviousConfigurationIndex = 2; matchingStable.PreviousConfigurationTerm = 2;
    matchingStable.PreviousJointIndex = 4; matchingStable.PreviousJointTerm = 3;
    matchingStable.ConfigurationTransitions = {
        {{1, 2, 3}, {1, 2, 3, 4}, 0, 0, 1, 2, 2, 2},
        {{1, 2, 3, 4}, {1, 2, 4, 5}, 2, 2, 4, 3, 5, 5},
    };
    const auto beforeUnauthorizedSender = static_cast<TStateFields>(*state);
    sendSnapshot(matchingStable, 6, 6);
    assert_true(static_cast<TStateFields>(*state) == beforeUnauthorizedSender);
    assert_int_equal(replies.size(), 2);

    sendSnapshot(matchingStable, 6, 3);
    assert_int_equal(replies.size(), 3);
    assert_true(replies.back()->Installed); assert_true(replies.back()->Success);
    assert_int_equal(state->CurrentTerm, 6);
    assert_int_equal(state->SnapshotIndex, 5); assert_int_equal(state->CommitIndex, 5);
    assert_true(*state->GetSnapshot() == matchingStable);
    assert_int_equal(app->LastAppliedIndex, 5);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 4, 5}));
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_joint_snapshot_install_requires_matching_joint_history_and_recovers(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-snapshot-runtime-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    TSnapshot snapshot{4, 2, {1}, "4"};
    snapshot.JointVoters = {1, 2};
    snapshot.ConfigurationIndex = 2; snapshot.ConfigurationTerm = 2;
    snapshot.PreviousVoters = {1};
    snapshot.PreviousConfigurationIndex = 1; snapshot.PreviousConfigurationTerm = 1;
    snapshot.PreviousJointIndex = 2; snapshot.PreviousJointTerm = 2;
    snapshot.ConfigurationTransitions.push_back({{1}, {1, 2}, 1, 1, 2, 2, 0, 0});
    auto olderFormatSnapshot = snapshot;
    olderFormatSnapshot.PreviousVoters.clear();
    olderFormatSnapshot.PreviousConfigurationIndex = 0;
    olderFormatSnapshot.PreviousConfigurationTerm = 0;
    olderFormatSnapshot.PreviousJointIndex = 0;
    olderFormatSnapshot.PreviousJointTerm = 0;
    olderFormatSnapshot.ConfigurationTransitions.clear();
    assert_true(EncodeSnapshot(olderFormatSnapshot).substr(0, 8) == "MRFTS002");
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        state->BindVoters({1}); // Original binding stays distinct from active joint membership.
        TSnapshot priorStable{1, 1, {1}, "1"};
        priorStable.StableConfiguration = true;
        priorStable.ConfigurationIndex = 1; priorStable.ConfigurationTerm = 1;
        state->InstallSnapshot(priorStable);
        state->CurrentTerm = 2;
        const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 1, 1, {1}, {1, 2}};
        state->Append(EncodeConfigurationEntry(joint));
        state->Append(Ordinary(2)); // Uncommitted tail is replaced by the snapshot.
        state->CommitIndex = 2; state->Commit();

        std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
        auto peer = std::make_shared<TEndpoint>();
        peer->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{2});
        auto app = std::make_shared<TSnapshotRsm>();
        TRequestProcessor processor(raft, app, {{1, peer}});
        processor.ProcessCommitted();
        assert_int_equal(app->LastAppliedIndex, 2);

        auto sendSnapshot = [&](const TSnapshot& image, uint64_t rpcTerm) {
            const auto bytes = EncodeSnapshot(image);
            auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
            request->Src = 1; request->Dst = 2; request->Term = rpcTerm;
            request->Seqno = replies.size() + 1;
            request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
            request->Offset = 0; request->TotalBytes = bytes.size();
            request->Done = TInstallSnapshotRequest::DoneFlag |
                TInstallSnapshotRequest::JointElectionFenceLeader;
            request->LeaderId = 1;
            memcpy(request->Data, bytes.data(), bytes.size());
            raft->Process(ITimeSource::Time{}, request);
        };

        auto forged = olderFormatSnapshot; forged.JointVoters = {1, 3};
        const auto beforeForgery = static_cast<TStateFields>(*state);
        sendSnapshot(forged, 3); // Must reject before persisting the higher RPC term.
        assert_true(static_cast<TStateFields>(*state) == beforeForgery);
        assert_true(replies.empty());

        sendSnapshot(olderFormatSnapshot, 3);
        assert_int_equal(state->CurrentTerm, 3);
        assert_int_equal(state->SnapshotIndex, snapshot.Index);
        assert_int_equal(state->CommitIndex, snapshot.Index);
        assert_true(*state->GetSnapshot() == snapshot);
        assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
        assert_int_equal(app->LastAppliedIndex, snapshot.Index);
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    }

    // Restart from the joint boundary, then prove that retained history can
    // append and recover the stable successor from that baseline.
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        auto peer = std::make_shared<TEndpoint>();
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{2});
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        assert_true(raft->IsVoter(2));
        auto app = std::make_shared<TSnapshotRsm>();
        TRequestProcessor processor(raft, app, {{1, peer}});
        assert_int_equal(app->LastAppliedIndex, snapshot.Index);
    }
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        state->CurrentTerm = 4;
        const TConfigurationRecord stable{EConfigurationPhase::Stable, 4, 2, 2, {1}, {1, 2}};
        state->Append(EncodeConfigurationEntry(stable));
        state->CommitIndex = state->LastLogIndex; state->Commit();
    }
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        auto peer = std::make_shared<TEndpoint>();
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{2});
        assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
        auto app = std::make_shared<TSnapshotRsm>();
        TRequestProcessor processor(raft, app, {{1, peer}});
        processor.ProcessCommitted();
        assert_int_equal(app->LastAppliedIndex, state->CommitIndex);
        const auto recoveredStable = CaptureSnapshot(*raft, *app);
        assert_true(recoveredStable.HasStablePredecessor());
        assert_true(recoveredStable.PreviousVoters == snapshot.PreviousVoters);
        assert_int_equal(recoveredStable.PreviousConfigurationIndex, snapshot.PreviousConfigurationIndex);
        assert_int_equal(recoveredStable.PreviousConfigurationTerm, snapshot.PreviousConfigurationTerm);
        assert_int_equal(recoveredStable.PreviousJointIndex, snapshot.PreviousJointIndex);
        assert_int_equal(recoveredStable.PreviousJointTerm, snapshot.PreviousJointTerm);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_committed_joint_snapshot_can_install_its_stable_successor(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-stable-successor-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();

    auto state = std::make_shared<TDurableState>(name, 2);
    state->BindVoters({1, 3}); // The original binding remains distinct from active joint voters.
    state->CurrentTerm = 3;
    const size_t snapshotPaddingBytes = 2 * TInstallSnapshotRequest::MaxChunkBytes + 17;
    TSnapshot joint{4, 2, {1, 3}, "4" + std::string(snapshotPaddingBytes, 's')};
    joint.JointVoters = {1, 2};
    joint.ConfigurationIndex = 2; joint.ConfigurationTerm = 2;
    state->InstallSnapshot(joint);

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>();
    peer->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    auto oldOnlyPeer = std::make_shared<TEndpoint>();
    TNodeDict peers{{1, peer}, {3, oldOnlyPeer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{2});
    auto app = std::make_shared<TSnapshotRsm>();
    app->SnapshotPaddingBytes = snapshotPaddingBytes;
    auto processor = std::make_unique<TRequestProcessor>(raft, app, peers);
    assert_int_equal(app->LastAppliedIndex, joint.Index);

    auto sendSnapshotChunk = [&](const TSnapshot& image, uint64_t offset,
            uint32_t count, uint32_t leaderId = 1) {
        const auto bytes = EncodeSnapshot(image);
        assert_true(offset <= bytes.size());
        assert_true(count <= bytes.size() - offset);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
        request->Src = leaderId; request->Dst = 2; request->Term = 4;
        request->Seqno = replies.size() + 1;
        request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
        request->Offset = offset; request->TotalBytes = bytes.size();
        request->Done = (offset + count == bytes.size() ? TInstallSnapshotRequest::DoneFlag : 0u) |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = leaderId;
        memcpy(request->Data, bytes.data() + offset, count);
        raft->Process(ITimeSource::Time{}, request);
    };
    auto sendSnapshot = [&](const TSnapshot& image, uint32_t leaderId = 1) {
        const auto bytes = EncodeSnapshot(image);
        const auto count = static_cast<uint32_t>(std::min<size_t>(
            bytes.size(), TInstallSnapshotRequest::MaxChunkBytes));
        sendSnapshotChunk(image, 0, count, leaderId);
    };

    TSnapshot wrongVoters{5, 3, {1, 3}, "5"};
    wrongVoters.StableConfiguration = true;
    wrongVoters.ConfigurationIndex = 5; wrongVoters.ConfigurationTerm = 3;
    const auto beforeWrongVoters = static_cast<TStateFields>(*state);
    sendSnapshot(wrongVoters);
    assert_true(static_cast<TStateFields>(*state) == beforeWrongVoters);
    assert_true(replies.empty()); // The invalid higher-term snapshot is rejected in preflight.

    TSnapshot skippedBoundary{5, 3, {1, 2}, "5"};
    skippedBoundary.StableConfiguration = true;
    skippedBoundary.ConfigurationIndex = 2; skippedBoundary.ConfigurationTerm = 2;
    sendSnapshot(skippedBoundary);
    assert_true(static_cast<TStateFields>(*state) == beforeWrongVoters);
    assert_true(replies.empty());

    TSnapshot stable{5, 3, {1, 2}, "5" + std::string(snapshotPaddingBytes, 's')};
    stable.StableConfiguration = true;
    stable.ConfigurationIndex = 5; stable.ConfigurationTerm = 3;
    sendSnapshot(stable, 3); // An old-only member cannot lead after stable finalization.
    assert_true(static_cast<TStateFields>(*state) == beforeWrongVoters);
    assert_true(replies.empty());

    const auto stableBytes = EncodeSnapshot(stable);
    assert_true(stableBytes.size() > TInstallSnapshotRequest::MaxChunkBytes);
    sendSnapshotChunk(stable, 0, TInstallSnapshotRequest::MaxChunkBytes);
    assert_int_equal(replies.size(), 1);
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, TInstallSnapshotRequest::MaxChunkBytes);
    assert_int_equal(state->SnapshotIndex, joint.Index);
    assert_true(*state->GetSnapshot() == joint);
    assert_int_equal(app->LastAppliedIndex, joint.Index);

    // Reconstruct the follower before completing the transfer. The partial
    // image is volatile, while the committed joint snapshot and original
    // bootstrap voter binding survive the restart.
    processor.reset();
    raft.reset();
    app.reset();
    state.reset();
    state = std::make_shared<TDurableState>(name, 2);
    raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{2});
    app = std::make_shared<TSnapshotRsm>();
    app->SnapshotPaddingBytes = snapshotPaddingBytes;
    processor = std::make_unique<TRequestProcessor>(raft, app, peers);
    processor->ProcessCommitted();
    assert_true(*state->GetSnapshot() == joint);
    assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1, 3}));
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));
    assert_int_equal(app->LastAppliedIndex, joint.Index);

    // A continuation from the lost transfer cannot skip the new assembly's
    // required zero offset. A complete retry then installs the stable successor.
    sendSnapshotChunk(stable, TInstallSnapshotRequest::MaxChunkBytes,
        TInstallSnapshotRequest::MaxChunkBytes);
    assert_int_equal(replies.size(), 2);
    assert_false(replies.back()->Success);
    assert_false(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, 0);
    assert_int_equal(state->SnapshotIndex, joint.Index);
    assert_true(*state->GetSnapshot() == joint);
    assert_int_equal(app->LastAppliedIndex, joint.Index);

    for (uint64_t offset = 0; offset < stableBytes.size();) {
        const auto count = static_cast<uint32_t>(std::min<uint64_t>(
            TInstallSnapshotRequest::MaxChunkBytes, stableBytes.size() - offset));
        sendSnapshotChunk(stable, offset, count);
        const auto& response = replies.back();
        assert_true(response->Success);
        assert_int_equal(response->NextOffset, offset + count);
        assert_int_equal(response->Installed, offset + count == stableBytes.size());
        offset += count;
    }
    assert_int_equal(state->CurrentTerm, 4);
    assert_true(*state->GetSnapshot() == stable);
    assert_int_equal(app->LastAppliedIndex, stable.Index);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    assert_true(raft->IsVoter(2));

    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_committed_joint_log_can_anchor_stable_snapshot_successor(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-log-stable-successor-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();

    auto state = std::make_shared<TDurableState>(name, 2);
    state->BindVoters({1, 3});
    state->CurrentTerm = 3;
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1, 3}, {1, 2}};
    state->Append(EncodeConfigurationEntry(joint));
    state->Append(Ordinary(2));
    state->CommitIndex = 2;
    state->Commit();

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>();
    peer->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    auto oldOnlyPeer = std::make_shared<TEndpoint>();
    TNodeDict peers{{1, peer}, {3, oldOnlyPeer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{2});
    auto app = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, app, peers);
    assert_true(!state->GetSnapshot());
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));

    unsigned seqno = 0;
    auto sendSnapshot = [&](const TSnapshot& image, uint32_t leaderId = 1) {
        const auto bytes = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
        request->Src = leaderId; request->Dst = 2; request->Term = 4;
        request->Seqno = ++seqno;
        request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
        request->Offset = 0; request->TotalBytes = bytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = leaderId;
        memcpy(request->Data, bytes.data(), bytes.size());
        raft->Process(ITimeSource::Time{}, request);
    };

    TSnapshot mismatched{3, 3, {1, 2}, "3"};
    mismatched.StableConfiguration = true;
    mismatched.ConfigurationIndex = 3; mismatched.ConfigurationTerm = 3;
    mismatched.PreviousVoters = {1, 4}; // Does not identify the committed joint predecessor.
    mismatched.PreviousJointIndex = 1; mismatched.PreviousJointTerm = 2;
    const auto beforeMismatch = static_cast<TStateFields>(*state);
    sendSnapshot(mismatched);
    assert_true(static_cast<TStateFields>(*state) == beforeMismatch);
    assert_true(replies.empty()); // Reject before adopting the higher term.

    TSnapshot stable = mismatched;
    stable.PreviousVoters = {1, 3};
    TSnapshot staleSuccessor = stable;
    staleSuccessor.ConfigurationIndex = 2; staleSuccessor.ConfigurationTerm = 2;
    sendSnapshot(staleSuccessor);
    assert_true(static_cast<TStateFields>(*state) == beforeMismatch);
    assert_true(replies.empty()); // A stable config cannot predate the committed boundary.

    sendSnapshot(stable, 3); // The removed old-only voter cannot certify the stable successor.
    assert_true(static_cast<TStateFields>(*state) == beforeMismatch);
    assert_true(replies.empty());

    sendSnapshot(stable);
    assert_int_equal(replies.size(), 1);
    assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
    assert_int_equal(state->CurrentTerm, 4);
    assert_int_equal(state->GetSnapshot()->Index, stable.Index);
    assert_true(state->GetSnapshot()->StableConfiguration);
    assert_true(state->GetSnapshot()->PreviousVoters == (std::vector<uint32_t>{1, 3}));
    assert_int_equal(app->LastAppliedIndex, stable.Index);
    assert_true(raft->GetVoterIds() == (std::vector<uint32_t>{1, 2}));
    assert_true(raft->IsVoter(2));

    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_stable_snapshot_successor_rejects_conflicting_earlier_transition_history(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-history-successor-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();

    const std::vector<TSnapshotConfigurationTransition> jointHistory{
        {{1, 3}, {1, 4}, 0, 0, 1, 1, 2, 1},
        {{1, 4}, {1, 2}, 2, 1, 3, 2, 0, 0},
    };
    auto stableHistory = jointHistory;
    stableHistory.back().StableIndex = 5;
    stableHistory.back().StableTerm = 3;
    auto state = std::make_shared<TDurableState>(name, 2);
    state->BindVoters({1, 3});
    state->CurrentTerm = 3;
    TSnapshot joint{4, 2, {1, 4}, "4"};
    joint.JointVoters = {1, 2};
    joint.ConfigurationIndex = 3; joint.ConfigurationTerm = 2;
    joint.PreviousVoters = {1, 4};
    joint.PreviousConfigurationIndex = 2; joint.PreviousConfigurationTerm = 1;
    joint.PreviousJointIndex = 3; joint.PreviousJointTerm = 2;
    joint.ConfigurationTransitions = jointHistory;
    state->InstallSnapshot(joint);

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>();
    peer->Route = [&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); };
    TNodeDict peers{{1, peer}, {3, std::make_shared<TEndpoint>()}, {4, std::make_shared<TEndpoint>()}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{2});
    auto app = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, app, peers);

    auto sendSnapshot = [&](const TSnapshot& image) {
        const auto bytes = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
        request->Src = 1; request->Dst = 2; request->Term = 3;
        request->Seqno = replies.size() + 1;
        request->SnapshotIndex = image.Index; request->SnapshotTerm = image.Term;
        request->Offset = 0; request->TotalBytes = bytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 1;
        memcpy(request->Data, bytes.data(), bytes.size());
        raft->Process(ITimeSource::Time{}, request);
    };

    TSnapshot conflicting{5, 3, {1, 2}, "5"};
    conflicting.StableConfiguration = true;
    conflicting.ConfigurationIndex = 5; conflicting.ConfigurationTerm = 3;
    conflicting.PreviousVoters = {1, 4};
    conflicting.PreviousConfigurationIndex = 2; conflicting.PreviousConfigurationTerm = 1;
    conflicting.PreviousJointIndex = 3; conflicting.PreviousJointTerm = 2;
    conflicting.ConfigurationTransitions = stableHistory;
    conflicting.ConfigurationTransitions.front().OldVoters = {1, 5};
    ValidateSnapshot(conflicting); // Structurally valid, but contradicts known local lineage.

    sendSnapshot(conflicting);
    assert_int_equal(state->SnapshotIndex, joint.Index);
    assert_true(state->GetSnapshot() && *state->GetSnapshot() == joint);
    assert_int_equal(app->LastAppliedIndex, joint.Index);
    assert_false(replies.empty());
    assert_false(replies.back()->Success);
    assert_false(replies.back()->Installed);

    TSnapshot stable{5, 3, {1, 2}, "5"};
    stable.StableConfiguration = true;
    stable.ConfigurationIndex = 5; stable.ConfigurationTerm = 3;
    stable.PreviousVoters = {1, 4};
    stable.PreviousConfigurationIndex = 2; stable.PreviousConfigurationTerm = 1;
    stable.PreviousJointIndex = 3; stable.PreviousJointTerm = 2;
    stable.ConfigurationTransitions = stableHistory;
    sendSnapshot(stable);
    assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
    assert_true(*state->GetSnapshot() == stable);
    assert_int_equal(app->LastAppliedIndex, stable.Index);

    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_uncommitted_joint_suffix_replacement_restores_old_voters(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-rollback-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    {
        auto peer = std::make_shared<TEndpoint>();
        auto state = std::make_shared<TDurableState>(name, 2);
        state->BindVoters({1});
        state->CurrentTerm = 2;
        auto joint = TConfigurationRecord{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
        state->Append(EncodeConfigurationEntry(joint));
        state->Commit();
        TRaft follower(state, 2, {{1, peer}}, {2});
        assert_true(follower.IsVoter(2));

        auto ordinary = Ordinary(3);
        auto replacement = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
            TAppendEntriesRequest{.LeaderId = 1, .Nentries = 1});
        replacement.InitPayload(1); replacement.Payload[0] = ordinary;
        follower.Process(ITimeSource::Time{}, replacement);
        assert_int_equal(state->CurrentTerm, 3);
        assert_int_equal(state->LastLogIndex, 1);
        assert_int_equal(state->RetainedLogFlags(1), 0);
        assert_false(follower.IsVoter(2));
        assert_true(follower.GetVoterIds() == (std::vector<uint32_t>{1}));
        assert_int_equal(peer->Sent, 1);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_durable_restart_recovers_stable_membership_roles(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-restart-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        state->BindVoters({1}); // Original binding stays fixed through both transitions.
        state->CurrentTerm = 3;
        auto addJoint = TConfigurationRecord{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
        state->Append(EncodeConfigurationEntry(addJoint));
        state->CommitIndex = 1; state->Commit();
        auto addStable = addJoint; addStable.Phase = EConfigurationPhase::Stable;
        addStable.PreviousIndex = 1; addStable.PreviousTerm = 2;
        state->Append(EncodeConfigurationEntry(addStable));
        state->CommitIndex = 2; state->Commit();
        auto removeJoint = TConfigurationRecord{EConfigurationPhase::Joint, 3, 2, 2, {1, 2}, {1}};
        state->Append(EncodeConfigurationEntry(removeJoint));
        state->CommitIndex = 3; state->Commit();
        auto removeStable = removeJoint; removeStable.Phase = EConfigurationPhase::Stable;
        removeStable.PreviousIndex = 3; removeStable.PreviousTerm = 3;
        state->Append(EncodeConfigurationEntry(removeStable));
        state->CommitIndex = 4; state->Commit();
    }
    {
        auto state = std::make_shared<TDurableState>(name, 2);
        auto peer = std::make_shared<TEndpoint>();
        TRaft raft(state, 2, {{1, peer}}, {2}); // Reconstruct from the original binding.
        assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1}));
        assert_false(raft.IsVoter(2));
        assert_true(raft.GetVoterIds() == (std::vector<uint32_t>{1}));
        assert_true(raft.GetLearnerIds() == (std::vector<uint32_t>{2}));
        assert_int_equal(raft.GetVolatileState()->CommitIndex, 4);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
#endif
}
void test_stable_snapshot_restart_accepts_persisted_vote_for_removed_voter(void**) {
#ifdef __linux__
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-removed-vote-snapshot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> originalVoters{1, 2, 3, 4, 5, 6, 7};
    const std::vector<uint32_t> activeVoters{1, 3, 4, 5, 6};
    TSnapshot snapshot{3, 3, activeVoters, "stable membership checkpoint"};
    snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 2;
    snapshot.ConfigurationTerm = 3;

    for (uint32_t id : {1, 2}) {
        const auto name = (dir / "raft").string();
        {
            TDurableState state(name, id);
            state.BindVoters(originalVoters);
            state.CurrentTerm = 3;
            state.VotedFor = 2; // This vote preceded removal of voter 2 in term 3.
            state.Commit();
            state.InstallSnapshot(snapshot);
        }
        {
            auto state = std::make_shared<TDurableState>(name, id);
            TNodeDict peers;
            for (auto voter : activeVoters)
                if (voter != id) peers.emplace(voter, std::make_shared<TEndpoint>());
            auto raft = std::make_shared<TRaft>(state, id, peers);
            assert_true(state->GetBoundVoters() == originalVoters);
            assert_true(raft->GetVoterIds() == activeVoters);
            assert_true(raft->IsVoter(id) == (id != 2));
            assert_int_equal(state->VotedFor, 2);
        }
    }

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}
void test_restart_after_membership_removal_needs_only_active_endpoints(void**) {
#ifdef __linux__
    for (const bool compacted : {false, true}) {
        auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-removed-endpoint-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            (compacted ? "-snapshot" : "-log"));
        assert_true(std::filesystem::create_directory(dir));
        const auto name = (dir / "raft").string();
        {
            auto state = std::make_shared<TDurableState>(name, 1);
            state->BindVoters({1, 2}); // The durable binding records the original voters.
            state->CurrentTerm = 3;
            state->Commit();
            if (compacted) {
                TSnapshot snapshot{3, 3, {1, 3}, "stable membership checkpoint"};
                snapshot.StableConfiguration = true;
                snapshot.ConfigurationIndex = 2;
                snapshot.ConfigurationTerm = 2;
                state->InstallSnapshot(snapshot);
            } else {
                TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2}, {1, 3}};
                state->Append(EncodeConfigurationEntry(joint));
                auto stable = joint;
                stable.Phase = EConfigurationPhase::Stable;
                stable.Term = 3;
                stable.PreviousIndex = 1;
                stable.PreviousTerm = 2;
                state->Append(EncodeConfigurationEntry(stable));
                state->CommitIndex = 2;
                state->Commit();
            }
        }
        {
            auto state = std::make_shared<TDurableState>(name, 1);
            TRaft raft(state, 1, {{3, std::make_shared<TEndpoint>()}});
            assert_true(state->GetBoundVoters() == (std::vector<uint32_t>{1, 2}));
            assert_true(raft.GetVoterIds() == (std::vector<uint32_t>{1, 3}));
            assert_true(raft.GetLearnerIds().empty());
            assert_int_equal(raft.GetVolatileState()->CommitIndex, compacted ? 3 : 2);
        }
        std::error_code ignored; std::filesystem::remove_all(dir, ignored);
    }
#endif
}
void test_configuration_application_advances_without_calling_state_machine(void**) {
    auto state = std::make_shared<TState>(); auto record = Joint(); record.Term = 1;
    record.PreviousIndex = record.PreviousTerm = 0;
    state->Append(EncodeConfigurationEntry(record));
    Reject([&] { TRaft invalid(state, 1, {}); });
    assert_true(state->GetBoundVoters().empty());
    auto live = std::make_shared<TState>(); auto raft = std::make_shared<TRaft>(live, 1, TNodeDict{});
    auto app = std::make_shared<TCountingRsm>(); TRequestProcessor processor(raft, app, {});
    raft->ProcessTimeout({});
    processor.ProcessCommitted();
    assert_int_equal(app->LastAppliedIndex, 1); // Leader's initial stub.
    record.Term = live->CurrentTerm;
    live->Append(EncodeConfigurationEntry(record));
    raft->ProcessTimeout(ITimeSource::Time{} + std::chrono::seconds(1));
    processor.ProcessCommitted();
    assert_int_equal(app->LastAppliedIndex, 2);
    assert_int_equal(app->Writes, 0);
    auto compacted = std::make_shared<TState>();
    { TRaft bootstrap(compacted, 1, {}); }
    compacted->Append(Ordinary(1));
    compacted->InstallSnapshot({1, 1, {1}, "fixed image"});
    compacted->CurrentTerm = record.Term;
    compacted->Append(EncodeConfigurationEntry(record));
    const auto fields = static_cast<TStateFields>(*compacted);
    Reject([&] { TRaft invalid(compacted, 1, {}); });
    assert_true(static_cast<TStateFields>(*compacted) == fields);
    assert_true(compacted->GetSnapshot()->MaterializeData() == "fixed image");
}
#ifdef __linux__
void test_durable_restart_rejects_reserved_entry_without_rewriting(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string(); const auto path = name + ".durable.1";
    auto record = Joint(); record.Term = 1; record.PreviousIndex = record.PreviousTerm = 0;
    { auto state = std::make_shared<TDurableState>(name, 1);
      { TRaft bootstrap(state, 1, {}); }
      state->Append(EncodeConfigurationEntry(record)); }
    std::ifstream input(path, std::ios::binary); std::string before(std::istreambuf_iterator<char>(input), {}); input.close();
    { auto state = std::make_shared<TDurableState>(name, 1);
      const auto fields = static_cast<TStateFields>(*state);
      Reject([&] { TRaft invalid(state, 1, {}); });
      assert_true(static_cast<TStateFields>(*state) == fields); }
    std::ifstream after(path, std::ios::binary);
    assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == before);
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_legacy_restart_uses_safe_flag_lookup_and_preserves_files(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-legacy-config-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    auto record = Joint(); record.Term = 1; record.PreviousIndex = record.PreviousTerm = 0;
    { TDiskState state(name, 2); state.Append(EncodeConfigurationEntry(record)); }
    std::vector<std::string> before;
    for (auto suffix : {".entries.2", ".index.2", ".state.2"}) {
        std::ifstream input(name + suffix, std::ios::binary);
        before.emplace_back(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    { auto state = std::make_shared<TDiskState>(name, 2);
      const auto fields = static_cast<TStateFields>(*state);
      Reject([&] { TRaft invalid(state, 2, {}); });
      assert_true(static_cast<TStateFields>(*state) == fields); }
    size_t i = 0;
    for (auto suffix : {".entries.2", ".index.2", ".state.2"}) {
        std::ifstream input(name + suffix, std::ios::binary);
        assert_true(std::string(std::istreambuf_iterator<char>(input), {}) == before[i++]);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_offline_history_scan_after_durable_snapshot_and_restart(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-history-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string(); const auto path = name + ".durable.1";
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    {
        auto state = std::make_shared<TDurableState>(name, 1);
        { TRaft bootstrap(state, 1, {}); }
        state->Append(Ordinary(1)); state->InstallSnapshot({1, 1, {1}, "fixed image"});
        state->CurrentTerm = 3; state->Commit();
        state->Append(EncodeConfigurationEntry(joint)); state->Append(Ordinary(2));
        state->Append(EncodeConfigurationEntry(final));
    }
    std::ifstream input(path, std::ios::binary); std::string before(std::istreambuf_iterator<char>(input), {}); input.close();
    {
        auto state = std::make_shared<TDurableState>(name, 1);
        // This fixture explicitly knows that index 1 precedes any config entry.
        auto history = TConfigurationHistory::Stable({1}, 0, 0, state->SnapshotIndex, state->SnapshotTerm);
        const auto scan = ScanConfigurationHistory(*state, history, 2);
        assert_true(scan.Committed.IsJoint()); assert_false(scan.Latest.IsJoint());
        assert_true(scan.Latest.OldVoters() == final.NewVoters);
        assert_true(scan.HasUncommittedConfiguration());
        history = history.Advance(2, state->Get(2));
        assert_true(history.IsJoint());
        history = history.Observe(3, state->Get(3));
        history = history.Advance(4, state->Get(4));
        assert_false(history.IsJoint()); assert_true(history.OldVoters() == final.NewVoters);
        assert_int_equal(history.ConfigurationIndex(), 4);
        Reject([&] { TRaft disabled(state, 1, {}); });
        assert_true(state->GetSnapshot()->MaterializeData() == "fixed image");
    }
    std::ifstream after(path, std::ios::binary);
    assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == before);
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_raft_recovers_joint_quorum_for_candidate_election(void**) {
    auto run = [](bool disjoint) {
        auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-quorum-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            (disjoint ? "-disjoint" : "-overlap"));
        assert_true(std::filesystem::create_directory(dir));
        const auto name = (dir / "raft").string();
        auto state = std::make_shared<TDurableState>(name, 4);
        state->BindVoters({1, 2, 3});
        state->CurrentTerm = 2;
        state->Commit();
        const auto newVoters = disjoint ? std::vector<uint32_t>{4, 5, 6} : std::vector<uint32_t>{3, 4, 5};
        TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2, 3}, newVoters};
        state->Append(EncodeConfigurationEntry(joint));
        state->Commit();

        TNodeDict peers;
        std::unordered_map<uint32_t, std::shared_ptr<TEndpoint>> endpoints;
        const std::vector<uint32_t> peerIds = disjoint
            ? std::vector<uint32_t>{1, 2, 3, 5, 6} : std::vector<uint32_t>{1, 2, 3, 5};
        for (uint32_t id : peerIds) {
            auto endpoint = std::make_shared<TEndpoint>();
            endpoints.emplace(id, endpoint);
            peers.emplace(id, endpoint);
        }
        auto raft = std::make_shared<TRaft>(state, 4, peers, disjoint
            ? std::vector<uint32_t>{4, 5, 6} : std::vector<uint32_t>{4, 5});
        assert_true(raft->IsVoter(4));
        assert_true(raft->IsVoter(5));
        assert_int_equal(raft->GetNservers(), disjoint ? 6 : 5);

        auto now = ITimeSource::Time{};
        raft->ProcessTimeout(now); // Start a joint pre-vote campaign.
        assert_true(raft->CurrentStateName() == EState::CANDIDATE);
        const auto prospectiveTerm = state->CurrentTerm + 1;
        auto preVote = [&](uint32_t source) {
            raft->Process(now, NewHoldedMessage(TMessageEx{.Src = source, .Dst = 4, .Term = state->CurrentTerm,
                    .Seqno = raft->GetVolatileState()->PreVoteSeqno},
                TPreVoteResponse{.ProspectiveTerm = prospectiveTerm, .VoteGranted = 1}));
        };
        if (disjoint) {
            for (uint32_t source : {1, 2, 5}) preVote(source);
        } else {
            for (uint32_t source : {1, 3}) preVote(source);
        }
        raft->ProcessTimeout(now + std::chrono::milliseconds(1)); // Pre-vote quorum starts the election term.
        assert_int_equal(state->CurrentTerm, 3);
        auto vote = [&](uint32_t source) {
            raft->Process(now, NewHoldedMessage(TMessageEx{.Src = source, .Dst = 4, .Term = state->CurrentTerm},
                TRequestVoteResponse{.VoteGranted = 1}));
            raft->ProcessTimeout(now + std::chrono::milliseconds(2));
        };

        vote(1); // This leaves both voter sets below their strict majority.
        assert_true(raft->CurrentStateName() == EState::CANDIDATE);
        if (disjoint) {
            vote(2); // The old majority alone cannot elect without the new majority.
            assert_true(raft->CurrentStateName() == EState::CANDIDATE);
            vote(5);
        } else {
            vote(3); // Shared voter 3 completes both majorities.
        }
        assert_true(raft->CurrentStateName() == EState::LEADER);

        raft->ProcessTimeout(now + std::chrono::milliseconds(2));
        auto ack = [&](uint32_t source, uint64_t expectedIndex, int responseMillis) {
            auto request = endpoints.at(source)->Messages.back().Cast<TAppendEntriesRequest>();
            const auto matchIndex = request->PrevLogIndex + request->Nentries;
            assert_int_equal(matchIndex, expectedIndex);
            raft->Process(now + std::chrono::milliseconds(responseMillis),
                NewHoldedMessage(TMessageEx{.Src = source, .Dst = 4, .Term = 3, .Seqno = request->Seqno},
                    TAppendEntriesResponse{.MatchIndex = matchIndex, .Success = 1,
                        .Padding = TAppendEntriesResponse::SupportsInstallSnapshot |
                            TAppendEntriesResponse::SupportsApplicationSnapshotInstall |
                            TAppendEntriesResponse::SupportsJointConsensus |
                            TAppendEntriesResponse::SupportsJointElectionFence}));
        };
        ack(3, 1, 3);
        ack(5, 1, 3);
        ack(1, 1, 3);
        assert_int_equal(raft->GetVolatileState()->CommitIndex, 0);

        raft->ProcessTimeout(now + std::chrono::milliseconds(4));
        ack(3, 2, 5);
        assert_int_equal(raft->GetVolatileState()->CommitIndex, 0);
        ack(5, 2, 5);
        assert_int_equal(raft->GetVolatileState()->CommitIndex, 0);
        assert_int_equal(raft->CommitSeqno(), 0);
        ack(1, 2, 5);
        assert_int_equal(raft->GetVolatileState()->CommitIndex, 2);
        assert_true(raft->CommitSeqno() > 0);
        raft.reset();
        state.reset();
        std::error_code ignored; std::filesystem::remove_all(dir, ignored);
    };
    run(false);
    run(true);
}
void test_joint_election_generated_reply_orders(void**) {
#ifdef __linux__
    auto run = [](std::vector<uint32_t> oldVoters, std::vector<uint32_t> newVoters) {
        const auto root = std::filesystem::temp_directory_path() / ("miniraft-joint-orders-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        assert_true(std::filesystem::create_directory(root));
        constexpr uint32_t candidateId = 4;
        std::vector<uint32_t> peerIds;
        peerIds.insert(peerIds.end(), oldVoters.begin(), oldVoters.end());
        peerIds.insert(peerIds.end(), newVoters.begin(), newVoters.end());
        std::sort(peerIds.begin(), peerIds.end());
        peerIds.erase(std::unique(peerIds.begin(), peerIds.end()), peerIds.end());
        peerIds.erase(std::remove(peerIds.begin(), peerIds.end(), candidateId), peerIds.end());

        std::vector<uint32_t> addedVoters;
        for (auto id : newVoters)
            if (!std::binary_search(oldVoters.begin(), oldVoters.end(), id)) addedVoters.push_back(id);

        auto hasJointQuorum = [&](const std::vector<uint32_t>& replies) {
            auto count = [&](const std::vector<uint32_t>& voters) {
                size_t result = std::binary_search(voters.begin(), voters.end(), candidateId) ? 1 : 0;
                for (auto id : replies)
                    if (std::binary_search(voters.begin(), voters.end(), id)) ++result;
                return result;
            };
            return count(oldVoters) > oldVoters.size() / 2 &&
                count(newVoters) > newVoters.size() / 2;
        };

        auto order = peerIds;
        uint64_t schedule = 0;
        do {
            const auto name = (root / ("raft-" + std::to_string(schedule++))).string();
            auto state = std::make_shared<TDurableState>(name, candidateId);
            state->BindVoters(oldVoters);
            state->CurrentTerm = 2;
            state->Commit();
            TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, oldVoters, newVoters};
            state->Append(EncodeConfigurationEntry(joint));
            state->CommitIndex = 1;
            state->Commit();

            TNodeDict peers;
            for (auto id : peerIds) peers.emplace(id, std::make_shared<TEndpoint>());
            TRaft raft(state, candidateId, peers, addedVoters);
            const auto start = ITimeSource::Time{};
            raft.ProcessTimeout(start);
            assert_true(raft.CurrentStateName() == EState::CANDIDATE);
            assert_true(raft.GetVolatileState()->PreVoting);
            const auto preVoteSeqno = raft.GetVolatileState()->PreVoteSeqno;
            const auto prospectiveTerm = state->CurrentTerm + 1;

            std::vector<uint32_t> preVoteReplies;
            bool startedElection = false;
            for (auto source : order) {
                preVoteReplies.push_back(source);
                raft.Process(start, NewHoldedMessage(
                    TMessageEx{.Src = source, .Dst = candidateId, .Term = state->CurrentTerm,
                        .Seqno = preVoteSeqno},
                    TPreVoteResponse{.ProspectiveTerm = prospectiveTerm, .VoteGranted = 1}));
                raft.ProcessTimeout(start + std::chrono::milliseconds(1));
                const bool expected = hasJointQuorum(preVoteReplies);
                assert_int_equal(state->CurrentTerm, expected ? prospectiveTerm : prospectiveTerm - 1);
                if (expected) {
                    startedElection = true;
                    assert_false(raft.GetVolatileState()->PreVoting);
                    break;
                }
                assert_true(raft.GetVolatileState()->PreVoting);
            }
            assert_true(startedElection);

            std::vector<uint32_t> voteReplies;
            bool elected = false;
            for (auto source : order) {
                voteReplies.push_back(source);
                raft.Process(start + std::chrono::milliseconds(2), NewHoldedMessage(
                    TMessageEx{.Src = source, .Dst = candidateId, .Term = state->CurrentTerm},
                    TRequestVoteResponse{.VoteGranted = 1}));
                raft.ProcessTimeout(start + std::chrono::milliseconds(2));
                const bool expected = hasJointQuorum(voteReplies);
                assert_true((raft.CurrentStateName() == EState::LEADER) == expected);
                if (expected) {
                    elected = true;
                    break;
                }
                assert_true(raft.CurrentStateName() == EState::CANDIDATE);
            }
            assert_true(elected);
        } while (std::next_permutation(order.begin(), order.end()));

        uint64_t expectedSchedules = 1;
        for (size_t i = 2; i <= peerIds.size(); ++i) expectedSchedules *= i;
        assert_int_equal(schedule, expectedSchedules);
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    };

    run({1, 2, 3}, {3, 4, 5});
    run({1, 2, 3}, {4, 5, 6});
#endif
}
void test_raft_keeps_joint_quorum_until_stable_successor_commits(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-joint-finalize-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto state = std::make_shared<TDurableState>((dir / "raft").string(), 4);
    state->BindVoters({1, 2, 3});
    state->CurrentTerm = 3;
    state->Commit();
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2, 3}, {3, 4, 5}};
    state->Append(EncodeConfigurationEntry(joint));
    auto stable = joint;
    stable.Phase = EConfigurationPhase::Stable;
    stable.Term = 3;
    stable.PreviousIndex = 1;
    stable.PreviousTerm = 2;
    state->Append(EncodeConfigurationEntry(stable));
    state->CommitIndex = 1;
    state->Commit();

    TNodeDict peers;
    for (uint32_t id : {1, 2, 3, 5}) peers.emplace(id, std::make_shared<TEndpoint>());
    {
        TRaft inJoint(state, 4, peers, std::vector<uint32_t>{4, 5});
        assert_true(inJoint.IsVoter(1));
        assert_true(inJoint.IsVoter(5));
        assert_int_equal(inJoint.GetNservers(), 5);
    }

    state->CommitIndex = 2;
    state->Commit();
    {
        TRaft afterStableCommit(state, 4, peers, std::vector<uint32_t>{4, 5});
        assert_false(afterStableCommit.IsVoter(1));
        assert_false(afterStableCommit.IsVoter(2));
        assert_true(afterStableCommit.IsVoter(3));
        assert_true(afterStableCommit.IsVoter(5));
        assert_int_equal(afterStableCommit.GetNservers(), 3);
    }
    state.reset();
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_raft_rejects_snapshot_when_configured_bootstrap_binding_mismatches(void**) {
    auto state = std::make_shared<TState>();
    state->BindVoters({1, 2, 3});
    state->CurrentTerm = 5;
    TSnapshot snapshot{5, 4, {2, 3, 4}, "stable image"};
    snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 4;
    snapshot.ConfigurationTerm = 3;
    state->InstallSnapshot(snapshot);

    TNodeDict peers{
        {3, std::make_shared<TEndpoint>()},
        {4, std::make_shared<TEndpoint>()},
    };
    const auto fields = static_cast<TStateFields>(*state);
    Reject([&] { TRaft raft(state, 2, peers); });
    assert_true(static_cast<TStateFields>(*state) == fields);
    assert_true(state->GetBoundVoters() == std::vector<uint32_t>({1, 2, 3}));
}

void test_persisted_commit_boundary_drives_committed_configuration_scan(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-committed-history-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    {
        auto state = std::make_shared<TDurableState>(name, 1);
        { TRaft bootstrap(state, 1, {}); }
        state->CurrentTerm = 3; state->Commit();
        state->Append(Ordinary(1));
        state->Append(EncodeConfigurationEntry(joint));
        state->Append(Ordinary(2));
        state->Append(EncodeConfigurationEntry(final));
        state->CommitIndex = 3; state->Commit();
    }
    {
        TDurableState state(name, 1);
        auto baseline = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
        const auto scan = ScanCommittedConfigurationHistory(state, baseline);
        assert_int_equal(state.CommitIndex, 3);
        assert_true(scan.Committed.IsJoint());
        assert_int_equal(scan.Committed.ConfigurationIndex(), 2);
        assert_false(scan.Latest.IsJoint());
        assert_true(scan.Latest.OldVoters() == final.NewVoters);
        assert_true(scan.HasUncommittedConfiguration());
        Reject([&] {
            auto wrong = TConfigurationHistory::Stable({2}, 0, 0, 0, 0);
            ScanCommittedConfigurationHistory(state, wrong);
        });
    }
    TState volatileState; volatileState.BindVoters({1});
    Reject([&] {
        auto baseline = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
        ScanCommittedConfigurationHistory(volatileState, baseline);
    });
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}

void test_configuration_snapshots_preserve_original_binding_across_transitions(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-snapshot-binding-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> firstActive{2, 3, 4};
    const std::vector<uint32_t> secondActive{2, 4, 5};
    const std::vector<uint32_t> thirdActive{4, 5, 6};

    TSnapshot jointSnapshot{5, 4, firstActive, "joint image"};
    jointSnapshot.JointVoters = secondActive;
    jointSnapshot.ConfigurationIndex = 4; jointSnapshot.ConfigurationTerm = 3;
    TConfigurationRecord stable{EConfigurationPhase::Stable, 5, 4, 3, firstActive, secondActive};

    {
        TDurableState state(name, 2);
        state.BindVoters(original);
        state.CurrentTerm = 4; state.Commit();
        state.InstallSnapshot(jointSnapshot);
        assert_true(state.GetBoundVoters() == original);
        assert_true(*state.GetSnapshot() == jointSnapshot);
    }

    TSnapshot stableSnapshot{6, 5, secondActive, "stable image"};
    stableSnapshot.StableConfiguration = true;
    stableSnapshot.ConfigurationIndex = 6; stableSnapshot.ConfigurationTerm = 5;
    TConfigurationRecord nextJoint{EConfigurationPhase::Joint, 6, 6, 5, secondActive, thirdActive};
    {
        TDurableState state(name, 2);
        assert_true(state.GetBoundVoters() == original);
        assert_true(*state.GetSnapshot() == jointSnapshot);
        auto history = TConfigurationHistory::FromJointSnapshot(*state.GetSnapshot());
        state.CurrentTerm = 5; state.Commit();
        state.Append(EncodeConfigurationEntry(stable));
        state.CommitIndex = state.LastLogIndex; state.Commit();
        auto scan = ScanCommittedConfigurationHistory(state, history);
        assert_false(scan.Committed.IsJoint());
        assert_true(scan.Committed.OldVoters() == secondActive);

        state.InstallSnapshot(stableSnapshot);
        state.CurrentTerm = 6; state.Commit();
        state.Append(EncodeConfigurationEntry(nextJoint));
        state.CommitIndex = state.LastLogIndex; state.Commit();
    }

    {
        auto state = std::make_shared<TDurableState>(name, 2);
        assert_true(state->GetBoundVoters() == original);
        assert_true(*state->GetSnapshot() == stableSnapshot);
        auto baseline = TConfigurationHistory::FromStableSnapshot(*state->GetSnapshot());
        const auto scan = ScanCommittedConfigurationHistory(*state, baseline);
        assert_true(scan.Committed.IsJoint());
        assert_true(scan.Committed.OldVoters() == secondActive);
        assert_true(scan.Committed.NewVoters() == thirdActive);
        assert_false(scan.HasUncommittedConfiguration());

        const auto fields = static_cast<TStateFields>(*state);
        Reject([&] { TRaft disabled(state, 2, {{4, std::make_shared<TEndpoint>()},
            {5, std::make_shared<TEndpoint>()}}); });
        assert_true(static_cast<TStateFields>(*state) == fields);
        assert_true(state->GetBoundVoters() == original);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_verified_multihop_disjoint_snapshot_recovers_and_readds_stale_voter(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-verified-disjoint-recovery-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto path = (dir / "raft").string();
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> intermediate{4, 5, 6};
    const std::vector<uint32_t> active{7, 8, 9};

    // Model a snapshot whose two-transition history has been verified out of
    // band. The stale store's original configuration is no longer the immediate
    // predecessor, and neither disjoint transition has a common voter.
    auto history = TConfigurationHistory::Stable(original, 0, 0, 0, 0);
    history = history.Observe(1, Ordinary(1));
    const TConfigurationRecord firstJoint{EConfigurationPhase::Joint, 2, 0, 0, original, intermediate};
    history = history.Advance(2, EncodeConfigurationEntry(firstJoint));
    auto firstStable = firstJoint;
    firstStable.Phase = EConfigurationPhase::Stable;
    firstStable.PreviousIndex = 2;
    firstStable.PreviousTerm = 2;
    history = history.Advance(3, EncodeConfigurationEntry(firstStable));
    history = history.Observe(4, Ordinary(2));
    const TConfigurationRecord secondJoint{EConfigurationPhase::Joint, 3, 3, 2, intermediate, active};
    history = history.Advance(5, EncodeConfigurationEntry(secondJoint));
    auto secondStable = secondJoint;
    secondStable.Phase = EConfigurationPhase::Stable;
    secondStable.PreviousIndex = 5;
    secondStable.PreviousTerm = 3;
    history = history.Advance(6, EncodeConfigurationEntry(secondStable));
    history = history.Observe(7, Ordinary(3));
    const auto verified = history.MakeSnapshot("7");
    assert_true(verified.StableConfiguration);
    assert_true(verified.Voters == active);
    assert_true(verified.PreviousVoters == intermediate);

    auto staleState = std::make_shared<TDurableState>(path, 2);
    staleState->BindVoters(original);
    staleState->CurrentTerm = 3;
    staleState->Append(Ordinary(1));
    staleState->CommitIndex = 1;
    staleState->Commit();
    {
        TNodeDict stalePeers;
        std::array<std::shared_ptr<TEndpoint>, 10> staleEndpoints{};
        for (auto peer : original) {
            if (peer == 2) continue;
            staleEndpoints[peer] = std::make_shared<TEndpoint>();
            stalePeers.emplace(peer, staleEndpoints[peer]);
        }
        for (auto peer : active) {
            staleEndpoints[peer] = std::make_shared<TEndpoint>();
            stalePeers.emplace(peer, staleEndpoints[peer]);
        }
        auto staleRaft = std::make_shared<TRaft>(staleState, 2, stalePeers, active);
        auto staleApplication = std::make_shared<TSnapshotRsm>();
        TRequestProcessor staleProcessor(staleRaft, staleApplication, stalePeers);
        const auto beforeNetworkInstall = static_cast<TStateFields>(*staleState);
        const auto beforeApplicationInstall = staleApplication->LastAppliedIndex;
        const auto encoded = EncodeSnapshot(verified);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Seqno = 1;
        request->Src = 7; request->Dst = 2; request->Term = 5;
        request->SnapshotIndex = verified.Index; request->SnapshotTerm = verified.Term;
        request->Offset = 0; request->TotalBytes = encoded.size(); request->Done = 1;
        request->LeaderId = 7;
        memcpy(request->Data, encoded.data(), encoded.size());
        staleRaft->Process({}, request);
        assert_true(static_cast<TStateFields>(*staleState) == beforeNetworkInstall);
        assert_int_equal(staleApplication->LastAppliedIndex, beforeApplicationInstall);
        for (auto& message : staleEndpoints[7]->Messages) {
            const auto response = message.Cast<TInstallSnapshotResponse>();
            assert_false(response->Success);
            assert_false(response->Installed);
        }
    }
    // The live transfer above cannot authorize the new voter set. After Raft
    // stops, the caller may install a snapshot whose history it verified out
    // of band through the offline API.
    staleState->InstallSnapshot(verified);
    assert_true(staleState->GetBoundVoters() == original);
    assert_true(*staleState->GetSnapshot() == verified);
    assert_int_equal(staleState->CommitIndex, verified.Index);
    staleState.reset();

    std::array<std::shared_ptr<TDurableState>, 10> states{};
    std::array<std::shared_ptr<TRaft>, 10> rafts{};
    std::array<std::shared_ptr<TSnapshotRsm>, 10> applications{};
    std::array<std::shared_ptr<TRequestProcessor>, 10> processors{};
    std::array<std::array<std::shared_ptr<TEndpoint>, 10>, 10> endpoints{};
    constexpr std::array<uint32_t, 4> members{2, 7, 8, 9};
    states[2] = std::make_shared<TDurableState>(path, 2);
    for (auto id : active) {
        states[id] = std::make_shared<TDurableState>(path, id);
        states[id]->BindVoters(original);
        states[id]->CurrentTerm = 3;
        states[id]->Commit();
        states[id]->InstallSnapshot(verified);
    }
    for (auto from : members) {
        for (auto to : members)
            if (from != to) endpoints[from][to] = std::make_shared<TEndpoint>();
    }
    for (auto id : members) {
        TNodeDict peers;
        for (auto peer : members)
            if (peer != id) peers.emplace(peer, endpoints[id][peer]);
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers,
            id == 2 ? std::vector<uint32_t>{} : std::vector<uint32_t>{2});
        applications[id] = std::make_shared<TSnapshotRsm>();
        processors[id] = std::make_shared<TRequestProcessor>(rafts[id], applications[id], peers);
    }
    ITimeSource::Time now{};
    for (auto from : members) {
        for (auto to : members) {
            if (from == to) continue;
            endpoints[from][to]->Route = [&, to](auto message) {
                rafts[to]->Process(now, std::move(message));
                processors[to]->ProcessCommitted();
            };
        }
    }
    assert_true(states[2]->GetBoundVoters() == original);
    assert_true(rafts[2]->GetVoterIds() == active);
    assert_true(rafts[2]->GetLearnerIds() == (std::vector<uint32_t>{2}));
    assert_false(rafts[2]->IsVoter(2));
    assert_int_equal(applications[2]->LastAppliedIndex, verified.Index);
    for (auto id : active) assert_true(rafts[id]->GetVoterIds() == active);

    // Catch up the recovered learner under the imported disjoint baseline,
    // then use the normal joint protocol to add it back as a voter.
    const_cast<TVolatileState*>(rafts[7]->GetVolatileState())->SetElectionDue(now);
    rafts[7]->ProcessTimeout(now);
    rafts[7]->ProcessTimeout(now); // Turn the synchronous pre-vote quorum into a real election.
    rafts[7]->ProcessTimeout(now); // Consume the synchronous vote responses.
    assert_true(rafts[7]->CurrentStateName() == EState::LEADER);
    auto tick = [&] {
        now += std::chrono::seconds(1);
        for (auto id : members) rafts[id]->ProcessTimeout(now);
        for (auto id : members) processors[id]->ProcessCommitted();
    };
    for (unsigned i = 0; i < 16 && !rafts[7]->IsLearnerCommitAcknowledged(2); ++i) tick();
    assert_true(rafts[7]->IsLearnerCommitAcknowledged(2));
    const auto jointIndex = rafts[7]->ProposeJointConfiguration({2, 7, 8, 9});
    for (unsigned i = 0; i < 16 && rafts[7]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick();
    assert_int_equal(rafts[7]->GetVolatileState()->CommitIndex, jointIndex);
    assert_true(rafts[2]->IsVoter(2));
    const auto stableIndex = rafts[7]->FinalizeConfiguration();
    for (unsigned i = 0; i < 16; ++i) {
        bool caughtUp = true;
        for (auto id : members) caughtUp = caughtUp && states[id]->CommitIndex >= stableIndex;
        if (caughtUp) break;
        tick();
    }
    for (auto id : members) {
        assert_true(rafts[id]->GetVoterIds() == (std::vector<uint32_t>{2, 7, 8, 9}));
        assert_true(rafts[id]->IsVoter(id));
        assert_true(states[id]->GetBoundVoters() == original);
        assert_true(states[id]->CommitIndex >= stableIndex);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}
void test_stale_snapshot_missing_new_voter_target_is_rejected_before_install(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-snapshot-missing-voter-target-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const std::vector<uint32_t> oldVoters{1, 2, 3};
    const std::vector<uint32_t> newVoters{1, 2, 4};

    auto history = TConfigurationHistory::Stable(oldVoters, 0, 0, 0, 0)
        .Observe(1, Ordinary(1));
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, oldVoters, newVoters};
    history = history.Advance(2, EncodeConfigurationEntry(joint));
    auto stable = joint;
    stable.Phase = EConfigurationPhase::Stable;
    stable.PreviousIndex = 2;
    stable.PreviousTerm = 2;
    history = history.Advance(3, EncodeConfigurationEntry(stable)).Observe(4, Ordinary(2));
    const auto snapshot = history.MakeSnapshot("4");

    auto state = std::make_shared<TDurableState>((dir / "stale").string(), 3);
    state->BindVoters(oldVoters);
    state->CurrentTerm = 2;
    state->Append(Ordinary(1));
    state->CommitIndex = 1;
    state->Commit();
    auto toOne = std::make_shared<TEndpoint>();
    auto toLeader = std::make_shared<TEndpoint>();
    TNodeDict peers{{1, toOne}, {2, toLeader}}; // Voter 4 is not provisioned yet.
    auto raft = std::make_shared<TRaft>(state, 3, peers);
    auto application = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, application, peers);

    const auto encoded = EncodeSnapshot(snapshot);
    auto install = [&](uint64_t term) {
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Seqno = 1;
        request->Src = 2;
        request->Dst = 3;
        request->Term = term;
        request->SnapshotIndex = snapshot.Index;
        request->SnapshotTerm = snapshot.Term;
        request->Offset = 0;
        request->TotalBytes = encoded.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 2;
        memcpy(request->Data, encoded.data(), encoded.size());
        raft->Process({}, std::move(request));
    };

    install(3);
    assert_int_equal(state->CurrentTerm, 3); // Recognized leader term is recorded.
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(state->LastLogIndex, 1);
    assert_true(!state->GetSnapshot());
    assert_int_equal(application->LastAppliedIndex, 0);
    assert_int_equal(toLeader->Messages.size(), 1);
    auto rejected = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
    assert_false(rejected->Success);
    assert_false(rejected->Installed);

    raft->RegisterPeerTarget(4, std::make_shared<TEndpoint>());
    install(3);
    assert_int_equal(state->SnapshotIndex, snapshot.Index);
    assert_true(*state->GetSnapshot() == snapshot);
    assert_true(raft->GetVoterIds() == newVoters);
    assert_int_equal(application->LastAppliedIndex, snapshot.Index);
    assert_int_equal(toLeader->Messages.size(), 2);
    auto installed = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
    assert_true(installed->Success);
    assert_true(installed->Installed);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}

void test_high_term_snapshot_metadata_spanning_chunks_progresses(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-large-snapshot-metadata-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    std::vector<uint32_t> oldVoters, newVoters;
    for (uint32_t id = 1; id <= TSnapshot::MaxVoters; ++id) oldVoters.push_back(id);
    newVoters = oldVoters;
    newVoters.back() = TSnapshot::MaxVoters + 1;

    auto history = TConfigurationHistory::Stable(oldVoters, 0, 0, 0, 0)
        .Observe(1, Ordinary(1));
    const TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, oldVoters, newVoters};
    history = history.Advance(2, EncodeConfigurationEntry(joint));
    auto stable = joint;
    stable.Phase = EConfigurationPhase::Stable;
    stable.PreviousIndex = 2;
    stable.PreviousTerm = 2;
    history = history.Advance(3, EncodeConfigurationEntry(stable)).Observe(4, Ordinary(2));
    const auto snapshot = history.MakeSnapshot("4");
    const auto encoded = EncodeSnapshot(snapshot);
    assert_true(encoded.size() > TInstallSnapshotRequest::MaxChunkBytes);

    auto state = std::make_shared<TDurableState>((dir / "stale").string(), 1);
    state->BindVoters(oldVoters);
    state->CurrentTerm = 2;
    state->Append(Ordinary(1));
    state->CommitIndex = 1;
    state->Commit();
    TNodeDict peers;
    std::shared_ptr<TEndpoint> toLeader;
    for (uint32_t id = 2; id <= TSnapshot::MaxVoters; ++id) {
        auto endpoint = std::make_shared<TEndpoint>();
        if (id == 2) toLeader = endpoint;
        peers.emplace(id, std::move(endpoint));
    }
    auto raft = std::make_shared<TRaft>(state, 1, peers);
    auto application = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, application, peers);
    raft->RegisterPeerTarget(TSnapshot::MaxVoters + 1, std::make_shared<TEndpoint>());

    auto sendChunk = [&](size_t offset, size_t count) {
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
        request->Seqno = offset + 1;
        request->Src = 2;
        request->Dst = 1;
        request->Term = 3;
        request->SnapshotIndex = snapshot.Index;
        request->SnapshotTerm = snapshot.Term;
        request->Offset = offset;
        request->TotalBytes = encoded.size();
        request->Done = uint32_t(offset + count == encoded.size()) |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 2;
        memcpy(request->Data, encoded.data() + offset, count);
        raft->Process({}, std::move(request));
    };

    const auto firstChunk = size_t(TInstallSnapshotRequest::MaxChunkBytes);
    sendChunk(0, firstChunk);
    assert_int_equal(state->CurrentTerm, 3);
    assert_int_equal(toLeader->Messages.size(), 1);
    auto partial = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
    assert_true(partial->Success);
    assert_false(partial->Installed);
    assert_int_equal(partial->NextOffset, firstChunk);
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(application->LastAppliedIndex, 0);
    assert_int_equal(raft->GetLeaderId(), 0); // Incomplete lineage is not leader contact.

    sendChunk(firstChunk, encoded.size() - firstChunk);
    assert_int_equal(state->SnapshotIndex, snapshot.Index);
    assert_true(*state->GetSnapshot() == snapshot);
    assert_true(raft->GetVoterIds() == newVoters);
    assert_int_equal(raft->GetLeaderId(), 2);
    assert_int_equal(application->LastAppliedIndex, snapshot.Index);
    assert_int_equal(toLeader->Messages.size(), 2);
    auto installed = toLeader->Messages.back().Cast<TInstallSnapshotResponse>();
    assert_true(installed->Success);
    assert_true(installed->Installed);

    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}

void test_overlapping_multihop_snapshot_recovers_stale_store_online(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-overlap-multihop-recovery-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto path = (dir / "stale").string();
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> intermediate{2, 3, 4};
    const std::vector<uint32_t> active{1, 2, 4};

    auto history = TConfigurationHistory::Stable(original, 0, 0, 0, 0);
    history = history.Observe(1, Ordinary(1));
    const TConfigurationRecord firstJoint{EConfigurationPhase::Joint, 2, 0, 0, original, intermediate};
    history = history.Advance(2, EncodeConfigurationEntry(firstJoint));
    auto firstStable = firstJoint;
    firstStable.Phase = EConfigurationPhase::Stable;
    firstStable.PreviousIndex = 2;
    firstStable.PreviousTerm = 2;
    history = history.Advance(3, EncodeConfigurationEntry(firstStable));
    const auto intermediateImage = history.MakeSnapshot("3");
    history = history.Observe(4, Ordinary(2));
    const TConfigurationRecord secondJoint{EConfigurationPhase::Joint, 3, 3, 2, intermediate, active};
    history = history.Advance(5, EncodeConfigurationEntry(secondJoint));
    auto secondStable = secondJoint;
    secondStable.Phase = EConfigurationPhase::Stable;
    secondStable.PreviousIndex = 5;
    secondStable.PreviousTerm = 3;
    history = history.Advance(6, EncodeConfigurationEntry(secondStable));
    history = history.Observe(7, Ordinary(3));
    const auto image = history.MakeSnapshot("7");
    assert_true(image.ConfigurationTransitions.size() == 2);
    assert_true(EncodeSnapshot(image).substr(0, 8) == "MRFTS006");

    auto staleState = std::make_shared<TDurableState>(path, 3);
    staleState->BindVoters(original);
    staleState->CurrentTerm = 3;
    staleState->Append(Ordinary(1));
    staleState->CommitIndex = 1;
    staleState->Commit();
    TNodeDict peers;
    std::array<std::shared_ptr<TEndpoint>, 6> endpoints{};
    for (auto peer : {1u, 2u, 4u, 5u}) {
        endpoints[peer] = std::make_shared<TEndpoint>();
        peers.emplace(peer, endpoints[peer]);
    }
    auto stale = std::make_shared<TRaft>(staleState, 3, peers, std::vector<uint32_t>{4, 5});
    auto application = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(stale, application, peers);
    auto sendSnapshot = [&](const TSnapshot& snapshot, uint32_t leaderId, uint64_t rpcTerm = 5) {
        const auto encoded = EncodeSnapshot(snapshot);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Seqno = 1;
        request->Src = leaderId; request->Dst = 3; request->Term = rpcTerm;
        request->SnapshotIndex = snapshot.Index; request->SnapshotTerm = snapshot.Term;
        request->Offset = 0; request->TotalBytes = encoded.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = leaderId;
        memcpy(request->Data, encoded.data(), encoded.size());
        stale->Process({}, request);
    };
    auto badAnchor = image;
    badAnchor.ConfigurationTransitions.front().OldVoters = {1, 3};
    sendSnapshot(badAnchor, 2);
    assert_int_equal(staleState->CurrentTerm, 3);
    assert_true(!staleState->GetSnapshot());
    assert_true(endpoints[2]->Messages.empty());
    // A sender absent from the intermediate voter set cannot authorize the
    // history, even though it belongs to both endpoint configurations.
    sendSnapshot(image, 1);
    assert_int_equal(staleState->CurrentTerm, 3);
    assert_true(!staleState->GetSnapshot());
    assert_true(endpoints[1]->Messages.empty());
    sendSnapshot(image, 2);

    assert_int_equal(staleState->CurrentTerm, 5);
    assert_true(*staleState->GetSnapshot() == image);
    assert_true(staleState->GetBoundVoters() == original);
    assert_true(application->LastAppliedIndex == image.Index);
    assert_false(stale->IsVoter(3));
    assert_true(stale->GetVoterIds() == active);
    assert_int_equal(endpoints[2]->Messages.size(), 1);
    assert_true(endpoints[2]->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);

    // A receiver anchored at an intermediate stable configuration may match
    // the corresponding suffix of a longer retained chain.
    const auto middlePath = (dir / "middle-stale").string();
    auto middleState = std::make_shared<TDurableState>(middlePath, 3);
    middleState->BindVoters(original);
    middleState->CurrentTerm = 3;
    middleState->InstallSnapshot(intermediateImage);
    TNodeDict middlePeers{{1, std::make_shared<TEndpoint>()},
        {2, std::make_shared<TEndpoint>()}, {4, std::make_shared<TEndpoint>()}};
    auto middleRaft = std::make_shared<TRaft>(middleState, 3, middlePeers, std::vector<uint32_t>{1});
    auto middleApplication = std::make_shared<TSnapshotRsm>();
    TRequestProcessor middleProcessor(middleRaft, middleApplication, middlePeers);
    const auto middleBytes = EncodeSnapshot(image);
    auto middleRequest = NewHoldedMessage<TInstallSnapshotRequest>(
        sizeof(TInstallSnapshotRequest) + middleBytes.size());
    middleRequest->Seqno = 1;
    middleRequest->Src = 2; middleRequest->Dst = 3; middleRequest->Term = 5;
    middleRequest->SnapshotIndex = image.Index; middleRequest->SnapshotTerm = image.Term;
    middleRequest->Offset = 0; middleRequest->TotalBytes = middleBytes.size();
    middleRequest->Done = TInstallSnapshotRequest::DoneFlag |
        TInstallSnapshotRequest::JointElectionFenceLeader;
    middleRequest->LeaderId = 2;
    memcpy(middleRequest->Data, middleBytes.data(), middleBytes.size());
    middleRaft->Process({}, middleRequest);
    assert_int_equal(middleState->CurrentTerm, 5);
    assert_true(*middleState->GetSnapshot() == image);
    assert_true(middleState->GetBoundVoters() == original);
    assert_false(middleRaft->IsVoter(3));
    assert_true(middleRaft->GetVoterIds() == active);
    assert_int_equal(middleApplication->LastAppliedIndex, image.Index);

    // A matching snapshot with only the final retained transition must not
    // discard the receiver's longer, already trusted history suffix.
    auto refreshedHistory = history.Observe(8, Ordinary(3));
    auto shorterHistoryImage = refreshedHistory.MakeSnapshot("8");
    shorterHistoryImage.ConfigurationTransitions.erase(
        shorterHistoryImage.ConfigurationTransitions.begin());
    const auto shorterHistoryBytes = EncodeSnapshot(shorterHistoryImage);
    auto shorterHistoryRequest = NewHoldedMessage<TInstallSnapshotRequest>(
        sizeof(TInstallSnapshotRequest) + shorterHistoryBytes.size());
    shorterHistoryRequest->Seqno = 1;
    shorterHistoryRequest->Src = 2; shorterHistoryRequest->Dst = 3; shorterHistoryRequest->Term = 5;
    shorterHistoryRequest->SnapshotIndex = shorterHistoryImage.Index;
    shorterHistoryRequest->SnapshotTerm = shorterHistoryImage.Term;
    shorterHistoryRequest->Offset = 0; shorterHistoryRequest->TotalBytes = shorterHistoryBytes.size();
    shorterHistoryRequest->Done = TInstallSnapshotRequest::DoneFlag |
        TInstallSnapshotRequest::JointElectionFenceLeader;
    shorterHistoryRequest->LeaderId = 2;
    memcpy(shorterHistoryRequest->Data, shorterHistoryBytes.data(), shorterHistoryBytes.size());
    middleRaft->Process({}, shorterHistoryRequest);
    assert_true(middleState->GetSnapshot()->ConfigurationTransitions == image.ConfigurationTransitions);
    assert_int_equal(middleApplication->LastAppliedIndex, shorterHistoryImage.Index);

    // A same-configuration refresh may not import older lineage that the
    // receiver cannot verify. Model a store that retained only the immediate
    // predecessor in MRFTS004, then send a longer chain with a forged prefix.
    auto predecessorOnlyImage = image;
    predecessorOnlyImage.ConfigurationTransitions.clear();
    assert_true(EncodeSnapshot(predecessorOnlyImage).substr(0, 8) == "MRFTS004");
    auto forgedHistoryImage = history.Observe(8, Ordinary(3)).MakeSnapshot("8");
    assert_true(forgedHistoryImage.ConfigurationTransitions.size() == 2);
    forgedHistoryImage.ConfigurationTransitions.front().OldVoters = {7, 8, 9};
    const auto forgedHistoryBytes = EncodeSnapshot(forgedHistoryImage);
    {
        auto lineageState = std::make_shared<TDurableState>((dir / "history-refresh").string(), 1);
        lineageState->BindVoters(original);
        lineageState->CurrentTerm = 3;
        lineageState->InstallSnapshot(predecessorOnlyImage);
        auto lineageEndpoint = std::make_shared<TEndpoint>();
        auto anotherEndpoint = std::make_shared<TEndpoint>();
        TNodeDict lineagePeers{{2, lineageEndpoint}, {4, anotherEndpoint}};
        auto lineageRaft = std::make_shared<TRaft>(lineageState, 1, lineagePeers);
        auto lineageApplication = std::make_shared<TSnapshotRsm>();
        TRequestProcessor lineageProcessor(lineageRaft, lineageApplication, lineagePeers);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + forgedHistoryBytes.size());
        request->Seqno = 1;
        request->Src = 2; request->Dst = 1; request->Term = 4;
        request->SnapshotIndex = forgedHistoryImage.Index; request->SnapshotTerm = forgedHistoryImage.Term;
        request->Offset = 0; request->TotalBytes = forgedHistoryBytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 2;
        memcpy(request->Data, forgedHistoryBytes.data(), forgedHistoryBytes.size());
        lineageRaft->Process({}, request);
        const auto installed = lineageState->GetSnapshot();
        assert_true(installed && installed->ConfigurationTransitions.size() == 1);
        assert_true(installed->ConfigurationTransitions.front() == image.ConfigurationTransitions.back());
        assert_true(lineageState->GetBoundVoters() == original);
        assert_int_equal(lineageApplication->LastAppliedIndex, forgedHistoryImage.Index);
        assert_true(lineageEndpoint->Messages.size() == 1);
        assert_true(lineageEndpoint->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);
    }

    const TConfigurationRecord nextJoint{EConfigurationPhase::Joint, 4, 6, 3,
        active, {4, 5}};
    auto jointHistory = history.Advance(8, EncodeConfigurationEntry(nextJoint))
        .Observe(9, Ordinary(4));
    const auto jointImage = jointHistory.MakeSnapshot("9");
    assert_true(jointImage.IsJoint());
    assert_true(jointImage.ConfigurationTransitions.size() == 3);
    assert_true(EncodeSnapshot(jointImage).substr(0, 8) == "MRFTS007");
    // The trusted baseline is the active stable configuration, so its
    // old-only leader may finish installing the joint-phase snapshot.
    sendSnapshot(jointImage, 1, 6);
    assert_int_equal(staleState->CurrentTerm, 6);
    assert_true(*staleState->GetSnapshot() == jointImage);
    assert_true(staleState->GetBoundVoters() == original);
    assert_false(stale->IsVoter(3));
    assert_true(stale->GetVoterIds() == (std::vector<uint32_t>{1, 2, 4, 5}));
    assert_int_equal(application->LastAppliedIndex, jointImage.Index);
    assert_int_equal(endpoints[1]->Messages.size(), 1);
    assert_true(endpoints[1]->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);

    // Exercise the same joint image from its older trusted baseline. The
    // sender must survive every completed transition before it can authorize
    // the active joint tail and a higher term.
    const auto jointStalePath = (dir / "joint-stale").string();
    auto jointStaleState = std::make_shared<TDurableState>(jointStalePath, 3);
    jointStaleState->BindVoters(original);
    jointStaleState->CurrentTerm = 3;
    jointStaleState->Append(Ordinary(1));
    jointStaleState->CommitIndex = 1;
    jointStaleState->Commit();
    TNodeDict jointPeers;
    std::array<std::shared_ptr<TEndpoint>, 6> jointEndpoints{};
    for (auto peer : {1u, 2u, 4u, 5u}) {
        jointEndpoints[peer] = std::make_shared<TEndpoint>();
        jointPeers.emplace(peer, jointEndpoints[peer]);
    }
    auto jointStale = std::make_shared<TRaft>(jointStaleState, 3, jointPeers,
        std::vector<uint32_t>{4, 5});
    auto jointApplication = std::make_shared<TSnapshotRsm>();
    TRequestProcessor jointProcessor(jointStale, jointApplication, jointPeers);
    auto sendJointSnapshot = [&](uint32_t leaderId) {
        const auto encoded = EncodeSnapshot(jointImage);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Seqno = 1;
        request->Src = leaderId; request->Dst = 3; request->Term = 6;
        request->SnapshotIndex = jointImage.Index; request->SnapshotTerm = jointImage.Term;
        request->Offset = 0; request->TotalBytes = encoded.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = leaderId;
        memcpy(request->Data, encoded.data(), encoded.size());
        jointStale->Process({}, request);
    };
    sendJointSnapshot(1);
    assert_int_equal(jointStaleState->CurrentTerm, 3);
    assert_true(!jointStaleState->GetSnapshot());
    assert_true(jointEndpoints[1]->Messages.empty());
    sendJointSnapshot(2);
    assert_int_equal(jointStaleState->CurrentTerm, 6);
    assert_true(*jointStaleState->GetSnapshot() == jointImage);
    assert_true(jointStaleState->GetBoundVoters() == original);
    assert_false(jointStale->IsVoter(3));
    assert_true(jointStale->GetVoterIds() == (std::vector<uint32_t>{1, 2, 4, 5}));
    assert_int_equal(jointApplication->LastAppliedIndex, jointImage.Index);
    assert_int_equal(jointEndpoints[2]->Messages.size(), 1);
    assert_true(jointEndpoints[2]->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);

    // A stable successor may retain only the transition that completes the
    // receiver's committed joint configuration. Preserve and complete the
    // receiver's trusted earlier transition history during installation.
    auto nextStable = nextJoint;
    nextStable.Phase = EConfigurationPhase::Stable;
    nextStable.Term = 5;
    nextStable.PreviousIndex = 8;
    nextStable.PreviousTerm = 4;
    auto stableHistory = jointHistory.Advance(10, EncodeConfigurationEntry(nextStable))
        .Observe(11, Ordinary(5));
    const auto stableSuccessorImage = stableHistory.MakeSnapshot("11");
    assert_true(!stableSuccessorImage.IsJoint());
    auto shorterSuccessorImage = stableSuccessorImage;
    shorterSuccessorImage.ConfigurationTransitions = {stableSuccessorImage.ConfigurationTransitions.back()};
    const auto shorterSuccessorBytes = EncodeSnapshot(shorterSuccessorImage);
    auto stableSuccessorRequest = NewHoldedMessage<TInstallSnapshotRequest>(
        sizeof(TInstallSnapshotRequest) + shorterSuccessorBytes.size());
    stableSuccessorRequest->Seqno = 1;
    stableSuccessorRequest->Src = 4; stableSuccessorRequest->Dst = 3; stableSuccessorRequest->Term = 7;
    stableSuccessorRequest->SnapshotIndex = shorterSuccessorImage.Index;
    stableSuccessorRequest->SnapshotTerm = shorterSuccessorImage.Term;
    stableSuccessorRequest->Offset = 0; stableSuccessorRequest->TotalBytes = shorterSuccessorBytes.size();
    stableSuccessorRequest->Done = TInstallSnapshotRequest::DoneFlag |
        TInstallSnapshotRequest::JointElectionFenceLeader;
    stableSuccessorRequest->LeaderId = 4;
    memcpy(stableSuccessorRequest->Data, shorterSuccessorBytes.data(), shorterSuccessorBytes.size());
    jointStale->Process({}, stableSuccessorRequest);
    assert_int_equal(jointStaleState->CurrentTerm, 7);
    assert_true(*jointStaleState->GetSnapshot() == stableSuccessorImage);
    assert_true(jointStaleState->GetBoundVoters() == original);
    assert_true(jointStale->GetVoterIds() == (std::vector<uint32_t>{4, 5}));
    assert_int_equal(jointApplication->LastAppliedIndex, stableSuccessorImage.Index);
    assert_int_equal(jointEndpoints[4]->Messages.size(), 1);
    assert_true(jointEndpoints[4]->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);

    // Middle-anchor stale recovery must retain its trusted local tail and append
    // only transitions after that anchor, not older unverified image history.
    auto forgedStaleImage = stableSuccessorImage;
    forgedStaleImage.ConfigurationTransitions.front().OldVoters = {7, 8, 9};
    const auto forgedStaleBytes = EncodeSnapshot(forgedStaleImage);
    {
        auto staleLineageState = std::make_shared<TDurableState>((dir / "stale-lineage").string(), 3);
        staleLineageState->BindVoters(original);
        staleLineageState->CurrentTerm = 3;
        staleLineageState->InstallSnapshot(predecessorOnlyImage);
        TNodeDict staleLineagePeers;
        auto fromFour = std::make_shared<TEndpoint>();
        for (auto peer : {1u, 2u, 5u}) staleLineagePeers.emplace(peer, std::make_shared<TEndpoint>());
        staleLineagePeers.emplace(4, fromFour);
        auto staleLineageRaft = std::make_shared<TRaft>(staleLineageState, 3, staleLineagePeers,
            std::vector<uint32_t>{3});
        auto staleLineageApplication = std::make_shared<TSnapshotRsm>();
        TRequestProcessor staleLineageProcessor(staleLineageRaft, staleLineageApplication, staleLineagePeers);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + forgedStaleBytes.size());
        request->Seqno = 1;
        request->Src = 4; request->Dst = 3; request->Term = 6;
        request->SnapshotIndex = forgedStaleImage.Index; request->SnapshotTerm = forgedStaleImage.Term;
        request->Offset = 0; request->TotalBytes = forgedStaleBytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 4;
        memcpy(request->Data, forgedStaleBytes.data(), forgedStaleBytes.size());
        staleLineageRaft->Process({}, request);
        const auto installed = staleLineageState->GetSnapshot();
        assert_true(installed && installed->ConfigurationTransitions.size() == 2);
        assert_true(installed->ConfigurationTransitions[0] == image.ConfigurationTransitions.back());
        assert_true(installed->ConfigurationTransitions[1] == stableSuccessorImage.ConfigurationTransitions.back());
        assert_true(staleLineageState->GetBoundVoters() == original);
        assert_int_equal(staleLineageApplication->LastAppliedIndex, forgedStaleImage.Index);
        assert_true(fromFour->Messages.size() == 1);
        assert_true(fromFour->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);
    }

    auto forgedJointStaleImage = jointImage;
    forgedJointStaleImage.ConfigurationTransitions.front().OldVoters = {7, 8, 9};
    const auto forgedJointStaleBytes = EncodeSnapshot(forgedJointStaleImage);
    {
        auto staleJointLineageState = std::make_shared<TDurableState>((dir / "stale-joint-lineage").string(), 3);
        staleJointLineageState->BindVoters(original);
        staleJointLineageState->CurrentTerm = 3;
        staleJointLineageState->InstallSnapshot(predecessorOnlyImage);
        TNodeDict staleJointLineagePeers;
        auto fromTwo = std::make_shared<TEndpoint>();
        for (auto peer : {1u, 4u, 5u}) staleJointLineagePeers.emplace(peer, std::make_shared<TEndpoint>());
        staleJointLineagePeers.emplace(2, fromTwo);
        auto staleJointLineageRaft = std::make_shared<TRaft>(staleJointLineageState, 3,
            staleJointLineagePeers, std::vector<uint32_t>{3});
        auto staleJointLineageApplication = std::make_shared<TSnapshotRsm>();
        TRequestProcessor staleJointLineageProcessor(staleJointLineageRaft,
            staleJointLineageApplication, staleJointLineagePeers);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + forgedJointStaleBytes.size());
        request->Seqno = 1;
        request->Src = 2; request->Dst = 3; request->Term = 6;
        request->SnapshotIndex = forgedJointStaleImage.Index; request->SnapshotTerm = forgedJointStaleImage.Term;
        request->Offset = 0; request->TotalBytes = forgedJointStaleBytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 2;
        memcpy(request->Data, forgedJointStaleBytes.data(), forgedJointStaleBytes.size());
        staleJointLineageRaft->Process({}, request);
        const auto installed = staleJointLineageState->GetSnapshot();
        assert_true(installed && installed->ConfigurationTransitions.size() == 2);
        assert_true(installed->ConfigurationTransitions[0] == image.ConfigurationTransitions.back());
        assert_true(installed->ConfigurationTransitions[1] == jointImage.ConfigurationTransitions.back());
        assert_true(staleJointLineageState->GetBoundVoters() == original);
        assert_int_equal(staleJointLineageApplication->LastAppliedIndex, forgedJointStaleImage.Index);
        assert_true(fromTwo->Messages.size() == 1);
        assert_true(fromTwo->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);
    }

    // A joint receiver with only MRFTS005 lineage must not adopt unverified
    // earlier transitions from a longer stable successor chain either.
    auto sparseJointImage = jointImage;
    sparseJointImage.ConfigurationTransitions.clear();
    assert_true(EncodeSnapshot(sparseJointImage).substr(0, 8) == "MRFTS005");
    auto forgedSuccessorImage = stableSuccessorImage;
    forgedSuccessorImage.ConfigurationTransitions.front().OldVoters = {7, 8, 9};
    const auto forgedSuccessorBytes = EncodeSnapshot(forgedSuccessorImage);
    {
        auto sparseJointState = std::make_shared<TDurableState>((dir / "sparse-joint").string(), 3);
        sparseJointState->BindVoters(original);
        sparseJointState->CurrentTerm = 6;
        sparseJointState->InstallSnapshot(sparseJointImage);
        TNodeDict sparseJointPeers;
        std::array<std::shared_ptr<TEndpoint>, 6> sparseJointEndpoints{};
        for (auto peer : {1u, 2u, 4u, 5u}) {
            sparseJointEndpoints[peer] = std::make_shared<TEndpoint>();
            sparseJointPeers.emplace(peer, sparseJointEndpoints[peer]);
        }
        auto sparseJointRaft = std::make_shared<TRaft>(sparseJointState, 3, sparseJointPeers,
            std::vector<uint32_t>{4, 5});
        auto sparseJointApplication = std::make_shared<TSnapshotRsm>();
        TRequestProcessor sparseJointProcessor(sparseJointRaft, sparseJointApplication, sparseJointPeers);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + forgedSuccessorBytes.size());
        request->Seqno = 1;
        request->Src = 4; request->Dst = 3; request->Term = 7;
        request->SnapshotIndex = forgedSuccessorImage.Index; request->SnapshotTerm = forgedSuccessorImage.Term;
        request->Offset = 0; request->TotalBytes = forgedSuccessorBytes.size();
        request->Done = TInstallSnapshotRequest::DoneFlag |
            TInstallSnapshotRequest::JointElectionFenceLeader;
        request->LeaderId = 4;
        memcpy(request->Data, forgedSuccessorBytes.data(), forgedSuccessorBytes.size());
        sparseJointRaft->Process({}, request);
        const auto installed = sparseJointState->GetSnapshot();
        assert_true(installed && installed->ConfigurationTransitions.size() == 1);
        assert_true(installed->ConfigurationTransitions.front() ==
            stableSuccessorImage.ConfigurationTransitions.back());
        assert_true(sparseJointState->GetBoundVoters() == original);
        assert_int_equal(sparseJointApplication->LastAppliedIndex, forgedSuccessorImage.Index);
        assert_true(sparseJointEndpoints[4]->Messages.size() == 1);
        assert_true(sparseJointEndpoints[4]->Messages.back().Cast<TInstallSnapshotResponse>()->Installed);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
}
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_raft_accepts_full_width_voter_ids),
        cmocka_unit_test(test_configuration_snapshot_projection_and_history_resume),
        cmocka_unit_test(test_generated_membership_sets_roundtrip_and_bootstrap_from_snapshots),
        cmocka_unit_test(test_configuration_history_rolls_bounded_transition_suffix_and_resumes),
        cmocka_unit_test(test_history_validates_joint_stable_chain_and_independent_rollback),
        cmocka_unit_test(test_configuration_scan_compares_full_configuration_state),
        cmocka_unit_test(test_history_from_joint_snapshot_requires_a_consistent_identity),
        cmocka_unit_test(test_history_rejects_skips_wrong_predecessors_and_term_regression),
        cmocka_unit_test(test_scan_requires_each_configuration_predecessor_to_be_committed),
        cmocka_unit_test(test_scan_separates_commit_and_replaced_suffix),
        cmocka_unit_test(test_scan_from_joint_snapshot_and_rejects_mismatched_baselines),
        cmocka_unit_test(test_scan_from_stable_configuration_snapshot),
        cmocka_unit_test(test_scan_fails_closed_on_invalid_suffix),
        cmocka_unit_test(test_configuration_roundtrips_both_phases_and_bounded_maximum),
        cmocka_unit_test(test_configuration_rejects_invalid_fields_and_valid_crc_corruption),
        cmocka_unit_test(test_configuration_rpc_and_proposal_are_rejected_before_mutation),
        cmocka_unit_test(test_follower_can_register_future_voter_endpoint_before_joint_entry),
        cmocka_unit_test(test_joint_configuration_readiness_reports_log_and_index_capacity),
        cmocka_unit_test(test_compaction_membership_readiness_reports_acknowledgement_blockers),
        cmocka_unit_test(test_new_voter_requires_snapshot_install_capability),
        cmocka_unit_test(test_joint_configuration_requires_every_member_capability),
        cmocka_unit_test(test_joint_removal_can_progress_without_removed_voter_capability),
        cmocka_unit_test(test_joint_configuration_rejects_legacy_election_requests),
        cmocka_unit_test(test_versioned_disk_state_can_start_voter_removal),
        cmocka_unit_test(test_new_voter_leader_can_forward_prior_term_joint_entry),
        cmocka_unit_test(test_new_side_election_replays_joint_entry_to_old_peer),
        cmocka_unit_test(test_new_stable_leader_replays_transition_to_lagging_joiner),
        cmocka_unit_test(test_runtime_peer_provisioning_commits_joint_membership_across_replicas),
        cmocka_unit_test(test_removed_member_rejoins_from_fresh_snapshot_after_compaction),
        cmocka_unit_test(test_quorum_retires_offline_voter_before_compaction_and_snapshot_rejoin),
        cmocka_unit_test(test_removed_member_rejoins_through_fresh_joint_snapshot_bootstrap),
        cmocka_unit_test(test_fresh_joint_snapshot_rejoin_recovers_voter_role_after_restart),
        cmocka_unit_test(test_joint_membership_addition_and_finalization_replicate_deterministically),
        cmocka_unit_test(test_live_joint_transition_waits_for_both_nontrivial_majorities),
        cmocka_unit_test(test_live_membership_fault_scenarios_transfer_finalize_and_restart),
        cmocka_unit_test(test_live_disjoint_joint_entry_waits_for_old_majority_over_network),
        cmocka_unit_test(test_joint_strong_read_waits_for_both_disjoint_majorities),
        cmocka_unit_test(test_timeout_now_retries_and_falls_back_under_joint_quorum),
        cmocka_unit_test(test_old_side_leader_can_transfer_after_joint_snapshot_with_pending_stable_successor),
        cmocka_unit_test(test_stable_membership_compaction_recovers_active_voters),
        cmocka_unit_test(test_snapshot_install_validates_and_advances_committed_membership_history),
        cmocka_unit_test(test_snapshot_transfer_before_uncommitted_configuration_preserves_suffix),
        cmocka_unit_test(test_joint_snapshot_install_requires_matching_joint_history_and_recovers),
        cmocka_unit_test(test_committed_joint_snapshot_can_install_its_stable_successor),
        cmocka_unit_test(test_committed_joint_log_can_anchor_stable_snapshot_successor),
        cmocka_unit_test(test_stable_snapshot_successor_rejects_conflicting_earlier_transition_history),
        cmocka_unit_test(test_uncommitted_joint_suffix_replacement_restores_old_voters),
        cmocka_unit_test(test_configuration_application_advances_without_calling_state_machine),
#ifdef __linux__
        cmocka_unit_test(test_durable_restart_recovers_stable_membership_roles),
        cmocka_unit_test(test_stable_snapshot_restart_accepts_persisted_vote_for_removed_voter),
        cmocka_unit_test(test_restart_after_membership_removal_needs_only_active_endpoints),
        cmocka_unit_test(test_durable_restart_rejects_reserved_entry_without_rewriting),
        cmocka_unit_test(test_legacy_restart_uses_safe_flag_lookup_and_preserves_files),
        cmocka_unit_test(test_offline_history_scan_after_durable_snapshot_and_restart),
        cmocka_unit_test(test_raft_recovers_joint_quorum_for_candidate_election),
        cmocka_unit_test(test_joint_election_generated_reply_orders),
        cmocka_unit_test(test_raft_keeps_joint_quorum_until_stable_successor_commits),
        cmocka_unit_test(test_raft_rejects_snapshot_when_configured_bootstrap_binding_mismatches),
        cmocka_unit_test(test_persisted_commit_boundary_drives_committed_configuration_scan),
        cmocka_unit_test(test_configuration_snapshots_preserve_original_binding_across_transitions),
        cmocka_unit_test(test_verified_multihop_disjoint_snapshot_recovers_and_readds_stale_voter),
        cmocka_unit_test(test_stale_snapshot_missing_new_voter_target_is_rejected_before_install),
        cmocka_unit_test(test_high_term_snapshot_metadata_spanning_chunks_progresses),
        cmocka_unit_test(test_overlapping_multihop_snapshot_recovers_stale_store_online),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
