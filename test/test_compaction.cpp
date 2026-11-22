#include <miniraft/raft.h>
#include <miniraft/durable.h>
#include <miniraft/snapshot_codec.h>
#include "../examples/kv.h"
#include "../examples/kv_messages.h"
#include <algorithm>
#include <deque>
#include <functional>
#include <set>
#include <filesystem>
#include <fstream>
#ifdef __linux__
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}
namespace {
template<class F> void Reject(F action) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}
struct TEndpoint : INode {
    std::function<void(TMessageHolder<TMessage>)> SendTo;
    explicit TEndpoint(std::function<void(TMessageHolder<TMessage>)> send) : SendTo(std::move(send)) {}
    void Send(TMessageHolder<TMessage> message) override { SendTo(std::move(message)); }
    void Drain() override {}
};
TMessageHolder<TWriteKv> Command(const std::string& key, const std::string& value) {
    auto message = NewHoldedMessage<TWriteKv>(sizeof(TWriteKv) + key.size() + value.size());
    message->Flags = TCommandRequest::EWrite;
    message->KeySize = key.size(); message->ValSize = value.size();
    memcpy(message->TKvEntry::Data, key.data(), key.size());
    memcpy(message->TKvEntry::Data + key.size(), value.data(), value.size());
    return message;
}
std::string Read(TKv& kv, const std::string& key) {
    auto message = NewHoldedMessage<TReadKv>(sizeof(TReadKv) + key.size());
    message->KeySize = key.size(); memcpy(message->Data, key.data(), key.size());
    auto response = kv.Read(message, kv.LastAppliedIndex).Cast<TCommandResponse>();
    return std::string(response->Data, response->Len - sizeof(TCommandResponse));
}
struct TApplication : TKv {
    size_t Loads = 0;
    void LoadSnapshot(std::string_view bytes, uint64_t index) override { TKv::LoadSnapshot(bytes, index); ++Loads; }
};
struct TRejectingSnapshotApplication : TApplication {
    void ValidateSnapshot(std::string_view, uint64_t) const override {
        throw std::runtime_error("Injected application snapshot validation failure");
    }
};
struct TOversizedBoundApplication : TKv {
    mutable size_t SaveCalls = 0;
    std::optional<uint64_t> SnapshotSizeUpperBound() const override { return TSnapshot::MaxBytes; }
    std::string SaveSnapshot() const override { ++SaveCalls; return "oversized"; }
};
void test_kv_snapshot_precomputes_size_without_changing_encoding(void**) {
    TKv source;
    const std::string binaryKey("a\0", 2), binaryValue("\0v", 2);
    source.Write(source.Prepare(Command("z", "last")), 1);
    source.Write(source.Prepare(Command(binaryKey, binaryValue)), 2);

    const auto bytes = source.SaveSnapshot();
    assert_int_equal(source.SnapshotSizeUpperBound().value(), bytes.size());
    std::string expected = "MRKV0001";
    NSnapshotCodec::Put(expected, 2, 8);
    NSnapshotCodec::Put(expected, binaryKey.size(), 4);
    NSnapshotCodec::Put(expected, binaryValue.size(), 4);
    NSnapshotCodec::Append(expected, binaryKey);
    NSnapshotCodec::Append(expected, binaryValue);
    NSnapshotCodec::Put(expected, 1, 4);
    NSnapshotCodec::Put(expected, 4, 4);
    NSnapshotCodec::Append(expected, "z");
    NSnapshotCodec::Append(expected, "last");
    assert_true(bytes == expected);

    TKv restored;
    restored.LoadSnapshot(bytes, source.LastAppliedIndex);
    assert_true(Read(restored, binaryKey) == binaryValue);
    assert_true(Read(restored, "z") == "last");
}
void test_snapshot_capture_preflights_application_size_bound(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto application = std::make_shared<TOversizedBoundApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, {});

    Reject([&] { raft->Compact(); });
    assert_int_equal(application->SaveCalls, 0);
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(state->LastLogIndex, 1);
}
struct TCluster {
    ITimeSource::Time Now{};
    uint32_t NodeCount;
    std::vector<std::shared_ptr<TRaft>> Nodes;
    std::vector<std::shared_ptr<TApplication>> Apps;
    std::vector<std::unique_ptr<TRequestProcessor>> Processors;
    std::deque<TMessageHolder<TMessage>> Packets;
    std::set<uint32_t> Isolated;
    std::vector<uint32_t> Learners;
    explicit TCluster(std::vector<uint32_t> learners = {}, uint32_t nodeCount = 3)
        : NodeCount(nodeCount), Nodes(nodeCount), Apps(nodeCount), Processors(nodeCount),
          Learners(std::move(learners)) {
        for (uint32_t id = 1; id <= NodeCount; ++id) Build(id, std::make_shared<TState>());
    }
    void Build(uint32_t id, std::shared_ptr<IState> state) {
        TNodeDict peers;
        for (uint32_t dst = 1; dst <= NodeCount; ++dst) if (dst != id)
            peers[dst] = std::make_shared<TEndpoint>([this](auto message) { Packets.push_back(std::move(message)); });
        Processors[id - 1].reset();
        Nodes[id - 1] = std::make_shared<TRaft>(state, id, peers, Learners);
        Apps[id - 1] = std::make_shared<TApplication>();
        Processors[id - 1] = std::make_unique<TRequestProcessor>(Nodes[id - 1], Apps[id - 1], peers);
    }
    void Restart(uint32_t id) { auto state = Nodes[id - 1]->GetState(); Build(id, state); }
    void Deliver(TMessageHolder<TMessage> message, bool duplicate = false) {
        const auto dst = message.Cast<TMessageEx>()->Dst;
        Nodes[dst - 1]->Process(Now, message); Processors[dst - 1]->ProcessCommitted();
        if (duplicate) { Nodes[dst - 1]->Process(Now, message); Processors[dst - 1]->ProcessCommitted(); }
    }
    void Drain(bool duplicate = false) {
        size_t budget = 10000;
        while (!Packets.empty()) {
            assert_true(budget-- > 0);
            auto message = Packets.front(); Packets.pop_front();
            auto header = message.Cast<TMessageEx>();
            if (!Isolated.contains(header->Src) && !Isolated.contains(header->Dst)) Deliver(message, duplicate);
        }
    }
    void Elect(uint32_t id) {
        Now += std::chrono::seconds(30);
        Nodes[id - 1]->ProcessTimeout(Now); Drain();
        Nodes[id - 1]->ProcessTimeout(Now); Drain();
        Nodes[id - 1]->ProcessTimeout(Now); Drain();
        assert_int_equal(Nodes[id - 1]->CurrentStateName(), EState::LEADER);
    }
    void Pump(uint32_t leader, int rounds = 40, bool duplicate = false) {
        for (int i = 0; i < rounds; ++i) {
            Now += std::chrono::milliseconds(100);
            Nodes[leader - 1]->ProcessTimeout(Now); Drain(duplicate);
            for (auto& processor : Processors) { processor->ProcessCommitted(); processor->ProcessWaiting(); }
        }
    }
    void Write(uint32_t leader, const std::string& key, const std::string& value) {
        Processors[leader - 1]->OnCommandRequest(Command(key, value), {}); Pump(leader);
    }
    void Prepare() {
        Elect(1); Pump(1);
        assert_true(Nodes[0]->GetVolatileState()->PeerCapabilities.at(2).InstallSnapshot);
        assert_true(Nodes[0]->GetVolatileState()->PeerCapabilities.at(3).InstallSnapshot);
        Isolated = {3};
        for (const std::string key : {"a", "b", "c"}) Write(1, key, std::string(60000, key[0]));
        Nodes[0]->Compact(); Nodes[1]->Compact();
        assert_true(Nodes[0]->GetState()->SnapshotIndex > 1);
        Packets.clear(); Isolated.clear();
    }
    TMessageHolder<TMessage> Take(uint32_t src, uint32_t dst, EMessageType type) {
        for (auto it = Packets.begin(); it != Packets.end(); ++it) {
            auto header = it->Cast<TMessageEx>();
            if (header->Src == src && header->Dst == dst && header->Type == uint32_t(type)) {
                auto message = *it; Packets.erase(it); return message;
            }
        }
        fail_msg("Missing packet %u -> %u type %u", src, dst, uint32_t(type)); return {};
    }
    TMessageHolder<TInstallSnapshotRequest> SendChunk() {
        Now += std::chrono::seconds(3);
        Nodes[0]->ProcessTimeout(Now);
        return Take(1, 3, EMessageType::INSTALL_SNAPSHOT_REQUEST).Cast<TInstallSnapshotRequest>();
    }
    TMessageHolder<TInstallSnapshotResponse> AckChunk(TMessageHolder<TInstallSnapshotRequest> request) {
        Deliver(request);
        auto response = Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE).Cast<TInstallSnapshotResponse>();
        Deliver(response); return response;
    }
    void ReprobeSnapshotPeer(uint32_t peer) {
        Packets.clear();
        Nodes[0]->ProcessTimeout(Now);
        auto probe = Take(1, peer, EMessageType::APPEND_ENTRIES_REQUEST);
        Deliver(probe);
        auto response = Take(peer, 1, EMessageType::APPEND_ENTRIES_RESPONSE);
        Deliver(response);
        Packets.clear();
    }
};
void test_shared_compaction_reuses_storage_snapshot_image(void**) {
    TCluster cluster;
    cluster.Elect(1);
    cluster.Pump(1);
    cluster.Write(1, "key", "value");

    const auto snapshot = cluster.Nodes[0]->CompactShared();
    const auto stored = cluster.Nodes[0]->GetState()->GetSnapshot();
    assert_true(snapshot == stored);
    assert_int_equal(snapshot->Index, cluster.Apps[0]->LastAppliedIndex);
    assert_true(Read(*cluster.Apps[0], "key") == "value");
}
void test_zero_index_follower_compaction_waits_for_capability_barrier(void**) {
    TCluster cluster;
    cluster.Elect(1);
    cluster.Pump(1);

    auto* followerState = const_cast<TVolatileState*>(cluster.Nodes[1]->GetVolatileState());
    auto readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_false(readiness.IsLeader);
    assert_int_equal(readiness.ConfigurationIndex, 0);
    assert_true(readiness.RequiresLeaderAcknowledgementBarrier);
    assert_true(readiness.HasLeaderAcknowledgementBarrier);
    assert_true(readiness.Ready());
    assert_int_equal(followerState->CompactionSafeConfigurationTerm,
        cluster.Nodes[1]->GetState()->CurrentTerm);

    const auto certificateTerm = followerState->CompactionSafeConfigurationTerm;
    ++cluster.Nodes[1]->GetState()->CurrentTerm;
    readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_false(readiness.HasLeaderAcknowledgementBarrier);
    assert_false(readiness.Ready());
    Reject([&] { cluster.Nodes[1]->Compact(); });
    cluster.Nodes[1]->GetState()->CurrentTerm = certificateTerm;

    followerState->HasCompactionSafeInitialConfigurationBarrier = false;
    readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_false(readiness.HasLeaderAcknowledgementBarrier);
    assert_false(readiness.Ready());
    Reject([&] { cluster.Nodes[1]->Compact(); });

    followerState->HasCompactionSafeInitialConfigurationBarrier = true;
    assert_true(cluster.Nodes[1]->CheckCompactionMembershipReadiness().Ready());
    assert_true(cluster.Nodes[1]->Compact().Index > 0);
}
void test_snapshot_history_encoding_is_negotiated_per_peer(void**) {
    const std::vector<uint32_t> stableVoters{1, 5}, addedVoters{1, 2, 5};
    auto history = TConfigurationHistory::Stable(stableVoters, 0, 0, 0, 0);
    auto ordinary = NewHoldedMessage<TLogEntry>();
    ordinary->Term = 1;
    history = history.Observe(1, ordinary);
    for (size_t transition = 0; transition < 258; ++transition) {
        const auto jointIndex = history.ThroughIndex() + 1;
        const auto& oldVoters = history.OldVoters();
        const auto& newVoters = oldVoters == stableVoters ? addedVoters : stableVoters;
        TConfigurationRecord record{EConfigurationPhase::Joint, 2,
            history.ConfigurationIndex(), history.ConfigurationTerm(), oldVoters, newVoters};
        history = history.Advance(jointIndex, EncodeConfigurationEntry(record));
        record.Phase = EConfigurationPhase::Stable;
        record.PreviousIndex = jointIndex;
        record.PreviousTerm = 2;
        history = history.Advance(jointIndex + 1, EncodeConfigurationEntry(record));
    }
    const auto snapshot = history.MakeSnapshot("negotiated history image");
    assert_int_equal(snapshot.ConfigurationTransitions.size(), 258);
    assert_true(snapshot.StableConfiguration);

    auto state = std::make_shared<TState>();
    state->BindVoters(stableVoters);
    state->CurrentTerm = snapshot.Term;
    state->Commit();
    state->InstallSnapshot(snapshot);

    std::shared_ptr<TRaft> raft;
    ITimeSource::Time now{};
    std::unordered_map<uint32_t, TMessageHolder<TInstallSnapshotRequest>> sentSnapshots;
    bool downgradePeer2AfterRejection = false;
    TNodeDict peers;
    for (const auto id : {2u, 3u, 4u, 5u, 6u, 7u}) {
        peers.emplace(id, std::make_shared<TEndpoint>([&, id](auto message) {
            if (message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
                auto append = message.template Cast<TAppendEntriesRequest>();
                uint32_t features = TAppendEntriesResponse::SupportsConfigurationBarrier;
                if (id != 5) features |= TAppendEntriesResponse::SupportsInstallSnapshot;
                if (id == 2 && !downgradePeer2AfterRejection)
                    features |= TAppendEntriesResponse::SupportsSnapshotHistory1024;
                if (id == 3) features |= TAppendEntriesResponse::SupportsSnapshotHistory256;
                if (id == 4) features |= TAppendEntriesResponse::SupportsSnapshotHistory32;
                if (id == 6) features |= TAppendEntriesResponse::SupportsSnapshotLineage;
                auto response = NewHoldedMessage(
                    TMessageEx{.Src = id, .Dst = 1, .Term = append->Term, .Seqno = append->Seqno},
                    TAppendEntriesResponse{.MatchIndex = 0, .Success = 0, .Padding = features});
                raft->Process(now, std::move(response));
            } else if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST)) {
                sentSnapshots[id] = message.template Cast<TInstallSnapshotRequest>();
            } else if (id == 5 && message->Type == uint32_t(EMessageType::PRE_VOTE_REQUEST)) {
                auto request = message.template Cast<TPreVoteRequest>();
                auto response = NewHoldedMessage(
                    TMessageEx{.Src = id, .Dst = 1, .Term = request->Term, .Seqno = request->Seqno},
                    TPreVoteResponse{.ProspectiveTerm = request->ProspectiveTerm, .VoteGranted = 1});
                raft->Process(now, std::move(response));
            } else if (id == 5 && message->Type == uint32_t(EMessageType::REQUEST_VOTE_REQUEST)) {
                auto request = message.template Cast<TRequestVoteRequest>();
                auto response = NewHoldedMessage(
                    TMessageEx{.Src = id, .Dst = 1, .Term = request->Term, .Seqno = request->Seqno},
                    TRequestVoteResponse{.VoteGranted = 1});
                raft->Process(now, std::move(response));
            }
        }));
    }
    raft = std::make_shared<TRaft>(state, 1, peers, std::vector<uint32_t>{2, 3, 4, 6, 7});
    for (unsigned attempt = 0; attempt < 30 && sentSnapshots.size() != 5; ++attempt) {
        now += std::chrono::seconds(1);
        raft->ProcessTimeout(now);
    }

    assert_int_equal(raft->CurrentStateName(), EState::LEADER);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(2).SnapshotHistory1024);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(2).SnapshotHistory256);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(2).SnapshotHistory32);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(3).SnapshotHistory256);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(3).SnapshotHistory1024);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(3).SnapshotHistory32);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(4).SnapshotHistory32);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(4).SnapshotHistory256);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(4).SnapshotHistory1024);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(2).InstallSnapshot);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(3).InstallSnapshot);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(4).InstallSnapshot);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(5).InstallSnapshot);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(6).InstallSnapshot);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(6).SnapshotHistory32);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(6).SnapshotLineage);
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(7).SnapshotLineage);
    assert_true(sentSnapshots.contains(2));
    assert_true(sentSnapshots.contains(3));
    assert_false(sentSnapshots.contains(5));
    assert_true(sentSnapshots.contains(4));
    assert_true(sentSnapshots.contains(6));
    assert_true(sentSnapshots.contains(7));
    for (const auto id : {2u, 3u, 4u, 6u, 7u}) {
        auto request = sentSnapshots.at(id);
        assert_true(request->Done);
        const std::string wire(request->Data, request->Len - sizeof(TInstallSnapshotRequest));
        const auto decoded = DecodeSnapshot(wire);
        if (id == 2) {
            assert_true(wire.substr(0, 8) == "MRFTS010");
            assert_int_equal(decoded.ConfigurationTransitions.size(), 258);
        } else if (id == 3) {
            assert_true(wire.substr(0, 8) == "MRFTS008");
            assert_int_equal(decoded.ConfigurationTransitions.size(), 256);
            assert_true(std::equal(decoded.ConfigurationTransitions.begin(), decoded.ConfigurationTransitions.end(),
                snapshot.ConfigurationTransitions.end() - 256));
        } else if (id == 4) {
            assert_true(wire.substr(0, 8) == "MRFTS006");
            assert_int_equal(decoded.ConfigurationTransitions.size(), 32);
            assert_true(std::equal(decoded.ConfigurationTransitions.begin(), decoded.ConfigurationTransitions.end(),
                snapshot.ConfigurationTransitions.end() - 32));
        } else if (id == 6) {
            assert_true(wire.substr(0, 8) == "MRFTS004");
            assert_true(decoded.ConfigurationTransitions.empty());
            assert_true(decoded.HasStablePredecessor());
            assert_true(decoded.PreviousVoters == snapshot.PreviousVoters);
        } else {
            assert_true(wire.substr(0, 8) == "MRFTS003");
            assert_true(decoded.ConfigurationTransitions.empty());
            assert_false(decoded.HasConfigurationLineage());
            assert_true(decoded.StableConfiguration);
            assert_int_equal(decoded.ConfigurationIndex, snapshot.ConfigurationIndex);
            assert_int_equal(decoded.ConfigurationTerm, snapshot.ConfigurationTerm);
        }
    }

    // A rejected transfer invalidates the old capability advertisement. If
    // the new probe reports fewer features, the retry must select a compatible
    // encoding instead of reusing the pinned, previously rejected encoding.
    auto oldEncodingRequest = sentSnapshots.at(2);
    assert_true(std::string_view(oldEncodingRequest->Data, 8) == "MRFTS010");
    downgradePeer2AfterRejection = true;
    raft->Process(now, NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = oldEncodingRequest->Term,
            .Seqno = oldEncodingRequest->Seqno},
        TInstallSnapshotResponse{.SnapshotIndex = oldEncodingRequest->SnapshotIndex}));
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(2).InstallSnapshot);

    ++now;
    raft->ProcessTimeout(now); // Boundary probe refreshes peer 2's capabilities.
    assert_false(raft->GetVolatileState()->PeerCapabilities.at(2).SnapshotHistory1024);
    assert_true(raft->GetVolatileState()->PeerCapabilities.at(2).InstallSnapshot);
    now += TTimeout::Rpc;
    raft->ProcessTimeout(now);
    auto retry = sentSnapshots.at(2);
    assert_true(retry->Done);
    assert_true(std::string_view(retry->Data, 8) == "MRFTS003");
    auto retrySnapshot = DecodeSnapshot(std::string_view(retry->Data,
        retry->Len - sizeof(TInstallSnapshotRequest)));
    assert_true(retrySnapshot.ConfigurationTransitions.empty());
    assert_false(retrySnapshot.HasConfigurationLineage());

    std::string compactionError;
    try {
        (void)raft->Compact();
    } catch (const std::logic_error& error) {
        compactionError = error.what();
    }
    assert_string_equal(compactionError.c_str(),
        "Compaction requires every active voter to advertise InstallSnapshot support");
}
void test_compaction_lagging_follower_installs_snapshot_then_tail(void**) {
    TCluster cluster; cluster.Prepare();
    const auto boundary = cluster.Nodes[0]->GetState()->SnapshotIndex;
    cluster.Isolated = {3}; cluster.Write(1, "tail", "after snapshot"); cluster.Isolated.clear();
    cluster.Pump(1, 80, true);
    assert_int_equal(cluster.Apps[2]->Loads, 1);
    assert_int_equal(cluster.Nodes[2]->GetState()->SnapshotIndex, boundary);
    for (auto& app : cluster.Apps) {
        assert_true(Read(*app, "a") == std::string(60000, 'a'));
        assert_true(Read(*app, "tail") == "after snapshot");
        assert_int_equal(app->LastAppliedIndex, cluster.Nodes[0]->GetLastIndex());
    }
    assert_false(cluster.Nodes[0]->GetState()->Get(1));
}
void test_simultaneous_startup_candidates_use_distinct_election_deadlines(void**) {
    TCluster cluster;
    for (auto& node : cluster.Nodes) node->ProcessTimeout(cluster.Now);
    cluster.Drain(); // All campaign without persisting a term; grants remain speculative.
    std::set<ITimeSource::Time> deadlines;
    uint32_t first = 1;
    for (uint32_t id = 1; id <= 3; ++id) {
        const auto due = cluster.Nodes[id - 1]->GetVolatileState()->ElectionDue;
        deadlines.insert(due);
        if (due < cluster.Nodes[first - 1]->GetVolatileState()->ElectionDue) first = id;
    }
    assert_int_equal(deadlines.size(), 3);
    cluster.Now = *deadlines.begin();
    cluster.Nodes[first - 1]->ProcessTimeout(cluster.Now); cluster.Drain();
    cluster.Nodes[first - 1]->ProcessTimeout(cluster.Now);
    assert_int_equal(cluster.Nodes[first - 1]->CurrentStateName(), EState::LEADER);
}
void test_partial_transfer_restart_and_delayed_ack_are_safe(void**) {
    TCluster cluster; cluster.Prepare();
    auto first = cluster.SendChunk();
    auto delayed = cluster.AckChunk(first);
    assert_false(delayed->Installed);
    assert_int_equal(cluster.Apps[2]->Loads, 0);
    auto invalid = NewHoldedMessage(TMessageEx{.Src = 3, .Dst = 1, .Term = first->Term, .Seqno = first->Seqno},
        TInstallSnapshotResponse{.SnapshotIndex = first->SnapshotIndex, .NextOffset = 1, .Success = 1, .Installed = 1});
    cluster.Deliver(invalid);
    assert_true(cluster.Nodes[0]->GetVolatileState()->MatchIndex.at(3) < first->SnapshotIndex);
    cluster.Restart(3); // all partial bytes disappear, stable log/vote remain
    auto second = cluster.SendChunk(); assert_true(second->Offset > 0);
    auto failure = cluster.AckChunk(second); assert_false(failure->Success);
    cluster.Deliver(delayed); // stale sequence cannot restore old progress
    cluster.ReprobeSnapshotPeer(3);
    auto restarted = cluster.SendChunk(); assert_int_equal(restarted->Offset, 0);
    cluster.AckChunk(restarted); cluster.Pump(1, 80, true);
    assert_int_equal(cluster.Apps[2]->Loads, 1);
    assert_true(Read(*cluster.Apps[2], "c") == std::string(60000, 'c'));
}
void test_snapshot_partial_ack_cannot_skip_unsent_bytes(void**) {
    TCluster cluster; cluster.Prepare();
    auto chunk = cluster.SendChunk();
    const auto end = chunk->Offset + chunk->Len - sizeof(TInstallSnapshotRequest);
    assert_true(end + 1 < chunk->TotalBytes);
    auto invalid = NewHoldedMessage(TMessageEx{.Src = 3, .Dst = 1, .Term = chunk->Term, .Seqno = chunk->Seqno},
        TInstallSnapshotResponse{.SnapshotIndex = chunk->SnapshotIndex, .NextOffset = end + 1, .Success = 1});
    cluster.Deliver(invalid);
    auto retry = cluster.SendChunk();
    assert_int_equal(retry->Offset, chunk->Offset); assert_int_equal(retry->Seqno, chunk->Seqno);
    cluster.AckChunk(retry); auto next = cluster.SendChunk();
    assert_int_equal(next->Offset, end);
    cluster.AckChunk(next); cluster.Pump(1, 80, true);
    assert_int_equal(cluster.Apps[2]->Loads, 1);
}
void ExerciseDelayedSnapshotAcks(bool readTraffic) {
    for (auto learners : {std::vector<uint32_t>{}, std::vector<uint32_t>{3}}) {
        TCluster cluster(learners); cluster.Prepare();
        auto acknowledgeVoter2Heartbeat = [&] {
            for (auto it = cluster.Packets.begin(); it != cluster.Packets.end(); ++it) {
                const auto header = it->Cast<TMessageEx>();
                if (header->Src != 1 || header->Dst != 2 ||
                    header->Type != uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) continue;
                auto heartbeat = *it;
                cluster.Packets.erase(it);
                cluster.Deliver(heartbeat);
                auto response = cluster.Take(2, 1, EMessageType::APPEND_ENTRIES_RESPONSE);
                cluster.Deliver(response);
                return;
            }
            fail_msg("Missing heartbeat 1 -> 2 while snapshot ACK is delayed");
        };
        const auto boundary = cluster.Nodes[0]->GetState()->SnapshotIndex;
        auto chunk = cluster.SendChunk();
        size_t chunks = 0;
        while (true) {
            assert_true(++chunks < 10);
            cluster.Deliver(chunk);
            auto ack = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE).Cast<TInstallSnapshotResponse>();
            // Every ACK arrives after several retries. Drop retry packets while
            // preserving the original successful reply, a valid lossy schedule.
            for (int retry = 0; retry < 3; ++retry) {
                if (readTraffic) cluster.Nodes[0]->ApproveRead();
                else { cluster.Now += std::chrono::seconds(3); cluster.Nodes[0]->ProcessTimeout(cluster.Now); }
                auto retransmit = cluster.Take(1, 3, EMessageType::INSTALL_SNAPSHOT_REQUEST).Cast<TInstallSnapshotRequest>();
                assert_int_equal(retransmit->Offset, chunk->Offset);
                acknowledgeVoter2Heartbeat();
                cluster.Packets.clear();
            }
            cluster.Deliver(ack);
            if (ack->Installed) break;
            cluster.Deliver(ack); // Also stale between consuming an ACK and the next send.
            auto next = cluster.SendChunk();
            assert_int_equal(next->Offset, ack->NextOffset);
            // A duplicate ACK for the previous chunk must not consume the new
            // chunk's correlation or reset its progress.
            cluster.Deliver(ack);
            chunk = next;
        }
        assert_true(chunks > 1);
        assert_int_equal(cluster.Nodes[0]->GetVolatileState()->MatchIndex.at(3), boundary);
        assert_int_equal(cluster.Apps[2]->Loads, 1);
        cluster.Write(1, "tail", "after delayed ACKs");
        cluster.Pump(1, 80);
        assert_true(Read(*cluster.Apps[2], "tail") == "after delayed ACKs");
        if (!learners.empty()) assert_true(cluster.Nodes[0]->IsLearnerCaughtUp(3));
    }
}
void test_snapshot_timeout_retries_do_not_starve_delayed_acks(void**) { ExerciseDelayedSnapshotAcks(false); }
void test_snapshot_read_traffic_does_not_starve_delayed_acks(void**) { ExerciseDelayedSnapshotAcks(true); }
void test_compaction_during_each_chunk_cannot_restart_snapshot_catchup(void**) {
    for (auto learners : {std::vector<uint32_t>{}, std::vector<uint32_t>{3}}) {
        TCluster cluster(learners); cluster.Prepare();
        auto chunk = cluster.SendChunk();
        const auto boundary = chunk->SnapshotIndex;
        std::weak_ptr<const TSnapshot> original = cluster.Nodes[0]->GetState()->GetSnapshot();
        size_t chunks = 0;
        while (true) {
            assert_true(++chunks < 10);
            cluster.Deliver(chunk);
            auto ack = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE).Cast<TInstallSnapshotResponse>();
            // Commit and compact again before every ACK arrives, including the
            // final one. The majority remains healthy while the receiver lags.
            cluster.Isolated = {3}; cluster.Packets.clear();
            cluster.Write(1, "latest", std::to_string(chunks));
            cluster.Nodes[0]->Compact();
            assert_true(cluster.Nodes[0]->GetState()->SnapshotIndex > boundary);
            assert_false(original.expired()); // Owned by the active transfer.
            cluster.Isolated.clear();
            auto retry = cluster.SendChunk();
            assert_int_equal(retry->SnapshotIndex, boundary);
            assert_int_equal(retry->Offset, chunk->Offset);
            cluster.Packets.clear(); cluster.Deliver(ack);
            if (ack->Installed) break;
            chunk = cluster.SendChunk();
            assert_int_equal(chunk->SnapshotIndex, boundary);
            assert_int_equal(chunk->Offset, ack->NextOffset);
        }
        assert_true(chunks > 1);
        assert_true(original.expired()); // Completion releases the pinned image.
        assert_int_equal(cluster.Apps[2]->Loads, 1);
        assert_int_equal(cluster.Nodes[2]->GetState()->SnapshotIndex, boundary);
        // The older completed image does not claim the leader's newer prefix.
        assert_int_equal(cluster.Nodes[0]->GetVolatileState()->MatchIndex.at(3), boundary);
        assert_true(Read(*cluster.Apps[2], "latest").empty());
        cluster.Packets.clear();
        auto newer = cluster.SendChunk();
        assert_true(newer->SnapshotIndex > boundary); assert_int_equal(newer->Offset, 0);
        cluster.AckChunk(newer); cluster.Pump(1, 80, true);
        assert_true(Read(*cluster.Apps[2], "latest") == std::to_string(chunks));
        cluster.Write(1, "tail", "after compaction during transfer"); cluster.Pump(1, 80);
        assert_true(Read(*cluster.Apps[2], "tail") == "after compaction during transfer");
        if (!learners.empty()) assert_true(cluster.Nodes[0]->IsLearnerCaughtUp(3));
    }
}
void test_stalled_snapshot_transfer_expires_and_releases_pinned_generation(void**) {
    TCluster cluster({4}, 4); cluster.Prepare();
    auto firstChunk = cluster.SendChunk();
    const auto firstIndex = firstChunk->SnapshotIndex;
    cluster.Deliver(firstChunk); // Lose the ACK; peer 3 keeps the first image pinned.
    cluster.Packets.clear();
    auto* progress = const_cast<TVolatileState*>(cluster.Nodes[0]->GetVolatileState());
    assert_true(progress->SnapshotRpcs.contains(3));

    cluster.Isolated = {3, 4};
    cluster.Write(1, "generation", "two");
    auto second = cluster.Nodes[0]->Compact();
    assert_true(second.Index > firstIndex);
    cluster.Write(1, "generation", "three");
    assert_true(cluster.Apps[0]->LastAppliedIndex > second.Index);

    // Force the lagging learner to begin a transfer from the newer generation.
    progress->NextIndex[4] = 1;
    cluster.Packets.clear();
    cluster.Nodes[0]->ApproveRead();
    auto secondChunk = cluster.Take(1, 4, EMessageType::INSTALL_SNAPSHOT_REQUEST).Cast<TInstallSnapshotRequest>();
    assert_int_equal(secondChunk->SnapshotIndex, second.Index);
    assert_int_equal(secondChunk->Offset, 0);
    assert_true(secondChunk->TotalBytes > TInstallSnapshotRequest::MaxChunkBytes);
    assert_int_equal(progress->SnapshotRpcs.at(3).Snapshot->Index, firstIndex);
    assert_int_equal(progress->SnapshotRpcs.at(4).Snapshot->Index, second.Index);

    const auto storedBoundary = cluster.Nodes[0]->GetState()->SnapshotIndex;
    const auto storedLastIndex = cluster.Nodes[0]->GetState()->LastLogIndex;
    const auto appliedIndex = cluster.Apps[0]->LastAppliedIndex;
    Reject([&] { cluster.Nodes[0]->Compact(); });
    assert_int_equal(cluster.Nodes[0]->GetState()->SnapshotIndex, storedBoundary);
    assert_int_equal(cluster.Nodes[0]->GetState()->LastLogIndex, storedLastIndex);
    assert_int_equal(cluster.Apps[0]->LastAppliedIndex, appliedIndex);
    assert_true(progress->SnapshotRpcs.contains(3)); assert_true(progress->SnapshotRpcs.contains(4));

    // A peer that has stopped making progress eventually releases its old
    // immutable image, allowing compaction to proceed while another transfer
    // remains active.
    std::weak_ptr<const TSnapshot> stalledImage = progress->SnapshotRpcs.at(3).Snapshot;
    // Keep the leader's quorum lease alive with voter 2 while the learner
    // advances and the isolated voter 3 remains stalled.
    cluster.Now += std::chrono::seconds(10);
    cluster.Deliver(secondChunk);
    auto partialAck = cluster.Take(4, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_false(partialAck->Installed);
    cluster.Deliver(partialAck);

    auto deliverVoterTwoTraffic = [&] {
        while (!cluster.Packets.empty()) {
            auto message = cluster.Packets.front(); cluster.Packets.pop_front();
            const auto header = message.Cast<TMessageEx>();
            if ((header->Src == 1 && header->Dst == 2) || (header->Src == 2 && header->Dst == 1))
                cluster.Deliver(message);
        }
    };
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    deliverVoterTwoTraffic();
    cluster.Now += std::chrono::seconds(10);
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    auto nextChunk = cluster.Take(1, 4, EMessageType::INSTALL_SNAPSHOT_REQUEST)
        .Cast<TInstallSnapshotRequest>();
    deliverVoterTwoTraffic();
    cluster.Deliver(nextChunk);
    partialAck = cluster.Take(4, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_false(partialAck->Installed);
    cluster.Deliver(partialAck);
    cluster.Now += std::chrono::seconds(10);
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    deliverVoterTwoTraffic();
    assert_false(progress->SnapshotRpcs.contains(3));
    assert_true(progress->SnapshotRpcs.contains(4));
    assert_true(stalledImage.expired());
    cluster.Now += TTimeout::Rpc;
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    auto retryChunk = cluster.Take(1, 4, EMessageType::INSTALL_SNAPSHOT_REQUEST)
        .Cast<TInstallSnapshotRequest>();
    cluster.Packets.clear();
    const auto afterIdleExpiry = cluster.Nodes[0]->Compact();
    assert_true(afterIdleExpiry.Index > storedBoundary);

    // The learner can still complete its pinned image after this newer
    // compaction; its installed ACK releases that remaining generation.
    bool installed = false;
    for (unsigned attempts = 0; !installed && attempts < 8; ++attempts) {
        auto request = attempts == 0 ? std::move(retryChunk) :
            cluster.Take(1, 4, EMessageType::INSTALL_SNAPSHOT_REQUEST).Cast<TInstallSnapshotRequest>();
        cluster.Deliver(request);
        auto ack = cluster.Take(4, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
            .Cast<TInstallSnapshotResponse>();
        installed = ack->Installed;
        cluster.Deliver(ack);
        if (!installed) cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    }
    assert_true(installed);
    assert_false(progress->SnapshotRpcs.contains(4));
    if (auto restarted = progress->SnapshotRpcs.find(3); restarted != progress->SnapshotRpcs.end())
        assert_true(restarted->second.Snapshot->Index > firstIndex);
    cluster.Packets.clear();
    auto latest = cluster.Nodes[0]->Compact();
    assert_true(latest.Index > storedBoundary);
    assert_int_equal(cluster.Nodes[0]->GetState()->SnapshotIndex, latest.Index);
}
void test_rejected_snapshot_reprobes_peer_capabilities_before_retry(void**) {
    TCluster cluster; cluster.Prepare();
    auto chunk = cluster.SendChunk();
    auto rejected = NewHoldedMessage(
        TMessageEx{.Src = 3, .Dst = 1, .Term = chunk->Term, .Seqno = chunk->Seqno},
        TInstallSnapshotResponse{.SnapshotIndex = chunk->SnapshotIndex});
    cluster.Deliver(rejected);

    auto readiness = cluster.Nodes[0]->CheckCompactionMembershipReadiness();
    assert_true(readiness.ActiveVotersWithoutSnapshotInstallSupport == (std::vector<uint32_t>{3}));
    assert_false(readiness.Ready());

    // A current capability probe distinguishes an old or restarted peer from
    // a transient snapshot rejection before the leader retries the image.
    cluster.Packets.clear();
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    auto probe = cluster.Take(1, 3, EMessageType::APPEND_ENTRIES_REQUEST);
    cluster.Deliver(probe);
    auto probeResponse = cluster.Take(3, 1, EMessageType::APPEND_ENTRIES_RESPONSE);
    cluster.Deliver(probeResponse);
    const auto& capability = cluster.Nodes[0]->GetVolatileState()->PeerCapabilities.at(3);
    assert_true(capability.InstallSnapshot);
    assert_int_equal(capability.Term, cluster.Nodes[0]->GetState()->CurrentTerm);

    cluster.Packets.clear();
    cluster.Nodes[0]->ProcessTimeout(cluster.Now + TTimeout::Rpc);
    auto retry = cluster.Take(1, 3, EMessageType::INSTALL_SNAPSHOT_REQUEST)
        .Cast<TInstallSnapshotRequest>();
    assert_int_equal(retry->SnapshotIndex, chunk->SnapshotIndex);
    assert_int_equal(retry->Offset, 0);
}
void test_leader_change_mid_snapshot_rejects_old_chunks(void**) {
    TCluster cluster; cluster.Prepare();
    auto old = cluster.SendChunk(); cluster.AckChunk(old);
    cluster.Packets.clear(); cluster.Isolated = {1};
    cluster.Elect(2); cluster.Pump(2, 80, true);
    assert_true(cluster.Nodes[2]->GetState()->CurrentTerm > old->Term);
    const auto loads = cluster.Apps[2]->Loads;
    cluster.Deliver(old);
    assert_int_equal(cluster.Apps[2]->Loads, loads);
    cluster.Write(2, "new leader", "tail");
    assert_true(Read(*cluster.Apps[2], "new leader") == "tail");
}
void test_partial_and_installed_snapshot_acks_do_not_confirm_strong_reads(void**) {
    TCluster cluster; cluster.Prepare(); cluster.Isolated = {2};
    size_t replies = 0;
    auto client = std::make_shared<TEndpoint>([&](auto message) {
        assert_int_equal(message.template Cast<TCommandResponse>()->ErrorCode, 0); ++replies;
    });
    auto read = NewHoldedMessage<TReadKv>(sizeof(TReadKv) + 1);
    read->KeySize = 1; read->Data[0] = 'a'; read->Flags = TCommandRequest::EConsistent;
    cluster.Processors[0]->OnCommandRequest(read, client);
    auto chunk = cluster.Take(1, 3, EMessageType::INSTALL_SNAPSHOT_REQUEST).Cast<TInstallSnapshotRequest>();
    while (true) {
        auto response = cluster.AckChunk(chunk);
        cluster.Processors[0]->ProcessWaiting(); assert_int_equal(replies, 0);
        if (response->Installed) break;
        chunk = cluster.SendChunk();
    }
    cluster.Pump(1); assert_int_equal(replies, 1);
}
void test_compacted_follower_hint_requires_boundary_probe_before_match(void**) {
    TCluster cluster; cluster.Elect(1); cluster.Write(1, "key", "value");
    cluster.Nodes[1]->Compact();
    const auto boundary = cluster.Nodes[1]->GetState()->SnapshotIndex;
    auto* progress = const_cast<TVolatileState*>(cluster.Nodes[0]->GetVolatileState());
    progress->NextIndex[2] = 1; progress->MatchIndex[2] = 0; progress->RpcDue[2] = {};
    cluster.Now += std::chrono::seconds(3); cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    cluster.Deliver(cluster.Take(1, 2, EMessageType::APPEND_ENTRIES_REQUEST));
    auto hint = cluster.Take(2, 1, EMessageType::APPEND_ENTRIES_RESPONSE).Cast<TAppendEntriesResponse>();
    assert_false(hint->Success); assert_int_equal(hint->MatchIndex, boundary);
    cluster.Deliver(hint);
    assert_int_equal(progress->MatchIndex[2], 0); assert_int_equal(progress->NextIndex[2], boundary + 1);
    cluster.Pump(1); assert_int_equal(progress->MatchIndex[2], cluster.Nodes[0]->GetLastIndex());
}

TMessageHolder<TInstallSnapshotRequest> Chunk(const TSnapshot& snapshot, std::string_view bytes, uint64_t offset, uint32_t count) {
    auto message = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
    message->Src = 1; message->Dst = 2; message->Term = 3; message->Seqno = offset + 1;
    message->SnapshotIndex = snapshot.Index; message->SnapshotTerm = snapshot.Term;
    message->Offset = offset; message->TotalBytes = bytes.size(); message->Done = offset + count == bytes.size();
    message->LeaderId = 1; memcpy(message->Data, bytes.data() + offset, count); return message;
}
void test_idle_snapshot_transfer_discards_partial_image_and_restarts(void**) {
    TCluster cluster({3}); // Learners must also reclaim stalled transfers.
    cluster.Prepare();

    auto first = cluster.SendChunk();
    cluster.Deliver(first);
    auto firstAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_true(firstAck->Success);
    assert_false(firstAck->Installed);
    assert_true(firstAck->NextOffset > 0);
    cluster.Deliver(firstAck);

    auto next = cluster.SendChunk();
    assert_int_equal(next->Offset, firstAck->NextOffset);
    cluster.Now += TTimeout::SnapshotTransferIdle;
    cluster.Nodes[2]->ProcessTimeout(cluster.Now);

    cluster.Deliver(next);
    auto expiredAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_false(expiredAck->Success);
    assert_int_equal(expiredAck->NextOffset, 0);

    // The sender may retry its original first chunk if the negative ACK is
    // delayed or lost. Receiver-side idle expiry must have released the old
    // assembly so that this offset-zero retry starts cleanly.
    cluster.Packets.clear();
    auto restarted = first;
    assert_int_equal(restarted->Offset, 0);
    cluster.Deliver(restarted);
    auto restartedAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_true(restartedAck->Success);
    assert_false(restartedAck->Installed);
    assert_int_equal(restartedAck->NextOffset,
        restarted->Offset + restarted->Len - sizeof(TInstallSnapshotRequest));
}
void test_snapshot_install_budget_is_shared_and_reclaimed_after_idle_expiry(void**) {
    auto budget = std::make_shared<TSnapshotInstallBudget>(200);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    std::vector<TMessageHolder<TMessage>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(std::move(message)); });
    TNodeDict peers{{1, peer}};

    auto raft2 = std::make_shared<TRaft>(std::make_shared<TState>(), 2, peers,
        std::vector<uint32_t>{}, limits);
    auto app2 = std::make_shared<TApplication>();
    TRequestProcessor processor2(raft2, app2, peers);
    auto raft3 = std::make_shared<TRaft>(std::make_shared<TState>(), 3, peers,
        std::vector<uint32_t>{}, limits);
    auto app3 = std::make_shared<TApplication>();
    TRequestProcessor processor3(raft3, app3, peers);

    auto request = [](uint32_t target) {
        constexpr std::string_view prefix = "MRFT";
        auto chunk = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + prefix.size());
        chunk->Src = 1; chunk->Dst = target; chunk->Term = 1; chunk->Seqno = 1;
        chunk->SnapshotIndex = 1; chunk->SnapshotTerm = 1;
        chunk->TotalBytes = 128; chunk->Offset = 0; chunk->LeaderId = 1; chunk->Done = 0;
        memcpy(chunk->Data, prefix.data(), prefix.size());
        return chunk;
    };

    raft2->Process(ITimeSource::Time{}, request(2));
    auto firstAck = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(firstAck->Success); assert_false(firstAck->Installed);
    assert_int_equal(firstAck->NextOffset, 4);
    assert_int_equal(budget->UsedBytes(), 128);

    replies.clear();
    raft3->Process(ITimeSource::Time{}, request(3));
    auto overBudget = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(overBudget->Success); assert_false(overBudget->Installed);
    assert_int_equal(overBudget->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 128);

    const auto expiredAt = ITimeSource::Time{} + TTimeout::SnapshotTransferIdle;
    raft2->ProcessTimeout(expiredAt);
    assert_int_equal(budget->UsedBytes(), 0);
    replies.clear();
    raft3->Process(expiredAt, request(3));
    auto retried = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(retried->Success); assert_false(retried->Installed);
    assert_int_equal(retried->NextOffset, 4);
    assert_int_equal(budget->UsedBytes(), 128);

    TKv source;
    const auto image = EncodeSnapshot(TSnapshot{1, 1, {1, 3}, source.SaveSnapshot()});
    auto complete = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + image.size());
    complete->Src = 1; complete->Dst = 3; complete->Term = 1; complete->Seqno = 2;
    complete->SnapshotIndex = 1; complete->SnapshotTerm = 1;
    complete->TotalBytes = image.size(); complete->Offset = 0; complete->LeaderId = 1; complete->Done = 1;
    memcpy(complete->Data, image.data(), image.size());
    replies.clear();
    raft3->Process(expiredAt, complete);
    auto installed = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(installed->Success); assert_true(installed->Installed);
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_snapshot_install_budget_releases_after_application_validation_failure(void**) {
    auto budget = std::make_shared<TSnapshotInstallBudget>(1024);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    std::vector<TMessageHolder<TMessage>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(std::move(message)); });
    TNodeDict peers{{1, peer}};

    auto raft3 = std::make_shared<TRaft>(std::make_shared<TState>(), 3, peers,
        std::vector<uint32_t>{}, limits);
    auto rejectingApp = std::make_shared<TRejectingSnapshotApplication>();
    TRequestProcessor rejectingProcessor(raft3, rejectingApp, peers);

    TKv source;
    const auto image = EncodeSnapshot(TSnapshot{1, 1, {1, 3}, source.SaveSnapshot()});
    assert_true(image.size() < budget->LimitBytes());
    auto complete = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + image.size());
    complete->Src = 1; complete->Dst = 3; complete->Term = 1; complete->Seqno = 1;
    complete->SnapshotIndex = 1; complete->SnapshotTerm = 1; complete->Offset = 0;
    complete->TotalBytes = image.size(); complete->Done = 1; complete->LeaderId = 1;
    memcpy(complete->Data, image.data(), image.size());

    raft3->Process(ITimeSource::Time{}, complete);
    auto rejected = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(rejected->Success); assert_false(rejected->Installed);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(raft3->GetState()->SnapshotIndex, 0);
    assert_int_equal(rejectingApp->LastAppliedIndex, 0);

    auto raft2 = std::make_shared<TRaft>(std::make_shared<TState>(), 2, peers,
        std::vector<uint32_t>{}, limits);
    auto app2 = std::make_shared<TApplication>();
    TRequestProcessor processor2(raft2, app2, peers);
    constexpr size_t FirstChunkBytes = 4;
    auto partial = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + FirstChunkBytes);
    partial->Src = 1; partial->Dst = 2; partial->Term = 1; partial->Seqno = 2;
    partial->SnapshotIndex = 1; partial->SnapshotTerm = 1; partial->Offset = 0;
    partial->TotalBytes = image.size(); partial->Done = 0; partial->LeaderId = 1;
    memcpy(partial->Data, image.data(), FirstChunkBytes);
    replies.clear();
    raft2->Process(ITimeSource::Time{}, partial);
    auto accepted = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(accepted->Success); assert_false(accepted->Installed);
    assert_int_equal(accepted->NextOffset, FirstChunkBytes);
    assert_int_equal(budget->UsedBytes(), image.size());
}
void test_snapshot_install_budget_releases_after_conflicting_duplicate(void**) {
    auto budget = std::make_shared<TSnapshotInstallBudget>(1024);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    std::vector<TMessageHolder<TMessage>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(std::move(message)); });
    TNodeDict peers{{1, peer}};

    auto raft3 = std::make_shared<TRaft>(std::make_shared<TState>(), 3, peers,
        std::vector<uint32_t>{}, limits);
    auto app3 = std::make_shared<TApplication>();
    TRequestProcessor processor3(raft3, app3, peers);
    auto raft2 = std::make_shared<TRaft>(std::make_shared<TState>(), 2, peers,
        std::vector<uint32_t>{}, limits);
    auto app2 = std::make_shared<TApplication>();
    TRequestProcessor processor2(raft2, app2, peers);

    TKv source;
    const auto image = EncodeSnapshot(TSnapshot{1, 1, {1, 3}, source.SaveSnapshot()});
    constexpr size_t FirstChunkBytes = 4;
    auto request = [&](uint32_t target, uint32_t seqno, bool corrupt) {
        auto chunk = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + FirstChunkBytes);
        chunk->Src = 1; chunk->Dst = target; chunk->Term = 1; chunk->Seqno = seqno;
        chunk->SnapshotIndex = 1; chunk->SnapshotTerm = 1; chunk->Offset = 0;
        chunk->TotalBytes = image.size(); chunk->Done = 0; chunk->LeaderId = 1;
        memcpy(chunk->Data, image.data(), FirstChunkBytes);
        if (corrupt) chunk->Data[0] ^= 1;
        return chunk;
    };

    raft3->Process(ITimeSource::Time{}, request(3, 1, false));
    auto accepted = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(accepted->Success); assert_int_equal(accepted->NextOffset, FirstChunkBytes);
    assert_int_equal(budget->UsedBytes(), image.size());

    replies.clear();
    raft3->Process(ITimeSource::Time{}, request(3, 2, true));
    auto rejected = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(rejected->Success); assert_false(rejected->Installed);
    assert_int_equal(rejected->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 0);

    replies.clear();
    raft2->Process(ITimeSource::Time{}, request(2, 3, false));
    auto retried = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(retried->Success); assert_int_equal(retried->NextOffset, FirstChunkBytes);
    assert_int_equal(budget->UsedBytes(), image.size());
}
void test_covered_snapshot_metadata_uses_shared_install_budget(void**) {
    auto budget = std::make_shared<TSnapshotInstallBudget>(60);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    std::vector<TMessageHolder<TMessage>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(std::move(message)); });
    TNodeDict peers{{1, peer}};
    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };
    auto raft2 = std::make_shared<TRaft>(makeState(), 2, peers, std::vector<uint32_t>{}, limits);
    auto raft3 = std::make_shared<TRaft>(makeState(), 3, peers, std::vector<uint32_t>{}, limits);

    auto request = [](uint32_t target, const std::string& image, size_t offset, size_t count) {
        auto chunk = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
        chunk->Src = 1; chunk->Dst = target; chunk->Term = 1; chunk->Seqno = offset + 1;
        chunk->SnapshotIndex = 1; chunk->SnapshotTerm = 1;
        chunk->TotalBytes = image.size(); chunk->Offset = offset; chunk->LeaderId = 1;
        chunk->Done = offset + count == image.size();
        memcpy(chunk->Data, image.data() + offset, count);
        return chunk;
    };
    const auto image2 = EncodeSnapshot(TSnapshot{1, 1, {1, 2}, std::string(200, 'a')});
    const auto replacement = EncodeSnapshot(TSnapshot{1, 1, {1, 2}, std::string(201, 'b')});
    const auto image3 = EncodeSnapshot(TSnapshot{1, 1, {1, 3}, std::string(200, 'c')});

    raft2->Process(ITimeSource::Time{}, request(2, image2, 0, 20));
    auto first = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(first->Success); assert_int_equal(first->NextOffset, 20);
    assert_int_equal(budget->UsedBytes(), 20);

    raft2->Process(ITimeSource::Time{}, request(2, replacement, 0, 20));
    auto replaced = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(replaced->Success); assert_int_equal(replaced->NextOffset, 20);
    assert_int_equal(budget->UsedBytes(), 20);

    raft3->Process(ITimeSource::Time{}, request(3, image3, 0, 20));
    assert_true(replies.back().Cast<TInstallSnapshotResponse>()->Success);
    assert_int_equal(budget->UsedBytes(), 40);
    raft2->Process(ITimeSource::Time{}, request(2, replacement, 20, 8));
    auto overBudget = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(overBudget->Success); assert_int_equal(overBudget->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 20);

    const auto expiredAt = ITimeSource::Time{} + TTimeout::SnapshotTransferIdle;
    raft3->ProcessTimeout(expiredAt);
    assert_int_equal(budget->UsedBytes(), 0);
    raft3->Process(expiredAt, request(3, image3, 0, 8));
    auto retried = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(retried->Success); assert_int_equal(retried->NextOffset, 8);
    assert_int_equal(budget->UsedBytes(), 8);
    const auto metadataBytes = image3.size() - 200 - sizeof(uint32_t);
    raft3->Process(expiredAt, request(3, image3, 8, metadataBytes - 8));
    auto covered = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(covered->Success); assert_true(covered->Installed);
    assert_int_equal(covered->NextOffset, image3.size());
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_snapshot_install_accepts_every_small_fixed_chunk_size(void**) {
    TKv source;
    source.Write(source.Prepare(Command("key", "value")), 1);
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);

    for (uint32_t chunkSize = 1; chunkSize <= bytes.size(); ++chunkSize) {
        auto state = std::make_shared<TState>();
        state->CurrentTerm = 3;
        state->Commit();
        uint64_t nextOffset = 0;
        size_t responses = 0;
        bool success = false, installed = false;
        auto peer = std::make_shared<TEndpoint>([&](auto message) {
            if (message->Type != uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE)) return;
            const auto response = message.template Cast<TInstallSnapshotResponse>();
            nextOffset = response->NextOffset;
            success = response->Success;
            installed = response->Installed;
            ++responses;
        });
        TNodeDict peers{{1, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers);
        auto app = std::make_shared<TApplication>();
        TRequestProcessor processor(raft, app, peers);

        for (uint64_t offset = 0; offset < bytes.size();) {
            const auto remaining = bytes.size() - offset;
            const auto count = static_cast<uint32_t>(remaining < chunkSize ? remaining : chunkSize);
            auto request = Chunk(snapshot, bytes, offset, count);
            raft->Process({}, request);
            assert_int_equal(responses, size_t(2 * (offset / chunkSize) + 1));
            assert_true(success);
            assert_int_equal(nextOffset, offset + count);

            const bool final = offset + count == bytes.size();
            assert_int_equal(installed, final);
            if (!final) {
                assert_int_equal(app->Loads, 0);
                assert_int_equal(state->SnapshotIndex, 0);
                // An exact retry of the most recent chunk is idempotent and
                // returns the receiver's next contiguous offset.
                const auto previousResponses = responses;
                raft->Process({}, request);
                assert_int_equal(responses, previousResponses + 1);
                assert_true(success); assert_false(installed);
                assert_int_equal(nextOffset, offset + count);
            }
            offset += count;
        }
        assert_int_equal(app->Loads, 1);
        assert_int_equal(app->LastAppliedIndex, snapshot.Index);
        assert_true(Read(*app, "key") == "value");
        assert_true(*state->GetSnapshot() == snapshot);
    }
}
void test_joint_snapshot_is_rejected_without_matching_history_even_if_boundary_is_covered(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "old")), 5);
    TSnapshot fixed{5, 1, {1, 2}, source.SaveSnapshot()};
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    auto fixedBytes = EncodeSnapshot(fixed);
    raft->Process({}, Chunk(fixed, fixedBytes, 0, fixedBytes.size()));
    assert_true(replies.back()->Installed); assert_int_equal(app->Loads, 1);
    auto joint = fixed; joint.JointVoters = {2, 3}; joint.ConfigurationIndex = 4; joint.ConfigurationTerm = 1;
    const auto bytes = EncodeSnapshot(joint);
    const auto before = static_cast<TStateFields>(*state);
    const auto repliesBeforeUnsupported = replies.size();

    // An unsupported snapshot format or an unclassifiable short first chunk
    // must not persist a higher term before the runtime can reject it.
    auto higherTermJoint = Chunk(joint, bytes, 0, static_cast<uint32_t>(bytes.size()));
    higherTermJoint->Term = state->CurrentTerm + 1;
    raft->Process({}, higherTermJoint);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(replies.size(), repliesBeforeUnsupported);
    auto wrongEmbeddedBoundary = fixed; wrongEmbeddedBoundary.Index = 4;
    const auto wrongEmbeddedBoundaryBytes = EncodeSnapshot(wrongEmbeddedBoundary);
    auto higherTermWrongBoundary = Chunk(fixed, wrongEmbeddedBoundaryBytes,
        0, static_cast<uint32_t>(wrongEmbeddedBoundaryBytes.size()));
    higherTermWrongBoundary->Term = state->CurrentTerm + 1;
    raft->Process({}, higherTermWrongBoundary);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(replies.size(), repliesBeforeUnsupported);
    auto wrongVotersBeforeTerm = fixed; wrongVotersBeforeTerm.Voters = {1, 3};
    const auto wrongVotersBeforeTermBytes = EncodeSnapshot(wrongVotersBeforeTerm);
    auto higherTermWrongVoters = Chunk(wrongVotersBeforeTerm, wrongVotersBeforeTermBytes,
        0, static_cast<uint32_t>(wrongVotersBeforeTermBytes.size()));
    higherTermWrongVoters->Term = state->CurrentTerm + 1;
    raft->Process({}, higherTermWrongVoters);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(replies.size(), repliesBeforeUnsupported);
    auto higherTermShort = Chunk(joint, bytes, 0, 4);
    higherTermShort->Term = state->CurrentTerm + 1;
    raft->Process({}, higherTermShort);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(replies.size(), repliesBeforeUnsupported);
    auto higherTermContinuation = Chunk(joint, bytes, 4, 4);
    higherTermContinuation->Term = state->CurrentTerm + 1;
    raft->Process({}, higherTermContinuation);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(replies.size(), repliesBeforeUnsupported);

    // At the current term, normal fail-closed rejection still returns a reply.
    raft->Become(EState::CANDIDATE);
    const auto electionDueBeforeUnsupported = raft->GetVolatileState()->ElectionDue;
    raft->Process({}, Chunk(joint, bytes, 0, 4));
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
    assert_true(raft->GetVolatileState()->ElectionDue == electionDueBeforeUnsupported);
    raft->Process({}, Chunk(joint, bytes, 4, 4));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
    assert_true(raft->GetVolatileState()->ElectionDue == electionDueBeforeUnsupported);
    raft->Process({}, Chunk(joint, bytes, 0, bytes.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
    assert_true(raft->GetVolatileState()->ElectionDue == electionDueBeforeUnsupported);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(app->Loads, 1);

    // Covered fast-path ACKs still require the embedded boundary and voter set
    // to match the request and this node's fixed membership.
    auto wrongVoters = fixed; wrongVoters.Voters = {1, 3};
    auto wrongVoterBytes = EncodeSnapshot(wrongVoters);
    raft->Process({}, Chunk(wrongVoters, wrongVoterBytes, 0, wrongVoterBytes.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(app->Loads, 1);

    auto wrongBoundary = fixed; wrongBoundary.Index = 4;
    auto wrongBoundaryBytes = EncodeSnapshot(wrongBoundary);
    raft->Process({}, Chunk(fixed, wrongBoundaryBytes, 0, wrongBoundaryBytes.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(app->Loads, 1);

    // The metadata-only probe remains bounded and handles a split prefix.
    const size_t metadataBytes = 36 + 4 * fixed.Voters.size();
    for (size_t offset = 0; offset < metadataBytes; offset += 4) {
        const auto count = std::min<size_t>(4, metadataBytes - offset);
        raft->Process({}, Chunk(fixed, fixedBytes, offset, static_cast<uint32_t>(count)));
        if (offset + count < metadataBytes) {
            assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, offset + count);
        }
    }
    assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, fixedBytes.size());
    raft->Process({}, Chunk(joint, bytes, 0, 4));
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    raft->Process({}, Chunk(joint, bytes, 4, 4));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    // A stable format cannot invent a configuration identity when the
    // receiver has no committed membership record to substantiate it.
    auto stable = fixed; stable.StableConfiguration = true; stable.ConfigurationIndex = 4; stable.ConfigurationTerm = 1;
    auto stableBytes = EncodeSnapshot(stable);
    raft->Process({}, Chunk(stable, stableBytes, 0, stableBytes.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(app->Loads, 1);

    auto stableWrongVoters = stable; stableWrongVoters.Voters = {1, 3};
    auto stableWrongVotersBytes = EncodeSnapshot(stableWrongVoters);
    raft->Process({}, Chunk(stableWrongVoters, stableWrongVotersBytes, 0, stableWrongVotersBytes.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    auto invalidStableIdentity = stableBytes;
    for (size_t i = 36; i < 44; ++i) invalidStableIdentity[i] = 0;
    raft->Process({}, Chunk(stable, invalidStableIdentity, 0, invalidStableIdentity.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    joint.Index = 6;
    const auto newer = EncodeSnapshot(joint);
    raft->Process({}, Chunk(joint, newer, 0, newer.size()));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "a") == "old");
}
void test_higher_term_snapshot_defers_term_change_until_split_metadata_is_validated(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "old")), 5);
    TSnapshot fixed{5, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(fixed);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});

    raft->Process({}, Chunk(fixed, bytes, 0, static_cast<uint32_t>(bytes.size())));
    assert_true(replies.back()->Installed); assert_int_equal(app->Loads, 1);
    source.Write(source.Prepare(Command("b", "new")), 6);
    TSnapshot next{6, 1, {1, 2}, source.SaveSnapshot()};
    const auto nextBytes = EncodeSnapshot(next);
    const auto currentTerm = state->CurrentTerm;
    const auto incomingTerm = currentTerm + 1;
    raft->Become(EState::CANDIDATE);
    const auto electionDue = raft->GetVolatileState()->ElectionDue;
    const size_t metadataBytes = 36 + 4 * next.Voters.size();
    for (size_t offset = 0; offset < nextBytes.size(); offset += 8) {
        const auto count = std::min<size_t>(8, nextBytes.size() - offset);
        auto chunk = Chunk(next, nextBytes, offset, static_cast<uint32_t>(count));
        chunk->Term = incomingTerm;
        raft->Process({}, chunk);
        if (offset + count < metadataBytes) {
            assert_int_equal(state->CurrentTerm, currentTerm);
            assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
            assert_true(raft->GetVolatileState()->ElectionDue == electionDue);
            assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, offset + count);
        } else if (offset + count < nextBytes.size()) {
            assert_int_equal(state->CurrentTerm, incomingTerm);
            assert_int_equal(raft->CurrentStateName(), EState::FOLLOWER);
            assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, offset + count);
        }
    }

    assert_int_equal(state->CurrentTerm, incomingTerm);
    assert_int_equal(raft->CurrentStateName(), EState::FOLLOWER);
    assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, nextBytes.size());
    assert_int_equal(app->Loads, 2);
    assert_int_equal(app->LastAppliedIndex, next.Index);
    assert_true(Read(*app, "b") == "new");
}

void test_invalid_split_higher_term_snapshot_preserves_candidate_state(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "old")), 5);
    TSnapshot fixed{5, 1, {1, 2}, source.SaveSnapshot()};
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    const auto fixedBytes = EncodeSnapshot(fixed);
    raft->Process({}, Chunk(fixed, fixedBytes, 0, static_cast<uint32_t>(fixedBytes.size())));
    assert_true(replies.back()->Installed); assert_int_equal(app->Loads, 1);

    auto invalid = fixed; invalid.Voters = {1, 3};
    const auto bytes = EncodeSnapshot(invalid);
    const auto currentTerm = state->CurrentTerm;
    const auto before = static_cast<TStateFields>(*state);
    const auto incomingTerm = currentTerm + 1;
    raft->Become(EState::CANDIDATE);
    const auto electionDue = raft->GetVolatileState()->ElectionDue;
    const size_t metadataBytes = 36 + 4 * invalid.Voters.size();
    for (size_t offset = 0; offset < metadataBytes; offset += 8) {
        const auto count = std::min<size_t>(8, metadataBytes - offset);
        auto chunk = Chunk(invalid, bytes, offset, static_cast<uint32_t>(count));
        chunk->Term = incomingTerm;
        raft->Process({}, chunk);
        assert_int_equal(state->CurrentTerm, currentTerm);
        assert_true(static_cast<TStateFields>(*state) == before);
        assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
        assert_true(raft->GetVolatileState()->ElectionDue == electionDue);
        if (offset + count < metadataBytes) {
            assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, offset + count);
        } else {
            assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, 0);
        }
    }
    assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "a") == "old");
}

void test_valid_split_higher_term_covered_snapshot_accepts_leader_without_reload(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "committed")), 5);
    TSnapshot fixed{5, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(fixed);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});

    raft->Process({}, Chunk(fixed, bytes, 0, static_cast<uint32_t>(bytes.size())));
    assert_true(replies.back()->Installed);
    assert_int_equal(app->Loads, 1);
    const auto before = static_cast<TStateFields>(*state);
    const auto incomingTerm = state->CurrentTerm + 1;
    raft->Become(EState::CANDIDATE);
    const auto electionDue = raft->GetVolatileState()->ElectionDue;
    const size_t metadataBytes = 36 + 4 * fixed.Voters.size();

    for (size_t offset = 0; offset < metadataBytes; offset += 8) {
        const auto count = std::min<size_t>(8, metadataBytes - offset);
        auto chunk = Chunk(fixed, bytes, offset, static_cast<uint32_t>(count));
        chunk->Term = incomingTerm;
        raft->Process({}, chunk);
        if (offset + count < metadataBytes) {
            assert_int_equal(state->CurrentTerm, before.CurrentTerm);
            assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
            assert_true(raft->GetVolatileState()->ElectionDue == electionDue);
            assert_true(replies.back()->Success);
            assert_false(replies.back()->Installed);
            assert_int_equal(replies.back()->NextOffset, offset + count);
        }
    }

    assert_int_equal(state->CurrentTerm, incomingTerm);
    assert_int_equal(raft->CurrentStateName(), EState::FOLLOWER);
    assert_true(static_cast<TStateFields>(*state) != before);
    assert_true(*state->GetSnapshot() == fixed);
    assert_true(replies.back()->Success);
    assert_true(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, bytes.size());
    assert_int_equal(app->Loads, 1);
    assert_int_equal(app->LastAppliedIndex, fixed.Index);
    assert_true(Read(*app, "a") == "committed");
}

void test_unmatched_joint_snapshot_does_not_accept_leader_contact(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "joint")), 5);
    TSnapshot joint{5, 2, {1, 2}, source.SaveSnapshot()};
    joint.JointVoters = {2, 3}; joint.ConfigurationIndex = 4; joint.ConfigurationTerm = 2;
    const auto bytes = EncodeSnapshot(joint);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    state->CurrentTerm = 3;
    raft->Become(EState::CANDIDATE);
    const auto before = static_cast<TStateFields>(*state);
    const auto electionDue = raft->GetVolatileState()->ElectionDue;
    const auto leaderContactDue = raft->GetVolatileState()->LeaderContactDue;

    // This image is newer than the local commit boundary, so it exercises the
    // full installation path rather than the covered-prefix fast path.
    raft->Process({}, Chunk(joint, bytes, 0, 4));
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
    assert_true(raft->GetVolatileState()->ElectionDue == electionDue);
    assert_true(raft->GetVolatileState()->LeaderContactDue == leaderContactDue);
    raft->Process({}, Chunk(joint, bytes, 4, 4));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    raft->Process({}, Chunk(joint, bytes, 0, static_cast<uint32_t>(bytes.size())));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(raft->CurrentStateName(), EState::CANDIDATE);
    assert_true(raft->GetVolatileState()->ElectionDue == electionDue);
    assert_true(raft->GetVolatileState()->LeaderContactDue == leaderContactDue);
    assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(app->Loads, 0); assert_int_equal(app->LastAppliedIndex, 0);
}
void test_stable_snapshot_completes_known_joint_membership_only_from_new_voter(void**) {
    TKv image;
    image.Write(image.Prepare(Command("membership", "joint")), 5);
    TSnapshot joint{5, 2, {1, 2}, image.SaveSnapshot()};
    joint.JointVoters = {2, 3};
    joint.ConfigurationIndex = 4;
    joint.ConfigurationTerm = 2;
    const TSnapshotConfigurationTransition priorStable{{7, 8}, {1, 2}, 0, 0, 2, 2, 3, 2};
    const TSnapshotConfigurationTransition currentJoint{{1, 2}, {2, 3}, 3, 2, 4, 2, 0, 0};
    joint.PreviousVoters = joint.Voters;
    joint.PreviousConfigurationIndex = 3;
    joint.PreviousConfigurationTerm = 2;
    joint.PreviousJointIndex = joint.ConfigurationIndex;
    joint.PreviousJointTerm = joint.ConfigurationTerm;
    joint.ConfigurationTransitions = {priorStable, currentJoint};

    const std::vector<uint32_t> bootstrapVoters{1, 2, 3};
    auto state = std::make_shared<TState>();
    state->BindVoters(bootstrapVoters);
    state->CurrentTerm = 3;
    state->InstallSnapshot(joint);

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto reply = [&](TMessageHolder<TMessage> message) {
        replies.push_back(message.Cast<TInstallSnapshotResponse>());
    };
    auto oldVoter = std::make_shared<TEndpoint>(reply);
    auto newVoter = std::make_shared<TEndpoint>(reply);
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, oldVoter}, {3, newVoter}});
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});
    assert_true(raft->GetConfigurationState().Committed.IsJoint());
    assert_int_equal(app->LastAppliedIndex, joint.Index);

    image.Write(image.Prepare(Command("membership", "stable")), 8);
    TSnapshot stable{8, 3, {2, 3}, image.SaveSnapshot()};
    stable.StableConfiguration = true;
    stable.ConfigurationIndex = 7;
    stable.ConfigurationTerm = 3;
    stable.PreviousVoters = joint.Voters;
    stable.PreviousConfigurationIndex = joint.PreviousConfigurationIndex;
    stable.PreviousConfigurationTerm = joint.PreviousConfigurationTerm;
    stable.PreviousJointIndex = joint.ConfigurationIndex;
    stable.PreviousJointTerm = joint.ConfigurationTerm;
    stable.ConfigurationTransitions = {priorStable, currentJoint};
    stable.ConfigurationTransitions.back().StableIndex = stable.ConfigurationIndex;
    stable.ConfigurationTransitions.back().StableTerm = stable.ConfigurationTerm;

    auto install = [&](const TSnapshot& candidate, uint32_t leader) {
        const auto bytes = EncodeSnapshot(candidate);
        auto request = Chunk(candidate, bytes, 0, static_cast<uint32_t>(bytes.size()));
        request->Src = leader;
        request->LeaderId = leader;
        raft->Process({}, request);
        return replies.back();
    };

    auto wrongHistory = stable;
    wrongHistory.ConfigurationTransitions.front().OldVoters = {9, 10};
    auto response = install(wrongHistory, 3);
    assert_false(response->Success);
    assert_false(response->Installed);
    assert_true(state->GetSnapshot() && *state->GetSnapshot() == joint);
    assert_int_equal(app->Loads, 1);

    // The former joint side cannot claim that the successor configuration is
    // committed; only a sender in the new voter set may complete this proof.
    response = install(stable, 1);
    assert_false(response->Success);
    assert_false(response->Installed);
    assert_true(state->GetSnapshot() && *state->GetSnapshot() == joint);
    assert_int_equal(app->Loads, 1);

    response = install(stable, 3);
    assert_true(response->Success);
    assert_true(response->Installed);
    assert_true(state->GetSnapshot() && *state->GetSnapshot() == stable);
    assert_false(raft->GetConfigurationState().Committed.IsJoint());
    assert_true(raft->GetVoterIds() == std::vector<uint32_t>({2, 3}));
    assert_int_equal(app->Loads, 2);
    assert_int_equal(app->LastAppliedIndex, stable.Index);
    assert_true(Read(*app, "membership") == "stable");
}
void ExerciseStaleStableSnapshotHistoryRecovery(bool jointSuccessor) {
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> intermediate{2, 3, 4};
    const std::vector<uint32_t> stableVoters{3, 4, 5};
    const std::vector<uint32_t> jointVoters{4, 5, 6};
    const TSnapshotConfigurationTransition prior{{7, 8, 9}, original, 0, 0, 2, 2, 3, 2};
    const TSnapshotConfigurationTransition anchor{original, intermediate, 3, 2, 6, 3, 7, 3};
    const TSnapshotConfigurationTransition stableTail{intermediate, stableVoters, 7, 3, 8, 3, 9, 3};
    const TSnapshotConfigurationTransition jointTail{intermediate, jointVoters, 7, 3, 8, 3, 0, 0};

    TKv image;
    image.Write(image.Prepare(Command("membership", "known stable")), 5);
    TSnapshot known{5, 2, original, image.SaveSnapshot()};
    known.StableConfiguration = true;
    known.ConfigurationIndex = 3;
    known.ConfigurationTerm = 2;

    auto state = std::make_shared<TState>();
    state->BindVoters(original);
    state->CurrentTerm = 3;
    state->InstallSnapshot(known);

    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    TNodeDict peers;
    for (uint32_t id : {1, 3, 4, 5, 6})
        peers.emplace(id, std::make_shared<TEndpoint>([&](auto message) {
            replies.push_back(message.template Cast<TInstallSnapshotResponse>());
        }));
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{4, 5, 6});
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});
    assert_int_equal(app->LastAppliedIndex, known.Index);

    const auto snapshotIndex = jointSuccessor ? 9 : 10;
    const auto value = jointSuccessor ? "recovered joint" : "recovered stable";
    image.Write(image.Prepare(Command("membership", value)), snapshotIndex);
    TSnapshot incoming;
    incoming.Index = snapshotIndex;
    incoming.Term = 3;
    incoming.Data = image.SaveSnapshot();
    incoming.ConfigurationTransitions = {prior, anchor};
    if (jointSuccessor) {
        incoming.Voters = intermediate;
        incoming.JointVoters = jointVoters;
        incoming.ConfigurationIndex = jointTail.JointIndex;
        incoming.ConfigurationTerm = jointTail.JointTerm;
        incoming.PreviousVoters = intermediate;
        incoming.PreviousConfigurationIndex = jointTail.PreviousConfigurationIndex;
        incoming.PreviousConfigurationTerm = jointTail.PreviousConfigurationTerm;
        incoming.PreviousJointIndex = jointTail.JointIndex;
        incoming.PreviousJointTerm = jointTail.JointTerm;
        incoming.ConfigurationTransitions.push_back(jointTail);
    } else {
        incoming.Voters = stableVoters;
        incoming.StableConfiguration = true;
        incoming.ConfigurationIndex = stableTail.StableIndex;
        incoming.ConfigurationTerm = stableTail.StableTerm;
        incoming.PreviousVoters = intermediate;
        incoming.PreviousConfigurationIndex = stableTail.PreviousConfigurationIndex;
        incoming.PreviousConfigurationTerm = stableTail.PreviousConfigurationTerm;
        incoming.PreviousJointIndex = stableTail.JointIndex;
        incoming.PreviousJointTerm = stableTail.JointTerm;
        incoming.ConfigurationTransitions.push_back(stableTail);
    }

    auto install = [&](const TSnapshot& candidate) {
        const auto bytes = EncodeSnapshot(candidate);
        auto request = Chunk(candidate, bytes, 0, static_cast<uint32_t>(bytes.size()));
        request->Src = 3;
        request->LeaderId = 3;
        raft->Process({}, request);
        return replies.back();
    };

    auto conflictingHistory = incoming;
    // Keep the sender in the image's final voter set, but remove it from one
    // missed intermediate transition. The history is structurally valid; it
    // must still fail the local trusted-anchor check.
    const std::vector<uint32_t> conflictingIntermediate{2, 4, 5};
    conflictingHistory.ConfigurationTransitions[1].NewVoters = conflictingIntermediate;
    if (jointSuccessor) {
        conflictingHistory.Voters = conflictingIntermediate;
        conflictingHistory.JointVoters = {3, 5, 6};
        conflictingHistory.PreviousVoters = conflictingIntermediate;
        conflictingHistory.ConfigurationTransitions[2].OldVoters = conflictingIntermediate;
        conflictingHistory.ConfigurationTransitions[2].NewVoters = conflictingHistory.JointVoters;
    } else {
        conflictingHistory.PreviousVoters = conflictingIntermediate;
        conflictingHistory.ConfigurationTransitions[2].OldVoters = conflictingIntermediate;
    }
    auto response = install(conflictingHistory);
    assert_false(response->Success);
    assert_false(response->Installed);
    assert_true(state->GetSnapshot() && *state->GetSnapshot() == known);
    assert_int_equal(app->Loads, 1);

    response = install(incoming);
    assert_true(response->Success);
    assert_true(response->Installed);
    const auto installed = state->GetSnapshot();
    assert_non_null(installed.get());
    assert_int_equal(installed->Index, incoming.Index);
    assert_true(installed->ConfigurationTransitions.size() == 2);
    assert_true(installed->ConfigurationTransitions.front() == anchor);
    assert_true(installed->IsJoint() == jointSuccessor);
    assert_true(raft->GetConfigurationState().Committed.IsJoint() == jointSuccessor);
    assert_true(state->GetBoundVoters() == original);
    assert_int_equal(app->Loads, 2);
    assert_int_equal(app->LastAppliedIndex, incoming.Index);
    assert_true(Read(*app, "membership") == value);
    if (jointSuccessor) {
        assert_true(installed->Voters == intermediate);
        assert_true(installed->JointVoters == jointVoters);
        assert_true(raft->GetVoterIds() == std::vector<uint32_t>({2, 3, 4, 5, 6}));
    } else {
        assert_true(installed->Voters == stableVoters);
        assert_true(raft->GetVoterIds() == stableVoters);
    }
}
void test_stale_stable_snapshot_recovers_multi_transition_stable_membership(void**) {
    ExerciseStaleStableSnapshotHistoryRecovery(false);
}
void test_stale_stable_snapshot_recovers_multi_transition_joint_membership(void**) {
    ExerciseStaleStableSnapshotHistoryRecovery(true);
}
void test_unproven_stable_configuration_snapshot_is_rejected_for_fixed_membership(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "stable snapshot")), 5);
    TSnapshot snapshot{5, 1, {1, 2}, source.SaveSnapshot()};
    snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 4; snapshot.ConfigurationTerm = 1;
    const auto bytes = EncodeSnapshot(snapshot);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); });
    auto state = std::make_shared<TState>();
    {
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
        auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
        state->CurrentTerm = 3; // Exercise a same-term rejection with a normal RPC response.
        raft->Process({}, Chunk(snapshot, bytes, 0, static_cast<uint32_t>(bytes.size())));
        assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
        assert_true(!state->GetSnapshot()); assert_int_equal(app->Loads, 0);

        // A legacy snapshot remains valid when the receiver has no membership
        // transition history and its voter list matches the fixed set.
        TSnapshot legacy{5, 1, {1, 2}, source.SaveSnapshot()};
        const auto legacyInitialBytes = EncodeSnapshot(legacy);
        raft->Process({}, Chunk(legacy, legacyInitialBytes, 0,
            static_cast<uint32_t>(legacyInitialBytes.size())));
        assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
        assert_true(*state->GetSnapshot() == legacy);
        assert_int_equal(app->Loads, 1); assert_true(Read(*app, "a") == "stable snapshot");

        // A covered stable image with a different configuration identity must
        // not be acknowledged as progress.
        auto conflictingCovered = snapshot;
        conflictingCovered.ConfigurationIndex = 3;
        const auto conflictingCoveredBytes = EncodeSnapshot(conflictingCovered);
        const auto beforeConflictingCovered = static_cast<TStateFields>(*state);
        const auto repliesBeforeConflictingCovered = replies.size();
        auto higherTermConflictingCovered = Chunk(conflictingCovered, conflictingCoveredBytes, 0,
            static_cast<uint32_t>(conflictingCoveredBytes.size()));
        higherTermConflictingCovered->Term = state->CurrentTerm + 1;
        raft->Process({}, higherTermConflictingCovered);
        assert_true(static_cast<TStateFields>(*state) == beforeConflictingCovered);
        assert_int_equal(replies.size(), repliesBeforeConflictingCovered);
        assert_true(*state->GetSnapshot() == legacy); assert_int_equal(app->Loads, 1);

        raft->Process({}, Chunk(conflictingCovered, conflictingCoveredBytes, 0,
            static_cast<uint32_t>(conflictingCoveredBytes.size())));
        assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
        assert_true(*state->GetSnapshot() == legacy); assert_int_equal(app->Loads, 1);

        // A newer legacy image remains compatible with the same fixed set.
        source.Write(source.Prepare(Command("a", "legacy refresh")), 6);
        legacy = {6, 1, {1, 2}, source.SaveSnapshot()};
        const auto legacyBytes = EncodeSnapshot(legacy);
        raft->Process({}, Chunk(legacy, legacyBytes, 0, static_cast<uint32_t>(legacyBytes.size())));
        assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
        assert_true(*state->GetSnapshot() == legacy);
        assert_int_equal(app->Loads, 2); assert_true(Read(*app, "a") == "legacy refresh");

        // Conflicting metadata at a newer boundary is rejected before either
        // durable Raft state or the application can be replaced.
        source.Write(source.Prepare(Command("a", "conflicting refresh")), 7);
        TSnapshot conflicting{7, 1, {1, 2}, source.SaveSnapshot()};
        conflicting.StableConfiguration = true;
        conflicting.ConfigurationIndex = 3; conflicting.ConfigurationTerm = 1;
        const auto conflictingBytes = EncodeSnapshot(conflicting);
        raft->Process({}, Chunk(conflicting, conflictingBytes, 0,
            static_cast<uint32_t>(conflictingBytes.size())));
        assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
        assert_true(*state->GetSnapshot() == legacy);
        assert_int_equal(app->Loads, 2); assert_int_equal(app->LastAppliedIndex, 6);
        assert_true(Read(*app, "a") == "legacy refresh");
    }
    auto restarted = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto recovered = std::make_shared<TApplication>(); TRequestProcessor recovery(restarted, recovered, {});
    assert_int_equal(recovered->LastAppliedIndex, 6);
    assert_true(Read(*recovered, "a") == "legacy refresh");
    const auto recoveredSnapshot = restarted->GetState()->GetSnapshot();
    assert_false(recoveredSnapshot->HasConfigurationMetadata());
}
void test_idle_partial_snapshot_expires_on_learner_despite_heartbeats(void**) {
    auto state = std::make_shared<TState>();
    state->BindVoters({1});
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1; state->Append(entry);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{2});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    TKv source; source.Write(source.Prepare(Command("key", "value")), 1);
    TSnapshot snapshot{1, 1, {1}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);
    const size_t metadataBytes = 36 + 4 * snapshot.Voters.size();

    raft->Process({}, Chunk(snapshot, bytes, 0, metadataBytes));
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, metadataBytes); assert_int_equal(app->Loads, 0);

    // Ordinary leader contact keeps Raft's election timer fresh, but must not
    // keep an abandoned multi-chunk snapshot allocation alive.
    const auto heartbeatAt = ITimeSource::Time{} + std::chrono::seconds(10);
    auto heartbeat = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 1, .LeaderCommit = 0, .LeaderId = 1});
    raft->Process(heartbeatAt, heartbeat);
    // Replaying an already-buffered chunk is idempotent, but does not refresh
    // the deadline for a transfer that has stopped making forward progress.
    raft->Process(heartbeatAt, Chunk(snapshot, bytes, 0, metadataBytes));
    assert_true(replies.back()->Success); assert_int_equal(replies.back()->NextOffset, metadataBytes);
    const auto nextChunkAt = heartbeatAt + std::chrono::seconds(10);
    raft->ProcessTimeout(nextChunkAt);
    raft->Process(nextChunkAt, Chunk(snapshot, bytes, metadataBytes, 8));
    assert_true(replies.back()->Success); assert_int_equal(replies.back()->NextOffset, metadataBytes + 8);
    const auto duplicateAt = nextChunkAt + TTimeout::SnapshotTransferIdle / 2;
    raft->Process(duplicateAt, Chunk(snapshot, bytes, metadataBytes, 8));
    assert_true(replies.back()->Success); assert_int_equal(replies.back()->NextOffset, metadataBytes + 8);

    // A continuation after expiry is rejected with offset zero so the leader
    // restarts from the beginning instead of extending a discarded image.
    const auto expiredAt = nextChunkAt + TTimeout::SnapshotTransferIdle + std::chrono::milliseconds(1);
    raft->ProcessTimeout(expiredAt);
    raft->Process(expiredAt, Chunk(snapshot, bytes, metadataBytes + 8, 8));
    assert_false(replies.back()->Success); assert_false(replies.back()->Installed);
    assert_int_equal(replies.back()->NextOffset, 0); assert_int_equal(app->Loads, 0);
    assert_int_equal(app->LastAppliedIndex, 0); assert_int_equal(state->SnapshotIndex, 0);

    raft->Process(expiredAt, Chunk(snapshot, bytes, 0, static_cast<uint32_t>(bytes.size())));
    assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
    assert_int_equal(app->Loads, 1); assert_int_equal(app->LastAppliedIndex, 1);
    assert_true(Read(*app, "key") == "value");
}
void test_corrupt_snapshot_and_invalid_headers_cannot_mutate_application(void**) {
    TKv source; source.Write(source.Prepare(Command("a", "new")), 5);
    TSnapshot snapshot{5, 1, {1, 2}, source.SaveSnapshot()};
    const auto encoded = EncodeSnapshot(snapshot);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); });
    auto state = std::make_shared<TState>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    for (int invalid = 0; invalid < 6; ++invalid) {
        auto request = Chunk(snapshot, encoded, 0, encoded.size()); request->Term = 99;
        if (invalid == 0) request->Offset = UINT64_MAX;
        if (invalid == 1) request->TotalBytes = TSnapshot::MaxBytes + 1;
        if (invalid == 2) request->Done = 0;
        if (invalid == 3) request->SnapshotTerm = 0;
        if (invalid == 4) request->LeaderId = 9;
        if (invalid == 5) request->SnapshotIndex = UINT64_MAX;
        raft->Process({}, request); assert_int_equal(state->CurrentTerm, 1);
    }
    auto corrupt = Chunk(snapshot, encoded, 0, encoded.size()); corrupt->Data[encoded.size() - 1] ^= 1;
    raft->Process({}, corrupt); assert_false(replies.back()->Success);
    assert_int_equal(app->LastAppliedIndex, 0); assert_int_equal(state->SnapshotIndex, 0);
    for (int invalid = 0; invalid < 2; ++invalid) {
        auto wrong = snapshot;
        if (invalid == 0) wrong.Voters = {1, 2, 3}; else wrong.Data = "invalid application image";
        const auto bytes = EncodeSnapshot(wrong);
        raft->Process({}, Chunk(wrong, bytes, 0, bytes.size()));
        assert_false(replies.back()->Success); assert_int_equal(state->SnapshotIndex, 0);
        assert_int_equal(app->LastAppliedIndex, 0);
    }
    auto valid = Chunk(snapshot, encoded, 0, encoded.size()); raft->Process({}, valid);
    assert_true(replies.back()->Installed); assert_int_equal(app->Loads, 1);
    assert_true(raft->GetVolatileState()->LeaderContactDue == raft->GetVolatileState()->ElectionDue);
    assert_true(raft->GetVolatileState()->LeaderContactDue > ITimeSource::Time{});
    assert_true(Read(*app, "a") == "new");
    raft->Process({}, valid); assert_int_equal(app->Loads, 1); // duplicate completion is idempotent
    snapshot.Index = 3; auto stale = EncodeSnapshot(snapshot);
    raft->Process({}, Chunk(snapshot, stale, 0, stale.size()));
    assert_int_equal(app->LastAppliedIndex, 5); assert_int_equal(app->Loads, 1);
}

void test_older_snapshot_cannot_exceed_compacted_boundary_term(void**) {
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { replies.push_back(message.template Cast<TInstallSnapshotResponse>()); });
    auto state = std::make_shared<TState>(); state->CurrentTerm = 2;
    TKv image; image.Write(image.Prepare(Command("key", "committed")), 5);
    state->InstallSnapshot({5, 2, {1, 2}, image.SaveSnapshot()});
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    const auto before = static_cast<TStateFields>(*state);
    TSnapshot older{3, 3, {1, 2}, image.SaveSnapshot()};
    auto bytes = EncodeSnapshot(older); auto invalid = Chunk(older, bytes, 0, bytes.size()); invalid->Term = 9;
    raft->Process({}, invalid);
    assert_true(replies.empty()); assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(app->Loads, 1); assert_true(Read(*app, "key") == "committed");
    for (uint64_t term : {1, 2}) {
        older.Term = term; bytes = EncodeSnapshot(older);
        raft->Process({}, Chunk(older, bytes, 0, bytes.size()));
        assert_true(replies.back()->Success); assert_true(replies.back()->Installed);
        assert_int_equal(app->Loads, 1); assert_int_equal(state->SnapshotIndex, 5);
    }
}
void test_newer_snapshot_cannot_regress_committed_term(void**) {
    for (bool compacted : {false, true}) {
        std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
        auto peer = std::make_shared<TEndpoint>([&](auto message) {
            if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
                replies.push_back(message.template Cast<TInstallSnapshotResponse>());
        });
        auto state = std::make_shared<TState>();
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
        auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
        state->CurrentTerm = 2;
        for (uint64_t index = 1; index <= 3; ++index) {
            auto entry = app->Prepare(Command("key", std::to_string(index)));
            entry->Term = index == 1 ? 1 : 2; state->Append(entry);
        }
        auto heartbeat = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 2},
            TAppendEntriesRequest{.PrevLogIndex = 3, .PrevLogTerm = 2, .LeaderCommit = 3, .LeaderId = 1});
        raft->Process({}, heartbeat); processor.ProcessCommitted();
        if (compacted) {
            auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
            volatileState->HasCompactionSafeInitialConfigurationBarrier = true;
            volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
            raft->Compact();
        }
        assert_int_equal(app->LastAppliedIndex, 3); assert_true(Read(*app, "key") == "3");
        TKv source; source.Write(source.Prepare(Command("key", "future")), 4);
        TSnapshot snapshot{4, 1, {1, 2}, source.SaveSnapshot()};
        auto bytes = EncodeSnapshot(snapshot); auto request = Chunk(snapshot, bytes, 0, bytes.size());
        request->Term = 9; // An invalid higher-term request must not change hard state.
        const auto before = static_cast<TStateFields>(*state);
        raft->Process({}, request);
        assert_true(static_cast<TStateFields>(*state) == before);
        assert_true(replies.empty()); assert_int_equal(app->LastAppliedIndex, 3);
        assert_true(Read(*app, "key") == "3"); assert_int_equal(app->Loads, 0);
        auto conflicting = snapshot; conflicting.Index = 3;
        const auto conflictingBytes = EncodeSnapshot(conflicting);
        auto covered = Chunk(conflicting, conflictingBytes, 0, conflictingBytes.size()); covered->Term = 9;
        raft->Process({}, covered);
        assert_true(static_cast<TStateFields>(*state) == before); assert_true(replies.empty());
        snapshot.Term = 2; bytes = EncodeSnapshot(snapshot);
        raft->Process({}, Chunk(snapshot, bytes, 0, bytes.size()));
        assert_true(replies.back()->Installed); assert_int_equal(state->SnapshotIndex, 4);
        assert_int_equal(app->LastAppliedIndex, 4); assert_true(Read(*app, "key") == "future");
    }
}

void test_snapshot_is_rechecked_when_commit_advances_mid_transfer(void**) {
    std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            replies.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>(); auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    state->CurrentTerm = 2;
    for (uint64_t index = 1; index <= 3; ++index) {
        auto entry = app->Prepare(Command("key", std::to_string(index)));
        entry->Term = index == 1 ? 1 : 2; state->Append(entry);
    }
    auto heartbeat = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.PrevLogIndex = 3, .PrevLogTerm = 2, .LeaderCommit = 1, .LeaderId = 1});
    raft->Process({}, heartbeat); processor.ProcessCommitted();
    TKv source; source.Write(source.Prepare(Command("key", "future")), 4);
    TSnapshot snapshot{4, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);
    raft->Process({}, Chunk(snapshot, bytes, 0, 8));
    assert_true(replies.back()->Success); assert_false(replies.back()->Installed);
    heartbeat.Cast<TAppendEntriesRequest>()->LeaderCommit = 3;
    raft->Process({}, heartbeat); processor.ProcessCommitted();
    const auto before = static_cast<TStateFields>(*state); const auto replyCount = replies.size();
    raft->Process({}, Chunk(snapshot, bytes, 8, bytes.size() - 8));
    assert_int_equal(replies.size(), replyCount); assert_true(static_cast<TStateFields>(*state) == before);
    assert_int_equal(app->LastAppliedIndex, 3); assert_true(Read(*app, "key") == "3");
    snapshot.Term = 2; const auto valid = EncodeSnapshot(snapshot);
    raft->Process({}, Chunk(snapshot, valid, 0, valid.size()));
    assert_true(replies.back()->Installed); assert_true(Read(*app, "key") == "future");
}

void CheckSnapshotStorageTerm(IState& state, std::function<std::string()> image = [] { return std::string{}; }) {
    state.CurrentTerm = 2; state.Commit();
    state.InstallSnapshot({2, 2, {1}, "committed"});
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 2; state.Append(entry);
    const auto before = static_cast<TStateFields>(state);
    const auto beforeImage = image();
    Reject([&] { state.InstallSnapshot({4, 1, {1}, "regressed"}); });
    assert_true(static_cast<TStateFields>(state) == before);
    assert_true(image() == beforeImage);
    assert_true(state.GetSnapshot()->Data == "committed"); assert_int_equal(state.LogTerm(3), 2);
    state.InstallSnapshot({4, 2, {1}, "valid"});
    assert_int_equal(state.SnapshotIndex, 4); assert_true(state.GetSnapshot()->Data == "valid");
}
void test_memory_snapshot_storage_rejects_term_regression(void**) {
    TState state; CheckSnapshotStorageTerm(state);
}
#ifdef __linux__
void test_durable_snapshot_storage_rejects_term_regression(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-term-boundary-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto name = (dir / "raft").string();
    { TDurableState state(name, 1); CheckSnapshotStorageTerm(state, [&] {
        std::ifstream input(name + ".durable.1", std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), {});
      }); }
    { TDurableState state(name, 1); assert_int_equal(state.SnapshotIndex, 4);
      assert_int_equal(state.SnapshotTerm, 2); assert_true(state.GetSnapshot()->Data == "valid"); }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
#endif

void CheckSnapshotCannotConflictAtCommit(IState& state, std::function<std::string()> image = [] { return std::string{}; }) {
    state.CurrentTerm = 2;
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 2; state.Append(entry);
    state.CommitIndex = 1; state.Commit();
    const auto before = static_cast<TStateFields>(state);
    const auto beforeImage = image();
    Reject([&] { state.InstallSnapshot({1, 1, {1}, "conflicting committed boundary"}); });
    assert_true(static_cast<TStateFields>(state) == before);
    assert_true(image() == beforeImage);
}
void test_memory_snapshot_cannot_replace_committed_index_with_conflicting_term(void**) {
    TState state; CheckSnapshotCannotConflictAtCommit(state);
}
#ifdef __linux__
void test_durable_snapshot_cannot_replace_committed_index_with_conflicting_term(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-committed-snapshot-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto name = (dir / "raft").string();
    { TDurableState state(name, 1); CheckSnapshotCannotConflictAtCommit(state, [&] {
        std::ifstream input(name + ".durable.1", std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), {});
      }); }
    { TDurableState state(name, 1); assert_int_equal(state.CommitIndex, 1); }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
#endif

void test_remote_snapshot_retains_suffix_without_applying_it_before_commit(void**) {
    auto state = std::make_shared<TState>();
    TKv prefix;
    for (const std::string key : {"a", "b", "c", "d"}) {
        auto entry = prefix.Prepare(Command(key, "value"));
        state->Append(entry);
        if (state->LastLogIndex <= 2) prefix.Write(entry, state->LastLogIndex);
    }
    auto peer = std::make_shared<TEndpoint>([](auto) {});
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto app = std::make_shared<TApplication>(); TRequestProcessor processor(raft, app, {});
    TSnapshot snapshot{2, 1, {1, 2}, prefix.SaveSnapshot()}; const auto bytes = EncodeSnapshot(snapshot);
    raft->Process({}, Chunk(snapshot, bytes, 0, bytes.size())); processor.ProcessCommitted();
    assert_int_equal(state->LastLogIndex, 4); assert_int_equal(app->LastAppliedIndex, 2);
    assert_true(Read(*app, "b") == "value"); assert_true(Read(*app, "c").empty());
    auto heartbeat = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.PrevLogIndex = 4, .PrevLogTerm = 1, .LeaderCommit = 4, .LeaderId = 1});
    raft->Process({}, heartbeat); processor.ProcessCommitted();
    assert_true(Read(*app, "d") == "value"); assert_int_equal(app->LastAppliedIndex, 4);
    assert_int_equal(app->Loads, 1);
}
void test_snapshot_boundary_index_exhaustion_does_not_append_wrapped_noop(void**) {
    auto state = std::make_shared<TState>();
    TKv image;
    state->InstallSnapshot({uint64_t(INT64_MAX), 1, {1}, image.SaveSnapshot()});
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto app = std::make_shared<TKv>(); TRequestProcessor processor(raft, app, {});
    Reject([&] { raft->ProcessTimeout({}); });
    assert_true(state->LastLogIndex == uint64_t(INT64_MAX));
    assert_int_equal(state->LastLogTerm, 1);
}
void CheckStorageIndexExhaustion(IState& state, std::function<std::string()> image = [] { return std::string{}; }) {
    state.InstallSnapshot({uint64_t(INT64_MAX) - 1, 1, {1}, "boundary"});
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1; state.Append(entry);
    assert_true(state.LastLogIndex == uint64_t(INT64_MAX));
    const auto before = static_cast<TStateFields>(state); const auto beforeImage = image();
    Reject([&] { state.Append(entry); });
    assert_true(static_cast<TStateFields>(state) == before); assert_true(image() == beforeImage);
    assert_int_equal(state.LogTerm(INT64_MAX), 1); assert_true(state.GetSnapshot()->Data == "boundary");
}
void test_memory_storage_rejects_index_exhaustion(void**) {
    TState state; CheckStorageIndexExhaustion(state);
}
#ifdef __linux__
void test_durable_storage_rejects_index_exhaustion(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-index-limit-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    auto name = (dir / "raft").string();
    { TDurableState state(name, 1); CheckStorageIndexExhaustion(state, [&] {
        std::ifstream input(name + ".durable.1", std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), {});
      }); }
    { TDurableState state(name, 1); assert_true(state.LastLogIndex == uint64_t(INT64_MAX));
      assert_int_equal(state.LogTerm(INT64_MAX), 1); }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
#endif

void ExerciseBoundary(IState& state) {
    state.CurrentTerm = 3; state.VotedFor = 2; state.Commit();
    for (uint64_t term : {1, 2, 3}) { auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = term; state.Append(entry); }
    state.InstallSnapshot({2, 2, {1, 2}, "application"});
    assert_int_equal(state.LastLogIndex, 3); assert_int_equal(state.SnapshotIndex, 2);
    assert_int_equal(state.LogTerm(1), 0); assert_int_equal(state.LogTerm(2), 2); assert_int_equal(state.Get(3)->Term, 3);
    assert_false(state.Get(1)); assert_false(state.Get(2));
    state.RemoveLast();
    assert_int_equal(state.LastLogIndex, 2); assert_int_equal(state.LastLogTerm, 2);
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 3; state.Append(entry);
    assert_int_equal(state.LastLogIndex, 3); assert_int_equal(state.Get(3)->Term, 3);
    state.InstallSnapshot({3, 2, {1, 2}, "different boundary"});
    assert_int_equal(state.LastLogIndex, 3); assert_int_equal(state.LastLogTerm, 2); assert_false(state.Get(3));
    Reject([&] { state.InstallSnapshot({2, 2, {1, 2}, "obsolete"}); });
    assert_int_equal(state.SnapshotIndex, 3);
}
void test_memory_snapshot_boundary_retains_matching_suffix_and_replaces_conflict(void**) { TState state; ExerciseBoundary(state); }
#ifdef __linux__
struct TFiles {
    std::filesystem::path Directory;
    std::string Name;
    TFiles() {
        static unsigned next = 0;
        Directory = std::filesystem::temp_directory_path() / ("miniraft-compaction-" + std::to_string(getpid()) + "-" + std::to_string(next++));
        assert_true(std::filesystem::create_directory(Directory)); Name = (Directory / "raft").string();
    }
    ~TFiles() { std::error_code ignored; std::filesystem::remove_all(Directory, ignored); }
};
void test_durable_stale_snapshot_membership_recovery_survives_restart(void**) {
    for (bool jointSuccessor : {false, true}) {
        TFiles files;
        const std::vector<uint32_t> original{1, 2, 3};
        const std::vector<uint32_t> intermediate{2, 3, 4};
        const std::vector<uint32_t> stableVoters{3, 4, 5};
        const std::vector<uint32_t> jointVoters{4, 5, 6};

        TKv image;
        image.Write(image.Prepare(Command("membership", "before recovery")), 5);
        TSnapshot known{5, 2, original, image.SaveSnapshot()};
        {
            TDurableState state(files.Name, 2);
            state.BindVoters(original);
            state.CurrentTerm = 3;
            state.Commit();
            state.InstallSnapshot(known);
        }

        const auto snapshotIndex = jointSuccessor ? 9 : 10;
        const auto value = jointSuccessor ? "recovered joint" : "recovered stable";
        image.Write(image.Prepare(Command("membership", value)), snapshotIndex);

        const TSnapshotConfigurationTransition anchor{original, intermediate, 0, 0, 6, 3, 7, 3};
        const TSnapshotConfigurationTransition tail{intermediate,
            jointSuccessor ? jointVoters : stableVoters, 7, 3, 8, 3,
            jointSuccessor ? 0u : 9u, jointSuccessor ? 0u : 3u};
        TSnapshot incoming;
        incoming.Index = snapshotIndex;
        incoming.Term = 3;
        incoming.Data = image.SaveSnapshot();
        incoming.ConfigurationTransitions = {anchor, tail};
        incoming.PreviousVoters = intermediate;
        incoming.PreviousConfigurationIndex = 7;
        incoming.PreviousConfigurationTerm = 3;
        incoming.PreviousJointIndex = 8;
        incoming.PreviousJointTerm = 3;
        if (jointSuccessor) {
            incoming.Voters = intermediate;
            incoming.JointVoters = jointVoters;
            incoming.ConfigurationIndex = 8;
            incoming.ConfigurationTerm = 3;
        } else {
            incoming.Voters = stableVoters;
            incoming.StableConfiguration = true;
            incoming.ConfigurationIndex = 9;
            incoming.ConfigurationTerm = 3;
        }

        std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
        auto buildRaft = [&](std::shared_ptr<IState> state, std::shared_ptr<TApplication> application) {
            TNodeDict peers;
            for (uint32_t id : {1, 3, 4, 5, 6})
                peers.emplace(id, std::make_shared<TEndpoint>([&](auto message) {
                    replies.push_back(message.template Cast<TInstallSnapshotResponse>());
                }));
            auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{4, 5, 6});
            auto processor = std::make_unique<TRequestProcessor>(raft, application, peers);
            return std::pair{std::move(raft), std::move(processor)};
        };
        auto install = [&](const std::shared_ptr<TRaft>& raft, const TSnapshot& candidate) {
            const auto bytes = EncodeSnapshot(candidate);
            auto request = Chunk(candidate, bytes, 0, static_cast<uint32_t>(bytes.size()));
            request->Src = 3;
            request->LeaderId = 3;
            raft->Process({}, request);
            return replies.back();
        };

        {
            auto state = std::make_shared<TDurableState>(files.Name, 2);
            auto application = std::make_shared<TApplication>();
            auto [raft, processor] = buildRaft(state, application);
            assert_int_equal(application->LastAppliedIndex, known.Index);

            auto conflict = incoming;
            const std::vector<uint32_t> conflictingIntermediate{2, 4, 5};
            conflict.ConfigurationTransitions[0].NewVoters = conflictingIntermediate;
            conflict.ConfigurationTransitions[1].OldVoters = conflictingIntermediate;
            conflict.PreviousVoters = conflictingIntermediate;
            if (jointSuccessor) conflict.Voters = conflictingIntermediate;
            auto response = install(raft, conflict);
            assert_false(response->Success);
            assert_false(response->Installed);
            assert_true(*state->GetSnapshot() == known);
            assert_int_equal(application->Loads, 1);

            response = install(raft, incoming);
            assert_true(response->Success);
            assert_true(response->Installed);
            assert_true(state->GetBoundVoters() == original);
            assert_int_equal(state->CommitIndex, snapshotIndex);
            assert_true(state->GetSnapshot()->ConfigurationTransitions ==
                std::vector<TSnapshotConfigurationTransition>({anchor, tail}));
            assert_true(raft->GetConfigurationState().Committed.IsJoint() == jointSuccessor);
            assert_int_equal(application->LastAppliedIndex, snapshotIndex);
            assert_true(Read(*application, "membership") == value);
        }

        replies.clear();
        auto recovered = std::make_shared<TDurableState>(files.Name, 2);
        assert_true(recovered->GetBoundVoters() == original);
        assert_int_equal(recovered->SnapshotIndex, snapshotIndex);
        assert_int_equal(recovered->CommitIndex, snapshotIndex);
        assert_true(recovered->GetSnapshot()->ConfigurationTransitions ==
            std::vector<TSnapshotConfigurationTransition>({anchor, tail}));
        auto application = std::make_shared<TApplication>();
        auto [raft, processor] = buildRaft(recovered, application);
        assert_true(raft->GetConfigurationState().Committed.IsJoint() == jointSuccessor);
        if (jointSuccessor)
            assert_true(raft->GetVoterIds() == std::vector<uint32_t>({2, 3, 4, 5, 6}));
        else
            assert_true(raft->GetVoterIds() == stableVoters);
        assert_int_equal(application->LastAppliedIndex, snapshotIndex);
        assert_true(Read(*application, "membership") == value);
    }
}
void test_durable_membership_snapshot_install_is_atomic_across_restart(void**) {
    using TStage = TDurableState::EStage;
    const std::vector<TStage> stages{TStage::TempCreated, TStage::PayloadStarted, TStage::Written,
        TStage::FileSynced, TStage::Renamed, TStage::DirectorySynced};
    for (bool jointSuccessor : {false, true}) for (auto stage : stages) {
        TFiles files;
        const std::vector<uint32_t> original{1, 2, 3};
        const std::vector<uint32_t> intermediate{2, 3, 4};
        const std::vector<uint32_t> stableVoters{3, 4, 5};
        const std::vector<uint32_t> jointVoters{4, 5, 6};
        TKv image;
        image.Write(image.Prepare(Command("membership", "before crash")), 5);
        const TSnapshot known{5, 2, original, image.SaveSnapshot()};
        {
            TDurableState state(files.Name, 2);
            state.BindVoters(original);
            state.CurrentTerm = 3;
            state.Commit();
            state.InstallSnapshot(known);
        }

        const auto snapshotIndex = jointSuccessor ? 9 : 10;
        const auto value = jointSuccessor ? "recovered joint" : "recovered stable";
        image.Write(image.Prepare(Command("membership", value)), snapshotIndex);
        const TSnapshotConfigurationTransition anchor{original, intermediate, 0, 0, 6, 3, 7, 3};
        const TSnapshotConfigurationTransition tail{intermediate,
            jointSuccessor ? jointVoters : stableVoters, 7, 3, 8, 3,
            jointSuccessor ? 0u : 9u, jointSuccessor ? 0u : 3u};
        TSnapshot incoming;
        incoming.Index = snapshotIndex;
        incoming.Term = 3;
        incoming.Data = image.SaveSnapshot();
        incoming.ConfigurationTransitions = {anchor, tail};
        incoming.PreviousVoters = intermediate;
        incoming.PreviousConfigurationIndex = 7;
        incoming.PreviousConfigurationTerm = 3;
        incoming.PreviousJointIndex = 8;
        incoming.PreviousJointTerm = 3;
        if (jointSuccessor) {
            incoming.Voters = intermediate;
            incoming.JointVoters = jointVoters;
            incoming.ConfigurationIndex = 8;
            incoming.ConfigurationTerm = 3;
        } else {
            incoming.Voters = stableVoters;
            incoming.StableConfiguration = true;
            incoming.ConfigurationIndex = 9;
            incoming.ConfigurationTerm = 3;
        }
        const auto bytes = EncodeSnapshot(incoming);
        auto request = Chunk(incoming, bytes, 0, static_cast<uint32_t>(bytes.size()));
        request->Src = 3;
        request->LeaderId = 3;

        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                auto state = std::make_shared<TDurableState>(files.Name, 2, [&](auto point) {
                    if (point == stage) _exit(90);
                });
                TNodeDict peers;
                for (uint32_t id : {1, 3, 4, 5, 6})
                    peers.emplace(id, std::make_shared<TEndpoint>([](auto) {}));
                auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{4, 5, 6});
                auto application = std::make_shared<TApplication>();
                TRequestProcessor processor(raft, application, peers);
                raft->Process({}, request);
            } catch (...) { _exit(91); }
            _exit(92);
        }
        int status = 0;
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status));
        assert_int_equal(WEXITSTATUS(status), 90);

        const bool updated = stage == TStage::Renamed || stage == TStage::DirectorySynced;
        auto recovered = std::make_shared<TDurableState>(files.Name, 2);
        assert_true(recovered->GetBoundVoters() == original);
        const auto expectedIndex = updated ? snapshotIndex : known.Index;
        assert_int_equal(recovered->SnapshotIndex, expectedIndex);
        assert_int_equal(recovered->CommitIndex, expectedIndex);
        assert_true(recovered->GetSnapshot()->Data == (updated ? incoming.Data : known.Data));
        auto application = std::make_shared<TApplication>();
        TNodeDict peers;
        for (uint32_t id : {1, 3, 4, 5, 6})
            peers.emplace(id, std::make_shared<TEndpoint>([](auto) {}));
        auto raft = std::make_shared<TRaft>(recovered, 2, peers, std::vector<uint32_t>{4, 5, 6});
        TRequestProcessor processor(raft, application, peers);
        assert_true(raft->GetConfigurationState().Committed.IsJoint() == (updated && jointSuccessor));
        const auto expectedVoters = updated
            ? jointSuccessor ? std::vector<uint32_t>{2, 3, 4, 5, 6} : stableVoters
            : original;
        assert_true(raft->GetVoterIds() == expectedVoters);
        assert_int_equal(application->LastAppliedIndex, expectedIndex);
        assert_true(Read(*application, "membership") == (updated ? value : "before crash"));
        if (updated)
            assert_true(recovered->GetSnapshot()->ConfigurationTransitions ==
                std::vector<TSnapshotConfigurationTransition>({anchor, tail}));
    }
}
struct TCountingDurable : TDurableState {
    using TDurableState::TDurableState;
    mutable unsigned Reads = 0;
    TMessageHolder<TLogEntry> Get(int64_t index) const override {
        ++Reads; return TDurableState::Get(index);
    }
};
void test_durable_term_lookup_avoids_payload_copies_across_compaction(void**) {
    TFiles files; TCountingDurable state(files.Name, 1);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1024 * 1024);
    entry->Data[0] = 'x'; state.Append(entry); state.Append(entry);
    state.InstallSnapshot({1, 1, {1}, "image"});
    for (int i = 0; i < 20; ++i) {
        assert_int_equal(state.LogTerm(1), 1); assert_int_equal(state.LogTerm(2), 1);
        assert_int_equal(state.LogTerm(3), 0);
    }
    assert_int_equal(state.Reads, 0);
    auto read = state.Get(2); read->Data[0] = 'y'; read->Term = 2;
    assert_int_equal(state.LogTerm(2), 1); assert_int_equal(state.Get(2)->Data[0], 'x');
}
void test_durable_compacted_boundary_survives_restart(void**) {
    TFiles files;
    { TDurableState state(files.Name, 1); ExerciseBoundary(state); }
    TDurableState state(files.Name, 1);
    assert_int_equal(state.SnapshotIndex, 3); assert_int_equal(state.LogTerm(3), 2);
    assert_int_equal(state.CurrentTerm, 3); assert_int_equal(state.VotedFor, 2);
    assert_true(state.GetSnapshot()->Data == "different boundary");
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 3; state.Append(entry);
    assert_int_equal(state.LastLogIndex, 4); assert_int_equal(state.Get(4)->Term, 3);
}
void test_snapshot_and_suffix_are_atomic_at_every_publication_stage(void**) {
    using TStage = TDurableState::EStage;
    for (bool priorSnapshot : {false, true}) for (bool retain : {false, true}) for (auto stage : {TStage::TempCreated, TStage::Written, TStage::FileSynced,
         TStage::Renamed, TStage::DirectorySynced, TStage::SnapshotTempCreated, TStage::SnapshotWritten,
         TStage::SnapshotFileSynced, TStage::SnapshotRenamed, TStage::SnapshotDirectorySynced}) {
        TFiles files;
        { TDurableState state(files.Name, 1); state.CurrentTerm = 3; state.VotedFor = 2;
          for (uint64_t term : {1, 2, 3}) { auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = term; state.Append(entry); }
          if (priorSnapshot) state.InstallSnapshot({1, 1, {1, 2}, "previous application"}); }
        const auto child = fork(); assert_true(child >= 0);
        if (child == 0) {
            try { TDurableState state(files.Name, 1, [&](auto point) { if (point == stage) _exit(90); });
                state.InstallSnapshot({retain ? 2u : 4u, retain ? 2u : 3u, {1, 2}, "durable application"});
            } catch (...) { _exit(91); } _exit(92);
        }
        int status = 0; assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status)); assert_int_equal(WEXITSTATUS(status), 90);
        TDurableState state(files.Name, 1);
        const bool updated = stage <= TStage::DirectorySynced && stage >= TStage::Renamed;
        assert_int_equal(state.CurrentTerm, 3); assert_int_equal(state.VotedFor, 2);
        assert_int_equal(state.SnapshotIndex, updated ? retain ? 2 : 4 : priorSnapshot ? 1 : 0);
        assert_int_equal(state.LastLogIndex, updated && !retain ? 4 : 3);
        if (updated) assert_true(state.GetSnapshot()->Data == "durable application");
        else if (priorSnapshot) assert_true(state.GetSnapshot()->Data == "previous application");
        if (retain || !updated) assert_int_equal(state.Get(3)->Term, 3);
    }
}
void test_remote_snapshot_install_crash_before_application_load_recovers_on_restart(void**) {
    TFiles files;
    {
        TDurableState state(files.Name, 2);
        state.BindVoters({1, 2});
    }

    TKv source;
    source.Write(source.Prepare(Command("crash-window", "durable image")), 1);
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);
    assert_true(bytes.size() <= TInstallSnapshotRequest::MaxChunkBytes);
    auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + bytes.size());
    request->Src = 1; request->Dst = 2; request->Term = 1; request->Seqno = 1;
    request->SnapshotIndex = snapshot.Index; request->SnapshotTerm = snapshot.Term;
    request->Offset = 0; request->TotalBytes = bytes.size(); request->Done = 1; request->LeaderId = 1;
    memcpy(request->Data, bytes.data(), bytes.size());

    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        struct TCrashDuringLoad final : TKv {
            void LoadSnapshot(std::string_view, uint64_t) override { _exit(87); }
        };
        try {
            auto state = std::make_shared<TDurableState>(files.Name, 2);
            TNodeDict peers{{1, std::make_shared<TEndpoint>([](auto) {})}};
            auto raft = std::make_shared<TRaft>(state, 2, peers);
            auto application = std::make_shared<TCrashDuringLoad>();
            TRequestProcessor processor(raft, application, peers);
            raft->Process({}, request);
        } catch (...) { _exit(91); }
        _exit(92);
    }
    int status = 0;
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 87);

    {
        auto state = std::make_shared<TDurableState>(files.Name, 2);
        assert_int_equal(state->SnapshotIndex, snapshot.Index);
        assert_int_equal(state->CommitIndex, snapshot.Index);
        assert_true(state->GetSnapshot() && state->GetSnapshot()->Data == snapshot.Data);
        auto raft = std::make_shared<TRaft>(state, 2,
            TNodeDict{{1, std::make_shared<TEndpoint>([](auto) {})}});
        auto application = std::make_shared<TKv>();
        TRequestProcessor processor(raft, application, {});
        assert_int_equal(application->LastAppliedIndex, snapshot.Index);
        assert_true(Read(*application, "crash-window") == "durable image");
    }
}
void test_compaction_reclaims_durable_prefix_bytes(void**) {
    TFiles files; TDurableState state(files.Name, 1);
    for (int i = 0; i < 30; ++i) {
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 4096);
        memset(entry->Data, 0, 4096); state.Append(entry);
    }
    const auto before = std::filesystem::file_size(files.Name + ".durable.1");
    state.InstallSnapshot({29, 1, {1}, "small current application"});
    const auto after = std::filesystem::file_size(files.Name + ".durable.1");
    assert_true(after < before / 10); assert_int_equal(state.LastLogIndex, 30);
    assert_true(bool(state.Get(30))); assert_false(state.Get(29));
}
void test_durable_v6_semantic_corruption_rejects_without_rewriting(void**) {
    for (int invalid = 0; invalid < 3; ++invalid) {
        TFiles files;
        { TDurableState state(files.Name, 1); ExerciseBoundary(state); }
        const auto path = files.Name + ".durable.1";
        std::ifstream input(path, std::ios::binary);
        std::string bytes(std::istreambuf_iterator<char>(input), {}); input.close();
        assert_true(bytes.substr(0, 8) == "MRFTD006");
        const auto snapshotPath = path + ".snapshot.3.2";
        if (invalid < 2) {
            const auto offset = invalid == 0 ? 52 : 32; // snapshot index reference or retained entry count
            for (size_t i = offset; i < size_t(offset + 8); ++i) bytes[i] = char(0xff);
            const auto crc = NSnapshotCodec::Checksum(bytes.substr(0, 8) + bytes.substr(20));
            for (int i = 0; i < 4; ++i) bytes[16 + i] = char(crc >> (8 * i));
            { std::ofstream out(path, std::ios::binary); out.write(bytes.data(), bytes.size()); }
        } else {
            std::ifstream sidecar(snapshotPath, std::ios::binary);
            std::string snapshot(std::istreambuf_iterator<char>(sidecar), {});
            snapshot.back() ^= 1; // inner snapshot checksum; manifest remains valid
            std::ofstream out(snapshotPath, std::ios::binary);
            out.write(snapshot.data(), snapshot.size());
        }
        Reject([&] { TDurableState state(files.Name, 1); });
        std::ifstream after(path, std::ios::binary);
        assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == bytes);
    }
}
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_kv_snapshot_precomputes_size_without_changing_encoding),
        cmocka_unit_test(test_snapshot_capture_preflights_application_size_bound),
        cmocka_unit_test(test_shared_compaction_reuses_storage_snapshot_image),
        cmocka_unit_test(test_zero_index_follower_compaction_waits_for_capability_barrier),
        cmocka_unit_test(test_snapshot_history_encoding_is_negotiated_per_peer),
        cmocka_unit_test(test_idle_snapshot_transfer_discards_partial_image_and_restarts),
        cmocka_unit_test(test_snapshot_install_budget_is_shared_and_reclaimed_after_idle_expiry),
        cmocka_unit_test(test_snapshot_install_budget_releases_after_application_validation_failure),
        cmocka_unit_test(test_snapshot_install_budget_releases_after_conflicting_duplicate),
        cmocka_unit_test(test_covered_snapshot_metadata_uses_shared_install_budget),
        cmocka_unit_test(test_snapshot_install_accepts_every_small_fixed_chunk_size),
        cmocka_unit_test(test_compaction_lagging_follower_installs_snapshot_then_tail),
        cmocka_unit_test(test_simultaneous_startup_candidates_use_distinct_election_deadlines),
        cmocka_unit_test(test_partial_transfer_restart_and_delayed_ack_are_safe),
        cmocka_unit_test(test_snapshot_partial_ack_cannot_skip_unsent_bytes),
        cmocka_unit_test(test_snapshot_timeout_retries_do_not_starve_delayed_acks),
        cmocka_unit_test(test_snapshot_read_traffic_does_not_starve_delayed_acks),
        cmocka_unit_test(test_compaction_during_each_chunk_cannot_restart_snapshot_catchup),
        cmocka_unit_test(test_stalled_snapshot_transfer_expires_and_releases_pinned_generation),
        cmocka_unit_test(test_rejected_snapshot_reprobes_peer_capabilities_before_retry),
        cmocka_unit_test(test_leader_change_mid_snapshot_rejects_old_chunks),
        cmocka_unit_test(test_partial_and_installed_snapshot_acks_do_not_confirm_strong_reads),
        cmocka_unit_test(test_compacted_follower_hint_requires_boundary_probe_before_match),
        cmocka_unit_test(test_corrupt_snapshot_and_invalid_headers_cannot_mutate_application),
        cmocka_unit_test(test_newer_snapshot_cannot_regress_committed_term),
        cmocka_unit_test(test_older_snapshot_cannot_exceed_compacted_boundary_term),
        cmocka_unit_test(test_snapshot_is_rechecked_when_commit_advances_mid_transfer),
        cmocka_unit_test(test_memory_snapshot_storage_rejects_term_regression),
        cmocka_unit_test(test_memory_snapshot_cannot_replace_committed_index_with_conflicting_term),
        cmocka_unit_test(test_joint_snapshot_is_rejected_without_matching_history_even_if_boundary_is_covered),
        cmocka_unit_test(test_higher_term_snapshot_defers_term_change_until_split_metadata_is_validated),
        cmocka_unit_test(test_invalid_split_higher_term_snapshot_preserves_candidate_state),
        cmocka_unit_test(test_valid_split_higher_term_covered_snapshot_accepts_leader_without_reload),
        cmocka_unit_test(test_unmatched_joint_snapshot_does_not_accept_leader_contact),
        cmocka_unit_test(test_stable_snapshot_completes_known_joint_membership_only_from_new_voter),
        cmocka_unit_test(test_stale_stable_snapshot_recovers_multi_transition_stable_membership),
        cmocka_unit_test(test_stale_stable_snapshot_recovers_multi_transition_joint_membership),
        cmocka_unit_test(test_unproven_stable_configuration_snapshot_is_rejected_for_fixed_membership),
        cmocka_unit_test(test_idle_partial_snapshot_expires_on_learner_despite_heartbeats),
        cmocka_unit_test(test_remote_snapshot_retains_suffix_without_applying_it_before_commit),
        cmocka_unit_test(test_snapshot_boundary_index_exhaustion_does_not_append_wrapped_noop),
        cmocka_unit_test(test_memory_storage_rejects_index_exhaustion),
        cmocka_unit_test(test_memory_snapshot_boundary_retains_matching_suffix_and_replaces_conflict),
#ifdef __linux__
        cmocka_unit_test(test_durable_snapshot_storage_rejects_term_regression),
        cmocka_unit_test(test_durable_snapshot_cannot_replace_committed_index_with_conflicting_term),
        cmocka_unit_test(test_durable_storage_rejects_index_exhaustion),
        cmocka_unit_test(test_durable_term_lookup_avoids_payload_copies_across_compaction),
        cmocka_unit_test(test_durable_compacted_boundary_survives_restart),
        cmocka_unit_test(test_snapshot_and_suffix_are_atomic_at_every_publication_stage),
        cmocka_unit_test(test_remote_snapshot_install_crash_before_application_load_recovers_on_restart),
        cmocka_unit_test(test_durable_stale_snapshot_membership_recovery_survives_restart),
        cmocka_unit_test(test_durable_membership_snapshot_install_is_atomic_across_restart),
        cmocka_unit_test(test_compaction_reclaims_durable_prefix_bytes),
        cmocka_unit_test(test_durable_v6_semantic_corruption_rejects_without_rewriting),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
