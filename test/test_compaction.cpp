#include <miniraft/raft.h>
#include <miniraft/durable.h>
#include <miniraft/snapshot_codec.h>
#include "../examples/kv.h"
#include "../examples/kv_messages.h"
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
struct TCluster {
    ITimeSource::Time Now{};
    std::vector<std::shared_ptr<TRaft>> Nodes{3};
    std::vector<std::shared_ptr<TApplication>> Apps{3};
    std::vector<std::unique_ptr<TRequestProcessor>> Processors{3};
    std::deque<TMessageHolder<TMessage>> Packets;
    std::set<uint32_t> Isolated;
    std::vector<uint32_t> Learners;
    explicit TCluster(std::vector<uint32_t> learners = {}) : Learners(std::move(learners)) {
        for (uint32_t id = 1; id <= 3; ++id) Build(id, std::make_shared<TState>());
    }
    void Build(uint32_t id, std::shared_ptr<IState> state) {
        TNodeDict peers;
        for (uint32_t dst = 1; dst <= 3; ++dst) if (dst != id)
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
        Now += std::chrono::seconds(10);
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
        Elect(1); Pump(1); Isolated = {3};
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
};
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
    cluster.Drain(); // all voted for themselves in the first term
    std::set<ITimeSource::Time> deadlines;
    uint32_t first = 1;
    for (uint32_t id = 1; id <= 3; ++id) {
        const auto due = cluster.Nodes[id - 1]->GetVolatileState()->ElectionDue;
        deadlines.insert(due);
        if (due < cluster.Nodes[first - 1]->GetVolatileState()->ElectionDue) first = id;
    }
    assert_int_equal(deadlines.size(), 3);
    cluster.Now = *deadlines.begin();
    for (auto& node : cluster.Nodes) node->ProcessTimeout(cluster.Now);
    cluster.Drain();
    cluster.Nodes[first - 1]->ProcessTimeout(cluster.Now); cluster.Drain();
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
    auto restarted = cluster.SendChunk(); assert_int_equal(restarted->Offset, 0);
    cluster.AckChunk(restarted); cluster.Pump(1, 80, true);
    assert_int_equal(cluster.Apps[2]->Loads, 1);
    assert_true(Read(*cluster.Apps[2], "c") == std::string(60000, 'c'));
}
void ExerciseDelayedSnapshotAcks(bool readTraffic) {
    for (auto learners : {std::vector<uint32_t>{}, std::vector<uint32_t>{3}}) {
        TCluster cluster(learners); cluster.Prepare();
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
    assert_true(Read(*app, "a") == "new");
    raft->Process({}, valid); assert_int_equal(app->Loads, 1); // duplicate completion is idempotent
    snapshot.Index = 3; auto stale = EncodeSnapshot(snapshot);
    raft->Process({}, Chunk(snapshot, stale, 0, stale.size()));
    assert_int_equal(app->LastAppliedIndex, 5); assert_int_equal(app->Loads, 1);
}

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

void ExerciseBoundary(IState& state) {
    state.CurrentTerm = 3; state.VotedFor = 2; state.Commit();
    for (uint64_t term : {1, 2, 3}) { auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = term; state.Append(entry); }
    state.InstallSnapshot({2, 2, {1, 2}, "application"});
    assert_int_equal(state.LastLogIndex, 3); assert_int_equal(state.SnapshotIndex, 2);
    assert_int_equal(state.LogTerm(1), 0); assert_int_equal(state.LogTerm(2), 2); assert_int_equal(state.Get(3)->Term, 3);
    assert_false(state.Get(1)); assert_false(state.Get(2));
    state.RemoveLast(); state.RemoveLast();
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
         TStage::Renamed, TStage::DirectorySynced}) {
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
        const bool updated = stage >= TStage::Renamed;
        assert_int_equal(state.CurrentTerm, 3); assert_int_equal(state.VotedFor, 2);
        assert_int_equal(state.SnapshotIndex, updated ? retain ? 2 : 4 : priorSnapshot ? 1 : 0);
        assert_int_equal(state.LastLogIndex, updated && !retain ? 4 : 3);
        if (updated) assert_true(state.GetSnapshot()->Data == "durable application");
        else if (priorSnapshot) assert_true(state.GetSnapshot()->Data == "previous application");
        if (retain || !updated) assert_int_equal(state.Get(3)->Term, 3);
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
void test_durable_v2_semantic_corruption_rejects_without_rewriting(void**) {
    for (int invalid = 0; invalid < 3; ++invalid) {
        TFiles files;
        { TDurableState state(files.Name, 1); ExerciseBoundary(state); }
        const auto path = files.Name + ".durable.1";
        std::ifstream input(path, std::ios::binary);
        std::string bytes(std::istreambuf_iterator<char>(input), {}); input.close();
        assert_true(bytes.substr(0, 8) == "MRFTD002");
        if (invalid < 2) {
            const auto offset = invalid == 0 ? 40 : 32; // snapshot length or retained entry count
            for (size_t i = offset; i < size_t(offset + 8); ++i) bytes[i] = char(0xff);
        } else bytes.back() ^= 1; // inner snapshot checksum with a valid outer checksum
        const auto crc = NSnapshotCodec::Checksum(bytes.substr(0, 8) + bytes.substr(20));
        for (int i = 0; i < 4; ++i) bytes[16 + i] = char(crc >> (8 * i));
        { std::ofstream out(path, std::ios::binary); out.write(bytes.data(), bytes.size()); }
        Reject([&] { TDurableState state(files.Name, 1); });
        std::ifstream after(path, std::ios::binary);
        assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == bytes);
    }
}
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_compaction_lagging_follower_installs_snapshot_then_tail),
        cmocka_unit_test(test_simultaneous_startup_candidates_use_distinct_election_deadlines),
        cmocka_unit_test(test_partial_transfer_restart_and_delayed_ack_are_safe),
        cmocka_unit_test(test_snapshot_timeout_retries_do_not_starve_delayed_acks),
        cmocka_unit_test(test_snapshot_read_traffic_does_not_starve_delayed_acks),
        cmocka_unit_test(test_leader_change_mid_snapshot_rejects_old_chunks),
        cmocka_unit_test(test_partial_and_installed_snapshot_acks_do_not_confirm_strong_reads),
        cmocka_unit_test(test_compacted_follower_hint_requires_boundary_probe_before_match),
        cmocka_unit_test(test_corrupt_snapshot_and_invalid_headers_cannot_mutate_application),
        cmocka_unit_test(test_remote_snapshot_retains_suffix_without_applying_it_before_commit),
        cmocka_unit_test(test_snapshot_boundary_index_exhaustion_does_not_append_wrapped_noop),
        cmocka_unit_test(test_memory_snapshot_boundary_retains_matching_suffix_and_replaces_conflict),
#ifdef __linux__
        cmocka_unit_test(test_durable_compacted_boundary_survives_restart),
        cmocka_unit_test(test_snapshot_and_suffix_are_atomic_at_every_publication_stage),
        cmocka_unit_test(test_compaction_reclaims_durable_prefix_bytes),
        cmocka_unit_test(test_durable_v2_semantic_corruption_rejects_without_rewriting),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
