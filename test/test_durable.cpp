#include <miniraft/durable.h>
#include <miniraft/configuration.h>
#include <miniraft/persist.h>
#include <miniraft/raft.h>
#include <miniraft/snapshot_codec.h>
#include "../examples/idempotent_kv_rsm.h"
#include "../examples/kv_messages.h"
#include <filesystem>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <set>
#include <tuple>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <csignal>
#include <unistd.h>
extern "C" {
#include <cmocka.h>
}

namespace {
namespace fs = std::filesystem;
using TStage = TDurableState::EStage;
struct TFiles {
    fs::path Directory;
    std::string Name;
    TFiles() {
        static unsigned next = 0;
        Directory = fs::temp_directory_path() / ("miniraft-durable-" +
            std::to_string(getpid()) + "-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(next++));
        assert_true(fs::create_directory(Directory));
        Name = (Directory / "raft").string();
    }
    ~TFiles() { std::error_code ignored; fs::remove_all(Directory, ignored); }
    fs::path Image() const { return Name + ".durable.1"; }
    void Seed();
};
void Put(std::string& bytes, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes.push_back(char(value >> (8 * i)));
}
uint32_t Crc32(std::string_view bytes) {
    uint32_t crc = UINT32_MAX;
    for (unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xedb88320u & (0u - (crc & 1u)));
    }
    return ~crc;
}
std::string LegacyImage(std::string_view magic, bool hasSnapshot, bool configured,
        const TSnapshot* replacementSnapshot = nullptr, uint32_t votedFor = 1,
        const std::vector<TMessageHolder<TLogEntry>>* replacementEntries = nullptr) {
    std::string payload;
    Put(payload, 3, 8); // current term
    Put(payload, votedFor, 4);
    Put(payload, replacementEntries ? replacementEntries->size() : 1, 8); // retained entry count
    if (configured) {
        Put(payload, 2, 4); Put(payload, 1, 4); Put(payload, 2, 4);
    }
    if (hasSnapshot) {
        const auto image = EncodeSnapshot(replacementSnapshot ? *replacementSnapshot :
            TSnapshot{1, 2, {1, 2}, "legacy snapshot"});
        Put(payload, image.size(), 8); payload.append(image);
    }
    if (replacementEntries) {
        for (const auto& entry : *replacementEntries) {
            const auto dataSize = entry->Len - sizeof(TLogEntry);
            Put(payload, entry->Term, 8);
            Put(payload, entry->Flags, 8);
            Put(payload, dataSize, 4);
            payload.append(entry->Data, dataSize);
        }
    } else {
        Put(payload, 3, 8); Put(payload, 0, 8); Put(payload, 4, 4); payload.append("tail");
    }
    std::string checksumInput;
    if (magic != "MRFTD001") checksumInput.append(magic);
    checksumInput.append(payload);
    std::string bytes(magic);
    Put(bytes, payload.size(), 8); Put(bytes, Crc32(checksumInput), 4); bytes.append(payload);
    return bytes;
}
TMessageHolder<TLogEntry> Entry(uint64_t term, const std::string& data) {
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + data.size());
    entry->Term = term;
    memcpy(entry->Data, data.data(), data.size());
    return entry;
}
struct TDurableControlRsm : IIdempotentRsm {
    TIdempotencySnapshot State;
    size_t IdempotentApplicationCalls = 0;
    uint64_t LastIdempotentResultIndex = 0;
    uint32_t LastIdempotentResultError = TCommandResponse::Success;
    std::string LastIdempotentResultData;

    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest>, uint64_t index) override {
        return NewHoldedMessage(TCommandResponse{.Index = index});
    }
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry>, uint64_t index) override {
        LastAppliedIndex = index;
        State.LastAppliedIndex = index;
        return NewHoldedMessage(TCommandResponse{.Index = index});
    }
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest>) override {
        return NewHoldedMessage<TLogEntry>();
    }
    void ValidateIdempotent(TMessageHolder<TIdempotentCommandRequest> request) const override {
        if (request->Epoch != State.Epoch)
            throw std::invalid_argument("Unexpected durable test epoch");
        const auto session = std::find_if(State.Sessions.begin(), State.Sessions.end(),
            [&](const auto& record) {
                return record.SessionIdHigh == request->SessionIdHigh &&
                    record.SessionIdLow == request->SessionIdLow;
            });
        if (session != State.Sessions.end() && session->Closed)
            throw std::invalid_argument("Durable test session is closed");
    }
    uint64_t CachedResponseDataUpperBound(
            TMessageHolder<TIdempotentCommandRequest>) const override { return 32; }
    TMessageHolder<TLogEntry> PrepareIdempotent(
            TMessageHolder<TIdempotentCommandRequest> request) override {
        const auto payloadSize = request->Len - sizeof(TIdempotentCommandRequest);
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + payloadSize);
        if (payloadSize) memcpy(entry->Data, request->Data, payloadSize);
        return entry;
    }
    TIdempotentWriteResult WriteIdempotent(TMessageHolder<TLogEntry> command, uint64_t index,
            const TIdempotentOperationIdentity& identity) override {
        const auto session = std::find_if(State.Sessions.begin(), State.Sessions.end(),
            [&](const auto& record) {
                return record.SessionIdHigh == identity.SessionIdHigh &&
                    record.SessionIdLow == identity.SessionIdLow;
            });
        if (session != State.Sessions.end()) {
            if (session->Closed)
                return {.Index = session->LogIndex, .ErrorCode = TCommandResponse::AlreadyCompleted};
            if (identity.Epoch != State.Epoch || identity.Sequence != session->HighestSequence ||
                identity.PayloadDigest != session->PayloadDigest)
                throw std::invalid_argument("Unexpected durable test duplicate");
            State.LastAppliedIndex = index;
            LastAppliedIndex = index;
            LastIdempotentResultIndex = session->LogIndex;
            LastIdempotentResultError = session->ErrorCode;
            LastIdempotentResultData = session->ResponseData;
            return {.Index = LastIdempotentResultIndex, .ErrorCode = LastIdempotentResultError,
                .Data = LastIdempotentResultData};
        }

        ++IdempotentApplicationCalls;
        State.ApplicationData.assign(command->Data, command->Len - sizeof(TLogEntry));
        State.Sessions.push_back({
            .SessionIdHigh = identity.SessionIdHigh,
            .SessionIdLow = identity.SessionIdLow,
            .HighestSequence = identity.Sequence,
            .PayloadDigest = identity.PayloadDigest,
            .LogIndex = index,
            .ErrorCode = TCommandResponse::Success,
            .ResponseData = "durable-result",
        });
        LastAppliedIndex = index;
        State.LastAppliedIndex = index;
        LastIdempotentResultIndex = index;
        LastIdempotentResultError = TCommandResponse::Success;
        LastIdempotentResultData = "durable-result";
        return {.Index = index, .ErrorCode = TCommandResponse::Success,
            .Data = "durable-result"};
    }
    void ValidateIdempotencyControl(const TIdempotencyControlOperation& operation) const override {
        ValidateIdempotencyControlOperation(operation);
        if (operation.Kind == EIdempotencyControlKind::AdvanceEpoch && State.Epoch != operation.Epoch)
            throw std::invalid_argument("Unexpected durable test epoch");
        if (operation.Kind == EIdempotencyControlKind::CloseSession) {
            if (State.Epoch != operation.Epoch)
                throw std::invalid_argument("Unexpected durable test session epoch");
            const auto session = std::find_if(State.Sessions.begin(), State.Sessions.end(),
                [&](const auto& record) {
                    return record.SessionIdHigh == operation.SessionIdHigh &&
                        record.SessionIdLow == operation.SessionIdLow;
                });
            if (session == State.Sessions.end())
                throw std::invalid_argument("Cannot close an unknown durable test session");
        }
    }
    TMessageHolder<TLogEntry> PrepareIdempotencyControl(
            const TIdempotencyControlOperation&) override {
        return NewHoldedMessage<TLogEntry>();
    }
    void ApplyIdempotencyControl(const TIdempotencyControlOperation& operation,
            TMessageHolder<TLogEntry> applicationEntry, uint64_t index) override {
        if (operation.Kind == EIdempotencyControlKind::AdvanceEpoch) {
            State.ApplicationData.assign(applicationEntry->Data,
                applicationEntry->Len - sizeof(TLogEntry));
            State.Epoch = operation.NewEpoch;
        } else if (operation.Kind == EIdempotencyControlKind::CloseSession) {
            const auto session = std::find_if(State.Sessions.begin(), State.Sessions.end(),
                [&](const auto& record) {
                    return record.SessionIdHigh == operation.SessionIdHigh &&
                        record.SessionIdLow == operation.SessionIdLow;
                });
            if (session == State.Sessions.end())
                throw std::logic_error("Committed close-session control lost its session");
            session->Closed = true;
            session->ErrorCode = TCommandResponse::Success;
            session->ResponseData.clear();
        }
        State.LastAppliedIndex = index;
        LastAppliedIndex = index;
    }
    TIdempotencySnapshot CaptureIdempotencySnapshot() const override { return State; }
    void ValidateIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
            uint64_t index) const override {
        ::ValidateIdempotencySnapshot(snapshot, SnapshotLimits);
        if (snapshot.LastAppliedIndex != index)
            throw std::invalid_argument("Durable test snapshot boundary mismatch");
    }
    void RestoreIdempotencySnapshot(TIdempotencySnapshot snapshot, uint64_t index) override {
        State = std::move(snapshot);
        LastAppliedIndex = index;
    }
};
std::string Data(TDurableState& state, int64_t index);

void test_v1_v2_v3_images_recover_conservatively_and_upgrade_on_publish(void**) {
    for (const auto [magic, hasSnapshot, configured] : {
        std::tuple<std::string_view, bool, bool>{"MRFTD001", false, false},
        {"MRFTD002", true, false},
        {"MRFTD003", true, true},
    }) {
        TFiles files;
        const auto legacy = LegacyImage(magic, hasSnapshot, configured);
        { std::ofstream output(files.Image(), std::ios::binary); output.write(legacy.data(), legacy.size()); }
        {
            TDurableState state(files.Name, 1);
            assert_int_equal(state.CurrentTerm, 3);
            assert_int_equal(state.SnapshotIndex, hasSnapshot ? 1 : 0);
            assert_int_equal(state.CommitIndex, state.SnapshotIndex);
            assert_int_equal(state.LastLogIndex, hasSnapshot ? 2 : 1);
            if (!configured) state.BindVotersFromTrustedBaseline({1, 2});
            assert_true((state.GetBoundVoters() == std::vector<uint32_t>{1, 2}));
            assert_true(state.SupportsCommitIndexPersistence());
            if (hasSnapshot) assert_true(state.GetSnapshot()->Data == "legacy snapshot");
            assert_string_equal(Data(state, state.LastLogIndex).c_str(), "tail");
            state.Commit();
        }
        std::ifstream upgraded(files.Image(), std::ios::binary);
        std::string published(8, '\0'); upgraded.read(published.data(), published.size());
        assert_true(published == "MRFTD004");
    }
}
void test_legacy_configuration_snapshot_migration_requires_complete_trusted_history(void**) {
    const std::vector<uint32_t> originalVoters{1, 2, 3};
    const std::vector<uint32_t> newVoters{2, 3, 4};
    auto history = TConfigurationHistory::Stable(originalVoters, 0, 0, 0, 0);
    history = history.Advance(1, EncodeConfigurationEntry({
        EConfigurationPhase::Joint, 2, 0, 0, originalVoters, newVoters}));
    auto snapshot = history.MakeSnapshot("legacy snapshot");

    TFiles files;
    const auto legacy = LegacyImage("MRFTD002", true, false, &snapshot);
    { std::ofstream output(files.Image(), std::ios::binary); output.write(legacy.data(), legacy.size()); }
    {
        TDurableState state(files.Name, 1);
        assert_true(state.HasUnboundRecoveredState());
        assert_true(state.GetSnapshot()->IsJoint());
        std::string reason;
        assert_false(state.CanBindVotersFromTrustedBaseline(originalVoters, &reason));
        assert_true(!reason.empty());
        assert_false(state.CanBindVotersFromTrustedHistory({1, 2, 4}, 2, &reason));
        assert_true(!reason.empty());
        assert_true(state.CanBindVotersFromTrustedHistory(originalVoters, 2, &reason));
        assert_true(reason.empty());
        state.BindVotersFromTrustedHistory(originalVoters, 2);
        assert_false(state.HasUnboundRecoveredState());
        assert_int_equal(state.CommitIndex, 2);
    }
    {
        TDurableState recovered(files.Name, 1);
        assert_true(recovered.GetBoundVoters() == originalVoters);
        const auto scan = ScanCommittedConfigurationHistory(recovered,
            TConfigurationHistory::FromJointSnapshot(*recovered.GetSnapshot()));
        assert_true(scan.Committed.IsJoint());
        assert_true(scan.Committed.NewVoters() == newVoters);
        assert_false(scan.HasUncommittedConfiguration());
    }

    auto stableHistory = TConfigurationHistory::Stable(originalVoters, 0, 0, 0, 0);
    stableHistory = stableHistory.Advance(1, EncodeConfigurationEntry({
        EConfigurationPhase::Joint, 2, 0, 0, originalVoters, newVoters}));
    stableHistory = stableHistory.Advance(2, EncodeConfigurationEntry({
        EConfigurationPhase::Stable, 3, 1, 2, originalVoters, newVoters}));
    auto stableSnapshot = stableHistory.MakeSnapshot("stable legacy snapshot");
    TFiles stableFiles;
    const auto stableLegacy = LegacyImage("MRFTD002", true, false, &stableSnapshot);
    { std::ofstream output(stableFiles.Image(), std::ios::binary); output.write(stableLegacy.data(), stableLegacy.size()); }
    {
        TDurableState state(stableFiles.Name, 1);
        state.BindVotersFromTrustedHistory(originalVoters, 3);
    }
    {
        TDurableState recovered(stableFiles.Name, 1);
        const auto scan = ScanCommittedConfigurationHistory(recovered,
            TConfigurationHistory::FromStableSnapshot(*recovered.GetSnapshot()));
        assert_false(scan.Committed.IsJoint());
        assert_true(scan.Committed.OldVoters() == newVoters);
        assert_int_equal(scan.Committed.ConfigurationIndex(), 2);
        assert_false(scan.HasUncommittedConfiguration());
    }

    auto unanchored = TSnapshot{1, 2, newVoters, "legacy snapshot"};
    unanchored.JointVoters = {3, 4};
    unanchored.ConfigurationIndex = 1;
    unanchored.ConfigurationTerm = 2;
    TFiles untrustedFiles;
    const auto untrusted = LegacyImage("MRFTD002", true, false, &unanchored);
    { std::ofstream output(untrustedFiles.Image(), std::ios::binary); output.write(untrusted.data(), untrusted.size()); }
    {
        TDurableState state(untrustedFiles.Name, 1);
        std::string reason;
        assert_false(state.CanBindVotersFromTrustedHistory(originalVoters, 2, &reason));
        assert_true(!reason.empty());
        bool rejected = false;
        try { state.BindVotersFromTrustedHistory(originalVoters, 2); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
        assert_true(state.HasUnboundRecoveredState());
    }
}
void test_legacy_vote_cannot_be_justified_by_uncommitted_membership(void**) {
    const std::vector<uint32_t> originalVoters{1, 2, 3};
    const std::vector<uint32_t> newVoters{2, 3, 4};
    std::vector<TMessageHolder<TLogEntry>> entries;
    entries.push_back(EncodeConfigurationEntry({
        EConfigurationPhase::Joint, 2, 0, 0, originalVoters, newVoters}));

    TFiles files;
    const auto legacy = LegacyImage("MRFTD001", false, false, nullptr, 4, &entries);
    { std::ofstream output(files.Image(), std::ios::binary); output.write(legacy.data(), legacy.size()); }
    {
        TDurableState state(files.Name, 1);
        assert_true(state.HasUnboundRecoveredState());
        std::string reason;
        assert_false(state.CanBindVotersFromTrustedHistory(originalVoters, 0, &reason));
        assert_true(reason.find("Stored vote is outside") != std::string::npos);

        bool rejected = false;
        try { state.BindVotersFromTrustedHistory(originalVoters, 0); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
        assert_true(state.HasUnboundRecoveredState());
        assert_true(state.GetBoundVoters().empty());
        assert_int_equal(state.CommitIndex, 0);

        std::string persisted(legacy.size(), '\0');
        std::ifstream input(files.Image(), std::ios::binary);
        assert_true(bool(input.read(persisted.data(), std::streamsize(persisted.size()))));
        assert_true(persisted == legacy);
    }
}
void TFiles::Seed() {
    TDurableState state(Name, 1);
    state.CurrentTerm = 3;
    state.VotedFor = 2;
    state.Append(Entry(1, "first"));
    state.Append(Entry(2, "second"));
}
std::string Data(TDurableState& state, int64_t index) {
    auto entry = state.Get(index);
    return std::string(entry->Data, entry->Len - sizeof(TLogEntry));
}
enum class EOperation { Append, Truncate, Vote, Replace, CommitIndex };
void Operate(TDurableState& state, EOperation operation) {
    if (operation == EOperation::Truncate) state.RemoveLast();
    else if (operation == EOperation::CommitIndex) {
        state.CommitIndex = 1;
        state.Commit();
    }
    else if (operation == EOperation::Vote) {
        state.CurrentTerm = 4;
        state.VotedFor = 3;
        state.Commit();
    } else state.Append(Entry(3, "replacement"));
}
void Check(TDurableState& state, EOperation operation, bool updated) {
    assert_int_equal(state.CurrentTerm, updated && operation == EOperation::Vote ? 4 : 3);
    assert_int_equal(state.VotedFor, updated && operation == EOperation::Vote ? 3 : 2);
    const auto oldCount = operation == EOperation::Replace ? 1 : 2;
    const auto count = !updated ? oldCount : operation == EOperation::Truncate ? 1 :
        operation == EOperation::Vote || operation == EOperation::CommitIndex ? 2 : oldCount + 1;
    assert_int_equal(state.LastLogIndex, count);
    assert_int_equal(state.CommitIndex, updated && operation == EOperation::CommitIndex ? 1 : 0);
    assert_string_equal(Data(state, 1).c_str(), "first");
    if (count >= 2) {
        const bool replaced = updated && operation == EOperation::Replace;
        assert_string_equal(Data(state, 2).c_str(), replaced ? "replacement" : "second");
        assert_int_equal(state.Get(2)->Term, replaced ? 3 : 2);
    }
    if (count == 3) assert_string_equal(Data(state, 3).c_str(), "replacement");
    assert_int_equal(state.LastLogTerm, state.Get(count)->Term);
}
void Wait(pid_t child, int expected) {
    int status;
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), expected);
}

void test_crash_at_every_publication_boundary(void**) {
    for (auto operation : {EOperation::Append, EOperation::Truncate, EOperation::Vote, EOperation::Replace, EOperation::CommitIndex}) {
        for (auto stage : {TStage::TempCreated, TStage::PayloadStarted, TStage::Written, TStage::FileSynced,
                           TStage::Renamed, TStage::DirectorySynced}) {
            TFiles files;
            files.Seed();
            const auto child = fork();
            assert_true(child >= 0);
            if (child == 0) {
                try {
                    bool armed = false;
                    TDurableState state(files.Name, 1, [&](auto point) {
                        if (armed && point == stage) _exit(90);
                    });
                    if (operation == EOperation::Replace) state.RemoveLast();
                    armed = true;
                    Operate(state, operation);
                } catch (...) { _exit(2); }
                _exit(3);
            }
            Wait(child, 90);
            TDurableState recovered(files.Name, 1);
            Check(recovered, operation, stage >= TStage::Renamed);
        }
    }
}

void test_acknowledged_updates_survive_immediate_exit(void**) {
    for (auto operation : {EOperation::Append, EOperation::Truncate, EOperation::Vote, EOperation::Replace, EOperation::CommitIndex}) {
        TFiles files;
        files.Seed();
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDurableState state(files.Name, 1);
                if (operation == EOperation::Replace) state.RemoveLast();
                Operate(state, operation);
                _exit(0); // No destructors or stream flushing after acknowledgement.
            } catch (...) { _exit(2); }
        }
        Wait(child, 0);
        TDurableState recovered(files.Name, 1);
        Check(recovered, operation, true);
    }
}

struct TEndpoint : INode {
    std::vector<TMessageHolder<TMessage>> Sent;
    std::vector<std::string>* Events = nullptr;
    void Send(TMessageHolder<TMessage> message) override {
        Sent.push_back(std::move(message));
        if (Events) Events->push_back("ack");
    }
    void Drain() override {}
};

void test_follower_persists_commit_before_ack_and_recovers_it(void**) {
    TFiles files;
    std::vector<std::string> events;
    auto peer = std::make_shared<TEndpoint>();
    peer->Events = &events;
    {
        auto state = std::make_shared<TDurableState>(files.Name, 1, [&](auto stage) {
            if (stage == TStage::DirectorySynced) events.push_back("durable");
        });
        TRaft raft(state, 1, {{2, peer}});
        events.clear(); // Ignore initial image and voter-binding publications.
        auto request = NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = 1},
            TAppendEntriesRequest{.PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 1,
                                  .LeaderId = 2, .Nentries = 1});
        request.InitPayload(1);
        request.Payload[0] = Entry(1, "committed");
        raft.Process(ITimeSource::Time{}, request);
        assert_int_equal(state->CommitIndex, 1);
        assert_int_equal(peer->Sent.size(), 1);
        assert_true(peer->Sent.back().Cast<TAppendEntriesResponse>()->Success);
        assert_true((events == std::vector<std::string>{"durable", "ack"}));
    }
    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.CommitIndex, 1);
    assert_int_equal(recovered.LastLogIndex, 1);
    bool protectedEntry = false;
    try { recovered.RemoveLast(); }
    catch (const std::logic_error&) { protectedEntry = true; }
    assert_true(protectedEntry);
    assert_int_equal(recovered.LastLogIndex, 1);
}

void test_follower_batches_durable_append_before_ack(void**) {
    TFiles files;
    std::vector<std::string> events;
    auto peer = std::make_shared<TEndpoint>();
    peer->Events = &events;
    {
        auto state = std::make_shared<TDurableState>(files.Name, 1, [&](auto stage) {
            if (stage == TDurableState::EStage::DirectorySynced) events.push_back("durable");
        });
        TRaft raft(state, 1, {{2, peer}});
        events.clear();

        auto request = NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = 1},
            TAppendEntriesRequest{.PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 2,
                                  .LeaderId = 2, .Nentries = 3});
        request.InitPayload(3);
        request.Payload[0] = Entry(1, "first");
        request.Payload[1] = Entry(1, "second");
        request.Payload[2] = Entry(1, "third");
        raft.Process(ITimeSource::Time{}, request);

        assert_int_equal(state->LastLogIndex, 3);
        assert_int_equal(state->CommitIndex, 2);
        assert_int_equal(peer->Sent.size(), 1);
        const auto response = peer->Sent.back().Cast<TAppendEntriesResponse>();
        assert_true(response->Success);
        assert_int_equal(response->MatchIndex, 3);
        assert_true((events == std::vector<std::string>{"durable", "ack"}));
    }

    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 3);
    assert_int_equal(recovered.CommitIndex, 2);
    assert_string_equal(Data(recovered, 1).c_str(), "first");
    assert_string_equal(Data(recovered, 2).c_str(), "second");
    assert_string_equal(Data(recovered, 3).c_str(), "third");
}

void test_durable_append_rejects_invalid_terms_before_mutation(void**) {
    TFiles files;
    files.Seed();
    auto readImage = [&] {
        std::ifstream input(files.Image(), std::ios::binary);
        return std::string{std::istreambuf_iterator<char>(input), {}};
    };
    const auto originalImage = readImage();
    {
        TDurableState state(files.Name, 1);
        auto rejectsInvalid = [&](auto operation) {
            bool rejected = false;
            try { operation(); }
            catch (const std::invalid_argument&) { rejected = true; }
            assert_true(rejected);
            assert_int_equal(state.LastLogIndex, 2);
            assert_int_equal(state.LastLogTerm, 2);
            assert_int_equal(state.CommitIndex, 0);
            assert_string_equal(Data(state, 1).c_str(), "first");
            assert_string_equal(Data(state, 2).c_str(), "second");
        };

        rejectsInvalid([&] { state.Append(Entry(0, "zero term")); });
        rejectsInvalid([&] { state.Append(Entry(1, "decreasing term")); });
        rejectsInvalid([&] { state.Append(Entry(4, "future term")); });

        std::vector<TMessageHolder<TLogEntry>> invalidBatch{
            Entry(3, "valid first"), Entry(2, "decreasing second")};
        rejectsInvalid([&] { state.AppendBatch(invalidBatch); });

        std::vector<TMessageHolder<TLogEntry>> invalidReplacement{
            Entry(2, "valid replacement"), Entry(1, "decreasing replacement")};
        rejectsInvalid([&] { state.ReplaceSuffixAndAppendBatch(2, invalidReplacement, 0); });
    }
    assert_true(readImage() == originalImage);
    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 2);
    assert_int_equal(recovered.LastLogTerm, 2);
    assert_int_equal(recovered.CommitIndex, 0);
    assert_string_equal(Data(recovered, 1).c_str(), "first");
    assert_string_equal(Data(recovered, 2).c_str(), "second");
}

void test_in_memory_state_rejects_invalid_terms_before_mutation(void**) {
    std::vector<TMessageHolder<TLogEntry>> original{Entry(1, "first"), Entry(2, "second")};
    TState state(3, 0, original);
    auto rejectsInvalid = [&](auto operation) {
        bool rejected = false;
        try { operation(); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.LastLogTerm, 2);
        assert_int_equal(state.CommitIndex, 0);
        auto first = state.Get(1);
        auto second = state.Get(2);
        assert_string_equal(std::string(first->Data, first->Len - sizeof(TLogEntry)).c_str(), "first");
        assert_string_equal(std::string(second->Data, second->Len - sizeof(TLogEntry)).c_str(), "second");
    };

    rejectsInvalid([&] { state.Append(Entry(0, "zero term")); });
    rejectsInvalid([&] { state.Append(Entry(1, "decreasing term")); });
    rejectsInvalid([&] { state.Append(Entry(4, "future term")); });

    std::vector<TMessageHolder<TLogEntry>> invalidBatch{
        Entry(3, "valid first"), Entry(2, "decreasing second")};
    rejectsInvalid([&] { state.AppendBatch(invalidBatch); });
    rejectsInvalid([&] { state.AppendBatchAndCommit(invalidBatch, 3); });

    std::vector<TMessageHolder<TLogEntry>> invalidReplacement{
        Entry(2, "valid replacement"), Entry(1, "decreasing replacement")};
    rejectsInvalid([&] { state.ReplaceSuffixAndAppendBatch(2, invalidReplacement, 0); });

    bool rejected = false;
    try { TState malformed(3, 0, {Entry(1, "first"), Entry(0, "zero term")}); }
    catch (const std::invalid_argument&) { rejected = true; }
    assert_true(rejected);
}

void test_follower_replaces_durable_suffix_in_one_publish(void**) {
    TFiles files;
    std::vector<std::string> events;
    auto peer = std::make_shared<TEndpoint>();
    peer->Events = &events;
    {
        auto state = std::make_shared<TDurableState>(files.Name, 1, [&](auto stage) {
            if (stage == TDurableState::EStage::DirectorySynced) events.push_back("durable");
        });
        state->CurrentTerm = 2;
        state->Commit();
        state->Append(Entry(1, "first"));
        state->Append(Entry(1, "old-second"));
        state->Append(Entry(2, "old-third"));
        TRaft raft(state, 1, {{2, peer}});
        events.clear();

        auto request = NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
            TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 1, .LeaderCommit = 2,
                                  .LeaderId = 2, .Nentries = 2});
        request.InitPayload(2);
        request.Payload[0] = Entry(2, "new-second");
        request.Payload[1] = Entry(2, "new-third");
        raft.Process(ITimeSource::Time{}, request);

        assert_int_equal(state->LastLogIndex, 3);
        assert_int_equal(state->CommitIndex, 2);
        assert_true((events == std::vector<std::string>{"durable", "ack"}));
    }

    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 3);
    assert_int_equal(recovered.CommitIndex, 2);
    assert_string_equal(Data(recovered, 1).c_str(), "first");
    assert_string_equal(Data(recovered, 2).c_str(), "new-second");
    assert_string_equal(Data(recovered, 3).c_str(), "new-third");
}

void test_publication_order(void**) {
    TFiles files;
    files.Seed();
    std::vector<TStage> stages;
    TDurableState state(files.Name, 1, [&](auto stage) { stages.push_back(stage); });
    state.CurrentTerm = 4;
    state.Commit();
    const std::vector<TStage> expected{TStage::TempCreated, TStage::PayloadStarted, TStage::Written,
                                      TStage::FileSynced, TStage::Renamed, TStage::DirectorySynced};
    assert_true(stages == expected);
}

void test_live_publication_failure_is_fatal(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDurableState state(files.Name, 1, [](auto stage) {
                if (stage == TStage::FileSynced) throw std::runtime_error("injected failure");
            });
            state.CurrentTerm = 4;
            state.Commit();
        } catch (...) { _exit(74); }
        _exit(75);
    }
    Wait(child, 73);
    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.CurrentTerm, 3);
    assert_int_equal(recovered.VotedFor, 2);
}

void test_mid_payload_failure_is_fatal_and_preserves_image(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDurableState state(files.Name, 1, [](auto stage) {
                if (stage == TStage::PayloadStarted) throw std::runtime_error("injected failure");
            });
            state.CurrentTerm = 4;
            state.Commit();
        } catch (...) { _exit(74); }
        _exit(75);
    }
    Wait(child, 73);
    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.CurrentTerm, 3);
    assert_int_equal(recovered.VotedFor, 2);
}

void test_write_error_is_fatal_and_preserves_image(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDurableState state(files.Name, 1);
            struct rlimit limit;
            if (getrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(76);
            limit.rlim_cur = 0;
            if (setrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(76);
            std::signal(SIGXFSZ, SIG_IGN);
            state.Append(Entry(3, "must not be acknowledged"));
        } catch (...) { _exit(74); }
        _exit(75);
    }
    Wait(child, 73);
    TDurableState recovered(files.Name, 1);
    Check(recovered, EOperation::Append, false);
}

void test_interrupted_initialization_is_not_reset(void**) {
    for (auto stage : {TStage::TempCreated, TStage::PayloadStarted, TStage::Written, TStage::FileSynced,
                       TStage::Renamed, TStage::DirectorySynced}) {
        TFiles files;
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDurableState state(files.Name, 1, [&](auto point) {
                    if (point == stage) _exit(90);
                });
            } catch (...) { _exit(2); }
            _exit(3);
        }
        Wait(child, 90);
        bool rejected = false;
        try {
            TDurableState recovered(files.Name, 1);
            assert_int_equal(recovered.CurrentTerm, 1);
            assert_int_equal(recovered.VotedFor, 0);
            assert_int_equal(recovered.LastLogIndex, 0);
        } catch (const std::runtime_error&) { rejected = true; }
        assert_int_equal(rejected, stage < TStage::Renamed);
    }
}

void Reject(const TFiles& files) {
    bool rejected = false;
    try { TDurableState state(files.Name, 1); }
    catch (const std::runtime_error&) { rejected = true; }
    assert_true(rejected);
}
void test_corrupt_images_are_rejected(void**) {
    for (const auto offset : {0, 8, 16, 20, 30, 45}) {
        TFiles files;
        files.Seed();
        std::fstream file(files.Image(), std::ios::in | std::ios::out | std::ios::binary);
        file.seekg(offset);
        char byte;
        file.read(&byte, 1);
        byte ^= 1;
        file.seekp(offset);
        file.write(&byte, 1);
        file.close();
        Reject(files);
    }
    for (const auto size : {0, 7, 19, 21, 40}) {
        TFiles files;
        files.Seed();
        fs::resize_file(files.Image(), size);
        Reject(files);
    }
}
void test_single_writer_lock(void**) {
    TFiles files;
    files.Seed();
    {
        TDurableState writer(files.Name, 1);
        Reject(files);
    }
    TDurableState afterClose(files.Name, 1);
    assert_int_equal(afterClose.CurrentTerm, 3);
}
void test_missing_image_is_not_recreated(void**) {
    TFiles files;
    files.Seed();
    fs::remove(files.Image());
    Reject(files);
    assert_false(fs::exists(files.Image()));
}
void test_legacy_store_is_not_silently_migrated(void**) {
    TFiles files;
    { TDiskState legacy(files.Name, 1); }
    Reject(files);
    assert_false(fs::exists(files.Image()));
}
void test_split_legacy_disk_state_migration_requires_trusted_baselines(void**) {
    TFiles files;
    const auto legacy = (files.Directory / "legacy").string();
    const auto durable = (files.Directory / "durable").string();
    const std::vector<uint32_t> voters{1, 2, 3};
    {
        TDiskState source(legacy, 2);
        source.CurrentTerm = 4;
        source.VotedFor = 3;
        source.Commit();
        source.Append(Entry(2, "committed"));
        source.Append(Entry(4, "tail"));
        source.CommitIndex = 1;
        source.Commit();
    }
    const auto bytesBefore = [&](const std::string& suffix) {
        std::ifstream input(legacy + suffix + ".2", std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    };
    const auto entriesBefore = bytesBefore(".entries");
    const auto indexBefore = bytesBefore(".index");
    const auto stateBefore = bytesBefore(".state");

    const auto checked = CheckLegacyDiskStateMigration(legacy, 2, voters, 1);
    assert_int_equal(checked.CurrentTerm, 4);
    assert_int_equal(checked.VotedFor, 3);
    assert_int_equal(checked.LastLogIndex, 2);
    assert_int_equal(checked.LastLogTerm, 4);
    assert_int_equal(checked.CommitIndex, 1);
    assert_true(bytesBefore(".entries") == entriesBefore);
    assert_true(bytesBefore(".index") == indexBefore);
    assert_true(bytesBefore(".state") == stateBefore);

    const auto migrated = MigrateLegacyDiskState(legacy, durable, 2, voters, 1);
    assert_int_equal(migrated.LastLogIndex, 2);
    {
        TDurableState state(durable, 2);
        assert_int_equal(state.CurrentTerm, 4);
        assert_int_equal(state.VotedFor, 3);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.LastLogTerm, 4);
        assert_int_equal(state.CommitIndex, 1);
        assert_true(state.GetBoundVoters() == voters);
        assert_string_equal(Data(state, 1).c_str(), "committed");
        assert_string_equal(Data(state, 2).c_str(), "tail");
    }
    assert_true(bytesBefore(".entries") == entriesBefore);
    assert_true(bytesBefore(".index") == indexBefore);
    assert_true(bytesBefore(".state") == stateBefore);

    const auto rejectedOutput = (files.Directory / "rejected").string();
    bool invalidCommitRejected = false;
    try { MigrateLegacyDiskState(legacy, rejectedOutput, 2, voters, 3); }
    catch (const std::exception&) { invalidCommitRejected = true; }
    assert_true(invalidCommitRejected);
    assert_false(std::filesystem::exists(rejectedOutput + ".durable.2"));

    const auto configuredLegacy = (files.Directory / "configured-legacy").string();
    const std::vector<uint32_t> newVoters{2, 3, 4};
    {
        TDiskState source(configuredLegacy, 2);
        source.CurrentTerm = 5;
        source.VotedFor = 4; // A valid vote for a voter introduced by the history.
        source.Commit();
        source.Append(Entry(2, "before joint"));
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 3, 0, 0, voters, newVoters}));
        source.Append(Entry(3, "in joint"));
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 4, 2, 3, voters, newVoters}));
        source.CommitIndex = 2;
        source.Commit();
    }
    const auto configDurable = (files.Directory / "configured-durable").string();
    const auto configInfo = MigrateLegacyDiskState(configuredLegacy, configDurable, 2, voters, 2);
    assert_int_equal(configInfo.CommitIndex, 2);
    {
        TDurableState state(configDurable, 2);
        assert_int_equal(state.VotedFor, 4);
        assert_true(state.GetBoundVoters() == voters);
        const auto history = ScanCommittedConfigurationHistory(
            state, TConfigurationHistory::Stable(voters, 0, 0, 0, 0));
        assert_true(history.Committed.IsJoint());
        assert_true(history.Committed.OldVoters() == voters);
        assert_true(history.Committed.NewVoters() == newVoters);
        assert_false(history.Latest.IsJoint());
        assert_true(history.Latest.OldVoters() == newVoters);
    }

    // An uncommitted configuration suffix must not be used to justify a
    // persisted vote for a voter outside the committed membership history.
    const auto uncommittedVoteLegacy = (files.Directory / "uncommitted-vote-legacy").string();
    {
        TDiskState source(uncommittedVoteLegacy, 2);
        source.CurrentTerm = 3;
        source.VotedFor = 4;
        source.Commit();
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 2, 0, 0, voters, newVoters}));
        source.Commit(); // The joint entry remains beyond the zero commit boundary.
    }
    bool uncommittedVoteRejected = false;
    try { CheckLegacyDiskStateMigration(uncommittedVoteLegacy, 2, voters, 0); }
    catch (const std::exception&) { uncommittedVoteRejected = true; }
    assert_true(uncommittedVoteRejected);
    const auto uncommittedVoteDurable = (files.Directory / "uncommitted-vote-durable").string();
    bool uncommittedVoteMigrationRejected = false;
    try { MigrateLegacyDiskState(uncommittedVoteLegacy, uncommittedVoteDurable, 2, voters, 0); }
    catch (const std::exception&) { uncommittedVoteMigrationRejected = true; }
    assert_true(uncommittedVoteMigrationRejected);
    assert_false(fs::exists(uncommittedVoteDurable + ".durable.2"));

    const auto invalidConfig = (files.Directory / "invalid-config").string();
    {
        TDiskState source(invalidConfig, 2);
        source.CurrentTerm = 2;
        source.Commit();
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 1, 0, 0, {7, 8}, {8, 9}}));
        source.CommitIndex = 1;
        source.Commit();
    }
    bool configHistoryRejected = false;
    try { CheckLegacyDiskStateMigration(invalidConfig, 2, voters, 1); }
    catch (const std::exception&) { configHistoryRejected = true; }
    assert_true(configHistoryRejected);

    const auto missingLegacy = (files.Directory / "missing-legacy").string();
    bool missingRejected = false;
    try { CheckLegacyDiskStateMigration(missingLegacy, 2, voters, 0); }
    catch (const std::exception&) { missingRejected = true; }
    assert_true(missingRejected);
    assert_false(fs::exists(missingLegacy + ".entries.2"));
    assert_false(fs::exists(missingLegacy + ".index.2"));
    assert_false(fs::exists(missingLegacy + ".state.2"));
}
void test_compacted_legacy_disk_state_migration_preserves_snapshot_and_suffix(void**) {
    TFiles files;
    const auto legacy = (files.Directory / "compacted-legacy").string();
    const auto durable = (files.Directory / "compacted-durable").string();
    const std::vector<uint32_t> original{1, 2, 3};
    const std::vector<uint32_t> intermediate{2, 3, 4};
    const std::vector<uint32_t> finalVoters{3, 4, 5};
    const TSnapshotConfigurationTransition anchor{original, intermediate, 0, 0, 6, 3, 7, 3};

    TSnapshot snapshot{7, 3, intermediate, "compacted application"};
    snapshot.StableConfiguration = true;
    snapshot.ConfigurationIndex = 7;
    snapshot.ConfigurationTerm = 3;
    snapshot.PreviousVoters = original;
    snapshot.PreviousConfigurationIndex = 0;
    snapshot.PreviousConfigurationTerm = 0;
    snapshot.PreviousJointIndex = 6;
    snapshot.PreviousJointTerm = 3;
    snapshot.ConfigurationTransitions = {anchor};
    {
        TDiskState source(legacy, 2);
        source.BindVoters(original);
        source.CurrentTerm = 5;
        source.VotedFor = 5;
        source.Commit();
        for (uint64_t term : {1, 2, 2, 3, 3}) source.Append(Entry(term, "prefix"));
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 3, 0, 0, original, intermediate}));
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 3, 6, 3, original, intermediate}));
        source.CommitIndex = 7;
        source.Commit();
        source.InstallSnapshot(snapshot);
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 4, 7, 3, intermediate, finalVoters}));
        source.Append(EncodeConfigurationEntry({
            EConfigurationPhase::Stable, 4, 8, 4, intermediate, finalVoters}));
        source.Append(Entry(4, "retained suffix"));
        source.CommitIndex = 9;
        source.Commit();
    }

    const auto info = CheckLegacyDiskStateMigration(legacy, 2, original, 9);
    assert_int_equal(info.SnapshotIndex, 7);
    assert_int_equal(info.SnapshotTerm, 3);
    assert_int_equal(info.LastLogIndex, 10);
    assert_int_equal(info.CommitIndex, 9);
    const auto migrated = MigrateLegacyDiskState(legacy, durable, 2, original, 9);
    assert_int_equal(migrated.SnapshotIndex, snapshot.Index);

    TDurableState recovered(durable, 2);
    assert_true(recovered.GetBoundVoters() == original);
    assert_true(recovered.GetSnapshot() && *recovered.GetSnapshot() == snapshot);
    assert_int_equal(recovered.SnapshotIndex, snapshot.Index);
    assert_int_equal(recovered.SnapshotTerm, snapshot.Term);
    assert_int_equal(recovered.LastLogIndex, 10);
    assert_int_equal(recovered.LastLogTerm, 4);
    assert_int_equal(recovered.CommitIndex, 9);
    assert_int_equal(recovered.VotedFor, 5);
    assert_string_equal(Data(recovered, 10).c_str(), "retained suffix");
    const auto membership = ScanConfigurationHistory(recovered,
        TConfigurationHistory::FromStableSnapshot(snapshot), recovered.CommitIndex);
    assert_true(membership.Committed.OldVoters() == finalVoters);
    assert_false(membership.Committed.IsJoint());

    const auto occupiedBase = (files.Directory / "occupied-destination").string();
    {
        TDurableState occupied(occupiedBase, 2);
        occupied.BindVoters(original);
        occupied.Append(Entry(1, "existing destination"));
    }
    const auto occupiedImagePath = occupiedBase + ".durable.2";
    auto readImage = [](const std::string& path) {
        std::ifstream input(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    };
    const auto occupiedBytes = readImage(occupiedImagePath);
    bool occupiedDestinationRejected = false;
    try { MigrateLegacyDiskState(legacy, occupiedBase, 2, original, 9); }
    catch (const std::exception&) { occupiedDestinationRejected = true; }
    assert_true(occupiedDestinationRejected);
    assert_true(readImage(occupiedImagePath) == occupiedBytes);

    const auto collidingBase = (files.Directory / "colliding-destination").string();
    const auto collidingImagePath = collidingBase + ".durable.2";
    {
        std::ofstream collision(collidingImagePath, std::ios::binary);
        collision << "preserve this pre-existing image";
        assert_true(bool(collision));
    }
    const auto collidingBytes = readImage(collidingImagePath);
    bool collidingDestinationRejected = false;
    try { MigrateLegacyDiskState(legacy, collidingBase, 2, original, 9); }
    catch (const std::exception&) { collidingDestinationRejected = true; }
    assert_true(collidingDestinationRejected);
    assert_true(readImage(collidingImagePath) == collidingBytes);
    assert_false(fs::exists(collidingImagePath + ".lock"));

    const auto unanchoredLegacy = (files.Directory / "unanchored-legacy").string();
    const auto rejectedDurable = (files.Directory / "unanchored-durable").string();
    const std::vector<uint32_t> unknownOriginal{7, 8, 9};
    TSnapshot unanchored{3, 2, intermediate, "unanchored snapshot"};
    unanchored.StableConfiguration = true;
    unanchored.ConfigurationIndex = 3;
    unanchored.ConfigurationTerm = 2;
    unanchored.PreviousVoters = unknownOriginal;
    unanchored.PreviousJointIndex = 2;
    unanchored.PreviousJointTerm = 2;
    unanchored.ConfigurationTransitions = {
        {unknownOriginal, intermediate, 0, 0, 2, 2, 3, 2}};
    {
        TDiskState source(unanchoredLegacy, 2);
        source.BindVoters(original);
        source.CurrentTerm = 3;
        source.Commit();
        source.Append(Entry(1, "prefix"));
        source.Append(Entry(2, "prefix"));
        source.Append(Entry(2, "prefix"));
        source.CommitIndex = 3;
        source.Commit();
        source.InstallSnapshot(unanchored);
    }
    bool unanchoredRejected = false;
    try { CheckLegacyDiskStateMigration(unanchoredLegacy, 2, original, 3); }
    catch (const std::exception&) { unanchoredRejected = true; }
    assert_true(unanchoredRejected);
    bool unanchoredMigrationRejected = false;
    try { MigrateLegacyDiskState(unanchoredLegacy, rejectedDurable, 2, original, 3); }
    catch (const std::exception&) { unanchoredMigrationRejected = true; }
    assert_true(unanchoredMigrationRejected);
    assert_false(fs::exists(rejectedDurable + ".durable.2"));
    assert_false(fs::exists(rejectedDurable + ".durable.2.lock"));
}
void test_get_and_append_do_not_share_mutable_entries(void**) {
    TFiles files;
    {
        TDurableState state(files.Name, 1);
        auto entry = Entry(1, "data");
        state.Append(entry);
        entry->Term = 100;
        auto result = state.Get(1);
        assert_int_equal(result->Term, 1);
        result->Term = 99;
        assert_int_equal(state.Get(1)->Term, 1);
        state.Commit();
    }
    TDurableState state(files.Name, 1);
    assert_int_equal(state.Get(1)->Term, 1);
}
void test_raft_election_and_application_replay_after_restart(void**) {
    TFiles files;
    uint64_t term = 0, index = 0;
    for (int restart = 0; restart < 2; ++restart) {
        auto state = std::make_shared<TDurableState>(files.Name, 1);
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        raft->ProcessTimeout(ITimeSource::Time{});
        assert_int_equal(raft->CurrentStateName(), EState::LEADER);
        assert_true(state->CurrentTerm > term);
        term = state->CurrentTerm;
        if (restart == 0) {
            index = raft->Append(Entry(term, "application-command"));
            raft->ProcessTimeout(ITimeSource::Time{});
        }
        assert_true(raft->GetVolatileState()->CommitIndex >= index);
        assert_string_equal(Data(*state, index).c_str(), "application-command");
        auto rsm = std::make_shared<TDummyRsm>();
        TRequestProcessor processor(raft, rsm, {});
        processor.ProcessCommitted();
        assert_int_equal(rsm->LastAppliedIndex, state->LastLogIndex);
    }
}
void test_idempotency_control_survives_durable_restart_and_replays_atomically(void**) {
    TFiles files;
    const TIdempotencyControlOperation operation{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 2,
        .NewEpoch = 3,
    };
    auto control = EncodeIdempotencyControlLogEntry(operation, Entry(1, "durable-control"));
    {
        TDurableState state(files.Name, 1);
        state.CurrentTerm = 1;
        state.BindVoters({1});
        state.AppendBatchAndCommit({control}, 1);
    }

    auto state = std::make_shared<TDurableState>(files.Name, 1);
    assert_int_equal(state->LastLogIndex, 1);
    assert_int_equal(state->CommitIndex, 1);
    assert_int_equal(state->Get(1)->Flags, TLogEntry::EIdempotencyControl);
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TDurableControlRsm>();
    rsm->State.Epoch = 2;
    TRequestProcessor processor(raft, rsm, {});
    processor.ProcessCommitted();

    assert_int_equal(rsm->LastAppliedIndex, 1);
    assert_int_equal(rsm->State.LastAppliedIndex, 1);
    assert_int_equal(rsm->State.Epoch, 3);
    assert_true(rsm->State.ApplicationData == "durable-control");
}
void test_duplicate_idempotent_records_apply_once_after_durable_restart(void**) {
    TFiles files;
    TIdempotentOperationIdentity identity;
    identity.Epoch = 1;
    identity.SessionIdHigh = 11;
    identity.SessionIdLow = 12;
    identity.Sequence = 1;
    identity.PayloadDigest = ComputeIdempotentPayloadDigest("client-payload");
    auto record = EncodeIdempotentLogEntry(identity, Entry(1, "durable-command"));
    {
        TDurableState state(files.Name, 1);
        state.CurrentTerm = 1;
        state.BindVoters({1});
        state.AppendBatchAndCommit({record, record}, 2);
    }

    auto state = std::make_shared<TDurableState>(files.Name, 1);
    assert_int_equal(state->LastLogIndex, 2);
    assert_int_equal(state->CommitIndex, 2);
    assert_int_equal(state->Get(1)->Flags, TLogEntry::EIdempotent);
    assert_int_equal(state->Get(2)->Flags, TLogEntry::EIdempotent);
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TDurableControlRsm>();
    rsm->State.Epoch = identity.Epoch;
    TRequestProcessor processor(raft, rsm, {});
    processor.ProcessCommitted();

    assert_int_equal(rsm->LastAppliedIndex, 2);
    assert_int_equal(rsm->State.LastAppliedIndex, 2);
    assert_int_equal(rsm->IdempotentApplicationCalls, 1);
    assert_true(rsm->State.ApplicationData == "durable-command");
    assert_int_equal(rsm->State.Sessions.size(), 1);
    assert_int_equal(rsm->State.Sessions.front().LogIndex, 1);
    assert_true(rsm->State.Sessions.front().ResponseData == "durable-result");
    assert_int_equal(rsm->LastIdempotentResultIndex, 1);
    assert_int_equal(rsm->LastIdempotentResultError, TCommandResponse::Success);
    assert_true(rsm->LastIdempotentResultData == "durable-result");
}

void test_idempotent_retry_after_durable_snapshot_restart_returns_cached_result(void**) {
    TFiles files;
    const auto makeRequest = [](uint32_t cookie) {
        constexpr std::string_view payload = "client-payload";
        auto request = NewHoldedMessage<TIdempotentCommandRequest>(
            sizeof(TIdempotentCommandRequest) + payload.size());
        request->Epoch = 1;
        request->SessionIdHigh = 11;
        request->SessionIdLow = 12;
        request->Sequence = 1;
        request->Cookie = cookie;
        memcpy(request->Data, payload.data(), payload.size());
        return request;
    };

    {
        auto state = std::make_shared<TDurableState>(files.Name, 1);
        state->CurrentTerm = 1;
        state->BindVoters({1});
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        auto rsm = std::make_shared<TDurableControlRsm>();
        rsm->State.Epoch = 1;
        TRequestProcessor processor(raft, rsm, {});
        raft->Become(EState::LEADER);

        auto client = std::make_shared<TEndpoint>();
        processor.OnIdempotentCommandRequest(makeRequest(61), client);
        assert_int_equal(state->LastLogIndex, 1);
        raft->ProcessTimeout(ITimeSource::Time{});
        processor.ProcessCommitted();
        assert_int_equal(state->CommitIndex, 1);
        assert_int_equal(rsm->IdempotentApplicationCalls, 1);
        assert_true(rsm->State.ApplicationData == "client-payload");
        assert_int_equal(client->Sent.size(), 1);
        auto response = client->Sent.back().Cast<TIdempotentCommandResponse>();
        ValidateIdempotentCommandResponse(response);
        assert_int_equal(response->ErrorCode, TCommandResponse::Success);
        assert_int_equal(response->Index, 1);
        assert_true(std::string(response->Data,
            response->Len - sizeof(TIdempotentCommandResponse)) == "durable-result");

        const auto snapshot = raft->CompactShared();
        assert_int_equal(snapshot->Index, 1);
        assert_int_equal(state->SnapshotIndex, 1);
        assert_true(state->GetSnapshot()->Data == snapshot->Data);
    }

    auto state = std::make_shared<TDurableState>(files.Name, 1);
    assert_int_equal(state->SnapshotIndex, 1);
    assert_int_equal(state->CommitIndex, 1);
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TDurableControlRsm>();
    TRequestProcessor processor(raft, rsm, {}); // Restores app data and dedup state from Raft's durable snapshot.
    assert_int_equal(rsm->LastAppliedIndex, 1);
    assert_true(rsm->State.ApplicationData == "client-payload");
    assert_int_equal(rsm->State.Sessions.size(), 1);
    assert_int_equal(rsm->State.Sessions.front().HighestSequence, 1);
    assert_int_equal(rsm->State.Sessions.front().LogIndex, 1);
    assert_true(rsm->State.Sessions.front().PayloadDigest ==
        ComputeIdempotentPayloadDigest("client-payload"));
    assert_true(rsm->State.Sessions.front().ResponseData == "durable-result");

    // Elect a fresh leader process from the recovered durable image. Its retry
    // is logged and committed, but replay returns the cached original response
    // without a second application mutation.
    const auto restartTime = ITimeSource::Time{} + std::chrono::seconds(30);
    raft->ProcessTimeout(restartTime);
    assert_int_equal(raft->CurrentStateName(), EState::LEADER);
    auto retryClient = std::make_shared<TEndpoint>();
    processor.OnIdempotentCommandRequest(makeRequest(62), retryClient);
    assert_int_equal(state->LastLogIndex, 3); // New-term leader stub plus the retry.
    raft->ProcessTimeout(restartTime + std::chrono::milliseconds(500));
    processor.ProcessCommitted();

    assert_int_equal(state->CommitIndex, 3);
    assert_int_equal(rsm->LastAppliedIndex, 3);
    assert_int_equal(rsm->IdempotentApplicationCalls, 0);
    assert_true(rsm->State.ApplicationData == "client-payload");
    assert_int_equal(rsm->State.Sessions.size(), 1);
    assert_int_equal(rsm->State.Sessions.front().HighestSequence, 1);
    assert_int_equal(rsm->State.Sessions.front().LogIndex, 1);
    assert_int_equal(retryClient->Sent.size(), 1);
    auto response = retryClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    assert_int_equal(response->Cookie, 62);
    assert_int_equal(response->ErrorCode, TCommandResponse::Success);
    assert_int_equal(response->Index, 1);
    assert_true(std::string(response->Data,
        response->Len - sizeof(TIdempotentCommandResponse)) == "durable-result");
}

struct TDurableIdempotencyPeer : INode {
    std::vector<TMessageHolder<TMessage>> Sent;
    void Send(TMessageHolder<TMessage> message) override { Sent.push_back(std::move(message)); }
    void Drain() override { }
};

struct TDurableQueuedPeer : INode {
    std::function<void(TMessageHolder<TMessage>)> Deliver;
    explicit TDurableQueuedPeer(std::function<void(TMessageHolder<TMessage>)> deliver)
        : Deliver(std::move(deliver)) { }
    void Send(TMessageHolder<TMessage> message) override { Deliver(std::move(message)); }
    void Drain() override { }
};

struct TDurableManualTimeSource : ITimeSource {
    Time Value{};
    Time Now() override { return Value; }
};

struct TDurableIdempotencyCluster {
    TFiles& Files;
    std::vector<std::shared_ptr<TDurableState>> States;
    std::vector<std::shared_ptr<TRaft>> Rafts;
    std::vector<std::shared_ptr<TDurableControlRsm>> Applications;
    std::vector<std::shared_ptr<TRequestProcessor>> Processors;
    std::deque<TMessageHolder<TMessage>> Packets;
    std::set<uint32_t> Isolated;
    std::shared_ptr<TDurableManualTimeSource> Clock =
        std::make_shared<TDurableManualTimeSource>();

    explicit TDurableIdempotencyCluster(TFiles& files, bool reopen)
        : Files(files) {
        constexpr std::vector<uint32_t>::size_type NodeCount = 3;
        std::vector<TNodeDict> peers(NodeCount);
        for (uint32_t id = 1; id <= NodeCount; ++id) {
            auto state = std::make_shared<TDurableState>(Files.Name, id);
            if (!reopen) {
                state->CurrentTerm = 1;
                state->BindVoters({1, 2, 3});
            } else {
                assert_true((state->GetBoundVoters() == std::vector<uint32_t>{1, 2, 3}));
            }
            States.push_back(std::move(state));
        }
        for (uint32_t id = 1; id <= NodeCount; ++id) {
            for (uint32_t dst = 1; dst <= NodeCount; ++dst) {
                if (dst != id) {
                    peers[id - 1][dst] = std::make_shared<TDurableQueuedPeer>(
                        [this](auto message) { Packets.push_back(std::move(message)); });
                }
            }
        }
        for (uint32_t id = 1; id <= NodeCount; ++id) {
            auto raft = std::make_shared<TRaft>(States[id - 1], id, peers[id - 1]);
            auto application = std::make_shared<TDurableControlRsm>();
            application->State.Epoch = 1;
            auto processor = std::make_shared<TRequestProcessor>(raft, application, peers[id - 1],
                TRequestProcessor::TAdmissionLimits{}, Clock);
            Rafts.push_back(std::move(raft));
            Applications.push_back(std::move(application));
            Processors.push_back(std::move(processor));
        }
    }

    void Drain() {
        size_t budget = 50000;
        while (!Packets.empty()) {
            assert_true(budget-- > 0);
            auto message = std::move(Packets.front());
            Packets.pop_front();
            auto rpc = message.Cast<TMessageEx>();
            if (Isolated.count(rpc->Src) || Isolated.count(rpc->Dst)) continue;
            const auto index = size_t(rpc->Dst - 1);
            Rafts.at(index)->Process(Clock->Value, message);
            Processors.at(index)->CheckStateChange();
            Processors.at(index)->ProcessCommitted();
            Processors.at(index)->ProcessWaiting();
        }
    }

    void Elect(uint32_t id) {
        Drain();
        Clock->Value += std::chrono::seconds(30);
        auto& raft = Rafts.at(id - 1);
        for (int attempt = 0; attempt < 3; ++attempt) {
            raft->ProcessTimeout(Clock->Value);
            Drain();
        }
        assert_int_equal(raft->CurrentStateName(), EState::LEADER);
    }

    void Replicate(uint32_t id, int rounds = 12) {
        auto& raft = Rafts.at(id - 1);
        for (int round = 0; round < rounds; ++round) {
            Clock->Value += std::chrono::milliseconds(500);
            raft->ProcessTimeout(Clock->Value);
            Drain();
        }
    }
};

void test_idempotent_retry_after_multi_voter_durable_snapshot_restart(void**) {
    TFiles files;
    const auto makeRequest = [](uint32_t cookie) {
        constexpr std::string_view payload = "client-payload";
        auto request = NewHoldedMessage<TIdempotentCommandRequest>(
            sizeof(TIdempotentCommandRequest) + payload.size());
        request->Epoch = 1;
        request->SessionIdHigh = 11;
        request->SessionIdLow = 12;
        request->Sequence = 1;
        request->Cookie = cookie;
        memcpy(request->Data, payload.data(), payload.size());
        return request;
    };

    {
        TDurableIdempotencyCluster cluster(files, false);
        cluster.Elect(1);
        cluster.Replicate(1);
        assert_true(cluster.Rafts[0]->CanAppendIdempotentLogEntries());

        // Voter 2 misses the committed write and is brought current by the
        // snapshot. The client has no reply route, modeling a lost response.
        cluster.Isolated.insert(2);
        cluster.Processors[0]->OnIdempotentCommandRequest(makeRequest(71), {});
        cluster.Replicate(1);
        assert_int_equal(cluster.States[0]->CommitIndex, 2);
        assert_int_equal(cluster.States[2]->CommitIndex, 2);
        assert_int_equal(cluster.Applications[0]->IdempotentApplicationCalls, 1);
        assert_int_equal(cluster.Applications[2]->IdempotentApplicationCalls, 1);

        const auto snapshot = cluster.Rafts[0]->CompactShared();
        assert_int_equal(snapshot->Index, 2);
        cluster.Isolated.erase(2);
        cluster.Replicate(1);
        assert_int_equal(cluster.States[1]->SnapshotIndex, 2);
        assert_int_equal(cluster.Applications[1]->LastAppliedIndex, 2);
        assert_true(cluster.Applications[1]->State.ApplicationData == "client-payload");
    }

    {
        TDurableIdempotencyCluster cluster(files, true);
        assert_int_equal(cluster.States[0]->SnapshotIndex, 2);
        assert_int_equal(cluster.States[1]->SnapshotIndex, 2);
        assert_int_equal(cluster.Applications[0]->LastAppliedIndex, 2);
        assert_int_equal(cluster.Applications[1]->LastAppliedIndex, 2);
        assert_int_equal(cluster.Applications[1]->State.Sessions.size(), 1);
        assert_int_equal(cluster.Applications[1]->State.Sessions.front().HighestSequence, 1);
        assert_int_equal(cluster.Applications[1]->State.Sessions.front().LogIndex, 2);
        assert_true(cluster.Applications[1]->State.Sessions.front().PayloadDigest ==
            ComputeIdempotentPayloadDigest("client-payload"));
        assert_true(cluster.Applications[1]->State.Sessions.front().ResponseData == "durable-result");

        // Voter 2 recovers the combined snapshot, wins a new term, and commits
        // a retry. Voter 3 recovers the same history by replaying its durable log.
        cluster.Elect(2);
        cluster.Replicate(2);
        assert_true(cluster.Rafts[1]->CanAppendIdempotentLogEntries());
        auto retryClient = std::make_shared<TDurableIdempotencyPeer>();
        cluster.Processors[1]->OnIdempotentCommandRequest(makeRequest(72), retryClient);
        cluster.Replicate(2);

        assert_int_equal(retryClient->Sent.size(), 1);
        auto response = retryClient->Sent.back().Cast<TIdempotentCommandResponse>();
        ValidateIdempotentCommandResponse(response);
        assert_int_equal(response->Cookie, 72);
        assert_int_equal(response->ErrorCode, TCommandResponse::Success);
        assert_int_equal(response->Index, 2);
        assert_true(std::string(response->Data,
            response->Len - sizeof(TIdempotentCommandResponse)) == "durable-result");

        for (size_t i = 0; i < cluster.States.size(); ++i) {
            assert_int_equal(cluster.States[i]->CommitIndex, 4);
            assert_int_equal(cluster.Applications[i]->LastAppliedIndex, 4);
            assert_true(cluster.Applications[i]->State.ApplicationData == "client-payload");
            assert_int_equal(cluster.Applications[i]->State.Sessions.size(), 1);
            assert_int_equal(cluster.Applications[i]->State.Sessions.front().HighestSequence, 1);
            assert_int_equal(cluster.Applications[i]->State.Sessions.front().LogIndex, 2);
            assert_true(cluster.Applications[i]->State.Sessions.front().ResponseData == "durable-result");
        }
        assert_int_equal(cluster.Applications[0]->IdempotentApplicationCalls, 0);
        assert_int_equal(cluster.Applications[1]->IdempotentApplicationCalls, 0);
        assert_int_equal(cluster.Applications[2]->IdempotentApplicationCalls, 1);
    }
}

void test_closed_idempotent_session_survives_durable_snapshot_restart(void**) {
    TFiles files;
    const auto makeRequest = [](uint32_t cookie) {
        constexpr std::string_view payload = "client-payload";
        auto request = NewHoldedMessage<TIdempotentCommandRequest>(
            sizeof(TIdempotentCommandRequest) + payload.size());
        request->Epoch = 1;
        request->SessionIdHigh = 11;
        request->SessionIdLow = 12;
        request->Sequence = 1;
        request->Cookie = cookie;
        memcpy(request->Data, payload.data(), payload.size());
        return request;
    };
    const TIdempotencyControlOperation closeSession{
        .Kind = EIdempotencyControlKind::CloseSession,
        .Epoch = 1,
        .SessionIdHigh = 11,
        .SessionIdLow = 12,
    };

    {
        TDurableIdempotencyCluster cluster(files, false);
        cluster.Elect(1);
        cluster.Replicate(1);
        assert_true(cluster.Rafts[0]->CanAppendIdempotentLogEntries());
        assert_true(cluster.Rafts[0]->CanAppendIdempotencyControlEntries());

        cluster.Isolated.insert(2);
        cluster.Processors[0]->OnIdempotentCommandRequest(makeRequest(81), {});
        cluster.Replicate(1);
        assert_int_equal(cluster.States[0]->CommitIndex, 2);
        assert_int_equal(cluster.States[2]->CommitIndex, 2);

        const auto closeIndex = cluster.Processors[0]->ProposeIdempotencyControl(closeSession);
        assert_int_equal(closeIndex, 3);
        cluster.Replicate(1);
        assert_int_equal(cluster.States[0]->CommitIndex, 3);
        assert_int_equal(cluster.States[2]->CommitIndex, 3);
        for (size_t node : {size_t(0), size_t(2)}) {
            const auto& session = cluster.Applications[node]->State.Sessions.front();
            assert_true(session.Closed);
            assert_int_equal(session.HighestSequence, 1);
            assert_int_equal(session.LogIndex, 2);
            assert_true(session.ResponseData.empty());
            assert_int_equal(session.ErrorCode, TCommandResponse::Success);
            assert_true(cluster.Applications[node]->State.ApplicationData == "client-payload");
        }

        const auto snapshot = cluster.Rafts[0]->CompactShared();
        assert_int_equal(snapshot->Index, 3);
        cluster.Isolated.erase(2);
        cluster.Replicate(1);
        assert_int_equal(cluster.States[1]->SnapshotIndex, 3);
        assert_true(cluster.Applications[1]->State.Sessions.front().Closed);
        assert_true(cluster.Applications[1]->State.ApplicationData == "client-payload");
    }

    {
        TDurableIdempotencyCluster cluster(files, true);
        assert_int_equal(cluster.States[0]->SnapshotIndex, 3);
        assert_int_equal(cluster.States[1]->SnapshotIndex, 3);
        for (size_t node : {size_t(0), size_t(1)}) {
            const auto& session = cluster.Applications[node]->State.Sessions.front();
            assert_true(session.Closed);
            assert_int_equal(session.HighestSequence, 1);
            assert_true(session.ResponseData.empty());
            assert_true(cluster.Applications[node]->State.ApplicationData == "client-payload");
        }

        cluster.Elect(2);
        cluster.Replicate(2);
        assert_true(cluster.Rafts[1]->CanAppendIdempotentLogEntries());
        assert_true(cluster.Rafts[1]->CanAppendIdempotencyControlEntries());
        const auto lastLogIndex = cluster.States[1]->LastLogIndex;
        auto retryClient = std::make_shared<TDurableIdempotencyPeer>();
        bool rejected = false;
        try {
            cluster.Processors[1]->OnIdempotentCommandRequest(makeRequest(82), retryClient);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }

        assert_true(rejected);
        assert_true(retryClient->Sent.empty());
        assert_int_equal(cluster.States[1]->LastLogIndex, lastLogIndex);
        assert_int_equal(cluster.Applications[1]->IdempotentApplicationCalls, 0);
        assert_int_equal(cluster.Applications[1]->State.Sessions.size(), 1);
        assert_true(cluster.Applications[1]->State.Sessions.front().Closed);
        assert_true(cluster.Applications[1]->State.Sessions.front().ResponseData.empty());
        for (size_t node = 0; node < cluster.States.size(); ++node) {
            assert_true(cluster.Applications[node]->State.Sessions.front().Closed);
            assert_true(cluster.Applications[node]->State.Sessions.front().ResponseData.empty());
            assert_true(cluster.Applications[node]->State.ApplicationData == "client-payload");
        }
    }
}

void test_reference_kv_idempotency_replays_from_durable_snapshot_and_log(void**) {
    TFiles files;
    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + sizeof(TKvEntry) + 3 + 3);
    request->Epoch = 1;
    request->SessionIdLow = 91;
    request->Sequence = 1;
    const uint16_t keySize = 3, valueSize = 3;
    memcpy(request->Data, &keySize, sizeof(keySize));
    memcpy(request->Data + sizeof(keySize), &valueSize, sizeof(valueSize));
    memcpy(request->Data + sizeof(TKvEntry), "keyone", 6);

    const TIdempotentOperationIdentity identity{
        .Epoch = 1,
        .SessionIdLow = 91,
        .Sequence = 1,
        .PayloadDigest = ComputeIdempotentPayloadDigest(std::string_view(
            request->Data, request->Len - sizeof(TIdempotentCommandRequest))),
    };
    TSnapshot raftSnapshot;
    {
        TIdempotentKvRsm application;
        auto prepared = application.PrepareIdempotent(request);
        prepared->Term = 1;
        const auto result = application.ApplyIdempotentLogRecord(
            EncodeIdempotentLogEntry(identity, std::move(prepared)), 1);
        assert_int_equal(result.ErrorCode, TCommandResponse::Success);
        assert_int_equal(result.Index, 1);
        raftSnapshot = TSnapshot{1, 1, {1}, application.SaveSnapshot()};

        TDurableState state(files.Name, 1);
        state.BindVoters({1});
        state.InstallSnapshot(raftSnapshot);
    }

    // Reopen the durable snapshot, restore the real application RSM, then
    // persist a retry as a committed retained log entry.
    {
        TDurableState state(files.Name, 1);
        const auto savedSnapshot = state.GetSnapshot();
        assert_true(savedSnapshot && *savedSnapshot == raftSnapshot);
        TIdempotentKvRsm application;
        application.LoadSnapshot(savedSnapshot->Data, savedSnapshot->Index);
        auto retry = application.PrepareIdempotent(request);
        retry->Term = state.CurrentTerm;
        std::vector<TMessageHolder<TLogEntry>> suffix;
        suffix.push_back(EncodeIdempotentLogEntry(identity, std::move(retry)));
        state.AppendBatchAndCommit(suffix, 2);
    }

    // A second process restart recovers both the snapshot and committed retry.
    // Replaying the log entry returns the cached original result at index 1.
    {
        TDurableState state(files.Name, 1);
        assert_int_equal(state.SnapshotIndex, 1);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.CommitIndex, 2);
        const auto savedSnapshot = state.GetSnapshot();
        assert_true(bool(savedSnapshot));
        TIdempotentKvRsm application;
        application.LoadSnapshot(savedSnapshot->Data, savedSnapshot->Index);
        const auto result = application.ApplyIdempotentLogRecord(state.Get(2), 2);
        assert_int_equal(result.ErrorCode, TCommandResponse::Success);
        assert_int_equal(result.Index, 1);
        assert_true(result.Data == "one");
        assert_int_equal(application.LastAppliedIndex, 2);

        auto read = NewHoldedMessage<TReadKv>(sizeof(TReadKv) + 3);
        read->KeySize = 3;
        memcpy(read->Data, "key", 3);
        const auto response = application.Read(read, 2).Cast<TCommandResponse>();
        assert_true(std::string(response->Data,
            response->Len - sizeof(TCommandResponse)) == "one");
    }
}
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_crash_at_every_publication_boundary),
        cmocka_unit_test(test_acknowledged_updates_survive_immediate_exit),
        cmocka_unit_test(test_follower_persists_commit_before_ack_and_recovers_it),
        cmocka_unit_test(test_follower_batches_durable_append_before_ack),
        cmocka_unit_test(test_durable_append_rejects_invalid_terms_before_mutation),
        cmocka_unit_test(test_in_memory_state_rejects_invalid_terms_before_mutation),
        cmocka_unit_test(test_follower_replaces_durable_suffix_in_one_publish),
        cmocka_unit_test(test_publication_order),
        cmocka_unit_test(test_v1_v2_v3_images_recover_conservatively_and_upgrade_on_publish),
        cmocka_unit_test(test_live_publication_failure_is_fatal),
        cmocka_unit_test(test_mid_payload_failure_is_fatal_and_preserves_image),
        cmocka_unit_test(test_write_error_is_fatal_and_preserves_image),
        cmocka_unit_test(test_interrupted_initialization_is_not_reset),
        cmocka_unit_test(test_corrupt_images_are_rejected),
        cmocka_unit_test(test_single_writer_lock),
        cmocka_unit_test(test_missing_image_is_not_recreated),
        cmocka_unit_test(test_legacy_store_is_not_silently_migrated),
        cmocka_unit_test(test_split_legacy_disk_state_migration_requires_trusted_baselines),
        cmocka_unit_test(test_compacted_legacy_disk_state_migration_preserves_snapshot_and_suffix),
        cmocka_unit_test(test_legacy_configuration_snapshot_migration_requires_complete_trusted_history),
        cmocka_unit_test(test_legacy_vote_cannot_be_justified_by_uncommitted_membership),
        cmocka_unit_test(test_get_and_append_do_not_share_mutable_entries),
        cmocka_unit_test(test_raft_election_and_application_replay_after_restart),
        cmocka_unit_test(test_idempotency_control_survives_durable_restart_and_replays_atomically),
        cmocka_unit_test(test_duplicate_idempotent_records_apply_once_after_durable_restart),
        cmocka_unit_test(test_idempotent_retry_after_durable_snapshot_restart_returns_cached_result),
        cmocka_unit_test(test_idempotent_retry_after_multi_voter_durable_snapshot_restart),
        cmocka_unit_test(test_closed_idempotent_session_survives_durable_snapshot_restart),
        cmocka_unit_test(test_reference_kv_idempotency_replays_from_durable_snapshot_and_log),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
