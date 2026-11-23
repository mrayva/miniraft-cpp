#include <miniraft/raft.h>
#include <miniraft/snapshot_codec.h>
#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <set>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <string>
#include <vector>
extern "C" {
#include <cmocka.h>
}

namespace {
TIdempotencySnapshot MakeSnapshot() {
    TIdempotencySnapshot snapshot;
    snapshot.LastAppliedIndex = 7;
    snapshot.Epoch = 3;
    snapshot.ApplicationData = std::string("state\0image", 11);

    TIdempotencySessionRecord active;
    active.SessionIdHigh = 1;
    active.SessionIdLow = 2;
    active.HighestSequence = 5;
    for (size_t i = 0; i < active.PayloadDigest.size(); ++i)
        active.PayloadDigest[i] = static_cast<uint8_t>(i + 1);
    active.LogIndex = 6;
    active.ResponseData = std::string("reply\0body", 10);

    TIdempotencySessionRecord closed;
    closed.SessionIdHigh = 3;
    closed.HighestSequence = 2;
    closed.LogIndex = 7;
    closed.Closed = true;
    snapshot.Sessions = {active, closed};
    return snapshot;
}

void Reject(const std::function<void()>& action) {
    bool rejected = false;
    try { action(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}

void PutU32(std::string& bytes, size_t offset, uint32_t value) {
    for (unsigned i = 0; i < 4; ++i) bytes.at(offset + i) = char(value >> (8 * i));
}

void RepairChecksum(std::string& bytes) {
    const auto checksum = NSnapshotCodec::Checksum(
        std::string_view(bytes).substr(0, bytes.size() - 4));
    PutU32(bytes, bytes.size() - 4, checksum);
}

std::string Hex(const std::array<uint8_t, 32>& bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (auto byte : bytes) {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

struct TSnapshotRsm : IIdempotentRsm {
    explicit TSnapshotRsm(TIdempotencySnapshotLimits limits = {})
        : IIdempotentRsm(std::move(limits)) { }

    TIdempotencySnapshot State;
    uint64_t ResponseBound = 64;
    size_t IdempotentWrites = 0;
    size_t ControlWrites = 0;
    mutable size_t ControlValidations = 0;
    TIdempotentOperationIdentity LastIdentity;
    TIdempotencyControlOperation LastControl;
    std::string LastAppliedCommand;
    std::string LastControlCommand;

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
    void ValidateIdempotent(TMessageHolder<TIdempotentCommandRequest>) const override { }
    uint64_t CachedResponseDataUpperBound(
            TMessageHolder<TIdempotentCommandRequest>) const override { return ResponseBound; }
    TMessageHolder<TLogEntry> PrepareIdempotent(
            TMessageHolder<TIdempotentCommandRequest> request) override {
        const auto payloadBytes = request->Len - sizeof(TIdempotentCommandRequest);
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + payloadBytes);
        if (payloadBytes) memcpy(entry->Data, request->Data, payloadBytes);
        return entry;
    }
    TIdempotentWriteResult WriteIdempotent(TMessageHolder<TLogEntry> command, uint64_t index,
            const TIdempotentOperationIdentity& identity) override {
        ++IdempotentWrites;
        LastIdentity = identity;
        LastAppliedCommand.assign(command->Data, command->Len - sizeof(TLogEntry));
        State.LastAppliedIndex = index;
        LastAppliedIndex = index;
        return {.Index = index, .ErrorCode = TCommandResponse::Success, .Data = "applied"};
    }
    void ValidateIdempotencyControl(const TIdempotencyControlOperation& operation) const override {
        ++ControlValidations;
        ValidateIdempotencyControlOperation(operation);
        if (operation.Kind == EIdempotencyControlKind::AdvanceEpoch && State.Epoch != operation.Epoch)
            throw std::invalid_argument("Unexpected test idempotency epoch");
    }
    TMessageHolder<TLogEntry> PrepareIdempotencyControl(
            const TIdempotencyControlOperation&) override {
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 8);
        memcpy(entry->Data, "prepared", 8);
        return entry;
    }
    void ApplyIdempotencyControl(const TIdempotencyControlOperation& operation,
            TMessageHolder<TLogEntry> applicationEntry, uint64_t index) override {
        ++ControlWrites;
        LastControl = operation;
        LastControlCommand.assign(applicationEntry->Data,
            applicationEntry->Len - sizeof(TLogEntry));
        State.ApplicationData = LastControlCommand;
        if (operation.Kind == EIdempotencyControlKind::AdvanceEpoch)
            State.Epoch = operation.NewEpoch;
        State.LastAppliedIndex = index;
        LastAppliedIndex = index;
    }
    TIdempotencySnapshot CaptureIdempotencySnapshot() const override { return State; }
    void ValidateIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
            uint64_t index) const override {
        ::ValidateIdempotencySnapshot(snapshot, SnapshotLimits);
        if (snapshot.LastAppliedIndex != index || snapshot.ApplicationData == "invalid")
            throw std::runtime_error("Invalid test application snapshot");
    }
    void RestoreIdempotencySnapshot(TIdempotencySnapshot snapshot, uint64_t index) override {
        State = std::move(snapshot);
        LastAppliedIndex = index;
    }
};

struct TIdempotencyPeer : INode {
    std::vector<TMessageHolder<TMessage>> Sent;
    bool SessionizedForwarding = false;
    void Send(TMessageHolder<TMessage> message) override { Sent.push_back(std::move(message)); }
    void Drain() override { }
    bool SupportsSessionizedForwarding() const override { return SessionizedForwarding; }
};

struct TIdempotencyManualTimeSource : ITimeSource {
    Time Value{};
    Time Now() override { return Value; }
};

struct TDeduplicatingSnapshotRsm : TSnapshotRsm {
    size_t ApplicationMutations = 0;

    explicit TDeduplicatingSnapshotRsm(TIdempotencySnapshotLimits limits = {})
        : TSnapshotRsm(std::move(limits)) { }

    void ValidateIdempotent(TMessageHolder<TIdempotentCommandRequest> request) const override {
        if (request->Epoch != State.Epoch)
            throw std::invalid_argument("Unexpected idempotent test epoch");
        const auto existing = std::find_if(State.Sessions.begin(), State.Sessions.end(),
            [&](const auto& session) {
                return session.SessionIdHigh == request->SessionIdHigh &&
                    session.SessionIdLow == request->SessionIdLow;
            });
        if (existing == State.Sessions.end()) {
            if (request->Sequence != 1)
                throw std::invalid_argument("New idempotent test sessions must start at sequence 1");
            const auto activeSessions = std::count_if(State.Sessions.begin(), State.Sessions.end(),
                [](const auto& session) { return !session.Closed; });
            if (activeSessions >= SnapshotLimits.MaxActiveSessions)
                throw std::invalid_argument("Idempotent test session cache is full");
            return;
        }
        if (request->Sequence < existing->HighestSequence)
            throw std::invalid_argument("Stale idempotent test sequence");
        if (request->Sequence > existing->HighestSequence &&
            request->Sequence - existing->HighestSequence > 1)
            throw std::invalid_argument("Gap in idempotent test sequence");
    }
    TIdempotentWriteResult WriteIdempotent(TMessageHolder<TLogEntry> command, uint64_t index,
            const TIdempotentOperationIdentity& identity) override {
        const auto payloadSize = command->Len - sizeof(TLogEntry);
        const std::string payload(command->Data, payloadSize);
        const auto existing = std::find_if(State.Sessions.begin(), State.Sessions.end(),
            [&](const auto& session) {
                return session.SessionIdHigh == identity.SessionIdHigh &&
                    session.SessionIdLow == identity.SessionIdLow;
            });

        LastAppliedCommand = payload;
        LastIdentity = identity;
        LastAppliedIndex = index;
        State.LastAppliedIndex = index;
        if (existing != State.Sessions.end()) {
            if (identity.Epoch == State.Epoch &&
                identity.Sequence == existing->HighestSequence &&
                identity.PayloadDigest == existing->PayloadDigest)
                return {.Index = existing->LogIndex, .ErrorCode = existing->ErrorCode,
                    .Data = existing->ResponseData};
            if (identity.Sequence == existing->HighestSequence)
                return {.Index = index, .ErrorCode = TCommandResponse::IdempotencyConflict};
            if (identity.Sequence < existing->HighestSequence)
                return {.Index = existing->LogIndex, .ErrorCode = TCommandResponse::AlreadyCompleted};
            if (identity.Sequence - existing->HighestSequence > 1)
                return {.Index = index, .ErrorCode = TCommandResponse::IdempotencyConflict};
        } else if (identity.Sequence != 1) {
            return {.Index = index, .ErrorCode = TCommandResponse::IdempotencyConflict};
        }

        ++ApplicationMutations;
        if (!State.ApplicationData.empty()) State.ApplicationData.push_back('|');
        State.ApplicationData += payload;
        const std::string response = "result:" + payload;
        if (existing != State.Sessions.end()) {
            existing->HighestSequence = identity.Sequence;
            existing->PayloadDigest = identity.PayloadDigest;
            existing->LogIndex = index;
            existing->ErrorCode = TCommandResponse::Success;
            existing->ResponseData = response;
        } else {
            State.Sessions.push_back({
                .SessionIdHigh = identity.SessionIdHigh,
                .SessionIdLow = identity.SessionIdLow,
                .HighestSequence = identity.Sequence,
                .PayloadDigest = identity.PayloadDigest,
                .LogIndex = index,
                .ErrorCode = TCommandResponse::Success,
                .ResponseData = response,
            });
        }
        return {.Index = index, .ErrorCode = TCommandResponse::Success, .Data = response};
    }
    TIdempotencySnapshot CaptureIdempotencySnapshot() const override {
        auto snapshot = State;
        snapshot.LastAppliedIndex = LastAppliedIndex;
        return snapshot;
    }
};

struct TIdempotencyQueuedPeer : INode {
    std::function<void(TMessageHolder<TMessage>)> Deliver;
    explicit TIdempotencyQueuedPeer(std::function<void(TMessageHolder<TMessage>)> deliver)
        : Deliver(std::move(deliver)) { }
    void Send(TMessageHolder<TMessage> message) override { Deliver(std::move(message)); }
    void Drain() override { }
};

struct TIdempotencyCluster {
    std::vector<std::shared_ptr<TRaft>> Rafts;
    std::vector<std::shared_ptr<TDeduplicatingSnapshotRsm>> Applications;
    std::vector<std::shared_ptr<TRequestProcessor>> Processors;
    std::deque<TMessageHolder<TMessage>> Packets;
    std::set<uint32_t> Isolated;
    std::set<std::pair<uint32_t, uint32_t>> DroppedLinks;
    std::set<uint32_t> DroppedAppendEntriesFrom;
    std::shared_ptr<TIdempotencyManualTimeSource> Clock =
        std::make_shared<TIdempotencyManualTimeSource>();

    explicit TIdempotencyCluster(uint32_t count, TIdempotencySnapshotLimits limits = {}) {
        std::vector<TNodeDict> peers(count);
        for (uint32_t id = 1; id <= count; ++id) {
            for (uint32_t dst = 1; dst <= count; ++dst) {
                if (dst != id) peers[id - 1][dst] = std::make_shared<TIdempotencyQueuedPeer>(
                    [this](auto message) { Packets.push_back(std::move(message)); });
            }
        }
        for (uint32_t id = 1; id <= count; ++id) {
            auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), id, peers[id - 1]);
            auto application = std::make_shared<TDeduplicatingSnapshotRsm>(limits);
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
            if (Isolated.count(rpc->Src) || Isolated.count(rpc->Dst) ||
                DroppedLinks.contains({rpc->Src, rpc->Dst}) ||
                (message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST) &&
                 DroppedAppendEntriesFrom.count(rpc->Src))) continue;
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

TMessageHolder<TIdempotentCommandRequest> IdempotentRequest(
        uint32_t cookie, std::string_view payload = "work") {
    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + payload.size());
    request->Epoch = 1;
    request->SessionIdHigh = 11;
    request->SessionIdLow = 12;
    request->Sequence = 1;
    request->Cookie = cookie;
    memcpy(request->Data, payload.data(), payload.size());
    return request;
}

void test_idempotency_snapshot_roundtrip_preserves_dedup_state(void**) {
    const auto source = MakeSnapshot();
    const auto bytes = EncodeIdempotencySnapshot(source);
    assert_true(DecodeIdempotencySnapshot(bytes) == source);

    auto corrupted = bytes;
    corrupted.back() ^= 1;
    Reject([&] { (void)DecodeIdempotencySnapshot(corrupted); });

    auto unsupportedVersion = bytes;
    PutU32(unsupportedVersion, 8, TIdempotencySnapshot::CurrentVersion + 1);
    RepairChecksum(unsupportedVersion);
    Reject([&] { (void)DecodeIdempotencySnapshot(unsupportedVersion); });
    auto excessiveCount = bytes;
    PutU32(excessiveCount, 36, UINT32_MAX);
    RepairChecksum(excessiveCount);
    Reject([&] { (void)DecodeIdempotencySnapshot(excessiveCount); });
    auto trailingData = bytes;
    trailingData.insert(trailingData.end() - 4, 'x');
    RepairChecksum(trailingData);
    Reject([&] { (void)DecodeIdempotencySnapshot(trailingData); });

    auto invalid = source;
    invalid.Epoch = 0;
    Reject([&] { (void)EncodeIdempotencySnapshot(invalid); });
    invalid = source;
    std::swap(invalid.Sessions[0], invalid.Sessions[1]);
    Reject([&] { (void)EncodeIdempotencySnapshot(invalid); });
    invalid = source;
    invalid.Sessions[1].SessionIdHigh = invalid.Sessions[0].SessionIdHigh;
    invalid.Sessions[1].SessionIdLow = invalid.Sessions[0].SessionIdLow;
    Reject([&] { (void)EncodeIdempotencySnapshot(invalid); });
    invalid = source;
    invalid.Sessions[0].LogIndex = invalid.LastAppliedIndex + 1;
    Reject([&] { (void)EncodeIdempotencySnapshot(invalid); });
    invalid = source;
    invalid.Sessions[1].ResponseData = "must be discarded when closed";
    Reject([&] { (void)EncodeIdempotencySnapshot(invalid); });

    auto limits = TIdempotencySnapshotLimits{};
    limits.MaxResponseBytes = 4;
    Reject([&] { (void)EncodeIdempotencySnapshot(source, limits); });
    limits = {};
    limits.MaxActiveSessions = 0;
    Reject([&] { (void)EncodeIdempotencySnapshot(source, limits); });
    limits = {};
    limits.MaxResponseBytes = TMessageLimits{}.MaxFrameBytes;
    Reject([&] { ValidateIdempotencySnapshotLimits(limits); });
}

void test_idempotent_payload_digest_matches_sha256_vectors(void**) {
    assert_string_equal(Hex(ComputeIdempotentPayloadDigest("")).c_str(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert_string_equal(Hex(ComputeIdempotentPayloadDigest("abc")).c_str(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert_string_equal(Hex(ComputeIdempotentPayloadDigest(
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")).c_str(),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    assert_string_equal(Hex(ComputeIdempotentPayloadDigest(
        "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn"
        "hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu")).c_str(),
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1");
    assert_true(ComputeIdempotentPayloadDigest(std::string_view("a\0b", 3)) !=
        ComputeIdempotentPayloadDigest("a"));
}

void test_idempotent_log_record_roundtrip_and_rejects_corruption(void**) {
    auto application = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 5);
    application->Term = 4;
    application->Data[0] = 'a';
    application->Data[1] = '\0';
    application->Data[2] = 'b';
    application->Data[3] = 'c';
    application->Data[4] = 'd';

    TIdempotentOperationIdentity identity;
    identity.Epoch = 8;
    identity.SessionIdHigh = 9;
    identity.SessionIdLow = 10;
    identity.Sequence = 11;
    identity.PayloadDigest = ComputeIdempotentPayloadDigest(std::string_view("a\0bcd", 5));
    auto encoded = EncodeIdempotentLogEntry(identity, application);
    assert_int_equal(encoded->Term, 4);
    assert_int_equal(encoded->Flags, TLogEntry::EIdempotent);

    const auto decoded = DecodeIdempotentLogEntry(encoded);
    assert_true(decoded.Identity == identity);
    assert_int_equal(decoded.ApplicationEntry->Term, 4);
    assert_int_equal(decoded.ApplicationEntry->Flags, TLogEntry::ENone);
    assert_int_equal(decoded.ApplicationEntry->Len, sizeof(TLogEntry) + 5);
    assert_memory_equal(decoded.ApplicationEntry->Data, "a\0bcd", 5);

    auto corrupt = NewHoldedMessage<TLogEntry>(encoded->Len);
    memcpy(corrupt.Mes, encoded.Mes, encoded->Len);
    corrupt->Data[12] ^= 1;
    Reject([&] { (void)DecodeIdempotentLogEntry(corrupt); });

    auto invalid = identity;
    invalid.Epoch = 0;
    Reject([&] { (void)EncodeIdempotentLogEntry(invalid, application); });
    application->Flags = TLogEntry::EConfiguration;
    Reject([&] { (void)EncodeIdempotentLogEntry(identity, application); });
    application->Flags = TLogEntry::EIdempotencyControl;
    Reject([&] { (void)EncodeIdempotentLogEntry(identity, application); });
}

void test_idempotency_control_log_record_roundtrip_and_rejects_corruption(void**) {
    TIdempotencyControlOperation operation{
        .Kind = EIdempotencyControlKind::CloseSession,
        .Epoch = 8,
        .SessionIdHigh = 9,
        .SessionIdLow = 10,
    };
    auto application = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 5);
    application->Term = 4;
    memcpy(application->Data, "close", 5);
    auto encoded = EncodeIdempotencyControlLogEntry(operation, application);
    assert_int_equal(encoded->Term, 4);
    assert_int_equal(encoded->Flags, TLogEntry::EIdempotencyControl);

    const auto decoded = DecodeIdempotencyControlLogEntry(encoded);
    assert_true(decoded.Operation == operation);
    assert_int_equal(decoded.ApplicationEntry->Term, 4);
    assert_int_equal(decoded.ApplicationEntry->Flags, TLogEntry::ENone);
    assert_int_equal(decoded.ApplicationEntry->Len, sizeof(TLogEntry) + 5);
    assert_memory_equal(decoded.ApplicationEntry->Data, "close", 5);

    auto corrupt = NewHoldedMessage<TLogEntry>(encoded->Len);
    memcpy(corrupt.Mes, encoded.Mes, encoded->Len);
    corrupt->Data[12] ^= 1;
    Reject([&] { (void)DecodeIdempotencyControlLogEntry(corrupt); });

    auto invalid = operation;
    invalid.SessionIdHigh = invalid.SessionIdLow = 0;
    Reject([&] { (void)EncodeIdempotencyControlLogEntry(invalid, application); });
    application->Flags = TLogEntry::EConfiguration;
    Reject([&] { (void)EncodeIdempotencyControlLogEntry(operation, application); });
}

void test_configuration_history_accepts_valid_idempotency_control_entries(void**) {
    TConfigurationHistory history = TConfigurationHistory::Stable({1}, 0, 0, 0, 0);
    TIdempotencyControlOperation operation{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 2,
        .NewEpoch = 3,
    };
    auto application = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry));
    application->Term = 1;
    auto control = EncodeIdempotencyControlLogEntry(operation, application);
    const auto next = history.Observe(1, control);
    assert_int_equal(next.ThroughIndex(), 1);
    assert_int_equal(next.ThroughTerm(), 1);
    assert_true(next.OldVoters() == (std::vector<uint32_t>{1}));
}

void test_idempotency_control_is_proposed_and_atomically_applied_after_commit(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1});
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TSnapshotRsm>();
    rsm->State.Epoch = 2;
    TRequestProcessor processor(raft, rsm, {});
    raft->Become(EState::LEADER);

    const TIdempotencyControlOperation operation{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 2,
        .NewEpoch = 3,
    };
    const auto index = processor.ProposeIdempotencyControl(operation);
    assert_int_equal(index, 1);
    assert_int_equal(state->Get(1)->Flags, TLogEntry::EIdempotencyControl);
    assert_int_equal(rsm->ControlWrites, 0);
    assert_int_equal(rsm->LastAppliedIndex, 0);
    assert_int_equal(rsm->State.Epoch, 2);

    const_cast<TVolatileState*>(raft->GetVolatileState())->CommitIndex = index;
    processor.ProcessCommitted();
    assert_int_equal(rsm->ControlWrites, 1);
    assert_true(rsm->LastControl == operation);
    assert_true(rsm->LastControlCommand == "prepared");
    assert_int_equal(rsm->ControlValidations, 2);
    assert_true(rsm->State.ApplicationData == "prepared");
    assert_int_equal(rsm->State.Epoch, 3);
    assert_int_equal(rsm->State.LastAppliedIndex, index);
    assert_int_equal(rsm->LastAppliedIndex, index);

    TSnapshotRsm restored;
    restored.LoadSnapshot(rsm->SaveSnapshot(), index);
    assert_true(restored.State == rsm->State);
    assert_int_equal(restored.LastAppliedIndex, index);
}

void test_committed_idempotent_log_replay_uses_identity_and_application_entry(void**) {
    auto prepared = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 4);
    prepared->Term = 1;
    memcpy(prepared->Data, "work", 4);
    TIdempotentOperationIdentity identity;
    identity.Epoch = 3;
    identity.SessionIdHigh = 4;
    identity.SessionIdLow = 5;
    identity.Sequence = 6;
    identity.PayloadDigest = ComputeIdempotentPayloadDigest("client payload");
    auto replicated = EncodeIdempotentLogEntry(identity, prepared);

    auto legacy = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    legacy->Term = 1;
    legacy->Data[0] = 'x';
    std::vector<TMessageHolder<TLogEntry>> entries{legacy, replicated};
    auto state = std::make_shared<TState>(1, 0, entries);
    state->CommitIndex = 2;
    state->Commit();
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, {});

    processor.ProcessCommitted();
    assert_int_equal(rsm->IdempotentWrites, 1);
    assert_true(rsm->LastIdentity == identity);
    assert_true(rsm->LastAppliedCommand == "work");
    assert_int_equal(rsm->LastAppliedIndex, 2);
    assert_int_equal(rsm->State.LastAppliedIndex, 2);
}

void test_idempotent_request_is_proposed_and_replied_after_application(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1});
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, {});
    auto client = std::make_shared<TIdempotencyPeer>();
    raft->Become(EState::LEADER);

    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + 4);
    request->Epoch = 3;
    request->SessionIdHigh = 4;
    request->SessionIdLow = 5;
    request->Sequence = 6;
    request->Cookie = 7;
    memcpy(request->Data, "work", 4);
    processor.OnIdempotentCommandRequest(request, client);

    assert_int_equal(state->LastLogIndex, 1);
    assert_int_equal(state->Get(1)->Flags, TLogEntry::EIdempotent);
    assert_true(client->Sent.empty());

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->CommitIndex = 1;
    processor.ProcessCommitted();

    assert_int_equal(client->Sent.size(), 1);
    auto response = client->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    assert_int_equal(response->Index, 1);
    assert_int_equal(response->Epoch, request->Epoch);
    assert_int_equal(response->SessionIdHigh, request->SessionIdHigh);
    assert_int_equal(response->SessionIdLow, request->SessionIdLow);
    assert_int_equal(response->Sequence, request->Sequence);
    assert_int_equal(response->Cookie, request->Cookie);
    assert_int_equal(response->ErrorCode, TCommandResponse::Success);
    assert_true(std::string(response->Data, response->Len - sizeof(TIdempotentCommandResponse)) == "applied");
    assert_int_equal(rsm->IdempotentWrites, 1);
    assert_true(rsm->LastAppliedCommand == "work");
    assert_true(rsm->LastIdentity.PayloadDigest == ComputeIdempotentPayloadDigest("work"));
}

void test_idempotent_request_forwards_through_sessionized_route(void**) {
    auto followerState = std::make_shared<TState>();
    followerState->CurrentTerm = 1;
    followerState->BindVoters({1, 2});
    auto peer = std::make_shared<TIdempotencyPeer>();
    peer->SessionizedForwarding = true;
    auto followerRaft = std::make_shared<TRaft>(followerState, 2, TNodeDict{{1, peer}});
    auto followerRsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor follower(followerRaft, followerRsm, TNodeDict{{1, peer}});
    const_cast<TVolatileState*>(followerRaft->GetVolatileState())->LeaderId = 1;
    auto client = std::make_shared<TIdempotencyPeer>();

    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + 4);
    request->Epoch = 3;
    request->SessionIdHigh = 4;
    request->SessionIdLow = 5;
    request->Sequence = 6;
    request->Cookie = 7;
    memcpy(request->Data, "work", 4);
    follower.OnIdempotentCommandRequest(request, client);

    assert_int_equal(peer->Sent.size(), 1);
    auto forwarded = peer->Sent.back().Cast<TSessionForwardedCommandRequest>();
    assert_true(forwarded->Flags & TSessionForwardedCommandRequest::EIdempotent);
    assert_int_equal(forwarded->ClientCookie, request->Cookie);

    auto leaderState = std::make_shared<TState>();
    leaderState->CurrentTerm = 1;
    leaderState->BindVoters({1});
    auto leaderRaft = std::make_shared<TRaft>(leaderState, 1, TNodeDict{});
    auto leaderRsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor leader(leaderRaft, leaderRsm, {});
    leaderRaft->Become(EState::LEADER);
    leader.OnSessionForwardedCommandRequest(forwarded, peer);
    assert_int_equal(leaderState->LastLogIndex, 1);

    const_cast<TVolatileState*>(leaderRaft->GetVolatileState())->CommitIndex = 1;
    leader.ProcessCommitted();
    assert_int_equal(peer->Sent.size(), 2);
    auto forwardedResponse = peer->Sent.back().Cast<TSessionForwardedCommandResponse>();
    follower.OnSessionForwardedCommandResponse(forwardedResponse, peer);

    assert_int_equal(client->Sent.size(), 1);
    auto response = client->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    assert_int_equal(response->Index, 1);
    assert_int_equal(response->Epoch, request->Epoch);
    assert_int_equal(response->SessionIdHigh, request->SessionIdHigh);
    assert_int_equal(response->SessionIdLow, request->SessionIdLow);
    assert_int_equal(response->Sequence, request->Sequence);
    assert_int_equal(response->Cookie, request->Cookie);
    assert_int_equal(response->ErrorCode, TCommandResponse::Success);
}

void test_forwarded_idempotent_request_is_not_forwarded_again_by_follower(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1, 2});
    auto peer = std::make_shared<TIdempotencyPeer>();
    peer->SessionizedForwarding = true;
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, TNodeDict{{1, peer}});
    const_cast<TVolatileState*>(raft->GetVolatileState())->LeaderId = 1;

    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + 1);
    request->Epoch = 1;
    request->SessionIdLow = 1;
    request->Sequence = 1;
    request->Cookie = 4;
    request->Data[0] = 'x';
    auto forwarded = NewHoldedMessage<TSessionForwardedCommandRequest>(
        sizeof(TSessionForwardedCommandRequest) + request->Len);
    forwarded->SessionId = 9;
    forwarded->ForwardCookie = 10;
    forwarded->Flags = TCommandRequest::EWrite | TCommandRequest::EForwarded |
        TSessionForwardedCommandRequest::EIdempotent;
    forwarded->ClientCookie = request->Cookie;
    memcpy(forwarded->Data, request.Mes, request->Len);

    processor.OnSessionForwardedCommandRequest(forwarded, peer);

    assert_int_equal(state->LastLogIndex, 0);
    assert_int_equal(peer->Sent.size(), 1);
    const auto response = peer->Sent.back().Cast<TSessionForwardedCommandResponse>();
    assert_int_equal(response->SessionId, forwarded->SessionId);
    assert_int_equal(response->ForwardCookie, forwarded->ForwardCookie);
    assert_int_equal(response->ErrorCode, TCommandResponse::Failed);
}

void test_idempotent_request_without_idempotent_rsm_is_rejected(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1});
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TDummyRsm>();
    TRequestProcessor processor(raft, rsm, {});
    auto client = std::make_shared<TIdempotencyPeer>();

    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + 1);
    request->Epoch = 1;
    request->SessionIdLow = 1;
    request->Sequence = 1;
    request->Data[0] = 'x';
    processor.OnIdempotentCommandRequest(request, client);

    assert_int_equal(state->LastLogIndex, 0);
    assert_int_equal(client->Sent.size(), 1);
    const auto response = client->Sent.back().Cast<TIdempotentCommandResponse>();
    assert_int_equal(response->ErrorCode, TCommandResponse::Unsupported);
}

void test_idempotent_request_timeout_releases_reply_but_keeps_log_entry(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1});
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto rsm = std::make_shared<TSnapshotRsm>();
    auto timeSource = std::make_shared<TIdempotencyManualTimeSource>();
    TRequestProcessor::TAdmissionLimits limits;
    limits.RequestTimeout = std::chrono::milliseconds(5);
    TRequestProcessor processor(raft, rsm, {}, limits, timeSource);
    auto client = std::make_shared<TIdempotencyPeer>();
    raft->Become(EState::LEADER);

    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + 1);
    request->Epoch = 1;
    request->SessionIdLow = 1;
    request->Sequence = 1;
    request->Data[0] = 'x';
    processor.OnIdempotentCommandRequest(request, client);
    assert_int_equal(state->LastLogIndex, 1);
    assert_true(client->Sent.empty());

    timeSource->Value += std::chrono::milliseconds(6);
    processor.ProcessWaiting();
    assert_int_equal(client->Sent.size(), 1);
    const auto timeout = client->Sent.back().Cast<TIdempotentCommandResponse>();
    assert_int_equal(timeout->ErrorCode, TCommandResponse::TimedOut);

    const_cast<TVolatileState*>(raft->GetVolatileState())->CommitIndex = 1;
    processor.ProcessCommitted();
    assert_int_equal(rsm->LastAppliedIndex, 1);
    assert_int_equal(rsm->IdempotentWrites, 1);
    assert_int_equal(client->Sent.size(), 1);
}

void test_idempotent_log_requires_every_member_capability_including_joint_additions(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1, 2, 3});
    TNodeDict peers{
        {2, std::make_shared<TIdempotencyPeer>()},
        {3, std::make_shared<TIdempotencyPeer>()},
        {4, std::make_shared<TIdempotencyPeer>()},
    };
    auto raft = std::make_shared<TRaft>(state, 1, peers, std::vector<uint32_t>{4});
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, peers);
    raft->Become(EState::LEADER);

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->PeerCapabilities[2] = {
        .AppendAckSeqno = 1, .Term = state->CurrentTerm, .ApplicationSnapshotInstall = true,
        .IdempotentLogEntries = true, .IdempotencyControlEntries = true};
    volatileState->PeerCapabilities[3] = {
        .AppendAckSeqno = 1, .Term = state->CurrentTerm, .ApplicationSnapshotInstall = true,
        .IdempotentLogEntries = true, .IdempotencyControlEntries = true};
    assert_false(raft->CanAppendIdempotentLogEntries());

    auto readiness = raft->CheckJointConfigurationReadiness({1, 2, 4});
    assert_true(readiness.MembersWithoutIdempotentLogSupport == (std::vector<uint32_t>{4}));
    assert_true(readiness.MembersWithoutIdempotencyControlSupport == (std::vector<uint32_t>{4}));

    volatileState->PeerCapabilities[4] = {
        .AppendAckSeqno = 1, .Term = state->CurrentTerm, .ApplicationSnapshotInstall = true,
        .IdempotentLogEntries = true};
    assert_true(raft->CanAppendIdempotentLogEntries());
    assert_false(raft->CanAppendIdempotencyControlEntries());
    readiness = raft->CheckJointConfigurationReadiness({1, 2, 4});
    assert_true(readiness.MembersWithoutIdempotentLogSupport.empty());
    assert_true(readiness.MembersWithoutIdempotencyControlSupport == (std::vector<uint32_t>{4}));

    volatileState->PeerCapabilities[4].IdempotencyControlEntries = true;
    assert_true(raft->CanAppendIdempotencyControlEntries());
    readiness = raft->CheckJointConfigurationReadiness({1, 2, 4});
    assert_true(readiness.MembersWithoutIdempotencyControlSupport.empty());

    auto application = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    application->Term = state->CurrentTerm;
    application->Data[0] = 'x';
    TIdempotentOperationIdentity identity;
    identity.Epoch = 1;
    identity.SessionIdHigh = 2;
    identity.SessionIdLow = 3;
    identity.Sequence = 1;
    identity.PayloadDigest = ComputeIdempotentPayloadDigest("x");
    auto replicated = EncodeIdempotentLogEntry(identity, application);
    assert_int_equal(raft->Append(replicated), 1);
    assert_int_equal(state->Get(1)->Flags, TLogEntry::EIdempotent);

    TIdempotencyControlOperation control{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 1,
        .NewEpoch = 2,
    };
    auto controlApplication = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    controlApplication->Term = state->CurrentTerm;
    controlApplication->Data[0] = 'c';
    auto controlEntry = EncodeIdempotencyControlLogEntry(control, controlApplication);
    assert_int_equal(raft->Append(controlEntry), 2);
    assert_int_equal(state->Get(2)->Flags, TLogEntry::EIdempotencyControl);
}

void test_rsm_without_idempotency_rejects_tagged_append_entries(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    auto leader = std::make_shared<TIdempotencyPeer>();
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{{2, leader}});

    auto application = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    application->Term = 1;
    application->Data[0] = 'x';
    TIdempotentOperationIdentity identity;
    identity.Epoch = 1;
    identity.SessionIdHigh = 2;
    identity.SessionIdLow = 3;
    identity.Sequence = 1;
    identity.PayloadDigest = ComputeIdempotentPayloadDigest("x");
    auto replicated = EncodeIdempotentLogEntry(identity, application);

    auto request = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 1, .Seqno = 1},
        TAppendEntriesRequest{
            .PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 1, .LeaderId = 2, .Nentries = 1});
    request.InitPayload(1);
    request.Payload[0] = replicated;

    raft->Process(ITimeSource::Time{}, std::move(request));
    assert_int_equal(state->LastLogIndex, 0);
    assert_int_equal(leader->Sent.size(), 1);
    const auto response = leader->Sent.back().Cast<TAppendEntriesResponse>();
    assert_false(response->Success);
    assert_false(response->Padding & TAppendEntriesResponse::SupportsIdempotentLogEntries);
    assert_false(response->Padding & TAppendEntriesResponse::SupportsIdempotencyControlEntries);

    TIdempotencyControlOperation control{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 1,
        .NewEpoch = 2,
    };
    auto controlApplication = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    controlApplication->Term = 1;
    controlApplication->Data[0] = 'c';
    auto controlEntry = EncodeIdempotencyControlLogEntry(control, controlApplication);
    auto controlRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 1, .Seqno = 2},
        TAppendEntriesRequest{
            .PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 1, .LeaderId = 2, .Nentries = 1});
    controlRequest.InitPayload(1);
    controlRequest.Payload[0] = controlEntry;

    raft->Process(ITimeSource::Time{}, std::move(controlRequest));
    assert_int_equal(state->LastLogIndex, 0);
    assert_int_equal(leader->Sent.size(), 2);
    const auto controlResponse = leader->Sent.back().Cast<TAppendEntriesResponse>();
    assert_false(controlResponse->Success);
    assert_false(controlResponse->Padding & TAppendEntriesResponse::SupportsIdempotentLogEntries);
    assert_false(controlResponse->Padding & TAppendEntriesResponse::SupportsIdempotencyControlEntries);

    auto retainedState = std::make_shared<TState>(1, 0,
        std::vector<TMessageHolder<TLogEntry>>{controlEntry});
    auto retainedRaft = std::make_shared<TRaft>(retainedState, 1, TNodeDict{});
    auto unsupportedRsm = std::make_shared<TDummyRsm>();
    Reject([&] { TRequestProcessor processor(retainedRaft, unsupportedRsm, {}); });
}

void test_idempotency_control_capability_is_advertised_and_recorded(void**) {
    auto leaderPeer = std::make_shared<TIdempotencyPeer>();
    TNodeDict followerNodes{{1, leaderPeer}};
    auto followerState = std::make_shared<TState>();
    followerState->CurrentTerm = 1;
    followerState->BindVoters({1, 2});
    auto followerRaft = std::make_shared<TRaft>(followerState, 2, followerNodes);
    auto followerRsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor followerProcessor(followerRaft, followerRsm, followerNodes);

    auto request = NewHoldedMessage(
        TMessageEx{.Src = 1, .Dst = 2, .Term = 1, .Seqno = 1},
        TAppendEntriesRequest{
            .PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 0, .LeaderId = 1, .Nentries = 0});
    followerRaft->Process(ITimeSource::Time{}, std::move(request));
    assert_int_equal(leaderPeer->Sent.size(), 1);
    auto response = leaderPeer->Sent.back().Cast<TAppendEntriesResponse>();
    assert_true(response->Success);
    assert_true(response->Padding & TAppendEntriesResponse::SupportsIdempotentLogEntries);
    assert_true(response->Padding & TAppendEntriesResponse::SupportsIdempotencyControlEntries);

    auto followerPeer = std::make_shared<TIdempotencyPeer>();
    TNodeDict leaderNodes{{2, followerPeer}};
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1, 2});
    auto raft = std::make_shared<TRaft>(state, 1, leaderNodes);
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, leaderNodes);
    raft->Become(EState::LEADER);

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->LastSentSeqno[2] = response->Seqno;
    volatileState->PendingAppendRpcs[2].push_back({response->Seqno, 0, 0});
    raft->Process(ITimeSource::Time{}, response);
    assert_true(volatileState->PeerCapabilities.at(2).IdempotentLogEntries);
    assert_true(volatileState->PeerCapabilities.at(2).IdempotencyControlEntries);
    assert_true(raft->CanAppendIdempotencyControlEntries());
}

void test_idempotent_rsm_snapshot_hook_validates_before_atomic_restore(void**) {
    TSnapshotRsm source;
    auto request = NewHoldedMessage<TIdempotentCommandRequest>();
    assert_int_equal(source.IdempotentResponseSizeUpperBound(request), 64);
    source.ResponseBound = TIdempotencySnapshotLimits{}.MaxResponseBytes + 1;
    Reject([&] { (void)source.IdempotentResponseSizeUpperBound(request); });

    source.State = MakeSnapshot();
    source.LastAppliedIndex = source.State.LastAppliedIndex;
    const auto bytes = source.SaveSnapshot();
    source.ValidateSnapshot(bytes, source.LastAppliedIndex);

    TSnapshotRsm restored;
    restored.State = MakeSnapshot();
    restored.State.ApplicationData = "old";
    restored.LastAppliedIndex = 2;
    restored.LoadSnapshot(bytes, 7);
    assert_true(restored.State == source.State);
    assert_int_equal(restored.LastAppliedIndex, 7);

    const auto before = restored.State;
    Reject([&] { restored.LoadSnapshot(bytes, 8); });
    assert_true(restored.State == before);
    assert_int_equal(restored.LastAppliedIndex, 7);

    auto invalid = MakeSnapshot();
    invalid.ApplicationData = "invalid";
    const auto invalidBytes = EncodeIdempotencySnapshot(invalid);
    Reject([&] { restored.LoadSnapshot(invalidBytes, 7); });
    assert_true(restored.State == before);
    assert_int_equal(restored.LastAppliedIndex, 7);
}

void test_idempotent_retry_after_snapshot_recovery_and_leader_change_returns_cached_result(void**) {
    TIdempotencyCluster cluster(3);
    cluster.Elect(1);
    cluster.Replicate(1);
    assert_true(cluster.Rafts[0]->CanAppendIdempotentLogEntries());

    // Voter 2 misses the write. Voters 1 and 3 commit and apply it, but the
    // client loses the successful response and must retry the same identity.
    cluster.Isolated.insert(2);
    auto firstClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(IdempotentRequest(31), firstClient);
    cluster.Replicate(1);
    assert_int_equal(firstClient->Sent.size(), 1);
    auto firstResponse = firstClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(firstResponse);
    assert_int_equal(firstResponse->ErrorCode, TCommandResponse::Success);
    assert_true(std::string(firstResponse->Data,
        firstResponse->Len - sizeof(TIdempotentCommandResponse)) == "result:work");
    const auto originalIndex = firstResponse->Index;
    firstClient->Sent.clear(); // Simulate losing the reply after the write committed.
    assert_int_equal(cluster.Applications[0]->ApplicationMutations, 1);
    assert_int_equal(cluster.Applications[2]->ApplicationMutations, 1);

    const auto snapshot = cluster.Rafts[0]->Compact();
    assert_int_equal(snapshot.Index, originalIndex);
    cluster.Isolated.erase(2);
    cluster.Replicate(1);
    assert_int_equal(cluster.Rafts[1]->GetState()->SnapshotIndex, snapshot.Index);
    assert_int_equal(cluster.Applications[1]->LastAppliedIndex, snapshot.Index);
    assert_true(cluster.Applications[1]->State.ApplicationData == "work");
    assert_int_equal(cluster.Applications[1]->State.Sessions.size(), 1);

    // The snapshot restored the dedup record on voter 2. It wins a new term,
    // then receives the client's retry while the original leader remains live.
    cluster.Elect(2);
    cluster.Replicate(2);
    assert_true(cluster.Rafts[1]->CanAppendIdempotentLogEntries());
    auto retryClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[1]->OnIdempotentCommandRequest(IdempotentRequest(32), retryClient);
    cluster.Replicate(2);

    assert_int_equal(retryClient->Sent.size(), 1);
    auto retryResponse = retryClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(retryResponse);
    assert_int_equal(retryResponse->ErrorCode, TCommandResponse::Success);
    assert_int_equal(retryResponse->Cookie, 32);
    assert_int_equal(retryResponse->Index, originalIndex);
    assert_true(std::string(retryResponse->Data,
        retryResponse->Len - sizeof(TIdempotentCommandResponse)) == "result:work");
    assert_true(cluster.Applications[1]->State.ApplicationData == "work");
    assert_int_equal(cluster.Applications[1]->ApplicationMutations, 0);
    assert_int_equal(cluster.Applications[2]->ApplicationMutations, 1);
    assert_true(cluster.Applications[2]->State.ApplicationData == "work");
}

void test_duplicate_idempotent_entries_from_two_leaders_apply_once(void**) {
    TIdempotencyCluster cluster(3);
    cluster.Elect(1);
    cluster.Replicate(1);
    const auto formerLeaderTerm = cluster.Rafts[0]->GetState()->CurrentTerm;

    // Node 2 stores leader 1's operation, but its acknowledgement is lost and
    // node 3 is unavailable, so the original copy remains uncommitted.
    cluster.Isolated.insert(3);
    cluster.DroppedLinks.insert({2, 1});
    auto abandonedClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(IdempotentRequest(41), abandonedClient);
    cluster.Replicate(1);
    assert_true(abandonedClient->Sent.empty());
    assert_int_equal(cluster.Rafts[0]->GetState()->LastLogIndex, 2);
    assert_int_equal(cluster.Rafts[0]->GetState()->CommitIndex, 1);
    assert_int_equal(cluster.Rafts[1]->GetState()->LastLogIndex, 2);
    assert_int_equal(cluster.Rafts[1]->GetState()->CommitIndex, 1);
    assert_int_equal(cluster.Applications[0]->ApplicationMutations, 0);
    assert_int_equal(cluster.Applications[1]->ApplicationMutations, 0);
    cluster.Processors[0]->CleanUp(abandonedClient);

    // Node 2 wins a later term with node 3. The old leader steps down on the
    // new term's replication probe. Before that suffix reaches either follower,
    // retry the same operation so the new leader appends a second log copy.
    cluster.Isolated.erase(3);
    cluster.DroppedLinks.clear();
    cluster.DroppedAppendEntriesFrom.insert(2);
    cluster.Elect(2);
    cluster.DroppedAppendEntriesFrom.erase(2);
    assert_true(cluster.Rafts[1]->GetState()->CurrentTerm > formerLeaderTerm);
    assert_int_equal(cluster.Rafts[1]->GetState()->LastLogIndex, 3);
    assert_int_equal(cluster.Applications[1]->LastAppliedIndex, 1);
    // Election sends the first heartbeat immediately and then backs off for
    // half an election timeout. Force one deterministic probe round now; both
    // followers reject the new leader's term-3 stub boundary and advertise
    // current-term capabilities without receiving the uncommitted suffix.
    auto* leaderVolatile = const_cast<TVolatileState*>(cluster.Rafts[1]->GetVolatileState());
    for (uint32_t peer : {1, 3}) {
        leaderVolatile->HeartbeatDue[peer] = cluster.Clock->Value;
        leaderVolatile->RpcDue[peer] = cluster.Clock->Value;
    }
    cluster.Replicate(2, 1);
    assert_true(cluster.Rafts[1]->CanAppendIdempotentLogEntries());

    auto retryClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[1]->OnIdempotentCommandRequest(IdempotentRequest(42), retryClient);
    assert_int_equal(cluster.Rafts[1]->GetState()->LastLogIndex, 4);
    assert_int_equal(cluster.Rafts[1]->GetState()->CommitIndex, 1);
    cluster.Replicate(2);

    assert_int_equal(retryClient->Sent.size(), 1);
    auto response = retryClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    assert_int_equal(response->Cookie, 42);
    assert_int_equal(response->ErrorCode, TCommandResponse::Success);
    assert_int_equal(response->Index, 2);
    assert_true(std::string(response->Data,
        response->Len - sizeof(TIdempotentCommandResponse)) == "result:work");

    for (size_t node = 0; node < 3; ++node) {
        assert_int_equal(cluster.Rafts[node]->GetVolatileState()->CommitIndex, 4);
        assert_int_equal(cluster.Applications[node]->ApplicationMutations, 1);
        assert_true(cluster.Applications[node]->State.ApplicationData == "work");
        assert_int_equal(cluster.Applications[node]->State.Sessions.size(), 1);
        assert_int_equal(cluster.Applications[node]->State.Sessions.front().LogIndex, 2);
    }
}

void test_idempotent_sequence_reuse_with_different_payload_conflicts_without_mutation(void**) {
    TIdempotencyCluster cluster(3);
    cluster.Elect(1);
    cluster.Replicate(1);

    auto originalClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(IdempotentRequest(51), originalClient);
    cluster.Replicate(1);
    assert_int_equal(originalClient->Sent.size(), 1);
    const auto original = originalClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(original);
    assert_int_equal(original->ErrorCode, TCommandResponse::Success);
    assert_int_equal(original->Index, 2);
    assert_int_equal(cluster.Applications[0]->ApplicationMutations, 1);
    assert_true(cluster.Applications[0]->State.ApplicationData == "work");

    // Keep the session and sequence fixed but change the payload digest. The
    // conflicting operation commits as a Raft entry but cannot change app or
    // dedup state, and the client receives an explicit conflict.
    assert_true(ComputeIdempotentPayloadDigest("work") !=
        ComputeIdempotentPayloadDigest("changed"));
    auto conflictingClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(
        IdempotentRequest(52, "changed"), conflictingClient);
    cluster.Replicate(1);

    assert_int_equal(conflictingClient->Sent.size(), 1);
    const auto conflict = conflictingClient->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(conflict);
    assert_int_equal(conflict->Cookie, 52);
    assert_int_equal(conflict->ErrorCode, TCommandResponse::IdempotencyConflict);
    assert_int_equal(conflict->Index, 3);
    for (size_t node = 0; node < 3; ++node) {
        const auto& application = cluster.Applications[node];
        assert_int_equal(cluster.Rafts[node]->GetVolatileState()->CommitIndex, 3);
        assert_int_equal(application->ApplicationMutations, 1);
        assert_true(application->State.ApplicationData == "work");
        assert_int_equal(application->State.Sessions.size(), 1);
        assert_int_equal(application->State.Sessions.front().HighestSequence, 1);
        assert_int_equal(application->State.Sessions.front().LogIndex, 2);
        assert_true(application->State.Sessions.front().PayloadDigest ==
            ComputeIdempotentPayloadDigest("work"));
        assert_int_equal(application->LastAppliedIndex, 3);
    }
}

void test_idempotent_stale_and_gap_sequences_reject_before_append(void**) {
    TIdempotencyCluster cluster(3);
    cluster.Elect(1);
    cluster.Replicate(1);

    const auto submit = [](uint32_t cookie, uint64_t sequence,
                            std::string_view payload = "work") {
        auto request = IdempotentRequest(cookie, payload);
        request->Sequence = sequence;
        return request;
    };
    auto firstClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(submit(61, 1), firstClient);
    cluster.Replicate(1);
    auto secondClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(submit(62, 2, "next"), secondClient);
    cluster.Replicate(1);

    const auto& application = *cluster.Applications[0];
    assert_int_equal(application.State.Sessions.size(), 1);
    assert_int_equal(application.State.Sessions.front().HighestSequence, 2);
    assert_int_equal(application.ApplicationMutations, 2);
    assert_true(application.State.ApplicationData == "work|next");
    const auto committedLogIndex = cluster.Rafts[0]->GetState()->LastLogIndex;
    const auto savedSession = application.State.Sessions.front();

    const auto rejectWithoutAppend = [&](uint32_t cookie, uint64_t sequence) {
        auto client = std::make_shared<TIdempotencyPeer>();
        bool rejected = false;
        try {
            cluster.Processors[0]->OnIdempotentCommandRequest(submit(cookie, sequence), client);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        assert_true(rejected);
        assert_true(client->Sent.empty());
        assert_int_equal(cluster.Rafts[0]->GetState()->LastLogIndex, committedLogIndex);
        assert_int_equal(application.ApplicationMutations, 2);
        assert_true(application.State.ApplicationData == "work|next");
        assert_int_equal(application.State.Sessions.front().HighestSequence, 2);
        assert_int_equal(application.State.Sessions.front().LogIndex, savedSession.LogIndex);
        assert_true(application.State.Sessions.front().PayloadDigest == savedSession.PayloadDigest);
        assert_true(application.State.Sessions.front().ResponseData == savedSession.ResponseData);
    };
    rejectWithoutAppend(63, 1); // Below the session high-water mark.
    rejectWithoutAppend(64, 4); // Skips sequence 3.

    auto newSessionGap = submit(65, 2, "new-session-gap");
    newSessionGap->SessionIdHigh = 21;
    newSessionGap->SessionIdLow = 22;
    auto client = std::make_shared<TIdempotencyPeer>();
    bool rejected = false;
    try {
        cluster.Processors[0]->OnIdempotentCommandRequest(newSessionGap, client);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert_true(rejected);
    assert_true(client->Sent.empty());
    assert_int_equal(cluster.Rafts[0]->GetState()->LastLogIndex, committedLogIndex);
    assert_int_equal(application.State.Sessions.size(), 1);
    assert_int_equal(application.ApplicationMutations, 2);
}

void test_idempotent_request_rejects_unsupported_peer_before_append(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1, 2});
    TNodeDict peers{{2, std::make_shared<TIdempotencyPeer>()}};
    auto raft = std::make_shared<TRaft>(state, 1, peers);
    auto rsm = std::make_shared<TSnapshotRsm>();
    TRequestProcessor processor(raft, rsm, peers);
    raft->Become(EState::LEADER);

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->PeerCapabilities[2] = {
        .AppendAckSeqno = 1, .Term = state->CurrentTerm, .IdempotentLogEntries = false};
    assert_false(raft->CanAppendIdempotentLogEntries());

    auto client = std::make_shared<TIdempotencyPeer>();
    processor.OnIdempotentCommandRequest(IdempotentRequest(73), client);

    assert_int_equal(state->LastLogIndex, 0);
    assert_int_equal(rsm->IdempotentWrites, 0);
    assert_int_equal(client->Sent.size(), 1);
    const auto response = client->Sent.back().Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    assert_int_equal(response->Cookie, 73);
    assert_int_equal(response->ErrorCode, TCommandResponse::Unsupported);
    assert_int_equal(response->Index, 0);
}

void test_idempotent_new_session_rejects_full_cache_before_append(void**) {
    auto limits = TIdempotencySnapshotLimits{};
    limits.MaxActiveSessions = 1;
    TIdempotencyCluster cluster(3, limits);
    cluster.Elect(1);
    cluster.Replicate(1);

    auto firstClient = std::make_shared<TIdempotencyPeer>();
    cluster.Processors[0]->OnIdempotentCommandRequest(IdempotentRequest(74), firstClient);
    cluster.Replicate(1);
    assert_int_equal(cluster.Applications[0]->State.Sessions.size(), 1);
    assert_int_equal(cluster.Applications[0]->ApplicationMutations, 1);
    const auto committedLogIndex = cluster.Rafts[0]->GetState()->LastLogIndex;

    auto newSession = IdempotentRequest(75, "second-session");
    newSession->SessionIdHigh = 21;
    newSession->SessionIdLow = 22;
    auto client = std::make_shared<TIdempotencyPeer>();
    bool rejected = false;
    try {
        cluster.Processors[0]->OnIdempotentCommandRequest(newSession, client);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    assert_true(rejected);
    assert_true(client->Sent.empty());
    for (size_t node = 0; node < 3; ++node) {
        assert_int_equal(cluster.Rafts[node]->GetState()->LastLogIndex, committedLogIndex);
        assert_int_equal(cluster.Applications[node]->State.Sessions.size(), 1);
        assert_int_equal(cluster.Applications[node]->ApplicationMutations, 1);
        assert_true(cluster.Applications[node]->State.ApplicationData == "work");
    }
}
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_idempotency_snapshot_roundtrip_preserves_dedup_state),
        cmocka_unit_test(test_idempotent_payload_digest_matches_sha256_vectors),
        cmocka_unit_test(test_idempotent_log_record_roundtrip_and_rejects_corruption),
        cmocka_unit_test(test_idempotency_control_log_record_roundtrip_and_rejects_corruption),
        cmocka_unit_test(test_configuration_history_accepts_valid_idempotency_control_entries),
        cmocka_unit_test(test_idempotency_control_is_proposed_and_atomically_applied_after_commit),
        cmocka_unit_test(test_committed_idempotent_log_replay_uses_identity_and_application_entry),
        cmocka_unit_test(test_idempotent_request_is_proposed_and_replied_after_application),
        cmocka_unit_test(test_idempotent_request_forwards_through_sessionized_route),
        cmocka_unit_test(test_forwarded_idempotent_request_is_not_forwarded_again_by_follower),
        cmocka_unit_test(test_idempotent_request_without_idempotent_rsm_is_rejected),
        cmocka_unit_test(test_idempotent_request_timeout_releases_reply_but_keeps_log_entry),
        cmocka_unit_test(test_idempotent_log_requires_every_member_capability_including_joint_additions),
        cmocka_unit_test(test_rsm_without_idempotency_rejects_tagged_append_entries),
        cmocka_unit_test(test_idempotency_control_capability_is_advertised_and_recorded),
        cmocka_unit_test(test_idempotent_rsm_snapshot_hook_validates_before_atomic_restore),
        cmocka_unit_test(test_idempotent_retry_after_snapshot_recovery_and_leader_change_returns_cached_result),
        cmocka_unit_test(test_duplicate_idempotent_entries_from_two_leaders_apply_once),
        cmocka_unit_test(test_idempotent_sequence_reuse_with_different_payload_conflicts_without_mutation),
        cmocka_unit_test(test_idempotent_stale_and_gap_sequences_reject_before_append),
        cmocka_unit_test(test_idempotent_request_rejects_unsupported_peer_before_append),
        cmocka_unit_test(test_idempotent_new_session_rejects_full_cache_before_append),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
