#include <miniraft/raft.h>
#include <miniraft/snapshot_codec.h>
#include <functional>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
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
    TIdempotencySnapshot State;
    uint64_t ResponseBound = 64;
    size_t IdempotentWrites = 0;
    TIdempotentOperationIdentity LastIdentity;
    std::string LastAppliedCommand;

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
            TMessageHolder<TIdempotentCommandRequest>) override {
        return NewHoldedMessage<TLogEntry>();
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
    void ValidateIdempotencyControl(const TIdempotencyControlOperation&) const override { }
    TMessageHolder<TLogEntry> PrepareIdempotencyControl(
            const TIdempotencyControlOperation&) override {
        return NewHoldedMessage<TLogEntry>();
    }
    void ApplyIdempotencyControl(const TIdempotencyControlOperation&, uint64_t) override { }
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
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_idempotency_snapshot_roundtrip_preserves_dedup_state),
        cmocka_unit_test(test_idempotent_payload_digest_matches_sha256_vectors),
        cmocka_unit_test(test_idempotent_log_record_roundtrip_and_rejects_corruption),
        cmocka_unit_test(test_committed_idempotent_log_replay_uses_identity_and_application_entry),
        cmocka_unit_test(test_idempotent_rsm_snapshot_hook_validates_before_atomic_restore),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
