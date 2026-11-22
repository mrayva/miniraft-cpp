#include "../examples/idempotent_kv_rsm.h"
#include "../examples/kv_messages.h"

#include <stdexcept>
#include <string>
#include <cstring>
#include <cmocka.h>

namespace {
template<class F> void Reject(F&& f) {
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}

TMessageHolder<TIdempotentCommandRequest> Put(uint64_t epoch, uint64_t session,
        uint64_t sequence, std::string_view key, std::string_view value) {
    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + sizeof(TKvEntry) + key.size() + value.size());
    request->Epoch = epoch;
    request->SessionIdLow = session;
    request->Sequence = sequence;
    const auto keySize = static_cast<uint16_t>(key.size());
    const auto valueSize = static_cast<uint16_t>(value.size());
    std::memcpy(request->Data, &keySize, sizeof(keySize));
    std::memcpy(request->Data + sizeof(keySize), &valueSize, sizeof(valueSize));
    if (!key.empty()) std::memcpy(request->Data + sizeof(TKvEntry), key.data(), key.size());
    if (!value.empty()) std::memcpy(request->Data + sizeof(TKvEntry) + key.size(),
        value.data(), value.size());
    return request;
}

TIdempotentOperationIdentity Identity(const TMessageHolder<TIdempotentCommandRequest>& request) {
    return {.Epoch = request->Epoch,
        .SessionIdHigh = request->SessionIdHigh,
        .SessionIdLow = request->SessionIdLow,
        .Sequence = request->Sequence,
        .PayloadDigest = ComputeIdempotentPayloadDigest(std::string_view(
            request->Data, request->Len - sizeof(TIdempotentCommandRequest)))};
}

TMessageHolder<TLogEntry> EncodeWrite(TIdempotentKvRsm& rsm,
        TMessageHolder<TIdempotentCommandRequest> request) {
    auto applicationEntry = rsm.PrepareIdempotent(request);
    applicationEntry->Term = 1;
    return EncodeIdempotentLogEntry(Identity(request), std::move(applicationEntry));
}

std::string Read(TIdempotentKvRsm& rsm, std::string_view key, uint64_t index) {
    auto request = NewHoldedMessage<TReadKv>(sizeof(TReadKv) + key.size());
    request->KeySize = static_cast<uint16_t>(key.size());
    if (!key.empty()) std::memcpy(request->Data, key.data(), key.size());
    auto response = rsm.Read(request, index).Cast<TCommandResponse>();
    return {response->Data, response->Len - sizeof(TCommandResponse)};
}
}

void test_idempotent_kv_application_snapshot_and_session_controls(void**) {
    TIdempotencySnapshotLimits limits;
    limits.MaxActiveSessions = 2;
    limits.MaxClosedSessions = 1;
    TIdempotentKvRsm source(limits);
    auto first = Put(1, 7, 1, "key", "one");
    source.ValidateIdempotent(first);
    assert_int_equal(source.CachedResponseDataUpperBound(first), 3);
    auto firstResult = source.ApplyIdempotentLogRecord(EncodeWrite(source, first), 1);
    assert_int_equal(firstResult.ErrorCode, TCommandResponse::Success);
    assert_int_equal(firstResult.Index, 1);
    assert_true(firstResult.Data == "one");
    assert_true(Read(source, "key", 1) == "one");

    const auto snapshot = source.SaveSnapshot();
    TIdempotentKvRsm restored(limits);
    restored.LoadSnapshot(snapshot, 1);
    auto retry = restored.ApplyIdempotentLogRecord(EncodeWrite(restored, first), 2);
    assert_int_equal(retry.ErrorCode, TCommandResponse::Success);
    assert_int_equal(retry.Index, 1);
    assert_true(retry.Data == "one");
    assert_int_equal(restored.LastAppliedIndex, 2);
    assert_true(Read(restored, "key", 2) == "one");

    auto second = Put(1, 7, 2, "key", "two");
    auto secondResult = restored.ApplyIdempotentLogRecord(EncodeWrite(restored, second), 3);
    assert_int_equal(secondResult.ErrorCode, TCommandResponse::Success);
    assert_int_equal(secondResult.Index, 3);
    assert_true(Read(restored, "key", 3) == "two");

    TIdempotencyControlOperation close{
        .Kind = EIdempotencyControlKind::CloseSession,
        .Epoch = 1,
        .SessionIdLow = 7,
    };
    restored.ValidateIdempotencyControl(close);
    auto closeEntry = restored.PrepareIdempotencyControl(close);
    closeEntry->Term = 1;
    auto decodedClose = DecodeIdempotencyControlLogEntry(
        EncodeIdempotencyControlLogEntry(close, std::move(closeEntry)));
    restored.ApplyIdempotencyControl(close, std::move(decodedClose.ApplicationEntry), 4);
    Reject([&] { restored.ValidateIdempotent(Put(1, 7, 3, "key", "three")); });

    // Closed-session and active-session capacity are independent. A full
    // tombstone table must not block a different session from starting.
    auto otherSession = Put(1, 8, 1, "other", "active");
    restored.ValidateIdempotent(otherSession);
    auto otherResult = restored.ApplyIdempotentLogRecord(
        EncodeWrite(restored, otherSession), 5);
    assert_int_equal(otherResult.ErrorCode, TCommandResponse::Success);

    const auto closedSnapshot = restored.SaveSnapshot();
    TIdempotentKvRsm recovered(limits);
    recovered.LoadSnapshot(closedSnapshot, 5);
    Reject([&] { recovered.ValidateIdempotent(Put(1, 7, 3, "key", "three")); });

    auto before = recovered.SaveSnapshot();
    auto invalid = recovered.CaptureIdempotencySnapshot();
    invalid.ApplicationData = "not a KV snapshot";
    const auto invalidBytes = EncodeIdempotencySnapshot(invalid);
    Reject([&] { recovered.LoadSnapshot(invalidBytes, 5); });
    assert_true(recovered.SaveSnapshot() == before);
    assert_true(Read(recovered, "key", 5) == "two");

    TIdempotencyControlOperation advance{
        .Kind = EIdempotencyControlKind::AdvanceEpoch,
        .Epoch = 1,
        .NewEpoch = 2,
    };
    recovered.ValidateIdempotencyControl(advance);
    auto advanceEntry = recovered.PrepareIdempotencyControl(advance);
    advanceEntry->Term = 1;
    auto decodedAdvance = DecodeIdempotencyControlLogEntry(
        EncodeIdempotencyControlLogEntry(advance, std::move(advanceEntry)));
    recovered.ApplyIdempotencyControl(advance, std::move(decodedAdvance.ApplicationEntry), 6);
    auto newEpochWrite = Put(2, 8, 1, "next", "epoch");
    auto epochResult = recovered.ApplyIdempotentLogRecord(
        EncodeWrite(recovered, newEpochWrite), 7);
    assert_int_equal(epochResult.ErrorCode, TCommandResponse::Success);
    assert_true(Read(recovered, "next", 7) == "epoch");
    auto lowerSessionId = Put(2, 3, 1, "sorted", "record");
    auto lowerResult = recovered.ApplyIdempotentLogRecord(
        EncodeWrite(recovered, lowerSessionId), 8);
    assert_int_equal(lowerResult.ErrorCode, TCommandResponse::Success);
    assert_true(!recovered.SaveSnapshot().empty());
}

void test_idempotent_kv_rejects_aggregate_response_cache_overflow(void**) {
    TIdempotencySnapshotLimits limits;
    limits.MaxActiveSessions = 2;
    limits.MaxCachedResponseBytes = 3;
    TIdempotentKvRsm rsm(limits);
    auto first = Put(1, 1, 1, "one", "abc");
    rsm.ValidateIdempotent(first);
    auto applied = rsm.ApplyIdempotentLogRecord(EncodeWrite(rsm, first), 1);
    assert_int_equal(applied.ErrorCode, TCommandResponse::Success);
    const auto before = rsm.SaveSnapshot();

    auto overLimit = Put(1, 2, 1, "two", "d");
    Reject([&] { rsm.ValidateIdempotent(overLimit); });
    assert_true(rsm.SaveSnapshot() == before);
    assert_true(Read(rsm, "one", 1) == "abc");
}

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_idempotent_kv_application_snapshot_and_session_controls),
        cmocka_unit_test(test_idempotent_kv_rejects_aggregate_response_cache_overflow),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
