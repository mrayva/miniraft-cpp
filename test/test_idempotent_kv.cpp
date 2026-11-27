#include "../examples/idempotent_kv_rsm.h"
#include "../examples/kv_messages.h"

#include <stdexcept>
#include <string>
#include <cstring>
#include <cmocka.h>
#include <cstdlib>
#include <filesystem>
#ifdef __linux__
#include <unistd.h>
#endif

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

struct TStringSnapshotDataSource final : TSnapshotDataSource {
    std::string Bytes;
    size_t MaxReadBytes;
    mutable uint64_t BytesRead = 0;
    explicit TStringSnapshotDataSource(std::string bytes, size_t maxReadBytes = 5)
        : Bytes(std::move(bytes)), MaxReadBytes(maxReadBytes) { }
    uint64_t Size() const override { return Bytes.size(); }
    size_t Read(uint64_t offset, std::span<char> destination) const override {
        if (offset >= Bytes.size() || destination.empty()) return 0;
        const auto count = std::min({destination.size(), MaxReadBytes,
            Bytes.size() - size_t(offset)});
        std::memcpy(destination.data(), Bytes.data() + offset, count);
        BytesRead += count;
        return count;
    }
};

struct TEndpoint final : INode {
    std::vector<TMessageHolder<TMessage>> Messages;
    void Send(TMessageHolder<TMessage> message) override { Messages.push_back(std::move(message)); }
    void Drain() override { }
};
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

void test_idempotent_snapshot_source_install_avoids_wrapper_materialization(void**) {
    TIdempotentKvRsm source;
    auto first = Put(1, 7, 1, "key", "one");
    source.ApplyIdempotentLogRecord(EncodeWrite(source, first), 1);
    auto second = Put(1, 7, 2, "key", "two");
    source.ApplyIdempotentLogRecord(EncodeWrite(source, second), 2);
    const auto encoded = source.SaveSnapshot();

    TIdempotentKvRsm target;
    auto stale = Put(1, 8, 1, "stale", "old");
    target.ApplyIdempotentLogRecord(EncodeWrite(target, stale), 1);
    TSnapshot snapshot;
    snapshot.Index = 2;
    auto sourceBytes = std::make_shared<TStringSnapshotDataSource>(encoded);
    snapshot.DataSource = sourceBytes;

    auto installation = PrepareApplicationSnapshotInstallation(target, snapshot);
    assert_true(bool(installation));
    assert_int_equal(sourceBytes->BytesRead, encoded.size());
    assert_int_equal(target.LastAppliedIndex, 1);
    assert_true(Read(target, "stale", 1) == "old");
    assert_true(Read(target, "key", 1).empty());

    auto corruptBytes = encoded;
    corruptBytes.back() ^= 1;
    TSnapshot corruptSnapshot;
    corruptSnapshot.Index = 2;
    corruptSnapshot.DataSource = std::make_shared<TStringSnapshotDataSource>(std::move(corruptBytes));
    Reject([&] { PrepareApplicationSnapshotInstallation(target, corruptSnapshot); });
    assert_int_equal(target.LastAppliedIndex, 1);
    assert_true(Read(target, "stale", 1) == "old");

    InstallApplicationSnapshot(target, snapshot, std::move(installation));
    assert_int_equal(target.LastAppliedIndex, 2);
    assert_true(Read(target, "key", 2) == "two");
    assert_true(Read(target, "stale", 2).empty());
    const auto retry = target.ApplyIdempotentLogRecord(EncodeWrite(target, second), 3);
    assert_int_equal(retry.ErrorCode, TCommandResponse::Success);
    assert_int_equal(retry.Index, 2);
    assert_true(retry.Data == "two");
    assert_int_equal(target.LastAppliedIndex, 3);
    assert_true(Read(target, "key", 3) == "two");
}

void test_disk_staged_idempotent_install_uses_source_decoder(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-idempotent-install-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto operation = Put(1, 7, 1, "installed", "value");
    TIdempotentKvRsm source;
    source.ApplyIdempotentLogRecord(EncodeWrite(source, operation), 1);
    const auto image = EncodeSnapshot(TSnapshot{1, 1, {1, 3}, source.SaveSnapshot()});
    assert_true(image.size() <= TInstallSnapshotRequest::MaxChunkBytes);

    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(image.size());
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    limits.SnapshotStagingNamespace = "idempotent-test";
    auto peer = std::make_shared<TEndpoint>();
    TNodeDict peers{{1, peer}};
    auto application = std::make_shared<TIdempotentKvRsm>();
    auto state = std::make_shared<TState>();
    {
        auto raft = std::make_shared<TRaft>(state, 3, peers, std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, application, peers);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + image.size());
        request->Src = 1;
        request->Dst = 3;
        request->Term = 1;
        request->Seqno = 1;
        request->SnapshotIndex = 1;
        request->SnapshotTerm = 1;
        request->TotalBytes = image.size();
        request->LeaderId = 1;
        request->Done = 1;
        std::memcpy(request->Data, image.data(), image.size());
        raft->Process(ITimeSource::Time{}, request);

        assert_int_equal(peer->Messages.size(), 1);
        const auto response = peer->Messages.back().Cast<TInstallSnapshotResponse>();
        assert_true(response->Success);
        assert_true(response->Installed);
        assert_int_equal(application->LastAppliedIndex, 1);
        assert_true(Read(*application, "installed", 1) == "value");
        const auto installedSnapshot = state->GetSnapshot();
        assert_non_null(installedSnapshot.get());
        assert_true(bool(installedSnapshot->DataSource));
        assert_true(installedSnapshot->Data.empty());
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), image.size());
    }
    state.reset();
    assert_int_equal(heapBudget->UsedBytes(), 0);
    assert_int_equal(stagingBudget->UsedBytes(), 0);
#endif
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

void test_idempotent_snapshot_capture_uses_staging_budget(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-idempotent-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    auto application = std::make_shared<TIdempotentKvRsm>();
    auto initial = application->CaptureIdempotencySnapshot();
    initial.LastAppliedIndex = 1;
    application->RestoreIdempotencySnapshot(std::move(initial), 1);
    const auto expectedImage = application->SaveSnapshot();

    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(1024);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    limits.SnapshotStagingNamespace = "idempotent-test";
    std::shared_ptr<const TSnapshot> snapshot;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, application, {});
        snapshot = raft->CompactShared();
        assert_true(snapshot->Data.empty());
        assert_true(bool(snapshot->DataSource));
        assert_true(snapshot->MaterializeData() == expectedImage);
        assert_true(DecodeIdempotencySnapshot(snapshot->MaterializeData()) ==
            application->CaptureIdempotencySnapshot());
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), expectedImage.size());
    }
    state.reset();
    assert_int_equal(stagingBudget->UsedBytes(), expectedImage.size());
    snapshot.reset();
    assert_int_equal(stagingBudget->UsedBytes(), 0);

    auto rejectedState = std::make_shared<TState>();
    rejectedState->CurrentTerm = 1;
    auto rejectedEntry = NewHoldedMessage<TLogEntry>();
    rejectedEntry->Term = 1;
    rejectedState->Append(rejectedEntry);
    rejectedState->CommitIndex = 1;
    auto rejectedHeapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto rejectedStagingBudget = std::make_shared<TSnapshotInstallBudget>(expectedImage.size() - 1);
    limits.SnapshotInstallBudget = rejectedHeapBudget;
    limits.SnapshotStagingBudget = rejectedStagingBudget;
    {
        auto raft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, application, {});
        Reject([&] { raft->Compact(); });
    }
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectedHeapBudget->UsedBytes(), 0);
    assert_int_equal(rejectedStagingBudget->UsedBytes(), 0);
#endif
}

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_idempotent_kv_application_snapshot_and_session_controls),
        cmocka_unit_test(test_idempotent_snapshot_source_install_avoids_wrapper_materialization),
        cmocka_unit_test(test_disk_staged_idempotent_install_uses_source_decoder),
        cmocka_unit_test(test_idempotent_kv_rejects_aggregate_response_cache_overflow),
        cmocka_unit_test(test_idempotent_snapshot_capture_uses_staging_budget),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
