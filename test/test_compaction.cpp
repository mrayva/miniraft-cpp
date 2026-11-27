#include <miniraft/raft.h>
#include <miniraft/durable.h>
#include <miniraft/persist.h>
#include <miniraft/snapshot_codec.h>
#include "../examples/kv.h"
#include "../examples/kv_messages.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <deque>
#include <functional>
#include <set>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#ifdef __linux__
#include <fcntl.h>
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
struct TEndpoint : INode, ISnapshotTransferResumeNode {
    std::function<void(TMessageHolder<TMessage>)> SendTo;
    bool ManifestTransfer = false, TransferResume = false;
    explicit TEndpoint(std::function<void(TMessageHolder<TMessage>)> send,
            bool manifestTransfer = false, bool transferResume = false)
        : SendTo(std::move(send)), ManifestTransfer(manifestTransfer), TransferResume(transferResume) {}
    void Send(TMessageHolder<TMessage> message) override { SendTo(std::move(message)); }
    void Drain() override {}
    bool SupportsSnapshotTransferManifest() const override { return ManifestTransfer; }
    bool SupportsSnapshotTransferResume() const override { return TransferResume; }
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
struct TStreamingInstallApplication : TApplication, IStreamingSnapshotInstallRsm {
    struct TInstallation : IStagedSnapshotInstallation {
        TStreamingInstallApplication& Application;
        std::string Bytes;
        uint64_t Index;
        TInstallation(TStreamingInstallApplication& application, std::string bytes, uint64_t index)
            : Application(application), Bytes(std::move(bytes)), Index(index) { }
        void Install() override { Application.LoadSnapshot(Bytes, Index); }
    };
    size_t StreamingPreparations = 0;
    std::string ExpectedImage;

    std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
            const TSnapshotDataSource& source, uint64_t index) override {
        ++StreamingPreparations;
        if (source.Size() != ExpectedImage.size())
            throw std::runtime_error("Unexpected streamed snapshot payload size");
        std::string bytes(size_t(source.Size()), '\0');
        for (size_t offset = 0; offset < bytes.size();) {
            const auto count = std::min<size_t>(113, bytes.size() - offset);
            size_t copied = 0;
            while (copied < count) {
                const auto read = source.Read(offset + copied,
                    std::span<char>(bytes.data() + offset + copied, count - copied));
                if (!read || read > count - copied)
                    throw std::runtime_error("Truncated streamed snapshot payload");
                copied += read;
            }
            offset += count;
        }
        if (bytes != ExpectedImage) throw std::runtime_error("Streamed snapshot payload changed");
        ValidateSnapshot(bytes, index);
        return std::make_unique<TInstallation>(*this, std::move(bytes), index);
    }
};
struct TLargeStreamingInstallApplication : TApplication, IStreamingSnapshotInstallRsm {
    struct TInstallation final : IStagedSnapshotInstallation {
        TLargeStreamingInstallApplication& Application;
        uint64_t Index;
        TInstallation(TLargeStreamingInstallApplication& application, uint64_t index)
            : Application(application), Index(index) { }
        void Install() override { Application.LastAppliedIndex = Index; }
    };
    uint64_t ExpectedBytes = 0;
    size_t Preparations = 0;
    std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
            const TSnapshotDataSource& source, uint64_t index) override {
        ++Preparations;
        if (source.Size() != ExpectedBytes || !source.Size())
            throw std::runtime_error("Unexpected large streamed snapshot size");
        std::array<char, 1> first{}, last{};
        if (source.Read(0, first) != 1 || source.Read(source.Size() - 1, last) != 1 ||
            first[0] != 's' || last[0] != 's')
            throw std::runtime_error("Large streamed snapshot source failed range validation");
        return std::make_unique<TInstallation>(*this, index);
    }
};
struct TCollectingSnapshotSink : ISnapshotSink {
    std::string Data;
    void Write(std::span<const char> bytes) override {
        if (!bytes.empty()) Data.append(bytes.data(), bytes.size());
    }
};
struct TBoundedSnapshotApplication : IRsm {
    mutable size_t SaveCalls = 0;
    uint64_t Bound = 80;
    size_t PayloadBytes = 40;
    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest>, uint64_t) override { return {}; }
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry>, uint64_t) override { return {}; }
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest>) override { return {}; }
    std::optional<uint64_t> SnapshotSizeUpperBound() const override { return Bound; }
    std::string SaveSnapshot() const override { ++SaveCalls; return std::string(PayloadBytes, 's'); }
    void ValidateSnapshot(std::string_view, uint64_t) const override { }
    void LoadSnapshot(std::string_view, uint64_t index) override { LastAppliedIndex = index; }
    bool SupportsApplicationSnapshotInstall() const override { return true; }
};
struct THeapBudgetObservingSnapshotApplication : TBoundedSnapshotApplication {
    std::shared_ptr<TSnapshotInstallBudget> Budget;
    mutable uint64_t ValidationHeapBytes = 0;
    uint64_t LoadHeapBytes = 0;
    void ValidateSnapshot(std::string_view, uint64_t) const override {
        ValidationHeapBytes = Budget->UsedBytes();
    }
    void LoadSnapshot(std::string_view, uint64_t index) override {
        LoadHeapBytes = Budget->UsedBytes();
        LastAppliedIndex = index;
    }
};
struct TUnboundedSnapshotApplication : TBoundedSnapshotApplication {
    std::optional<uint64_t> SnapshotSizeUpperBound() const override { return std::nullopt; }
};
struct TBudgetObservingUnboundedSnapshotApplication : TUnboundedSnapshotApplication {
    std::shared_ptr<TSnapshotInstallBudget> Budget;
    mutable uint64_t BudgetBytesDuringSave = 0;
    std::string SaveSnapshot() const override {
        ++SaveCalls;
        BudgetBytesDuringSave = Budget->UsedBytes();
        return std::string(PayloadBytes, 's');
    }
};
struct TStreamingWriterSnapshotApplication : TApplication {
    mutable size_t SaveCalls = 0;
    mutable size_t WriteCalls = 0;
    std::string Payload = std::string(40, 'w');
    bool IgnoreSinkFailures = false;
    std::optional<uint64_t> SnapshotSizeUpperBound() const override { return std::nullopt; }
    std::string SaveSnapshot() const override {
        ++SaveCalls;
        throw std::logic_error("Incremental capture must not call SaveSnapshot");
    }
    void WriteSnapshot(uint64_t index, ISnapshotSink& sink) const override {
        if (index != LastAppliedIndex) throw std::logic_error("Snapshot writer index mismatch");
        ++WriteCalls;
        constexpr size_t ChunkBytes = 10;
        for (size_t offset = 0; offset < Payload.size(); offset += ChunkBytes) {
            const auto count = std::min(ChunkBytes, Payload.size() - offset);
            try {
                sink.Write(std::span<const char>(Payload.data() + offset, count));
            } catch (...) {
                if (!IgnoreSinkFailures) throw;
            }
        }
    }
    void ValidateSnapshot(std::string_view, uint64_t) const override { }
};
struct TStreamingWriterInstallApplication : TStreamingWriterSnapshotApplication,
        IStreamingSnapshotInstallRsm {
    struct TNoopInstallation : IStagedSnapshotInstallation {
        void Install() override { }
    };
    size_t RangePreparations = 0;
    std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
            const TSnapshotDataSource& source, uint64_t index) override {
        if (index != LastAppliedIndex || source.Size() != Payload.size())
            throw std::runtime_error("Unexpected staged capture image");
        ++RangePreparations;
        std::array<char, 7> buffer{};
        for (uint64_t offset = 0; offset < source.Size();) {
            const auto count = size_t(std::min<uint64_t>(buffer.size(), source.Size() - offset));
            size_t copied = 0;
            while (copied < count) {
                const auto read = source.Read(offset + copied,
                    std::span<char>(buffer.data() + copied, count - copied));
                if (!read || read > count - copied)
                    throw std::runtime_error("Truncated staged capture image");
                copied += read;
            }
            if (memcmp(buffer.data(), Payload.data() + offset, count) != 0)
                throw std::runtime_error("Staged capture image changed");
            offset += count;
        }
        return std::make_unique<TNoopInstallation>();
    }
};
struct TApplicationGenerationCatalog {
    std::filesystem::path Directory;
    std::unordered_map<std::string, std::string> Images;
    std::string ActiveId;
    std::string LastPreparedId;
    std::vector<std::string> Events;
    uint64_t NextId = 0;
    bool RaftManifestSynced = false;
};
struct TGenerationAwareSnapshotApplication : TStreamingWriterInstallApplication,
        IApplicationSnapshotGenerationRsm {
    struct TGeneration : IStagedApplicationSnapshotGeneration {
        TGenerationAwareSnapshotApplication& Application;
        TApplicationSnapshotGenerationReference Ref;
        std::string Bytes;
        TGeneration(TGenerationAwareSnapshotApplication& application,
                TApplicationSnapshotGenerationReference reference, std::string bytes)
            : Application(application), Ref(std::move(reference)), Bytes(std::move(bytes)) { }
        const TApplicationSnapshotGenerationReference& Reference() const override { return Ref; }
        void Publish() override {
            const auto path = Application.Catalog->Directory / Ref.Id;
            const auto imageFd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
            if (imageFd < 0) throw std::runtime_error("Cannot open application generation image");
            size_t offset = 0;
            while (offset < Bytes.size()) {
                const auto written = write(imageFd, Bytes.data() + offset, Bytes.size() - offset);
                if (written < 0 && errno == EINTR) continue;
                if (written <= 0) {
                    close(imageFd);
                    throw std::runtime_error("Cannot write application generation image");
                }
                offset += size_t(written);
            }
            const auto imageSynced = fsync(imageFd) == 0;
            close(imageFd);
            if (!imageSynced) throw std::runtime_error("Cannot sync application generation image");
            const auto directoryFd = open(Application.Catalog->Directory.c_str(),
                O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (directoryFd < 0) throw std::runtime_error("Cannot open application generation directory");
            const auto directorySynced = fsync(directoryFd) == 0;
            close(directoryFd);
            if (!directorySynced) throw std::runtime_error("Cannot sync application generation directory");
            if (!Application.Catalog->Images.emplace(Ref.Id, Bytes).second)
                throw std::logic_error("Application generation ID was reused");
            Application.Catalog->LastPreparedId = Ref.Id;
            Application.Catalog->Events.push_back("publish");
        }
        void Activate() override {
            if (!Application.Catalog->RaftManifestSynced)
                throw std::logic_error("Application generation activated before Raft publication");
            Application.Catalog->ActiveId = Ref.Id;
            Application.LastAppliedIndex = Ref.SnapshotIndex;
            Application.Catalog->Events.push_back("activate");
        }
    };

    std::shared_ptr<TApplicationGenerationCatalog> Catalog;
    explicit TGenerationAwareSnapshotApplication(
            std::shared_ptr<TApplicationGenerationCatalog> catalog)
        : Catalog(std::move(catalog)) { }

    std::unique_ptr<IStagedApplicationSnapshotGeneration> PrepareSnapshotGeneration(
            const TSnapshot& snapshot) override {
        auto bytes = snapshot.MaterializeData();
        TApplicationSnapshotGenerationReference reference;
        reference.Id = "generation-" + std::to_string(++Catalog->NextId);
        reference.SnapshotIndex = snapshot.Index;
        reference.DataBytes = bytes.size();
        TSha256DigestBuilder digest;
        digest.Update(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()));
        reference.DataSha256 = digest.Finish();
        return std::make_unique<TGeneration>(*this, std::move(reference), std::move(bytes));
    }

    void ValidateSnapshotGeneration(const TApplicationSnapshotGenerationReference& reference,
            uint64_t index) const override {
        if (reference.SnapshotIndex != index)
            throw std::runtime_error("Application snapshot generation index mismatch");
        auto image = Catalog->Images.find(reference.Id);
        if (image == Catalog->Images.end()) {
            std::ifstream input(Catalog->Directory / reference.Id, std::ios::binary);
            if (!input) throw std::runtime_error("Referenced application snapshot generation is missing");
            std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
            image = Catalog->Images.emplace(reference.Id, std::move(bytes)).first;
        }
        if (image->second.size() != reference.DataBytes)
            throw std::runtime_error("Referenced application snapshot generation is missing");
        TSha256DigestBuilder digest;
        digest.Update(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(image->second.data()), image->second.size()));
        if (digest.Finish() != reference.DataSha256)
            throw std::runtime_error("Referenced application snapshot generation is corrupt");
    }

    void RestoreSnapshotGeneration(const TApplicationSnapshotGenerationReference& reference,
            uint64_t index) override {
        ValidateSnapshotGeneration(reference, index);
        Catalog->ActiveId = reference.Id;
        LastAppliedIndex = index;
        Catalog->Events.push_back("restore");
    }
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
struct TGeneratedSnapshotDataSource : TSnapshotDataSource, ISnapshotSource {
    uint64_t Bytes;
    explicit TGeneratedSnapshotDataSource(uint64_t bytes) : Bytes(bytes) { }
    uint64_t Size() const override { return Bytes; }
    size_t Read(uint64_t offset, std::span<char> destination) const override {
        if (offset >= Bytes || destination.empty()) return 0;
        const auto count = size_t(std::min<uint64_t>(destination.size(), Bytes - offset));
        memset(destination.data(), 's', count);
        return count;
    }
};
struct TStringSnapshotDataSource : TSnapshotDataSource {
    std::string Bytes;
    size_t MaxReadBytes;
    mutable uint64_t BytesRead = 0;
    TStringSnapshotDataSource(std::string bytes, size_t maxReadBytes = SIZE_MAX)
        : Bytes(std::move(bytes)), MaxReadBytes(maxReadBytes) { }
    uint64_t Size() const override { return Bytes.size(); }
    size_t Read(uint64_t offset, std::span<char> destination) const override {
        if (offset >= Bytes.size() || destination.empty()) return 0;
        const auto count = std::min({destination.size(), MaxReadBytes,
            Bytes.size() - size_t(offset)});
        memcpy(destination.data(), Bytes.data() + offset, count);
        BytesRead += count;
        return count;
    }
};
struct TUnchargedStringSnapshotDataSource : TStringSnapshotDataSource,
        ISnapshotDataHeapUsage {
    using TStringSnapshotDataSource::TStringSnapshotDataSource;
    uint64_t HeapBytes() const override { return 0; }
};
struct TNoopSnapshotInstallation : IStagedSnapshotInstallation {
    void Install() override { }
};
struct TLargeSourceSnapshotApplication : IRsm, IStreamingSnapshotRsm, IStreamingSnapshotInstallRsm {
    uint64_t Bytes = uint64_t(TSnapshot::LegacyMaxBytes) + 1;
    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest>, uint64_t) override { return {}; }
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry>, uint64_t) override { return {}; }
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest>) override { return {}; }
    std::optional<uint64_t> SnapshotSizeUpperBound() const override { return Bytes; }
    std::shared_ptr<const ISnapshotSource> OpenSnapshotSource(uint64_t) const override {
        return std::make_shared<TGeneratedSnapshotDataSource>(Bytes);
    }
    void ValidateSnapshot(std::string_view, uint64_t) const override { }
    std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
            const TSnapshotDataSource&, uint64_t) override {
        return std::make_unique<TNoopSnapshotInstallation>();
    }
    void LoadSnapshot(std::string_view, uint64_t index) override { LastAppliedIndex = index; }
    bool SupportsApplicationSnapshotInstall() const override { return true; }
};
struct TUnchargedGeneratedSnapshotDataSource : TGeneratedSnapshotDataSource,
        ISnapshotSourceHeapUsage {
    using TGeneratedSnapshotDataSource::TGeneratedSnapshotDataSource;
    uint64_t HeapBytes() const override { return 0; }
};
struct TUnchargedLargeSourceSnapshotApplication : TLargeSourceSnapshotApplication {
    std::shared_ptr<const ISnapshotSource> OpenSnapshotSource(uint64_t) const override {
        return std::make_shared<TUnchargedGeneratedSnapshotDataSource>(Bytes);
    }
};
void test_kv_snapshot_precomputes_size_without_changing_encoding(void**) {
    TKv source;
    const std::string binaryKey("a\0", 2), binaryValue("\0v", 2);
    source.Write(source.Prepare(Command("z", "last")), 1);
    source.Write(source.Prepare(Command(binaryKey, binaryValue)), 2);

    const auto bytes = source.SaveSnapshot();
    assert_int_equal(source.SnapshotSizeUpperBound().value(), bytes.size());
    TCollectingSnapshotSink streamed;
    source.WriteSnapshot(source.LastAppliedIndex, streamed);
    assert_true(streamed.Data == bytes);
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
void test_snapshot_capture_respects_raft_image_limit(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    TRaft::TLogLimits limits{.MaxSnapshotImageBytes = 64};
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{}, std::vector<uint32_t>{}, limits);
    auto application = std::make_shared<TBoundedSnapshotApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, {});

    Reject([&] { raft->Compact(); });
    assert_int_equal(application->SaveCalls, 0);
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(state->LastLogIndex, 1);
}
void test_snapshot_capture_reserves_declared_bound_before_materializing(void**) {
    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto rejectedState = makeState();
    auto rejectedBudget = std::make_shared<TSnapshotInstallBudget>(79);
    TRaft::TLogLimits rejectedLimits;
    rejectedLimits.SnapshotInstallBudget = rejectedBudget;
    auto rejectedRaft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
        std::vector<uint32_t>{}, rejectedLimits);
    auto rejectedApp = std::make_shared<TBoundedSnapshotApplication>();
    rejectedApp->LastAppliedIndex = 1;
    TRequestProcessor rejectedProcessor(rejectedRaft, rejectedApp, {});
    Reject([&] { rejectedRaft->Compact(); });
    assert_int_equal(rejectedApp->SaveCalls, 0);
    assert_int_equal(rejectedBudget->UsedBytes(), 0);
    assert_int_equal(rejectedState->SnapshotIndex, 0);

    auto invalidState = makeState();
    auto invalidBudget = std::make_shared<TSnapshotInstallBudget>(80);
    TRaft::TLogLimits invalidLimits;
    invalidLimits.SnapshotInstallBudget = invalidBudget;
    auto invalidRaft = std::make_shared<TRaft>(invalidState, 1, TNodeDict{},
        std::vector<uint32_t>{}, invalidLimits);
    auto invalidApp = std::make_shared<TBoundedSnapshotApplication>();
    invalidApp->Bound = 39;
    invalidApp->LastAppliedIndex = 1;
    TRequestProcessor invalidProcessor(invalidRaft, invalidApp, {});
    Reject([&] { invalidRaft->Compact(); });
    assert_int_equal(invalidApp->SaveCalls, 1);
    assert_int_equal(invalidBudget->UsedBytes(), 0);
    assert_int_equal(invalidState->SnapshotIndex, 0);

    auto state = makeState();
    auto budget = std::make_shared<TSnapshotInstallBudget>(80);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        auto app = std::make_shared<TBoundedSnapshotApplication>();
        app->LastAppliedIndex = 1;
        TRequestProcessor processor(raft, app, {});
        auto snapshot = raft->CompactShared();
        assert_int_equal(app->SaveCalls, 1);
        assert_int_equal(snapshot->DataSize(), 40);
        // The 80-byte declared bound is held before SaveSnapshot, then shrunk
        // atomically to the 40-byte retained image.
        assert_int_equal(budget->UsedBytes(), 40);
    }
    state.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_unbounded_legacy_snapshot_precharges_before_callback(void**) {
    auto state = std::make_shared<TState>();
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto application = std::make_shared<TBudgetObservingUnboundedSnapshotApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, {});

    auto insufficientBudget = std::make_shared<TSnapshotInstallBudget>(1024);
    application->Budget = insufficientBudget;
    Reject([&] { CaptureSnapshot(*raft, *application, insufficientBudget); });
    assert_int_equal(application->SaveCalls, 0);
    assert_int_equal(insufficientBudget->UsedBytes(), 0);

    auto budget = std::make_shared<TSnapshotInstallBudget>(TSnapshot::MaxBytes);
    application->Budget = budget;
    {
        auto snapshot = CaptureSnapshot(*raft, *application, budget);
        assert_int_equal(application->SaveCalls, 1);
        assert_true(application->BudgetBytesDuringSave > TSnapshot::MaxBytes - 1024);
        assert_int_equal(snapshot.DataSize(), application->PayloadBytes);
        assert_int_equal(budget->UsedBytes(), snapshot.HeapBytes());
    }
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_snapshot_capture_charges_retained_string_capacity(void**) {
    struct TOverallocatedSnapshotApplication : TBoundedSnapshotApplication {
        std::optional<uint64_t> SnapshotSizeUpperBound() const override { return PayloadBytes; }
        std::string SaveSnapshot() const override {
            std::string bytes(PayloadBytes, 's');
            bytes.reserve(1024);
            return bytes;
        }
    };
    auto state = std::make_shared<TState>();
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
    auto application = std::make_shared<TOverallocatedSnapshotApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, {});

    auto tooSmallBudget = std::make_shared<TSnapshotInstallBudget>(512);
    Reject([&] { CaptureSnapshot(*raft, *application, tooSmallBudget); });
    assert_int_equal(tooSmallBudget->UsedBytes(), 0);

    auto budget = std::make_shared<TSnapshotInstallBudget>(1024);
    auto snapshot = CaptureSnapshot(*raft, *application, budget);
    assert_int_equal(snapshot.DataSize(), 40);
    assert_true(snapshot.HeapBytes() >= 1024);
    assert_int_equal(budget->UsedBytes(), snapshot.HeapBytes());
    snapshot.HeapReservation.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_example_kv_streaming_install_validates_before_atomic_publish(void**) {
    TStreamingKv source;
    source.Write(source.Prepare(Command("alpha", "one")), 1);
    source.Write(source.Prepare(Command("bravo", "two")), 2);
    const auto encoded = source.SaveSnapshot();

    TStreamingKv target;
    target.Write(target.Prepare(Command("stale", "keep")), 1);
    TSnapshot snapshot;
    snapshot.Index = 2;
    auto sourceBytes = std::make_shared<TStringSnapshotDataSource>(encoded, 3);
    snapshot.DataSource = sourceBytes;

    auto installation = PrepareApplicationSnapshotInstallation(target, snapshot);
    assert_true(bool(installation));
    assert_int_equal(sourceBytes->BytesRead, encoded.size());
    assert_int_equal(target.LastAppliedIndex, 1);
    assert_true(Read(target, "stale") == "keep");
    assert_true(Read(target, "alpha").empty());

    InstallApplicationSnapshot(target, snapshot, std::move(installation));
    assert_int_equal(target.LastAppliedIndex, 2);
    assert_true(Read(target, "alpha") == "one");
    assert_true(Read(target, "bravo") == "two");
    assert_true(Read(target, "stale").empty());

    TSnapshot malformed;
    malformed.Index = 2;
    malformed.DataSource = std::make_shared<TStringSnapshotDataSource>(encoded + "x", 5);
    Reject([&] { PrepareApplicationSnapshotInstallation(target, malformed); });
    assert_int_equal(target.LastAppliedIndex, 2);
    assert_true(Read(target, "alpha") == "one");
}
void test_incremental_snapshot_capture_charges_each_growth_before_writing(void**) {
    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto rejectedState = makeState();
    auto rejectedBudget = std::make_shared<TSnapshotInstallBudget>(39);
    TRaft::TLogLimits rejectedLimits;
    rejectedLimits.SnapshotInstallBudget = rejectedBudget;
    auto rejectedRaft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
        std::vector<uint32_t>{}, rejectedLimits);
    auto rejectedApp = std::make_shared<TStreamingWriterSnapshotApplication>();
    rejectedApp->IgnoreSinkFailures = true;
    rejectedApp->LastAppliedIndex = 1;
    TRequestProcessor rejectedProcessor(rejectedRaft, rejectedApp, {});
    Reject([&] { rejectedRaft->Compact(); });
    assert_int_equal(rejectedApp->WriteCalls, 1);
    assert_int_equal(rejectedApp->SaveCalls, 0);
    assert_int_equal(rejectedBudget->UsedBytes(), 0);
    assert_int_equal(rejectedState->SnapshotIndex, 0);

    auto state = makeState();
    auto budget = std::make_shared<TSnapshotInstallBudget>(1024);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    auto app = std::make_shared<TStreamingWriterSnapshotApplication>();
    app->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> snapshot;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        snapshot = raft->CompactShared();
    }
    assert_int_equal(app->WriteCalls, 1);
    assert_int_equal(app->SaveCalls, 0);
    assert_true(snapshot->MaterializeData() == app->Payload);
    assert_int_equal(budget->UsedBytes(), snapshot->HeapBytes());
    assert_true(snapshot->HeapBytes() >= snapshot->DataSize());
    snapshot.reset();
    state.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_snapshot_source_can_report_non_heap_payload_storage(void**) {
    auto state = std::make_shared<TState>();
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    auto budget = std::make_shared<TSnapshotInstallBudget>(1);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = budget;
    auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
        std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TUnchargedLargeSourceSnapshotApplication>();
    app->Bytes = 40;
    app->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, app, {});

    const auto snapshot = raft->CompactShared();
    assert_int_equal(snapshot->DataSize(), 40);
    assert_int_equal(snapshot->HeapBytes(), 0);
    assert_int_equal(budget->UsedBytes(), 0);
}
#ifdef __linux__
void test_incremental_snapshot_capture_uses_disk_staging_quota(void**) {
    char directoryTemplate[] = "/tmp/miniraft-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto state = makeState();
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(40);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    auto app = std::make_shared<TStreamingWriterInstallApplication>();
    app->LastAppliedIndex = 1;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        auto snapshot = raft->CompactShared();
        assert_true(snapshot->Data.empty());
        assert_true(bool(snapshot->DataSource));
        assert_int_equal(snapshot->DataSize(), app->Payload.size());
        assert_int_equal(app->RangePreparations, 1);
        std::array<char, 11> chunk{};
        for (size_t offset = 0; offset < app->Payload.size();) {
            const auto count = std::min(chunk.size(), app->Payload.size() - offset);
            assert_int_equal(snapshot->ReadDataRange(offset, std::span<char>(chunk.data(), count)), count);
            assert_true(memcmp(chunk.data(), app->Payload.data() + offset, count) == 0);
            offset += count;
        }
        assert_true(DecodeSnapshot(EncodeSnapshot(*snapshot)) == *snapshot);
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), app->Payload.size());
    }
    state.reset();
    assert_int_equal(stagingBudget->UsedBytes(), 0);

    auto rejectedState = makeState();
    auto rejectedHeapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto rejectedStagingBudget = std::make_shared<TSnapshotInstallBudget>(39);
    limits.SnapshotInstallBudget = rejectedHeapBudget;
    limits.SnapshotStagingBudget = rejectedStagingBudget;
    auto rejectedApp = std::make_shared<TStreamingWriterSnapshotApplication>();
    rejectedApp->IgnoreSinkFailures = true;
    rejectedApp->LastAppliedIndex = 1;
    {
        auto raft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, rejectedApp, {});
        Reject([&] { raft->Compact(); });
    }
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectedHeapBudget->UsedBytes(), 0);
    assert_int_equal(rejectedStagingBudget->UsedBytes(), 0);
}
void test_disk_staged_capture_becomes_durable_snapshot_across_restart(void**) {
    char directoryTemplate[] = "/tmp/miniraft-durable-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto name = (std::filesystem::path(createdDirectory) / "raft").string();
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(40);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    auto app = std::make_shared<TStreamingWriterInstallApplication>();
    app->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> captured;

    auto state = std::make_shared<TDurableState>(name, 1);
    state->CurrentTerm = 1;
    state->BindVoters({1});
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;
    state->Commit();
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        captured = raft->CompactShared();
        assert_true(bool(captured->DataSource));
        assert_true(captured->Data.empty());
        assert_int_equal(captured->DataSize(), app->Payload.size());
        assert_int_equal(stagingBudget->UsedBytes(), app->Payload.size());
        assert_int_equal(state->SnapshotIndex, 1);
    }
    state.reset();
    assert_int_equal(stagingBudget->UsedBytes(), app->Payload.size());

    {
        TDurableState recovered(name, 1);
        const auto snapshot = recovered.GetSnapshot();
        assert_non_null(snapshot.get());
        assert_int_equal(snapshot->Index, 1);
        assert_int_equal(snapshot->DataSize(), app->Payload.size());
        assert_true(snapshot->MaterializeData() == app->Payload);
        assert_int_equal(recovered.CommitIndex, 1);
    }

    captured.reset();
    assert_int_equal(stagingBudget->UsedBytes(), 0);
}
void test_durable_application_snapshot_generation_is_published_before_raft_and_restored(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-application-generation-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto name = (std::filesystem::path(createdDirectory) / "raft").string();
    auto catalog = std::make_shared<TApplicationGenerationCatalog>();
    catalog->Directory = createdDirectory;
    {
        TDurableState state(name, 1);
        state.CurrentTerm = 1;
        state.BindVoters({1});
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 6);
        entry->Term = 1;
        memcpy(entry->Data, "prefix", 6);
        state.Append(entry);
        auto suffix = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 6);
        suffix->Term = 1;
        memcpy(suffix->Data, "suffix", 6);
        state.Append(suffix);
        state.CommitIndex = 2;
        state.Commit();
    }
    const auto orphanedId = std::string("generation-1");
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        try {
            auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
            application->LastAppliedIndex = 1;
            auto state = std::make_shared<TDurableState>(name, 1,
                [catalog](TDurableState::EStage stage) {
                    if (stage == TDurableState::EStage::TempCreated &&
                        !catalog->LastPreparedId.empty()) {
                        const auto exists = std::filesystem::exists(
                            catalog->Directory / catalog->LastPreparedId);
                        _exit(exists ? 77 : 78);
                    }
                });
            auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
            TRequestProcessor processor(raft, application, {});
            raft->CompactShared();
        } catch (...) { _exit(79); }
        _exit(80);
    }
    int childStatus = 0;
    assert_int_equal(waitpid(child, &childStatus, 0), child);
    assert_true(WIFEXITED(childStatus));
    assert_int_equal(WEXITSTATUS(childStatus), 77);
    assert_true(std::filesystem::exists(catalog->Directory / orphanedId));
    {
        TDurableState recovered(name, 1);
        assert_int_equal(recovered.SnapshotIndex, 0);
        assert_int_equal(recovered.CommitIndex, 2);
        assert_int_equal(recovered.LastLogIndex, 2);
        assert_true(std::string(recovered.Get(2)->Data, 6) == "suffix");
        assert_true(!recovered.GetApplicationSnapshotGenerationReference());
    }

    // The generation was durable but unreferenced when the process crashed.
    // A later compaction may publish a fresh generation.
    catalog->NextId = 1;
    catalog->RaftManifestSynced = false;
    auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
    application->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> snapshot;
    TApplicationSnapshotGenerationReference reference;
    {
        auto state = std::make_shared<TDurableState>(name, 1, [catalog](TDurableState::EStage stage) {
            if (catalog->LastPreparedId.empty()) return;
            if (stage == TDurableState::EStage::TempCreated) {
                assert_true(catalog->Images.count(catalog->LastPreparedId) == 1);
                catalog->Events.push_back("raft-manifest-start");
            } else if (stage == TDurableState::EStage::DirectorySynced) {
                catalog->RaftManifestSynced = true;
                catalog->Events.push_back("raft-manifest-synced");
            }
        });
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        TRequestProcessor processor(raft, application, {});
        snapshot = raft->CompactShared();
        const auto storedReference = state->GetApplicationSnapshotGenerationReference();
        assert_true(storedReference.has_value());
        reference = *storedReference;
        assert_int_equal(reference.SnapshotIndex, snapshot->Index);
        assert_int_equal(snapshot->Index, 1);
        assert_true(reference.Id != orphanedId);
        assert_true(catalog->ActiveId == reference.Id);
        assert_true(catalog->RaftManifestSynced);
        assert_int_equal(application->LastAppliedIndex, snapshot->Index);
    }

    const auto publish = std::find(catalog->Events.begin(), catalog->Events.end(), "publish");
    const auto manifest = std::find(catalog->Events.begin(), catalog->Events.end(), "raft-manifest-synced");
    const auto activate = std::find(catalog->Events.begin(), catalog->Events.end(), "activate");
    assert_true(publish < manifest);
    assert_true(manifest < activate);

    {
        auto recovered = std::make_shared<TDurableState>(name, 1);
        const auto recoveredReference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(recoveredReference.has_value());
        assert_true(*recoveredReference == reference);
        assert_int_equal(recovered->SnapshotIndex, reference.SnapshotIndex);
        assert_int_equal(recovered->CommitIndex, 2);
        assert_int_equal(recovered->LastLogIndex, 2);
        assert_true(std::string(recovered->Get(2)->Data, 6) == "suffix");
        auto restoredApplication = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        TRequestProcessor processor(raft, restoredApplication, {});
        assert_int_equal(restoredApplication->LastAppliedIndex, reference.SnapshotIndex);
        assert_true(catalog->ActiveId == reference.Id);
        assert_true(catalog->Images.at(reference.Id) == snapshot->MaterializeData());
    }

    // A crash after the Raft directory sync but before Activate must recover
    // the newly selected generation from the application store.
    catalog->NextId = 2;
    catalog->LastPreparedId.clear();
    catalog->RaftManifestSynced = false;
    const auto activationCrash = fork();
    assert_true(activationCrash >= 0);
    if (activationCrash == 0) {
        try {
            auto nextApplication = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
            nextApplication->LastAppliedIndex = 2;
            auto state = std::make_shared<TDurableState>(name, 1,
                [catalog](TDurableState::EStage stage) {
                    if (stage == TDurableState::EStage::DirectorySynced &&
                        !catalog->LastPreparedId.empty()) {
                        const auto exists = std::filesystem::exists(
                            catalog->Directory / catalog->LastPreparedId);
                        _exit(exists ? 87 : 88);
                    }
                });
            auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
            TRequestProcessor processor(raft, nextApplication, {});
            raft->CompactShared();
        } catch (...) { _exit(89); }
        _exit(90);
    }
    childStatus = 0;
    assert_int_equal(waitpid(activationCrash, &childStatus, 0), activationCrash);
    assert_true(WIFEXITED(childStatus));
    assert_int_equal(WEXITSTATUS(childStatus), 87);
    {
        auto recovered = std::make_shared<TDurableState>(name, 1);
        const auto recoveredReference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(recoveredReference.has_value());
        assert_int_equal(recoveredReference->SnapshotIndex, 2);
        assert_int_equal(recovered->SnapshotIndex, 2);
        assert_int_equal(recovered->CommitIndex, 2);
        auto restoredApplication = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        TRequestProcessor processor(raft, restoredApplication, {});
        assert_int_equal(restoredApplication->LastAppliedIndex, 2);
        assert_true(catalog->ActiveId == recoveredReference->Id);
        assert_true(catalog->Images.at(recoveredReference->Id) == std::string(40, 'w'));
    }
#endif
}
void test_versioned_disk_application_snapshot_generation_roundtrips(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-disk-application-generation-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto name = (std::filesystem::path(createdDirectory) / "raft").string();
    auto catalog = std::make_shared<TApplicationGenerationCatalog>();
    catalog->Directory = createdDirectory;
    auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
    application->LastAppliedIndex = 1;
    TApplicationSnapshotGenerationReference reference;
    {
        auto state = std::make_shared<TDiskState>(name, 1,
            [catalog](TDiskState::ECommitStage stage) {
                if (stage == TDiskState::ECommitStage::DirectorySynced &&
                    !catalog->LastPreparedId.empty())
                    catalog->RaftManifestSynced = true;
            });
        state->CurrentTerm = 1;
        state->BindVoters({1});
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        state->Commit();
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        TRequestProcessor processor(raft, application, {});
        const auto snapshot = raft->CompactShared();
        const auto storedReference = state->GetApplicationSnapshotGenerationReference();
        assert_true(storedReference.has_value());
        reference = *storedReference;
        assert_int_equal(reference.SnapshotIndex, snapshot->Index);
        assert_int_equal(state->SnapshotIndex, snapshot->Index);
    }
    {
        auto recovered = std::make_shared<TDiskState>(name, 1);
        const auto recoveredReference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(recoveredReference.has_value());
        assert_true(*recoveredReference == reference);
        auto restoredApplication = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        TRequestProcessor processor(raft, restoredApplication, {});
        assert_int_equal(restoredApplication->LastAppliedIndex, reference.SnapshotIndex);
        assert_true(catalog->ActiveId == reference.Id);
    }
#endif
}
void test_remote_snapshot_install_publishes_and_activates_local_generation(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-remote-application-generation-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto name = (std::filesystem::path(createdDirectory) / "raft").string();
    auto catalog = std::make_shared<TApplicationGenerationCatalog>();
    catalog->Directory = createdDirectory;
    std::vector<TMessageHolder<TMessage>> replies;
    auto replyTarget = std::make_shared<TEndpoint>([&](auto message) {
        replies.push_back(std::move(message));
    });
    TNodeDict peers{{1, replyTarget}, {3, replyTarget}};
    TApplicationSnapshotGenerationReference reference;
    {
        auto state = std::make_shared<TDurableState>(name, 2,
            [catalog](TDurableState::EStage stage) {
                if (stage == TDurableState::EStage::DirectorySynced &&
                    !catalog->LastPreparedId.empty())
                    catalog->RaftManifestSynced = true;
            });
        state->BindVoters({1, 2, 3});
        state->CurrentTerm = 2;
        state->Commit();
        auto raft = std::make_shared<TRaft>(state, 2, peers);
        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        TRequestProcessor processor(raft, application, peers);

        const TSnapshot image{1, 1, {1, 2, 3}, std::string(40, 'r')};
        const auto encoded = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Src = 1;
        request->Dst = 2;
        request->Term = 2;
        request->Seqno = 1;
        request->SnapshotIndex = image.Index;
        request->SnapshotTerm = image.Term;
        request->TotalBytes = encoded.size();
        request->LeaderId = 1;
        request->Done = TInstallSnapshotRequest::DoneFlag;
        memcpy(request->Data, encoded.data(), encoded.size());
        raft->Process(ITimeSource::Time{}, request);

        assert_int_equal(state->SnapshotIndex, image.Index);
        const auto storedReference = state->GetApplicationSnapshotGenerationReference();
        assert_true(storedReference.has_value());
        reference = *storedReference;
        assert_int_equal(reference.SnapshotIndex, image.Index);
        assert_true(catalog->ActiveId == reference.Id);
        assert_true(catalog->RaftManifestSynced);
        assert_int_equal(application->LastAppliedIndex, image.Index);
        assert_int_equal(replies.size(), 1);
        const auto response = replies.back().Cast<TInstallSnapshotResponse>();
        assert_true(response->Success && response->Installed);
        assert_int_equal(response->NextOffset, encoded.size());
    }
    {
        auto recovered = std::make_shared<TDurableState>(name, 2);
        const auto recoveredReference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(recoveredReference.has_value());
        assert_true(*recoveredReference == reference);
        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        auto raft = std::make_shared<TRaft>(recovered, 2, peers);
        TRequestProcessor processor(raft, application, peers);
        assert_int_equal(application->LastAppliedIndex, reference.SnapshotIndex);
        assert_true(catalog->ActiveId == reference.Id);
        assert_true(catalog->Images.at(reference.Id) == std::string(40, 'r'));
    }
#endif
}
void test_durable_application_generation_replaces_snapshot_sidecar_payload(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-durable-generation-source-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto root = std::filesystem::path(createdDirectory);
    const auto applicationDirectory = root / "application";
    assert_true(std::filesystem::create_directory(applicationDirectory));
    const auto name = (root / "raft").string();
    auto resolver = [applicationDirectory](const TApplicationSnapshotGenerationReference& reference)
            -> std::shared_ptr<const TSnapshotDataSource> {
        std::ifstream input(applicationDirectory / reference.Id, std::ios::binary);
        if (!input) return {};
        std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
        return std::make_shared<TStringSnapshotDataSource>(std::move(bytes));
    };

    auto catalog = std::make_shared<TApplicationGenerationCatalog>();
    catalog->Directory = applicationDirectory;
    TApplicationSnapshotGenerationReference reference;
    std::string expectedWireImage;
    uint64_t sidecarBytes = 0;
    constexpr size_t PayloadBytes = 1024 * 1024;
    {
        auto state = std::make_shared<TDurableState>(name, 1,
            [catalog](TDurableState::EStage stage) {
                if (stage == TDurableState::EStage::DirectorySynced &&
                    !catalog->LastPreparedId.empty())
                    catalog->RaftManifestSynced = true;
            }, resolver);
        state->CurrentTerm = 1;
        state->BindVoters({1});
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        state->Commit();

        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        application->Payload.assign(PayloadBytes, 'g');
        application->LastAppliedIndex = 1;
        TRequestProcessor processor(raft, application, {});
        const auto snapshot = raft->CompactShared();
        const auto storedReference = state->GetApplicationSnapshotGenerationReference();
        assert_true(storedReference.has_value());
        reference = *storedReference;
        assert_int_equal(reference.DataBytes, PayloadBytes);
        assert_int_equal(snapshot->DataSize(), PayloadBytes);
        expectedWireImage = EncodeSnapshot(*snapshot);
        const auto sidecar = std::filesystem::path(name + ".durable.1.snapshot." +
            std::to_string(snapshot->Index) + "." + std::to_string(snapshot->Term));
        sidecarBytes = std::filesystem::file_size(sidecar);
        assert_true(sidecarBytes < reference.DataBytes);
        assert_true(catalog->ActiveId == reference.Id);
    }

    // A generation-backed metadata-only sidecar must fail closed when opened
    // without the matching application source resolver.
    Reject([&] { TDurableState missingResolver(name, 1); });

    auto recoveredCatalog = std::make_shared<TApplicationGenerationCatalog>();
    recoveredCatalog->Directory = applicationDirectory;
    {
        auto recovered = std::make_shared<TDurableState>(name, 1,
            TDurableState::THook{}, resolver);
        const auto snapshot = recovered->GetSnapshot();
        assert_non_null(snapshot.get());
        assert_int_equal(snapshot->DataSize(), reference.DataBytes);
        assert_true(EncodeSnapshot(*snapshot) == expectedWireImage);
        const auto sidecar = std::filesystem::path(name + ".durable.1.snapshot." +
            std::to_string(snapshot->Index) + "." + std::to_string(snapshot->Term));
        assert_int_equal(std::filesystem::file_size(sidecar), sidecarBytes);

        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(recoveredCatalog);
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        TRequestProcessor processor(raft, application, {});
        assert_int_equal(application->LastAppliedIndex, reference.SnapshotIndex);
        assert_true(recoveredCatalog->ActiveId == reference.Id);
        assert_true(recoveredCatalog->Images.at(reference.Id) == std::string(PayloadBytes, 'g'));
    }
#endif
}
void test_versioned_disk_application_generation_replaces_snapshot_payload(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-split-generation-source-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto root = std::filesystem::path(createdDirectory);
    const auto applicationDirectory = root / "application";
    assert_true(std::filesystem::create_directory(applicationDirectory));
    const auto name = (root / "raft").string();
    auto resolver = [applicationDirectory](const TApplicationSnapshotGenerationReference& reference)
            -> std::shared_ptr<const TSnapshotDataSource> {
        std::ifstream input(applicationDirectory / reference.Id, std::ios::binary);
        if (!input) return {};
        std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
        return std::make_shared<TStringSnapshotDataSource>(std::move(bytes));
    };

    auto catalog = std::make_shared<TApplicationGenerationCatalog>();
    catalog->Directory = applicationDirectory;
    TApplicationSnapshotGenerationReference reference;
    std::string expectedWireImage;
    uint64_t sidecarBytes = 0;
    constexpr size_t PayloadBytes = 1024 * 1024;
    {
        auto state = std::make_shared<TDiskState>(name, 1,
            [catalog](TDiskState::ECommitStage stage) {
                if (stage == TDiskState::ECommitStage::DirectorySynced &&
                    !catalog->LastPreparedId.empty())
                    catalog->RaftManifestSynced = true;
            }, true, resolver);
        state->CurrentTerm = 1;
        state->BindVoters({1});
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        state->Commit();

        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        application->Payload.assign(PayloadBytes, 'd');
        application->LastAppliedIndex = 1;
        TRequestProcessor processor(raft, application, {});
        const auto snapshot = raft->CompactShared();
        const auto storedReference = state->GetApplicationSnapshotGenerationReference();
        assert_true(storedReference.has_value());
        reference = *storedReference;
        assert_int_equal(reference.DataBytes, PayloadBytes);
        assert_int_equal(snapshot->DataSize(), PayloadBytes);
        expectedWireImage = EncodeSnapshot(*snapshot);
        const auto sidecar = std::filesystem::path(name + ".state.1.snapshot.g1");
        sidecarBytes = std::filesystem::file_size(sidecar);
        assert_true(sidecarBytes < reference.DataBytes);
        assert_true(catalog->ActiveId == reference.Id);
    }

    Reject([&] { TDiskState missingResolver(name, 1); });
    auto wrongResolver = [](const TApplicationSnapshotGenerationReference&)
            -> std::shared_ptr<const TSnapshotDataSource> {
        return std::make_shared<TStringSnapshotDataSource>(std::string(PayloadBytes, 'x'));
    };
    Reject([&] { TDiskState wrongSource(name, 1, TDiskState::TCommitHook{}, true, wrongResolver); });

    auto recoveredCatalog = std::make_shared<TApplicationGenerationCatalog>();
    recoveredCatalog->Directory = applicationDirectory;
    {
        auto recovered = std::make_shared<TDiskState>(name, 1,
            TDiskState::TCommitHook{}, true, resolver);
        const auto snapshot = recovered->GetSnapshot();
        assert_non_null(snapshot.get());
        assert_int_equal(snapshot->DataSize(), reference.DataBytes);
        assert_true(EncodeSnapshot(*snapshot) == expectedWireImage);
        assert_int_equal(std::filesystem::file_size(name + ".state.1.snapshot.g1"), sidecarBytes);

        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(recoveredCatalog);
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        TRequestProcessor processor(raft, application, {});
        assert_int_equal(application->LastAppliedIndex, reference.SnapshotIndex);
        assert_true(recoveredCatalog->ActiveId == reference.Id);
        assert_true(recoveredCatalog->Images.at(reference.Id) == std::string(PayloadBytes, 'd'));
    }
#endif
}
void test_split_application_snapshot_generation_crash_boundaries(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-split-generation-crash-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const std::array stages{
        TDiskState::ECommitStage::SnapshotImagePublished,
        TDiskState::ECommitStage::LogFilesSynced,
        TDiskState::ECommitStage::MetadataFileSynced,
        TDiskState::ECommitStage::MetadataRenamed,
        TDiskState::ECommitStage::DirectorySynced,
    };
    constexpr size_t PayloadBytes = 128 * 1024;
    for (size_t attempt = 0; attempt < stages.size(); ++attempt) {
        const auto root = std::filesystem::path(createdDirectory) / std::to_string(attempt);
        assert_true(std::filesystem::create_directory(root));
        const auto applicationDirectory = root / "application";
        assert_true(std::filesystem::create_directory(applicationDirectory));
        const auto name = (root / "raft").string();
        auto resolver = [applicationDirectory](const TApplicationSnapshotGenerationReference& reference)
                -> std::shared_ptr<const TSnapshotDataSource> {
            std::ifstream input(applicationDirectory / reference.Id, std::ios::binary);
            if (!input) return {};
            std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
            return std::make_shared<TStringSnapshotDataSource>(std::move(bytes));
        };

        {
            TDiskState seed(name, 1);
            seed.CurrentTerm = 1;
            seed.BindVoters({1});
            auto entry = NewHoldedMessage<TLogEntry>();
            entry->Term = 1;
            seed.Append(entry);
            seed.CommitIndex = 1;
            seed.Commit();
        }

        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                auto catalog = std::make_shared<TApplicationGenerationCatalog>();
                catalog->Directory = applicationDirectory;
                bool snapshotImagePublished = false;
                auto state = std::make_shared<TDiskState>(name, 1,
                    [target = stages[attempt], &snapshotImagePublished,
                     catalog](TDiskState::ECommitStage stage) {
                        if (stage == TDiskState::ECommitStage::SnapshotImagePublished) {
                            snapshotImagePublished = true;
                            if (target == stage && !catalog->LastPreparedId.empty()) _exit(91);
                        } else if (snapshotImagePublished && target == stage &&
                                !catalog->LastPreparedId.empty()) {
                            _exit(91);
                        }
                    }, true, resolver);
                auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
                auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
                application->Payload.assign(PayloadBytes, 'c');
                application->LastAppliedIndex = 1;
                TRequestProcessor processor(raft, application, {});
                (void)raft->CompactShared();
            } catch (...) { _exit(92); }
            _exit(93);
        }

        int childStatus = 0;
        assert_int_equal(waitpid(child, &childStatus, 0), child);
        assert_true(WIFEXITED(childStatus));
        assert_int_equal(WEXITSTATUS(childStatus), 91);

        const bool metadataSelected = stages[attempt] == TDiskState::ECommitStage::MetadataRenamed ||
            stages[attempt] == TDiskState::ECommitStage::DirectorySynced;
        if (!metadataSelected) {
            TDiskState recovered(name, 1);
            assert_int_equal(recovered.SnapshotIndex, 0);
            assert_int_equal(recovered.CommitIndex, 1);
            assert_int_equal(recovered.LastLogIndex, 1);
            assert_true(!recovered.GetApplicationSnapshotGenerationReference());
            continue;
        }

        auto recovered = std::make_shared<TDiskState>(name, 1,
            TDiskState::TCommitHook{}, true, resolver);
        assert_int_equal(recovered->SnapshotIndex, 1);
        assert_int_equal(recovered->CommitIndex, 1);
        assert_int_equal(recovered->LastLogIndex, 1);
        const auto reference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(reference.has_value());
        assert_int_equal(reference->DataBytes, PayloadBytes);
        assert_int_equal(recovered->GetSnapshot()->DataSize(), PayloadBytes);

        auto catalog = std::make_shared<TApplicationGenerationCatalog>();
        catalog->Directory = applicationDirectory;
        auto raft = std::make_shared<TRaft>(recovered, 1, TNodeDict{});
        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(catalog);
        TRequestProcessor processor(raft, application, {});
        assert_int_equal(application->LastAppliedIndex, reference->SnapshotIndex);
        assert_true(catalog->ActiveId == reference->Id);
        assert_true(catalog->Images.at(reference->Id) == std::string(PayloadBytes, 'c'));
    }
#endif
}
void test_remote_application_snapshot_generation_crash_boundaries(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-remote-generation-crash-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    const auto root = std::filesystem::path(createdDirectory);
    auto makeStore = [](const std::string& name) {
        auto state = std::make_shared<TDurableState>(name, 2);
        state->BindVoters({1, 2, 3});
        state->CurrentTerm = 2;
        state->Commit();
        return state;
    };
    auto installRemoteSnapshot = [](const std::shared_ptr<TDurableState>& state,
            const std::shared_ptr<TGenerationAwareSnapshotApplication>& application) {
        std::vector<TMessageHolder<TMessage>> replies;
        auto peer = std::make_shared<TEndpoint>([&](auto message) {
            replies.push_back(std::move(message));
        });
        TNodeDict peers{{1, peer}, {3, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers);
        TRequestProcessor processor(raft, application, peers);
        const TSnapshot image{1, 1, {1, 2, 3}, std::string(40, 'r')};
        const auto encoded = EncodeSnapshot(image);
        auto request = NewHoldedMessage<TInstallSnapshotRequest>(
            sizeof(TInstallSnapshotRequest) + encoded.size());
        request->Src = 1;
        request->Dst = 2;
        request->Term = 2;
        request->Seqno = 1;
        request->SnapshotIndex = image.Index;
        request->SnapshotTerm = image.Term;
        request->TotalBytes = encoded.size();
        request->LeaderId = 1;
        request->Done = TInstallSnapshotRequest::DoneFlag;
        memcpy(request->Data, encoded.data(), encoded.size());
        raft->Process(ITimeSource::Time{}, request);
    };

    const auto beforeRaft = root / "before-raft";
    const auto beforeApplication = root / "before-raft-application";
    assert_true(std::filesystem::create_directory(beforeRaft));
    assert_true(std::filesystem::create_directory(beforeApplication));
    const auto beforeName = (beforeRaft / "raft").string();
    auto beforeCatalog = std::make_shared<TApplicationGenerationCatalog>();
    beforeCatalog->Directory = beforeApplication;
    makeStore(beforeName).reset();

    const auto beforeCrash = fork();
    assert_true(beforeCrash >= 0);
    if (beforeCrash == 0) {
        try {
            auto state = std::make_shared<TDurableState>(beforeName, 2,
                [beforeCatalog](TDurableState::EStage stage) {
                    if (stage == TDurableState::EStage::SnapshotTempCreated) {
                        const auto generationExists = std::filesystem::exists(
                            beforeCatalog->Directory / "generation-1");
                        _exit(generationExists ? 91 : 92);
                    }
                });
            auto application = std::make_shared<TGenerationAwareSnapshotApplication>(beforeCatalog);
            installRemoteSnapshot(state, application);
        } catch (...) { _exit(93); }
        _exit(94);
    }
    int childStatus = 0;
    assert_int_equal(waitpid(beforeCrash, &childStatus, 0), beforeCrash);
    assert_true(WIFEXITED(childStatus));
    assert_int_equal(WEXITSTATUS(childStatus), 91);
    assert_true(std::filesystem::exists(beforeApplication / "generation-1"));
    {
        TDurableState recovered(beforeName, 2);
        assert_int_equal(recovered.SnapshotIndex, 0);
        assert_int_equal(recovered.CommitIndex, 0);
        assert_int_equal(recovered.LastLogIndex, 0);
        assert_true(!recovered.GetApplicationSnapshotGenerationReference());
    }

    const auto afterRaft = root / "after-raft";
    const auto afterApplication = root / "after-raft-application";
    assert_true(std::filesystem::create_directory(afterRaft));
    assert_true(std::filesystem::create_directory(afterApplication));
    const auto afterName = (afterRaft / "raft").string();
    auto afterCatalog = std::make_shared<TApplicationGenerationCatalog>();
    afterCatalog->Directory = afterApplication;
    makeStore(afterName).reset();

    const auto activationCrash = fork();
    assert_true(activationCrash >= 0);
    if (activationCrash == 0) {
        try {
            auto state = std::make_shared<TDurableState>(afterName, 2,
                [afterCatalog](TDurableState::EStage stage) {
                    if (stage == TDurableState::EStage::DirectorySynced &&
                        !afterCatalog->LastPreparedId.empty()) {
                        const auto generationExists = std::filesystem::exists(
                            afterCatalog->Directory / afterCatalog->LastPreparedId);
                        _exit(generationExists ? 95 : 96);
                    }
                });
            auto application = std::make_shared<TGenerationAwareSnapshotApplication>(afterCatalog);
            installRemoteSnapshot(state, application);
        } catch (...) { _exit(97); }
        _exit(98);
    }
    childStatus = 0;
    assert_int_equal(waitpid(activationCrash, &childStatus, 0), activationCrash);
    assert_true(WIFEXITED(childStatus));
    assert_int_equal(WEXITSTATUS(childStatus), 95);
    {
        auto recovered = std::make_shared<TDurableState>(afterName, 2);
        assert_int_equal(recovered->SnapshotIndex, 1);
        assert_int_equal(recovered->CommitIndex, 1);
        const auto reference = recovered->GetApplicationSnapshotGenerationReference();
        assert_true(reference.has_value());
        assert_int_equal(reference->SnapshotIndex, 1);
        assert_true(std::filesystem::exists(afterApplication / reference->Id));

        auto application = std::make_shared<TGenerationAwareSnapshotApplication>(afterCatalog);
        auto peer = std::make_shared<TEndpoint>([](auto) {});
        TNodeDict peers{{1, peer}, {3, peer}};
        auto raft = std::make_shared<TRaft>(recovered, 2, peers);
        TRequestProcessor processor(raft, application, peers);
        assert_int_equal(application->LastAppliedIndex, reference->SnapshotIndex);
        assert_true(afterCatalog->ActiveId == reference->Id);
        assert_true(afterCatalog->Images.at(reference->Id) == std::string(40, 'r'));
    }
#endif
}
void test_range_snapshot_source_uses_disk_staging_quota(void**) {
    char directoryTemplate[] = "/tmp/miniraft-range-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto state = makeState();
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(40);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    auto app = std::make_shared<TLargeSourceSnapshotApplication>();
    app->Bytes = 40;
    app->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> snapshot;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        snapshot = raft->CompactShared();
        assert_true(snapshot->Data.empty());
        assert_true(bool(snapshot->DataSource));
        assert_true(snapshot->MaterializeData() == std::string(40, 's'));
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), 40);
    }
    snapshot.reset();
    state.reset();
    assert_int_equal(stagingBudget->UsedBytes(), 0);

    auto rejectedState = makeState();
    auto rejectedHeapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto rejectedStagingBudget = std::make_shared<TSnapshotInstallBudget>(39);
    limits.SnapshotInstallBudget = rejectedHeapBudget;
    limits.SnapshotStagingBudget = rejectedStagingBudget;
    auto rejectedApp = std::make_shared<TLargeSourceSnapshotApplication>();
    rejectedApp->Bytes = 40;
    rejectedApp->LastAppliedIndex = 1;
    {
        auto raft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, rejectedApp, {});
        Reject([&] { raft->Compact(); });
    }
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectedHeapBudget->UsedBytes(), 0);
    assert_int_equal(rejectedStagingBudget->UsedBytes(), 0);
}
void test_legacy_bounded_snapshot_uses_disk_staging_quota(void**) {
    char directoryTemplate[] = "/tmp/miniraft-legacy-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto state = makeState();
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(80);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(40);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    auto app = std::make_shared<TBoundedSnapshotApplication>();
    app->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> snapshot;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        snapshot = raft->CompactShared();
        assert_int_equal(app->SaveCalls, 1);
        assert_true(snapshot->Data.empty());
        assert_true(bool(snapshot->DataSource));
        assert_true(snapshot->MaterializeData() == std::string(40, 's'));
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), 40);
    }
    state.reset();
    snapshot.reset();
    assert_int_equal(heapBudget->UsedBytes(), 0);
    assert_int_equal(stagingBudget->UsedBytes(), 0);

    auto rejectedState = makeState();
    auto rejectedHeapBudget = std::make_shared<TSnapshotInstallBudget>(80);
    auto rejectedStagingBudget = std::make_shared<TSnapshotInstallBudget>(39);
    limits.SnapshotInstallBudget = rejectedHeapBudget;
    limits.SnapshotStagingBudget = rejectedStagingBudget;
    auto rejectedApp = std::make_shared<TBoundedSnapshotApplication>();
    rejectedApp->LastAppliedIndex = 1;
    {
        auto raft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, rejectedApp, {});
        Reject([&] { raft->Compact(); });
    }
    assert_int_equal(rejectedApp->SaveCalls, 1);
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectedHeapBudget->UsedBytes(), 0);
    assert_int_equal(rejectedStagingBudget->UsedBytes(), 0);
}
void test_legacy_unbounded_snapshot_uses_disk_staging_quota(void**) {
    char directoryTemplate[] = "/tmp/miniraft-legacy-unbounded-capture-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};

    auto makeState = [] {
        auto state = std::make_shared<TState>();
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 1;
        state->Append(entry);
        state->CommitIndex = 1;
        return state;
    };

    auto state = makeState();
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(TSnapshot::MaxBytes);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(40);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = heapBudget;
    limits.SnapshotStagingBudget = stagingBudget;
    limits.SnapshotStagingDirectory = createdDirectory;
    auto app = std::make_shared<TUnboundedSnapshotApplication>();
    app->LastAppliedIndex = 1;
    std::shared_ptr<const TSnapshot> snapshot;
    {
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, app, {});
        snapshot = raft->CompactShared();
        assert_int_equal(app->SaveCalls, 1);
        assert_true(snapshot->Data.empty());
        assert_true(bool(snapshot->DataSource));
        assert_true(snapshot->MaterializeData() == std::string(40, 's'));
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), 40);
    }
    state.reset();
    snapshot.reset();
    assert_int_equal(heapBudget->UsedBytes(), 0);
    assert_int_equal(stagingBudget->UsedBytes(), 0);

    auto rejectedState = makeState();
    auto rejectedHeapBudget = std::make_shared<TSnapshotInstallBudget>(TSnapshot::MaxBytes);
    auto rejectedStagingBudget = std::make_shared<TSnapshotInstallBudget>(39);
    limits.SnapshotInstallBudget = rejectedHeapBudget;
    limits.SnapshotStagingBudget = rejectedStagingBudget;
    auto rejectedApp = std::make_shared<TUnboundedSnapshotApplication>();
    rejectedApp->LastAppliedIndex = 1;
    {
        auto raft = std::make_shared<TRaft>(rejectedState, 1, TNodeDict{},
            std::vector<uint32_t>{}, limits);
        TRequestProcessor processor(raft, rejectedApp, {});
        Reject([&] { raft->Compact(); });
    }
    assert_int_equal(rejectedApp->SaveCalls, 1);
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectedHeapBudget->UsedBytes(), 0);
    assert_int_equal(rejectedStagingBudget->UsedBytes(), 0);
}
#endif
void test_recovered_snapshot_retains_heap_budget_lease(void**) {
    auto state = std::make_shared<TState>();
    const TSnapshot image{1, 1, {1}, std::string(40, 's')};
    state->InstallSnapshot(image);

    auto tooSmallBudget = std::make_shared<TSnapshotInstallBudget>(39);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = tooSmallBudget;
    Reject([&] { TRaft raft(state, 1, TNodeDict{}, {}, limits); });
    assert_int_equal(tooSmallBudget->UsedBytes(), 0);
    assert_false(bool(state->GetSnapshot()->HeapReservation));

    auto budget = std::make_shared<TSnapshotInstallBudget>(40);
    limits.SnapshotInstallBudget = budget;
    {
        TRaft raft(state, 1, TNodeDict{}, {}, limits);
        assert_int_equal(budget->UsedBytes(), image.DataSize());
        assert_true(*state->GetSnapshot() == image);
    }
    // The storage snapshot outlives the Raft object and keeps its lease.
    assert_int_equal(budget->UsedBytes(), image.DataSize());
    state.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_recovered_source_snapshot_is_charged_to_heap_budget(void**) {
    auto state = std::make_shared<TState>();
    TSnapshot image{1, 1, {1}, {}};
    image.DataSource = std::make_shared<TStringSnapshotDataSource>(std::string(40, 's'));
    state->InstallSnapshot(image);

    auto tooSmallBudget = std::make_shared<TSnapshotInstallBudget>(39);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = tooSmallBudget;
    Reject([&] { TRaft raft(state, 1, TNodeDict{}, {}, limits); });
    assert_int_equal(tooSmallBudget->UsedBytes(), 0);
    assert_false(bool(state->GetSnapshot()->HeapReservation));

    auto budget = std::make_shared<TSnapshotInstallBudget>(40);
    limits.SnapshotInstallBudget = budget;
    {
        TRaft raft(state, 1, TNodeDict{}, {}, limits);
        assert_int_equal(budget->UsedBytes(), 40);
        assert_true(*state->GetSnapshot() == image);
    }
    assert_int_equal(budget->UsedBytes(), 40);
    state.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_snapshot_materialization_reserves_temporary_heap_bytes(void**) {
    TSnapshot snapshot{1, 1, {1}, {}};
    snapshot.DataSource = std::make_shared<TStringSnapshotDataSource>(std::string(40, 's'));
    auto tooSmallBudget = std::make_shared<TSnapshotInstallBudget>(79);
    snapshot.HeapReservation = tooSmallBudget->TryReserve(40);
    snapshot.HeapReservationBudget = tooSmallBudget;
    TBoundedSnapshotApplication rejected;
    Reject([&] { PrepareApplicationSnapshotInstallation(rejected, snapshot); });
    Reject([&] { InstallApplicationSnapshot(rejected, snapshot, {}); });
    assert_int_equal(tooSmallBudget->UsedBytes(), 40);
    snapshot.HeapReservation.reset();
    assert_int_equal(tooSmallBudget->UsedBytes(), 0);

    auto budget = std::make_shared<TSnapshotInstallBudget>(80);
    snapshot.HeapReservation = budget->TryReserve(40);
    snapshot.HeapReservationBudget = budget;
    THeapBudgetObservingSnapshotApplication accepted;
    accepted.Budget = budget;
    auto installation = PrepareApplicationSnapshotInstallation(accepted, snapshot);
    assert_false(bool(installation));
    assert_int_equal(accepted.ValidationHeapBytes, 80);
    assert_int_equal(budget->UsedBytes(), 40);
    InstallApplicationSnapshot(accepted, snapshot, std::move(installation));
    assert_int_equal(accepted.LoadHeapBytes, 80);
    assert_int_equal(accepted.LastAppliedIndex, snapshot.Index);
    assert_int_equal(budget->UsedBytes(), 40);
    snapshot.HeapReservation.reset();
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_recovered_file_snapshot_budgets_fallback_materialization(void**) {
    auto state = std::make_shared<TState>();
    TSnapshot image{1, 1, {1}, {}};
    image.DataSource = std::make_shared<TUnchargedStringSnapshotDataSource>(std::string(40, 's'));
    state->InstallSnapshot(image);

    auto tooSmallBudget = std::make_shared<TSnapshotInstallBudget>(39);
    TRaft::TLogLimits limits;
    limits.SnapshotInstallBudget = tooSmallBudget;
    TRaft rejectedRaft(state, 1, TNodeDict{}, {}, limits);
    TBoundedSnapshotApplication rejected;
    const auto rejectedImage = state->GetSnapshot();
    Reject([&] { RestoreSnapshot(*rejectedImage, rejectedRaft, rejected); });
    assert_int_equal(rejected.LastAppliedIndex, 0);
    assert_int_equal(tooSmallBudget->UsedBytes(), 0);

    auto budget = std::make_shared<TSnapshotInstallBudget>(40);
    limits.SnapshotInstallBudget = budget;
    TRaft raft(state, 1, TNodeDict{}, {}, limits);
    TBoundedSnapshotApplication accepted;
    const auto recoveredImage = state->GetSnapshot();
    RestoreSnapshot(*recoveredImage, raft, accepted);
    assert_int_equal(accepted.LastAppliedIndex, 1);
    assert_int_equal(budget->UsedBytes(), 0);
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
    bool ManifestTransfer = false;
    bool AsymmetricManifest = false;
    bool SnapshotTransferResume = false;
    uint64_t MaxSnapshotImageBytes = TSnapshot::MaxBytes;
    std::string SnapshotStagingDirectory;
    explicit TCluster(std::vector<uint32_t> learners = {}, uint32_t nodeCount = 3,
            bool manifestTransfer = false, uint64_t maxSnapshotImageBytes = TSnapshot::MaxBytes,
            std::string snapshotStagingDirectory = {}, bool asymmetricManifest = false,
            bool snapshotTransferResume = false)
        : NodeCount(nodeCount), Nodes(nodeCount), Apps(nodeCount), Processors(nodeCount),
          Learners(std::move(learners)), ManifestTransfer(manifestTransfer),
          AsymmetricManifest(asymmetricManifest), SnapshotTransferResume(snapshotTransferResume),
          MaxSnapshotImageBytes(maxSnapshotImageBytes),
          SnapshotStagingDirectory(std::move(snapshotStagingDirectory)) {
        for (uint32_t id = 1; id <= NodeCount; ++id) Build(id, std::make_shared<TState>());
    }
    void Build(uint32_t id, std::shared_ptr<IState> state) {
        TNodeDict peers;
        for (uint32_t dst = 1; dst <= NodeCount; ++dst) if (dst != id) {
            const bool manifestTransfer = AsymmetricManifest ? dst == 1 : ManifestTransfer;
            peers[dst] = std::make_shared<TEndpoint>(
                [this](auto message) { Packets.push_back(std::move(message)); }, manifestTransfer,
                SnapshotTransferResume);
        }
        Processors[id - 1].reset();
        TRaft::TLogLimits limits;
        limits.MaxSnapshotImageBytes = MaxSnapshotImageBytes;
        limits.SnapshotStagingDirectory = SnapshotStagingDirectory;
        Nodes[id - 1] = std::make_shared<TRaft>(state, id, peers, Learners, limits);
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

    auto budget = TSnapshotInstallBudget::DefaultProcessBudget();
    const auto usedBefore = budget->UsedBytes();
    const auto snapshot = cluster.Nodes[0]->CompactShared();
    const auto stored = cluster.Nodes[0]->GetState()->GetSnapshot();
    assert_true(snapshot == stored);
    assert_int_equal(budget->UsedBytes(), usedBefore + snapshot->HeapBytes());
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
void test_restarted_follower_reacquires_compaction_certificate_without_new_term(void**) {
    TCluster cluster;
    cluster.Elect(1);
    cluster.Pump(1);

    auto readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_true(readiness.IsActiveVoter);
    assert_true(readiness.HasLeaderAcknowledgementBarrier);
    assert_true(readiness.Ready());
    const auto term = cluster.Nodes[0]->GetState()->CurrentTerm;

    // Restart drops volatile Raft certificates while retaining durable term,
    // vote, commit, and log state.
    cluster.Restart(2);
    readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_true(readiness.IsActiveVoter);
    assert_true(readiness.RequiresLeaderAcknowledgementBarrier);
    assert_false(readiness.HasLeaderAcknowledgementBarrier);
    assert_false(readiness.Ready());
    cluster.Processors[1]->ProcessCommitted();
    Reject([&] { cluster.Nodes[1]->Compact(); });

    // The leader refreshes the certificate every 30 seconds. Advance virtual
    // time in heartbeat-sized steps so it maintains quorum throughout.
    const auto refreshRounds = int(TTimeout::ConfigurationBarrierRefresh.count() / 100) + 20;
    cluster.Pump(1, refreshRounds);
    assert_int_equal(cluster.Nodes[0]->CurrentStateName(), EState::LEADER);
    assert_int_equal(cluster.Nodes[0]->GetState()->CurrentTerm, term);
    assert_int_equal(cluster.Nodes[1]->GetState()->CurrentTerm, term);
    readiness = cluster.Nodes[1]->CheckCompactionMembershipReadiness();
    assert_true(readiness.HasLeaderAcknowledgementBarrier);
    assert_true(readiness.Ready());
}
void test_snapshot_size_negotiation_keeps_legacy_fallback_and_certifies_voter_limit(void**) {
    constexpr auto legacyLimit = uint64_t(TSnapshot::LegacyMaxBytes);
    constexpr auto currentLimit = uint64_t(TSnapshot::MaxBytes);
    const auto legacyPadding = uint32_t(0);
    const auto emptyNegotiatedPadding = TAppendEntriesResponse::SupportsSnapshotSizeNegotiation;
    const auto currentPadding = TAppendEntriesResponse::EncodeSnapshotMaxBytes(
        uint32_t(TSnapshot::MaxBytes));
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(legacyPadding, legacyLimit),
        legacyLimit);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(
        emptyNegotiatedPadding, legacyLimit), 0);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(currentPadding, legacyLimit),
        currentLimit);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(
        currentPadding | TAppendEntriesResponse::SupportsJointElectionFence, legacyLimit), currentLimit);
    assert_int_equal(TAppendEntriesResponse::EncodeConfigurationBarrierSnapshotLimit(
        legacyLimit, legacyLimit), 0);
    assert_int_equal(TAppendEntriesResponse::EncodeConfigurationBarrierSnapshotLimit(
        currentLimit, legacyLimit), currentLimit / TAppendEntriesResponse::SnapshotSizeUnitBytes);

    TCluster cluster;
    cluster.Elect(1);
    cluster.Pump(1);
    assert_int_equal(cluster.Nodes[0]->GetVolatileState()->PeerCapabilities.at(2).MaxSnapshotBytes,
        currentLimit);
    assert_int_equal(cluster.Nodes[0]->GetVolatileState()->PeerCapabilities.at(3).MaxSnapshotBytes,
        currentLimit);
    assert_int_equal(cluster.Nodes[1]->GetVolatileState()->CompactionSafeMaxSnapshotBytes, currentLimit);
    assert_int_equal(cluster.Nodes[2]->GetVolatileState()->CompactionSafeMaxSnapshotBytes, currentLimit);

    auto* firstFollowerState = const_cast<TVolatileState*>(cluster.Nodes[1]->GetVolatileState());
    firstFollowerState->CompactionSafeMaxSnapshotBytes = legacyLimit;
    auto impossibleCertificate = NewHoldedMessage(
        TMessageEx{.Src = 1, .Dst = 2, .Term = cluster.Nodes[0]->GetState()->CurrentTerm, .Seqno = 999},
        TConfigurationBarrierRequest{
            .LeaderId = 1,
            .Padding = uint32_t(currentLimit / TAppendEntriesResponse::SnapshotSizeUnitBytes + 1),
            .ConfigurationIndex = 0,
            .ConfigurationTerm = 0,
        });
    cluster.Nodes[1]->Process(cluster.Now, impossibleCertificate);
    assert_int_equal(firstFollowerState->CompactionSafeMaxSnapshotBytes, legacyLimit);
    assert_true(cluster.Packets.empty());
    firstFollowerState->CompactionSafeMaxSnapshotBytes = currentLimit;

    // A barrier-capable peer that predates size negotiation sends no size bit.
    // Its 64 MiB limit must be represented by the old zero-padding certificate.
    cluster.Now += TTimeout::Election / 2;
    cluster.Nodes[0]->ProcessTimeout(cluster.Now);
    const auto oldPeerAppend = cluster.Take(1, 2, EMessageType::APPEND_ENTRIES_REQUEST)
        .Cast<TAppendEntriesRequest>();
    const auto currentPeerAppend = cluster.Take(1, 3, EMessageType::APPEND_ENTRIES_REQUEST);
    const auto oldPeerResponse = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = oldPeerAppend->Term, .Seqno = oldPeerAppend->Seqno},
        TAppendEntriesResponse{
            .MatchIndex = oldPeerAppend->PrevLogIndex + oldPeerAppend->Nentries,
            .Success = 1,
            .Padding = TAppendEntriesResponse::SupportsConfigurationBarrier |
                TAppendEntriesResponse::SupportsInstallSnapshot |
                TAppendEntriesResponse::SupportsApplicationSnapshotInstall,
        });
    cluster.Deliver(oldPeerResponse);
    cluster.Deliver(currentPeerAppend);
    cluster.Deliver(cluster.Take(3, 1, EMessageType::APPEND_ENTRIES_RESPONSE));

    auto oldPeerBarrier = cluster.Take(1, 2, EMessageType::CONFIGURATION_BARRIER_REQUEST)
        .Cast<TConfigurationBarrierRequest>();
    auto currentPeerBarrier = cluster.Take(1, 3, EMessageType::CONFIGURATION_BARRIER_REQUEST)
        .Cast<TConfigurationBarrierRequest>();
    assert_int_equal(oldPeerBarrier->Padding,
        TAppendEntriesResponse::SupportsApplicationSnapshotInstall);
    assert_int_equal(currentPeerBarrier->Padding,
        TAppendEntriesResponse::SupportsApplicationSnapshotInstall);
    cluster.Deliver(oldPeerBarrier);
    cluster.Deliver(currentPeerBarrier);
    assert_int_equal(cluster.Nodes[1]->GetVolatileState()->CompactionSafeMaxSnapshotBytes, legacyLimit);
    assert_int_equal(cluster.Nodes[2]->GetVolatileState()->CompactionSafeMaxSnapshotBytes, legacyLimit);
}
void test_configuration_barrier_respects_each_voters_manifest_capability(void**) {
    // The follower can accept a 256 MiB manifest from this leader, so its
    // response advertises that budget. The leader cannot send manifests to
    // this follower, though, and must certify only the legacy 128 MiB limit.
    TCluster cluster({}, 3, false, TSnapshot::MaxStreamedBytes,
        std::filesystem::temp_directory_path().string(), true);
    cluster.Elect(1);
    cluster.Pump(1);

    assert_int_equal(cluster.Nodes[0]->GetVolatileState()->PeerCapabilities.at(2).MaxSnapshotBytes,
        TSnapshot::MaxStreamedBytes);
    assert_int_equal(cluster.Nodes[0]->GetVolatileState()->PeerCapabilities.at(3).MaxSnapshotBytes,
        TSnapshot::MaxStreamedBytes);
    assert_int_equal(cluster.Nodes[1]->GetVolatileState()->CompactionSafeMaxSnapshotBytes,
        TSnapshot::MaxBytes);
    assert_int_equal(cluster.Nodes[2]->GetVolatileState()->CompactionSafeMaxSnapshotBytes,
        TSnapshot::MaxBytes);
}
void test_snapshot_size_capability_respects_receiver_budgets(void**) {
    constexpr uint64_t unit = TAppendEntriesResponse::SnapshotSizeUnitBytes;
    constexpr uint64_t heapLimit = 64 * 1024 * 1024;
    // A legacy RSM needs the received split payload and a second materialized
    // copy during installation, so advertise only the budgeted half.
    constexpr uint64_t advertisedLimit = ((heapLimit / 2) / unit) * unit;
    auto budget = std::make_shared<TSnapshotInstallBudget>(heapLimit);
    auto state = std::make_shared<TState>();
    state->BindVoters({1, 2});
    state->CurrentTerm = 3;
    state->Commit();
    std::vector<TMessageHolder<TInstallSnapshotResponse>> snapshotResponses;
    TMessageHolder<TAppendEntriesResponse> appendResponse;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (auto append = message.template Maybe<TAppendEntriesResponse>())
            appendResponse = append.Cast();
        if (auto snapshot = message.template Maybe<TInstallSnapshotResponse>())
            snapshotResponses.push_back(snapshot.Cast());
    }, true);
    TRaft::TLogLimits limits{.SnapshotInstallBudget = budget};
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}},
        std::vector<uint32_t>{}, limits);
    auto application = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, application, {});

    auto heartbeat = NewHoldedMessage(
        TMessageEx{.Src = 1, .Dst = 2, .Term = 3, .Seqno = 1},
        TAppendEntriesRequest{.PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 0,
            .LeaderId = 1, .Nentries = 0});
    raft->Process({}, heartbeat);
    assert_non_null(appendResponse);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(
        appendResponse->Padding, TSnapshot::LegacyMaxBytes), advertisedLimit);

    const TSnapshot snapshot{1, 1, {1, 2}, {}};
    TSnapshotTransferManifest manifest;
    manifest.Version = TSnapshotTransferManifest::CurrentVersion;
    manifest.Metadata.assign(44, 'x');
    manifest.PayloadBytes = advertisedLimit - manifest.Metadata.size() + 1;
    auto tooLarge = NewHoldedMessage<TInstallSnapshotRangeRequest>(
        sizeof(TInstallSnapshotRangeRequest) + manifest.Metadata.size());
    tooLarge->Src = 1;
    tooLarge->Dst = 2;
    tooLarge->Term = 3;
    tooLarge->Seqno = 2;
    tooLarge->SnapshotIndex = snapshot.Index;
    tooLarge->SnapshotTerm = snapshot.Term;
    tooLarge->MetadataBytes = manifest.Metadata.size();
    tooLarge->PayloadBytes = manifest.PayloadBytes;
    tooLarge->Offset = 0;
    tooLarge->LeaderId = 1;
    tooLarge->Phase = uint32_t(TInstallSnapshotRangeRequest::EPhase::Metadata);
    tooLarge->ManifestVersion = manifest.Version;
    tooLarge->Checksum = manifest.Checksum;
    memcpy(tooLarge->Data, manifest.Metadata.data(), manifest.Metadata.size());
    raft->Process({}, tooLarge);
    assert_int_equal(snapshotResponses.size(), 1);
    assert_false(snapshotResponses.back()->Success);
    assert_int_equal(snapshotResponses.back()->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 0);

    constexpr uint64_t stagingLimit = 16 * 1024 * 1024;
    auto stagedState = std::make_shared<TState>();
    stagedState->BindVoters({1, 2});
    stagedState->CurrentTerm = 3;
    stagedState->Commit();
    TMessageHolder<TAppendEntriesResponse> stagedAppendResponse;
    auto stagedPeer = std::make_shared<TEndpoint>([&](auto message) {
        if (auto append = message.template Maybe<TAppendEntriesResponse>())
            stagedAppendResponse = append.Cast();
    });
    TRaft::TLogLimits stagedLimits{.SnapshotInstallBudget = std::make_shared<TSnapshotInstallBudget>(heapLimit),
        .SnapshotStagingDirectory = "/tmp",
        .SnapshotStagingBudget = std::make_shared<TSnapshotInstallBudget>(stagingLimit)};
    auto stagedRaft = std::make_shared<TRaft>(stagedState, 2, TNodeDict{{1, stagedPeer}},
        std::vector<uint32_t>{}, stagedLimits);
    auto stagedApplication = std::make_shared<TStreamingInstallApplication>();
    TRequestProcessor stagedProcessor(stagedRaft, stagedApplication, {});
    auto stagedHeartbeat = NewHoldedMessage(
        TMessageEx{.Src = 1, .Dst = 2, .Term = 3, .Seqno = 1},
        TAppendEntriesRequest{.PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 0,
            .LeaderId = 1, .Nentries = 0});
    stagedRaft->Process({}, stagedHeartbeat);
    assert_non_null(stagedAppendResponse);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(
        stagedAppendResponse->Padding, TSnapshot::LegacyMaxBytes), stagingLimit);

    constexpr uint64_t configuredLimit = 32 * 1024;
    auto configuredState = std::make_shared<TState>();
    configuredState->BindVoters({1, 2});
    configuredState->CurrentTerm = 3;
    configuredState->Commit();
    TMessageHolder<TAppendEntriesResponse> configuredAppendResponse;
    TMessageHolder<TInstallSnapshotResponse> configuredSnapshotResponse;
    auto configuredPeer = std::make_shared<TEndpoint>([&](auto message) {
        if (auto append = message.template Maybe<TAppendEntriesResponse>())
            configuredAppendResponse = append.Cast();
        if (auto snapshot = message.template Maybe<TInstallSnapshotResponse>())
            configuredSnapshotResponse = snapshot.Cast();
    });
    auto configuredBudget = std::make_shared<TSnapshotInstallBudget>(64 * 1024);
    TRaft::TLogLimits configuredLimits{.SnapshotInstallBudget = configuredBudget,
        .MaxSnapshotImageBytes = configuredLimit};
    auto configuredRaft = std::make_shared<TRaft>(configuredState, 2,
        TNodeDict{{1, configuredPeer}}, std::vector<uint32_t>{}, configuredLimits);
    auto configuredApplication = std::make_shared<TApplication>();
    TRequestProcessor configuredProcessor(configuredRaft, configuredApplication, {});
    auto configuredHeartbeat = NewHoldedMessage(
        TMessageEx{.Src = 1, .Dst = 2, .Term = 3, .Seqno = 1},
        TAppendEntriesRequest{.PrevLogIndex = 0, .PrevLogTerm = 0, .LeaderCommit = 0,
            .LeaderId = 1, .Nentries = 0});
    configuredRaft->Process({}, configuredHeartbeat);
    assert_non_null(configuredAppendResponse);
    assert_int_equal(TAppendEntriesResponse::DecodeSnapshotMaxBytesOrLegacy(
        configuredAppendResponse->Padding, TSnapshot::LegacyMaxBytes), 0);

    const TSnapshot configuredSnapshot{1, 1, {1, 2}, std::string(configuredLimit, 'p')};
    const auto configuredImage = EncodeSnapshot(configuredSnapshot);
    assert_true(configuredImage.size() > configuredLimit);
    assert_true(configuredImage.size() <= TInstallSnapshotRequest::MaxChunkBytes);
    auto configuredChunk = NewHoldedMessage<TInstallSnapshotRequest>(
        sizeof(TInstallSnapshotRequest) + configuredImage.size());
    configuredChunk->Src = 1; configuredChunk->Dst = 2; configuredChunk->Term = 3;
    configuredChunk->Seqno = 1; configuredChunk->SnapshotIndex = configuredSnapshot.Index;
    configuredChunk->SnapshotTerm = configuredSnapshot.Term; configuredChunk->Offset = 0;
    configuredChunk->TotalBytes = configuredImage.size();
    configuredChunk->LeaderId = 1; configuredChunk->Done = TInstallSnapshotRequest::DoneFlag;
    memcpy(configuredChunk->Data, configuredImage.data(), configuredImage.size());
    configuredRaft->Process({}, configuredChunk);
    assert_non_null(configuredSnapshotResponse);
    assert_false(configuredSnapshotResponse->Success);
    assert_int_equal(configuredSnapshotResponse->NextOffset, 0);
    assert_int_equal(configuredBudget->UsedBytes(), 0);
}
void test_snapshot_encoder_accepts_image_above_legacy_limit(void**) {
    TSnapshot snapshot;
    snapshot.Index = 1;
    snapshot.Term = 1;
    snapshot.Voters = {1};
    snapshot.DataSource = std::make_shared<TGeneratedSnapshotDataSource>(
        TSnapshot::LegacyMaxBytes + 1);

    const auto encoding = PrepareSnapshotEncoding(snapshot);
    assert_true(SnapshotEncodingSize(*encoding) > TSnapshot::LegacyMaxBytes);
    assert_true(SnapshotEncodingSize(*encoding) <= TSnapshot::MaxBytes);
}
void test_stale_peer_capability_cannot_authorize_snapshot_or_barrier(void**) {
    auto state = std::make_shared<TState>();
    state->BindVoters({1, 2});
    state->CurrentTerm = 2;
    state->InstallSnapshot(TSnapshot{1, 1, {1, 2}, "snapshot"});

    std::vector<TMessageHolder<TMessage>> sent;
    auto peer = std::make_shared<TEndpoint>([&](auto message) { sent.push_back(std::move(message)); });
    auto raft = std::make_shared<TRaft>(state, uint32_t(1), TNodeDict{{2, peer}});
    raft->SetLocalApplicationSnapshotInstallSupport(true);
    raft->Become(EState::LEADER);

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->NextIndex[2] = state->SnapshotIndex;
    volatileState->CommitAckIndex[2] = state->CommitIndex;
    volatileState->LastSentSeqno[2] = 7;
    volatileState->PeerCapabilities[2] = {
        .AppendAckSeqno = 7,
        .Term = state->CurrentTerm - 1,
        .ConfigurationBarrier = true,
        .InstallSnapshot = true,
        .ApplicationSnapshotInstall = true,
    };

    raft->ProcessTimeout(ITimeSource::Time{});
    assert_false(sent.empty());
    bool probedWithAppendEntries = false;
    for (const auto& message : sent) {
        assert_int_not_equal(message->Type, uint32_t(EMessageType::INSTALL_SNAPSHOT_REQUEST));
        assert_int_not_equal(message->Type, uint32_t(EMessageType::CONFIGURATION_BARRIER_REQUEST));
        probedWithAppendEntries |= message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST);
    }
    assert_true(probedWithAppendEntries);
}
void test_single_voter_compacts_snapshot_above_legacy_limit(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    auto raft = std::make_shared<TRaft>(state, uint32_t(1), TNodeDict{});
    auto application = std::make_shared<TLargeSourceSnapshotApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, {});
    const auto snapshot = raft->CompactShared();

    assert_true(snapshot->DataSize() > TSnapshot::LegacyMaxBytes);
    assert_true(snapshot->DataSize() <= TSnapshot::MaxBytes);
    assert_int_equal(state->SnapshotIndex, 1);
}
void test_learner_without_voter_size_certificate_keeps_legacy_compaction_limit(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 1;
    state->BindVoters({1, 2});
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 1;
    state->Append(entry);
    state->CommitIndex = 1;

    TNodeDict peers{
        {1, std::make_shared<TEndpoint>([](auto) { })},
        {2, std::make_shared<TEndpoint>([](auto) { })},
    };
    auto raft = std::make_shared<TRaft>(state, uint32_t(3), peers, std::vector<uint32_t>{3});
    auto application = std::make_shared<TLargeSourceSnapshotApplication>();
    application->LastAppliedIndex = 1;
    TRequestProcessor processor(raft, application, peers);

    Reject([&] { raft->Compact(); });
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(state->LastLogIndex, 1);
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
                if (id != 5) features |= TAppendEntriesResponse::SupportsInstallSnapshot |
                    TAppendEntriesResponse::SupportsApplicationSnapshotInstall;
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
void test_outgoing_snapshot_chunk_holds_heap_budget_lease(void**) {
    auto budget = TSnapshotInstallBudget::DefaultProcessBudget();
    TCluster cluster;
    cluster.Prepare();
    const auto retainedBytes = budget->UsedBytes();
    auto chunk = cluster.SendChunk();
    assert_true(chunk->Len > sizeof(TInstallSnapshotRequest));
    assert_int_equal(budget->UsedBytes(), retainedBytes + chunk->Len);
    chunk = {};
    assert_int_equal(budget->UsedBytes(), retainedBytes);
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
void test_snapshot_sender_accepts_negotiated_retained_prefix_after_cursor_reset(void**) {
    TCluster cluster({}, 3, false, TSnapshot::MaxBytes, {}, false, true);
    cluster.Prepare();

    auto first = cluster.SendChunk();
    cluster.Deliver(first);
    auto firstAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE);
    assert_true(firstAck.Cast<TInstallSnapshotResponse>()->Success);
    cluster.Deliver(firstAck);

    auto second = cluster.SendChunk();
    cluster.Deliver(second);
    auto secondAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
        .Cast<TInstallSnapshotResponse>();
    assert_true(secondAck->Success);
    assert_true(secondAck->NextOffset > first->Len - sizeof(TInstallSnapshotRequest));
    assert_false(secondAck->Installed);

    // Model a sender that lost its in-memory cursor after the receiver accepted
    // more bytes. It starts again at zero with a fresh sequence number.
    auto* progress = const_cast<TVolatileState*>(cluster.Nodes[0]->GetVolatileState());
    auto& rpc = progress->SnapshotRpcs.at(3);
    rpc.Offset = 0;
    rpc.Seqno = 0;
    progress->RpcDue[3] = {};
    auto replay = cluster.SendChunk();
    assert_int_equal(replay->Offset, 0);
    cluster.Deliver(replay);
    auto resumedAck = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE);
    assert_int_equal(resumedAck.Cast<TInstallSnapshotResponse>()->NextOffset,
        secondAck->NextOffset);
    cluster.Deliver(resumedAck);

    auto next = cluster.SendChunk();
    assert_int_equal(next->Offset, secondAck->NextOffset);
    size_t chunks = 0;
    while (true) {
        assert_true(++chunks < 10);
        auto ack = cluster.AckChunk(next);
        if (ack->Installed) break;
        next = cluster.SendChunk();
        assert_int_equal(next->Offset, ack->NextOffset);
    }
    assert_int_equal(cluster.Apps[2]->Loads, 1);
    assert_int_equal(cluster.Nodes[2]->GetState()->SnapshotIndex, first->SnapshotIndex);
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
    assert_true(readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport == (std::vector<uint32_t>{3}));
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
    assert_true(capability.ApplicationSnapshotInstall);
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

TMessageHolder<TInstallSnapshotRequest> Chunk(const TSnapshot& snapshot, std::string_view bytes,
        uint64_t offset, uint32_t count, bool jointElectionFenceLeader = false) {
    auto message = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + count);
    message->Src = 1; message->Dst = 2; message->Term = 3; message->Seqno = offset + 1;
    message->SnapshotIndex = snapshot.Index; message->SnapshotTerm = snapshot.Term;
    message->Offset = offset; message->TotalBytes = bytes.size();
    message->Done = (offset + count == bytes.size() ? TInstallSnapshotRequest::DoneFlag : 0u) |
        (jointElectionFenceLeader ? TInstallSnapshotRequest::JointElectionFenceLeader : 0u);
    message->LeaderId = 1; memcpy(message->Data, bytes.data() + offset, count); return message;
}
TMessageHolder<TInstallSnapshotRangeRequest> RangeChunk(const TSnapshot& snapshot,
        const TSnapshotTransferManifest& manifest, TInstallSnapshotRangeRequest::EPhase phase,
        uint64_t offset, std::string_view bytes, uint64_t sequence) {
    auto message = NewHoldedMessage<TInstallSnapshotRangeRequest>(
        sizeof(TInstallSnapshotRangeRequest) + bytes.size());
    message->Src = 1; message->Dst = 2; message->Term = 3; message->Seqno = sequence;
    message->SnapshotIndex = snapshot.Index; message->SnapshotTerm = snapshot.Term;
    message->MetadataBytes = manifest.Metadata.size(); message->PayloadBytes = manifest.PayloadBytes;
    message->Offset = offset; message->LeaderId = 1; message->Phase = uint32_t(phase);
    message->ManifestVersion = manifest.Version; message->Checksum = manifest.Checksum;
    memcpy(message->Data, bytes.data(), bytes.size());
    return message;
}
void test_snapshot_manifest_ranges_install_with_separate_payload_staging(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-range-install-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};
#endif
    TKv source;
    const std::string value(40000, 'x');
    source.Write(source.Prepare(Command("a", value)), 1);
    source.Write(source.Prepare(Command("b", std::string(40000, 'y'))), 2);
    source.Write(source.Prepare(Command("c", std::string(40000, 'z'))), 3);
    const TSnapshot snapshot{3, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            responses.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    TRaft::TLogLimits limits;
#ifdef __linux__
    limits.SnapshotStagingDirectory = createdDirectory;
#endif
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}},
        std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});

    uint64_t sequence = 1;
    uint64_t expectedImageOffset = 0;
    auto transfer = [&](TInstallSnapshotRangeRequest::EPhase phase, std::string_view bytes) {
        for (uint64_t offset = 0; offset < bytes.size();) {
            const auto count = size_t(std::min<uint64_t>(TInstallSnapshotRangeRequest::MaxChunkBytes,
                bytes.size() - offset));
            auto request = RangeChunk(snapshot, manifest, phase, offset,
                bytes.substr(size_t(offset), count), sequence++);
            raft->Process({}, request);
            assert_int_equal(responses.size(), size_t(sequence - 1));
            assert_true(responses.back()->Success);
            expectedImageOffset += count;
            assert_int_equal(responses.back()->NextOffset, expectedImageOffset);
            const bool final = expectedImageOffset == manifest.Metadata.size() +
                manifest.PayloadBytes + 4;
            assert_int_equal(responses.back()->Installed, final);
            offset += count;
        }
    };

    transfer(TInstallSnapshotRangeRequest::EPhase::Metadata, manifest.Metadata);
    assert_int_equal(app->Loads, 0);
    transfer(TInstallSnapshotRangeRequest::EPhase::Payload, snapshot.Data);
    assert_int_equal(app->Loads, 0);
    std::string checksum(4, '\0');
    for (unsigned i = 0; i < 4; ++i) checksum[i] = char(manifest.Checksum >> (8 * i));
    transfer(TInstallSnapshotRangeRequest::EPhase::Checksum, checksum);
    assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "a") == value);
    assert_true(*state->GetSnapshot() == snapshot);
#ifdef __linux__
    assert_true(state->GetSnapshot()->Data.empty());
    assert_true(bool(state->GetSnapshot()->DataSource));
    auto sourceUsage = dynamic_cast<const ISnapshotDataHeapUsage*>(
        state->GetSnapshot()->DataSource.get());
    assert_non_null(sourceUsage);
    assert_int_equal(sourceUsage->HeapBytes(), 0);
#endif
}
void test_snapshot_manifest_ranges_reject_gaps_conflicts_and_corruption(void**) {
#ifdef __linux__
    char directoryTemplate[] = "/tmp/miniraft-range-failures-XXXXXX";
    auto* createdDirectory = mkdtemp(directoryTemplate);
    assert_non_null(createdDirectory);
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{createdDirectory};
#endif
    TKv source;
    const std::string value(40000, 'x');
    source.Write(source.Prepare(Command("a", value)), 1);
    source.Write(source.Prepare(Command("b", std::string(40000, 'y'))), 2);
    source.Write(source.Prepare(Command("c", std::string(40000, 'z'))), 3);
    const TSnapshot snapshot{3, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            responses.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    TRaft::TLogLimits limits;
#ifdef __linux__
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(manifest.PayloadBytes * 2);
    limits.SnapshotStagingDirectory = createdDirectory;
    limits.SnapshotStagingBudget = stagingBudget;
#endif
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}},
        std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});

    uint64_t sequence = 1;
    auto send = [&](TInstallSnapshotRangeRequest::EPhase phase, uint64_t offset,
            std::string_view bytes) {
        const auto expectedResponses = responses.size() + 1;
        raft->Process({}, RangeChunk(snapshot, manifest, phase, offset, bytes, sequence++));
        assert_int_equal(responses.size(), expectedResponses);
        return responses.back();
    };
    auto sendPayload = [&](std::string_view payload) {
        for (uint64_t offset = 0; offset < payload.size();) {
            const auto count = size_t(std::min<uint64_t>(TInstallSnapshotRangeRequest::MaxChunkBytes,
                payload.size() - offset));
            auto response = send(TInstallSnapshotRangeRequest::EPhase::Payload, offset,
                payload.substr(size_t(offset), count));
            assert_true(response->Success);
            assert_int_equal(response->NextOffset, manifest.Metadata.size() + offset + count);
            offset += count;
        }
    };
    auto sendChecksum = [&] {
        std::string checksum(4, '\0');
        for (unsigned i = 0; i < 4; ++i) checksum[i] = char(manifest.Checksum >> (8 * i));
        return send(TInstallSnapshotRangeRequest::EPhase::Checksum, 0, checksum);
    };
    auto sendMetadata = [&] {
        auto response = send(TInstallSnapshotRangeRequest::EPhase::Metadata, 0, manifest.Metadata);
        assert_true(response->Success);
        assert_false(response->Installed);
        assert_int_equal(response->NextOffset, manifest.Metadata.size());
    };

    sendMetadata();
    auto gap = send(TInstallSnapshotRangeRequest::EPhase::Payload, 1, "x");
    assert_false(gap->Success);
    assert_int_equal(app->Loads, 0);

    const auto firstPayloadBytes = size_t(std::min<uint64_t>(
        TInstallSnapshotRangeRequest::MaxChunkBytes, manifest.PayloadBytes));
    auto accepted = send(TInstallSnapshotRangeRequest::EPhase::Payload, 0,
        std::string_view(snapshot.Data).substr(0, firstPayloadBytes));
    assert_true(accepted->Success);
    assert_int_equal(accepted->NextOffset, manifest.Metadata.size() + firstPayloadBytes);

    auto conflictingMetadata = manifest.Metadata;
    conflictingMetadata[0] ^= 1;
    auto conflict = send(TInstallSnapshotRangeRequest::EPhase::Metadata, 0, conflictingMetadata);
    assert_false(conflict->Success);
#ifdef __linux__
    assert_int_equal(stagingBudget->UsedBytes(), 0);
#endif
    auto staleContinuation = send(TInstallSnapshotRangeRequest::EPhase::Payload,
        firstPayloadBytes, std::string_view(snapshot.Data).substr(firstPayloadBytes, 1));
    assert_false(staleContinuation->Success);
    assert_int_equal(app->Loads, 0);

    sendMetadata();
    auto corruptedPayload = snapshot.Data;
    corruptedPayload[0] ^= 1;
    sendPayload(corruptedPayload);
    auto corruptChecksum = sendChecksum();
    assert_false(corruptChecksum->Success);
    assert_false(corruptChecksum->Installed);
    assert_int_equal(app->Loads, 0);
#ifdef __linux__
    assert_int_equal(stagingBudget->UsedBytes(), 0);
#endif

    sendMetadata();
    sendPayload(snapshot.Data);
    auto installed = sendChecksum();
    assert_true(installed->Success);
    assert_true(installed->Installed);
    assert_int_equal(installed->NextOffset, manifest.Metadata.size() + manifest.PayloadBytes + 4);
    assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "a") == value);
    assert_true(*state->GetSnapshot() == snapshot);
#ifdef __linux__
    assert_int_equal(stagingBudget->UsedBytes(), manifest.PayloadBytes);
#endif
}
void test_negotiated_snapshot_manifest_transfer_uses_distinct_wire_phases(void**) {
    TCluster cluster({3}, 3, true);
    cluster.Prepare();
    std::set<uint32_t> phases;
    bool installed = false;
    for (unsigned attempt = 0; attempt < 12 && !installed; ++attempt) {
        cluster.Now += std::chrono::seconds(3);
        cluster.Nodes[0]->ProcessTimeout(cluster.Now);
        auto request = cluster.Take(1, 3, EMessageType::INSTALL_SNAPSHOT_RANGE_REQUEST)
            .Cast<TInstallSnapshotRangeRequest>();
        ValidateMessageTree(TMessageHolder<TMessage>(request));
        phases.insert(request->Phase);
        cluster.Deliver(request);
        auto response = cluster.Take(3, 1, EMessageType::INSTALL_SNAPSHOT_RESPONSE)
            .Cast<TInstallSnapshotResponse>();
        installed = response->Installed;
        cluster.Deliver(response);
    }
    assert_true(installed);
    assert_true(phases.contains(uint32_t(TInstallSnapshotRangeRequest::EPhase::Metadata)));
    assert_true(phases.contains(uint32_t(TInstallSnapshotRangeRequest::EPhase::Payload)));
    assert_true(phases.contains(uint32_t(TInstallSnapshotRangeRequest::EPhase::Checksum)));
    assert_true(cluster.Nodes[2]->GetState()->SnapshotIndex == cluster.Nodes[0]->GetState()->SnapshotIndex);
    assert_true(Read(*cluster.Apps[2], "a") == std::string(60000, 'a'));
    const auto installedSnapshot = cluster.Nodes[2]->GetState()->GetSnapshot();
    assert_true(installedSnapshot && installedSnapshot->Data.empty() && installedSnapshot->DataSource);
    const auto* heapUsage = dynamic_cast<const ISnapshotDataHeapUsage*>(installedSnapshot->DataSource.get());
    assert_non_null(heapUsage);
    assert_true(heapUsage->HeapBytes() > 0);
}
void test_snapshot_manifest_ranges_use_budgeted_heap_payload_without_file_staging(void**) {
    TKv source;
    source.Write(source.Prepare(Command("key", "heap-backed payload")), 1);
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    const auto totalBytes = manifest.Metadata.size() + manifest.PayloadBytes + 4;
    auto budget = std::make_shared<TSnapshotInstallBudget>(2 * totalBytes);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            responses.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    TRaft::TLogLimits limits{.SnapshotInstallBudget = budget};
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});
    uint64_t sequence = 1;
    auto send = [&](TInstallSnapshotRangeRequest::EPhase phase, uint64_t offset,
            std::string_view bytes) {
        raft->Process({}, RangeChunk(snapshot, manifest, phase, offset, bytes, sequence++));
        assert_false(responses.empty());
        return responses.back();
    };
    auto sendMetadata = [&] {
        auto response = send(TInstallSnapshotRangeRequest::EPhase::Metadata, 0, manifest.Metadata);
        assert_true(response->Success);
        assert_false(response->Installed);
        assert_int_equal(response->NextOffset, manifest.Metadata.size());
    };
    auto sendChecksum = [&] {
        std::string checksum(4, '\0');
        for (unsigned i = 0; i < 4; ++i) checksum[i] = char(manifest.Checksum >> (8 * i));
        return send(TInstallSnapshotRangeRequest::EPhase::Checksum, 0, checksum);
    };

    sendMetadata();
    assert_int_equal(budget->UsedBytes(), totalBytes);
    auto firstPayload = send(TInstallSnapshotRangeRequest::EPhase::Payload, 0,
        std::string_view(snapshot.Data).substr(0, 1));
    assert_true(firstPayload->Success);
    assert_int_equal(firstPayload->NextOffset, manifest.Metadata.size() + 1);
    const char conflictingByte = char(snapshot.Data[0] ^ 1);
    auto conflict = send(TInstallSnapshotRangeRequest::EPhase::Payload, 0,
        std::string_view(&conflictingByte, 1));
    assert_false(conflict->Success);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(app->Loads, 0);

    sendMetadata();
    auto payload = send(TInstallSnapshotRangeRequest::EPhase::Payload, 0, snapshot.Data);
    assert_true(payload->Success);
    assert_int_equal(payload->NextOffset, manifest.Metadata.size() + manifest.PayloadBytes);
    auto installed = sendChecksum();
    assert_true(installed->Success);
    assert_true(installed->Installed);
    assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "key") == "heap-backed payload");
    const auto installedSnapshot = state->GetSnapshot();
    assert_true(installedSnapshot && installedSnapshot->Data.empty() && installedSnapshot->DataSource);
    const auto* heapUsage = dynamic_cast<const ISnapshotDataHeapUsage*>(installedSnapshot->DataSource.get());
    assert_non_null(heapUsage);
    assert_true(heapUsage->HeapBytes() > 0);
    assert_true(heapUsage->HeapBytes() < totalBytes);
    assert_int_equal(budget->UsedBytes(), heapUsage->HeapBytes());
}
void test_snapshot_manifest_without_streaming_installer_reserves_copy_headroom(void**) {
    TKv source;
    source.Write(source.Prepare(Command("key", std::string(4096, 'x'))), 1);
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    const auto totalBytes = manifest.Metadata.size() + manifest.PayloadBytes + 4;
    auto budget = std::make_shared<TSnapshotInstallBudget>(2 * totalBytes - 1);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            responses.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    TRaft::TLogLimits limits{.SnapshotInstallBudget = budget};
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{}, limits);
    auto application = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, application, {});

    auto metadata = RangeChunk(snapshot, manifest, TInstallSnapshotRangeRequest::EPhase::Metadata,
        0, manifest.Metadata, 1);
    raft->Process({}, metadata);

    assert_int_equal(responses.size(), 1);
    assert_false(responses.back()->Success);
    assert_int_equal(responses.back()->NextOffset, 0);
    assert_int_equal(application->Loads, 0);
    assert_int_equal(budget->UsedBytes(), 0);
}
void test_large_versioned_snapshot_installs_over_manifest_ranges(void**) {
    auto payload = std::make_shared<TGeneratedSnapshotDataSource>(TSnapshot::MaxBytes + 1);
    TSnapshot snapshot;
    snapshot.Index = 1;
    snapshot.Term = 1;
    snapshot.Voters = {1, 2};
    snapshot.DataSource = payload;
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    assert_true(manifest.Metadata.substr(0, 8) == "MRFTS012");
    const auto totalBytes = manifest.Metadata.size() + manifest.PayloadBytes + 4;
    assert_true(totalBytes > TSnapshot::MaxBytes);
    assert_true(totalBytes <= TSnapshot::MaxStreamedBytes);

    auto budget = std::make_shared<TSnapshotInstallBudget>(TSnapshot::MaxStreamedBytes);
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::INSTALL_SNAPSHOT_RESPONSE))
            responses.push_back(message.template Cast<TInstallSnapshotResponse>());
    });
    auto state = std::make_shared<TState>();
    state->BindVoters({1, 2});
    state->CurrentTerm = 3;
    state->Commit();
    TRaft::TLogLimits limits{.SnapshotInstallBudget = budget,
        .MaxSnapshotImageBytes = TSnapshot::MaxStreamedBytes};
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}}, std::vector<uint32_t>{}, limits);
    auto application = std::make_shared<TLargeStreamingInstallApplication>();
    application->ExpectedBytes = payload->Size();
    TRequestProcessor processor(raft, application, {});

    uint64_t sequence = 1;
    auto transfer = [&](TInstallSnapshotRangeRequest::EPhase phase, uint64_t offset,
            std::string_view bytes) {
        auto request = RangeChunk(snapshot, manifest, phase, offset, bytes, sequence++);
        raft->Process({}, request);
        assert_false(responses.empty());
        return responses.back();
    };
    auto metadata = transfer(TInstallSnapshotRangeRequest::EPhase::Metadata, 0, manifest.Metadata);
    assert_true(metadata->Success);
    assert_int_equal(metadata->NextOffset, manifest.Metadata.size());

    for (uint64_t offset = 0; offset < manifest.PayloadBytes;) {
        const auto count = size_t(std::min<uint64_t>(TInstallSnapshotRangeRequest::MaxChunkBytes,
            manifest.PayloadBytes - offset));
        const std::string chunk(count, 's');
        auto response = transfer(TInstallSnapshotRangeRequest::EPhase::Payload, offset, chunk);
        offset += count;
        assert_true(response->Success);
        assert_false(response->Installed);
        assert_int_equal(response->NextOffset, manifest.Metadata.size() + offset);
    }
    std::string checksum(4, '\0');
    for (unsigned i = 0; i < 4; ++i) checksum[i] = char(manifest.Checksum >> (8 * i));
    auto installed = transfer(TInstallSnapshotRangeRequest::EPhase::Checksum, 0, checksum);
    assert_true(installed->Success);
    assert_true(installed->Installed);
    assert_int_equal(application->Preparations, 1);
    assert_int_equal(application->LastAppliedIndex, snapshot.Index);
    assert_true(state->GetSnapshot()->DataSize() == payload->Size());
    assert_true(bool(state->GetSnapshot()->DataSource));
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

    auto request = [](uint32_t target, uint64_t total = 128) {
        constexpr std::string_view prefix = "MRFT";
        auto chunk = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + prefix.size());
        chunk->Src = 1; chunk->Dst = target; chunk->Term = 1; chunk->Seqno = 1;
        chunk->SnapshotIndex = 1; chunk->SnapshotTerm = 1;
        chunk->TotalBytes = total; chunk->Offset = 0; chunk->LeaderId = 1; chunk->Done = 0;
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
    assert_int_equal(budget->UsedBytes(), image.size());

    replies.clear();
    raft2->Process(expiredAt, request(2, 200));
    auto retainedSnapshotBlocksAnotherImage = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(retainedSnapshotBlocksAnotherImage->Success);
    assert_false(retainedSnapshotBlocksAnotherImage->Installed);
    assert_int_equal(budget->UsedBytes(), image.size());
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
    constexpr size_t FirstChunkBytes = 4;
    auto first = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + FirstChunkBytes);
    first->Src = 1; first->Dst = 3; first->Term = 1; first->Seqno = 1;
    first->SnapshotIndex = 1; first->SnapshotTerm = 1; first->Offset = 0;
    first->TotalBytes = image.size(); first->Done = 0; first->LeaderId = 1;
    memcpy(first->Data, image.data(), FirstChunkBytes);
    raft3->Process(ITimeSource::Time{}, first);
    auto accepted = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(accepted->Success); assert_false(accepted->Installed);
    assert_int_equal(accepted->NextOffset, FirstChunkBytes);
    assert_int_equal(budget->UsedBytes(), image.size());

    const auto finalChunkBytes = image.size() - FirstChunkBytes;
    auto finalChunk = NewHoldedMessage<TInstallSnapshotRequest>(
        sizeof(TInstallSnapshotRequest) + finalChunkBytes);
    finalChunk->Src = 1; finalChunk->Dst = 3; finalChunk->Term = 1; finalChunk->Seqno = 2;
    finalChunk->SnapshotIndex = 1; finalChunk->SnapshotTerm = 1; finalChunk->Offset = FirstChunkBytes;
    finalChunk->TotalBytes = image.size(); finalChunk->Done = TInstallSnapshotRequest::DoneFlag;
    finalChunk->LeaderId = 1;
    memcpy(finalChunk->Data, image.data() + FirstChunkBytes, finalChunkBytes);
    raft3->Process(ITimeSource::Time{}, finalChunk);
    auto rejected = replies.back().Cast<TInstallSnapshotResponse>();
    assert_false(rejected->Success); assert_false(rejected->Installed);
    assert_int_equal(rejected->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(raft3->GetState()->SnapshotIndex, 0);
    assert_int_equal(rejectingApp->LastAppliedIndex, 0);

    auto raft2 = std::make_shared<TRaft>(std::make_shared<TState>(), 2, peers,
        std::vector<uint32_t>{}, limits);
    auto app2 = std::make_shared<TApplication>();
    TRequestProcessor processor2(raft2, app2, peers);
    constexpr size_t RetryFirstChunkBytes = 4;
    auto partial = NewHoldedMessage<TInstallSnapshotRequest>(sizeof(TInstallSnapshotRequest) + RetryFirstChunkBytes);
    partial->Src = 1; partial->Dst = 2; partial->Term = 1; partial->Seqno = 2;
    partial->SnapshotIndex = 1; partial->SnapshotTerm = 1; partial->Offset = 0;
    partial->TotalBytes = image.size(); partial->Done = 0; partial->LeaderId = 1;
    memcpy(partial->Data, image.data(), RetryFirstChunkBytes);
    replies.clear();
    raft2->Process(ITimeSource::Time{}, partial);
    auto retryAccepted = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(retryAccepted->Success); assert_false(retryAccepted->Installed);
    assert_int_equal(retryAccepted->NextOffset, RetryFirstChunkBytes);
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
    auto application2 = std::make_shared<TBoundedSnapshotApplication>();
    auto application3 = std::make_shared<TBoundedSnapshotApplication>();
    TRequestProcessor processor2(raft2, application2, peers);
    TRequestProcessor processor3(raft3, application3, peers);

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
    // The continuation carries the remaining metadata and application bytes.
    // Only the metadata may count against the shared prefix budget.
    raft3->Process(expiredAt, request(3, image3, 8, image3.size() - 8));
    auto covered = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(covered->Success); assert_true(covered->Installed);
    assert_int_equal(covered->NextOffset, image3.size());
    assert_int_equal(budget->UsedBytes(), 0);

    raft2->Process(expiredAt, request(2, image2, 0, image2.size()));
    covered = replies.back().Cast<TInstallSnapshotResponse>();
    assert_true(covered->Success); assert_true(covered->Installed);
    assert_int_equal(covered->NextOffset, image2.size());
    assert_int_equal(budget->UsedBytes(), 0);
}
void CheckSnapshotInstallChunkSizes(const TRaft::TLogLimits& limits) {
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
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
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
void test_snapshot_install_accepts_every_small_fixed_chunk_size(void**) {
    CheckSnapshotInstallChunkSizes({});
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

void test_snapshot_install_requires_nonzero_sequence(void**) {
    TKv source;
    source.Write(source.Prepare(Command("a", "snapshot")), 5);
    const TSnapshot snapshot{5, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);

    auto run = [&](uint64_t sequence, bool expectInstall) {
        std::vector<TMessageHolder<TInstallSnapshotResponse>> replies;
        auto peer = std::make_shared<TEndpoint>([&](auto message) {
            replies.push_back(message.template Cast<TInstallSnapshotResponse>());
        });
        auto state = std::make_shared<TState>();
        auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}});
        auto app = std::make_shared<TApplication>();
        TRequestProcessor processor(raft, app, {});

        auto request = Chunk(snapshot, bytes, 0, static_cast<uint32_t>(bytes.size()));
        request->Seqno = sequence;
        raft->Process({}, request);

        if (expectInstall) {
            assert_int_equal(replies.size(), 1);
            assert_true(replies.back()->Success);
            assert_true(replies.back()->Installed);
            assert_int_equal(app->LastAppliedIndex, snapshot.Index);
        } else {
            assert_true(replies.empty());
            assert_int_equal(state->CurrentTerm, 1);
            assert_false(bool(state->GetSnapshot()));
            assert_int_equal(app->LastAppliedIndex, 0);
        }
    };

    run(1, true);
    run(0, false);
}

void test_truncated_snapshot_header_is_rejected_for_covered_and_new_boundaries(void**) {
    TKv source;
    const TSnapshot known{5, 1, {1, 2}, source.SaveSnapshot()};
    const auto knownBytes = EncodeSnapshot(known);
    TMessageHolder<TInstallSnapshotResponse> response;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        response = message.template Cast<TInstallSnapshotResponse>();
    });
    auto state = std::make_shared<TState>();
    auto budget = std::make_shared<TSnapshotInstallBudget>();
    auto raft = std::make_shared<TRaft>(state, 2, TNodeDict{{1, peer}},
        std::vector<uint32_t>{}, TRaft::TLogLimits{.SnapshotInstallBudget = budget});
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, {});
    raft->Process({}, Chunk(known, knownBytes, 0, static_cast<uint32_t>(knownBytes.size())));
    assert_true(response->Installed);

    const auto term = state->CurrentTerm;
    for (const auto index : {known.Index, known.Index + 1}) {
        auto candidate = known;
        candidate.Index = index;
        auto bytes = EncodeSnapshot(candidate);
        bytes.replace(0, 8, "MRFTS003");
        bytes.resize(48); // The stable header alone needs 52 bytes.
        auto chunk = Chunk(candidate, bytes, 0, static_cast<uint32_t>(bytes.size()));
        chunk->Term = term;
        raft->Process({}, chunk);
        assert_true(bool(response));
        assert_false(response->Success);
        assert_false(response->Installed);
        assert_int_equal(response->NextOffset, 0);
        assert_int_equal(state->CurrentTerm, term);
        assert_int_equal(budget->UsedBytes(), knownBytes.size());

        response = {};
        chunk->Term = term + 1;
        raft->Process({}, chunk);
        assert_false(bool(response));
        assert_int_equal(state->CurrentTerm, term);
        assert_int_equal(budget->UsedBytes(), knownBytes.size());
    }
    assert_int_equal(app->Loads, 1);
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
        request->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
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
        request->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
        request->Src = 3;
        request->LeaderId = 3;
        raft->Process({}, request);
        return replies.back();
    };

    auto truncated = EncodeSnapshot(incoming);
    const size_t baseMetadataBytes = (jointSuccessor ? 92 : 88) +
        4 * (incoming.Voters.size() + incoming.JointVoters.size() + incoming.PreviousVoters.size());
    // The declared payload leaves room for only one transition, while the
    // metadata count still advertises all three.
    truncated.resize(baseMetadataBytes + 4 + 64 + incoming.Data.size() + 4);
    auto truncatedRequest = Chunk(incoming, truncated, 0, static_cast<uint32_t>(baseMetadataBytes + 4));
    truncatedRequest->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
    truncatedRequest->Src = truncatedRequest->LeaderId = 3;
    truncatedRequest->Term = state->CurrentTerm + 1;
    const auto term = state->CurrentTerm;
    raft->Process({}, truncatedRequest);
    assert_true(replies.empty());
    assert_int_equal(state->CurrentTerm, term);

    const auto split = baseMetadataBytes - 4;
    auto first = Chunk(incoming, truncated, 0, static_cast<uint32_t>(split));
    auto second = Chunk(incoming, truncated, split, 8);
    for (auto* chunk : {&first, &second}) {
        (*chunk)->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
        (*chunk)->Src = (*chunk)->LeaderId = 3;
        (*chunk)->Term = term + 1;
    }
    raft->Process({}, first);
    assert_int_equal(replies.size(), 1);
    assert_true(replies.back()->Success);
    assert_int_equal(state->CurrentTerm, term);
    raft->Process({}, second);
    assert_int_equal(replies.size(), 2);
    assert_false(replies.back()->Success);
    assert_int_equal(replies.back()->NextOffset, 0);
    assert_int_equal(state->CurrentTerm, term);

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

    raft->Process({}, Chunk(snapshot, bytes, 0, metadataBytes, true));
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

void test_partial_snapshot_rechecks_membership_after_joint_append(void**) {
    struct TPersistedCommitState : TState {
        bool SupportsCommitIndexPersistence() const override { return true; }
    };
    TMessageHolder<TMessage> reply;
    TNodeDict peers{{1, std::make_shared<TEndpoint>([&](auto message) { reply = std::move(message); })},
        {3, std::make_shared<TEndpoint>([](auto) {})}};
    auto state = std::make_shared<TPersistedCommitState>();
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{3});
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, peers);
    const TSnapshot snapshot{5, 2, {1, 2}, std::string(128, 'x')};
    const auto bytes = EncodeSnapshot(snapshot);
    const auto metadataBytes = bytes.size() - snapshot.Data.size() - sizeof(uint32_t);
    raft->Process({}, Chunk(snapshot, bytes, 0, metadataBytes));
    assert_true(reply.Cast<TInstallSnapshotResponse>()->Success);
    assert_int_equal(raft->GetLeaderId(), 1);
    raft->Process({}, Chunk(snapshot, bytes, metadataBytes, 1, true));
    assert_true(reply.Cast<TInstallSnapshotResponse>()->Success);

    auto append = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 3},
        TAppendEntriesRequest{.LeaderCommit = TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 1, .Nentries = 1});
    append.InitPayload(1);
    append.Payload[0] = EncodeConfigurationEntry({
        EConfigurationPhase::Joint, 3, 0, 0, {1, 2}, {1, 2, 3}});
    raft->Process({}, append);
    assert_true(reply.Cast<TAppendEntriesResponse>()->Success);
    assert_true(raft->GetConfigurationState().HasUncommittedConfiguration());
    assert_int_equal(state->CommitIndex, 0);

    raft->Process({}, Chunk(snapshot, bytes, metadataBytes + 1, 1, true));
    const auto rejected = reply.Cast<TInstallSnapshotResponse>();
    assert_false(rejected->Success);
    assert_int_equal(rejected->NextOffset, 0);
    assert_int_equal(state->SnapshotIndex, 0);
    assert_int_equal(app->Loads, 0);
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
    assert_true(state.GetSnapshot()->MaterializeData() == "committed"); assert_int_equal(state.LogTerm(3), 2);
    state.InstallSnapshot({4, 2, {1}, "valid"});
    assert_int_equal(state.SnapshotIndex, 4); assert_true(state.GetSnapshot()->MaterializeData() == "valid");
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
      assert_int_equal(state.SnapshotTerm, 2); assert_true(state.GetSnapshot()->MaterializeData() == "valid"); }
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
    assert_int_equal(state.LogTerm(INT64_MAX), 1); assert_true(state.GetSnapshot()->MaterializeData() == "boundary");
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
        Directory = std::filesystem::temp_directory_path() / ("miniraft-compaction-" +
            std::to_string(getpid()) + "-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(next++));
        assert_true(std::filesystem::create_directory(Directory)); Name = (Directory / "raft").string();
    }
    ~TFiles() { std::error_code ignored; std::filesystem::remove_all(Directory, ignored); }
};
void test_file_staged_snapshot_install_accepts_every_small_fixed_chunk_size(void**) {
    TFiles files;
    auto budget = std::make_shared<TSnapshotInstallBudget>();
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>();
    CheckSnapshotInstallChunkSizes({.SnapshotInstallBudget = budget,
        .SnapshotStagingDirectory = files.Directory.string(), .SnapshotStagingBudget = stagingBudget});
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(stagingBudget->UsedBytes(), 0);
    assert_true(std::filesystem::is_empty(files.Directory));
}

void test_manifest_snapshot_transfer_resumes_after_receiver_restart(void**) {
    TFiles files;
    TKv source;
    source.Write(source.Prepare(Command("restart", std::string(60000, 'r'))), 1);
    source.Write(source.Prepare(Command("second", std::string(60000, 's'))), 2);
    const TSnapshot snapshot{2, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    auto diskBudget = std::make_shared<TSnapshotInstallBudget>(2 * manifest.PayloadBytes);
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>();
    TRaft::TLogLimits limits{.SnapshotInstallBudget = heapBudget,
        .SnapshotStagingDirectory = files.Directory.string(), .SnapshotStagingBudget = diskBudget};
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto makePeer = [&] {
        return std::make_shared<TEndpoint>([&](auto message) {
            if (auto response = message.template Maybe<TInstallSnapshotResponse>())
                responses.push_back(response.Cast());
        });
    };
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    const auto first = RangeChunk(snapshot, manifest,
        TInstallSnapshotRangeRequest::EPhase::Metadata, 0,
        std::string_view(manifest.Metadata).substr(0, 8), 1);
    {
        auto peer = makePeer();
        TNodeDict peers{{1, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
        auto app = std::make_shared<TApplication>();
        TRequestProcessor processor(raft, app, peers);
        assert_true(raft->SupportsDurableSnapshotTransferResume());
        raft->Process({}, first);
        assert_int_equal(responses.size(), 1);
        assert_true(responses.back()->Success);
        assert_false(responses.back()->Installed);
        assert_int_equal(responses.back()->NextOffset, 8);
        assert_true(std::filesystem::exists(files.Directory / "miniraft-incoming-2.state"));
        assert_true(std::filesystem::exists(files.Directory / "miniraft-incoming-2.payload"));
    }
    assert_int_equal(diskBudget->UsedBytes(), 0);

    responses.clear();
    auto peer = makePeer();
    TNodeDict peers{{1, peer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, peers);
    auto replay = RangeChunk(snapshot, manifest,
        TInstallSnapshotRangeRequest::EPhase::Metadata, 0,
        std::string_view(manifest.Metadata).substr(0, 8), 2);
    raft->Process({}, replay);
    assert_int_equal(responses.size(), 1);
    assert_true(responses.back()->Success);
    assert_false(responses.back()->Installed);
    assert_int_equal(responses.back()->NextOffset, 8);
    assert_int_equal(diskBudget->UsedBytes(), manifest.PayloadBytes);

    uint64_t cursor = responses.back()->NextOffset;
    uint64_t sequence = 3;
    const auto payloadStart = uint64_t(manifest.Metadata.size());
    const auto checksumStart = payloadStart + manifest.PayloadBytes;
    const auto total = checksumStart + 4;
    while (cursor < total) {
        TInstallSnapshotRangeRequest::EPhase phase;
        uint64_t phaseOffset;
        std::string_view bytes;
        std::string checksum;
        if (cursor < payloadStart) {
            phase = TInstallSnapshotRangeRequest::EPhase::Metadata;
            phaseOffset = cursor;
            const auto count = size_t(std::min<uint64_t>(
                TInstallSnapshotRangeRequest::MaxChunkBytes, payloadStart - cursor));
            bytes = std::string_view(manifest.Metadata).substr(size_t(cursor), count);
        } else if (cursor < checksumStart) {
            phase = TInstallSnapshotRangeRequest::EPhase::Payload;
            phaseOffset = cursor - payloadStart;
            const auto count = size_t(std::min<uint64_t>(
                TInstallSnapshotRangeRequest::MaxChunkBytes, checksumStart - cursor));
            bytes = std::string_view(snapshot.Data).substr(size_t(phaseOffset), count);
        } else {
            phase = TInstallSnapshotRangeRequest::EPhase::Checksum;
            phaseOffset = 0;
            checksum.resize(4);
            for (unsigned i = 0; i < 4; ++i) checksum[i] = char(manifest.Checksum >> (8 * i));
            bytes = checksum;
        }
        const auto expectedResponses = responses.size() + 1;
        raft->Process({}, RangeChunk(snapshot, manifest, phase, phaseOffset, bytes, sequence++));
        assert_int_equal(responses.size(), expectedResponses);
        assert_true(responses.back()->Success);
        cursor = responses.back()->NextOffset;
        if (cursor == total) assert_true(responses.back()->Installed);
        else assert_false(responses.back()->Installed);
    }
    assert_int_equal(state->SnapshotIndex, snapshot.Index);
    assert_int_equal(app->Loads, 1);
    assert_true(Read(*app, "restart") == std::string(60000, 'r'));
    assert_true(std::filesystem::is_empty(files.Directory));
}

void test_manifest_snapshot_restart_rejects_cursor_bound_to_another_leader(void**) {
    TFiles files;
    TKv source;
    source.Write(source.Prepare(Command("identity", std::string(60000, 'i'))), 1);
    source.Write(source.Prepare(Command("second", std::string(60000, 'j'))), 2);
    const TSnapshot snapshot{2, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    auto diskBudget = std::make_shared<TSnapshotInstallBudget>(2 * manifest.PayloadBytes);
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>();
    TRaft::TLogLimits limits{.SnapshotInstallBudget = heapBudget,
        .SnapshotStagingDirectory = files.Directory.string(), .SnapshotStagingBudget = diskBudget};
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto makePeer = [&] {
        return std::make_shared<TEndpoint>([&](auto message) {
            if (auto response = message.template Maybe<TInstallSnapshotResponse>())
                responses.push_back(response.Cast());
        });
    };
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    const auto first = RangeChunk(snapshot, manifest,
        TInstallSnapshotRangeRequest::EPhase::Metadata, 0,
        std::string_view(manifest.Metadata).substr(0, 8), 1);
    const auto second = RangeChunk(snapshot, manifest,
        TInstallSnapshotRangeRequest::EPhase::Metadata, 8,
        std::string_view(manifest.Metadata).substr(8, 8), 2);
    {
        auto peer = makePeer();
        TNodeDict peers{{1, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
        auto app = std::make_shared<TApplication>();
        TRequestProcessor processor(raft, app, peers);
        raft->Process({}, first);
        assert_true(responses.back()->Success);
        assert_int_equal(responses.back()->NextOffset, 8);
        raft->Process({}, second);
        assert_true(responses.back()->Success);
        assert_int_equal(responses.back()->NextOffset, 16);
    }
    assert_int_equal(diskBudget->UsedBytes(), 0);
    {
        std::fstream cursor(files.Directory / "miniraft-incoming-2.state",
            std::ios::in | std::ios::out | std::ios::binary);
        assert_true(cursor.is_open());
        cursor.seekp(12);
        const char otherLeader[4] = {9, 0, 0, 0};
        cursor.write(otherLeader, sizeof(otherLeader));
        assert_true(cursor.good());
    }

    responses.clear();
    auto peer = makePeer();
    TNodeDict peers{{1, peer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, peers);
    raft->Process({}, first);
    assert_int_equal(responses.size(), 1);
    assert_true(responses.back()->Success);
    assert_false(responses.back()->Installed);
    assert_int_equal(responses.back()->NextOffset, 8);
}

void test_durable_manifest_stage_is_accounted_and_expires_after_receiver_restart(void**) {
    TFiles files;
    TKv source;
    source.Write(source.Prepare(Command("expiry", std::string(60000, 'e'))), 1);
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto manifest = PrepareSnapshotTransferManifest(snapshot);
    auto diskBudget = std::make_shared<TSnapshotInstallBudget>(2 * manifest.PayloadBytes);
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>();
    TRaft::TLogLimits limits{.SnapshotInstallBudget = heapBudget,
        .SnapshotStagingDirectory = files.Directory.string(), .SnapshotStagingBudget = diskBudget};
    std::vector<TMessageHolder<TInstallSnapshotResponse>> responses;
    auto makePeer = [&] {
        return std::make_shared<TEndpoint>([&](auto message) {
            if (auto response = message.template Maybe<TInstallSnapshotResponse>())
                responses.push_back(response.Cast());
        });
    };
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 3;
    const auto first = RangeChunk(snapshot, manifest,
        TInstallSnapshotRangeRequest::EPhase::Metadata, 0,
        std::string_view(manifest.Metadata).substr(0, 8), 1);
    {
        auto peer = makePeer();
        TNodeDict peers{{1, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
        auto app = std::make_shared<TApplication>();
        TRequestProcessor processor(raft, app, peers);
        raft->Process({}, first);
        assert_int_equal(responses.size(), 1);
        assert_true(responses.back()->Success);
    }
    assert_int_equal(diskBudget->UsedBytes(), 0);
    assert_true(std::filesystem::exists(files.Directory / "miniraft-incoming-2.state"));

    responses.clear();
    auto peer = makePeer();
    TNodeDict peers{{1, peer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
    auto app = std::make_shared<TApplication>();
    TRequestProcessor processor(raft, app, peers);
    const ITimeSource::Time start{};
    raft->ProcessTimeout(start);
    assert_int_equal(diskBudget->UsedBytes(), manifest.PayloadBytes);
    assert_true(std::filesystem::exists(files.Directory / "miniraft-incoming-2.state"));
    raft->ProcessTimeout(start + TTimeout::SnapshotTransferIdle);
    assert_int_equal(diskBudget->UsedBytes(), 0);
    assert_true(std::filesystem::is_empty(files.Directory));
}

void test_file_staging_failure_releases_snapshot_reservation(void**) {
    TFiles files;
    auto budget = std::make_shared<TSnapshotInstallBudget>();
    TMessageHolder<TInstallSnapshotResponse> response;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (auto reply = message.template Maybe<TInstallSnapshotResponse>()) response = reply.Cast();
    });
    auto state = std::make_shared<TState>();
    auto app = std::make_shared<TApplication>();
    TNodeDict peers{{1, peer}};
    auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{},
        TRaft::TLogLimits{.SnapshotStagingDirectory = files.Name, .SnapshotStagingBudget = budget});
    TRequestProcessor processor(raft, app, peers);
    TKv source;
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);
    auto first = Chunk(snapshot, bytes, 0, 8);
    raft->Process({}, first);
    assert_non_null(response);
    assert_false(response->Success);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(state->SnapshotIndex, 0);

    assert_true(std::filesystem::create_directory(files.Name));
    raft->Process({}, first);
    assert_true(response->Success);
    assert_false(response->Installed);
    assert_int_equal(budget->UsedBytes(), bytes.size());
    raft->ProcessTimeout(ITimeSource::Time{} + TTimeout::SnapshotTransferIdle);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_true(std::filesystem::is_empty(files.Name));

    auto corrupt = bytes;
    corrupt.back() ^= 1;
    const auto retryTime = ITimeSource::Time{} + TTimeout::SnapshotTransferIdle;
    raft->Process(retryTime, Chunk(snapshot, corrupt, 0, corrupt.size()));
    assert_false(response->Success);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(state->SnapshotIndex, 0);
    raft->Process(retryTime, Chunk(snapshot, bytes, 0, bytes.size()));
    assert_true(response->Success);
    assert_true(response->Installed);
    assert_int_equal(state->SnapshotIndex, 1);
    assert_int_equal(budget->UsedBytes(), 0);

    auto rejectedState = std::make_shared<TState>();
    auto rejectingApp = std::make_shared<TRejectingSnapshotApplication>();
    TNodeDict rejectingPeers{{1, peer}};
    auto rejectingRaft = std::make_shared<TRaft>(rejectedState, 2, rejectingPeers,
        std::vector<uint32_t>{}, TRaft::TLogLimits{.SnapshotStagingDirectory = files.Name,
            .SnapshotStagingBudget = budget});
    TRequestProcessor rejectingProcessor(rejectingRaft, rejectingApp, rejectingPeers);
    constexpr uint32_t FirstChunkBytes = 8;
    response = {};
    rejectingRaft->Process(retryTime, Chunk(snapshot, bytes, 0, FirstChunkBytes));
    assert_non_null(response);
    assert_true(response->Success);
    assert_false(response->Installed);
    assert_int_equal(budget->UsedBytes(), bytes.size());
    rejectingRaft->Process(retryTime, Chunk(snapshot, bytes, FirstChunkBytes,
        static_cast<uint32_t>(bytes.size() - FirstChunkBytes)));
    assert_false(response->Success);
    assert_false(response->Installed);
    assert_int_equal(response->NextOffset, 0);
    assert_int_equal(budget->UsedBytes(), 0);
    assert_int_equal(rejectedState->SnapshotIndex, 0);
    assert_int_equal(rejectingApp->LastAppliedIndex, 0);
    assert_true(std::filesystem::is_empty(files.Name));
}
void test_file_staged_streaming_install_retains_source_quota(void**) {
    TFiles files;
    TKv source;
    source.Write(source.Prepare(Command("key", "streamed value")), 1);
    const auto applicationImage = source.SaveSnapshot();
    TSnapshot snapshot{1, 1, {1, 2}, applicationImage};
    const auto bytes = EncodeSnapshot(snapshot);
    auto heapBudget = std::make_shared<TSnapshotInstallBudget>(1);
    auto stagingBudget = std::make_shared<TSnapshotInstallBudget>(2 * bytes.size());
    {
        TMessageHolder<TInstallSnapshotResponse> response;
        auto peer = std::make_shared<TEndpoint>([&](auto message) {
            if (auto reply = message.template Maybe<TInstallSnapshotResponse>()) response = reply.Cast();
        });
        auto state = std::make_shared<TState>();
        state->CurrentTerm = 3;
        state->Commit();
        TNodeDict peers{{1, peer}};
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{},
            TRaft::TLogLimits{.SnapshotInstallBudget = heapBudget,
                .SnapshotStagingDirectory = files.Directory.string(), .SnapshotStagingBudget = stagingBudget});
        auto app = std::make_shared<TStreamingInstallApplication>();
        app->ExpectedImage = applicationImage;
        TRequestProcessor processor(raft, app, peers);

        raft->Process({}, Chunk(snapshot, bytes, 0, uint32_t(bytes.size())));
        assert_non_null(response);
        assert_true(response->Success);
        assert_true(response->Installed);
        assert_int_equal(app->StreamingPreparations, 1);
        assert_int_equal(app->Loads, 1);
        assert_int_equal(heapBudget->UsedBytes(), 0);
        assert_int_equal(stagingBudget->UsedBytes(), bytes.size());
        auto firstSnapshot = state->GetSnapshot();
        assert_true(firstSnapshot && firstSnapshot->Data.empty() && firstSnapshot->DataSource);
        assert_true(firstSnapshot->MaterializeData() == applicationImage);

        snapshot.Index = 2;
        const auto nextBytes = EncodeSnapshot(snapshot);
        assert_int_equal(nextBytes.size(), bytes.size());
        raft->Process({}, Chunk(snapshot, nextBytes, 0, uint32_t(nextBytes.size())));
        assert_true(response->Success);
        assert_true(response->Installed);
        assert_int_equal(app->StreamingPreparations, 2);
        assert_int_equal(app->Loads, 2);
        assert_int_equal(stagingBudget->UsedBytes(), 2 * bytes.size());
        assert_int_equal(state->SnapshotIndex, 2);
        assert_true(firstSnapshot->MaterializeData() == applicationImage);

        firstSnapshot.reset();
        assert_int_equal(stagingBudget->UsedBytes(), bytes.size());
    }
    assert_int_equal(stagingBudget->UsedBytes(), 0);
}
void test_file_staging_and_decode_use_separate_shared_budgets(void**) {
    TFiles files;
    TKv source;
    const TSnapshot snapshot{1, 1, {1, 2}, source.SaveSnapshot()};
    const auto bytes = EncodeSnapshot(snapshot);
    auto heap = std::make_shared<TSnapshotInstallBudget>(bytes.size());
    auto disk = std::make_shared<TSnapshotInstallBudget>(bytes.size());
    TMessageHolder<TInstallSnapshotResponse> response;
    auto peer = std::make_shared<TEndpoint>([&](auto message) {
        if (auto reply = message.template Maybe<TInstallSnapshotResponse>()) response = reply.Cast();
    });
    TNodeDict peers{{1, peer}};
    std::vector<std::unique_ptr<TRequestProcessor>> processors;
    auto makeReceiver = [&](bool staged) {
        auto state = std::make_shared<TState>();
        state->CurrentTerm = 3;
        TRaft::TLogLimits limits{.SnapshotInstallBudget = heap};
        if (staged) {
            limits.SnapshotStagingDirectory = files.Directory.string();
            limits.SnapshotStagingBudget = disk;
        }
        auto raft = std::make_shared<TRaft>(state, 2, peers, std::vector<uint32_t>{}, limits);
        processors.push_back(std::make_unique<TRequestProcessor>(raft, std::make_shared<TApplication>(), peers));
        return raft;
    };
    auto memoryReceiver = makeReceiver(false);
    auto stagedReceiver = makeReceiver(true);
    auto otherStagedReceiver = makeReceiver(true);
    auto first = Chunk(snapshot, bytes, 0, 8);
    auto last = Chunk(snapshot, bytes, 8, bytes.size() - 8);

    memoryReceiver->Process({}, first);
    assert_true(response->Success);
    assert_int_equal(heap->UsedBytes(), bytes.size());
    stagedReceiver->Process({}, first);
    assert_true(response->Success);
    assert_int_equal(disk->UsedBytes(), bytes.size());
    otherStagedReceiver->Process({}, first);
    assert_false(response->Success);
    assert_int_equal(disk->UsedBytes(), bytes.size());

    const auto finalChunkTime = ITimeSource::Time{} + TTimeout::SnapshotTransferIdle / 2;
    response = {};
    stagedReceiver->Process(finalChunkTime, last);
    assert_false(bool(response));
    assert_int_equal(stagedReceiver->GetState()->SnapshotIndex, 0);
    assert_int_equal(heap->UsedBytes(), bytes.size());
    assert_int_equal(disk->UsedBytes(), bytes.size());

    const auto retryTime = ITimeSource::Time{} + TTimeout::SnapshotTransferIdle;
    memoryReceiver->ProcessTimeout(retryTime);
    assert_int_equal(heap->UsedBytes(), 0);
    stagedReceiver->Process(retryTime, last);
    assert_true(bool(response));
    assert_true(response->Success);
    assert_true(response->Installed);
    assert_int_equal(stagedReceiver->GetState()->SnapshotIndex, 1);
    assert_int_equal(heap->UsedBytes(), bytes.size());
    assert_int_equal(disk->UsedBytes(), 0);

    auto heldHeap = heap->TryReserve(bytes.size());
    assert_false(bool(heldHeap));
    otherStagedReceiver->Process(retryTime, first);
    assert_true(response->Success);
    response = {};
    otherStagedReceiver->Process(retryTime, last);
    assert_false(bool(response));
    otherStagedReceiver->Process(retryTime + TTimeout::SnapshotTransferIdle / 2, last);
    assert_false(bool(response));
    assert_int_equal(disk->UsedBytes(), bytes.size());
    otherStagedReceiver->ProcessTimeout(retryTime + TTimeout::SnapshotTransferIdle);
    assert_int_equal(disk->UsedBytes(), 0);
    assert_int_equal(otherStagedReceiver->GetState()->SnapshotIndex, 0);
    assert_true(std::filesystem::is_empty(files.Directory));
}
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
            request->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
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
        request->Done |= TInstallSnapshotRequest::JointElectionFenceLeader;
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
        assert_true(recovered->GetSnapshot()->MaterializeData() ==
            (updated ? incoming.MaterializeData() : known.MaterializeData()));
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
    assert_true(state.GetSnapshot()->MaterializeData() == "different boundary");
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = 3; state.Append(entry);
    assert_int_equal(state.LastLogIndex, 4); assert_int_equal(state.Get(4)->Term, 3);
}
void test_snapshot_and_suffix_are_atomic_at_every_publication_stage(void**) {
    using TStage = TDurableState::EStage;
    for (bool priorSnapshot : {false, true}) for (bool retain : {false, true}) for (auto stage : {TStage::TempCreated, TStage::Written, TStage::FileSynced,
         TStage::Renamed, TStage::DirectorySynced, TStage::SnapshotTempCreated, TStage::SnapshotWritten,
         TStage::SnapshotFileSynced, TStage::SnapshotRenamed, TStage::SnapshotDirectorySynced,
         TStage::LogTempCreated, TStage::LogWritten, TStage::LogFileSynced, TStage::LogRenamed,
         TStage::LogDirectorySynced}) {
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
        const bool updated = stage == TStage::Renamed || stage == TStage::DirectorySynced;
        assert_int_equal(state.CurrentTerm, 3); assert_int_equal(state.VotedFor, 2);
        assert_int_equal(state.SnapshotIndex, updated ? retain ? 2 : 4 : priorSnapshot ? 1 : 0);
        assert_int_equal(state.LastLogIndex, updated && !retain ? 4 : 3);
        if (updated) assert_true(state.GetSnapshot()->MaterializeData() == "durable application");
        else if (priorSnapshot) assert_true(state.GetSnapshot()->MaterializeData() == "previous application");
        if (retain || !updated) assert_int_equal(state.Get(3)->Term, 3);
    }
}
template<class TMakeApplication>
void ExerciseRemoteSnapshotInstallCrashRecovery(TFiles& files, TMakeApplication makeApplication) {
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
        try {
            auto state = std::make_shared<TDurableState>(files.Name, 2);
            TNodeDict peers{{1, std::make_shared<TEndpoint>([](auto) {})}};
            auto raft = std::make_shared<TRaft>(state, 2, peers);
            auto application = makeApplication();
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
        assert_true(state->GetSnapshot() && state->GetSnapshot()->MaterializeData() ==
            snapshot.MaterializeData());
        auto raft = std::make_shared<TRaft>(state, 2,
            TNodeDict{{1, std::make_shared<TEndpoint>([](auto) {})}});
        auto application = std::make_shared<TKv>();
        TRequestProcessor processor(raft, application, {});
        assert_int_equal(application->LastAppliedIndex, snapshot.Index);
        assert_true(Read(*application, "crash-window") == "durable image");
    }
}
void test_remote_snapshot_install_crash_before_application_load_recovers_on_restart(void**) {
    TFiles files;
    ExerciseRemoteSnapshotInstallCrashRecovery(files, [] {
        struct TCrashDuringLoad final : TKv {
            void LoadSnapshot(std::string_view, uint64_t) override { _exit(87); }
        };
        return std::make_shared<TCrashDuringLoad>();
    });
}
void test_remote_snapshot_install_crash_during_staged_activation_recovers_on_restart(void**) {
    TFiles files;
    ExerciseRemoteSnapshotInstallCrashRecovery(files, [] {
        struct TCrashDuringActivation final : TKv, IStagedSnapshotRsm {
            struct TInstallation final : IStagedSnapshotInstallation {
                void Install() override { _exit(87); }
            };
            std::unique_ptr<IStagedSnapshotInstallation> PrepareSnapshotInstallation(
                    std::string_view bytes, uint64_t index) override {
                ValidateSnapshot(bytes, index);
                return std::make_unique<TInstallation>();
            }
            bool SupportsApplicationSnapshotInstall() const override { return true; }
        };
        return std::make_shared<TCrashDuringActivation>();
    });
}
void test_compaction_reclaims_durable_prefix_bytes(void**) {
    TFiles files; TDurableState state(files.Name, 1);
    for (int i = 0; i < 30; ++i) {
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 4096);
        memset(entry->Data, 0, 4096); state.Append(entry);
    }
    const auto before = std::filesystem::file_size(files.Name + ".durable.1.log.1");
    state.InstallSnapshot({29, 1, {1}, "small current application"});
    const auto after = std::filesystem::file_size(files.Name + ".durable.1.log.2");
    assert_true(after < before / 10); assert_int_equal(state.LastLogIndex, 30);
    assert_false(std::filesystem::exists(files.Name + ".durable.1.log.1"));
    assert_true(bool(state.Get(30))); assert_false(state.Get(29));
}
void test_durable_v8_semantic_corruption_rejects_without_rewriting(void**) {
    for (int invalid = 0; invalid < 3; ++invalid) {
        TFiles files;
        { TDurableState state(files.Name, 1); ExerciseBoundary(state); }
        const auto path = files.Name + ".durable.1";
        std::ifstream input(path, std::ios::binary);
        std::string bytes(std::istreambuf_iterator<char>(input), {}); input.close();
        assert_true(bytes.substr(0, 8) == "MRFTD008");
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
        cmocka_unit_test(test_example_kv_streaming_install_validates_before_atomic_publish),
        cmocka_unit_test(test_snapshot_capture_preflights_application_size_bound),
        cmocka_unit_test(test_snapshot_capture_respects_raft_image_limit),
        cmocka_unit_test(test_snapshot_capture_reserves_declared_bound_before_materializing),
        cmocka_unit_test(test_unbounded_legacy_snapshot_precharges_before_callback),
        cmocka_unit_test(test_snapshot_capture_charges_retained_string_capacity),
        cmocka_unit_test(test_incremental_snapshot_capture_charges_each_growth_before_writing),
        cmocka_unit_test(test_snapshot_source_can_report_non_heap_payload_storage),
        cmocka_unit_test(test_recovered_snapshot_retains_heap_budget_lease),
        cmocka_unit_test(test_recovered_source_snapshot_is_charged_to_heap_budget),
        cmocka_unit_test(test_snapshot_materialization_reserves_temporary_heap_bytes),
        cmocka_unit_test(test_recovered_file_snapshot_budgets_fallback_materialization),
        cmocka_unit_test(test_shared_compaction_reuses_storage_snapshot_image),
        cmocka_unit_test(test_zero_index_follower_compaction_waits_for_capability_barrier),
        cmocka_unit_test(test_restarted_follower_reacquires_compaction_certificate_without_new_term),
        cmocka_unit_test(test_snapshot_size_negotiation_keeps_legacy_fallback_and_certifies_voter_limit),
        cmocka_unit_test(test_snapshot_size_capability_respects_receiver_budgets),
        cmocka_unit_test(test_snapshot_encoder_accepts_image_above_legacy_limit),
        cmocka_unit_test(test_stale_peer_capability_cannot_authorize_snapshot_or_barrier),
        cmocka_unit_test(test_single_voter_compacts_snapshot_above_legacy_limit),
        cmocka_unit_test(test_learner_without_voter_size_certificate_keeps_legacy_compaction_limit),
        cmocka_unit_test(test_snapshot_history_encoding_is_negotiated_per_peer),
        cmocka_unit_test(test_idle_snapshot_transfer_discards_partial_image_and_restarts),
        cmocka_unit_test(test_snapshot_install_budget_is_shared_and_reclaimed_after_idle_expiry),
        cmocka_unit_test(test_snapshot_install_budget_releases_after_application_validation_failure),
        cmocka_unit_test(test_snapshot_install_budget_releases_after_conflicting_duplicate),
        cmocka_unit_test(test_covered_snapshot_metadata_uses_shared_install_budget),
        cmocka_unit_test(test_snapshot_install_accepts_every_small_fixed_chunk_size),
        cmocka_unit_test(test_snapshot_manifest_ranges_install_with_separate_payload_staging),
        cmocka_unit_test(test_snapshot_manifest_ranges_reject_gaps_conflicts_and_corruption),
        cmocka_unit_test(test_negotiated_snapshot_manifest_transfer_uses_distinct_wire_phases),
        cmocka_unit_test(test_snapshot_manifest_ranges_use_budgeted_heap_payload_without_file_staging),
        cmocka_unit_test(test_snapshot_manifest_without_streaming_installer_reserves_copy_headroom),
        cmocka_unit_test(test_large_versioned_snapshot_installs_over_manifest_ranges),
        cmocka_unit_test(test_compaction_lagging_follower_installs_snapshot_then_tail),
        cmocka_unit_test(test_outgoing_snapshot_chunk_holds_heap_budget_lease),
        cmocka_unit_test(test_simultaneous_startup_candidates_use_distinct_election_deadlines),
        cmocka_unit_test(test_partial_transfer_restart_and_delayed_ack_are_safe),
        cmocka_unit_test(test_snapshot_partial_ack_cannot_skip_unsent_bytes),
        cmocka_unit_test(test_snapshot_sender_accepts_negotiated_retained_prefix_after_cursor_reset),
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
        cmocka_unit_test(test_partial_snapshot_rechecks_membership_after_joint_append),
        cmocka_unit_test(test_memory_snapshot_storage_rejects_term_regression),
        cmocka_unit_test(test_memory_snapshot_cannot_replace_committed_index_with_conflicting_term),
        cmocka_unit_test(test_joint_snapshot_is_rejected_without_matching_history_even_if_boundary_is_covered),
        cmocka_unit_test(test_higher_term_snapshot_defers_term_change_until_split_metadata_is_validated),
        cmocka_unit_test(test_invalid_split_higher_term_snapshot_preserves_candidate_state),
        cmocka_unit_test(test_valid_split_higher_term_covered_snapshot_accepts_leader_without_reload),
        cmocka_unit_test(test_snapshot_install_requires_nonzero_sequence),
        cmocka_unit_test(test_truncated_snapshot_header_is_rejected_for_covered_and_new_boundaries),
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
        cmocka_unit_test(test_file_staged_snapshot_install_accepts_every_small_fixed_chunk_size),
        cmocka_unit_test(test_manifest_snapshot_transfer_resumes_after_receiver_restart),
        cmocka_unit_test(test_manifest_snapshot_restart_rejects_cursor_bound_to_another_leader),
        cmocka_unit_test(test_durable_manifest_stage_is_accounted_and_expires_after_receiver_restart),
        cmocka_unit_test(test_incremental_snapshot_capture_uses_disk_staging_quota),
        cmocka_unit_test(test_disk_staged_capture_becomes_durable_snapshot_across_restart),
        cmocka_unit_test(test_durable_application_snapshot_generation_is_published_before_raft_and_restored),
        cmocka_unit_test(test_versioned_disk_application_snapshot_generation_roundtrips),
        cmocka_unit_test(test_remote_snapshot_install_publishes_and_activates_local_generation),
        cmocka_unit_test(test_durable_application_generation_replaces_snapshot_sidecar_payload),
        cmocka_unit_test(test_versioned_disk_application_generation_replaces_snapshot_payload),
        cmocka_unit_test(test_split_application_snapshot_generation_crash_boundaries),
        cmocka_unit_test(test_remote_application_snapshot_generation_crash_boundaries),
        cmocka_unit_test(test_range_snapshot_source_uses_disk_staging_quota),
        cmocka_unit_test(test_legacy_bounded_snapshot_uses_disk_staging_quota),
        cmocka_unit_test(test_legacy_unbounded_snapshot_uses_disk_staging_quota),
        cmocka_unit_test(test_file_staging_failure_releases_snapshot_reservation),
        cmocka_unit_test(test_file_staged_streaming_install_retains_source_quota),
        cmocka_unit_test(test_file_staging_and_decode_use_separate_shared_budgets),
        cmocka_unit_test(test_durable_snapshot_storage_rejects_term_regression),
        cmocka_unit_test(test_durable_snapshot_cannot_replace_committed_index_with_conflicting_term),
        cmocka_unit_test(test_durable_storage_rejects_index_exhaustion),
        cmocka_unit_test(test_durable_term_lookup_avoids_payload_copies_across_compaction),
        cmocka_unit_test(test_durable_compacted_boundary_survives_restart),
        cmocka_unit_test(test_snapshot_and_suffix_are_atomic_at_every_publication_stage),
        cmocka_unit_test(test_remote_snapshot_install_crash_before_application_load_recovers_on_restart),
        cmocka_unit_test(test_remote_snapshot_install_crash_during_staged_activation_recovers_on_restart),
        cmocka_unit_test(test_durable_stale_snapshot_membership_recovery_survives_restart),
        cmocka_unit_test(test_durable_membership_snapshot_install_is_atomic_across_restart),
        cmocka_unit_test(test_compaction_reclaims_durable_prefix_bytes),
        cmocka_unit_test(test_durable_v8_semantic_corruption_rejects_without_rewriting),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
