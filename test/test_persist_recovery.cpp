#include <miniraft/persist.h>
#include <miniraft/raft.h>
#include <miniraft/snapshot_codec.h>
#include <filesystem>
#include <chrono>
#include <map>
#include <iterator>
#include <exception>
#ifndef _WIN32
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
namespace fs = std::filesystem;
struct TFiles {
    fs::path Directory;
    std::string Name;
    TFiles() {
        static unsigned counter = 0;
        Directory = fs::temp_directory_path() /
            ("miniraft-recovery-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
             "-" + std::to_string(counter++));
        assert_true(fs::create_directory(Directory));
        Name = (Directory / "raft").string();
    }
    ~TFiles() { std::error_code ignored; fs::remove_all(Directory, ignored); }
    fs::path Path(const std::string& kind) const { return Name + "." + kind + ".1"; }
    void DowngradeRecords(uint64_t lastIndex) {
        std::ifstream entriesInput(Path("entries"), std::ios::binary);
        std::string encodedEntries{std::istreambuf_iterator<char>(entriesInput), std::istreambuf_iterator<char>()};
        std::string legacyEntries;
        size_t entryOffset = 0;
        for (uint64_t i = 0; i < lastIndex; ++i) {
            assert_true(entryOffset + sizeof(TLogEntry) <= encodedEntries.size());
            uint32_t len = 0;
            memcpy(&len, encodedEntries.data() + entryOffset + sizeof(uint32_t), sizeof(len));
            assert_true(entryOffset + len + sizeof(uint32_t) <= encodedEntries.size());
            legacyEntries.append(encodedEntries, entryOffset, len);
            entryOffset += size_t(len) + sizeof(uint32_t);
        }
        entriesInput.close();
        Rewrite("entries", legacyEntries);

        std::ifstream indexInput(Path("index"), std::ios::binary);
        std::string encodedIndex{std::istreambuf_iterator<char>(indexInput), std::istreambuf_iterator<char>()};
        std::string legacyIndex;
        for (uint64_t i = 0; i < lastIndex; ++i) {
            assert_true((i + 1) * 12 <= encodedIndex.size());
            uint64_t offset = 0;
            memcpy(&offset, encodedIndex.data() + size_t(i) * 12, sizeof(offset));
            offset -= i * sizeof(uint32_t); // Remove each prior entry's CRC trailer.
            legacyIndex.append(reinterpret_cast<const char*>(&offset), sizeof(offset));
        }
        indexInput.close();
        Rewrite("index", legacyIndex);
    }
    void WriteLegacyMetadata(uint64_t lastIndex, uint64_t term, uint32_t vote) {
        DowngradeRecords(lastIndex);

        std::ofstream file(Path("state"), std::ios::binary | std::ios::trunc);
        file.write(reinterpret_cast<const char*>(&lastIndex), sizeof(lastIndex));
        file.write(reinterpret_cast<const char*>(&term), sizeof(term));
        file.write(reinterpret_cast<const char*>(&vote), sizeof(vote));
        file.flush();
        assert_true(bool(file));
    }
    void WritePriorVersionedMetadata(uint64_t lastIndex, uint64_t term, uint32_t vote,
            uint64_t commitIndex, const std::vector<uint32_t>& voters) {
        DowngradeRecords(lastIndex);
        std::string metadata;
        NSnapshotCodec::Append(metadata, "MRFTL001");
        NSnapshotCodec::Put(metadata, 1, 4);
        NSnapshotCodec::Put(metadata, lastIndex, 8);
        NSnapshotCodec::Put(metadata, term, 8);
        NSnapshotCodec::Put(metadata, vote, 4);
        NSnapshotCodec::Put(metadata, commitIndex, 8);
        NSnapshotCodec::Put(metadata, voters.size(), 4);
        NSnapshotCodec::Put(metadata, 1, 4); // Trusted commit boundary; no record CRC flag existed yet.
        for (auto id : voters) NSnapshotCodec::Put(metadata, id, 4);
        NSnapshotCodec::Put(metadata, NSnapshotCodec::Checksum(metadata), 4);
        Rewrite("state", metadata);
    }
    void RepairMetadataChecksum() {
        std::ifstream input(Path("state"), std::ios::binary);
        std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        if (bytes.size() < 4 ||
            (bytes.substr(0, 8) != "MRFTL001" && bytes.substr(0, 8) != "MRFTL002")) return;
        const auto checksum = NSnapshotCodec::Checksum(std::string_view(bytes).substr(0, bytes.size() - 4));
        for (unsigned i = 0; i < 4; ++i) bytes[bytes.size() - 4 + i] = char(checksum >> (8 * i));
        input.close();
        std::ofstream output(Path("state"), std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), bytes.size());
        output.flush();
        assert_true(bool(output));
    }
    void Rewrite(const std::string& kind, const std::string& bytes) {
        std::ofstream output(Path(kind), std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), bytes.size());
        output.flush();
        assert_true(bool(output));
    }
    void OverwriteEntry(uint64_t entryIndex, size_t fieldOffset, const void* value, size_t valueSize) {
        std::ifstream input(Path("entries"), std::ios::binary);
        std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        size_t recordOffset = 0;
        for (uint64_t i = 0; i < entryIndex; ++i) {
            uint32_t len = 0;
            assert_true(recordOffset + sizeof(uint32_t) * 2 <= bytes.size());
            memcpy(&len, bytes.data() + recordOffset + sizeof(uint32_t), sizeof(len));
            assert_true(len >= sizeof(TLogEntry));
            recordOffset += size_t(len) + sizeof(uint32_t);
            assert_true(recordOffset <= bytes.size());
        }
        uint32_t len = 0;
        assert_true(recordOffset + sizeof(uint32_t) * 2 <= bytes.size());
        memcpy(&len, bytes.data() + recordOffset + sizeof(uint32_t), sizeof(len));
        assert_true(len >= sizeof(TLogEntry) && recordOffset + len + sizeof(uint32_t) <= bytes.size());
        assert_true(fieldOffset <= len && valueSize <= len - fieldOffset);
        memcpy(bytes.data() + recordOffset + fieldOffset, value, valueSize);
        const auto checksum = NSnapshotCodec::Checksum(std::string_view(bytes).substr(recordOffset, len));
        for (unsigned i = 0; i < 4; ++i) bytes[recordOffset + len + i] = char(checksum >> (8 * i));
        input.close();
        Rewrite("entries", bytes);
    }
    template<class T> void OverwriteEntry(uint64_t entryIndex, size_t fieldOffset, T value) {
        OverwriteEntry(entryIndex, fieldOffset, &value, sizeof(value));
    }
    void OverwriteIndexOffset(uint64_t recordIndex, uint64_t offset) {
        std::ifstream input(Path("index"), std::ios::binary);
        std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        const auto start = size_t(recordIndex) * 12;
        assert_true(start + 12 <= bytes.size());
        memcpy(bytes.data() + start, &offset, sizeof(offset));
        const auto checksum = NSnapshotCodec::Checksum(std::string_view(bytes).substr(start, sizeof(offset)));
        for (unsigned i = 0; i < 4; ++i) bytes[start + sizeof(offset) + i] = char(checksum >> (8 * i));
        input.close();
        Rewrite("index", bytes);
    }
    void CorruptEntryChecksum(uint64_t entryIndex) {
        std::ifstream input(Path("entries"), std::ios::binary);
        std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        size_t recordOffset = 0;
        for (uint64_t i = 0; i < entryIndex; ++i) {
            uint32_t len = 0;
            memcpy(&len, bytes.data() + recordOffset + sizeof(uint32_t), sizeof(len));
            recordOffset += size_t(len) + sizeof(uint32_t);
        }
        uint32_t len = 0;
        memcpy(&len, bytes.data() + recordOffset + sizeof(uint32_t), sizeof(len));
        const auto checksumByte = recordOffset + len;
        assert_true(checksumByte < bytes.size());
        bytes[checksumByte] ^= 0x40;
        input.close();
        Rewrite("entries", bytes);
    }
    void CorruptIndexChecksum(uint64_t recordIndex) {
        std::ifstream input(Path("index"), std::ios::binary);
        std::string bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
        const auto checksumByte = size_t(recordIndex) * 12 + sizeof(uint64_t);
        assert_true(checksumByte < bytes.size());
        bytes[checksumByte] ^= 0x40;
        input.close();
        Rewrite("index", bytes);
    }
    void CorruptMetadataChecksum() {
        std::fstream file(Path("state"), std::ios::in | std::ios::out | std::ios::binary);
        file.seekg(0, std::ios::end);
        auto size = file.tellg();
        assert_true(size >= std::streamoff(4));
        file.seekg(size - std::streamoff(1));
        char byte = 0;
        assert_true(bool(file.read(&byte, 1)));
        byte ^= 0x40;
        file.seekp(size - std::streamoff(1));
        file.write(&byte, 1);
        file.flush();
        assert_true(bool(file));
    }
    void Seed() {
        TDiskState state(Name, 1);
        state.CurrentTerm = 7;
        state.VotedFor = 2;
        for (uint64_t term : {2, 5}) {
            auto entry = NewHoldedMessage<TLogEntry>();
            entry->Term = term;
            state.Append(entry);
        }
    }
    template<class T> void Overwrite(const std::string& kind, uint64_t offset, T value) {
        std::fstream file(Path(kind), std::ios::in | std::ios::out | std::ios::binary);
        file.seekp(offset);
        file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        file.flush();
        assert_true(bool(file));
        file.close();
        if (kind == "state") RepairMetadataChecksum();
    }
    void RejectRecovery() {
        auto contents = [&] {
            std::map<std::string, std::string> result;
            for (auto& file : fs::directory_iterator(Directory)) {
                std::ifstream input(file.path(), std::ios::binary);
                result[file.path().filename().string()] =
                    std::string(std::istreambuf_iterator<char>(input), {});
            }
            return result;
        };
        const auto before = contents();
        bool rejected = false;
        try { TDiskState state(Name, 1); }
        catch (const std::runtime_error&) { rejected = true; }
        assert_true(rejected);
        assert_true(before == contents());
    }
};

void test_truncated_metadata_is_not_reset(void**) {
    for (uint64_t size : {0, 1, 8, 16, 19}) {
        TFiles files;
        files.Seed();
        fs::resize_file(files.Path("state"), size);
        files.RejectRecovery();
        assert_int_equal(fs::file_size(files.Path("state")), size);
    }
}
void test_oversized_metadata_is_rejected(void**) {
    TFiles files;
    files.Seed();
    fs::resize_file(files.Path("state"), 21);
    files.RejectRecovery();
    assert_int_equal(fs::file_size(files.Path("state")), 21);
}
void test_versioned_metadata_checksum_is_verified(void**) {
    TFiles files;
    files.Seed();
    files.CorruptMetadataChecksum();
    files.RejectRecovery();
}
void test_checksums_detect_log_and_index_record_corruption(void**) {
    {
        TFiles files;
        files.Seed();
        std::ifstream metadata(files.Path("state"), std::ios::binary);
        std::string magic(8, '\0');
        assert_true(bool(metadata.read(magic.data(), magic.size())));
        assert_true(magic == "MRFTL004");
        uint32_t flags = 0;
        metadata.seekg(44);
        assert_true(bool(metadata.read(reinterpret_cast<char*>(&flags), sizeof(flags))));
        assert_true(flags & (1u << 1));
        files.CorruptEntryChecksum(0);
        files.RejectRecovery();
    }
    {
        TFiles files;
        files.Seed();
        files.CorruptIndexChecksum(0);
        files.RejectRecovery();
    }
}
void test_versioned_metadata_persists_commit_and_voter_baseline(void**) {
    TFiles fresh;
    auto freshState = std::make_shared<TDiskState>(fresh.Name, 1);
    {
        TRaft raft(freshState, 1, TNodeDict{});
        assert_true(freshState->GetBoundVoters() == (std::vector<uint32_t>{1}));
    }

    TFiles oldEmpty;
    { TDiskState state(oldEmpty.Name, 1); }
    oldEmpty.WriteLegacyMetadata(0, 1, 0);
    auto oldEmptyState = std::make_shared<TDiskState>(oldEmpty.Name, 1);
    {
        TRaft raft(oldEmptyState, 1, TNodeDict{});
        assert_true(oldEmptyState->GetBoundVoters() == (std::vector<uint32_t>{1}));
        assert_true(oldEmptyState->HasTrustedCommitIndex());
    }

    TFiles files;
    files.Seed();
    files.WriteLegacyMetadata(2, 7, 2);
    {
        TDiskState state(files.Name, 1);
        assert_true(state.SupportsCommitIndexPersistence());
        assert_true(state.SupportsVoterBinding());
        assert_true(state.HasUnboundRecoveredState());
        assert_int_equal(state.CommitIndex, 0); // Old metadata had no commit boundary.
        assert_int_equal(state.Get(1)->Term, 2); // Old entries and offsets remain readable.
        bool directBindRejected = false;
        try { state.BindVoters({1, 2, 3}); }
        catch (const std::exception&) { directBindRejected = true; }
        assert_true(directBindRejected);
        state.ValidateTrustedLegacyVoterBaseline({1, 2, 3}, 1);
        assert_int_equal(state.CommitIndex, 1);
        assert_true(state.GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
        assert_false(state.HasUnboundRecoveredState());
    }
    {
        TDiskState state(files.Name, 1);
        assert_int_equal(state.CommitIndex, 1);
        assert_true(state.GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
        assert_false(state.HasUnboundRecoveredState());
    }

    TFiles priorVersioned;
    priorVersioned.Seed();
    priorVersioned.WritePriorVersionedMetadata(2, 7, 2, 1, {1, 2, 3});
    {
        TDiskState state(priorVersioned.Name, 1);
        assert_int_equal(state.CommitIndex, 1);
        assert_true(state.GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
        assert_int_equal(state.Get(1)->Term, 2);
        assert_int_equal(state.Get(2)->Term, 5);
    }

    TFiles configured;
    {
        TDiskState state(configured.Name, 1);
        state.Append(EncodeConfigurationEntry({EConfigurationPhase::Joint, 1, 0, 0,
            {1, 2}, {1, 3}}));
    }
    {
        TDiskState state(configured.Name, 1);
        bool configurationHistoryRejected = false;
        try { state.ValidateTrustedLegacyVoterBaseline({1, 2}, 0); }
        catch (const std::invalid_argument&) { configurationHistoryRejected = true; }
        assert_true(configurationHistoryRejected);
        assert_true(state.GetBoundVoters().empty());
    }
}
void test_disk_snapshot_generation_recovers_suffix_and_replaces_conflict(void**) {
    TFiles files;
    {
        TDiskState state(files.Name, 1);
        assert_true(state.SupportsSnapshots());
        assert_true(state.SupportsInstallSnapshot());
        state.BindVoters({1, 2, 3});
        state.CurrentTerm = 7;
        state.VotedFor = 2;
        for (uint64_t term : {2, 5}) {
            auto entry = NewHoldedMessage<TLogEntry>();
            entry->Term = term;
            state.Append(entry);
        }
        state.CommitIndex = 1;
        state.Commit();
        TSnapshot first{1, 2, {2, 3}, "first image"};
        first.StableConfiguration = true;
        first.ConfigurationIndex = 1;
        first.ConfigurationTerm = 2;
        state.InstallSnapshot(first);
        assert_int_equal(state.SnapshotIndex, 1);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.LogTerm(1), 2);
        assert_false(state.Get(1));
        assert_int_equal(state.Get(2)->Term, 5);
        assert_int_equal(fs::file_size(files.Path("entries")), 0);
        assert_int_equal(fs::file_size(files.Name + ".entries.1.g1"), sizeof(TLogEntry) + sizeof(uint32_t));
    }
    {
        TDiskState state(files.Name, 1);
        TSnapshot expectedFirst{1, 2, {2, 3}, "first image"};
        expectedFirst.StableConfiguration = true;
        expectedFirst.ConfigurationIndex = 1;
        expectedFirst.ConfigurationTerm = 2;
        assert_true(state.GetSnapshot() && *state.GetSnapshot() == expectedFirst);
        assert_true(state.GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
        assert_int_equal(state.LastLogTerm, 5);
        state.RemoveLast();
        assert_int_equal(state.LastLogIndex, 1);
        assert_int_equal(state.LastLogTerm, 2);
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 7;
        state.Append(entry);
        assert_int_equal(state.Get(2)->Term, 7);
        TSnapshot replacement{2, 5, {2, 3}, "replacement image"};
        replacement.StableConfiguration = true;
        replacement.ConfigurationIndex = 1;
        replacement.ConfigurationTerm = 2;
        state.InstallSnapshot(replacement);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.LastLogTerm, 5);
        assert_false(state.Get(2));
    }
    TDiskState recovered(files.Name, 1);
    TSnapshot expectedReplacement{2, 5, {2, 3}, "replacement image"};
    expectedReplacement.StableConfiguration = true;
    expectedReplacement.ConfigurationIndex = 1;
    expectedReplacement.ConfigurationTerm = 2;
    assert_true(recovered.GetSnapshot() && *recovered.GetSnapshot() == expectedReplacement);
    assert_true(recovered.GetBoundVoters() == (std::vector<uint32_t>{1, 2, 3}));
    assert_int_equal(recovered.SnapshotIndex, 2);
    assert_int_equal(recovered.LastLogIndex, 2);
    assert_int_equal(recovered.CommitIndex, 2);
    assert_int_equal(recovered.LogTerm(2), 5);
    assert_false(recovered.Get(2));
    auto afterSnapshot = NewHoldedMessage<TLogEntry>();
    afterSnapshot->Term = 7;
    recovered.Append(afterSnapshot);
    assert_int_equal(recovered.LastLogIndex, 3);
    assert_int_equal(recovered.Get(3)->Term, 7);
    TDiskState afterAppend(files.Name, 1);
    assert_int_equal(afterAppend.SnapshotIndex, 2);
    assert_int_equal(afterAppend.LastLogIndex, 3);
    assert_int_equal(afterAppend.LastLogTerm, 7);
    assert_int_equal(afterAppend.Get(3)->Term, 7);
}
void test_disk_snapshot_generation_rejects_corrupt_image_without_rewriting(void**) {
    TFiles files;
    files.Seed();
    {
        TDiskState state(files.Name, 1);
        state.InstallSnapshot(TSnapshot{1, 2, {1, 2}, "durable image"});
    }
    fs::resize_file(files.Name + ".state.1.snapshot.g1", 4);
    files.RejectRecovery();
}
void test_disk_compacted_entries_keep_record_checksums(void**) {
    TFiles files;
    files.Seed();
    {
        TDiskState state(files.Name, 1);
        state.InstallSnapshot(TSnapshot{1, 2, {1, 2}, "compacted image"});
    }
    const auto entriesPath = files.Name + ".entries.1.g1";
    std::fstream entries(entriesPath, std::ios::in | std::ios::out | std::ios::binary);
    entries.seekg(sizeof(TLogEntry));
    char checksumByte = 0;
    assert_true(bool(entries.read(&checksumByte, 1)));
    checksumByte ^= 0x20;
    entries.seekp(sizeof(TLogEntry));
    assert_true(bool(entries.write(&checksumByte, 1)));
    entries.flush();
    assert_true(bool(entries));
    entries.close();
    files.RejectRecovery();
}
void test_previous_snapshot_metadata_version_remains_readable(void**) {
    TFiles files;
    files.Seed();
    std::ifstream originalInput(files.Path("entries"), std::ios::binary);
    std::string originalEntries{std::istreambuf_iterator<char>(originalInput), std::istreambuf_iterator<char>()};
    {
        TDiskState state(files.Name, 1);
        state.InstallSnapshot(TSnapshot{1, 2, {1, 2}, "MRFTL003 snapshot"});
    }
    files.Rewrite("entries", originalEntries);
    const uint64_t retainedOffset = sizeof(TLogEntry) + sizeof(uint32_t);
    std::string previousIndex;
    previousIndex.append(reinterpret_cast<const char*>(&retainedOffset), sizeof(retainedOffset));
    NSnapshotCodec::Put(previousIndex,
        NSnapshotCodec::Checksum(std::string_view(reinterpret_cast<const char*>(&retainedOffset), sizeof(retainedOffset))),
        sizeof(uint32_t));
    {
        std::ofstream index(files.Name + ".index.1.g1", std::ios::binary | std::ios::trunc);
        index.write(previousIndex.data(), previousIndex.size());
        assert_true(bool(index));
    }
    std::string metadata;
    NSnapshotCodec::Append(metadata, "MRFTL003");
    NSnapshotCodec::Put(metadata, 3, 4);
    NSnapshotCodec::Put(metadata, 2, 8); // Last log index.
    NSnapshotCodec::Put(metadata, 7, 8); // Current term.
    NSnapshotCodec::Put(metadata, 2, 4); // Voted for.
    NSnapshotCodec::Put(metadata, 1, 8); // Commit index.
    NSnapshotCodec::Put(metadata, 0, 4); // No voter binding.
    NSnapshotCodec::Put(metadata, 3, 4); // Trusted commit + checksummed records.
    NSnapshotCodec::Put(metadata, 1, 8); // Index generation.
    NSnapshotCodec::Put(metadata, 2, 8); // First retained index.
    NSnapshotCodec::Put(metadata, 1, 8); // Snapshot index.
    NSnapshotCodec::Put(metadata, 2, 8); // Snapshot term.
    NSnapshotCodec::Put(metadata, NSnapshotCodec::Checksum(metadata), 4);
    files.Rewrite("state", metadata);

    TDiskState recovered(files.Name, 1);
    const TSnapshot expected{1, 2, {1, 2}, "MRFTL003 snapshot"};
    assert_true(recovered.GetSnapshot() && *recovered.GetSnapshot() == expected);
    assert_int_equal(recovered.LastLogIndex, 2);
    assert_int_equal(recovered.Get(2)->Term, 5);
}
void test_disk_append_rejects_invalid_or_nonmonotonic_terms_before_mutation(void**) {
    TFiles files;
    files.Seed();
    TDiskState state(files.Name, 1);
    for (uint64_t term : {uint64_t(0), uint64_t(4), uint64_t(8)}) {
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = term;
        bool rejected = false;
        try { state.Append(entry); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
        assert_int_equal(state.LastLogIndex, 2);
        assert_int_equal(state.LastLogTerm, 5);
    }
    TDiskState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 2);
    assert_int_equal(recovered.LastLogTerm, 5);
}
#ifndef _WIN32
void test_snapshot_generation_crashes_publish_old_or_new_complete_image(void**) {
    const TDiskState::ECommitStage stages[] = {
        TDiskState::ECommitStage::EntriesPublished,
        TDiskState::ECommitStage::SnapshotIndexPublished,
        TDiskState::ECommitStage::SnapshotImagePublished,
        TDiskState::ECommitStage::LogFilesSynced,
        TDiskState::ECommitStage::MetadataFileSynced,
        TDiskState::ECommitStage::MetadataRenamed,
        TDiskState::ECommitStage::DirectorySynced,
    };
    for (const auto stage : stages) {
        TFiles files;
        files.Seed();
        {
            TDiskState state(files.Name, 1);
            state.CommitIndex = 1;
            state.Commit();
        }
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDiskState state(files.Name, 1, [stage](TDiskState::ECommitStage reached) {
                    if (reached == stage) _exit(120 + static_cast<int>(stage));
                });
                state.InstallSnapshot(TSnapshot{1, 2, {1, 2}, "durable image"});
            } catch (...) { _exit(130); }
            _exit(131);
        }
        int status = 0;
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status));
        assert_int_equal(WEXITSTATUS(status), 120 + static_cast<int>(stage));
        TDiskState recovered(files.Name, 1);
        const bool published = stage == TDiskState::ECommitStage::MetadataRenamed ||
            stage == TDiskState::ECommitStage::DirectorySynced;
        assert_int_equal(recovered.SnapshotIndex, published ? 1 : 0);
        assert_int_equal(recovered.LastLogIndex, 2);
        assert_int_equal(recovered.Get(2)->Term, 5);
        assert_true(bool(recovered.GetSnapshot()) == published);
    }
}
#endif
void test_missing_storage_is_not_recreated(void**) {
    for (const std::string kind : {"state", "index", "entries"}) {
        TFiles files;
        files.Seed();
        fs::remove(files.Path(kind));
        files.RejectRecovery();
        assert_false(fs::exists(files.Path(kind)));
    }
}
void test_short_index_is_rejected(void**) {
    TFiles files;
    files.Seed();
    fs::resize_file(files.Path("index"), sizeof(uint64_t));
    files.RejectRecovery();
}
void test_short_entry_is_rejected(void**) {
    TFiles files;
    files.Seed();
    fs::resize_file(files.Path("entries"), sizeof(TLogEntry) + 4);
    files.RejectRecovery();
}
void test_invalid_entry_header_is_rejected(void**) {
    for (uint32_t len : {0u, 7u, 8u, uint32_t(sizeof(TLogEntry) - 1), UINT32_MAX}) {
        TFiles files;
        files.Seed();
        files.OverwriteEntry(0, sizeof(uint32_t), len);
        files.RejectRecovery();
    }
    TFiles files;
    files.Seed();
    files.OverwriteEntry(0, 0, uint32_t(EMessageType::COMMAND_REQUEST));
    files.RejectRecovery();
}
void test_invalid_offsets_are_rejected(void**) {
    for (uint64_t offset : {uint64_t(0), uint64_t(1), UINT64_MAX}) {
        TFiles files;
        files.Seed();
        files.OverwriteIndexOffset(1, offset);
        files.RejectRecovery();
    }
}
void test_invalid_term_is_rejected(void**) {
    TFiles files;
    files.Seed();
    files.Overwrite("state", 20, uint64_t(0));
    files.RejectRecovery();
}
void test_invalid_log_count_is_rejected(void**) {
    TFiles files;
    files.Seed();
    files.Overwrite("state", 12, UINT64_MAX);
    files.RejectRecovery();
}
void test_truncate_updates_last_term_and_restarts(void**) {
    TFiles files;
    files.Seed();
    {
        TDiskState state(files.Name, 1);
        state.RemoveLast();
        assert_int_equal(state.LastLogIndex, 1);
        assert_int_equal(state.LastLogTerm, 2);
    }
    {
        TDiskState state(files.Name, 1);
        assert_int_equal(state.CurrentTerm, 7);
        assert_int_equal(state.VotedFor, 2);
        assert_int_equal(state.LastLogTerm, 2);
        state.RemoveLast();
        assert_int_equal(state.LastLogIndex, 0);
        assert_int_equal(state.LastLogTerm, 0);
    }
    {
        // Logical truncation leaves a suffix on disk. Reopen it as an empty
        // log, then ensure the replacement really starts at byte zero.
        TDiskState state(files.Name, 1);
        assert_int_equal(state.LastLogIndex, 0);
        assert_int_equal(state.LastLogTerm, 0);
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Term = 7;
        state.Append(entry);
        assert_int_equal(state.Get(1)->Term, 7);
    }
    TDiskState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 1);
    assert_int_equal(recovered.LastLogTerm, 7);
    assert_int_equal(recovered.Get(1)->Term, 7);
    assert_int_equal(recovered.VotedFor, 2);
    // Also replace an empty log without reopening: the shared stream position
    // is still at the end of the removed entry, rather than at byte zero.
    recovered.RemoveLast();
    auto replacement = NewHoldedMessage<TLogEntry>();
    replacement->Term = 6;
    recovered.Append(replacement);
    assert_int_equal(recovered.Get(1)->Term, 6);
    assert_int_equal(recovered.LastLogTerm, 6);
}
void test_zero_log_entry_terms_are_rejected_without_mutation(void**) {
    for (uint64_t index : {0, 1}) {
        TFiles files;
        files.Seed();
        files.OverwriteEntry(index, sizeof(TMessage), uint64_t(0));
        files.RejectRecovery();
    }
}
void test_log_terms_newer_than_current_term_are_rejected(void**) {
    for (uint64_t term : {uint64_t(8), UINT64_MAX}) {
        TFiles files;
        files.Seed();
        files.OverwriteEntry(1, sizeof(TMessage), term);
        files.RejectRecovery();
    }
    TFiles files;
    files.Seed();
    files.Overwrite("state", 20, uint64_t(4));
    files.RejectRecovery();
}
void test_decreasing_log_terms_are_rejected(void**) {
    for (uint64_t index : {0, 1}) {
        TFiles files;
        files.Seed();
        files.OverwriteEntry(index, sizeof(TMessage), uint64_t(index == 0 ? 6 : 1));
        files.RejectRecovery();
    }
}
void test_equal_and_skipped_terms_and_full_width_current_term_recover(void**) {
    TFiles files;
    {
        TDiskState state(files.Name, 1);
        state.CurrentTerm = UINT64_MAX;
        state.VotedFor = 2;
        for (uint64_t term : {uint64_t(1), uint64_t(1), uint64_t(5), UINT64_MAX, UINT64_MAX}) {
            auto entry = NewHoldedMessage<TLogEntry>();
            entry->Term = term;
            state.Append(entry);
        }
    }
    TDiskState recovered(files.Name, 1);
    assert_true(recovered.CurrentTerm == UINT64_MAX);
    assert_true(recovered.LastLogTerm == UINT64_MAX);
    assert_int_equal(recovered.LastLogIndex, 5);
    assert_int_equal(recovered.VotedFor, 2);
    assert_int_equal(recovered.Get(1)->Term, 1);
    assert_int_equal(recovered.Get(2)->Term, 1);
    assert_int_equal(recovered.Get(3)->Term, 5);
}
void test_invalid_terms_in_logically_removed_suffix_are_ignored(void**) {
    TFiles files;
    files.Seed();
    {
        TDiskState state(files.Name, 1);
        state.RemoveLast();
    }
    files.OverwriteEntry(1, sizeof(TMessage), uint64_t(0));
    TDiskState recovered(files.Name, 1);
    assert_int_equal(recovered.LastLogIndex, 1);
    assert_int_equal(recovered.LastLogTerm, 2);
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Term = 7;
    recovered.Append(entry);
    assert_int_equal(recovered.LastLogIndex, 2);
    assert_int_equal(recovered.Get(2)->Term, 7);
}

#ifndef _WIN32
void test_crash_during_metadata_publication_recovers_complete_log_prefix(void**) {
    const TDiskState::ECommitStage stages[] = {
        TDiskState::ECommitStage::LogFilesSynced,
        TDiskState::ECommitStage::MetadataFileSynced,
        TDiskState::ECommitStage::MetadataRenamed,
        TDiskState::ECommitStage::DirectorySynced,
    };
    for (const auto stage : stages) {
        TFiles files;
        files.Seed();
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDiskState state(files.Name, 1, [stage](TDiskState::ECommitStage reached) {
                    if (reached == stage) _exit(80 + static_cast<int>(stage));
                });
                auto entry = NewHoldedMessage<TLogEntry>();
                entry->Term = 7;
                state.Append(entry);
            } catch (...) { _exit(90); }
            _exit(91);
        }
        int status = 0;
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status));
        assert_int_equal(WEXITSTATUS(status), 80 + static_cast<int>(stage));

        TDiskState recovered(files.Name, 1);
        const bool metadataWasPublished = stage == TDiskState::ECommitStage::MetadataRenamed ||
            stage == TDiskState::ECommitStage::DirectorySynced;
        assert_int_equal(recovered.LastLogIndex, metadataWasPublished ? 3 : 2);
        assert_int_equal(recovered.LastLogTerm, metadataWasPublished ? 7 : 5);
        assert_int_equal(recovered.Get(1)->Term, 2);
        assert_int_equal(recovered.Get(2)->Term, 5);
        if (metadataWasPublished) assert_int_equal(recovered.Get(3)->Term, 7);
        assert_int_equal(recovered.CurrentTerm, 7);
        assert_int_equal(recovered.VotedFor, 2);
    }
}

void test_trusted_voter_binding_is_atomic_at_each_metadata_stage(void**) {
    const TDiskState::ECommitStage stages[] = {
        TDiskState::ECommitStage::LogFilesSynced,
        TDiskState::ECommitStage::MetadataFileSynced,
        TDiskState::ECommitStage::MetadataRenamed,
        TDiskState::ECommitStage::DirectorySynced,
    };
    for (const auto stage : stages) {
        TFiles files;
        files.Seed();
        files.WriteLegacyMetadata(2, 7, 2);
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDiskState state(files.Name, 1, [stage](TDiskState::ECommitStage reached) {
                    if (reached == stage) _exit(100 + static_cast<int>(stage));
                });
                state.ValidateTrustedLegacyVoterBaseline({1, 2, 3}, 1);
            } catch (...) { _exit(110); }
            _exit(111);
        }
        int status = 0;
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status));
        assert_int_equal(WEXITSTATUS(status), 100 + static_cast<int>(stage));

        TDiskState recovered(files.Name, 1);
        const bool metadataWasPublished = stage == TDiskState::ECommitStage::MetadataRenamed ||
            stage == TDiskState::ECommitStage::DirectorySynced;
        assert_int_equal(recovered.CommitIndex, metadataWasPublished ? 1 : 0);
        assert_true(recovered.GetBoundVoters() == (metadataWasPublished
            ? std::vector<uint32_t>{1, 2, 3} : std::vector<uint32_t>{}));
        assert_true(recovered.HasUnboundRecoveredState() == !metadataWasPublished);
    }
}

void test_live_storage_failure_is_fatal(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDiskState state(files.Name, 1);
            fs::resize_file(files.Path("entries"), 0);
            state.Get(1);
        } catch (...) { _exit(74); }
        _exit(75);
    }
    int status = 0;
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 73);
}
void test_live_invalid_log_term_is_fatal(void**) {
    for (uint64_t term : {uint64_t(0), uint64_t(8)}) {
        TFiles files;
        files.Seed();
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            std::set_terminate([] { _exit(73); });
            try {
                TDiskState state(files.Name, 1);
                files.OverwriteEntry(0, sizeof(TMessage), term);
                state.Get(1);
            } catch (...) { _exit(74); }
            _exit(75);
        }
        int status = 0;
        assert_int_equal(waitpid(child, &status, 0), child);
        assert_true(WIFEXITED(status));
        assert_int_equal(WEXITSTATUS(status), 73);
    }
}

#endif
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_truncated_metadata_is_not_reset),
        cmocka_unit_test(test_oversized_metadata_is_rejected),
        cmocka_unit_test(test_versioned_metadata_checksum_is_verified),
        cmocka_unit_test(test_checksums_detect_log_and_index_record_corruption),
        cmocka_unit_test(test_versioned_metadata_persists_commit_and_voter_baseline),
        cmocka_unit_test(test_disk_snapshot_generation_recovers_suffix_and_replaces_conflict),
        cmocka_unit_test(test_disk_snapshot_generation_rejects_corrupt_image_without_rewriting),
        cmocka_unit_test(test_disk_compacted_entries_keep_record_checksums),
        cmocka_unit_test(test_previous_snapshot_metadata_version_remains_readable),
        cmocka_unit_test(test_disk_append_rejects_invalid_or_nonmonotonic_terms_before_mutation),
#ifndef _WIN32
        cmocka_unit_test(test_snapshot_generation_crashes_publish_old_or_new_complete_image),
#endif
        cmocka_unit_test(test_missing_storage_is_not_recreated),
        cmocka_unit_test(test_short_index_is_rejected),
        cmocka_unit_test(test_short_entry_is_rejected),
        cmocka_unit_test(test_invalid_entry_header_is_rejected),
        cmocka_unit_test(test_invalid_offsets_are_rejected),
        cmocka_unit_test(test_invalid_term_is_rejected),
        cmocka_unit_test(test_invalid_log_count_is_rejected),
        cmocka_unit_test(test_truncate_updates_last_term_and_restarts),
        cmocka_unit_test(test_zero_log_entry_terms_are_rejected_without_mutation),
        cmocka_unit_test(test_log_terms_newer_than_current_term_are_rejected),
        cmocka_unit_test(test_decreasing_log_terms_are_rejected),
        cmocka_unit_test(test_equal_and_skipped_terms_and_full_width_current_term_recover),
        cmocka_unit_test(test_invalid_terms_in_logically_removed_suffix_are_ignored),
#ifndef _WIN32
        cmocka_unit_test(test_crash_during_metadata_publication_recovers_complete_log_prefix),
        cmocka_unit_test(test_trusted_voter_binding_is_atomic_at_each_metadata_stage),
        cmocka_unit_test(test_live_storage_failure_is_fatal),
        cmocka_unit_test(test_live_invalid_log_term_is_fatal),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
