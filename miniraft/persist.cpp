#include "persist.h"
#include "snapshot_codec.h"
#include <filesystem>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <array>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string_view>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
constexpr std::string_view DiskMetadataMagic = "MRFTL001";
constexpr uint32_t DiskMetadataVersion = 1;
constexpr uint32_t DiskMetadataCommitIndexTrusted = 1u << 0;
constexpr size_t LegacyDiskMetadataSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);
constexpr size_t DiskMetadataFixedSize = 52;

struct TDiskMetadata {
    uint64_t LastLogIndex = 0, CurrentTerm = 1, CommitIndex = 0;
    uint32_t VotedFor = 0;
    std::vector<uint32_t> Voters;
    bool CommitIndexTrusted = false;
};

TDiskMetadata DecodeDiskMetadata(std::string_view bytes) {
    TDiskMetadata metadata;
    if (bytes.size() == LegacyDiskMetadataSize) {
        size_t offset = 0;
        auto take = [&](auto& value) {
            memcpy(&value, bytes.data() + offset, sizeof(value));
            offset += sizeof(value);
        };
        take(metadata.LastLogIndex);
        take(metadata.CurrentTerm);
        take(metadata.VotedFor);
        return metadata; // Old split stores did not persist commit or voters.
    }

    if (bytes.size() < DiskMetadataFixedSize) throw std::runtime_error("Truncated Raft metadata");
    NSnapshotCodec::TReader reader{bytes};
    if (reader.Read(DiskMetadataMagic.size()) != DiskMetadataMagic ||
        reader.Take(4) != DiskMetadataVersion)
        throw std::runtime_error("Unknown Raft metadata format");
    metadata.LastLogIndex = reader.Take(8);
    metadata.CurrentTerm = reader.Take(8);
    metadata.VotedFor = reader.Take(4);
    metadata.CommitIndex = reader.Take(8);
    const auto voterCount = reader.Take(4);
    const auto flags = reader.Take(4);
    if (voterCount > TSnapshot::MaxVoters || (flags & ~DiskMetadataCommitIndexTrusted) ||
        reader.Input.size() != voterCount * sizeof(uint32_t) + sizeof(uint32_t))
        throw std::runtime_error("Invalid Raft metadata dimensions");
    for (uint32_t i = 0; i < voterCount; ++i)
        metadata.Voters.push_back(static_cast<uint32_t>(reader.Take(4)));
    const auto checksum = static_cast<uint32_t>(reader.Take(4));
    if (!reader.Input.empty() ||
        NSnapshotCodec::Checksum(bytes.substr(0, bytes.size() - sizeof(uint32_t))) != checksum)
        throw std::runtime_error("Raft metadata checksum mismatch");
    metadata.CommitIndexTrusted = flags & DiskMetadataCommitIndexTrusted;
    if (!metadata.CurrentTerm || metadata.LastLogIndex > uint64_t(INT64_MAX) ||
        metadata.CommitIndex > metadata.LastLogIndex ||
        (!metadata.Voters.empty() && !metadata.CommitIndexTrusted))
        throw std::runtime_error("Invalid Raft metadata bounds");
    if (!metadata.Voters.empty()) {
        try { ValidateFixedVoters(metadata.Voters); }
        catch (const std::invalid_argument&) { throw std::runtime_error("Invalid Raft metadata voter binding"); }
        if (metadata.VotedFor && !std::binary_search(metadata.Voters.begin(), metadata.Voters.end(), metadata.VotedFor))
            throw std::runtime_error("Stored vote is outside the voter binding");
    }
    return metadata;
}

std::string EncodeDiskMetadata(uint64_t lastLogIndex, uint64_t currentTerm, uint32_t votedFor,
        uint64_t commitIndex, const std::vector<uint32_t>& voters, bool commitIndexTrusted) {
    if (!currentTerm || lastLogIndex > uint64_t(INT64_MAX) || commitIndex > lastLogIndex)
        throw std::logic_error("Invalid Raft metadata bounds");
    if (!voters.empty()) {
        ValidateFixedVoters(voters);
        if (!commitIndexTrusted) throw std::logic_error("Voter binding requires a trusted commit boundary");
        if (votedFor && !std::binary_search(voters.begin(), voters.end(), votedFor))
            throw std::logic_error("Stored vote is outside the voter binding");
    }
    std::string bytes;
    NSnapshotCodec::Append(bytes, DiskMetadataMagic);
    NSnapshotCodec::Put(bytes, DiskMetadataVersion, 4);
    NSnapshotCodec::Put(bytes, lastLogIndex, 8);
    NSnapshotCodec::Put(bytes, currentTerm, 8);
    NSnapshotCodec::Put(bytes, votedFor, 4);
    NSnapshotCodec::Put(bytes, commitIndex, 8);
    NSnapshotCodec::Put(bytes, voters.size(), 4);
    NSnapshotCodec::Put(bytes, commitIndexTrusted ? DiskMetadataCommitIndexTrusted : 0, 4);
    for (auto id : voters) NSnapshotCodec::Put(bytes, id, 4);
    NSnapshotCodec::Put(bytes, NSnapshotCodec::Checksum(bytes), 4);
    return bytes;
}

bool SyncFile(const std::string& path) {
#ifdef _WIN32
    HANDLE file = CreateFileA(path.c_str(), GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE) return false;
    const bool synced = FlushFileBuffers(file) != 0;
    return CloseHandle(file) != 0 && synced;
#else
    const int file = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (file < 0) return false;
    const bool synced = fsync(file) == 0;
    return close(file) == 0 && synced;
#endif
}

bool WriteAndSyncFile(const std::string& path, const char* data, size_t size) {
#ifdef _WIN32
    HANDLE file = CreateFileA(path.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE) return false;
    size_t offset = 0;
    bool ok = true;
    while (offset < size) {
        const auto count = static_cast<DWORD>(std::min<size_t>(size - offset, MAXDWORD));
        DWORD written = 0;
        if (!WriteFile(file, data + offset, count, &written, nullptr) || !written) {
            ok = false;
            break;
        }
        offset += written;
    }
    if (ok) ok = FlushFileBuffers(file) != 0;
    return CloseHandle(file) != 0 && ok;
#else
    int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int file = open(path.c_str(), flags, 0600);
    if (file < 0) return false;
    size_t offset = 0;
    bool ok = true;
    while (offset < size) {
        const auto written = write(file, data + offset, size - offset);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            ok = false;
            break;
        }
        offset += static_cast<size_t>(written);
    }
    if (ok) ok = fsync(file) == 0;
    return close(file) == 0 && ok;
#endif
}

bool ReplaceFile(const std::string& source, const std::string& target) {
#ifdef _WIN32
    return MoveFileExA(source.c_str(), target.c_str(),
        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
#else
    return std::rename(source.c_str(), target.c_str()) == 0;
#endif
}

bool SyncDirectory(const std::string& path) {
#ifdef _WIN32
    // MoveFileEx with MOVEFILE_WRITE_THROUGH makes the replacement operation
    // wait for the move to reach disk on Windows.
    (void)path;
    return true;
#else
    const auto parent = std::filesystem::path(path).parent_path();
    const auto directory = parent.empty() ? std::string(".") : parent.string();
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    const int fd = open(directory.c_str(), flags);
    if (fd < 0) return false;
    const bool synced = fsync(fd) == 0;
    return close(fd) == 0 && synced;
#endif
}
}

TDiskState::TDiskState(const std::string& name, uint32_t id, TCommitHook commitHook)
    : CommitHook(std::move(commitHook))
{
    const auto suffix = "." + std::to_string(id);
    EntriesPath = name + ".entries" + suffix;
    IndexPath = name + ".index" + suffix;
    StatePath = name + ".state" + suffix;
    const int existing = std::filesystem::exists(EntriesPath) +
        std::filesystem::exists(IndexPath) + std::filesystem::exists(StatePath);
    // Never turn a partially missing store into a fresh Raft voter.
    if (existing != 0 && existing != 3) {
        throw std::runtime_error("Incomplete Raft storage: " + name);
    }
    const bool fresh = existing == 0;
    Entries = Open(EntriesPath, fresh);
    Index = Open(IndexPath, fresh);
    State = Open(StatePath, fresh);
    if (fresh) {
        Commit();
        Recovering = false;
        return;
    }

    State.seekg(0, std::ios::end);
    const auto size = State.tellg();
    constexpr auto maxMetadataSize = std::streamoff(DiskMetadataFixedSize + TSnapshot::MaxVoters * sizeof(uint32_t));
    if (size < 0 || (size != std::streamoff(LegacyDiskMetadataSize) &&
        (size < std::streamoff(DiskMetadataFixedSize) || size > maxMetadataSize)))
        throw std::runtime_error("Invalid Raft metadata size: " + StatePath);
    std::string encodedMetadata(static_cast<size_t>(size), '\0');
    State.seekg(0);
    if (!State.read(encodedMetadata.data(), size))
        throw std::runtime_error("Cannot read Raft metadata: " + StatePath);
    const auto metadata = DecodeDiskMetadata(encodedMetadata);
    LastLogIndex = metadata.LastLogIndex;
    CurrentTerm = metadata.CurrentTerm;
    VotedFor = metadata.VotedFor;
    CommitIndex = metadata.CommitIndex;
    Voters = metadata.Voters;
    CommitIndexTrusted = metadata.CommitIndexTrusted;
    RecoveredStore = true;
    if (!CurrentTerm) throw std::runtime_error("Invalid Raft metadata term: " + StatePath);

    Index.seekg(0, std::ios::end);
    const auto indexSize = Index.tellg();
    if (indexSize < 0 || LastLogIndex > uint64_t(indexSize) / sizeof(uint64_t)) {
        throw std::runtime_error("Truncated Raft index: " + IndexPath);
    }
    // Validate the entire selected prefix, not only its last entry. Trailing
    // bytes can be left by logical truncation and are deliberately ignored.
    uint64_t expectedOffset = 0;
    for (uint64_t i = 1; i <= LastLogIndex; ++i) {
        uint64_t offset;
        Index.seekg((i - 1) * sizeof(offset));
        if (!Index.read(reinterpret_cast<char*>(&offset), sizeof(offset)) || offset != expectedOffset) {
            throw std::runtime_error("Invalid Raft log offset: " + IndexPath);
        }
        auto entry = Get(i);
        if (entry->Term < LastLogTerm) {
            throw std::runtime_error("Decreasing Raft log terms: " + EntriesPath);
        }
        expectedOffset += entry->Len;
        LastLogTerm = entry->Term;
    }
    Recovering = false;
}

std::fstream TDiskState::Open(const std::string& name, bool create)
{
    if (create) {
        std::ofstream file(name, std::ios::binary);
        if (!file) throw std::runtime_error("Cannot create file: " + name);
    }
    std::fstream file(name, std::ios::in | std::ios::out | std::ios::binary);
    // An open failure must never fall back to truncating an existing file.
    if (!file) throw std::runtime_error("Cannot open file: " + name);
    return file;
}

[[noreturn]] void TDiskState::Fail(const char* message) const
{
    if (Recovering) throw std::runtime_error(message);
    // The network layer catches ordinary exceptions and keeps the node alive.
    // Continuing after a live storage failure could acknowledge unpersisted
    // votes or logs. Fail-stop instead; startup validation remains catchable.
    std::cerr << "Fatal Raft storage error: " << message << '\n';
    std::terminate();
}

TMessageHolder<TMessage> TDiskState::Read() const
{
    const auto start = Entries.tellg();
    Entries.seekg(0, std::ios::end);
    const auto end = Entries.tellg();
    if (start < 0 || end < start || end - start < std::streamoff(sizeof(TLogEntry))) {
        Fail("Truncated Raft log entry");
    }
    Entries.seekg(start);
    uint32_t type, len;
    if (!Entries.read(reinterpret_cast<char*>(&type), sizeof(type)) ||
        !Entries.read(reinterpret_cast<char*>(&len), sizeof(len)) ||
        type != uint32_t(EMessageType::LOG_ENTRY) || len < sizeof(TLogEntry) ||
        uint64_t(len) > uint64_t(end - start)) {
        Fail("Invalid Raft log entry header");
    }
    // Bound the allocation and subtraction by the validated file extent.
    auto message = NewHoldedMessage<TMessage>(type, len);
    if (!Entries.read(reinterpret_cast<char*>(message->Value), len - sizeof(TMessage))) {
        Fail("Cannot read Raft log entry");
    }
    const auto term = message.Cast<TLogEntry>()->Term;
    if (!term || term > CurrentTerm) {
        Fail("Raft log term outside current term range");
    }
    return message;
}

void TDiskState::Write(TMessageHolder<TMessage> message)
{
    if (!message.HasValidExtent()) Fail("Invalid Raft log entry buffer");
    if (!Entries.write(reinterpret_cast<const char*>(message.Mes), message->Len)) {
        Fail("Cannot write Raft log entry");
    }
}

void TDiskState::RemoveLast()
{
    if (LastLogIndex > 0) {
        if (LastLogIndex <= CommitIndex)
            throw std::logic_error("Cannot remove a committed log entry");
        const auto previousTerm = LastLogIndex == 1 ? 0 : Get(LastLogIndex - 1)->Term;
        --LastLogIndex;
        LastLogTerm = previousTerm;
        Commit();
    }
}

void TDiskState::Append(TMessageHolder<TLogEntry> entry)
{
    uint64_t offset = 0;
    if (Get(LastLogIndex)) {
        offset = Entries.tellg();
    }
    // In particular, after truncation to zero the stream may still point into
    // the old suffix. Always position the output explicitly.
    Entries.seekp(offset);
    Write(entry);
    Index.seekp(LastLogIndex * sizeof(offset));
    if (!Index.write(reinterpret_cast<const char*>(&offset), sizeof(offset))) {
        Fail("Cannot write Raft log index");
    }
    LastLogTerm = entry->Term;
    ++LastLogIndex;
    Commit();
}

TMessageHolder<TLogEntry> TDiskState::Get(int64_t index) const
{
    if (index > LastLogIndex || index < 1) return {};
    uint64_t offset;
    Index.seekg((index - 1) * sizeof(offset));
    if (!Index.read(reinterpret_cast<char*>(&offset), sizeof(offset))) {
        Fail("Cannot read Raft log index");
    }
    Entries.seekg(0, std::ios::end);
    const auto end = Entries.tellg();
    if (end < 0 || offset > uint64_t(end)) {
        Fail("Raft log offset outside file");
    }
    Entries.seekg(offset);
    return Read().Cast<TLogEntry>();
}

void TDiskState::Commit()
{
    // Persist log payload and indexes before atomically publishing the metadata
    // that makes those entries visible during recovery.
    Entries.flush();
    Index.flush();
    if (!Entries || !Index) Fail("Cannot flush Raft log");
    if (!SyncFile(EntriesPath) || !SyncFile(IndexPath)) Fail("Cannot sync Raft log");
    if (CommitHook) CommitHook(ECommitStage::LogFilesSynced);

    if (State.is_open()) {
        State.close();
        if (State.fail()) Fail("Cannot close Raft metadata");
    }
    auto metadata = EncodeDiskMetadata(LastLogIndex, CurrentTerm, VotedFor,
        CommitIndex, Voters, CommitIndexTrusted);

    // The backend requires one process per node, as before. Reusing one
    // same-directory temporary file also makes interrupted writes recoverable
    // without accumulating orphaned temp files across restarts.
    const auto tempPath = StatePath + ".tmp";
    if (!WriteAndSyncFile(tempPath, metadata.data(), metadata.size()))
        Fail("Cannot write and sync Raft metadata");
    if (CommitHook) CommitHook(ECommitStage::MetadataFileSynced);
    if (!ReplaceFile(tempPath, StatePath)) Fail("Cannot atomically publish Raft metadata");
    if (CommitHook) CommitHook(ECommitStage::MetadataRenamed);
    if (!SyncDirectory(StatePath)) Fail("Cannot sync Raft metadata directory");
    if (CommitHook) CommitHook(ECommitStage::DirectorySynced);
}

bool TDiskState::HasUnboundRecoveredState() const {
    const bool emptyBootstrap = CurrentTerm == 1 && !VotedFor && !LastLogIndex && !CommitIndex;
    return RecoveredStore && Voters.empty() && !emptyBootstrap;
}

std::vector<uint32_t> TDiskState::GetBoundVoters() const { return Voters; }

void TDiskState::BindVoters(const std::vector<uint32_t>& voters) {
    ValidateVoterBinding(*this, Voters, voters);
    if (Voters == voters) return;
    const bool pristine = CurrentTerm == 1 && !VotedFor && !LastLogIndex && !CommitIndex;
    if (RecoveredStore && Voters.empty()) {
        if (!pristine)
            throw std::logic_error("Recovered unbound state requires a trusted baseline before binding voters");
        CommitIndexTrusted = true; // The empty legacy store has no unknown commit history.
    }
    Voters = voters;
    Commit();
}

void TDiskState::ValidateTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) {
    ValidateFixedVoters(voters);
    if (RaftStarted)
        throw std::invalid_argument("Trusted legacy voter migration must happen before Raft startup");
    if (!Voters.empty()) {
        if (Voters == voters && CommitIndex == commitIndex && CommitIndexTrusted) return;
        throw std::invalid_argument("Disk-state voter binding already exists");
    }
    if (!RecoveredStore)
        throw std::invalid_argument("Trusted legacy baseline is only for recovered state");
    if (SnapshotIndex || SnapshotTerm)
        throw std::invalid_argument("Legacy disk state cannot validate compacted voter history");
    if (commitIndex > LastLogIndex)
        throw std::invalid_argument("Trusted commit boundary exceeds the retained log");
    if (CommitIndexTrusted && CommitIndex != commitIndex)
        throw std::invalid_argument("Trusted commit boundary differs from persisted metadata");
    if (VotedFor && !std::binary_search(voters.begin(), voters.end(), VotedFor))
        throw std::invalid_argument("Stored vote is outside the trusted voter baseline");
    for (uint64_t index = 1; index <= LastLogIndex; ++index) {
        auto entry = Get(index);
        if (entry->Flags & TLogEntry::EConfiguration)
            throw std::invalid_argument("Legacy disk voter binding requires a log without configuration entries");
    }
    Voters = voters;
    CommitIndex = commitIndex;
    CommitIndexTrusted = true;
    Commit();
}
