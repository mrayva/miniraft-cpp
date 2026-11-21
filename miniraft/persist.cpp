#include "persist.h"
#include "configuration.h"
#include "snapshot_codec.h"
#include <filesystem>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <array>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string_view>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
constexpr std::string_view PreviousDiskMetadataMagic = "MRFTL001";
constexpr std::string_view ChecksummedDiskMetadataMagic = "MRFTL002";
constexpr std::string_view SnapshotDiskMetadataMagic = "MRFTL003";
constexpr std::string_view EntriesGenerationDiskMetadataMagic = "MRFTL004";
constexpr std::string_view DiskMetadataMagic = "MRFTL005";
constexpr uint32_t PreviousDiskMetadataVersion = 1;
constexpr uint32_t ChecksummedDiskMetadataVersion = 2;
constexpr uint32_t SnapshotDiskMetadataVersion = 3;
constexpr uint32_t EntriesGenerationDiskMetadataVersion = 4;
constexpr uint32_t DiskMetadataVersion = 5;
constexpr uint32_t DiskMetadataCommitIndexTrusted = 1u << 0;
constexpr uint32_t DiskMetadataChecksummedRecords = 1u << 1;
constexpr size_t LegacyDiskMetadataSize = sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);
constexpr size_t ChecksummedDiskMetadataFixedSize = 52;
constexpr size_t SnapshotDiskMetadataFixedSize = 84;
constexpr size_t DiskMetadataFixedSize = 92;

struct TDiskMetadata {
    uint64_t LastLogIndex = 0, CurrentTerm = 1, CommitIndex = 0;
    uint64_t EntriesGeneration = 0, IndexGeneration = 0, IndexBaseIndex = 1;
    uint64_t SnapshotIndex = 0, SnapshotTerm = 0;
    uint32_t VotedFor = 0;
    std::vector<uint32_t> Voters;
    bool CommitIndexTrusted = false, ChecksummedRecords = false;
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

    if (bytes.size() < ChecksummedDiskMetadataFixedSize) throw std::runtime_error("Truncated Raft metadata");
    NSnapshotCodec::TReader reader{bytes};
    const auto magic = reader.Read(DiskMetadataMagic.size());
    const auto version = reader.Take(4);
    const bool previousFormat = magic == PreviousDiskMetadataMagic && version == PreviousDiskMetadataVersion;
    const bool checksummedFormat = magic == ChecksummedDiskMetadataMagic && version == ChecksummedDiskMetadataVersion;
    const bool snapshotFormat = magic == SnapshotDiskMetadataMagic && version == SnapshotDiskMetadataVersion;
    const bool entriesGenerationFormat = magic == EntriesGenerationDiskMetadataMagic &&
        version == EntriesGenerationDiskMetadataVersion;
    const bool currentFormat = magic == DiskMetadataMagic && version == DiskMetadataVersion;
    if (!previousFormat && !checksummedFormat && !snapshotFormat && !entriesGenerationFormat && !currentFormat)
        throw std::runtime_error("Unknown Raft metadata format");
    metadata.LastLogIndex = reader.Take(8);
    metadata.CurrentTerm = reader.Take(8);
    metadata.VotedFor = reader.Take(4);
    metadata.CommitIndex = reader.Take(8);
    const auto voterCount = reader.Take(4);
    const auto flags = reader.Take(4);
    const auto allowedFlags = DiskMetadataCommitIndexTrusted |
        ((checksummedFormat || snapshotFormat || entriesGenerationFormat || currentFormat)
            ? DiskMetadataChecksummedRecords : 0u);
    if (voterCount > TSnapshot::MaxVoters || (flags & ~allowedFlags))
        throw std::runtime_error("Invalid Raft metadata dimensions");
    if (snapshotFormat || entriesGenerationFormat || currentFormat) {
        if (entriesGenerationFormat || currentFormat) metadata.EntriesGeneration = reader.Take(8);
        metadata.IndexGeneration = reader.Take(8);
        metadata.IndexBaseIndex = reader.Take(8);
        metadata.SnapshotIndex = reader.Take(8);
        metadata.SnapshotTerm = reader.Take(8);
    }
    if (reader.Input.size() != voterCount * sizeof(uint32_t) + sizeof(uint32_t))
        throw std::runtime_error("Invalid Raft metadata dimensions");
    for (uint32_t i = 0; i < voterCount; ++i)
        metadata.Voters.push_back(static_cast<uint32_t>(reader.Take(4)));
    const auto checksum = static_cast<uint32_t>(reader.Take(4));
    if (!reader.Input.empty() ||
        NSnapshotCodec::Checksum(bytes.substr(0, bytes.size() - sizeof(uint32_t))) != checksum)
        throw std::runtime_error("Raft metadata checksum mismatch");
    metadata.CommitIndexTrusted = flags & DiskMetadataCommitIndexTrusted;
    metadata.ChecksummedRecords =
        (checksummedFormat || snapshotFormat || entriesGenerationFormat || currentFormat) &&
        (flags & DiskMetadataChecksummedRecords);
    if (!metadata.CurrentTerm || metadata.LastLogIndex > uint64_t(INT64_MAX) ||
        metadata.CommitIndex > metadata.LastLogIndex ||
        metadata.IndexBaseIndex != metadata.SnapshotIndex + 1 ||
        metadata.SnapshotIndex > metadata.LastLogIndex ||
        ((metadata.SnapshotIndex == 0) != (metadata.SnapshotTerm == 0)) ||
        metadata.SnapshotTerm > metadata.CurrentTerm ||
        metadata.CommitIndex < metadata.SnapshotIndex ||
        ((!currentFormat && ((metadata.IndexGeneration == 0) != (metadata.SnapshotIndex == 0))) ||
            (currentFormat && !metadata.IndexGeneration && metadata.SnapshotIndex)) ||
        (metadata.EntriesGeneration && metadata.EntriesGeneration != metadata.IndexGeneration) ||
        (!metadata.Voters.empty() && !metadata.CommitIndexTrusted))
        throw std::runtime_error("Invalid Raft metadata bounds");
    if (!metadata.Voters.empty()) {
        try { ValidateFixedVoters(metadata.Voters); }
        catch (const std::invalid_argument&) { throw std::runtime_error("Invalid Raft metadata voter binding"); }
        // VotedFor may name a voter added after this original storage binding;
        // membership-aware validation runs after the snapshot and log recover.
    }
    return metadata;
}

std::string EncodeDiskMetadata(uint64_t lastLogIndex, uint64_t currentTerm, uint32_t votedFor,
        uint64_t commitIndex, const std::vector<uint32_t>& voters, bool commitIndexTrusted,
        bool checksummedRecords, uint64_t entriesGeneration, uint64_t indexGeneration, uint64_t indexBaseIndex,
        uint64_t snapshotIndex, uint64_t snapshotTerm) {
    if (!currentTerm || lastLogIndex > uint64_t(INT64_MAX) || commitIndex > lastLogIndex ||
        indexBaseIndex != snapshotIndex + 1 || snapshotIndex > lastLogIndex ||
        ((snapshotIndex == 0) != (snapshotTerm == 0)) || snapshotTerm > currentTerm ||
        commitIndex < snapshotIndex || (!indexGeneration && snapshotIndex) ||
        (entriesGeneration && entriesGeneration != indexGeneration))
        throw std::logic_error("Invalid Raft metadata bounds");
    if (!voters.empty()) {
        ValidateFixedVoters(voters);
        if (!commitIndexTrusted) throw std::logic_error("Voter binding requires a trusted commit boundary");
        // Do not compare votedFor with this original voter set. Runtime
        // membership changes can make a later voter a valid election target.
    }
    std::string bytes;
    NSnapshotCodec::Append(bytes, DiskMetadataMagic);
    NSnapshotCodec::Put(bytes, DiskMetadataVersion, 4);
    NSnapshotCodec::Put(bytes, lastLogIndex, 8);
    NSnapshotCodec::Put(bytes, currentTerm, 8);
    NSnapshotCodec::Put(bytes, votedFor, 4);
    NSnapshotCodec::Put(bytes, commitIndex, 8);
    NSnapshotCodec::Put(bytes, voters.size(), 4);
    const uint32_t flags = (commitIndexTrusted ? DiskMetadataCommitIndexTrusted : 0) |
        (checksummedRecords ? DiskMetadataChecksummedRecords : 0);
    NSnapshotCodec::Put(bytes, flags, 4);
    NSnapshotCodec::Put(bytes, entriesGeneration, 8);
    NSnapshotCodec::Put(bytes, indexGeneration, 8);
    NSnapshotCodec::Put(bytes, indexBaseIndex, 8);
    NSnapshotCodec::Put(bytes, snapshotIndex, 8);
    NSnapshotCodec::Put(bytes, snapshotTerm, 8);
    for (auto id : voters) NSnapshotCodec::Put(bytes, id, 4);
    NSnapshotCodec::Put(bytes, NSnapshotCodec::Checksum(bytes), 4);
    return bytes;
}

class TSafeTempFile {
public:
    ~TSafeTempFile() { (void)Close(); }

    bool Open(const std::string& path) {
#ifdef _WIN32
        if (!DeleteFileA(path.c_str())) {
            const auto error = GetLastError();
            if (error != ERROR_FILE_NOT_FOUND && error != ERROR_PATH_NOT_FOUND) return false;
        }
        File = CreateFileA(path.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        if (File == INVALID_HANDLE_VALUE) return false;
        BY_HANDLE_FILE_INFORMATION info{};
        if (!GetFileInformationByHandle(File, &info) ||
            (info.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY))) {
            (void)Close();
            return false;
        }
        return true;
#else
        if (unlink(path.c_str()) != 0 && errno != ENOENT) return false;
#ifdef O_NOFOLLOW
        const int flags = O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW
#ifdef O_NONBLOCK
            | O_NONBLOCK
#endif
            ;
        File = open(path.c_str(), flags, 0600);
        if (File < 0) return false;
        struct stat info{};
        if (fstat(File, &info) != 0 || !S_ISREG(info.st_mode)) {
            (void)Close();
            return false;
        }
        return true;
#else
        // Fail closed on platforms without a way to forbid following the final
        // path component.
        return false;
#endif
#endif
    }

    bool Write(const char* data, size_t size) {
        size_t offset = 0;
        while (offset < size) {
#ifdef _WIN32
            const auto count = static_cast<DWORD>(std::min<size_t>(size - offset, MAXDWORD));
            DWORD written = 0;
            if (!WriteFile(File, data + offset, count, &written, nullptr) || !written) return false;
            offset += written;
#else
            const auto count = std::min<size_t>(size - offset,
                static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
            const auto written = write(File, data + offset, count);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) return false;
            offset += static_cast<size_t>(written);
#endif
        }
        return true;
    }

    bool SyncAndClose() {
#ifdef _WIN32
        const bool synced = File != INVALID_HANDLE_VALUE && FlushFileBuffers(File) != 0;
#else
        bool synced = false;
        if (File >= 0) {
            int result;
            do { result = fsync(File); } while (result != 0 && errno == EINTR);
            synced = result == 0;
        }
#endif
        return Close() && synced;
    }

private:
    bool Close() {
#ifdef _WIN32
        if (File == INVALID_HANDLE_VALUE) return true;
        const auto file = File;
        File = INVALID_HANDLE_VALUE;
        return CloseHandle(file) != 0;
#else
        if (File < 0) return true;
        const auto file = File;
        File = -1;
        return close(file) == 0;
#endif
    }

#ifdef _WIN32
    HANDLE File = INVALID_HANDLE_VALUE;
#else
    int File = -1;
#endif
};

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

bool TruncateAndSyncRegularFileNoFollow(const std::string& path) {
#ifdef _WIN32
    HANDLE file = CreateFileA(path.c_str(), GENERIC_WRITE, 0, nullptr, OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
    if (file == INVALID_HANDLE_VALUE) return false;
    BY_HANDLE_FILE_INFORMATION info{};
    bool ok = GetFileInformationByHandle(file, &info) != 0 &&
        !(info.dwFileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY));
    LARGE_INTEGER beginning{};
    if (ok) ok = SetFilePointerEx(file, beginning, nullptr, FILE_BEGIN) != 0 && SetEndOfFile(file) != 0;
    if (ok) ok = FlushFileBuffers(file) != 0;
    return CloseHandle(file) != 0 && ok;
#else
#ifdef O_NOFOLLOW
    int flags = O_WRONLY | O_CLOEXEC | O_NOFOLLOW;
#ifdef O_NONBLOCK
    flags |= O_NONBLOCK;
#endif
    const int file = open(path.c_str(), flags);
    if (file < 0) return false;
    struct stat info{};
    bool ok = fstat(file, &info) == 0 && S_ISREG(info.st_mode) && ftruncate(file, 0) == 0;
    if (ok) {
        int result;
        do { result = fsync(file); } while (result != 0 && errno == EINTR);
        ok = result == 0;
    }
    return close(file) == 0 && ok;
#else
    return false;
#endif
#endif
}

bool IsRegularFileNoFollow(const std::string& path) {
#ifdef _WIN32
    const auto attributes = GetFileAttributesA(path.c_str());
    return attributes != INVALID_FILE_ATTRIBUTES &&
        !(attributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY));
#else
    struct stat info{};
    return lstat(path.c_str(), &info) == 0 && S_ISREG(info.st_mode);
#endif
}

bool WriteAndSyncFile(const std::string& path, const char* data, size_t size) {
    TSafeTempFile output;
    return output.Open(path) && output.Write(data, size) && output.SyncAndClose();
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
    EntriesBasePath = name + ".entries" + suffix;
    EntriesPath = EntriesBasePath;
    IndexBasePath = name + ".index" + suffix;
    IndexPath = IndexBasePath;
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
        (size < std::streamoff(ChecksummedDiskMetadataFixedSize) || size > maxMetadataSize)))
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
    ChecksummedRecords = metadata.ChecksummedRecords;
    EntriesGeneration = metadata.EntriesGeneration;
    IndexGeneration = metadata.IndexGeneration;
    IndexBaseIndex = metadata.IndexBaseIndex;
    SnapshotIndex = metadata.SnapshotIndex;
    SnapshotTerm = metadata.SnapshotTerm;
    if (EntriesGeneration) {
        Entries.close();
        if (Entries.fail()) throw std::runtime_error("Cannot close base Raft log file: " + EntriesBasePath);
        EntriesPath = EntryGenerationPath(EntriesGeneration);
        Entries = Open(EntriesPath, false);
    }
    if (IndexGeneration) {
        Index.close();
        IndexPath = GenerationPath(IndexGeneration);
        Index = Open(IndexPath, false);
        SnapshotFilePath = SnapshotPath(IndexGeneration);
        if (SnapshotIndex) {
            if (!IsRegularFileNoFollow(SnapshotFilePath))
                throw std::runtime_error("Missing or invalid Raft snapshot: " + SnapshotFilePath);
            std::ifstream snapshotFile(SnapshotFilePath, std::ios::binary);
            if (!snapshotFile) throw std::runtime_error("Cannot open Raft snapshot: " + SnapshotFilePath);
            std::string encodedSnapshot{std::istreambuf_iterator<char>(snapshotFile), std::istreambuf_iterator<char>()};
            Snapshot = std::make_shared<const TSnapshot>(DecodeSnapshotOwned(std::move(encodedSnapshot)));
            if (Snapshot->Index != SnapshotIndex || Snapshot->Term != SnapshotTerm)
                throw std::runtime_error("Raft snapshot boundary disagrees with metadata: " + SnapshotFilePath);
        } else if (SnapshotTerm) {
            throw std::runtime_error("Raft snapshot term has no snapshot boundary: " + StatePath);
        } else {
            SnapshotFilePath.clear();
        }
    }
    RecoveredStore = true;
    if (!CurrentTerm) throw std::runtime_error("Invalid Raft metadata term: " + StatePath);

    Index.seekg(0, std::ios::end);
    const auto indexSize = Index.tellg();
    const auto retainedCount = LastLogIndex - SnapshotIndex;
    if (indexSize < 0 || retainedCount > uint64_t(indexSize) / IndexRecordSize()) {
        throw std::runtime_error("Truncated Raft index: " + IndexPath);
    }
    // Validate the entire selected prefix, not only its last entry. Trailing
    // bytes can be left by logical truncation and are deliberately ignored.
    LastLogTerm = SnapshotTerm;
    uint64_t expectedOffset = retainedCount ? ReadOffset(IndexBaseIndex) : 0;
    for (uint64_t i = IndexBaseIndex; i <= LastLogIndex; ++i) {
        const auto offset = ReadOffset(i);
        if (offset != expectedOffset) {
            throw std::runtime_error("Invalid Raft log offset: " + IndexPath);
        }
        auto entry = Get(i);
        if (entry->Term < LastLogTerm) {
            throw std::runtime_error("Decreasing Raft log terms: " + EntriesPath);
        }
        expectedOffset += entry->Len + (ChecksummedRecords ? sizeof(uint32_t) : 0);
        LastLogTerm = entry->Term;
    }
    ValidateStoredVoteMembershipHistory();
    Recovering = false;
    if (EntriesGeneration && EntriesGeneration == IndexGeneration)
        ReclaimPreviousGeneration(EntriesGeneration - 1, IndexGeneration - 1);
}

std::string TDiskState::GenerationPath(uint64_t generation) const {
    return IndexBasePath + ".g" + std::to_string(generation);
}

std::string TDiskState::EntryGenerationPath(uint64_t generation) const {
    return EntriesBasePath + ".g" + std::to_string(generation);
}

std::string TDiskState::SnapshotPath(uint64_t generation) const {
    return StatePath + ".snapshot.g" + std::to_string(generation);
}

std::fstream TDiskState::Open(const std::string& name, bool create)
{
#ifdef _WIN32
    const auto attributes = GetFileAttributesA(name.c_str());
    bool missing = false;
    if (attributes == INVALID_FILE_ATTRIBUTES) {
        const auto error = GetLastError();
        if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) missing = true;
        else throw std::runtime_error("Cannot inspect Raft storage path: " + name);
    } else if (attributes & FILE_ATTRIBUTE_REPARSE_POINT) {
        throw std::runtime_error("Raft storage path cannot be a reparse point: " + name);
    } else if (attributes & FILE_ATTRIBUTE_DIRECTORY) {
        throw std::runtime_error("Raft storage path must be a regular file: " + name);
    }
#else
    std::error_code statusError;
    const auto status = std::filesystem::symlink_status(name, statusError);
    if (statusError && statusError != std::errc::no_such_file_or_directory)
        throw std::runtime_error("Cannot inspect Raft storage path: " + name);
    const bool missing = status.type() == std::filesystem::file_type::not_found ||
        status.type() == std::filesystem::file_type::none ||
        statusError == std::errc::no_such_file_or_directory;
    if (!missing && std::filesystem::is_symlink(status))
        throw std::runtime_error("Raft storage path cannot be a symlink: " + name);
    if (!missing && !std::filesystem::is_regular_file(status))
        throw std::runtime_error("Raft storage path must be a regular file: " + name);
#endif
    if (create) {
        if (!missing) throw std::runtime_error("Raft storage file already exists: " + name);
        TSafeTempFile file;
        if (!file.Open(name) || !file.SyncAndClose())
            throw std::runtime_error("Cannot safely create file: " + name);
    } else if (missing) {
        throw std::runtime_error("Cannot open file: " + name);
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
        uint64_t(len) + (ChecksummedRecords ? sizeof(uint32_t) : 0) > uint64_t(end - start)) {
        Fail("Invalid Raft log entry header");
    }
    // Bound the allocation and subtraction by the validated file extent.
    auto message = NewHoldedMessage<TMessage>(type, len);
    if (!Entries.read(reinterpret_cast<char*>(message->Value), len - sizeof(TMessage))) {
        Fail("Cannot read Raft log entry");
    }
    if (ChecksummedRecords) {
        char checksumBytes[sizeof(uint32_t)];
        if (!Entries.read(checksumBytes, sizeof(checksumBytes))) Fail("Cannot read Raft log checksum");
        NSnapshotCodec::TReader reader{std::string_view(checksumBytes, sizeof(checksumBytes))};
        const auto expected = static_cast<uint32_t>(reader.Take(4));
        const auto actual = NSnapshotCodec::Checksum(std::string_view(
            reinterpret_cast<const char*>(message.Mes), len));
        if (actual != expected) Fail("Raft log entry checksum mismatch");
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
    if (ChecksummedRecords) {
        const auto checksum = NSnapshotCodec::Checksum(std::string_view(
            reinterpret_cast<const char*>(message.Mes), message->Len));
        std::string encoded;
        NSnapshotCodec::Put(encoded, checksum, sizeof(uint32_t));
        if (!Entries.write(encoded.data(), encoded.size())) Fail("Cannot write Raft log checksum");
    }
}

uint64_t TDiskState::ReadOffset(uint64_t index) const {
    if (index < IndexBaseIndex || index > LastLogIndex ||
        index - IndexBaseIndex > uint64_t(INT64_MAX) / IndexRecordSize())
        Fail("Raft index position overflow");
    const auto position = std::streamoff((index - IndexBaseIndex) * IndexRecordSize());
    Index.seekg(position);
    std::array<char, sizeof(uint64_t)> offsetBytes{};
    if (!Index.read(offsetBytes.data(), offsetBytes.size())) Fail("Cannot read Raft log index");
    uint64_t offset;
    memcpy(&offset, offsetBytes.data(), sizeof(offset));
    if (ChecksummedRecords) {
        char checksumBytes[sizeof(uint32_t)];
        if (!Index.read(checksumBytes, sizeof(checksumBytes))) Fail("Cannot read Raft index checksum");
        NSnapshotCodec::TReader reader{std::string_view(checksumBytes, sizeof(checksumBytes))};
        const auto expected = static_cast<uint32_t>(reader.Take(4));
        if (NSnapshotCodec::Checksum(std::string_view(offsetBytes.data(), offsetBytes.size())) != expected)
            Fail("Raft index checksum mismatch");
    }
    return offset;
}

void TDiskState::WriteOffset(uint64_t index, uint64_t offset) {
    if (index < SnapshotIndex || index - SnapshotIndex > uint64_t(INT64_MAX) / IndexRecordSize())
        Fail("Raft index position overflow");
    const auto position = std::streamoff((index - SnapshotIndex) * IndexRecordSize());
    Index.seekp(position);
    std::array<char, sizeof(uint64_t)> offsetBytes{};
    memcpy(offsetBytes.data(), &offset, sizeof(offset));
    if (!Index.write(offsetBytes.data(), offsetBytes.size())) Fail("Cannot write Raft log index");
    if (ChecksummedRecords) {
        std::string checksum;
        NSnapshotCodec::Put(checksum,
            NSnapshotCodec::Checksum(std::string_view(offsetBytes.data(), offsetBytes.size())), sizeof(uint32_t));
        if (!Index.write(checksum.data(), checksum.size())) Fail("Cannot write Raft index checksum");
    }
}

void TDiskState::RemoveLast()
{
    if (LastLogIndex > SnapshotIndex) {
        if (LastLogIndex <= CommitIndex)
            throw std::logic_error("Cannot remove a committed log entry");
        const auto previousTerm = LastLogIndex - 1 == SnapshotIndex
            ? SnapshotTerm : Get(LastLogIndex - 1)->Term;
        --LastLogIndex;
        LastLogTerm = previousTerm;
        Commit();
    }
}

void TDiskState::Append(TMessageHolder<TLogEntry> entry)
{
    std::vector<TMessageHolder<TLogEntry>> entries;
    entries.push_back(std::move(entry));
    AppendBatchAndCommit(entries, CommitIndex);
}

void TDiskState::AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries)
{
    AppendBatchAndCommit(entries, CommitIndex);
}

void TDiskState::AppendBatchAndCommit(
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex)
{
    if (LastLogIndex > uint64_t(INT64_MAX) ||
        entries.size() > uint64_t(INT64_MAX) - LastLogIndex)
        throw std::overflow_error("Raft log index exhausted");
    const auto newLastLogIndex = LastLogIndex + entries.size();
    const auto commitFloor = std::max(CommitIndex, SnapshotIndex);
    if (commitIndex < commitFloor || commitIndex > newLastLogIndex)
        throw std::logic_error("Invalid batched Raft commit boundary");

    auto previousTerm = LastLogTerm;
    for (const auto& entry : entries) {
        if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
            entry->Len < sizeof(TLogEntry))
            throw std::invalid_argument("Invalid Raft log entry");
        if (!entry->Term || entry->Term < previousTerm || entry->Term > CurrentTerm)
            throw std::invalid_argument("Raft log entry term outside current log range");
        previousTerm = entry->Term;
    }
    if (entries.empty() && commitIndex == CommitIndex) return;

    uint64_t offset = 0;
    if (!entries.empty()) {
        if (LastLogIndex > SnapshotIndex && Get(LastLogIndex)) {
            const auto end = Entries.tellg();
            if (end < 0) Fail("Cannot determine Raft log end");
            offset = uint64_t(end);
        } else {
            Entries.seekg(0, std::ios::end);
            const auto end = Entries.tellg();
            if (end < 0) Fail("Cannot determine Raft log end");
            offset = uint64_t(end);
        }

        auto index = LastLogIndex;
        for (const auto& entry : entries) {
            const auto recordBytes = uint64_t(entry->Len) +
                (ChecksummedRecords ? sizeof(uint32_t) : 0);
            if (offset > uint64_t(INT64_MAX) - recordBytes)
                throw std::overflow_error("Raft log file size exhausted");
            Entries.seekp(std::streamoff(offset));
            Write(entry);
            WriteOffset(index, offset);
            const auto end = Entries.tellp();
            if (end < 0) Fail("Cannot determine Raft log end");
            offset = uint64_t(end);
            ++index;
        }
    }

    LastLogIndex = newLastLogIndex;
    LastLogTerm = previousTerm;
    CommitIndex = commitIndex;
    Commit();
}

void TDiskState::ReplaceSuffixAndAppendBatch(uint64_t firstIndex,
        const std::vector<TMessageHolder<TLogEntry>>& entries, uint64_t commitIndex)
{
    if (LastLogIndex > uint64_t(INT64_MAX))
        throw std::overflow_error("Raft log index exhausted");
    const auto commitFloor = std::max(CommitIndex, SnapshotIndex);
    if (firstIndex <= commitFloor || firstIndex > LastLogIndex + 1)
        throw std::logic_error("Invalid Raft suffix replacement boundary");
    const auto retainedLastIndex = firstIndex - 1;
    if (entries.size() > uint64_t(INT64_MAX) - retainedLastIndex)
        throw std::overflow_error("Raft log index exhausted");
    const auto newLastLogIndex = retainedLastIndex + entries.size();
    if (commitIndex < commitFloor || commitIndex > newLastLogIndex)
        throw std::logic_error("Invalid Raft suffix replacement commit boundary");
    if (EntriesGeneration && EntriesGeneration != IndexGeneration)
        throw std::logic_error("Inconsistent Raft log generations");
    const auto currentGeneration = std::max(EntriesGeneration, IndexGeneration);
    if (currentGeneration == UINT64_MAX)
        throw std::overflow_error("Disk log generation exhausted");

    uint64_t previousTerm = SnapshotTerm;
    if (retainedLastIndex > SnapshotIndex) {
        auto retained = Get(static_cast<int64_t>(retainedLastIndex));
        if (!retained) Fail("Missing retained entry before Raft suffix replacement");
        previousTerm = retained->Term;
    }
    for (const auto& entry : entries) {
        if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
            entry->Len < sizeof(TLogEntry))
            throw std::invalid_argument("Invalid Raft log entry");
        if (!entry->Term || entry->Term < previousTerm || entry->Term > CurrentTerm)
            throw std::invalid_argument("Raft log entry term outside current log range");
        previousTerm = entry->Term;
    }

    const auto previousEntriesGeneration = EntriesGeneration;
    const auto previousIndexGeneration = IndexGeneration;
    const auto nextGeneration = currentGeneration + 1;
    const auto nextEntriesPath = EntryGenerationPath(nextGeneration);
    const auto nextIndexPath = GenerationPath(nextGeneration);
    uint64_t nextLastTerm = SnapshotTerm;
    const auto retainedCount = WriteCompactedLogGeneration(nextEntriesPath, nextIndexPath,
        SnapshotIndex + 1, retainedLastIndex, nextLastTerm, entries);
    const auto nextSnapshotPath = SnapshotPath(nextGeneration);
    if (Snapshot) {
        WriteSnapshotGenerationFile(nextSnapshotPath, *Snapshot);
        if (CommitHook) CommitHook(ECommitStage::SnapshotImagePublished);
    }

    Entries.close();
    if (Entries.fail()) Fail("Cannot close previous Raft log file");
    Index.close();
    if (Index.fail()) Fail("Cannot close previous Raft log index");
    EntriesGeneration = nextGeneration;
    EntriesPath = nextEntriesPath;
    try { Entries = Open(EntriesPath, false); }
    catch (...) { Fail("Cannot open replacement Raft log generation"); }
    IndexGeneration = nextGeneration;
    IndexBaseIndex = SnapshotIndex + 1;
    IndexPath = nextIndexPath;
    try { Index = Open(IndexPath, false); }
    catch (...) { Fail("Cannot open replacement Raft log index"); }
    SnapshotFilePath = Snapshot ? nextSnapshotPath : std::string{};
    LastLogIndex = SnapshotIndex + retainedCount;
    if (LastLogIndex != newLastLogIndex)
        Fail("Replacement Raft log generation has an invalid length");
    LastLogTerm = nextLastTerm;
    CommitIndex = commitIndex;
    Commit();
    ReclaimPreviousGeneration(previousEntriesGeneration, previousIndexGeneration);
}

TMessageHolder<TLogEntry> TDiskState::Get(int64_t index) const
{
    if (index > LastLogIndex || index < 1 || uint64_t(index) <= SnapshotIndex) return {};
    const auto offset = ReadOffset(uint64_t(index));
    Entries.seekg(0, std::ios::end);
    const auto end = Entries.tellg();
    if (end < 0 || offset > uint64_t(end)) {
        Fail("Raft log offset outside file");
    }
    Entries.seekg(offset);
    return Read().Cast<TLogEntry>();
}

void TDiskState::InstallSnapshot(const TSnapshot& snapshot) {
    InstallSnapshotImpl(snapshot, false);
}

void TDiskState::InstallRaftSnapshot(const TSnapshot& snapshot,
        const TRaftInstallSnapshotPermit&) {
    InstallSnapshotImpl(snapshot, true);
}

void TDiskState::InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        const TRaftInstallSnapshotPermit&) {
    InstallSnapshotImpl(std::move(snapshot));
}

void TDiskState::InstallSnapshotImpl(const TSnapshot& snapshot, bool configurationHistoryValidated) {
    ValidateSnapshot(snapshot);
    TSnapshot image = snapshot;
    if (!configurationHistoryValidated) PreserveStableSnapshotIdentity(Snapshot.get(), image);
    ValidateSnapshot(image);
    InstallValidatedSnapshot(std::make_shared<const TSnapshot>(std::move(image)));
}

void TDiskState::InstallSnapshotImpl(std::shared_ptr<const TSnapshot> snapshot) {
    if (!snapshot) throw std::invalid_argument("Missing Raft snapshot");
    ValidateSnapshot(*snapshot);
    InstallValidatedSnapshot(std::move(snapshot));
}

void TDiskState::InstallValidatedSnapshot(std::shared_ptr<const TSnapshot> snapshot) {
    if (!SnapshotCompatibleWithVoterBinding(*snapshot, Voters))
        throw std::invalid_argument("Snapshot is incompatible with the persisted voter binding");
    if (snapshot->Index <= SnapshotIndex || snapshot->Term < SnapshotTerm || snapshot->Term > CurrentTerm)
        throw std::invalid_argument("Obsolete or future disk snapshot");
    if (snapshot->Index <= CommitIndex &&
        (snapshot->Index > LastLogIndex || LogTerm(snapshot->Index) != snapshot->Term))
        throw std::invalid_argument("Snapshot would discard committed log entries");

    const bool retainSuffix = snapshot->Index <= LastLogIndex &&
        LogTerm(snapshot->Index) == snapshot->Term;
    if (IndexGeneration == UINT64_MAX) throw std::overflow_error("Disk snapshot generation exhausted");
    const auto previousEntriesGeneration = EntriesGeneration;
    const auto previousIndexGeneration = IndexGeneration;
    const auto nextGeneration = IndexGeneration + 1;
    uint64_t nextLastTerm = snapshot->Term;
    const auto nextEntriesPath = EntryGenerationPath(nextGeneration);
    const auto nextIndexPath = GenerationPath(nextGeneration);
    const auto nextLastIndex = retainSuffix ? LastLogIndex : snapshot->Index;
    const auto suffixCount = WriteCompactedLogGeneration(nextEntriesPath, nextIndexPath,
        snapshot->Index + 1, nextLastIndex, nextLastTerm);
    const auto nextSnapshotPath = SnapshotPath(nextGeneration);
    WriteSnapshotGenerationFile(nextSnapshotPath, *snapshot);
    if (CommitHook) CommitHook(ECommitStage::SnapshotImagePublished);

    Entries.close();
    if (Entries.fail()) Fail("Cannot close previous Raft log file");
    Index.close();
    if (Index.fail()) Fail("Cannot close previous Raft log index");
    EntriesGeneration = nextGeneration;
    EntriesPath = nextEntriesPath;
    try { Entries = Open(EntriesPath, false); }
    catch (...) { Fail("Cannot open new Raft log generation"); }
    IndexGeneration = nextGeneration;
    IndexBaseIndex = snapshot->Index + 1;
    IndexPath = nextIndexPath;
    try { Index = Open(IndexPath, false); }
    catch (...) { Fail("Cannot open new Raft log index"); }
    SnapshotFilePath = nextSnapshotPath;
    Snapshot = std::move(snapshot);
    SnapshotIndex = Snapshot->Index;
    SnapshotTerm = Snapshot->Term;
    LastLogIndex = SnapshotIndex + suffixCount;
    LastLogTerm = nextLastTerm;
    CommitIndex = std::max(CommitIndex, SnapshotIndex);
    Commit();
    // Metadata now selects the complete replacement. Reclaiming prior files
    // before this commit would make a crash lose the only selected log image.
    ReclaimPreviousGeneration(previousEntriesGeneration, previousIndexGeneration);
}

void TDiskState::WriteSnapshotGenerationFile(const std::string& path, const TSnapshot& snapshot) {
    const auto encoding = PrepareSnapshotEncoding(snapshot);
    const auto totalBytes = SnapshotEncodingSize(*encoding);
    const auto tempPath = path + ".tmp";
    TSafeTempFile output;
    if (!output.Open(tempPath)) Fail("Cannot create Raft snapshot generation file");
    std::array<char, 64 * 1024> buffer{};
    for (uint64_t offset = 0; offset < totalBytes;) {
        const auto length = std::min<uint64_t>(buffer.size(), totalBytes - offset);
        CopySnapshotEncodingRange(snapshot, *encoding, offset, buffer.data(), size_t(length));
        if (!output.Write(buffer.data(), size_t(length)))
            Fail("Cannot write Raft snapshot generation file");
        offset += length;
    }
    if (!output.SyncAndClose() || !ReplaceFile(tempPath, path) || !SyncDirectory(path))
        Fail("Cannot publish Raft snapshot generation file");
}

uint64_t TDiskState::WriteCompactedLogGeneration(const std::string& entriesPath,
        const std::string& indexPath, uint64_t firstIndex, uint64_t lastIndex, uint64_t& lastTerm,
        const std::vector<TMessageHolder<TLogEntry>>& appended) {
    const auto entriesTempPath = entriesPath + ".tmp";
    const auto indexTempPath = indexPath + ".tmp";
    TSafeTempFile entriesOutput;
    TSafeTempFile indexOutput;
    if (!entriesOutput.Open(entriesTempPath) || !indexOutput.Open(indexTempPath))
        Fail("Cannot create compacted Raft log generation");

    uint64_t count = 0;
    uint64_t offset = 0;
    auto writeEntry = [&](const auto& entry) {
        const auto checksumBytes = ChecksummedRecords ? sizeof(uint32_t) : 0;
        if (offset > uint64_t(INT64_MAX) - entry->Len - checksumBytes)
            Fail("Replacement Raft log exceeds supported file size");
        if (!entriesOutput.Write(reinterpret_cast<const char*>(entry.Mes), entry->Len))
            Fail("Cannot write replacement Raft log entry");
        if (ChecksummedRecords) {
            std::string checksum;
            NSnapshotCodec::Put(checksum, NSnapshotCodec::Checksum(std::string_view(
                reinterpret_cast<const char*>(entry.Mes), entry->Len)), sizeof(uint32_t));
            if (!entriesOutput.Write(checksum.data(), checksum.size()))
                Fail("Cannot write replacement Raft log checksum");
        }

        std::array<char, sizeof(offset)> offsetBytes{};
        memcpy(offsetBytes.data(), &offset, sizeof(offset));
        if (!indexOutput.Write(offsetBytes.data(), offsetBytes.size()))
            Fail("Cannot write replacement Raft log index");
        if (ChecksummedRecords) {
            std::string checksum;
            NSnapshotCodec::Put(checksum,
                NSnapshotCodec::Checksum(std::string_view(offsetBytes.data(), offsetBytes.size())), sizeof(uint32_t));
            if (!indexOutput.Write(checksum.data(), checksum.size()))
                Fail("Cannot write replacement Raft index checksum");
        }
        offset += entry->Len + checksumBytes;
        ++count;
        lastTerm = entry->Term;
    };
    for (uint64_t index = firstIndex; index <= lastIndex; ++index) {
        auto entry = Get(index);
        if (!entry) Fail("Missing retained entry while replacing Raft log suffix");
        writeEntry(entry);
    }
    for (const auto& entry : appended) writeEntry(entry);

    if (!entriesOutput.SyncAndClose() || !indexOutput.SyncAndClose() ||
        !ReplaceFile(entriesTempPath, entriesPath) || !SyncDirectory(entriesPath))
        Fail("Cannot publish compacted Raft entries generation");
    if (CommitHook) CommitHook(ECommitStage::EntriesPublished);
    if (!ReplaceFile(indexTempPath, indexPath) || !SyncDirectory(indexPath))
        Fail("Cannot publish compacted Raft index generation");
    if (CommitHook) CommitHook(ECommitStage::SnapshotIndexPublished);
    return count;
}

void TDiskState::ReclaimPreviousGeneration(uint64_t previousEntriesGeneration,
        uint64_t previousIndexGeneration) {
    bool changed = false;
    std::error_code ignored;
    if (previousEntriesGeneration) {
        changed |= std::filesystem::remove(EntryGenerationPath(previousEntriesGeneration), ignored);
        ignored.clear();
    } else {
        changed |= TruncateAndSyncRegularFileNoFollow(EntriesBasePath);
    }
    if (previousIndexGeneration) {
        changed |= std::filesystem::remove(GenerationPath(previousIndexGeneration), ignored);
        ignored.clear();
        changed |= std::filesystem::remove(SnapshotPath(previousIndexGeneration), ignored);
    }
    if (changed) (void)SyncDirectory(StatePath);
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
        CommitIndex, Voters, CommitIndexTrusted, ChecksummedRecords,
        EntriesGeneration, IndexGeneration, IndexBaseIndex, SnapshotIndex, SnapshotTerm);

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

namespace {
TConfigurationHistory TrustedDiskSnapshotHistory(const TSnapshot& snapshot,
        const std::vector<uint32_t>& voters) {
    if (!snapshot.HasConfigurationMetadata()) {
        if (snapshot.Voters != voters)
            throw std::invalid_argument("Legacy snapshot does not match the trusted voter baseline");
        return TConfigurationHistory::Stable(voters, 0, 0, snapshot.Index, snapshot.Term);
    }

    auto history = snapshot.IsJoint()
        ? TConfigurationHistory::FromJointSnapshot(snapshot)
        : TConfigurationHistory::FromStableSnapshot(snapshot);
    const auto& transitions = history.ConfigurationTransitions();
    if (transitions.empty() || transitions.front().PreviousConfigurationIndex != 0 ||
        transitions.front().PreviousConfigurationTerm != 0 || transitions.front().OldVoters != voters)
        throw std::invalid_argument("Snapshot membership history is not anchored to the trusted original voter set");
    return history;
}

TConfigurationHistory StoredDiskSnapshotHistory(const TSnapshot& snapshot,
        const std::vector<uint32_t>& voters) {
    if (!snapshot.HasConfigurationMetadata()) {
        if (snapshot.Voters != voters)
            throw std::invalid_argument("Legacy snapshot does not match the stored voter binding");
        return TConfigurationHistory::Stable(voters, 0, 0, snapshot.Index, snapshot.Term);
    }
    return snapshot.IsJoint()
        ? TConfigurationHistory::FromJointSnapshot(snapshot)
        : TConfigurationHistory::FromStableSnapshot(snapshot);
}

void AddDiskSnapshotVoters(const TConfigurationHistory& history, std::vector<uint32_t>& voters) {
    for (const auto& transition : history.ConfigurationTransitions()) {
        voters.insert(voters.end(), transition.OldVoters.begin(), transition.OldVoters.end());
        voters.insert(voters.end(), transition.NewVoters.begin(), transition.NewVoters.end());
    }
    voters.insert(voters.end(), history.OldVoters().begin(), history.OldVoters().end());
    voters.insert(voters.end(), history.NewVoters().begin(), history.NewVoters().end());
}
}

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

void TDiskState::ValidateTrustedLegacyVoterHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) const {
    ValidateFixedVoters(voters);
    if (RaftStarted)
        throw std::invalid_argument("Trusted legacy history migration must happen before Raft startup");
    if (!Voters.empty())
        throw std::invalid_argument("Trusted legacy history migration requires an unbound store");
    if (!RecoveredStore)
        throw std::invalid_argument("Trusted legacy history migration is only for recovered state");
    if (!CurrentTerm || LastLogIndex > uint64_t(INT64_MAX) || SnapshotIndex > LastLogIndex ||
        commitIndex < SnapshotIndex || commitIndex > LastLogIndex || commitIndex < CommitIndex)
        throw std::invalid_argument("Invalid trusted history migration boundaries");
    if (CommitIndexTrusted && CommitIndex != commitIndex)
        throw std::invalid_argument("Trusted commit boundary differs from persisted metadata");

    auto baseline = TConfigurationHistory::Stable(voters, 0, 0, SnapshotIndex, SnapshotTerm);
    auto historicalVoters = voters;
    if (Snapshot) {
        ValidateSnapshot(*Snapshot);
        if (Snapshot->Index != SnapshotIndex || Snapshot->Term != SnapshotTerm ||
            Snapshot->Term > CurrentTerm)
            throw std::invalid_argument("Legacy snapshot boundary mismatch");
        baseline = TrustedDiskSnapshotHistory(*Snapshot, voters);
        AddDiskSnapshotVoters(baseline, historicalVoters);
    } else if (SnapshotIndex || SnapshotTerm) {
        throw std::invalid_argument("Legacy compacted state is missing its snapshot");
    }

    auto previousTerm = SnapshotTerm;
    for (uint64_t index = SnapshotIndex + 1; index <= LastLogIndex; ++index) {
        const auto entry = Get(static_cast<int64_t>(index));
        if (!entry) throw std::invalid_argument("Missing retained legacy log entry");
        ValidateMessageTree(entry);
        if (entry->Flags != TLogEntry::EConfiguration && entry->Flags != TLogEntry::ENone &&
            entry->Flags != TLogEntry::EStub) {
            throw std::invalid_argument("Legacy log contains unsupported entry flags");
        }
        if (!entry->Term || entry->Term < previousTerm || entry->Term > CurrentTerm)
            throw std::invalid_argument("Invalid legacy log term sequence");
        previousTerm = entry->Term;
    }
    if (LastLogTerm != previousTerm)
        throw std::invalid_argument("Legacy log last term mismatch");

    const auto configuration = ScanConfigurationHistory(*this, baseline, commitIndex);
    for (const auto& transition : configuration.Committed.ConfigurationTransitions()) {
        historicalVoters.insert(historicalVoters.end(), transition.OldVoters.begin(), transition.OldVoters.end());
        historicalVoters.insert(historicalVoters.end(), transition.NewVoters.begin(), transition.NewVoters.end());
    }
    historicalVoters.insert(historicalVoters.end(), configuration.Committed.OldVoters().begin(),
        configuration.Committed.OldVoters().end());
    historicalVoters.insert(historicalVoters.end(), configuration.Committed.NewVoters().begin(),
        configuration.Committed.NewVoters().end());
    std::sort(historicalVoters.begin(), historicalVoters.end());
    historicalVoters.erase(std::unique(historicalVoters.begin(), historicalVoters.end()), historicalVoters.end());
    if (VotedFor && !std::binary_search(historicalVoters.begin(), historicalVoters.end(), VotedFor))
        throw std::invalid_argument("Stored vote is outside the validated membership history");
}

void TDiskState::ValidateStoredVoteMembershipHistory() const {
    if (!VotedFor || Voters.empty() || std::binary_search(Voters.begin(), Voters.end(), VotedFor)) return;

    // The original voter binding alone cannot validate a vote after membership
    // changes. Reconstruct committed membership before deciding whether it is
    // safe to recover this persisted vote.
    auto baseline = TConfigurationHistory::Stable(Voters, 0, 0, SnapshotIndex, SnapshotTerm);
    std::vector<uint32_t> historicalVoters = Voters;
    if (Snapshot) {
        ValidateSnapshot(*Snapshot);
        if (Snapshot->Index != SnapshotIndex || Snapshot->Term != SnapshotTerm ||
            Snapshot->Term > CurrentTerm)
            throw std::invalid_argument("Stored vote membership snapshot boundary mismatch");
        baseline = StoredDiskSnapshotHistory(*Snapshot, Voters);
        AddDiskSnapshotVoters(baseline, historicalVoters);
    } else if (SnapshotIndex || SnapshotTerm) {
        throw std::invalid_argument("Stored vote membership snapshot is missing");
    }

    const auto configuration = ScanConfigurationHistory(*this, baseline, CommitIndex);
    for (const auto& transition : configuration.Committed.ConfigurationTransitions()) {
        historicalVoters.insert(historicalVoters.end(), transition.OldVoters.begin(), transition.OldVoters.end());
        historicalVoters.insert(historicalVoters.end(), transition.NewVoters.begin(), transition.NewVoters.end());
    }
    historicalVoters.insert(historicalVoters.end(), configuration.Committed.OldVoters().begin(),
        configuration.Committed.OldVoters().end());
    historicalVoters.insert(historicalVoters.end(), configuration.Committed.NewVoters().begin(),
        configuration.Committed.NewVoters().end());
    std::sort(historicalVoters.begin(), historicalVoters.end());
    historicalVoters.erase(std::unique(historicalVoters.begin(), historicalVoters.end()), historicalVoters.end());
    if (VotedFor && !std::binary_search(historicalVoters.begin(), historicalVoters.end(), VotedFor))
        throw std::invalid_argument("Stored vote is outside the validated membership history");
}

bool TDiskState::CanBindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex, std::string* reason) const {
    if (reason) reason->clear();
    try {
        ValidateTrustedLegacyVoterHistory(voters, commitIndex);
        return true;
    } catch (const std::exception& error) {
        if (reason) *reason = error.what();
        return false;
    } catch (...) {
        if (reason) *reason = "Unknown trusted legacy history validation failure";
        return false;
    }
}

void TDiskState::BindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) {
    ValidateTrustedLegacyVoterHistory(voters, commitIndex);
    Voters = voters;
    CommitIndex = commitIndex;
    CommitIndexTrusted = true;
    Commit();
}
