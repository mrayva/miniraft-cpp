#include "durable.h"
#include "persist.h"
#include "configuration.h"
#include "snapshot_codec.h"
#include <array>
#include <climits>
#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <span>
#ifdef __linux__
#include <fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <system_error>
#endif

namespace {
constexpr std::array<uint8_t, 8> Magic{'M','R','F','T','D','0','0','1'};
constexpr std::array<uint8_t, 8> SnapshotMagic{'M','R','F','T','D','0','0','2'};
constexpr std::array<uint8_t, 8> ConfigurationMagic{'M','R','F','T','D','0','0','3'};
constexpr std::array<uint8_t, 8> CommitMagic{'M','R','F','T','D','0','0','4'};
uint64_t Take(std::span<const uint8_t> bytes, size_t& position, unsigned width) {
    if (position > bytes.size() || width > bytes.size() - position)
        throw std::runtime_error("Truncated durable state");
    uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= uint64_t(bytes[position++]) << (8 * i);
    return value;
}
uint32_t Checksum(std::span<const uint8_t> bytes, size_t start,
        const std::array<uint8_t, 8>* version = nullptr) {
    NSnapshotCodec::TChecksum checksum;
    if (version) checksum.Update(version->data(), version->size());
    if (start < bytes.size()) checksum.Update(bytes.data() + start, bytes.size() - start);
    return checksum.Finish();
}
TMessageHolder<TLogEntry> Clone(TMessageHolder<TLogEntry> entry) {
    if (!entry.HasValidExtent() || entry->Len < sizeof(TLogEntry) || entry->Type != uint32_t(EMessageType::LOG_ENTRY))
        throw std::runtime_error("Invalid durable log entry");
    auto copy = NewHoldedMessage<TLogEntry>(entry->Len);
    memcpy(copy.Mes, entry.Mes, entry->Len);
    return copy;
}
#ifdef __linux__
struct TFd {
    int Value = -1;
    TFd() = default;
    explicit TFd(int fd) : Value(fd) {}
    ~TFd() { if (Value >= 0) close(Value); }
    TFd(const TFd&) = delete;
    TFd& operator=(const TFd&) = delete;
};
void Require(bool ok, const char* operation) {
    if (!ok) throw std::system_error(errno, std::generic_category(), operation);
}
void Sync(int fd) {
    int result;
    do { result = fsync(fd); } while (result < 0 && errno == EINTR);
    Require(result == 0, "fsync durable state");
}
void WriteAll(int fd, const void* data, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        auto n = write(fd, bytes + done, size - done);
        if (n < 0 && errno == EINTR) continue;
        Require(n > 0, "write durable state");
        done += size_t(n);
    }
}
void WriteAtAll(int fd, const void* data, size_t size, off_t offset) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        auto n = pwrite(fd, bytes + done, size - done, offset + off_t(done));
        if (n < 0 && errno == EINTR) continue;
        Require(n > 0, "patch durable header");
        done += size_t(n);
    }
}
class TReadOnlyMap {
public:
    explicit TReadOnlyMap(int fd) {
        struct stat info;
        Require(fstat(fd, &info) == 0, "stat durable state");
        if (!S_ISREG(info.st_mode) || info.st_size < 20 || uint64_t(info.st_size) > SIZE_MAX)
            throw std::runtime_error("Invalid durable state file size/type");
        Size = size_t(info.st_size);
        Address = mmap(nullptr, Size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (Address == MAP_FAILED) Require(false, "map durable state");
        if (!Address) {
            munmap(Address, Size);
            Address = MAP_FAILED;
            throw std::runtime_error("Invalid durable state mapping address");
        }
    }
    ~TReadOnlyMap() { if (Address != MAP_FAILED) munmap(Address, Size); }
    TReadOnlyMap(const TReadOnlyMap&) = delete;
    TReadOnlyMap& operator=(const TReadOnlyMap&) = delete;
    std::span<const uint8_t> Bytes() const {
        return {static_cast<const uint8_t*>(Address), Size};
    }
private:
    void* Address = MAP_FAILED;
    size_t Size = 0;
};
#endif
}

struct TDurableState::TImpl {
    std::vector<TMessageHolder<TLogEntry>> Log;
    std::shared_ptr<const TSnapshot> Snapshot;
    std::shared_ptr<const TSnapshot> EncodedSnapshotSource;
    std::shared_ptr<const TSnapshotEncoding> SnapshotEncoding;
    // Original trusted voter set; configuration snapshots can carry a later
    // active set without rewriting this binding.
    std::vector<uint32_t> Voters;
    bool Recovered = false;
    bool RaftStarted = false;
    THook Hook;
    std::string File, Temp;
#ifdef __linux__
    TFd Directory, Lock;
#endif
    void Stage(EStage stage) { if (Hook) Hook(stage); }
};

TDurableState::TDurableState(const std::string& name, uint32_t id, THook hook)
    : Impl(std::make_unique<TImpl>())
{
#ifndef __linux__
    throw std::runtime_error("TDurableState currently requires Linux");
#else
    const auto base = std::filesystem::path(name);
    auto directory = base.parent_path();
    if (directory.empty()) directory = ".";
    Impl->File = base.filename().string() + ".durable." + std::to_string(id);
    Impl->Temp = Impl->File + ".tmp";
    Impl->Hook = std::move(hook);
    Impl->Directory.Value = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    Require(Impl->Directory.Value >= 0, "open durable directory");
    // Never interpret a legacy store as a fresh node. Migration must be explicit.
    for (const auto* kind : {"entries", "index", "state"}) {
        if (std::filesystem::exists(name + "." + kind + "." + std::to_string(id)))
            throw std::runtime_error("Legacy storage exists; durable migration is not automatic");
    }
    const auto lockName = Impl->File + ".lock";
    Impl->Lock.Value = openat(Impl->Directory.Value, lockName.c_str(),
                            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    const bool newLock = Impl->Lock.Value >= 0;
    if (!newLock && errno == EEXIST)
        Impl->Lock.Value = openat(Impl->Directory.Value, lockName.c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    Require(Impl->Lock.Value >= 0, "open durable lock");
    Require(flock(Impl->Lock.Value, LOCK_EX | LOCK_NB) == 0, "lock durable state");
    TFd file(openat(Impl->Directory.Value, Impl->File.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (file.Value < 0) {
        Require(errno == ENOENT, "open durable state");
        if (!newLock) throw std::runtime_error("Published durable state is missing");
        Publish();
        return;
    }
    TReadOnlyMap mapped(file.Value);
    const auto bytes = mapped.Bytes();
    const bool commitAware = std::equal(CommitMagic.begin(), CommitMagic.end(), bytes.begin());
    const bool configured = std::equal(ConfigurationMagic.begin(), ConfigurationMagic.end(), bytes.begin());
    const bool compacted = std::equal(SnapshotMagic.begin(), SnapshotMagic.end(), bytes.begin());
    const auto* version = commitAware ? &CommitMagic : configured ? &ConfigurationMagic : compacted ? &SnapshotMagic : nullptr;
    if (!version && !std::equal(Magic.begin(), Magic.end(), bytes.begin()))
        throw std::runtime_error("Unknown durable state format");
    size_t position = Magic.size();
    const auto length = Take(bytes, position, 8);
    const auto crc = Take(bytes, position, 4);
    if (length != bytes.size() - position || crc != Checksum(bytes, position, version))
        throw std::runtime_error("Durable state length/checksum mismatch");
    CurrentTerm = Take(bytes, position, 8);
    VotedFor = Take(bytes, position, 4);
    const auto count = Take(bytes, position, 8);
    if (commitAware) CommitIndex = Take(bytes, position, 8);
    if (configured) {
        const auto voters = Take(bytes, position, 4);
        if (!voters || voters > TSnapshot::MaxVoters || voters > (bytes.size() - position) / 4)
            throw std::runtime_error("Invalid durable voter count");
        for (uint32_t i = 0; i < voters; ++i) Impl->Voters.push_back(Take(bytes, position, 4));
        try { ValidateFixedVoters(Impl->Voters); }
        catch (const std::invalid_argument&) { throw std::runtime_error("Invalid durable voter configuration"); }
    }
    if (commitAware) {
        const auto voters = Take(bytes, position, 4);
        if (voters > TSnapshot::MaxVoters || voters > (bytes.size() - position) / 4)
            throw std::runtime_error("Invalid durable voter count");
        for (uint32_t i = 0; i < voters; ++i) Impl->Voters.push_back(Take(bytes, position, 4));
        if (voters) {
            try { ValidateFixedVoters(Impl->Voters); }
            catch (const std::invalid_argument&) { throw std::runtime_error("Invalid durable voter configuration"); }
        }
        const auto length = Take(bytes, position, 8);
        if (length > TSnapshot::MaxBytes || length > bytes.size() - position)
            throw std::runtime_error("Invalid embedded snapshot size");
        if (length) {
            auto snapshot = DecodeSnapshot(std::string_view(reinterpret_cast<const char*>(bytes.data() + position), size_t(length)));
            if (!Impl->Voters.empty() && !SnapshotCompatibleWithVoterBinding(snapshot, Impl->Voters))
                throw std::runtime_error("Durable snapshot is incompatible with the persisted voter binding");
            if (snapshot.Term > CurrentTerm) throw std::runtime_error("Future durable snapshot term");
            position += size_t(length);
            SnapshotIndex = snapshot.Index;
            SnapshotTerm = snapshot.Term;
            LastLogTerm = SnapshotTerm;
            Impl->Snapshot = std::make_shared<const TSnapshot>(std::move(snapshot));
        }
    } else if (compacted || configured) {
        const auto length = Take(bytes, position, 8);
        if (length > TSnapshot::MaxBytes || length > bytes.size() - position)
            throw std::runtime_error("Invalid embedded snapshot size");
        if (!length && compacted) throw std::runtime_error("Missing embedded snapshot");
        if (length) {
            auto snapshot = DecodeSnapshot(std::string_view(reinterpret_cast<const char*>(bytes.data() + position), size_t(length)));
            if (snapshot.Term > CurrentTerm) throw std::runtime_error("Future durable snapshot term");
            if (configured && !SnapshotCompatibleWithVoterBinding(snapshot, Impl->Voters))
                throw std::runtime_error("Durable snapshot is incompatible with the persisted voter binding");
            position += size_t(length);
            SnapshotIndex = snapshot.Index;
            SnapshotTerm = snapshot.Term;
            LastLogTerm = SnapshotTerm;
            Impl->Snapshot = std::make_shared<const TSnapshot>(std::move(snapshot));
        }
    }
    if (!commitAware) CommitIndex = SnapshotIndex;
    if (!CurrentTerm || count > uint64_t(INT64_MAX) - SnapshotIndex || count > (bytes.size() - position) / 20)
        throw std::runtime_error("Invalid durable state metadata");
    for (uint64_t i = 0; i < count; ++i) {
        const auto term = Take(bytes, position, 8);
        const auto flags = Take(bytes, position, 8);
        const auto length = Take(bytes, position, 4);
        if (length > bytes.size() - position || length > UINT32_MAX - sizeof(TLogEntry) ||
            !term || term < LastLogTerm || term > CurrentTerm)
            throw std::runtime_error("Invalid durable log entry");
        auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + length);
        entry->Term = term;
        entry->Flags = flags;
        memcpy(entry->Data, bytes.data() + position, length);
        position += length;
        Impl->Log.push_back(entry);
        LastLogTerm = term;
    }
    if (position != bytes.size()) throw std::runtime_error("Trailing durable state data");
    LastLogIndex = SnapshotIndex + count;
    if (CommitIndex < SnapshotIndex || CommitIndex > LastLogIndex)
        throw std::runtime_error("Invalid durable commit index");
    // A crash after rename but before directory fsync may leave a visible new
    // image. Stabilize the selected image and its name before serving requests.
    Sync(file.Value);
    Sync(Impl->Directory.Value);
    Impl->Recovered = true;
    // A leftover temp is never a recovery candidate, even if its checksum is valid.
#endif
}

TDurableState::~TDurableState() = default;

void TDurableState::Publish()
{
#ifndef __linux__
    throw std::runtime_error("TDurableState currently requires Linux");
#else
    if (!CurrentTerm || LastLogIndex > uint64_t(INT64_MAX) || LastLogIndex != SnapshotIndex + Impl->Log.size() ||
        CommitIndex < SnapshotIndex || CommitIndex > LastLogIndex ||
        LastLogTerm != (Impl->Log.empty() ? SnapshotTerm : Impl->Log.back()->Term) ||
        (Impl->Snapshot ? (Impl->Snapshot->Index != SnapshotIndex || Impl->Snapshot->Term != SnapshotTerm ||
            SnapshotTerm > CurrentTerm) : (SnapshotIndex != 0 || SnapshotTerm != 0)))
        throw std::runtime_error("Invalid durable state fields");
    std::shared_ptr<const TSnapshotEncoding> snapshotEncoding;
    if (Impl->EncodedSnapshotSource != Impl->Snapshot) {
        auto encoding = Impl->Snapshot ? PrepareSnapshotEncoding(*Impl->Snapshot) :
            std::shared_ptr<const TSnapshotEncoding>{};
        Impl->SnapshotEncoding = std::move(encoding);
        Impl->EncodedSnapshotSource = Impl->Snapshot;
    }
    snapshotEncoding = Impl->SnapshotEncoding;
    const auto imageBytes = snapshotEncoding ? SnapshotEncodingSize(*snapshotEncoding) : 0;
    if (Impl->Snapshot && !SnapshotCompatibleWithVoterBinding(*Impl->Snapshot, Impl->Voters))
        throw std::runtime_error("Durable snapshot is incompatible with the persisted voter binding");
    if (!Impl->Voters.empty()) {
        ValidateFixedVoters(Impl->Voters);
    }
    uint64_t previousTerm = SnapshotTerm;
    for (const auto& entry : Impl->Log) {
        if (!entry->Term || entry->Term < previousTerm || entry->Term > CurrentTerm)
            throw std::runtime_error("Invalid durable log term");
        previousTerm = entry->Term;
    }
    TFd temp(openat(Impl->Directory.Value, Impl->Temp.c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600));
    Require(temp.Value >= 0, "create durable temporary image");
    Impl->Stage(EStage::TempCreated);
    constexpr size_t PayloadOffset = CommitMagic.size() + 8 + 4;
    std::array<uint8_t, PayloadOffset> header{};
    std::copy(CommitMagic.begin(), CommitMagic.end(), header.begin());
    WriteAll(temp.Value, header.data(), header.size());

    NSnapshotCodec::TChecksum checksum;
    auto updateChecksum = [&](const uint8_t* data, size_t size) {
        checksum.Update(data, size);
    };
    updateChecksum(CommitMagic.data(), CommitMagic.size());
    uint64_t payloadLength = 0;
    bool payloadStarted = false;
    auto writePayload = [&](const void* data, size_t size) {
        if (size > UINT64_MAX - payloadLength)
            throw std::length_error("Durable state image exceeds format size limit");
        WriteAll(temp.Value, data, size);
        updateChecksum(static_cast<const uint8_t*>(data), size);
        payloadLength += size;
        if (!payloadStarted && size) {
            payloadStarted = true;
            Impl->Stage(EStage::PayloadStarted);
        }
    };
    auto writeInteger = [&](uint64_t value, unsigned width) {
        uint8_t encoded[8];
        for (unsigned i = 0; i < width; ++i) encoded[i] = uint8_t(value >> (8 * i));
        writePayload(encoded, width);
    };

    writeInteger(CurrentTerm, 8);
    writeInteger(VotedFor, 4);
    writeInteger(Impl->Log.size(), 8);
    writeInteger(CommitIndex, 8);
    writeInteger(Impl->Voters.size(), 4);
    for (auto id : Impl->Voters) writeInteger(id, 4);
    writeInteger(imageBytes, 8);
    if (snapshotEncoding) {
        std::array<char, 64 * 1024> chunk{};
        for (uint64_t offset = 0; offset < imageBytes;) {
            const auto count = size_t(std::min<uint64_t>(chunk.size(), imageBytes - offset));
            CopySnapshotEncodingRange(*Impl->Snapshot, *snapshotEncoding, offset, chunk.data(), count);
            writePayload(chunk.data(), count);
            offset += count;
        }
    }
    for (const auto& entry : Impl->Log) {
        const auto dataBytes = entry->Len - sizeof(TLogEntry);
        writeInteger(entry->Term, 8);
        writeInteger(entry->Flags, 8);
        writeInteger(dataBytes, 4);
        writePayload(entry->Data, dataBytes);
    }

    std::array<uint8_t, 12> encodedHeader{};
    for (unsigned i = 0; i < 8; ++i) encodedHeader[i] = uint8_t(payloadLength >> (8 * i));
    const auto checksumValue = checksum.Finish();
    for (unsigned i = 0; i < 4; ++i) encodedHeader[8 + i] = uint8_t(checksumValue >> (8 * i));
    WriteAtAll(temp.Value, encodedHeader.data(), encodedHeader.size(), off_t(CommitMagic.size()));
    Impl->Stage(EStage::Written);
    Sync(temp.Value);
    Impl->Stage(EStage::FileSynced);
    Require(renameat(Impl->Directory.Value, Impl->Temp.c_str(),
                     Impl->Directory.Value, Impl->File.c_str()) == 0, "publish durable image");
    Impl->Stage(EStage::Renamed);
    Sync(Impl->Directory.Value);
    Impl->Stage(EStage::DirectorySynced);
#endif
}

void TDurableState::Commit()
{
    try { Publish(); }
    catch (const std::exception& error) {
        std::cerr << "Fatal durable storage error: " << error.what() << '\n';
        std::terminate();
    } catch (...) { std::terminate(); }
}

void TDurableState::Append(TMessageHolder<TLogEntry> entry)
{
    if (LastLogIndex >= uint64_t(INT64_MAX)) throw std::overflow_error("Raft log index exhausted");
    Impl->Log.push_back(Clone(entry));
    LastLogIndex = SnapshotIndex + Impl->Log.size();
    LastLogTerm = Impl->Log.back()->Term;
    Commit();
}

void TDurableState::AppendBatch(const std::vector<TMessageHolder<TLogEntry>>& entries)
{
    if (entries.empty()) return;
    if (LastLogIndex >= uint64_t(INT64_MAX) ||
        entries.size() > uint64_t(INT64_MAX) - LastLogIndex)
        throw std::overflow_error("Raft log index exhausted");
    if (entries.size() > Impl->Log.max_size() - Impl->Log.size())
        throw std::length_error("Durable log is too large for this platform");

    std::vector<TMessageHolder<TLogEntry>> cloned;
    cloned.reserve(entries.size());
    for (const auto& entry : entries) cloned.push_back(Clone(entry));
    Impl->Log.reserve(Impl->Log.size() + cloned.size());
    for (auto& entry : cloned) Impl->Log.push_back(std::move(entry));

    LastLogIndex = SnapshotIndex + Impl->Log.size();
    LastLogTerm = Impl->Log.back()->Term;
    Commit();
}

void TDurableState::RemoveLast()
{
    if (Impl->Log.empty()) return;
    if (LastLogIndex <= CommitIndex)
        throw std::logic_error("Cannot remove a committed log entry");
    Impl->Log.pop_back();
    LastLogIndex = SnapshotIndex + Impl->Log.size();
    LastLogTerm = Impl->Log.empty() ? SnapshotTerm : Impl->Log.back()->Term;
    Commit();
}

TMessageHolder<TLogEntry> TDurableState::Get(int64_t index) const
{
    if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex) return {};
    return Clone(Impl->Log[index - SnapshotIndex - 1]);
}

uint64_t TDurableState::RetainedLogTerm(int64_t index) const {
    if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex) return 0;
    return Impl->Log[index - SnapshotIndex - 1]->Term;
}
uint64_t TDurableState::RetainedLogFlags(int64_t index) const {
    if (index < 1 || uint64_t(index) <= SnapshotIndex || uint64_t(index) > LastLogIndex)
        throw std::out_of_range("Missing retained log entry");
    return Impl->Log[index - SnapshotIndex - 1]->Flags;
}

std::shared_ptr<const TSnapshot> TDurableState::GetSnapshot() const { return Impl->Snapshot; }

void TDurableState::InstallSnapshot(const TSnapshot& snapshot) {
    InstallSnapshotImpl(snapshot, false);
}

void TDurableState::InstallRaftSnapshot(const TSnapshot& snapshot,
        const TRaftInstallSnapshotPermit&) {
    InstallSnapshotImpl(snapshot, true);
}

void TDurableState::InstallRaftSnapshot(std::shared_ptr<const TSnapshot> snapshot,
        const TRaftInstallSnapshotPermit&) {
    InstallSnapshotImpl(std::move(snapshot));
}

void TDurableState::InstallSnapshotImpl(const TSnapshot& snapshot, bool configurationHistoryValidated) {
    ValidateSnapshot(snapshot);
    TSnapshot image = snapshot;
    if (!configurationHistoryValidated) PreserveStableSnapshotIdentity(Impl->Snapshot.get(), image);
    ValidateSnapshot(image);
    InstallValidatedSnapshot(std::make_shared<const TSnapshot>(std::move(image)));
}

void TDurableState::InstallSnapshotImpl(std::shared_ptr<const TSnapshot> saved) {
    if (!saved) throw std::invalid_argument("Missing Raft snapshot");
    ValidateSnapshot(*saved);
    InstallValidatedSnapshot(std::move(saved));
}

void TDurableState::InstallValidatedSnapshot(std::shared_ptr<const TSnapshot> saved) {
    if (!SnapshotCompatibleWithVoterBinding(*saved, Impl->Voters))
        throw std::invalid_argument("Snapshot is incompatible with the persisted voter binding");
    if (saved->Index <= SnapshotIndex || saved->Term < SnapshotTerm || saved->Term > CurrentTerm)
        throw std::invalid_argument("Obsolete or future durable snapshot");
    if (saved->Index <= CommitIndex &&
        (saved->Index > LastLogIndex || LogTerm(saved->Index) != saved->Term))
        throw std::invalid_argument("Snapshot would discard committed log entries");
    std::vector<TMessageHolder<TLogEntry>> suffix;
    if (saved->Index <= LastLogIndex && LogTerm(saved->Index) == saved->Term)
        suffix.assign(Impl->Log.begin() + (saved->Index - SnapshotIndex), Impl->Log.end());
    Impl->Log.swap(suffix);
    Impl->Snapshot = std::move(saved);
    SnapshotIndex = Impl->Snapshot->Index;
    SnapshotTerm = Impl->Snapshot->Term;
    LastLogIndex = SnapshotIndex + Impl->Log.size();
    LastLogTerm = Impl->Log.empty() ? SnapshotTerm : Impl->Log.back()->Term;
    CommitIndex = std::max(CommitIndex, SnapshotIndex);
    Commit(); // Atomically publishes hard state, snapshot and the selected suffix.
}

std::vector<uint32_t> TDurableState::GetBoundVoters() const { return Impl->Voters; }

bool TDurableState::HasUnboundRecoveredState() const {
    return Impl->Recovered && Impl->Voters.empty();
}

void TDurableState::MarkRaftStarted() { Impl->RaftStarted = true; }

void TDurableState::BindVoters(const std::vector<uint32_t>& voters) {
    ValidateVoterBinding(*this, Impl->Voters, voters);
    if (Impl->Voters == voters) return;
    Impl->Voters = voters;
    Commit(); // Bind before acknowledging any vote/replication traffic.
}

void TDurableState::ValidateTrustedLegacyVoterBaseline(const std::vector<uint32_t>& voters) const {
    ValidateFixedVoters(voters);
    if (Impl->RaftStarted)
        throw std::invalid_argument("Trusted legacy voter migration must happen before Raft startup");
    if (!Impl->Voters.empty()) {
        if (Impl->Voters == voters) return;
        throw std::invalid_argument("Durable voter binding already exists");
    }
    if (!CurrentTerm || LastLogIndex > uint64_t(INT64_MAX) || SnapshotIndex > LastLogIndex ||
        LastLogIndex - SnapshotIndex != Impl->Log.size())
        throw std::invalid_argument("Invalid legacy state bounds");
    if (!Impl->Recovered)
        throw std::invalid_argument("Trusted legacy baseline is only for recovered state");
    if (VotedFor && !std::binary_search(voters.begin(), voters.end(), VotedFor))
        throw std::invalid_argument("Stored vote is outside the trusted voter baseline");

    if (Impl->Snapshot) {
        const auto& snapshot = *Impl->Snapshot;
        ValidateSnapshot(snapshot);
        if (snapshot.HasConfigurationMetadata() || snapshot.Index != SnapshotIndex ||
            snapshot.Term != SnapshotTerm || snapshot.Term > CurrentTerm || snapshot.Voters != voters)
            throw std::invalid_argument("Legacy snapshot does not match the trusted voter baseline");
    } else if (SnapshotIndex || SnapshotTerm) {
        throw std::invalid_argument("Legacy compacted state is missing its snapshot");
    }

    auto previousTerm = SnapshotTerm;
    for (uint64_t index = SnapshotIndex + 1; index <= LastLogIndex; ++index) {
        if (RetainedLogFlags(index) & TLogEntry::EConfiguration)
            throw std::invalid_argument("Configuration log migration requires an explicit history baseline");
        const auto term = RetainedLogTerm(index);
        if (!term || term < previousTerm || term > CurrentTerm)
            throw std::invalid_argument("Invalid legacy log term sequence");
        previousTerm = term;
    }
    if (LastLogTerm != previousTerm)
        throw std::invalid_argument("Legacy log last term mismatch");
}

bool TDurableState::CanBindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters,
        std::string* reason) const {
    if (reason) reason->clear();
    try {
        ValidateTrustedLegacyVoterBaseline(voters);
        return true;
    } catch (const std::exception& error) {
        if (reason) *reason = error.what();
        return false;
    } catch (...) {
        if (reason) *reason = "Unknown trusted legacy voter baseline validation failure";
        return false;
    }
}

void TDurableState::BindVotersFromTrustedBaseline(const std::vector<uint32_t>& voters) {
    ValidateTrustedLegacyVoterBaseline(voters);
    if (Impl->Voters == voters) return;

    // Persist the operator-supplied baseline before Raft can serve votes or
    // install configuration snapshots against it.
    Impl->Voters = voters;
    Commit();
}

namespace {
TConfigurationHistory TrustedSnapshotHistory(const TSnapshot& snapshot,
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

void AddSnapshotVoters(const TConfigurationHistory& history, std::vector<uint32_t>& voters) {
    for (const auto& transition : history.ConfigurationTransitions()) {
        voters.insert(voters.end(), transition.OldVoters.begin(), transition.OldVoters.end());
        voters.insert(voters.end(), transition.NewVoters.begin(), transition.NewVoters.end());
    }
    voters.insert(voters.end(), history.OldVoters().begin(), history.OldVoters().end());
    voters.insert(voters.end(), history.NewVoters().begin(), history.NewVoters().end());
}
}

void TDurableState::ValidateTrustedLegacyVoterHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) const {
    ValidateFixedVoters(voters);
    if (Impl->RaftStarted)
        throw std::invalid_argument("Trusted legacy history migration must happen before Raft startup");
    if (!Impl->Voters.empty())
        throw std::invalid_argument("Trusted legacy history migration requires an unbound store");
    if (!Impl->Recovered)
        throw std::invalid_argument("Trusted legacy history migration is only for recovered state");
    if (!CurrentTerm || LastLogIndex > uint64_t(INT64_MAX) || SnapshotIndex > LastLogIndex ||
        LastLogIndex - SnapshotIndex != Impl->Log.size() || commitIndex < SnapshotIndex ||
        commitIndex > LastLogIndex || commitIndex < CommitIndex)
        throw std::invalid_argument("Invalid trusted history migration boundaries");

    TConfigurationHistory baseline = TConfigurationHistory::Stable(
        voters, 0, 0, SnapshotIndex, SnapshotTerm);
    std::vector<uint32_t> historicalVoters = voters;
    if (Impl->Snapshot) {
        const auto& snapshot = *Impl->Snapshot;
        ValidateSnapshot(snapshot);
        if (snapshot.Index != SnapshotIndex || snapshot.Term != SnapshotTerm || snapshot.Term > CurrentTerm)
            throw std::invalid_argument("Legacy snapshot boundary mismatch");
        baseline = TrustedSnapshotHistory(snapshot, voters);
        AddSnapshotVoters(baseline, historicalVoters);
    } else if (SnapshotIndex || SnapshotTerm) {
        throw std::invalid_argument("Legacy compacted state is missing its snapshot");
    }

    auto previousTerm = SnapshotTerm;
    for (uint64_t index = SnapshotIndex + 1; index <= LastLogIndex; ++index) {
        const auto entry = Get(static_cast<int64_t>(index));
        if (!entry) throw std::invalid_argument("Missing retained legacy log entry");
        ValidateMessageTree(entry);
        if (entry->Flags == TLogEntry::EConfiguration) {
            (void)DecodeConfigurationEntry(entry);
        } else if (entry->Flags != TLogEntry::ENone && entry->Flags != TLogEntry::EStub) {
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

bool TDurableState::CanBindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
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

void TDurableState::BindVotersFromTrustedHistory(const std::vector<uint32_t>& voters,
        uint64_t commitIndex) {
    ValidateTrustedLegacyVoterHistory(voters, commitIndex);
    Impl->Voters = voters;
    CommitIndex = commitIndex;
    Commit();
}

namespace {
void RequireExistingLegacyDiskFiles(const std::string& base, uint32_t nodeId) {
    const auto suffix = "." + std::to_string(nodeId);
    const std::array<std::string, 3> paths{
        base + ".entries" + suffix, base + ".index" + suffix, base + ".state" + suffix};
    const auto existing = std::count_if(paths.begin(), paths.end(),
        [](const auto& path) { return std::filesystem::exists(path); });
    if (existing != 3)
        throw std::runtime_error(existing ? "Incomplete legacy TDiskState files" : "Legacy TDiskState store does not exist");
}

TLegacyDiskStateMigrationInfo InspectLegacyDiskState(const IState& source, uint32_t nodeId,
        const std::vector<uint32_t>& voters, uint64_t commitIndex) {
    ValidateFixedVoters(voters);
    if (!nodeId || !std::binary_search(voters.begin(), voters.end(), nodeId))
        throw std::invalid_argument("The migrated node must belong to the trusted voter set");
    if (!source.CurrentTerm || source.LastLogIndex > uint64_t(INT64_MAX) ||
        source.SnapshotIndex || source.SnapshotTerm || source.GetSnapshot())
        throw std::invalid_argument("Only un-compacted legacy disk state can be migrated");
    if (source.HasTrustedCommitIndex() && source.CommitIndex != commitIndex)
        throw std::invalid_argument("Supplied commit boundary differs from versioned disk metadata");
    const auto boundVoters = source.GetBoundVoters();
    if (!boundVoters.empty() && boundVoters != voters)
        throw std::invalid_argument("Supplied voter baseline differs from versioned disk metadata");
    if (commitIndex > source.LastLogIndex)
        throw std::invalid_argument("Supplied commit index exceeds the legacy log");
    std::vector<uint32_t> historicalVoters = voters;
    uint64_t previousTerm = 0;
    for (uint64_t index = 1; index <= source.LastLogIndex; ++index) {
        const auto entry = source.Get(static_cast<int64_t>(index));
        if (!entry) throw std::invalid_argument("Missing legacy log entry");
        ValidateMessageTree(entry);
        if (entry->Flags == TLogEntry::EConfiguration) {
            (void)DecodeConfigurationEntry(entry);
        } else if (entry->Flags != TLogEntry::ENone && entry->Flags != TLogEntry::EStub) {
            throw std::invalid_argument("Legacy log contains unsupported entry flags");
        }
        if (!entry->Term || entry->Term < previousTerm || entry->Term > source.CurrentTerm)
            throw std::invalid_argument("Invalid legacy log term sequence");
        previousTerm = entry->Term;
    }
    if (source.LastLogTerm != previousTerm)
        throw std::invalid_argument("Legacy last-log term does not match its entries");

    // A vote can name a voter introduced or removed by a retained membership
    // transition, but only committed membership history is trusted here. An
    // uncommitted suffix must not legitimize a persisted vote during migration.
    const auto baseline = TConfigurationHistory::Stable(voters, 0, 0, 0, 0);
    const auto configuration = ScanConfigurationHistory(source, baseline, commitIndex);
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
    if (source.VotedFor && !std::binary_search(historicalVoters.begin(), historicalVoters.end(), source.VotedFor))
        throw std::invalid_argument("Stored vote is outside the validated membership history");

    return {source.CurrentTerm, source.VotedFor, source.LastLogIndex, source.LastLogTerm, commitIndex};
}

std::filesystem::path NormalizedBase(const std::string& base) {
    if (base.empty()) throw std::invalid_argument("Store base path must not be empty");
    auto path = std::filesystem::absolute(base).lexically_normal();
    auto parent = path.parent_path();
    if (parent.empty()) parent = ".";
    return std::filesystem::weakly_canonical(parent) / path.filename();
}
}

TLegacyDiskStateMigrationInfo CheckLegacyDiskStateMigration(const std::string& legacyBase,
        uint32_t nodeId, const std::vector<uint32_t>& trustedVoters, uint64_t commitIndex) {
    (void)NormalizedBase(legacyBase);
    RequireExistingLegacyDiskFiles(legacyBase, nodeId);
    TDiskState source(legacyBase, nodeId);
    return InspectLegacyDiskState(source, nodeId, trustedVoters, commitIndex);
}

TLegacyDiskStateMigrationInfo MigrateLegacyDiskState(const std::string& legacyBase,
        const std::string& durableBase, uint32_t nodeId,
        const std::vector<uint32_t>& trustedVoters, uint64_t commitIndex) {
    if (NormalizedBase(legacyBase) == NormalizedBase(durableBase))
        throw std::invalid_argument("Legacy and durable stores must use different base paths");

    RequireExistingLegacyDiskFiles(legacyBase, nodeId);
    TDiskState source(legacyBase, nodeId);
    const auto info = InspectLegacyDiskState(source, nodeId, trustedVoters, commitIndex);
    std::vector<TMessageHolder<TLogEntry>> entries;
    if (info.LastLogIndex > entries.max_size())
        throw std::length_error("Legacy log is too large for this platform");
    entries.reserve(size_t(info.LastLogIndex));
    for (uint64_t index = 1; index <= info.LastLogIndex; ++index)
        entries.push_back(Clone(source.Get(static_cast<int64_t>(index))));

    {
        TDurableState destination(durableBase, nodeId);
        if (destination.Impl->Recovered || !destination.Impl->Log.empty() ||
            destination.Impl->Snapshot || !destination.Impl->Voters.empty() ||
            destination.LastLogIndex || destination.SnapshotIndex || destination.CommitIndex ||
            destination.CurrentTerm != 1 || destination.VotedFor)
            throw std::invalid_argument("Migration destination is not a fresh durable store");
        destination.Impl->Voters = trustedVoters;
        destination.Impl->Log = std::move(entries);
        destination.CurrentTerm = info.CurrentTerm;
        destination.VotedFor = info.VotedFor;
        destination.LastLogIndex = info.LastLogIndex;
        destination.LastLogTerm = info.LastLogTerm;
        destination.CommitIndex = info.CommitIndex;
        destination.Commit();
    }
    TDurableState verified(durableBase, nodeId);
    if (verified.CurrentTerm != info.CurrentTerm || verified.VotedFor != info.VotedFor ||
        verified.LastLogIndex != info.LastLogIndex || verified.LastLogTerm != info.LastLogTerm ||
        verified.CommitIndex != info.CommitIndex || verified.GetBoundVoters() != trustedVoters)
        throw std::runtime_error("Migrated durable metadata failed reopen verification");
    for (uint64_t index = 1; index <= info.LastLogIndex; ++index) {
        const auto oldEntry = source.Get(static_cast<int64_t>(index));
        const auto newEntry = verified.Get(static_cast<int64_t>(index));
        if (!newEntry || oldEntry->Len != newEntry->Len ||
            memcmp(oldEntry.Mes, newEntry.Mes, oldEntry->Len) != 0)
            throw std::runtime_error("Migrated durable log failed reopen verification");
    }
    return info;
}
