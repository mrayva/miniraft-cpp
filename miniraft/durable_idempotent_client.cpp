#include "idempotent_client.h"

#include "snapshot_codec.h"

#include <array>
#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <utility>

#ifdef __linux__
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
constexpr std::array<uint8_t, 8> Magic{'M', 'R', 'C', 'I', 'D', 'C', 'S', '1'};
constexpr uint32_t FileVersion = 1;
constexpr size_t FixedBytes = 8 + 4 + 4 + 1 + 8 * 4 + 1 + 8 + 4;

void ValidateState(const TIdempotentClientSessionState& state) {
    if (state.Version != TIdempotentClientSessionState::CurrentVersion)
        throw std::invalid_argument("Unsupported idempotent client session state version");
    if (!state.Initialized || !state.Epoch || (!state.SessionIdHigh && !state.SessionIdLow) ||
        !state.NextSequence)
        throw std::invalid_argument("Invalid idempotent client session identity");
    if (!state.HasPending && !state.PendingPayload.empty())
        throw std::invalid_argument("Idempotent client state has an orphan pending payload");
    const auto maxPayload = TMessageLimits{}.MaxFrameBytes - sizeof(TIdempotentCommandRequest);
    if (state.PendingPayload.size() > maxPayload)
        throw std::invalid_argument("Pending idempotent client payload exceeds the frame limit");
}

std::string Encode(const TIdempotentClientSessionState& state) {
    ValidateState(state);
    std::string bytes(reinterpret_cast<const char*>(Magic.data()), Magic.size());
    NSnapshotCodec::Put(bytes, FileVersion, 4);
    NSnapshotCodec::Put(bytes, state.Version, 4);
    NSnapshotCodec::Put(bytes, state.Initialized ? 1 : 0, 1);
    NSnapshotCodec::Put(bytes, state.Epoch, 8);
    NSnapshotCodec::Put(bytes, state.SessionIdHigh, 8);
    NSnapshotCodec::Put(bytes, state.SessionIdLow, 8);
    NSnapshotCodec::Put(bytes, state.NextSequence, 8);
    NSnapshotCodec::Put(bytes, state.HasPending ? 1 : 0, 1);
    NSnapshotCodec::Put(bytes, state.PendingPayload.size(), 8);
    NSnapshotCodec::Append(bytes, state.PendingPayload);
    NSnapshotCodec::Put(bytes, NSnapshotCodec::Checksum(bytes), 4);
    return bytes;
}

TIdempotentClientSessionState Decode(std::string_view bytes) {
    if (bytes.size() < FixedBytes)
        throw std::runtime_error("Truncated durable client session state");
    if (bytes.size() > FixedBytes + TMessageLimits{}.MaxFrameBytes -
            sizeof(TIdempotentCommandRequest))
        throw std::runtime_error("Durable client session state exceeds the frame limit");
    if (bytes.substr(0, Magic.size()) != std::string_view(
            reinterpret_cast<const char*>(Magic.data()), Magic.size()))
        throw std::runtime_error("Unknown durable client session state format");

    const auto expectedChecksum = [&] {
        NSnapshotCodec::TReader checksumReader{bytes.substr(bytes.size() - 4)};
        return checksumReader.Take(4);
    }();
    if (expectedChecksum != NSnapshotCodec::Checksum(bytes.substr(0, bytes.size() - 4)))
        throw std::runtime_error("Durable client session state checksum mismatch");

    NSnapshotCodec::TReader reader{bytes.substr(Magic.size(), bytes.size() - Magic.size() - 4)};
    if (reader.Take(4) != FileVersion)
        throw std::runtime_error("Unsupported durable client session file version");
    TIdempotentClientSessionState state;
    state.Version = static_cast<uint32_t>(reader.Take(4));
    const auto initialized = reader.Take(1);
    if (initialized > 1) throw std::runtime_error("Invalid client session initialized flag");
    state.Initialized = initialized != 0;
    state.Epoch = reader.Take(8);
    state.SessionIdHigh = reader.Take(8);
    state.SessionIdLow = reader.Take(8);
    state.NextSequence = reader.Take(8);
    const auto hasPending = reader.Take(1);
    if (hasPending > 1) throw std::runtime_error("Invalid client session pending flag");
    state.HasPending = hasPending != 0;
    const auto payloadSize = reader.Take(8);
    const auto maxPayload = TMessageLimits{}.MaxFrameBytes - sizeof(TIdempotentCommandRequest);
    if (payloadSize > maxPayload || payloadSize > reader.Input.size())
        throw std::runtime_error("Invalid durable client session payload length");
    const auto payload = reader.Read(static_cast<size_t>(payloadSize));
    if (!reader.Input.empty())
        throw std::runtime_error("Trailing durable client session state data");
    state.PendingPayload.assign(payload);
    try {
        ValidateState(state);
    } catch (const std::exception& error) {
        throw std::runtime_error(std::string("Invalid durable client session state: ") + error.what());
    }
    return state;
}
}

struct TDurableIdempotentClientStateStore::TImpl {
#ifdef __linux__
    struct TFd {
        int Value = -1;
        TFd() = default;
        explicit TFd(int fd) : Value(fd) {}
        ~TFd() { if (Value >= 0) ::close(Value); }
        TFd(const TFd&) = delete;
        TFd& operator=(const TFd&) = delete;
    };

    TFd Directory;
    TFd Lock;
    std::string File;
    std::string Temp;
    THook Hook;

    void Stage(EStage stage) { if (Hook) Hook(stage); }
#endif
};

#ifdef __linux__
namespace {
void Require(bool ok, const char* operation) {
    if (!ok) throw std::system_error(errno, std::generic_category(), operation);
}

void Sync(int fd) {
    int result;
    do { result = fsync(fd); } while (result < 0 && errno == EINTR);
    Require(result == 0, "fsync durable client state");
}

void WriteAll(int fd, const char* bytes, size_t size) {
    size_t offset = 0;
    while (offset < size) {
        const auto written = write(fd, bytes + offset, size - offset);
        if (written < 0 && errno == EINTR) continue;
        Require(written > 0, "write durable client state");
        offset += static_cast<size_t>(written);
    }
}

std::string ReadAll(int fd, size_t size) {
    std::string bytes(size, '\0');
    size_t offset = 0;
    while (offset < size) {
        const auto count = read(fd, bytes.data() + offset, size - offset);
        if (count < 0 && errno == EINTR) continue;
        Require(count > 0, "read durable client state");
        offset += static_cast<size_t>(count);
    }
    return bytes;
}
}
#endif

TDurableIdempotentClientStateStore::TDurableIdempotentClientStateStore(
        const std::string& path, THook hook)
    : Impl(std::make_unique<TImpl>()) {
#ifndef __linux__
    (void)path;
    (void)hook;
    throw std::runtime_error("TDurableIdempotentClientStateStore currently requires Linux");
#else
    const auto base = std::filesystem::path(path);
    if (base.filename().empty() || base.filename() == "." || base.filename() == "..")
        throw std::invalid_argument("Invalid durable client state path");
    auto directory = base.parent_path();
    if (directory.empty()) directory = ".";
    Impl->File = base.filename().string();
    Impl->Temp = Impl->File + ".tmp";
    Impl->Hook = std::move(hook);
    Impl->Directory.Value = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    Require(Impl->Directory.Value >= 0, "open durable client state directory");

    const auto lockName = Impl->File + ".lock";
    Impl->Lock.Value = openat(Impl->Directory.Value, lockName.c_str(),
        O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    const bool newLock = Impl->Lock.Value >= 0;
    if (!newLock && errno == EEXIST)
        Impl->Lock.Value = openat(Impl->Directory.Value, lockName.c_str(),
            O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    Require(Impl->Lock.Value >= 0, "open durable client state lock");
    struct stat lockInfo;
    Require(fstat(Impl->Lock.Value, &lockInfo) == 0, "stat durable client state lock");
    if (!S_ISREG(lockInfo.st_mode) || lockInfo.st_uid != geteuid() || (lockInfo.st_mode & 0077))
        throw std::runtime_error("Unsafe durable client state lock type, owner or permissions");
    Require(flock(Impl->Lock.Value, LOCK_EX | LOCK_NB) == 0,
        "lock durable client state");
    (void)newLock;
#endif
}

TDurableIdempotentClientStateStore::~TDurableIdempotentClientStateStore() = default;

std::optional<TIdempotentClientSessionState>
TDurableIdempotentClientStateStore::Load() {
#ifndef __linux__
    throw std::runtime_error("TDurableIdempotentClientStateStore currently requires Linux");
#else
    TImpl::TFd file(openat(Impl->Directory.Value, Impl->File.c_str(),
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (file.Value < 0) {
        if (errno == ENOENT) return std::nullopt;
        Require(false, "open durable client state");
    }
    struct stat info;
    Require(fstat(file.Value, &info) == 0, "stat durable client state");
    const auto maxBytes = FixedBytes + TMessageLimits{}.MaxFrameBytes -
        sizeof(TIdempotentCommandRequest);
    if (!S_ISREG(info.st_mode) || info.st_uid != geteuid() || (info.st_mode & 0077) ||
        info.st_size < 0 ||
        uint64_t(info.st_size) < FixedBytes || uint64_t(info.st_size) > maxBytes)
        throw std::runtime_error("Invalid durable client state file type, owner, permissions or size");
    return Decode(ReadAll(file.Value, static_cast<size_t>(info.st_size)));
#endif
}

void TDurableIdempotentClientStateStore::Save(
        const TIdempotentClientSessionState& state) {
#ifndef __linux__
    (void)state;
    throw std::runtime_error("TDurableIdempotentClientStateStore currently requires Linux");
#else
    const auto bytes = Encode(state);
    if (unlinkat(Impl->Directory.Value, Impl->Temp.c_str(), 0) < 0 && errno != ENOENT)
        Require(false, "remove stale durable client state temp");
    TImpl::TFd temp(openat(Impl->Directory.Value, Impl->Temp.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    Require(temp.Value >= 0, "create durable client state temp");
    bool renamed = false;
    try {
        Impl->Stage(EStage::TempCreated);
        WriteAll(temp.Value, bytes.data(), bytes.size());
        Impl->Stage(EStage::Written);
        Sync(temp.Value);
        Impl->Stage(EStage::FileSynced);
        const int fd = temp.Value;
        temp.Value = -1;
        Require(close(fd) == 0, "close durable client state temp");
        Require(renameat(Impl->Directory.Value, Impl->Temp.c_str(),
            Impl->Directory.Value, Impl->File.c_str()) == 0,
            "publish durable client state");
        renamed = true;
        Impl->Stage(EStage::Renamed);
        Sync(Impl->Directory.Value);
        Impl->Stage(EStage::DirectorySynced);
    } catch (...) {
        if (!renamed) (void)unlinkat(Impl->Directory.Value, Impl->Temp.c_str(), 0);
        throw;
    }
#endif
}
