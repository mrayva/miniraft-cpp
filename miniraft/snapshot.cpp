#include "snapshot.h"
#include "snapshot_codec.h"
#include "raft.h"
#include <algorithm>
#include <climits>
#include <filesystem>
#include <system_error>
#ifdef __linux__
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
using namespace NSnapshotCodec;
constexpr std::string_view Magic = "MRFTS001";
void Validate(const TSnapshot& snapshot) {
    if (!snapshot.Index || snapshot.Index > uint64_t(INT64_MAX) || !snapshot.Term ||
        snapshot.Voters.empty() || snapshot.Voters.size() > TSnapshot::MaxVoters ||
        snapshot.Data.size() > TSnapshot::MaxBytes - 40 - 4 * snapshot.Voters.size())
        throw std::runtime_error("Invalid snapshot metadata/size");
    uint32_t previous = 0;
    for (auto voter : snapshot.Voters) {
        if (voter <= previous) throw std::runtime_error("Snapshot voters must be positive, sorted and unique");
        previous = voter;
    }
}
void CheckLocalLog(const TSnapshot& snapshot, const TRaft& raft) {
    Validate(snapshot);
    if (snapshot.Voters != raft.GetVoterIds() || snapshot.Index > raft.GetLastIndex() ||
        snapshot.Term > raft.GetState()->CurrentTerm ||
        raft.GetState()->LogTerm(snapshot.Index) != snapshot.Term)
        throw std::runtime_error("Snapshot does not match retained log and fixed configuration");
}
#ifdef __linux__
struct TFd {
    int Value = -1;
    explicit TFd(int fd = -1) : Value(fd) {}
    ~TFd() { if (Value >= 0) close(Value); }
    TFd(const TFd&) = delete;
    TFd& operator=(const TFd&) = delete;
};
void Require(bool ok, const char* message) {
    if (!ok) throw std::system_error(errno, std::generic_category(), message);
}
void Sync(int fd) {
    int rc;
    do { rc = fsync(fd); } while (rc < 0 && errno == EINTR);
    Require(rc == 0, "sync snapshot");
}
#endif
}

std::string EncodeSnapshot(const TSnapshot& snapshot) {
    Validate(snapshot);
    std::string out(Magic);
    Put(out, snapshot.Index, 8);
    Put(out, snapshot.Term, 8);
    Put(out, snapshot.Voters.size(), 4);
    Put(out, snapshot.Data.size(), 8);
    for (auto voter : snapshot.Voters) Put(out, voter, 4);
    Append(out, snapshot.Data);
    Put(out, Checksum(out), 4);
    return out;
}

TSnapshot DecodeSnapshot(std::string_view bytes) {
    if (bytes.size() < 44 || bytes.size() > TSnapshot::MaxBytes)
        throw std::runtime_error("Invalid snapshot size");
    TReader trailer{bytes.substr(bytes.size() - 4)};
    if (Checksum(bytes.substr(0, bytes.size() - 4)) != trailer.Take(4))
        throw std::runtime_error("Snapshot checksum mismatch");
    TReader reader{bytes.substr(0, bytes.size() - 4)};
    if (reader.Read(Magic.size()) != Magic) throw std::runtime_error("Unknown snapshot format");
    TSnapshot snapshot;
    snapshot.Index = reader.Take(8);
    snapshot.Term = reader.Take(8);
    const auto count = reader.Take(4);
    const auto length = reader.Take(8);
    if (count > TSnapshot::MaxVoters || count > reader.Input.size() / 4)
        throw std::runtime_error("Invalid snapshot voter count");
    for (uint32_t i = 0; i < count; ++i) snapshot.Voters.push_back(reader.Take(4));
    if (length > reader.Input.size()) throw std::runtime_error("Invalid snapshot data length");
    snapshot.Data = reader.Read(size_t(length));
    if (!reader.Input.empty()) throw std::runtime_error("Trailing snapshot data");
    Validate(snapshot);
    return snapshot;
}

TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm) {
    const auto index = rsm.LastAppliedIndex;
    if (!index || index > raft.GetVolatileState()->CommitIndex || index > raft.GetLastIndex())
        throw std::runtime_error("Snapshot requires applied committed progress");
    TSnapshot snapshot{index, raft.GetState()->LogTerm(index), raft.GetVoterIds(), {}};
    CheckLocalLog(snapshot, raft);
    snapshot.Data = rsm.SaveSnapshot();
    Validate(snapshot);
    return snapshot;
}

void RestoreSnapshot(const TSnapshot& snapshot, const TRaft& raft, IRsm& rsm) {
    CheckLocalLog(snapshot, raft);
    if (rsm.LastAppliedIndex != 0) throw std::runtime_error("Snapshot restore requires a fresh application");
    rsm.LoadSnapshot(snapshot.Data, snapshot.Index);
    if (rsm.LastAppliedIndex != snapshot.Index) throw std::runtime_error("Application failed to restore snapshot progress");
}

struct TSnapshotStore::TImpl {
    THook Hook;
    std::string File, Temp;
#ifdef __linux__
    TFd Directory, Lock;
#endif
    void Stage(EStage stage) { if (Hook) Hook(stage); }
};

TSnapshotStore::TSnapshotStore(const std::string& name, uint32_t id, THook hook)
    : Impl(std::make_unique<TImpl>())
{
#ifndef __linux__
    throw std::runtime_error("Snapshot store currently requires Linux");
#else
    if (!id) throw std::invalid_argument("Snapshot store requires a positive node ID");
    const auto base = std::filesystem::path(name);
    auto directory = base.parent_path();
    if (directory.empty()) directory = ".";
    Impl->File = base.filename().string() + ".snapshot." + std::to_string(id);
    Impl->Temp = Impl->File + ".tmp";
    Impl->Hook = std::move(hook);
    Impl->Directory.Value = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    Require(Impl->Directory.Value >= 0, "open snapshot directory");
    const auto lock = Impl->File + ".lock";
    Impl->Lock.Value = openat(Impl->Directory.Value, lock.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600);
    Require(Impl->Lock.Value >= 0, "open snapshot lock");
    struct stat lockInfo;
    Require(fstat(Impl->Lock.Value, &lockInfo) == 0, "stat snapshot lock");
    if (!S_ISREG(lockInfo.st_mode)) throw std::runtime_error("Invalid snapshot lock file type");
    Require(flock(Impl->Lock.Value, LOCK_EX | LOCK_NB) == 0, "lock snapshot store");
#endif
}

TSnapshotStore::~TSnapshotStore() = default;

std::optional<TSnapshot> TSnapshotStore::Load() {
#ifndef __linux__
    throw std::runtime_error("Snapshot store currently requires Linux");
#else
    TFd file(openat(Impl->Directory.Value, Impl->File.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (file.Value < 0 && errno == ENOENT) return {};
    Require(file.Value >= 0, "open snapshot");
    struct stat info;
    Require(fstat(file.Value, &info) == 0, "stat snapshot");
    if (!S_ISREG(info.st_mode) || info.st_size < 44 || uint64_t(info.st_size) > TSnapshot::MaxBytes)
        throw std::runtime_error("Invalid snapshot file type/size");
    std::string bytes(size_t(info.st_size), '\0');
    size_t done = 0;
    while (done < bytes.size()) {
        const auto n = read(file.Value, bytes.data() + done, bytes.size() - done);
        if (n < 0 && errno == EINTR) continue;
        if (!n) throw std::runtime_error("Truncated snapshot file");
        Require(n > 0, "read snapshot");
        done += size_t(n);
    }
    auto snapshot = DecodeSnapshot(bytes);
    Sync(file.Value);
    Sync(Impl->Directory.Value);
    return snapshot;
#endif
}

void TSnapshotStore::Save(const TSnapshot& snapshot) {
    const auto bytes = EncodeSnapshot(snapshot);
#ifndef __linux__
    throw std::runtime_error("Snapshot store currently requires Linux");
#else
    TFd file(openat(Impl->Directory.Value, Impl->Temp.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600));
    Require(file.Value >= 0, "create snapshot temporary image");
    struct stat info;
    Require(fstat(file.Value, &info) == 0, "stat snapshot temporary image");
    if (!S_ISREG(info.st_mode)) throw std::runtime_error("Invalid snapshot temporary image type");
    Impl->Stage(EStage::TempCreated);
    size_t done = 0;
    while (done < bytes.size()) {
        const auto n = write(file.Value, bytes.data() + done, bytes.size() - done);
        if (n < 0 && errno == EINTR) continue;
        Require(n > 0, "write snapshot");
        done += size_t(n);
    }
    Impl->Stage(EStage::Written);
    Sync(file.Value);
    Impl->Stage(EStage::FileSynced);
    Require(renameat(Impl->Directory.Value, Impl->Temp.c_str(), Impl->Directory.Value, Impl->File.c_str()) == 0,
        "publish snapshot");
    Impl->Stage(EStage::Renamed);
    Sync(Impl->Directory.Value);
    Impl->Stage(EStage::DirectorySynced);
#endif
}
