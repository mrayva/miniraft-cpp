#include "snapshot.h"
#include "snapshot_codec.h"
#include "raft.h"
#include <algorithm>
#include <climits>
#include <cstring>
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
constexpr std::string_view JointMagic = "MRFTS002";
constexpr std::string_view StableMagic = "MRFTS003";
constexpr std::string_view StableLineageMagic = "MRFTS004";
constexpr std::string_view JointLineageMagic = "MRFTS005";
constexpr std::string_view StableHistoryMagic = "MRFTS006";
constexpr std::string_view JointHistoryMagic = "MRFTS007";
constexpr std::string_view StableExtendedHistoryMagic = "MRFTS008";
constexpr std::string_view JointExtendedHistoryMagic = "MRFTS009";
void Validate(const TSnapshot& snapshot, size_t dataSize) {
    size_t historyBytes = snapshot.ConfigurationTransitions.empty() ? 0 : 4;
    for (const auto& transition : snapshot.ConfigurationTransitions) {
        if (transition.OldVoters.size() > TSnapshot::MaxVoters ||
            transition.NewVoters.size() > TSnapshot::MaxVoters ||
            historyBytes > TSnapshot::MaxBytes - 56 -
                4 * (transition.OldVoters.size() + transition.NewVoters.size()))
            throw std::runtime_error("Invalid snapshot configuration history size");
        historyBytes += 56 + 4 * (transition.OldVoters.size() + transition.NewVoters.size());
    }
    const size_t baseBytes = snapshot.HasStablePredecessor() ? 92 :
        snapshot.HasConfigurationLineage() ? 96 :
        snapshot.StableConfiguration ? 56 : snapshot.IsJoint() ? 60 : 40;
    const size_t voterBytes = 4 * (snapshot.Voters.size() + snapshot.JointVoters.size() +
        snapshot.PreviousVoters.size());
    if (!snapshot.Index || snapshot.Index > uint64_t(INT64_MAX) || !snapshot.Term ||
        snapshot.Voters.empty() || snapshot.Voters.size() > TSnapshot::MaxVoters ||
        snapshot.JointVoters.size() > TSnapshot::MaxVoters ||
        snapshot.PreviousVoters.size() > TSnapshot::MaxVoters ||
        snapshot.ConfigurationTransitions.size() > TSnapshot::MaxConfigurationTransitions ||
        baseBytes > TSnapshot::MaxBytes || voterBytes > TSnapshot::MaxBytes - baseBytes ||
        historyBytes > TSnapshot::MaxBytes - baseBytes - voterBytes ||
        dataSize > TSnapshot::MaxBytes - baseBytes - voterBytes - historyBytes)
        throw std::runtime_error("Invalid snapshot metadata/size");
    const auto validateVoters = [](const auto& voters) {
        uint32_t previous = 0;
        for (auto voter : voters) {
            if (voter <= previous) throw std::runtime_error("Snapshot voters must be positive, sorted and unique");
            previous = voter;
        }
    };
    validateVoters(snapshot.Voters);
    validateVoters(snapshot.PreviousVoters);
    bool hasIncompleteTransition = false;
    for (size_t i = 0; i < snapshot.ConfigurationTransitions.size(); ++i) {
        const auto& transition = snapshot.ConfigurationTransitions[i];
        validateVoters(transition.OldVoters);
        validateVoters(transition.NewVoters);
        if (transition.OldVoters.empty() || transition.NewVoters.empty() ||
            transition.OldVoters == transition.NewVoters ||
            ((transition.PreviousConfigurationIndex == 0) != (transition.PreviousConfigurationTerm == 0)) ||
            !transition.JointIndex || !transition.JointTerm ||
            transition.JointIndex <= transition.PreviousConfigurationIndex ||
            transition.JointTerm < transition.PreviousConfigurationTerm ||
            ((transition.StableIndex == 0) != (transition.StableTerm == 0)) ||
            (transition.StableIndex && (transition.StableIndex <= transition.JointIndex ||
                transition.StableTerm < transition.JointTerm)))
            throw std::runtime_error("Invalid snapshot configuration transition");
        if (i) {
            const auto& previous = snapshot.ConfigurationTransitions[i - 1];
            if (!previous.StableIndex || previous.NewVoters != transition.OldVoters ||
                previous.StableIndex != transition.PreviousConfigurationIndex ||
                previous.StableTerm != transition.PreviousConfigurationTerm ||
                transition.JointIndex <= previous.StableIndex || transition.JointTerm < previous.StableTerm)
                throw std::runtime_error("Discontinuous snapshot configuration history");
        }
        if (!transition.StableIndex) {
            if (i + 1 != snapshot.ConfigurationTransitions.size())
                throw std::runtime_error("Incomplete snapshot configuration transition is not the tail");
            hasIncompleteTransition = true;
        }
    }
    if (!snapshot.ConfigurationTransitions.empty()) {
        const auto& tail = snapshot.ConfigurationTransitions.back();
        const bool tailMatches = snapshot.StableConfiguration
            ? tail.StableIndex == snapshot.ConfigurationIndex && tail.StableTerm == snapshot.ConfigurationTerm &&
                tail.NewVoters == snapshot.Voters
            : snapshot.IsJoint() && !tail.StableIndex && tail.JointIndex == snapshot.ConfigurationIndex &&
                tail.JointTerm == snapshot.ConfigurationTerm && tail.OldVoters == snapshot.Voters &&
                tail.NewVoters == snapshot.JointVoters;
        if (!tailMatches || hasIncompleteTransition == snapshot.StableConfiguration)
            throw std::runtime_error("Snapshot configuration history does not match its active configuration");
    }
    if (snapshot.HasConfigurationLineage()) {
        const bool invalidStableLineage = snapshot.StableConfiguration &&
            (snapshot.PreviousVoters == snapshot.Voters ||
             snapshot.PreviousJointIndex <= snapshot.PreviousConfigurationIndex ||
             snapshot.ConfigurationIndex <= snapshot.PreviousJointIndex ||
             snapshot.PreviousJointTerm > snapshot.ConfigurationTerm);
        const bool invalidJointLineage = snapshot.IsJoint() &&
            (snapshot.PreviousVoters != snapshot.Voters ||
             snapshot.PreviousJointIndex != snapshot.ConfigurationIndex ||
             snapshot.PreviousJointTerm != snapshot.ConfigurationTerm);
        if (!snapshot.HasConfigurationMetadata() || invalidStableLineage || invalidJointLineage ||
            !snapshot.PreviousJointIndex || snapshot.PreviousJointIndex <= snapshot.PreviousConfigurationIndex ||
            ((snapshot.PreviousConfigurationIndex == 0) != (snapshot.PreviousConfigurationTerm == 0)) ||
            snapshot.PreviousJointTerm < snapshot.PreviousConfigurationTerm ||
            snapshot.PreviousJointIndex > snapshot.Index || !snapshot.PreviousJointTerm)
            throw std::runtime_error("Invalid snapshot configuration lineage");
        if (!snapshot.ConfigurationTransitions.empty()) {
            const auto& tail = snapshot.ConfigurationTransitions.back();
            if (tail.OldVoters != snapshot.PreviousVoters ||
                tail.PreviousConfigurationIndex != snapshot.PreviousConfigurationIndex ||
                tail.PreviousConfigurationTerm != snapshot.PreviousConfigurationTerm ||
                tail.JointIndex != snapshot.PreviousJointIndex ||
                tail.JointTerm != snapshot.PreviousJointTerm)
                throw std::runtime_error("Snapshot configuration history tail disagrees with lineage");
        }
    } else if (!snapshot.PreviousVoters.empty() || snapshot.PreviousConfigurationIndex ||
        snapshot.PreviousConfigurationTerm || snapshot.PreviousJointIndex || snapshot.PreviousJointTerm) {
        throw std::runtime_error("Incomplete snapshot configuration lineage");
    }
    if (snapshot.HasConfigurationMetadata()) {
        if ((snapshot.StableConfiguration ? !snapshot.JointVoters.empty() : snapshot.JointVoters.empty()) || !snapshot.ConfigurationIndex ||
            snapshot.ConfigurationIndex > snapshot.Index || !snapshot.ConfigurationTerm ||
            snapshot.ConfigurationTerm > snapshot.Term ||
            (snapshot.ConfigurationIndex == snapshot.Index && snapshot.ConfigurationTerm != snapshot.Term))
            throw std::runtime_error("Invalid joint snapshot configuration boundary");
        validateVoters(snapshot.JointVoters);
    }
}
void Validate(const TSnapshot& snapshot) { Validate(snapshot, snapshot.Data.size()); }
void CheckSnapshotConfigurationContinuity(const TSnapshot* current, const TSnapshot& incoming) {
    if (!current || !current->HasConfigurationMetadata()) return;
    if (!incoming.ConfigurationTransitions.empty() && !current->ConfigurationTransitions.empty()) {
        bool sameTail = true;
        const auto count = std::min(incoming.ConfigurationTransitions.size(), current->ConfigurationTransitions.size());
        for (size_t i = 0; i < count; ++i) {
            if (incoming.ConfigurationTransitions[incoming.ConfigurationTransitions.size() - count + i] !=
                current->ConfigurationTransitions[current->ConfigurationTransitions.size() - count + i]) {
                sameTail = false;
                break;
            }
        }
        if (!sameTail) {
            bool completesKnownJoint = incoming.ConfigurationTransitions.size() == current->ConfigurationTransitions.size();
            for (size_t i = 0; completesKnownJoint && i + 1 < count; ++i) {
                if (incoming.ConfigurationTransitions[incoming.ConfigurationTransitions.size() - count + i] !=
                    current->ConfigurationTransitions[current->ConfigurationTransitions.size() - count + i])
                    completesKnownJoint = false;
            }
            if (completesKnownJoint) {
                const auto& knownJoint = current->ConfigurationTransitions.back();
                const auto& stableSuccessor = incoming.ConfigurationTransitions.back();
                completesKnownJoint = !knownJoint.StableIndex && !knownJoint.StableTerm &&
                    stableSuccessor.StableIndex > knownJoint.JointIndex &&
                    stableSuccessor.StableTerm >= knownJoint.JointTerm &&
                    stableSuccessor.OldVoters == knownJoint.OldVoters &&
                    stableSuccessor.NewVoters == knownJoint.NewVoters &&
                    stableSuccessor.PreviousConfigurationIndex == knownJoint.PreviousConfigurationIndex &&
                    stableSuccessor.PreviousConfigurationTerm == knownJoint.PreviousConfigurationTerm &&
                    stableSuccessor.JointIndex == knownJoint.JointIndex &&
                    stableSuccessor.JointTerm == knownJoint.JointTerm;
            }
            if (!completesKnownJoint)
                throw std::invalid_argument("Snapshot configuration history changed");
        }
    }
    if (current->StableConfiguration) {
        if (incoming.StableConfiguration) {
            if (incoming.ConfigurationIndex != current->ConfigurationIndex ||
                incoming.ConfigurationTerm != current->ConfigurationTerm)
                throw std::invalid_argument("Stable snapshot configuration identity changed");
            if (incoming.Voters != current->Voters)
                throw std::invalid_argument("Stable snapshot voters changed without a new configuration identity");
            if (incoming.HasStablePredecessor() &&
                (!current->HasStablePredecessor() ||
                 incoming.PreviousVoters != current->PreviousVoters ||
                 incoming.PreviousConfigurationIndex != current->PreviousConfigurationIndex ||
                 incoming.PreviousConfigurationTerm != current->PreviousConfigurationTerm ||
                 incoming.PreviousJointIndex != current->PreviousJointIndex ||
                 incoming.PreviousJointTerm != current->PreviousJointTerm))
                throw std::invalid_argument("Stable snapshot predecessor lineage changed");
            return;
        }
        if (incoming.IsJoint()) {
            if (incoming.Voters != current->Voters ||
                incoming.ConfigurationIndex <= current->Index ||
                incoming.ConfigurationTerm < current->Term)
                throw std::invalid_argument("Joint snapshot does not extend the stable configuration");
            if (incoming.HasConfigurationLineage() &&
                (incoming.PreviousVoters != current->Voters ||
                 incoming.PreviousConfigurationIndex != current->ConfigurationIndex ||
                 incoming.PreviousConfigurationTerm != current->ConfigurationTerm))
                throw std::invalid_argument("Joint snapshot predecessor lineage is not continuous");
            return;
        }
        if (incoming.Voters != current->Voters)
            throw std::invalid_argument("Legacy snapshot would change the active voter set");
        return;
    }

    if (incoming.IsJoint()) {
        if (incoming.Voters != current->Voters || incoming.JointVoters != current->JointVoters ||
            incoming.ConfigurationIndex != current->ConfigurationIndex ||
            incoming.ConfigurationTerm != current->ConfigurationTerm)
            throw std::invalid_argument("Joint snapshot configuration changed without a stable successor");
        if (incoming.HasConfigurationLineage() &&
            (incoming.PreviousVoters != current->Voters ||
             incoming.PreviousJointIndex != current->ConfigurationIndex ||
             incoming.PreviousJointTerm != current->ConfigurationTerm ||
             (current->HasConfigurationLineage() &&
                (incoming.PreviousConfigurationIndex != current->PreviousConfigurationIndex ||
                 incoming.PreviousConfigurationTerm != current->PreviousConfigurationTerm))))
            throw std::invalid_argument("Joint snapshot predecessor lineage changed");
        return;
    }
    if (incoming.StableConfiguration) {
        if (incoming.Voters != current->JointVoters ||
            incoming.ConfigurationIndex <= current->Index ||
            incoming.ConfigurationTerm < current->Term)
            throw std::invalid_argument("Stable snapshot does not extend the joint configuration");
        if (incoming.HasStablePredecessor() &&
            (incoming.PreviousVoters != current->Voters ||
             incoming.PreviousJointIndex != current->ConfigurationIndex ||
             incoming.PreviousJointTerm != current->ConfigurationTerm ||
             (current->HasConfigurationLineage() &&
                (incoming.PreviousConfigurationIndex != current->PreviousConfigurationIndex ||
                 incoming.PreviousConfigurationTerm != current->PreviousConfigurationTerm))))
            throw std::invalid_argument("Stable snapshot predecessor lineage is not continuous");
        return;
    }
    throw std::invalid_argument("Legacy snapshot cannot discard joint configuration metadata");
}
bool MatchesConfiguration(const TSnapshot& snapshot, const TConfigurationHistory& committed) {
    if (!snapshot.ConfigurationTransitions.empty() && !committed.ConfigurationTransitions().empty()) {
        const auto count = std::min(snapshot.ConfigurationTransitions.size(),
            committed.ConfigurationTransitions().size());
        for (size_t i = 0; i < count; ++i) {
            if (snapshot.ConfigurationTransitions[snapshot.ConfigurationTransitions.size() - count + i] !=
                committed.ConfigurationTransitions()[committed.ConfigurationTransitions().size() - count + i])
                return false;
        }
    }
    if (committed.IsJoint()) {
        return snapshot.IsJoint() && !snapshot.StableConfiguration &&
            snapshot.Voters == committed.OldVoters() && snapshot.JointVoters == committed.NewVoters() &&
            snapshot.ConfigurationIndex == committed.ConfigurationIndex() &&
            snapshot.ConfigurationTerm == committed.ConfigurationTerm();
    }
    if (snapshot.IsJoint() || snapshot.Voters != committed.OldVoters()) return false;
    if (snapshot.StableConfiguration)
        return snapshot.ConfigurationIndex == committed.ConfigurationIndex() &&
            snapshot.ConfigurationTerm == committed.ConfigurationTerm();
    return snapshot.Index >= committed.ConfigurationIndex();
}

void CheckLocalLog(const TSnapshot& snapshot, const TRaft& raft,
        const TConfigurationHistory& configurationAtBoundary) {
    Validate(snapshot);
    if (!MatchesConfiguration(snapshot, configurationAtBoundary) ||
        snapshot.Index > raft.GetLastIndex() ||
        snapshot.Term > raft.GetState()->CurrentTerm ||
        raft.GetState()->LogTerm(snapshot.Index) != snapshot.Term)
        throw std::runtime_error("Snapshot does not match retained log and active configuration");
}

std::string EncodeSnapshotMetadata(const TSnapshot& snapshot) {
    const bool extendedHistory = snapshot.ConfigurationTransitions.size() >
        TSnapshot::MaxLegacyConfigurationTransitions;
    std::string out(!snapshot.ConfigurationTransitions.empty()
        ? (snapshot.StableConfiguration
            ? (extendedHistory ? StableExtendedHistoryMagic : StableHistoryMagic)
            : (extendedHistory ? JointExtendedHistoryMagic : JointHistoryMagic))
        : snapshot.HasStablePredecessor() ? StableLineageMagic :
        snapshot.HasConfigurationLineage() ? JointLineageMagic :
        snapshot.StableConfiguration ? StableMagic : snapshot.IsJoint() ? JointMagic : Magic);
    Put(out, snapshot.Index, 8);
    Put(out, snapshot.Term, 8);
    Put(out, snapshot.Voters.size(), 4);
    Put(out, snapshot.Data.size(), 8);
    if (snapshot.HasConfigurationMetadata()) {
        Put(out, snapshot.ConfigurationIndex, 8);
        Put(out, snapshot.ConfigurationTerm, 8);
        if (!snapshot.StableConfiguration) Put(out, snapshot.JointVoters.size(), 4);
        if (snapshot.HasConfigurationLineage()) {
            Put(out, snapshot.PreviousConfigurationIndex, 8);
            Put(out, snapshot.PreviousConfigurationTerm, 8);
            Put(out, snapshot.PreviousJointIndex, 8);
            Put(out, snapshot.PreviousJointTerm, 8);
            Put(out, snapshot.PreviousVoters.size(), 4);
        }
    }
    for (auto voter : snapshot.Voters) Put(out, voter, 4);
    for (auto voter : snapshot.JointVoters) Put(out, voter, 4);
    for (auto voter : snapshot.PreviousVoters) Put(out, voter, 4);
    if (!snapshot.ConfigurationTransitions.empty()) {
        Put(out, snapshot.ConfigurationTransitions.size(), 4);
        for (const auto& transition : snapshot.ConfigurationTransitions) {
            Put(out, transition.PreviousConfigurationIndex, 8);
            Put(out, transition.PreviousConfigurationTerm, 8);
            Put(out, transition.JointIndex, 8);
            Put(out, transition.JointTerm, 8);
            Put(out, transition.StableIndex, 8);
            Put(out, transition.StableTerm, 8);
            Put(out, transition.OldVoters.size(), 4);
            Put(out, transition.NewVoters.size(), 4);
            for (auto voter : transition.OldVoters) Put(out, voter, 4);
            for (auto voter : transition.NewVoters) Put(out, voter, 4);
        }
    }
    return out;
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

struct TSnapshotEncoding {
    std::string Metadata;
    uint32_t Checksum = 0;
    uint64_t TotalBytes = 0;
};

void ValidateSnapshot(const TSnapshot& snapshot) { Validate(snapshot); }

void PreserveStableSnapshotIdentity(const TSnapshot* current, TSnapshot& incoming) {
    if (current && current->StableConfiguration && !incoming.HasConfigurationMetadata()) {
        // Legacy images carry no membership claim, so retain the known stable
        // identity after confirming their active voters are unchanged.
        if (incoming.Voters != current->Voters)
            throw std::invalid_argument("Legacy snapshot would change the active voter set");
        incoming.StableConfiguration = true;
        incoming.ConfigurationIndex = current->ConfigurationIndex;
        incoming.ConfigurationTerm = current->ConfigurationTerm;
        incoming.PreviousVoters = current->PreviousVoters;
        incoming.PreviousConfigurationIndex = current->PreviousConfigurationIndex;
        incoming.PreviousConfigurationTerm = current->PreviousConfigurationTerm;
        incoming.PreviousJointIndex = current->PreviousJointIndex;
        incoming.PreviousJointTerm = current->PreviousJointTerm;
        incoming.ConfigurationTransitions = current->ConfigurationTransitions;
    } else if (current && current->StableConfiguration && incoming.StableConfiguration &&
        incoming.Voters == current->Voters &&
        incoming.ConfigurationIndex == current->ConfigurationIndex &&
        incoming.ConfigurationTerm == current->ConfigurationTerm &&
        !incoming.HasStablePredecessor() && current->HasStablePredecessor()) {
        // An older peer may send MRFTS003 after this node learned the bounded
        // predecessor from its own committed history. Keep the local lineage.
        incoming.PreviousVoters = current->PreviousVoters;
        incoming.PreviousConfigurationIndex = current->PreviousConfigurationIndex;
        incoming.PreviousConfigurationTerm = current->PreviousConfigurationTerm;
        incoming.PreviousJointIndex = current->PreviousJointIndex;
        incoming.PreviousJointTerm = current->PreviousJointTerm;
        incoming.ConfigurationTransitions = current->ConfigurationTransitions;
    } else if (current && current->StableConfiguration && incoming.StableConfiguration &&
        incoming.Voters == current->Voters &&
        incoming.ConfigurationIndex == current->ConfigurationIndex &&
        incoming.ConfigurationTerm == current->ConfigurationTerm &&
        incoming.ConfigurationTransitions.empty() && !current->ConfigurationTransitions.empty() &&
        incoming.HasStablePredecessor() &&
        incoming.PreviousVoters == current->PreviousVoters &&
        incoming.PreviousConfigurationIndex == current->PreviousConfigurationIndex &&
        incoming.PreviousConfigurationTerm == current->PreviousConfigurationTerm &&
        incoming.PreviousJointIndex == current->PreviousJointIndex &&
        incoming.PreviousJointTerm == current->PreviousJointTerm) {
        // Older peers can carry the bounded tail but not the complete history.
        // Retain the already validated chain when both snapshots share its tail.
        incoming.ConfigurationTransitions = current->ConfigurationTransitions;
    }
    CheckSnapshotConfigurationContinuity(current, incoming);
}

std::string EncodeSnapshot(const TSnapshot& snapshot) {
    const auto encoding = PrepareSnapshotEncoding(snapshot);
    std::string out(size_t(SnapshotEncodingSize(*encoding)), '\0');
    CopySnapshotEncodingRange(snapshot, *encoding, 0, out.data(), out.size());
    return out;
}

std::shared_ptr<const TSnapshotEncoding> PrepareSnapshotEncoding(const TSnapshot& snapshot) {
    Validate(snapshot);
    auto encoding = std::make_shared<TSnapshotEncoding>();
    encoding->Metadata = EncodeSnapshotMetadata(snapshot);
    encoding->TotalBytes = encoding->Metadata.size() + snapshot.Data.size() + 4;
    if (encoding->TotalBytes > TSnapshot::MaxBytes)
        throw std::runtime_error("Snapshot exceeds size limit");
    TChecksum checksum;
    checksum.Update(encoding->Metadata);
    checksum.Update(snapshot.Data);
    encoding->Checksum = checksum.Finish();
    return encoding;
}

uint64_t SnapshotEncodingSize(const TSnapshotEncoding& encoding) {
    return encoding.TotalBytes;
}

void CopySnapshotEncodingRange(const TSnapshot& snapshot, const TSnapshotEncoding& encoding,
        uint64_t offset, char* destination, size_t length) {
    if (offset > encoding.TotalBytes || length > encoding.TotalBytes - offset ||
        (length && !destination) || encoding.TotalBytes != encoding.Metadata.size() + snapshot.Data.size() + 4)
        throw std::invalid_argument("Invalid snapshot encoding range");
    char checksumBytes[4];
    for (unsigned i = 0; i < 4; ++i) checksumBytes[i] = char(encoding.Checksum >> (8 * i));
    const std::string_view checksum(checksumBytes, sizeof(checksumBytes));
    size_t written = 0;
    auto copyPart = [&](std::string_view part, uint64_t partOffset) {
        if (written == length || offset >= partOffset + part.size()) return;
        const auto begin = offset > partOffset ? size_t(offset - partOffset) : 0;
        const auto count = std::min(length - written, part.size() - begin);
        if (count) memcpy(destination + written, part.data() + begin, count);
        written += count;
        offset += count;
    };
    copyPart(encoding.Metadata, 0);
    copyPart(snapshot.Data, encoding.Metadata.size());
    copyPart(checksum, encoding.Metadata.size() + snapshot.Data.size());
    if (written != length) throw std::logic_error("Incomplete snapshot encoding range");
}

namespace {
TSnapshot DecodeSnapshotImpl(std::string_view bytes, std::string* reusableStorage) {
    if (bytes.size() < 44 || bytes.size() > TSnapshot::MaxBytes)
        throw std::runtime_error("Invalid snapshot size");
    if (reusableStorage && bytes.data() != reusableStorage->data())
        throw std::invalid_argument("Snapshot decode buffer does not match its input");
    TReader trailer{bytes.substr(bytes.size() - 4)};
    if (Checksum(bytes.substr(0, bytes.size() - 4)) != trailer.Take(4))
        throw std::runtime_error("Snapshot checksum mismatch");
    TReader reader{bytes.substr(0, bytes.size() - 4)};
    const auto magic = reader.Read(Magic.size());
    const bool jointHistory = magic == JointHistoryMagic || magic == JointExtendedHistoryMagic;
    const bool stableHistory = magic == StableHistoryMagic || magic == StableExtendedHistoryMagic;
    const bool extendedHistory = magic == StableExtendedHistoryMagic || magic == JointExtendedHistoryMagic;
    const bool jointLineage = magic == JointLineageMagic || jointHistory;
    const bool joint = magic == JointMagic || jointLineage;
    const bool stableLineage = magic == StableLineageMagic || stableHistory;
    const bool stable = magic == StableMagic || stableLineage;
    const bool configurationHistory = stableHistory || jointHistory;
    if (!joint && !stable && magic != Magic) throw std::runtime_error("Unknown snapshot format");
    TSnapshot snapshot;
    snapshot.StableConfiguration = stable;
    snapshot.Index = reader.Take(8);
    snapshot.Term = reader.Take(8);
    const auto count = reader.Take(4);
    const auto length = reader.Take(8);
    uint64_t newCount = 0, previousCount = 0;
    if (joint || stable) {
        snapshot.ConfigurationIndex = reader.Take(8);
        snapshot.ConfigurationTerm = reader.Take(8);
        if (joint) newCount = reader.Take(4);
        if (stableLineage || jointLineage) {
            snapshot.PreviousConfigurationIndex = reader.Take(8);
            snapshot.PreviousConfigurationTerm = reader.Take(8);
            snapshot.PreviousJointIndex = reader.Take(8);
            snapshot.PreviousJointTerm = reader.Take(8);
            previousCount = reader.Take(4);
        }
    }
    if (count > TSnapshot::MaxVoters || newCount > TSnapshot::MaxVoters ||
        previousCount > TSnapshot::MaxVoters ||
        count + newCount + previousCount > reader.Input.size() / 4)
        throw std::runtime_error("Invalid snapshot voter count");
    for (uint32_t i = 0; i < count; ++i) snapshot.Voters.push_back(reader.Take(4));
    for (uint32_t i = 0; i < newCount; ++i) snapshot.JointVoters.push_back(reader.Take(4));
    for (uint32_t i = 0; i < previousCount; ++i) snapshot.PreviousVoters.push_back(reader.Take(4));
    if (configurationHistory) {
        const auto transitions = reader.Take(4);
        const auto transitionLimit = extendedHistory ? TSnapshot::MaxConfigurationTransitions :
            TSnapshot::MaxLegacyConfigurationTransitions;
        if (!transitions || transitions > transitionLimit)
            throw std::runtime_error("Invalid snapshot configuration history count");
        for (uint32_t i = 0; i < transitions; ++i) {
            TSnapshotConfigurationTransition transition;
            transition.PreviousConfigurationIndex = reader.Take(8);
            transition.PreviousConfigurationTerm = reader.Take(8);
            transition.JointIndex = reader.Take(8);
            transition.JointTerm = reader.Take(8);
            transition.StableIndex = reader.Take(8);
            transition.StableTerm = reader.Take(8);
            const auto oldCount = reader.Take(4), newCount = reader.Take(4);
            if (oldCount > TSnapshot::MaxVoters || newCount > TSnapshot::MaxVoters ||
                oldCount + newCount > reader.Input.size() / 4)
                throw std::runtime_error("Invalid snapshot configuration history voter count");
            for (uint32_t j = 0; j < oldCount; ++j) transition.OldVoters.push_back(reader.Take(4));
            for (uint32_t j = 0; j < newCount; ++j) transition.NewVoters.push_back(reader.Take(4));
            snapshot.ConfigurationTransitions.push_back(std::move(transition));
        }
    }
    if (joint && !snapshot.IsJoint()) throw std::runtime_error("Empty joint snapshot configuration");
    if (length > reader.Input.size()) throw std::runtime_error("Invalid snapshot data length");
    const auto dataOffset = bytes.size() - 4 - reader.Input.size();
    const auto data = reader.Read(size_t(length));
    if (!reader.Input.empty()) throw std::runtime_error("Trailing snapshot data");
    Validate(snapshot, size_t(length));
    if (reusableStorage) {
        reusableStorage->erase(0, dataOffset);
        reusableStorage->resize(size_t(length));
        snapshot.Data = std::move(*reusableStorage);
    } else {
        snapshot.Data = data;
    }
    return snapshot;
}
}

TSnapshot DecodeSnapshot(std::string_view bytes) {
    return DecodeSnapshotImpl(bytes, nullptr);
}

TSnapshot DecodeSnapshotOwned(std::string&& bytes) {
    return DecodeSnapshotImpl(bytes, &bytes);
}

TSnapshot CaptureSnapshot(const TRaft& raft, const IRsm& rsm) {
    const auto index = rsm.LastAppliedIndex;
    if (!index || index > raft.GetVolatileState()->CommitIndex || index > raft.GetLastIndex())
        throw std::runtime_error("Snapshot requires applied committed progress");
    if (!raft.Configuration)
        throw std::logic_error("Snapshot capture requires validated configuration history");
    if (raft.Configuration->HasUncommittedConfiguration() &&
        index >= raft.Configuration->Latest.ConfigurationIndex())
        throw std::logic_error("Snapshot boundary cannot cover an uncommitted configuration");
    const auto& committed = raft.Configuration->Committed;
    if (committed.ConfigurationIndex() > index)
        throw std::logic_error("Application progress precedes the committed configuration");
    TSnapshot snapshot{index, raft.GetState()->LogTerm(index), committed.OldVoters(), {}};
    snapshot.ConfigurationTransitions = committed.ConfigurationTransitions();
    if (committed.IsJoint()) {
        snapshot.JointVoters = committed.NewVoters();
        snapshot.ConfigurationIndex = committed.ConfigurationIndex();
        snapshot.ConfigurationTerm = committed.ConfigurationTerm();
        snapshot.PreviousVoters = committed.PreviousVoters();
        snapshot.PreviousConfigurationIndex = committed.PreviousConfigurationIndex();
        snapshot.PreviousConfigurationTerm = committed.PreviousConfigurationTerm();
        snapshot.PreviousJointIndex = committed.PreviousJointIndex();
        snapshot.PreviousJointTerm = committed.PreviousJointTerm();
    } else if (committed.ConfigurationIndex()) {
        snapshot.StableConfiguration = true;
        snapshot.ConfigurationIndex = committed.ConfigurationIndex();
        snapshot.ConfigurationTerm = committed.ConfigurationTerm();
        snapshot.PreviousVoters = committed.PreviousVoters();
        snapshot.PreviousConfigurationIndex = committed.PreviousConfigurationIndex();
        snapshot.PreviousConfigurationTerm = committed.PreviousConfigurationTerm();
        snapshot.PreviousJointIndex = committed.PreviousJointIndex();
        snapshot.PreviousJointTerm = committed.PreviousJointTerm();
    }
    if (!snapshot.StableConfiguration) {
        const auto previous = raft.GetState()->GetSnapshot();
        if (previous && previous->StableConfiguration) {
            snapshot.StableConfiguration = true;
            snapshot.ConfigurationIndex = previous->ConfigurationIndex;
            snapshot.ConfigurationTerm = previous->ConfigurationTerm;
            snapshot.PreviousVoters = previous->PreviousVoters;
            snapshot.PreviousConfigurationIndex = previous->PreviousConfigurationIndex;
            snapshot.PreviousConfigurationTerm = previous->PreviousConfigurationTerm;
            snapshot.PreviousJointIndex = previous->PreviousJointIndex;
            snapshot.PreviousJointTerm = previous->PreviousJointTerm;
            snapshot.ConfigurationTransitions = previous->ConfigurationTransitions;
        }
    }
    CheckLocalLog(snapshot, raft, raft.Configuration->Committed);
    snapshot.Data = rsm.SaveSnapshot();
    Validate(snapshot);
    return snapshot;
}

void RestoreSnapshot(const TSnapshot& snapshot, const TRaft& raft, IRsm& rsm) {
    const auto knownSnapshot = raft.GetState()->GetSnapshot();
    ValidateSnapshot(snapshot);
    CheckSnapshotConfigurationContinuity(knownSnapshot.get(), snapshot);
    if (snapshot.Index < raft.GetState()->SnapshotIndex ||
        snapshot.Index > raft.GetVolatileState()->CommitIndex || !raft.ConfigurationBaseline)
        throw std::runtime_error("Snapshot is outside the committed retained configuration history");
    const auto configurationAtBoundary = ScanConfigurationHistoryPrefix(*raft.GetState(),
        *raft.ConfigurationBaseline, snapshot.Index);
    CheckLocalLog(snapshot, raft, configurationAtBoundary);
    if (rsm.LastAppliedIndex >= snapshot.Index)
        throw std::runtime_error("Application is not behind the snapshot");
    rsm.ValidateSnapshot(snapshot.Data, snapshot.Index);
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
    auto snapshot = DecodeSnapshotOwned(std::move(bytes));
    Sync(file.Value);
    Sync(Impl->Directory.Value);
    return snapshot;
#endif
}

void TSnapshotStore::Save(const TSnapshot& snapshot) {
#ifndef __linux__
    throw std::runtime_error("Snapshot store currently requires Linux");
#else
    const auto encoding = PrepareSnapshotEncoding(snapshot);
    const auto totalBytes = SnapshotEncodingSize(*encoding);
    TFd file(openat(Impl->Directory.Value, Impl->Temp.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600));
    Require(file.Value >= 0, "create snapshot temporary image");
    struct stat info;
    Require(fstat(file.Value, &info) == 0, "stat snapshot temporary image");
    if (!S_ISREG(info.st_mode)) throw std::runtime_error("Invalid snapshot temporary image type");
    Impl->Stage(EStage::TempCreated);
    std::string buffer(size_t(std::min<uint64_t>(64 * 1024, totalBytes)), '\0');
    uint64_t offset = 0;
    while (offset < totalBytes) {
        const auto count = size_t(std::min<uint64_t>(buffer.size(), totalBytes - offset));
        CopySnapshotEncodingRange(snapshot, *encoding, offset, buffer.data(), count);
        size_t written = 0;
        while (written < count) {
            const auto n = write(file.Value, buffer.data() + written, count - written);
            if (n < 0 && errno == EINTR) continue;
            Require(n > 0, "write snapshot");
            written += size_t(n);
        }
        offset += count;
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
