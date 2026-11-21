#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "messages.h"
#include "snapshot.h"

struct TIdempotentOperationIdentity {
    uint64_t Epoch = 0;
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    uint64_t Sequence = 0;
    std::array<uint8_t, 32> PayloadDigest{};
    bool operator==(const TIdempotentOperationIdentity&) const = default;
};

enum class EIdempotencyControlKind : uint32_t {
    CloseSession = 1,
    AdvanceEpoch = 2,
};

// CloseSession uses Epoch plus a nonzero session ID. AdvanceEpoch uses Epoch
// as the expected current epoch and NewEpoch as the strictly greater successor;
// session IDs must be zero for that operation.
struct TIdempotencyControlOperation {
    EIdempotencyControlKind Kind = EIdempotencyControlKind::CloseSession;
    uint64_t Epoch = 0;
    uint64_t NewEpoch = 0;
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    bool operator==(const TIdempotencyControlOperation&) const = default;
};

struct TIdempotencySessionRecord {
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    uint64_t HighestSequence = 0;
    std::array<uint8_t, 32> PayloadDigest{};
    uint64_t LogIndex = 0;
    uint32_t ErrorCode = 0;
    bool Closed = false;
    std::string ResponseData;
    bool operator==(const TIdempotencySessionRecord&) const = default;
};

// This whole value is captured and restored by the state machine as one unit.
// ApplicationData, session records, epoch and progress must share one atomic
// state-machine snapshot boundary.
struct TIdempotencySnapshot {
    static constexpr uint32_t CurrentVersion = 1;
    uint64_t LastAppliedIndex = 0;
    uint64_t Epoch = 0;
    std::string ApplicationData;
    std::vector<TIdempotencySessionRecord> Sessions;
    bool operator==(const TIdempotencySnapshot&) const = default;
};

struct TIdempotencySnapshotLimits {
    uint32_t MaxActiveSessions = 4096;
    uint32_t MaxClosedSessions = 4096;
    uint32_t MaxResponseBytes = 1024 * 1024;
    uint64_t MaxCachedResponseBytes = 16 * 1024 * 1024;
    uint64_t MaxApplicationBytes = 32 * 1024 * 1024;
    uint64_t MaxEncodedBytes = TSnapshot::MaxBytes;
};

void ValidateIdempotencySnapshotLimits(const TIdempotencySnapshotLimits& limits);
void ValidateIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
                                 const TIdempotencySnapshotLimits& limits = {});
std::string EncodeIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
                                      const TIdempotencySnapshotLimits& limits = {});
TIdempotencySnapshot DecodeIdempotencySnapshot(std::string_view bytes,
                                               const TIdempotencySnapshotLimits& limits = {});
