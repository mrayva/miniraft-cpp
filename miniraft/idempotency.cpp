#include "idempotency.h"
#include "snapshot_codec.h"

#include <cstring>
#include <limits>
#include <stdexcept>

namespace {
using namespace NSnapshotCodec;
constexpr std::string_view Magic = "MRIDEM01";
constexpr uint32_t EncodedVersion = TIdempotencySnapshot::CurrentVersion;
constexpr uint64_t FixedHeaderBytes = 40;
constexpr uint64_t FixedRecordBytes = 80;
constexpr uint64_t ChecksumBytes = 4;

[[noreturn]] void Invalid(const char* message) {
    throw std::runtime_error(message);
}

bool SessionIdLess(const TIdempotencySessionRecord& lhs,
                   const TIdempotencySessionRecord& rhs) {
    if (lhs.SessionIdHigh != rhs.SessionIdHigh)
        return lhs.SessionIdHigh < rhs.SessionIdHigh;
    return lhs.SessionIdLow < rhs.SessionIdLow;
}

uint64_t EncodedSize(const TIdempotencySnapshot& snapshot) {
    uint64_t result = FixedHeaderBytes + ChecksumBytes;
    if (snapshot.ApplicationData.size() > UINT64_MAX - result)
        Invalid("Idempotency snapshot size overflow");
    result += snapshot.ApplicationData.size();
    if (snapshot.Sessions.size() > (UINT64_MAX - result) / FixedRecordBytes)
        Invalid("Idempotency snapshot size overflow");
    result += uint64_t(snapshot.Sessions.size()) * FixedRecordBytes;
    for (const auto& session : snapshot.Sessions) {
        if (session.ResponseData.size() > UINT64_MAX - result)
            Invalid("Idempotency snapshot size overflow");
        result += session.ResponseData.size();
    }
    return result;
}
}

void ValidateIdempotencySnapshotLimits(const TIdempotencySnapshotLimits& limits) {
    if (limits.MaxEncodedBytes < FixedHeaderBytes + ChecksumBytes ||
        limits.MaxEncodedBytes > TSnapshot::MaxBytes ||
        limits.MaxApplicationBytes > limits.MaxEncodedBytes ||
        limits.MaxCachedResponseBytes > limits.MaxEncodedBytes ||
        limits.MaxResponseBytes > limits.MaxEncodedBytes ||
        uint64_t(limits.MaxActiveSessions) + limits.MaxClosedSessions > UINT32_MAX)
        throw std::invalid_argument("Invalid idempotency snapshot limits");
}

void ValidateIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
        const TIdempotencySnapshotLimits& limits) {
    ValidateIdempotencySnapshotLimits(limits);
    if (!snapshot.LastAppliedIndex || !snapshot.Epoch)
        Invalid("Idempotency snapshot requires applied progress and a nonzero epoch");
    if (snapshot.ApplicationData.size() > limits.MaxApplicationBytes)
        Invalid("Idempotency application image exceeds byte limit");

    uint64_t active = 0, closed = 0, cachedResponseBytes = 0;
    const TIdempotencySessionRecord* previous = nullptr;
    for (const auto& session : snapshot.Sessions) {
        if ((!session.SessionIdHigh && !session.SessionIdLow) || !session.HighestSequence ||
            !session.LogIndex || session.LogIndex > snapshot.LastAppliedIndex ||
            session.ErrorCode > TCommandResponse::AlreadyCompleted)
            Invalid("Invalid idempotency session record");
        if (previous && !SessionIdLess(*previous, session))
            Invalid("Idempotency sessions must be uniquely sorted by ID");
        previous = &session;

        if (session.Closed) {
            ++closed;
            if (!session.ResponseData.empty() || session.ErrorCode != TCommandResponse::Success)
                Invalid("Closed idempotency sessions cannot retain a response");
            continue;
        }

        ++active;
        if (session.ResponseData.size() > limits.MaxResponseBytes ||
            cachedResponseBytes > limits.MaxCachedResponseBytes ||
            session.ResponseData.size() > limits.MaxCachedResponseBytes - cachedResponseBytes)
            Invalid("Idempotency cached responses exceed byte limit");
        cachedResponseBytes += session.ResponseData.size();
    }
    if (active > limits.MaxActiveSessions || closed > limits.MaxClosedSessions)
        Invalid("Idempotency session count exceeds limit");
    if (EncodedSize(snapshot) > limits.MaxEncodedBytes)
        Invalid("Idempotency snapshot exceeds encoded byte limit");
}

std::string EncodeIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
        const TIdempotencySnapshotLimits& limits) {
    ValidateIdempotencySnapshot(snapshot, limits);
    std::string bytes;
    bytes.reserve(static_cast<size_t>(EncodedSize(snapshot)));
    Append(bytes, Magic);
    Put(bytes, EncodedVersion, 4);
    Put(bytes, snapshot.LastAppliedIndex, 8);
    Put(bytes, snapshot.Epoch, 8);
    Put(bytes, snapshot.ApplicationData.size(), 8);
    Put(bytes, snapshot.Sessions.size(), 4);
    Append(bytes, snapshot.ApplicationData);
    for (const auto& session : snapshot.Sessions) {
        Put(bytes, session.SessionIdHigh, 8);
        Put(bytes, session.SessionIdLow, 8);
        Put(bytes, session.HighestSequence, 8);
        Append(bytes, std::string_view(
            reinterpret_cast<const char*>(session.PayloadDigest.data()),
            session.PayloadDigest.size()));
        Put(bytes, session.LogIndex, 8);
        Put(bytes, session.ErrorCode, 4);
        Put(bytes, session.Closed ? 1 : 0, 4);
        Put(bytes, session.ResponseData.size(), 8);
        Append(bytes, session.ResponseData);
    }
    Put(bytes, Checksum(bytes), 4);
    if (bytes.size() != EncodedSize(snapshot) || bytes.size() > limits.MaxEncodedBytes)
        throw std::logic_error("Idempotency snapshot size calculation mismatch");
    return bytes;
}

TIdempotencySnapshot DecodeIdempotencySnapshot(std::string_view bytes,
        const TIdempotencySnapshotLimits& limits) {
    ValidateIdempotencySnapshotLimits(limits);
    if (bytes.size() < FixedHeaderBytes + ChecksumBytes || bytes.size() > limits.MaxEncodedBytes)
        Invalid("Invalid idempotency snapshot size");

    TReader checksumReader{bytes.substr(bytes.size() - ChecksumBytes)};
    const auto expectedChecksum = checksumReader.Take(4);
    const auto body = bytes.substr(0, bytes.size() - ChecksumBytes);
    if (Checksum(body) != expectedChecksum)
        Invalid("Idempotency snapshot checksum mismatch");

    TReader reader{body};
    if (reader.Read(Magic.size()) != Magic || reader.Take(4) != EncodedVersion)
        Invalid("Unknown idempotency snapshot format");
    TIdempotencySnapshot snapshot;
    snapshot.LastAppliedIndex = reader.Take(8);
    snapshot.Epoch = reader.Take(8);
    const auto applicationBytes = reader.Take(8);
    const auto sessionCount = reader.Take(4);
    if (applicationBytes > limits.MaxApplicationBytes || applicationBytes > reader.Input.size() ||
        sessionCount > uint64_t(limits.MaxActiveSessions) + limits.MaxClosedSessions)
        Invalid("Idempotency snapshot metadata exceeds limits");
    snapshot.ApplicationData.assign(reader.Read(static_cast<size_t>(applicationBytes)));
    if (sessionCount > reader.Input.size() / FixedRecordBytes)
        Invalid("Idempotency snapshot session count exceeds its encoded size");
    snapshot.Sessions.reserve(static_cast<size_t>(sessionCount));

    uint64_t cachedResponseBytes = 0;
    for (uint64_t i = 0; i < sessionCount; ++i) {
        TIdempotencySessionRecord session;
        session.SessionIdHigh = reader.Take(8);
        session.SessionIdLow = reader.Take(8);
        session.HighestSequence = reader.Take(8);
        const auto digest = reader.Read(session.PayloadDigest.size());
        std::memcpy(session.PayloadDigest.data(), digest.data(), digest.size());
        session.LogIndex = reader.Take(8);
        session.ErrorCode = static_cast<uint32_t>(reader.Take(4));
        const auto flags = reader.Take(4);
        if (flags > 1) Invalid("Invalid idempotency session flags");
        session.Closed = flags != 0;
        const auto responseBytes = reader.Take(8);
        if (responseBytes > limits.MaxResponseBytes || responseBytes > reader.Input.size())
            Invalid("Idempotency response exceeds byte limit");
        if (!session.Closed) {
            if (cachedResponseBytes > limits.MaxCachedResponseBytes ||
                responseBytes > limits.MaxCachedResponseBytes - cachedResponseBytes)
                Invalid("Idempotency cached responses exceed byte limit");
            cachedResponseBytes += responseBytes;
        }
        session.ResponseData.assign(reader.Read(static_cast<size_t>(responseBytes)));
        snapshot.Sessions.push_back(std::move(session));
    }
    if (!reader.Input.empty()) Invalid("Trailing bytes in idempotency snapshot");
    ValidateIdempotencySnapshot(snapshot, limits);
    return snapshot;
}
