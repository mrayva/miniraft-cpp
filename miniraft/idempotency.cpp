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

constexpr std::array<uint32_t, 64> Sha256RoundConstants{
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

uint32_t RotateRight(uint32_t value, unsigned bits) {
    return (value >> bits) | (value << (32 - bits));
}

uint32_t ReadBigEndianWord(const uint8_t* bytes) {
    return (uint32_t(bytes[0]) << 24) | (uint32_t(bytes[1]) << 16) |
        (uint32_t(bytes[2]) << 8) | uint32_t(bytes[3]);
}

void ProcessSha256Block(const uint8_t* block, std::array<uint32_t, 8>& hash) {
    std::array<uint32_t, 64> words{};
    for (size_t i = 0; i < 16; ++i) words[i] = ReadBigEndianWord(block + 4 * i);
    for (size_t i = 16; i < words.size(); ++i) {
        const auto s0 = RotateRight(words[i - 15], 7) ^ RotateRight(words[i - 15], 18) ^ (words[i - 15] >> 3);
        const auto s1 = RotateRight(words[i - 2], 17) ^ RotateRight(words[i - 2], 19) ^ (words[i - 2] >> 10);
        words[i] = words[i - 16] + s0 + words[i - 7] + s1;
    }

    auto a = hash[0], b = hash[1], c = hash[2], d = hash[3];
    auto e = hash[4], f = hash[5], g = hash[6], h = hash[7];
    for (size_t i = 0; i < words.size(); ++i) {
        const auto sum1 = RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
        const auto choose = (e & f) ^ (~e & g);
        const auto temp1 = h + sum1 + choose + Sha256RoundConstants[i] + words[i];
        const auto sum0 = RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
        const auto majority = (a & b) ^ (a & c) ^ (b & c);
        const auto temp2 = sum0 + majority;
        h = g; g = f; f = e; e = d + temp1;
        d = c; c = b; b = a; a = temp1 + temp2;
    }
    hash[0] += a; hash[1] += b; hash[2] += c; hash[3] += d;
    hash[4] += e; hash[5] += f; hash[6] += g; hash[7] += h;
}

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

std::array<uint8_t, 32> ComputeIdempotentPayloadDigest(std::string_view payload) {
    if (payload.size() > UINT64_MAX / 8)
        throw std::length_error("Idempotent payload is too large to hash");
    const auto blocks = payload.size() / 64 + 1 + (payload.size() % 64 >= 56 ? 1 : 0);
    if (blocks > SIZE_MAX / 64)
        throw std::length_error("Idempotent payload padding size overflow");
    std::vector<uint8_t> padded(blocks * 64, 0);
    if (!payload.empty()) memcpy(padded.data(), payload.data(), payload.size());
    padded[payload.size()] = 0x80;
    const uint64_t bitLength = uint64_t(payload.size()) * 8;
    for (unsigned i = 0; i < 8; ++i)
        padded[padded.size() - 1 - i] = uint8_t(bitLength >> (8 * i));

    std::array<uint32_t, 8> hash{
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    };
    for (size_t offset = 0; offset < padded.size(); offset += 64)
        ProcessSha256Block(padded.data() + offset, hash);

    std::array<uint8_t, 32> digest{};
    for (size_t i = 0; i < hash.size(); ++i) {
        for (unsigned j = 0; j < 4; ++j)
            digest[4 * i + j] = uint8_t(hash[i] >> (24 - 8 * j));
    }
    return digest;
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
