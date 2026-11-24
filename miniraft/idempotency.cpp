#include "idempotency.h"
#include "raft.h"
#include "snapshot_codec.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace {
using namespace NSnapshotCodec;
constexpr std::string_view Magic = "MRIDEM01";
constexpr std::string_view LogMagic = "MRILOG01";
constexpr std::string_view ControlLogMagic = "MRIDCT01";
constexpr uint32_t EncodedVersion = TIdempotencySnapshot::CurrentVersion;
constexpr uint32_t LogVersion = 1;
constexpr uint32_t ControlLogVersion = 1;
constexpr uint64_t FixedHeaderBytes = 40;
constexpr uint64_t FixedRecordBytes = 80;
constexpr uint64_t ChecksumBytes = 4;
constexpr uint64_t FixedLogRecordBytes = 92;
constexpr uint64_t FixedControlLogRecordBytes = 64;

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

class TIdempotencySnapshotSourceReader {
public:
    TIdempotencySnapshotSourceReader(const TSnapshotDataSource& source, uint64_t totalBytes)
        : Source(source), TotalBytes(totalBytes), BodyBytes(totalBytes - ChecksumBytes) { }

    uint64_t Take(unsigned width) {
        if (!width || width > sizeof(uint64_t)) Invalid("Invalid idempotency snapshot integer width");
        std::array<char, sizeof(uint64_t)> bytes{};
        ReadBody(std::span<char>(bytes.data(), width));
        uint64_t value = 0;
        for (unsigned i = 0; i < width; ++i)
            value |= uint64_t(static_cast<unsigned char>(bytes[i])) << (8 * i);
        return value;
    }

    std::string ReadString(size_t size) {
        if (uint64_t(size) > BodyBytes - Position)
            Invalid("Truncated idempotency snapshot source");
        std::string result(size, '\0');
        ReadBody(std::span<char>(result.data(), result.size()));
        return result;
    }

    uint64_t RemainingBodyBytes() const { return BodyBytes - Position; }

    uint32_t ReadExpectedChecksum() {
        if (Position != BodyBytes) Invalid("Trailing bytes in idempotency snapshot");
        std::array<char, ChecksumBytes> bytes{};
        ReadRaw(BodyBytes, bytes);
        if (Source.Size() != TotalBytes) Invalid("Idempotency snapshot source changed size");
        uint32_t value = 0;
        for (unsigned i = 0; i < ChecksumBytes; ++i)
            value |= uint32_t(static_cast<unsigned char>(bytes[i])) << (8 * i);
        return value;
    }

    uint32_t ActualChecksum() const { return Checksum.Finish(); }

private:
    void ReadRaw(uint64_t offset, std::span<char> destination) const {
        if (offset > TotalBytes || destination.size() > TotalBytes - offset)
            Invalid("Truncated idempotency snapshot source");
        size_t copied = 0;
        while (copied < destination.size()) {
            const auto count = Source.Read(offset + copied, destination.subspan(copied));
            if (!count || count > destination.size() - copied)
                Invalid("Idempotency snapshot source returned an invalid read length");
            copied += count;
        }
    }

    void ReadBody(std::span<char> destination) {
        if (Position > BodyBytes || destination.size() > BodyBytes - Position)
            Invalid("Truncated idempotency snapshot source");
        size_t copied = 0;
        while (copied < destination.size()) {
            const auto count = Source.Read(Position + copied, destination.subspan(copied));
            if (!count || count > destination.size() - copied)
                Invalid("Idempotency snapshot source returned an invalid read length");
            Checksum.Update(reinterpret_cast<const uint8_t*>(destination.data() + copied), count);
            copied += count;
        }
        Position += destination.size();
    }

    const TSnapshotDataSource& Source;
    const uint64_t TotalBytes;
    const uint64_t BodyBytes;
    uint64_t Position = 0;
    TChecksum Checksum;
};
}

TSha256DigestBuilder::TSha256DigestBuilder()
    : Hash{
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    } { }

void TSha256DigestBuilder::Update(std::span<const uint8_t> bytes) {
    if (Finalized) throw std::logic_error("SHA-256 digest builder is finalized");
    if (bytes.size() > UINT64_MAX / 8 - TotalBytes)
        throw std::length_error("SHA-256 input is too large to hash");

    size_t offset = 0;
    if (PendingBytes) {
        const auto count = std::min(Pending.size() - PendingBytes, bytes.size());
        if (count) memcpy(Pending.data() + PendingBytes, bytes.data(), count);
        PendingBytes += count;
        offset += count;
        if (PendingBytes < Pending.size()) {
            TotalBytes += bytes.size();
            return;
        }
        ProcessSha256Block(Pending.data(), Hash);
        PendingBytes = 0;
    }

    while (bytes.size() - offset >= Pending.size()) {
        ProcessSha256Block(bytes.data() + offset, Hash);
        offset += Pending.size();
    }
    PendingBytes = bytes.size() - offset;
    if (PendingBytes) memcpy(Pending.data(), bytes.data() + offset, PendingBytes);
    TotalBytes += bytes.size();
}

std::array<uint8_t, 32> TSha256DigestBuilder::Finish() {
    if (Finalized) throw std::logic_error("SHA-256 digest builder is finalized");
    Finalized = true;

    std::array<uint8_t, 128> tail{};
    if (PendingBytes) memcpy(tail.data(), Pending.data(), PendingBytes);
    tail[PendingBytes] = 0x80;
    const size_t tailBytes = PendingBytes < 56 ? 64 : 128;
    const uint64_t bitLength = TotalBytes * 8;
    for (unsigned i = 0; i < 8; ++i)
        tail[tailBytes - 1 - i] = uint8_t(bitLength >> (8 * i));
    ProcessSha256Block(tail.data(), Hash);
    if (tailBytes == 128) ProcessSha256Block(tail.data() + 64, Hash);

    std::array<uint8_t, 32> digest{};
    for (size_t i = 0; i < Hash.size(); ++i) {
        for (unsigned j = 0; j < 4; ++j)
            digest[4 * i + j] = uint8_t(Hash[i] >> (24 - 8 * j));
    }
    return digest;
}

std::array<uint8_t, 32> ComputeIdempotentPayloadDigest(std::string_view payload) {
    TSha256DigestBuilder digest;
    digest.Update(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));
    return digest.Finish();
}

TMessageHolder<TLogEntry> EncodeIdempotentLogEntry(
        const TIdempotentOperationIdentity& identity,
        TMessageHolder<TLogEntry> applicationEntry) {
    constexpr uint64_t reservedFlags = TLogEntry::EStub | TLogEntry::EIdempotent |
        TLogEntry::EIdempotencyControl | TLogEntry::EConfiguration;
    if (!identity.Epoch || (!identity.SessionIdHigh && !identity.SessionIdLow) || !identity.Sequence ||
        !applicationEntry.HasValidExtent() || applicationEntry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        applicationEntry->Len < sizeof(TLogEntry) || !applicationEntry->Term ||
        (applicationEntry->Flags & reservedFlags))
        throw std::invalid_argument("Invalid idempotent operation log record");

    const uint64_t payloadBytes = applicationEntry->Len - sizeof(TLogEntry);
    const uint64_t encodedBytes = sizeof(TLogEntry) + FixedLogRecordBytes + payloadBytes + ChecksumBytes;
    if (encodedBytes > TMessageLimits{}.MaxFrameBytes)
        throw TMessageError("Idempotent log record exceeds the frame limit");

    std::string bytes;
    bytes.reserve(static_cast<size_t>(FixedLogRecordBytes + payloadBytes + ChecksumBytes));
    Append(bytes, LogMagic);
    Put(bytes, LogVersion, 4);
    Put(bytes, identity.Epoch, 8);
    Put(bytes, identity.SessionIdHigh, 8);
    Put(bytes, identity.SessionIdLow, 8);
    Put(bytes, identity.Sequence, 8);
    Append(bytes, std::string_view(reinterpret_cast<const char*>(identity.PayloadDigest.data()),
        identity.PayloadDigest.size()));
    Put(bytes, applicationEntry->Flags, 8);
    Put(bytes, payloadBytes, 8);
    Append(bytes, std::string_view(applicationEntry->Data, static_cast<size_t>(payloadBytes)));
    Put(bytes, Checksum(bytes), 4);

    auto entry = NewHoldedMessage<TLogEntry>(static_cast<size_t>(encodedBytes));
    entry->Term = applicationEntry->Term;
    entry->Flags = TLogEntry::EIdempotent;
    memcpy(entry->Data, bytes.data(), bytes.size());
    return entry;
}

TDecodedIdempotentLogEntry DecodeIdempotentLogEntry(TMessageHolder<TLogEntry> entry) {
    if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        entry->Len < sizeof(TLogEntry) + FixedLogRecordBytes + ChecksumBytes ||
        entry->Len > TMessageLimits{}.MaxFrameBytes || !entry->Term ||
        entry->Flags != TLogEntry::EIdempotent)
        Invalid("Invalid idempotent operation log entry");
    const std::string_view bytes(entry->Data, entry->Len - sizeof(TLogEntry));
    TReader checksumReader{bytes.substr(bytes.size() - ChecksumBytes)};
    const auto expectedChecksum = checksumReader.Take(4);
    const auto body = bytes.substr(0, bytes.size() - ChecksumBytes);
    if (Checksum(body) != expectedChecksum)
        Invalid("Idempotent operation log checksum mismatch");

    TReader reader{body};
    if (reader.Read(LogMagic.size()) != LogMagic || reader.Take(4) != LogVersion)
        Invalid("Unknown idempotent operation log format");
    TDecodedIdempotentLogEntry decoded;
    auto& identity = decoded.Identity;
    identity.Epoch = reader.Take(8);
    identity.SessionIdHigh = reader.Take(8);
    identity.SessionIdLow = reader.Take(8);
    identity.Sequence = reader.Take(8);
    const auto digest = reader.Read(identity.PayloadDigest.size());
    memcpy(identity.PayloadDigest.data(), digest.data(), digest.size());
    const auto applicationFlags = reader.Take(8);
    const auto applicationBytes = reader.Take(8);
    constexpr uint64_t reservedFlags = TLogEntry::EStub | TLogEntry::EIdempotent |
        TLogEntry::EIdempotencyControl | TLogEntry::EConfiguration;
    if (!identity.Epoch || (!identity.SessionIdHigh && !identity.SessionIdLow) || !identity.Sequence ||
        (applicationFlags & reservedFlags) || applicationBytes != reader.Input.size())
        Invalid("Invalid idempotent operation log metadata");

    decoded.ApplicationEntry = NewHoldedMessage<TLogEntry>(
        sizeof(TLogEntry) + static_cast<size_t>(applicationBytes));
    decoded.ApplicationEntry->Term = entry->Term;
    decoded.ApplicationEntry->Flags = applicationFlags;
    if (applicationBytes)
        memcpy(decoded.ApplicationEntry->Data, reader.Read(static_cast<size_t>(applicationBytes)).data(),
            static_cast<size_t>(applicationBytes));
    if (!reader.Input.empty()) Invalid("Trailing idempotent operation log data");
    return decoded;
}

void ValidateIdempotencyControlOperation(const TIdempotencyControlOperation& operation) {
    switch (operation.Kind) {
    case EIdempotencyControlKind::CloseSession:
        if (!operation.Epoch || (!operation.SessionIdHigh && !operation.SessionIdLow) ||
            operation.NewEpoch)
            throw std::invalid_argument("Invalid idempotency close-session operation");
        return;
    case EIdempotencyControlKind::AdvanceEpoch:
        if (!operation.Epoch || operation.NewEpoch <= operation.Epoch ||
            operation.SessionIdHigh || operation.SessionIdLow)
            throw std::invalid_argument("Invalid idempotency epoch-advance operation");
        return;
    }
    throw std::invalid_argument("Unknown idempotency control operation");
}

TMessageHolder<TLogEntry> EncodeIdempotencyControlLogEntry(
        const TIdempotencyControlOperation& operation,
        TMessageHolder<TLogEntry> applicationEntry) {
    ValidateIdempotencyControlOperation(operation);
    if (!applicationEntry.HasValidExtent() ||
        applicationEntry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        applicationEntry->Len < sizeof(TLogEntry) || !applicationEntry->Term ||
        applicationEntry->Flags != TLogEntry::ENone)
        throw std::invalid_argument("Invalid idempotency control application entry");

    const uint64_t payloadBytes = applicationEntry->Len - sizeof(TLogEntry);
    const uint64_t fixedBytes = FixedControlLogRecordBytes + ChecksumBytes;
    if (payloadBytes > TMessageLimits{}.MaxFrameBytes - sizeof(TLogEntry) - fixedBytes)
        throw TMessageError("Idempotency control log record exceeds the frame limit");
    const auto encodedBytes = sizeof(TLogEntry) + fixedBytes + payloadBytes;

    std::string bytes;
    bytes.reserve(static_cast<size_t>(fixedBytes + payloadBytes));
    Append(bytes, ControlLogMagic);
    Put(bytes, ControlLogVersion, 4);
    Put(bytes, static_cast<uint32_t>(operation.Kind), 4);
    Put(bytes, operation.Epoch, 8);
    Put(bytes, operation.NewEpoch, 8);
    Put(bytes, operation.SessionIdHigh, 8);
    Put(bytes, operation.SessionIdLow, 8);
    Put(bytes, applicationEntry->Flags, 8);
    Put(bytes, payloadBytes, 8);
    Append(bytes, std::string_view(applicationEntry->Data, static_cast<size_t>(payloadBytes)));
    Put(bytes, Checksum(bytes), 4);

    auto entry = NewHoldedMessage<TLogEntry>(static_cast<size_t>(encodedBytes));
    entry->Term = applicationEntry->Term;
    entry->Flags = TLogEntry::EIdempotencyControl;
    memcpy(entry->Data, bytes.data(), bytes.size());
    return entry;
}

TDecodedIdempotencyControlLogEntry DecodeIdempotencyControlLogEntry(
        TMessageHolder<TLogEntry> entry) {
    if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        entry->Len < sizeof(TLogEntry) + FixedControlLogRecordBytes + ChecksumBytes ||
        entry->Len > TMessageLimits{}.MaxFrameBytes || !entry->Term ||
        entry->Flags != TLogEntry::EIdempotencyControl)
        Invalid("Invalid idempotency control log entry");
    const std::string_view bytes(entry->Data, entry->Len - sizeof(TLogEntry));
    TReader checksumReader{bytes.substr(bytes.size() - ChecksumBytes)};
    const auto expectedChecksum = checksumReader.Take(4);
    const auto body = bytes.substr(0, bytes.size() - ChecksumBytes);
    if (Checksum(body) != expectedChecksum)
        Invalid("Idempotency control log checksum mismatch");

    TReader reader{body};
    if (reader.Read(ControlLogMagic.size()) != ControlLogMagic ||
        reader.Take(4) != ControlLogVersion)
        Invalid("Unknown idempotency control log format");
    TDecodedIdempotencyControlLogEntry decoded;
    decoded.Operation.Kind = static_cast<EIdempotencyControlKind>(reader.Take(4));
    decoded.Operation.Epoch = reader.Take(8);
    decoded.Operation.NewEpoch = reader.Take(8);
    decoded.Operation.SessionIdHigh = reader.Take(8);
    decoded.Operation.SessionIdLow = reader.Take(8);
    const auto applicationFlags = reader.Take(8);
    const auto applicationBytes = reader.Take(8);
    try { ValidateIdempotencyControlOperation(decoded.Operation); }
    catch (const std::exception&) { Invalid("Invalid idempotency control metadata"); }
    if (applicationFlags != TLogEntry::ENone || applicationBytes != reader.Input.size())
        Invalid("Invalid idempotency control application metadata");

    decoded.ApplicationEntry = NewHoldedMessage<TLogEntry>(
        sizeof(TLogEntry) + static_cast<size_t>(applicationBytes));
    decoded.ApplicationEntry->Term = entry->Term;
    decoded.ApplicationEntry->Flags = applicationFlags;
    if (applicationBytes)
        memcpy(decoded.ApplicationEntry->Data,
            reader.Read(static_cast<size_t>(applicationBytes)).data(),
            static_cast<size_t>(applicationBytes));
    if (!reader.Input.empty()) Invalid("Trailing idempotency control log data");
    return decoded;
}

void ValidateIdempotencySnapshotLimits(const TIdempotencySnapshotLimits& limits) {
    if (limits.MaxEncodedBytes < FixedHeaderBytes + ChecksumBytes ||
        limits.MaxEncodedBytes > TSnapshot::MaxBytes ||
        limits.MaxApplicationBytes > limits.MaxEncodedBytes ||
        limits.MaxCachedResponseBytes > limits.MaxEncodedBytes ||
        limits.MaxResponseBytes > limits.MaxEncodedBytes ||
        limits.MaxResponseBytes > TMessageLimits{}.MaxFrameBytes - sizeof(TIdempotentCommandResponse) ||
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

uint64_t WriteIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
        const TIdempotencySnapshotLimits& limits, ISnapshotSink& sink) {
    ValidateIdempotencySnapshot(snapshot, limits);
    const auto encodedBytes = EncodedSize(snapshot);
    uint64_t written = 0;
    NSnapshotCodec::TChecksum checksum;
    auto writePayload = [&](std::string_view bytes) {
        if (bytes.empty()) return;
        if (written > encodedBytes || bytes.size() > encodedBytes - written)
            throw std::logic_error("Idempotency snapshot size calculation mismatch");
        sink.Write(std::span<const char>(bytes.data(), bytes.size()));
        checksum.Update(bytes);
        written += bytes.size();
    };
    auto writeInteger = [&](uint64_t value, unsigned width) {
        std::array<char, 8> encoded{};
        for (unsigned i = 0; i < width; ++i) encoded[i] = char(value >> (8 * i));
        writePayload(std::string_view(encoded.data(), width));
    };

    writePayload(Magic);
    writeInteger(EncodedVersion, 4);
    writeInteger(snapshot.LastAppliedIndex, 8);
    writeInteger(snapshot.Epoch, 8);
    writeInteger(snapshot.ApplicationData.size(), 8);
    writeInteger(snapshot.Sessions.size(), 4);
    writePayload(snapshot.ApplicationData);
    for (const auto& session : snapshot.Sessions) {
        writeInteger(session.SessionIdHigh, 8);
        writeInteger(session.SessionIdLow, 8);
        writeInteger(session.HighestSequence, 8);
        writePayload(std::string_view(
            reinterpret_cast<const char*>(session.PayloadDigest.data()),
            session.PayloadDigest.size()));
        writeInteger(session.LogIndex, 8);
        writeInteger(session.ErrorCode, 4);
        writeInteger(session.Closed ? 1 : 0, 4);
        writeInteger(session.ResponseData.size(), 8);
        writePayload(session.ResponseData);
    }

    if (written != encodedBytes - ChecksumBytes)
        throw std::logic_error("Idempotency snapshot size calculation mismatch");
    const auto checksumBytes = checksum.Finish();
    std::array<char, ChecksumBytes> encodedChecksum{};
    for (unsigned i = 0; i < ChecksumBytes; ++i)
        encodedChecksum[i] = char(checksumBytes >> (8 * i));
    sink.Write(encodedChecksum);
    written += encodedChecksum.size();
    if (written != encodedBytes)
        throw std::logic_error("Idempotency snapshot size calculation mismatch");
    return written;
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

TIdempotencySnapshot DecodeIdempotencySnapshot(const TSnapshotDataSource& source,
        const TIdempotencySnapshotLimits& limits) {
    ValidateIdempotencySnapshotLimits(limits);
    const auto totalBytes = source.Size();
    if (totalBytes < FixedHeaderBytes + ChecksumBytes || totalBytes > limits.MaxEncodedBytes)
        Invalid("Invalid idempotency snapshot size");

    TIdempotencySnapshotSourceReader reader(source, totalBytes);
    if (reader.ReadString(Magic.size()) != Magic || reader.Take(4) != EncodedVersion)
        Invalid("Unknown idempotency snapshot format");
    TIdempotencySnapshot snapshot;
    snapshot.LastAppliedIndex = reader.Take(8);
    snapshot.Epoch = reader.Take(8);
    const auto applicationBytes = reader.Take(8);
    const auto sessionCount = reader.Take(4);
    if (applicationBytes > limits.MaxApplicationBytes ||
        applicationBytes > reader.RemainingBodyBytes() ||
        sessionCount > uint64_t(limits.MaxActiveSessions) + limits.MaxClosedSessions)
        Invalid("Idempotency snapshot metadata exceeds limits");
    snapshot.ApplicationData = reader.ReadString(static_cast<size_t>(applicationBytes));
    if (sessionCount > reader.RemainingBodyBytes() / FixedRecordBytes)
        Invalid("Idempotency snapshot session count exceeds its encoded size");
    snapshot.Sessions.reserve(static_cast<size_t>(sessionCount));

    uint64_t cachedResponseBytes = 0;
    for (uint64_t i = 0; i < sessionCount; ++i) {
        TIdempotencySessionRecord session;
        session.SessionIdHigh = reader.Take(8);
        session.SessionIdLow = reader.Take(8);
        session.HighestSequence = reader.Take(8);
        const auto digest = reader.ReadString(session.PayloadDigest.size());
        std::memcpy(session.PayloadDigest.data(), digest.data(), digest.size());
        session.LogIndex = reader.Take(8);
        session.ErrorCode = static_cast<uint32_t>(reader.Take(4));
        const auto flags = reader.Take(4);
        if (flags > 1) Invalid("Invalid idempotency session flags");
        session.Closed = flags != 0;
        const auto responseBytes = reader.Take(8);
        if (responseBytes > limits.MaxResponseBytes || responseBytes > reader.RemainingBodyBytes())
            Invalid("Idempotency response exceeds byte limit");
        if (!session.Closed) {
            if (cachedResponseBytes > limits.MaxCachedResponseBytes ||
                responseBytes > limits.MaxCachedResponseBytes - cachedResponseBytes)
                Invalid("Idempotency cached responses exceed byte limit");
            cachedResponseBytes += responseBytes;
        }
        session.ResponseData = reader.ReadString(static_cast<size_t>(responseBytes));
        snapshot.Sessions.push_back(std::move(session));
    }
    if (reader.RemainingBodyBytes()) Invalid("Trailing bytes in idempotency snapshot");
    if (reader.ActualChecksum() != reader.ReadExpectedChecksum())
        Invalid("Idempotency snapshot checksum mismatch");
    ValidateIdempotencySnapshot(snapshot, limits);
    return snapshot;
}
