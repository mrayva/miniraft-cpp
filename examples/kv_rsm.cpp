#include "kv.h"
#include "kv_messages.h"
#include "payload_validation.h"
#include <string_view>
#include <algorithm>
#include <array>
#include <climits>
#include <vector>
#include <miniraft/snapshot_codec.h>

namespace {
class TSnapshotSourceReader {
public:
    explicit TSnapshotSourceReader(const TSnapshotDataSource& source) : Source(source) { }

    uint64_t Take(unsigned width) {
        if (!width || width > sizeof(uint64_t))
            throw std::invalid_argument("Invalid KV snapshot integer width");
        std::array<char, sizeof(uint64_t)> bytes{};
        Read(std::span<char>(bytes.data(), width));
        uint64_t value = 0;
        for (unsigned i = 0; i < width; ++i)
            value |= uint64_t(static_cast<unsigned char>(bytes[i])) << (8 * i);
        return value;
    }

    std::string ReadString(size_t size) {
        std::string result(size, '\0');
        Read(std::span<char>(result.data(), result.size()));
        return result;
    }

    uint64_t Offset() const { return Position; }
    uint64_t Size() const { return Source.Size(); }

private:
    void Read(std::span<char> destination) {
        if (Position > Source.Size() || destination.size() > Source.Size() - Position)
            throw std::runtime_error("Truncated KV snapshot source");
        size_t copied = 0;
        while (copied < destination.size()) {
            const auto count = Source.Read(Position + copied, destination.subspan(copied));
            if (!count || count > destination.size() - copied)
                throw std::runtime_error("KV snapshot source returned an invalid read length");
            copied += count;
        }
        Position += destination.size();
    }

    const TSnapshotDataSource& Source;
    uint64_t Position = 0;
};

std::unordered_map<std::string, std::string> DecodeKvSnapshotSource(
        const TSnapshotDataSource& source, uint64_t index, uint64_t currentIndex) {
    if (!index || index > uint64_t(INT64_MAX) || index < currentIndex ||
        source.Size() > TSnapshot::MaxBytes || source.Size() < 16)
        throw std::runtime_error("Invalid KV snapshot progress/size");

    TSnapshotSourceReader reader(source);
    if (reader.ReadString(8) != "MRKV0001")
        throw std::runtime_error("Unknown KV snapshot format");
    const auto count = reader.Take(8);
    if (count > (source.Size() - 16) / 8)
        throw std::runtime_error("Invalid KV snapshot count");

    std::unordered_map<std::string, std::string> candidate;
    std::string previous;
    for (uint64_t i = 0; i < count; ++i) {
        const auto keyLength = reader.Take(4), valueLength = reader.Take(4);
        if (keyLength > UINT16_MAX || valueLength > UINT16_MAX)
            throw std::runtime_error("KV snapshot field exceeds application limits");
        auto key = reader.ReadString(size_t(keyLength));
        auto value = reader.ReadString(size_t(valueLength));
        if (!valueLength || (i && key <= previous))
            throw std::runtime_error("Invalid KV snapshot order/value");
        previous = key;
        candidate.emplace(std::move(key), std::move(value));
    }
    if (reader.Offset() != source.Size())
        throw std::runtime_error("Trailing KV snapshot bytes");
    return candidate;
}
}

void TKv::Validate(TMessageHolder<TCommandRequest> message) const {
    if (!message || message->Len < sizeof(TCommandRequest)) throw TMessageError("Short KV command");
    if (message->Flags & TCommandRequest::EWrite) {
        auto write = message.Cast<TWriteKv>();
        ValidatePayloadRange(message, write->TKvEntry::Data, uint64_t(write->KeySize) + write->ValSize);
    } else {
        auto read = message.Cast<TReadKv>();
        ValidatePayloadRange(message, read->Data, read->KeySize);
    }
}

TMessageHolder<TMessage> TKv::Read(TMessageHolder<TCommandRequest> message, uint64_t index) {
    auto readKv = message.Cast<TReadKv>();
    ValidatePayloadRange(message, readKv->Data, readKv->KeySize);
    if (readKv->KeySize == 0) {
        std::string data;
        for (auto& [k, v] : H) {
            if (k.size() + v.size() + 4 > TMessageLimits{}.MaxFrameBytes - sizeof(TCommandResponse) - data.size())
                throw TMessageError("KV list response exceeds frame limit");
            data += k + " = " + v + "\n";
        }
        auto res = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse) + data.size());
        memcpy(res->Data, data.data(), data.size());
        res->Index = index;
        return res;
    } else {
        std::string_view k(readKv->Data, readKv->KeySize);
        auto it = H.find(std::string(k));
        if (it == H.end()) {
            auto res = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse));
            res->Index = index;
            return res;
        } else {
            auto res = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse)+it->second.size());
            res->Index = index;
            memcpy(res->Data, it->second.data(), it->second.size());
            return res;
        }
    }
}

TMessageHolder<TMessage> TKv::Write(TMessageHolder<TLogEntry> message, uint64_t index) {
    if (LastAppliedIndex < index) {
        auto entry = message.Cast<TKvLogEntry>();
        ValidatePayloadRange(message, entry->TKvEntry::Data, uint64_t(entry->KeySize) + entry->ValSize);
        std::string_view k(entry->TKvEntry::Data, entry->KeySize);
        if (entry->ValSize) {
            std::string_view v(entry->TKvEntry::Data + entry->KeySize, entry->ValSize);
            H[std::string(k)] = std::string(v);
        } else {
            H.erase(std::string(k));
        }
        LastAppliedIndex = index;
    }
    return {};
}

TMessageHolder<TLogEntry> TKv::Prepare(TMessageHolder<TCommandRequest> command) {
    auto write = command.Cast<TWriteKv>();
    ValidatePayloadRange(command, write->TKvEntry::Data, uint64_t(write->KeySize) + write->ValSize);
    auto dataSize = command->Len - sizeof(TCommandRequest);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry)+dataSize);
    memcpy(entry->Data, command->Data, dataSize);
    return entry;
}

std::optional<uint64_t> TKv::SnapshotSizeUpperBound() const {
    uint64_t encodedSize = 16; // magic[8] and entry count[8]
    const auto account = [&](size_t amount) {
        if (amount > TSnapshot::MaxBytes - encodedSize)
            throw std::runtime_error("KV snapshot exceeds size limit");
        encodedSize += amount;
    };
    for (const auto& [key, value] : H) {
        if (key.size() > UINT16_MAX || value.size() > UINT16_MAX)
            throw std::runtime_error("KV snapshot field too large");
        account(8); // key and value lengths
        account(key.size());
        account(value.size());
    }
    return encodedSize;
}

std::string TKv::SaveSnapshot() const {
    using namespace NSnapshotCodec;
    const auto encodedSize = size_t(SnapshotSizeUpperBound().value());
    std::vector<std::string_view> keys;
    keys.reserve(H.size());
    for (const auto& [key, _] : H) keys.push_back(key);
    std::sort(keys.begin(), keys.end());
    std::string bytes;
    bytes.reserve(encodedSize);
    Append(bytes, "MRKV0001");
    Put(bytes, H.size(), 8);
    for (auto key : keys) {
        const auto& value = H.at(std::string(key));
        Put(bytes, key.size(), 4);
        Put(bytes, value.size(), 4);
        Append(bytes, key);
        Append(bytes, value);
    }
    if (bytes.size() != encodedSize) throw std::logic_error("KV snapshot size calculation mismatch");
    return bytes;
}

void TKv::WriteSnapshot(uint64_t index, ISnapshotSink& sink) const {
    if (index != LastAppliedIndex) throw std::logic_error("KV snapshot writer index mismatch");
    (void)SnapshotSizeUpperBound();

    using TEntry = std::unordered_map<std::string, std::string>::value_type;
    std::vector<const TEntry*> entries;
    entries.reserve(H.size());
    for (const auto& entry : H) entries.push_back(&entry);
    std::sort(entries.begin(), entries.end(), [](const TEntry* lhs, const TEntry* rhs) {
        return lhs->first < rhs->first;
    });

    std::string header = "MRKV0001";
    NSnapshotCodec::Put(header, H.size(), 8);
    sink.Write(std::span<const char>(header.data(), header.size()));
    for (const auto* entry : entries) {
        std::array<char, 8> lengths{};
        for (unsigned i = 0; i < 4; ++i) {
            lengths[i] = char(entry->first.size() >> (8 * i));
            lengths[4 + i] = char(entry->second.size() >> (8 * i));
        }
        sink.Write(lengths);
        sink.Write(std::span<const char>(entry->first.data(), entry->first.size()));
        sink.Write(std::span<const char>(entry->second.data(), entry->second.size()));
    }
}

void TKv::LoadSnapshot(std::string_view bytes, uint64_t index) {
    using namespace NSnapshotCodec;
    if (!index || index > uint64_t(INT64_MAX) || index < LastAppliedIndex || bytes.size() > TSnapshot::MaxBytes)
        throw std::runtime_error("Invalid KV snapshot progress/size");
    TReader reader{bytes};
    if (reader.Read(8) != "MRKV0001") throw std::runtime_error("Unknown KV snapshot format");
    const auto count = reader.Take(8);
    if (count > reader.Input.size() / 8) throw std::runtime_error("Invalid KV snapshot count");
    std::unordered_map<std::string, std::string> candidate;
    std::string_view previous;
    for (uint64_t i = 0; i < count; ++i) {
        const auto keyLength = reader.Take(4), valueLength = reader.Take(4);
        if (keyLength > UINT16_MAX || valueLength > UINT16_MAX)
            throw std::runtime_error("KV snapshot field exceeds application limits");
        const auto key = reader.Read(keyLength), value = reader.Read(valueLength);
        if (!valueLength || (i && key <= previous)) throw std::runtime_error("Invalid KV snapshot order/value");
        candidate.emplace(std::string(key), std::string(value));
        previous = key;
    }
    if (!reader.Input.empty()) throw std::runtime_error("Trailing KV snapshot bytes");
    H.swap(candidate);
    LastAppliedIndex = index;
}

void TKv::InstallSnapshotEntries(TEntries&& entries, uint64_t index) {
    H.swap(entries);
    LastAppliedIndex = index;
}

struct TStreamingKv::TInstallation final : IStagedSnapshotInstallation {
    TStreamingKv& Application;
    TEntries Entries;
    uint64_t Index;
    uint64_t ExpectedCurrentIndex;

    TInstallation(TStreamingKv& application, TEntries entries, uint64_t index)
        : Application(application), Entries(std::move(entries)), Index(index),
          ExpectedCurrentIndex(application.LastAppliedIndex) { }

    void Install() override {
        if (Application.LastAppliedIndex != ExpectedCurrentIndex)
            throw std::logic_error("KV application advanced after snapshot preparation");
        Application.InstallSnapshotEntries(std::move(Entries), Index);
    }
};

std::unique_ptr<IStagedSnapshotInstallation> TStreamingKv::PrepareSnapshotInstallation(
        const TSnapshotDataSource& source, uint64_t index) {
    auto candidate = DecodeKvSnapshotSource(source, index, LastAppliedIndex);
    return std::make_unique<TInstallation>(*this, std::move(candidate), index);
}

void TKv::ValidateSnapshot(std::string_view bytes, uint64_t index) const {
    TKv candidate;
    candidate.LoadSnapshot(bytes, index);
}

