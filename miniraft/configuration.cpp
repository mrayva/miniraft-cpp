#include "configuration.h"
#include "snapshot_codec.h"
#include "state.h"
#include <climits>
#include <cstring>
#include <stdexcept>
#include <string_view>

namespace {
using namespace NSnapshotCodec;
constexpr std::string_view Magic = "MRFTC001";
constexpr size_t MaxPayload = 48 + 8 * TSnapshot::MaxVoters;
void Validate(const TConfigurationRecord& record) {
    if ((record.Phase != EConfigurationPhase::Joint && record.Phase != EConfigurationPhase::Stable) ||
        !record.Term || record.PreviousIndex > uint64_t(INT64_MAX) ||
        ((record.PreviousIndex == 0) != (record.PreviousTerm == 0)) ||
        record.PreviousTerm > record.Term ||
        (record.Phase == EConfigurationPhase::Stable && !record.PreviousIndex))
        throw std::invalid_argument("Invalid configuration entry metadata");
    ValidateFixedVoters(record.OldVoters);
    ValidateFixedVoters(record.NewVoters);
}
}

TMessageHolder<TLogEntry> EncodeConfigurationEntry(const TConfigurationRecord& record) {
    Validate(record);
    std::string bytes(Magic);
    Put(bytes, uint32_t(record.Phase), 4);
    Put(bytes, record.Term, 8);
    Put(bytes, record.PreviousIndex, 8);
    Put(bytes, record.PreviousTerm, 8);
    Put(bytes, record.OldVoters.size(), 4);
    Put(bytes, record.NewVoters.size(), 4);
    for (auto id : record.OldVoters) Put(bytes, id, 4);
    for (auto id : record.NewVoters) Put(bytes, id, 4);
    Put(bytes, Checksum(bytes), 4);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + bytes.size());
    entry->Term = record.Term; entry->Flags = TLogEntry::EConfiguration;
    memcpy(entry->Data, bytes.data(), bytes.size());
    return entry;
}

TConfigurationRecord DecodeConfigurationEntry(TMessageHolder<TLogEntry> entry) {
    if (!entry.HasValidExtent() || entry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        entry->Len < sizeof(TLogEntry) || entry->Flags != TLogEntry::EConfiguration || entry.PayloadSize)
        throw std::invalid_argument("Invalid configuration entry frame");
    const auto length = entry->Len - sizeof(TLogEntry);
    if (length < 56 || length > MaxPayload) throw std::invalid_argument("Invalid configuration entry size");
    const std::string_view bytes(entry->Data, length);
    TReader trailer{bytes.substr(length - 4)};
    if (Checksum(bytes.substr(0, length - 4)) != trailer.Take(4))
        throw std::invalid_argument("Configuration entry checksum mismatch");
    TReader reader{bytes.substr(0, length - 4)};
    if (reader.Read(Magic.size()) != Magic) throw std::invalid_argument("Unknown configuration entry format");
    TConfigurationRecord record;
    record.Phase = EConfigurationPhase(reader.Take(4));
    record.Term = reader.Take(8); record.PreviousIndex = reader.Take(8); record.PreviousTerm = reader.Take(8);
    const auto oldCount = reader.Take(4), newCount = reader.Take(4);
    if (oldCount > TSnapshot::MaxVoters || newCount > TSnapshot::MaxVoters ||
        oldCount + newCount > reader.Input.size() / 4)
        throw std::invalid_argument("Invalid configuration voter counts");
    for (uint64_t i = 0; i < oldCount; ++i) record.OldVoters.push_back(reader.Take(4));
    for (uint64_t i = 0; i < newCount; ++i) record.NewVoters.push_back(reader.Take(4));
    if (!reader.Input.empty() || entry->Term != record.Term)
        throw std::invalid_argument("Configuration entry term or length mismatch");
    Validate(record);
    return record;
}
