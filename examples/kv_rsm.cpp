#include "kv.h"
#include "kv_messages.h"
#include "payload_validation.h"
#include <string_view>
#include <algorithm>
#include <climits>
#include <miniraft/snapshot_codec.h>

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

std::string TKv::SaveSnapshot() const {
    using namespace NSnapshotCodec;
    std::vector<std::string_view> keys;
    for (const auto& [key, _] : H) keys.push_back(key);
    std::sort(keys.begin(), keys.end());
    std::string bytes = "MRKV0001";
    Put(bytes, H.size(), 8);
    for (auto key : keys) {
        const auto& value = H.at(std::string(key));
        if (key.size() > UINT16_MAX || value.size() > UINT16_MAX)
            throw std::runtime_error("KV snapshot field too large");
        Put(bytes, key.size(), 4);
        Put(bytes, value.size(), 4);
        Append(bytes, key);
        Append(bytes, value);
    }
    return bytes;
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

