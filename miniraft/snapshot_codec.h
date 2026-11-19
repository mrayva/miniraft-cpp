#pragma once
#include "snapshot.h"
#include <array>
#include <stdexcept>

namespace NSnapshotCodec {
inline constexpr auto Crc32Table = [] {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < table.size(); ++i) {
        auto crc = i;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (0xedb88320u & (0u - (crc & 1u)));
        table[i] = crc;
    }
    return table;
}();

inline void Append(std::string& out, std::string_view part) {
    if (out.size() > TSnapshot::MaxBytes || part.size() > TSnapshot::MaxBytes - out.size())
        throw std::runtime_error("Snapshot exceeds size limit");
    out.append(part);
}
inline void Put(std::string& out, uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) {
        const char byte = char(value >> (8 * i));
        Append(out, std::string_view(&byte, 1));
    }
}
struct TReader {
    std::string_view Input;
    std::string_view Read(size_t length) {
        if (length > Input.size()) throw std::runtime_error("Truncated snapshot");
        const auto result = Input.substr(0, length);
        Input.remove_prefix(length);
        return result;
    }
    uint64_t Take(unsigned width) {
        const auto bytes = Read(width);
        uint64_t value = 0;
        for (unsigned i = 0; i < width; ++i) value |= uint64_t(uint8_t(bytes[i])) << (8 * i);
        return value;
    }
};
struct TChecksum {
    uint32_t State = UINT32_MAX;
    void Update(std::string_view bytes) {
        Update(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    }
    void Update(const uint8_t* bytes, size_t size) {
        for (size_t i = 0; i < size; ++i)
            State = (State >> 8) ^ Crc32Table[(State ^ bytes[i]) & 0xffu];
    }
    uint32_t Finish() const { return ~State; }
};
inline uint32_t Checksum(std::string_view bytes) {
    TChecksum checksum;
    checksum.Update(bytes);
    return checksum.Finish();
}
}
