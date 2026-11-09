#pragma once
#include "snapshot.h"
#include <stdexcept>

namespace NSnapshotCodec {
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
inline uint32_t Checksum(std::string_view bytes) {
    uint32_t crc = UINT32_MAX;
    for (uint8_t byte : bytes) {
        crc ^= byte;
        for (int i = 0; i < 8; ++i) crc = (crc >> 1) ^ (0xedb88320u & (0u - (crc & 1u)));
    }
    return ~crc;
}
}
