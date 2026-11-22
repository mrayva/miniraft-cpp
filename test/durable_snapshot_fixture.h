#pragma once

#include <miniraft/idempotency.h>
#include <miniraft/snapshot_codec.h>
#include <stdexcept>
#include <string>
#include <string_view>

// Tests that intentionally synthesize a recovered store need a consistent
// manifest so recovery reaches the membership checks they exercise.
inline void RebindDurableSnapshotFixture(std::string& manifest, std::string_view image) {
    if (!manifest.starts_with("MRFTD006") || manifest.size() < 108)
        throw std::runtime_error("Unexpected durable fixture format");
    uint32_t voters = 0;
    for (unsigned i = 0; i < 4; ++i)
        voters |= uint32_t(static_cast<unsigned char>(manifest[48 + i])) << (8 * i);
    const auto offset = 68 + 4 * uint64_t(voters);
    const auto digest = ComputeIdempotentPayloadDigest(image);
    if (offset + 8 + digest.size() > manifest.size())
        throw std::runtime_error("Truncated durable fixture");
    for (unsigned i = 0; i < 8; ++i)
        manifest[offset + i] = char(uint64_t(image.size()) >> (8 * i));
    manifest.replace(offset + 8, digest.size(), reinterpret_cast<const char*>(digest.data()), digest.size());
    const auto crc = NSnapshotCodec::Checksum(manifest.substr(0, 8) + manifest.substr(20));
    for (unsigned i = 0; i < 4; ++i)
        manifest[16 + i] = char(crc >> (8 * i));
}
