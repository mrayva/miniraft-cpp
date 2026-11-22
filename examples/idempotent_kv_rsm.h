#pragma once

#include "kv.h"
#include <memory>
#include <vector>

// A small reference IIdempotentRsm for the KV example. It keeps application
// data and deduplication metadata in the same MiniRaft snapshot. Writes use a
// copy-then-publish step so allocation or validation failures cannot publish
// only one half of the application/session update.
class TIdempotentKvRsm final : public IIdempotentRsm {
public:
    explicit TIdempotentKvRsm(TIdempotencySnapshotLimits limits = {});

    void Validate(TMessageHolder<TCommandRequest> message) const override;
    TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest> message,
                                  uint64_t index) override;
    TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> message,
                                   uint64_t index) override;
    TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest> message) override;

    void ValidateIdempotent(TMessageHolder<TIdempotentCommandRequest> request) const override;
    uint64_t CachedResponseDataUpperBound(
        TMessageHolder<TIdempotentCommandRequest> request) const override;
    TMessageHolder<TLogEntry> PrepareIdempotent(
        TMessageHolder<TIdempotentCommandRequest> request) override;
    TIdempotentWriteResult WriteIdempotent(
        TMessageHolder<TLogEntry> command, uint64_t index,
        const TIdempotentOperationIdentity& identity) override;

    void ValidateIdempotencyControl(
        const TIdempotencyControlOperation& operation) const override;
    TMessageHolder<TLogEntry> PrepareIdempotencyControl(
        const TIdempotencyControlOperation& operation) override;
    void ApplyIdempotencyControl(const TIdempotencyControlOperation& operation,
        TMessageHolder<TLogEntry> applicationEntry, uint64_t index) override;

    TIdempotencySnapshot CaptureIdempotencySnapshot() const override;
    void ValidateIdempotencySnapshot(const TIdempotencySnapshot& snapshot,
        uint64_t index) const override;
    void RestoreIdempotencySnapshot(TIdempotencySnapshot snapshot,
        uint64_t index) override;

private:
    std::shared_ptr<TKv> Kv;
    uint64_t Epoch = 1;
    std::vector<TIdempotencySessionRecord> Sessions;
};
