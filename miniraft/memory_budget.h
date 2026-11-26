#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>

enum class EProcessMemoryCategory : uint8_t {
    OutboundQueue,
    InboundNetwork,
    SnapshotHeap,
    Count,
};

struct TProcessMemoryBudgetLimits {
    static constexpr uint64_t MiB = 1024ULL * 1024;

    uint64_t MaxBytes = 1024 * MiB;
    uint64_t MaxOutboundQueueBytes = 512 * MiB;
    uint64_t MaxInboundNetworkBytes = 512 * MiB;
    uint64_t MaxSnapshotHeapBytes = 256 * MiB;
    uint64_t ReservedOutboundQueueBytes = 128 * MiB;
    uint64_t ReservedInboundNetworkBytes = 128 * MiB;
    uint64_t ReservedSnapshotHeapBytes = 128 * MiB;
    size_t MaxOutboundQueueMessages = 16 * 1024;
};

// One byte counter shared by transport and retained snapshot budgets. Category
// ceilings preserve each pool's headroom; the total ceiling bounds their sum.
class TProcessMemoryBudget {
public:
    explicit TProcessMemoryBudget(TProcessMemoryBudgetLimits limits = {})
        : Limits(limits) {
        uint64_t categoryBytes = Limits.MaxOutboundQueueBytes;
        if (Limits.MaxInboundNetworkBytes > UINT64_MAX - categoryBytes)
            throw std::invalid_argument("Process memory category limit overflow");
        categoryBytes += Limits.MaxInboundNetworkBytes;
        if (Limits.MaxSnapshotHeapBytes > UINT64_MAX - categoryBytes)
            throw std::invalid_argument("Process memory category limit overflow");
        categoryBytes += Limits.MaxSnapshotHeapBytes;
        if (!Limits.MaxBytes || Limits.ReservedOutboundQueueBytes > Limits.MaxOutboundQueueBytes ||
            Limits.ReservedInboundNetworkBytes > Limits.MaxInboundNetworkBytes ||
            Limits.ReservedSnapshotHeapBytes > Limits.MaxSnapshotHeapBytes)
            throw std::invalid_argument("Invalid process memory category reservation");
        uint64_t reservedBytes = Limits.ReservedOutboundQueueBytes;
        if (Limits.ReservedInboundNetworkBytes > UINT64_MAX - reservedBytes)
            throw std::invalid_argument("Process memory reservation overflow");
        reservedBytes += Limits.ReservedInboundNetworkBytes;
        if (Limits.ReservedSnapshotHeapBytes > UINT64_MAX - reservedBytes)
            throw std::invalid_argument("Process memory reservation overflow");
        reservedBytes += Limits.ReservedSnapshotHeapBytes;
        if (reservedBytes > Limits.MaxBytes)
            throw std::invalid_argument("Process memory cap cannot preserve category headroom");
    }

    static std::shared_ptr<TProcessMemoryBudget> DefaultProcessBudget() {
        static auto budget = std::make_shared<TProcessMemoryBudget>();
        return budget;
    }

    bool TryReplace(EProcessMemoryCategory category,
            size_t oldMessages, uint64_t oldBytes,
            size_t newMessages, uint64_t newBytes) {
        const auto index = Index(category);
        std::lock_guard lock(Mutex);
        if (oldMessages > UsedMessages[index] || oldBytes > UsedBytesByCategory[index] ||
            oldBytes > TotalUsedBytes)
            throw std::logic_error("Process memory accounting underflow");

        const auto retainedMessages = UsedMessages[index] - oldMessages;
        const auto retainedCategoryBytes = UsedBytesByCategory[index] - oldBytes;
        const auto retainedTotalBytes = TotalUsedBytes - oldBytes;
        const auto categoryLimit = CategoryLimit(category);
        if (retainedCategoryBytes > categoryLimit ||
            newBytes > categoryLimit - retainedCategoryBytes ||
            retainedTotalBytes > Limits.MaxBytes ||
            newBytes > Limits.MaxBytes - retainedTotalBytes)
            return false;
        auto remainingAfterReplacement = Limits.MaxBytes - (retainedTotalBytes + newBytes);
        for (size_t other = 0; other < UsedBytesByCategory.size(); ++other) {
            if (other == index) continue;
            const auto reserved = CategoryReservation(static_cast<EProcessMemoryCategory>(other));
            const auto used = UsedBytesByCategory[other];
            if (used < reserved) {
                const auto headroom = reserved - used;
                if (headroom > remainingAfterReplacement) return false;
                remainingAfterReplacement -= headroom;
            }
        }

        if (category == EProcessMemoryCategory::OutboundQueue) {
            if (retainedMessages > Limits.MaxOutboundQueueMessages ||
                newMessages > Limits.MaxOutboundQueueMessages - retainedMessages)
                return false;
        } else if (oldMessages || newMessages) {
            throw std::invalid_argument("Only outbound queues reserve process message counts");
        }

        UsedMessages[index] = retainedMessages + newMessages;
        UsedBytesByCategory[index] = retainedCategoryBytes + newBytes;
        TotalUsedBytes = retainedTotalBytes + newBytes;
        return true;
    }

    void Release(EProcessMemoryCategory category, size_t messages, uint64_t bytes) noexcept {
        const auto index = static_cast<size_t>(category);
        if (index >= UsedBytesByCategory.size()) std::terminate();
        std::lock_guard lock(Mutex);
        if (messages > UsedMessages[index] || bytes > UsedBytesByCategory[index] ||
            bytes > TotalUsedBytes)
            std::terminate();
        UsedMessages[index] -= messages;
        UsedBytesByCategory[index] -= bytes;
        TotalUsedBytes -= bytes;
    }

    uint64_t UsedTotalBytes() const {
        std::lock_guard lock(Mutex);
        return TotalUsedBytes;
    }

    uint64_t UsedBytes(EProcessMemoryCategory category) const {
        std::lock_guard lock(Mutex);
        return UsedBytesByCategory[Index(category)];
    }

    uint64_t LimitBytes(EProcessMemoryCategory category) const {
        return CategoryLimit(category);
    }

    uint64_t TotalLimitBytes() const { return Limits.MaxBytes; }

private:
    static size_t Index(EProcessMemoryCategory category) {
        const auto index = static_cast<size_t>(category);
        if (index >= static_cast<size_t>(EProcessMemoryCategory::Count))
            throw std::invalid_argument("Invalid process memory category");
        return index;
    }

    uint64_t CategoryLimit(EProcessMemoryCategory category) const {
        switch (category) {
            case EProcessMemoryCategory::OutboundQueue: return Limits.MaxOutboundQueueBytes;
            case EProcessMemoryCategory::InboundNetwork: return Limits.MaxInboundNetworkBytes;
            case EProcessMemoryCategory::SnapshotHeap: return Limits.MaxSnapshotHeapBytes;
            case EProcessMemoryCategory::Count: break;
        }
        throw std::invalid_argument("Invalid process memory category");
    }

    uint64_t CategoryReservation(EProcessMemoryCategory category) const {
        switch (category) {
            case EProcessMemoryCategory::OutboundQueue: return Limits.ReservedOutboundQueueBytes;
            case EProcessMemoryCategory::InboundNetwork: return Limits.ReservedInboundNetworkBytes;
            case EProcessMemoryCategory::SnapshotHeap: return Limits.ReservedSnapshotHeapBytes;
            case EProcessMemoryCategory::Count: break;
        }
        throw std::invalid_argument("Invalid process memory category");
    }

    const TProcessMemoryBudgetLimits Limits;
    mutable std::mutex Mutex;
    std::array<size_t, static_cast<size_t>(EProcessMemoryCategory::Count)> UsedMessages{};
    std::array<uint64_t, static_cast<size_t>(EProcessMemoryCategory::Count)> UsedBytesByCategory{};
    uint64_t TotalUsedBytes = 0;
};
