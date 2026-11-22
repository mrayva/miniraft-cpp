#pragma once

#include "messages.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

// Store implementations must make Save atomic and durable before returning.
// Persist this state in the client's own durable transaction or journal; in
// particular, a pending payload must reach durable storage before its request
// is sent to Raft.
struct TIdempotentClientSessionState {
    static constexpr uint32_t CurrentVersion = 1;
    uint32_t Version = CurrentVersion;
    bool Initialized = false;
    uint64_t Epoch = 0;
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    uint64_t NextSequence = 0;
    bool HasPending = false;
    std::string PendingPayload;
    bool operator==(const TIdempotentClientSessionState&) const = default;
};

struct IIdempotentClientStateStore {
    virtual ~IIdempotentClientStateStore() = default;
    virtual std::optional<TIdempotentClientSessionState> Load() = 0;
    virtual void Save(const TIdempotentClientSessionState& state) = 0;
};

// Linux single-file client session store. Each Save publishes one checksummed
// state image with file and directory fsync before returning. A store holds an
// exclusive advisory lock for its lifetime; use one store instance per session
// file. The file contains client identity and any pending request payload.
class TDurableIdempotentClientStateStore final : public IIdempotentClientStateStore {
public:
    enum class EStage { TempCreated, Written, FileSynced, Renamed, DirectorySynced };
    using THook = std::function<void(EStage)>;

    explicit TDurableIdempotentClientStateStore(const std::string& path, THook hook = {});
    ~TDurableIdempotentClientStateStore() override;
    TDurableIdempotentClientStateStore(const TDurableIdempotentClientStateStore&) = delete;
    TDurableIdempotentClientStateStore& operator=(const TDurableIdempotentClientStateStore&) = delete;

    std::optional<TIdempotentClientSessionState> Load() override;
    void Save(const TIdempotentClientSessionState& state) override;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

// One in-flight write per session. BeginWrite durably records an operation
// before returning its request. Repeating the same payload returns the same
// operation identity; a different payload is rejected until success is
// acknowledged. The caller supplies a unique session ID and a durable store.
// This object is intended for serialized, single-threaded use.
class TIdempotentClientSession {
public:
    explicit TIdempotentClientSession(std::shared_ptr<IIdempotentClientStateStore> store);

    void StartSession(uint64_t epoch, uint64_t sessionIdHigh, uint64_t sessionIdLow);
    TMessageHolder<TIdempotentCommandRequest> BeginWrite(std::string_view payload,
        uint32_t cookie = 0);
    TMessageHolder<TIdempotentCommandRequest> RetryPending(uint32_t cookie = 0) const;

    // Returns true only when a matching successful response was durably
    // acknowledged and the next sequence number was advanced. Errors and
    // unrelated/late responses leave the pending operation intact.
    bool Complete(const TMessageHolder<TIdempotentCommandResponse>& response);

    const TIdempotentClientSessionState& State() const { return Session; }

private:
    TMessageHolder<TIdempotentCommandRequest> BuildRequest(
        const TIdempotentClientSessionState& state, uint32_t cookie) const;
    void RequireInitialized() const;

    std::shared_ptr<IIdempotentClientStateStore> Store;
    TIdempotentClientSessionState Session;
};
