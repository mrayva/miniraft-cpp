#pragma once
#include <chrono>
#include <concepts>
#include <memory>
#include <vector>

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <typeinfo>
#include <stdexcept>

struct TMessageLimits {
    uint32_t MaxFrameBytes = 16 * 1024 * 1024;
    uint64_t MaxBatchBytes = 64 * 1024 * 1024;
    uint32_t MaxEntries = 4096;
};

struct TMessageError : std::runtime_error { using std::runtime_error::runtime_error; };
void ValidateMessageLimits(const TMessageLimits& limits);
void ValidateMessageHeader(uint32_t type, uint32_t length, const TMessageLimits& limits = {});

enum class EMessageType : uint32_t {
    NONE = 0,
    LOG_ENTRY = 1,
    REQUEST_VOTE_REQUEST = 2,
    REQUEST_VOTE_RESPONSE = 3,
    APPEND_ENTRIES_REQUEST = 4,
    APPEND_ENTRIES_RESPONSE = 5,
    INSTALL_SNAPSHOT_REQUEST = 6,
    INSTALL_SNAPSHOT_RESPONSE = 7,
    COMMAND_REQUEST = 8,
    COMMAND_RESPONSE = 9,
    TIMEOUT_NOW_REQUEST = 10,
    CONFIGURATION_BARRIER_REQUEST = 11,
    CONFIGURATION_BARRIER_RESPONSE = 12,
    PRE_VOTE_REQUEST = 13,
    PRE_VOTE_RESPONSE = 14,
    FORWARDED_COMMAND_REQUEST = 15,
    FORWARDED_COMMAND_RESPONSE = 16,
    PEER_CAPABILITIES_REQUEST = 17,
    PEER_CAPABILITIES_RESPONSE = 18,
    SESSION_FORWARDED_COMMAND_REQUEST = 19,
    SESSION_FORWARDED_COMMAND_RESPONSE = 20,
    IDEMPOTENT_COMMAND_REQUEST = 21,
    IDEMPOTENT_COMMAND_RESPONSE = 22,
};

struct TMessage {
    static constexpr EMessageType MessageType = EMessageType::NONE;
    uint32_t Type;
    uint32_t Len;
    char Value[0];
};

static_assert(sizeof(TMessage) == 8);

struct TLogEntry: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::LOG_ENTRY;
    enum EFlags : uint64_t {
        ENone = 0,
        EStub = 1,
        // Reserved for versioned idempotent-operation log records.
        EIdempotent = uint64_t(1) << 1,
        // Marks entries consumed by the guarded live membership protocol.
        EConfiguration = uint64_t(1) << 63,
    };
    uint64_t Term = 1;
    uint64_t Flags = 0;
    char Data[0];
};

struct TMessageEx: public TMessage {
    uint32_t Src = 0;
    uint32_t Dst = 0;
    uint64_t Term = 0;
    uint64_t Seqno = 0;
};

static_assert(sizeof(TMessageEx) == sizeof(TMessage)+24);

struct TRequestVoteRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::REQUEST_VOTE_REQUEST;
    uint64_t LastLogIndex;
    uint64_t LastLogTerm;
    uint32_t CandidateId;
    uint32_t Padding = 0;
};

static_assert(sizeof(TRequestVoteRequest) == sizeof(TMessageEx)+24);

struct TRequestVoteResponse: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::REQUEST_VOTE_RESPONSE;
    uint32_t VoteGranted;
    uint32_t Padding = 0;
};

static_assert(sizeof(TRequestVoteResponse) == sizeof(TMessageEx)+8);

struct TPreVoteRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::PRE_VOTE_REQUEST;
    uint64_t ProspectiveTerm;
    uint64_t LastLogIndex;
    uint64_t LastLogTerm;
    uint32_t CandidateId;
    uint32_t Padding = 0;
};

static_assert(sizeof(TPreVoteRequest) == sizeof(TMessageEx)+32);

struct TPreVoteResponse: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::PRE_VOTE_RESPONSE;
    uint64_t ProspectiveTerm;
    uint32_t VoteGranted;
    uint32_t Padding = 0;
};

static_assert(sizeof(TPreVoteResponse) == sizeof(TMessageEx)+16);

struct TAppendEntriesRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::APPEND_ENTRIES_REQUEST;
    uint64_t PrevLogIndex = 0;
    uint64_t PrevLogTerm = 0;
    uint64_t LeaderCommit = 0;
    uint32_t LeaderId = 0;
    uint32_t Nentries = 0;
};

static_assert(sizeof(TAppendEntriesRequest) == sizeof(TMessageEx) + 32);

struct TAppendEntriesResponse: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::APPEND_ENTRIES_RESPONSE;
    static constexpr uint32_t SupportsConfigurationBarrier = 1u << 0;
    static constexpr uint32_t SupportsSnapshotHistory1024 = 1u << 1;
    static constexpr uint32_t SupportsSnapshotHistory256 = 1u << 2;
    static constexpr uint32_t SupportsInstallSnapshot = 1u << 3;
    static constexpr uint32_t SupportsSnapshotHistory32 = 1u << 4;
    static constexpr uint32_t SupportsSnapshotLineage = 1u << 5;
    static constexpr uint32_t SupportsJointConsensus = 1u << 6;
    // Success: matched index. Failure: zero or a compacted-boundary probe hint;
    // a hint never advances MatchIndex or confirms a read.
    uint64_t MatchIndex;
    uint32_t Success;
    // Feature advertisement bits. Older binaries leave reserved bits zero;
    // receivers ignore feature bits they do not recognize.
    uint32_t Padding = 0;
};

static_assert(sizeof(TAppendEntriesResponse) == sizeof(TMessageEx) + 16);

struct TInstallSnapshotRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::INSTALL_SNAPSHOT_REQUEST;
    static constexpr uint32_t MaxChunkBytes = 64 * 1024;
    uint64_t SnapshotIndex = 0, SnapshotTerm = 0, TotalBytes = 0, Offset = 0;
    uint32_t LeaderId = 0, Done = 0;
    char Data[0];
};
static_assert(sizeof(TInstallSnapshotRequest) == sizeof(TMessageEx) + 40);
struct TInstallSnapshotResponse: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::INSTALL_SNAPSHOT_RESPONSE;
    uint64_t SnapshotIndex = 0, NextOffset = 0;
    uint32_t Success = 0, Installed = 0;
};
static_assert(sizeof(TInstallSnapshotResponse) == sizeof(TMessageEx) + 24);

// Ask a caught-up voter in the committed new set of a joint configuration to
// start an election immediately so a leader excluded from that set can step
// down before the stable successor is proposed.
struct TTimeoutNowRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::TIMEOUT_NOW_REQUEST;
    uint32_t LeaderId = 0, TargetId = 0;
    uint64_t LastLogIndex = 0, LastLogTerm = 0;
    uint64_t CommitIndex = 0, CommitTerm = 0;
    uint64_t ConfigurationIndex = 0, ConfigurationTerm = 0;
};
static_assert(sizeof(TTimeoutNowRequest) == sizeof(TMessageEx) + 56);

// Leader certificate that every active voter acknowledged a committed
// configuration boundary. Followers use it to compact safely without
// inferring peer progress from their own local commit index.
struct TConfigurationBarrierRequest: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::CONFIGURATION_BARRIER_REQUEST;
    uint32_t LeaderId = 0, Padding = 0;
    uint64_t ConfigurationIndex = 0, ConfigurationTerm = 0;
};
static_assert(sizeof(TConfigurationBarrierRequest) == sizeof(TMessageEx) + 24);

struct TConfigurationBarrierResponse: public TMessageEx {
    static constexpr EMessageType MessageType = EMessageType::CONFIGURATION_BARRIER_RESPONSE;
    uint64_t ConfigurationIndex = 0, ConfigurationTerm = 0;
    uint32_t Accepted = 0, Padding = 0;
};
static_assert(sizeof(TConfigurationBarrierResponse) == sizeof(TMessageEx) + 24);

struct TCommandRequest: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::COMMAND_REQUEST;
    enum EFlags {
        ENone = 0,
        EWrite = 1, //

        // read semantics, default: read from leader w/o ping check, possible stale reads if there are 2 leaders
        EStale = 2, // stale read, can read from follower
        EConsistent = 4, // strong consistent read (wait for pings, low latency, no stale read)
        EForwarded = 8, // internal one-hop marker, independent of client cookies
    };
    uint32_t Flags = ENone;
    uint32_t Cookie = 0;
    char Data[0];
};

static_assert(sizeof(TCommandRequest) == sizeof(TMessage) + 8);

// Opt-in request envelope for durable write deduplication. It remains separate
// from TCommandRequest so legacy payloads and at-least-once semantics do not
// change. Until a node has the durable RSM contract, it must reject this type.
struct TIdempotentCommandRequest: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::IDEMPOTENT_COMMAND_REQUEST;
    static constexpr uint32_t CurrentVersion = 1;
    uint32_t Version = CurrentVersion, Reserved = 0;
    uint64_t Epoch = 0;
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    uint64_t Sequence = 0;
    uint32_t Flags = TCommandRequest::EWrite;
    uint32_t Cookie = 0;
    char Data[0];
};
static_assert(sizeof(TIdempotentCommandRequest) == sizeof(TMessage) + 48);

struct TCommandResponse: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::COMMAND_RESPONSE;
    enum EErrorCode : uint32_t {
        Success = 0,
        Failed = 1,
        ApplicationReadFailed = 2,
        Overloaded = 3,
        TimedOut = 4,
        Unsupported = 5,
        // Deterministic idempotent-write protocol outcomes.
        IdempotencyConflict = 6,
        SequenceGap = 7,
        StaleEpoch = 8,
        SessionClosed = 9,
        AlreadyCompleted = 10,
    };
    uint64_t Index = 0;
    uint32_t Cookie = 0;
    uint32_t ErrorCode = 0;
    char Data[0];
};

static_assert(sizeof(TCommandResponse) == sizeof(TMessage) + 16);

struct TIdempotentCommandResponse: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::IDEMPOTENT_COMMAND_RESPONSE;
    uint32_t Version = TIdempotentCommandRequest::CurrentVersion, Reserved = 0;
    uint64_t Index = 0;
    uint64_t Epoch = 0;
    uint64_t SessionIdHigh = 0, SessionIdLow = 0;
    uint64_t Sequence = 0;
    uint32_t Cookie = 0;
    uint32_t ErrorCode = 0;
    char Data[0];
};
static_assert(sizeof(TIdempotentCommandResponse) == sizeof(TMessage) + 56);

// Internal forwarding envelope used after the legacy 32-bit command cookie
// range is exhausted. Its opaque payload is the original TCommandRequest data.
struct TForwardedCommandRequest: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::FORWARDED_COMMAND_REQUEST;
    uint64_t ForwardCookie = 0;
    uint32_t Flags = TCommandRequest::ENone;
    uint32_t ClientCookie = 0;
    char Data[0];
};
static_assert(sizeof(TForwardedCommandRequest) == sizeof(TMessage) + 16);

// Response fields are carried directly, replacing the 32-bit client cookie
// slot with the 64-bit forwarding correlation while preserving the response
// index, application error and data.
struct TForwardedCommandResponse: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::FORWARDED_COMMAND_RESPONSE;
    uint64_t ForwardCookie = 0;
    uint64_t Index = 0;
    uint32_t ClientCookie = 0;
    uint32_t ErrorCode = 0;
    char Data[0];
};
static_assert(sizeof(TForwardedCommandResponse) == sizeof(TMessage) + 24);

// Sessionized forwarding prevents a delayed reply from an earlier processor
// instance matching a reused 64-bit correlation ID on a live peer connection.
struct TSessionForwardedCommandRequest: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::SESSION_FORWARDED_COMMAND_REQUEST;
    uint64_t SessionId = 0;
    uint64_t ForwardCookie = 0;
    uint32_t Flags = TCommandRequest::ENone;
    uint32_t ClientCookie = 0;
    char Data[0];
};
static_assert(sizeof(TSessionForwardedCommandRequest) == sizeof(TMessage) + 24);

struct TSessionForwardedCommandResponse: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::SESSION_FORWARDED_COMMAND_RESPONSE;
    uint64_t SessionId = 0;
    uint64_t ForwardCookie = 0;
    uint64_t Index = 0;
    uint32_t ClientCookie = 0;
    uint32_t ErrorCode = 0;
    char Data[0];
};
static_assert(sizeof(TSessionForwardedCommandResponse) == sizeof(TMessage) + 32);

// Connection-level compatibility probe. Unknown message types are rejected by
// older nodes, which close the probe connection and allow a legacy reconnect.
struct TPeerCapabilitiesRequest: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::PEER_CAPABILITIES_REQUEST;
    static constexpr uint32_t CurrentVersion = 1;
    static constexpr uint64_t ExtendedForwarding = uint64_t(1) << 0;
    static constexpr uint64_t SessionizedForwarding = uint64_t(1) << 1;
    uint32_t MinVersion = CurrentVersion;
    uint32_t MaxVersion = CurrentVersion;
    uint64_t Features = 0;
    uint64_t ConnectionId = 0;
};
static_assert(sizeof(TPeerCapabilitiesRequest) == sizeof(TMessage) + 24);

struct TPeerCapabilitiesResponse: public TMessage {
    static constexpr EMessageType MessageType = EMessageType::PEER_CAPABILITIES_RESPONSE;
    uint32_t Version = TPeerCapabilitiesRequest::CurrentVersion;
    uint32_t Reserved = 0;
    uint64_t Features = 0;
    uint64_t ConnectionId = 0;
};
static_assert(sizeof(TPeerCapabilitiesResponse) == sizeof(TMessage) + 24);

struct TTimeout {
    static constexpr std::chrono::milliseconds Election = std::chrono::milliseconds(5000);
    static constexpr std::chrono::milliseconds Heartbeat = std::chrono::milliseconds(1000);
    // Yield stale leaders after twenty heartbeat intervals without a fresh
    // quorum of voter responses.
    static constexpr std::chrono::milliseconds LeadershipExpiration = Heartbeat * 20;
    static constexpr std::chrono::milliseconds LeadershipTransferRetry = std::chrono::milliseconds(1000);
    static constexpr std::chrono::milliseconds Rpc = std::chrono::milliseconds(10000);
    // Keep snapshot transfers across normal RPC retries, but expire sender pins
    // and receiver buffers after this much time without forward progress.
    static constexpr std::chrono::milliseconds SnapshotTransferIdle = std::chrono::seconds(30);
    // Re-advertise configuration-compaction certificates so a restarted
    // follower can reconstruct its volatile certificate without a new term.
    static constexpr std::chrono::milliseconds ConfigurationBarrierRefresh = std::chrono::seconds(30);
};

template<typename T>
requires std::derived_from<T, TMessage>
struct TMessageHolder {
    T* Mes;
    std::shared_ptr<char[]> RawData;
    size_t Capacity = 0; // Allocated bytes accessible starting at Mes.

    uint32_t PayloadSize = 0;
    std::shared_ptr<TMessageHolder<TMessage>[]> Payload;
    size_t PayloadCapacity = 0;

    TMessageHolder()
        : Mes(nullptr)
    { }

    template<typename U>
    requires std::derived_from<U, T>
    TMessageHolder(U* u,
        const std::shared_ptr<char[]>& rawData,
        uint32_t payloadSize = 0,
        const std::shared_ptr<TMessageHolder<TMessage>[]>& payload = {}, size_t capacity = 0,
        size_t payloadCapacity = 0)
        : Mes(u)
        , RawData(rawData)
        , Capacity(capacity)
        , PayloadSize(payloadSize)
        , Payload(payload)
        , PayloadCapacity(payloadCapacity)
    { }

    template<typename U>
    requires std::derived_from<U, T>
    TMessageHolder(const TMessageHolder<U>& other)
        : Mes(other.Mes)
        , RawData(other.RawData)
        , Capacity(other.Capacity)
        , PayloadSize(other.PayloadSize)
        , Payload(other.Payload)
        , PayloadCapacity(other.PayloadCapacity)
    { }

    void InitPayload(uint32_t size) {
        InitPayload(size, {});
    }

    void InitPayload(uint32_t size, std::shared_ptr<void> lifetime) {
        PayloadSize = size;
        auto* payload = new TMessageHolder<TMessage>[size];
        if (lifetime) {
            Payload = std::shared_ptr<TMessageHolder<TMessage>[]>(payload,
                [lifetime = std::move(lifetime)](TMessageHolder<TMessage>* value) {
                    (void)lifetime;
                    delete[] value;
                });
        } else {
            Payload = std::shared_ptr<TMessageHolder<TMessage>[]>(payload);
        }
        PayloadCapacity = size;
    }

    T* operator->() {
        return Mes;
    }

    const T* operator->() const {
        return Mes;
    }

    operator bool() const {
        return !!Mes;
    }

    bool HasValidExtent() const {
        return Mes && RawData && Capacity >= sizeof(TMessage) &&
            Mes->Len >= sizeof(TMessage) && Mes->Len <= Capacity;
    }

    bool IsEx() const {
        return HasValidExtent() && Mes->Len >= sizeof(TMessageEx) &&
            ((2 <= Mes->Type && Mes->Type <= 7) ||
             (10 <= Mes->Type && Mes->Type <= 14));
    }

    template<typename U>
    requires std::derived_from<U, T>
    TMessageHolder<U> Cast() {
        if (!HasValidExtent() || Mes->Len < sizeof(U) ||
            (U::MessageType != EMessageType::NONE && Mes->Type != uint32_t(U::MessageType)))
            throw std::bad_cast();
        return TMessageHolder<U>(static_cast<U*>(Mes), RawData, PayloadSize, Payload, Capacity, PayloadCapacity);
    }

    template<typename U>
    requires std::derived_from<U, T>
    auto Maybe() {
        struct Maybe {
            TMessageHolder<TMessage> Mes;

            operator bool() const {
                return Mes;
            }

            TMessageHolder<U> Cast() {
                if (*this) {
                    return Mes.Cast<U>();
                }
                throw std::bad_cast();
            }
        };

        return HasValidExtent() && Mes->Len >= sizeof(U) && Mes->Type == static_cast<uint32_t>(U::MessageType)
            ? Maybe { *this }
            : Maybe { };
    }
};

// Validate the complete frame/batch before emitting any bytes.
void ValidateMessageTree(const TMessageHolder<TMessage>& message, const TMessageLimits& limits = {});
void ValidateIdempotentCommandRequest(
    const TMessageHolder<TIdempotentCommandRequest>& message,
    const TMessageLimits& limits = {});
void ValidateIdempotentCommandResponse(
    const TMessageHolder<TIdempotentCommandResponse>& message,
    const TMessageLimits& limits = {});

template<typename T>
requires std::derived_from<T, TMessage>
T* NewMessage(uint32_t type, size_t len) {
    if (len < sizeof(T) || len > UINT32_MAX) throw std::invalid_argument("Invalid message allocation size");
    char* data = new char[len];
    T* mes =  new (data) T;
    mes->Type = type;
    mes->Len = len;
    return mes;
}

template<typename T>
requires std::derived_from<T, TMessage>
TMessageHolder<T> NewHoldedMessage(uint32_t type, size_t len)
{
    T* mes = NewMessage<T>(type, len);
    return TMessageHolder<T>(mes, std::shared_ptr<char[]>(reinterpret_cast<char*>(mes)), 0, {}, len);
}

template<typename T>
TMessageHolder<T> NewHoldedMessage() {
    return NewHoldedMessage<T>(static_cast<uint32_t>(T::MessageType), sizeof(T));
}

template<typename T>
TMessageHolder<T> NewHoldedMessage(size_t size) {
    return NewHoldedMessage<T>(static_cast<uint32_t>(T::MessageType), size);
}

template<typename T>
TMessageHolder<T> NewHoldedMessage(T t) {
    auto m = NewHoldedMessage<T>();
    memcpy((char*)m.Mes + sizeof(TMessage), (char*)&t + sizeof(TMessage), sizeof(T) - sizeof(TMessage));
    return m;
}

template<typename T>
requires std::derived_from<T, TMessageEx>
TMessageHolder<T> NewHoldedMessage(TMessageEx h, T t) {
    auto m = NewHoldedMessage<T>();
    memcpy((char*)m.Mes + sizeof(TMessage), (char*)&h + sizeof(TMessage), sizeof(h) - sizeof(TMessage));
    memcpy((char*)m.Mes + sizeof(TMessageEx), (char*)&t + sizeof(TMessageEx), sizeof(T) - sizeof(TMessageEx));
    return m;
}
