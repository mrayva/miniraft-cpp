#pragma once

#include <exception>
#include <cstddef>
#include <cstdint>
#include <chrono>
#include <memory>
#include <mutex>
#include <coroutine>
#include <stdexcept>
#include <string>
#include <string_view>
#include <charconv>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <coroio/all.hpp>
#include <miniraft/net/retirable_ssl_socket.h>

#include <miniraft/timesource.h>
#include <miniraft/messages.h>
#include <miniraft/raft.h>

namespace NNet::TDetail {
// Every registered peer is a replication target, including staged learners
// whose AppendEntries replies provide the catch-up proof needed for promotion.
// The started set prevents duplicate readers until that endpoint is removed.
template<class TPeerMap, class TStarted, class TStartReader>
void ReconcilePeerResponseReaders(const TPeerMap& peers,
        TStarted& started, TStartReader startReader) {
    for (const auto& [id, peer] : peers)
        if (started.insert(id).second) startReader(id, peer);
}

template<class TConnections, class TClose>
void ExpireIdleConnections(TConnections& connections, ITimeSource::Time now,
        std::chrono::milliseconds idleTimeout, TClose close) {
    for (auto it = connections.begin(); it != connections.end();) {
        if (now < it->second || now - it->second < idleTimeout || !close(it->first))
            ++it;
        else
            it = connections.erase(it);
    }
}
}

class TNodeQueueFull : public std::runtime_error {
public:
    TNodeQueueFull() : std::runtime_error("Outbound node queue limit exceeded") {}
};

struct TOutboundQueueLimits {
    size_t MaxMessages = 4096;
    uint64_t MaxBytes = 256ULL * 1024 * 1024;
};

// Shared by a server's peer nodes so per-peer limits cannot multiply without
// bound as the cluster grows. A replacement reserves its new usage atomically
// with releasing the superseded message usage.
class TOutboundQueueBudget {
public:
    explicit TOutboundQueueBudget(TOutboundQueueLimits limits = {})
        : Limits(limits) { }

    bool TryReplace(size_t oldMessages, uint64_t oldBytes,
            size_t newMessages, uint64_t newBytes) {
        std::lock_guard lock(Mutex);
        if (oldMessages > UsedMessages || oldBytes > UsedBytes)
            throw std::logic_error("Aggregate outbound queue accounting underflow");
        const auto retainedMessages = UsedMessages - oldMessages;
        const auto retainedBytes = UsedBytes - oldBytes;
        if (retainedMessages > Limits.MaxMessages || retainedBytes > Limits.MaxBytes ||
            newMessages > Limits.MaxMessages - retainedMessages ||
            newBytes > Limits.MaxBytes - retainedBytes)
            return false;
        UsedMessages = retainedMessages + newMessages;
        UsedBytes = retainedBytes + newBytes;
        return true;
    }

    void Release(size_t messages, uint64_t bytes) noexcept {
        std::lock_guard lock(Mutex);
        if (messages > UsedMessages || bytes > UsedBytes) std::terminate();
        UsedMessages -= messages;
        UsedBytes -= bytes;
    }

private:
    const TOutboundQueueLimits Limits;
    std::mutex Mutex;
    size_t UsedMessages = 0;
    uint64_t UsedBytes = 0;
};

struct TInboundMessageBudgetLimits {
    uint64_t MaxBytes = 256ULL * 1024 * 1024;
    size_t MaxConnections = 1024;
    std::chrono::milliseconds ConnectionIdleTimeout = std::chrono::seconds(60);
};

// Bounds serialized frame storage and AppendEntries payload-holder arrays while
// the decoded messages remain alive. This is a network-buffer quota, not a
// process-wide allocator limit.
class TInboundMessageBudget : public std::enable_shared_from_this<TInboundMessageBudget> {
    class TConnectionReservation {
    public:
        explicit TConnectionReservation(std::shared_ptr<TInboundMessageBudget> budget)
            : Budget(std::move(budget)) { }
        ~TConnectionReservation() { Budget->ReleaseConnection(); }
    private:
        std::shared_ptr<TInboundMessageBudget> Budget;
    };

    class TReservation {
    public:
        TReservation(std::shared_ptr<TInboundMessageBudget> budget, uint64_t bytes)
            : Budget(std::move(budget)), Bytes(bytes) { }
        ~TReservation() { Budget->Release(Bytes); }
    private:
        std::shared_ptr<TInboundMessageBudget> Budget;
        uint64_t Bytes;
    };

public:
    explicit TInboundMessageBudget(TInboundMessageBudgetLimits limits = {})
        : Limits(limits) {
        if (Limits.ConnectionIdleTimeout.count() < 0)
            throw std::invalid_argument("Inbound connection idle timeout cannot be negative");
    }

    std::shared_ptr<void> TryReserveConnection() {
        {
            std::lock_guard lock(Mutex);
            if (ActiveConnections >= Limits.MaxConnections) return {};
            ++ActiveConnections;
        }
        try {
            return std::make_shared<TConnectionReservation>(shared_from_this());
        } catch (...) {
            ReleaseConnection();
            throw;
        }
    }

    std::chrono::milliseconds ConnectionIdleTimeout() const {
        return Limits.ConnectionIdleTimeout;
    }

    std::shared_ptr<void> TryReserve(uint64_t bytes) {
        if (!bytes) return {};
        {
            std::lock_guard lock(Mutex);
            if (UsedBytes > Limits.MaxBytes || bytes > Limits.MaxBytes - UsedBytes)
                return {};
            UsedBytes += bytes;
        }
        try {
            return std::make_shared<TReservation>(shared_from_this(), bytes);
        } catch (...) {
            Release(bytes);
            throw;
        }
    }

private:
    void ReleaseConnection() noexcept {
        std::lock_guard lock(Mutex);
        if (!ActiveConnections) std::terminate();
        --ActiveConnections;
    }

    void Release(uint64_t bytes) noexcept {
        std::lock_guard lock(Mutex);
        if (bytes > UsedBytes) std::terminate();
        UsedBytes -= bytes;
    }

    const TInboundMessageBudgetLimits Limits;
    std::mutex Mutex;
    uint64_t UsedBytes = 0;
    size_t ActiveConnections = 0;
};

template<typename TSocket>
class TMessageReader {
public:
    TMessageReader(TSocket& socket, TMessageLimits limits = {},
            std::shared_ptr<TInboundMessageBudget> budget = {})
        : Socket(socket), Limits(limits), Budget(std::move(budget)) { ValidateMessageLimits(Limits); }

    NNet::TValueTask<TMessageHolder<TMessage>> Read() {
        if (Failed) throw TMessageError("Cannot reuse a failed frame reader");
        try {
            uint64_t remaining = Limits.MaxBatchBytes;
            co_return co_await ReadFrame(false, remaining);
        } catch (...) {
            Failed = true;
            throw;
        }
    }

private:
    NNet::TValueTask<TMessageHolder<TMessage>> ReadFrame(bool logOnly, uint64_t& remaining) {
        TMessage header;
        co_await NNet::TByteReader(Socket).Read(&header, sizeof(header));
        ValidateMessageHeader(header.Type, header.Len, Limits);
        if (logOnly && header.Type != uint32_t(EMessageType::LOG_ENTRY))
            throw TMessageError("AppendEntries children must be log entries");
        if (header.Len > remaining) throw TMessageError("Message batch exceeds byte limit");
        remaining -= header.Len;
        auto frameReservation = Budget ? Budget->TryReserve(header.Len) : std::shared_ptr<void>{};
        if (Budget && !frameReservation)
            throw TMessageError("Aggregate inbound message byte limit exceeded");
        TMessageHolder<TMessage> message;
        if (Budget) {
            auto* rawMessage = NewMessage<TMessage>(header.Type, header.Len);
            auto rawData = std::shared_ptr<char[]>(reinterpret_cast<char*>(rawMessage),
                [reservation = std::move(frameReservation)](char* data) {
                    (void)reservation;
                    delete[] data;
                });
            message = TMessageHolder<TMessage>(rawMessage, rawData, 0, {}, header.Len);
        } else {
            message = NewHoldedMessage<TMessage>(header.Type, header.Len);
        }
        co_await NNet::TByteReader(Socket).Read(message->Value, header.Len - sizeof(TMessage));
        if (header.Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
            const auto count = message.template Cast<TAppendEntriesRequest>()->Nentries;
            if (count > Limits.MaxEntries || uint64_t(count) * sizeof(TLogEntry) > remaining)
                throw TMessageError("AppendEntries count exceeds limits");
            const auto payloadBytes = uint64_t(count) * sizeof(TMessageHolder<TMessage>);
            auto payloadReservation = Budget && payloadBytes ? Budget->TryReserve(payloadBytes) : std::shared_ptr<void>{};
            if (Budget && payloadBytes && !payloadReservation)
                throw TMessageError("Aggregate inbound message byte limit exceeded");
            message.InitPayload(count, std::move(payloadReservation));
            for (uint32_t i = 0; i < count; ++i)
                message.Payload[i] = co_await ReadFrame(true, remaining);
        }
        co_return message;
    }
    TSocket& Socket;
    TMessageLimits Limits;
    std::shared_ptr<TInboundMessageBudget> Budget;
    bool Failed = false;
};

template<typename TSocket>
class TMessageWriter {
public:
    TMessageWriter(TSocket& socket, TMessageLimits limits = {})
        : Socket(socket), Limits(limits) { ValidateMessageLimits(Limits); }

    NNet::TValueTask<void> Write(TMessageHolder<TMessage> message) {
        ValidateMessageTree(message, Limits);
        co_await NNet::TByteWriter(Socket).Write(message.Mes, message->Len);
        for (uint32_t i = 0; i < message.PayloadSize; ++i)
            co_await Write(message.Payload[i]);
    }
private:
    TSocket& Socket;
    TMessageLimits Limits;
};

struct THost {
    std::string Address;
    int Port = 0;
    uint32_t Id = 0;

    THost() { }

    operator bool() const {
        return !!Id;
    }

    THost(const std::string& str) {
        const std::string_view input(str);
        size_t addressBegin = 0;
        size_t addressEnd = 0;
        size_t portBegin = 0;
        size_t portEnd = 0;
        size_t idBegin = 0;

        if (!input.empty() && input.front() == '[') {
            const auto close = input.find(']');
            if (close == std::string_view::npos || close == 1 || close + 1 >= input.size() ||
                input[close + 1] != ':')
                throw std::invalid_argument("Invalid bracketed node address");
            addressBegin = 1;
            addressEnd = close;
            portBegin = close + 2;
            portEnd = input.find(':', portBegin);
            if (portEnd == std::string_view::npos || input.find(':', portEnd + 1) != std::string_view::npos)
                throw std::invalid_argument("Node endpoint must be address:port:id");
            idBegin = portEnd + 1;
        } else {
            addressEnd = input.find(':');
            if (addressEnd == std::string_view::npos || addressEnd == 0)
                throw std::invalid_argument("Node endpoint must be address:port:id");
            portBegin = addressEnd + 1;
            portEnd = input.find(':', portBegin);
            if (portEnd == std::string_view::npos || input.find(':', portEnd + 1) != std::string_view::npos)
                throw std::invalid_argument("Node endpoint must be address:port:id");
            idBegin = portEnd + 1;
        }

        const auto address = input.substr(addressBegin, addressEnd - addressBegin);
        const auto port = input.substr(portBegin, portEnd - portBegin);
        const auto id = input.substr(idBegin);
        uint32_t parsedPort = 0;
        const auto parsedPortResult = std::from_chars(port.data(), port.data() + port.size(), parsedPort);
        const auto parsedIdResult = std::from_chars(id.data(), id.data() + id.size(), Id);
        if (address.empty() || parsedPortResult.ec != std::errc{} ||
            parsedPortResult.ptr != port.data() + port.size() || parsedPort == 0 || parsedPort > 65535 ||
            parsedIdResult.ec != std::errc{} || parsedIdResult.ptr != id.data() + id.size() || !Id)
            throw std::invalid_argument("Invalid node address, port, or ID");
        Address.assign(address);
        Port = static_cast<int>(parsedPort);
    }

    void DebugPrint() const {
        std::cout << "Addr: '" << Address << "'\n";
        std::cout << "Port: " << Port << "\n";
        std::cout << "Id: " << Id << "\n";
    }
};

template<typename TSocket>
class TNode: public INode {
public:
    static constexpr size_t MaxQueuedDataMessages = 128;
    static constexpr uint64_t MaxQueuedDataBytes = 16 * 1024 * 1024;
    // Four pending control messages; queued retries are coalesced by traffic
    // class. AppendEntries and snapshot chunks share the replication class.
    static constexpr size_t MaxQueuedRaftMessages = 4;
    static constexpr uint64_t MaxQueuedRaftBytes = 64 * 1024 * 1024 + 4096;

    TNode(const std::function<TSocket(const NNet::TAddress&)> factory, const std::string& name, NNet::TAddress address, const std::shared_ptr<ITimeSource>& ts)
        : Name(name)
        , Address(address)
        , TimeSource(ts)
        , SocketFactory(factory)
    { }

    TNode(const std::string& name, TSocket socket, const std::shared_ptr<ITimeSource>& ts)
        : Name(name)
        , Socket(std::move(socket))
        , Connected(true)
        , TimeSource(ts)
    { }

    ~TNode() {
        // Completed suspended tasks own their coroutine frames until destroyed.
        // The server normally reaps them from Drain(), but standalone nodes can
        // be destroyed after their final drain without another poller turn.
        if (Drainer && Drainer.done()) { Drainer.destroy(); Drainer = {}; }
        if (Connector && Connector.done()) { Connector.destroy(); Connector = {}; }
        if (auto budget = QueueBudget.lock())
            budget->Release(PendingMessageCount(), PendingMessageBytes());
    }

    void Send(TMessageHolder<TMessage> message) override;
    bool TrySend(TMessageHolder<TMessage> message) override;
    void Drain() override;
    // Called after a failed outbound read, once its reader has unwound. A stale
    // reader cannot disconnect a newer connection or its pending connector.
    void Disconnect() { Disconnect(ConnectionGeneration); }
    void Disconnect(uint64_t generation);
    void Retire();
    // Returns true only when this call attached the budget. Reattaching the
    // same budget is harmless; switching budgets requires a prior detach.
    bool AttachQueueBudget(const std::shared_ptr<TOutboundQueueBudget>& budget) {
        if (!budget) throw std::invalid_argument("Outbound queue budget is null");
        const auto attachedBudget = QueueBudget.lock();
        if (attachedBudget == budget) return false;
        if (attachedBudget) throw std::logic_error("Node already has an outbound queue budget");
        if (!budget->TryReplace(0, 0, PendingMessageCount(), PendingMessageBytes()))
            throw TNodeQueueFull{};
        QueueBudget = budget;
        return true;
    }
    void DetachQueueBudget(const std::shared_ptr<TOutboundQueueBudget>& budget) noexcept {
        const auto attachedBudget = QueueBudget.lock();
        if (!budget || attachedBudget != budget) return;
        attachedBudget->Release(PendingMessageCount(), PendingMessageBytes());
        QueueBudget.reset();
    }
    bool SupportsRetirement() const {
        if constexpr (requires(TSocket& socket) { socket.SupportsRetirement(); }) {
            if (Socket.SupportsRetirement()) return true;
        } else if constexpr (requires(TSocket& socket) { { socket.Shutdown() } noexcept; }) {
            return true;
        } else if constexpr (requires(TSocket& socket) { socket.Fd(); }) {
            if (Socket.Fd() >= 0) return true;
        }
        // A disconnected endpoint with no live writer or connector has no
        // socket operation to interrupt; an idle response reader will exit
        // when its retry timer observes Retired.
        return !Connected && (!Drainer || Drainer.done()) && (!Connector || Connector.done());
    }
    bool IsRetired() const { return Retired; }
    bool HasResponseReader() const { return ResponseReaderActive; }
    void MarkResponseReaderStarted() { ResponseReaderActive = true; }
    void MarkResponseReaderStopped() { ResponseReaderActive = false; }
    bool RetirementComplete() const { return Retired && RetiredCleaned; }
    uint64_t GetConnectionGeneration() const { return ConnectionGeneration; }
    bool IsConnected() { return Connected; }
    bool SupportsExtendedForwarding() const override {
        return Connected && !CapabilityNegotiating &&
            (PeerFeatures & TPeerCapabilitiesRequest::ExtendedForwarding);
    }
    bool SupportsSessionizedForwarding() const override {
        return Connected && !CapabilityNegotiating &&
            (PeerFeatures & TPeerCapabilitiesRequest::SessionizedForwarding);
    }
    bool IsCapabilityNegotiating() const { return CapabilityNegotiating; }
    bool CompleteCapabilityNegotiation(
            const TMessageHolder<TPeerCapabilitiesResponse>& response, uint64_t generation) {
        if (!CapabilityNegotiating || generation != ConnectionGeneration ||
            response->ConnectionId != generation || response->Reserved != 0 ||
            response->Version > TPeerCapabilitiesRequest::CurrentVersion ||
            (!response->Version && response->Features) ||
            (response->Features & ~(TPeerCapabilitiesRequest::ExtendedForwarding |
                TPeerCapabilitiesRequest::SessionizedForwarding)))
            return false;
        PeerFeatures = response->Version == TPeerCapabilitiesRequest::CurrentVersion
            ? response->Features : 0;
        CapabilityNegotiating = false;
        DropUnsupportedForwardingMessages();
        return true;
    }
    void FailCapabilityNegotiation(uint64_t generation) {
        if (!CapabilityNegotiating || generation != ConnectionGeneration) return;
        CapabilityNegotiating = false;
        PeerFeatures = 0;
        SkipNextCapabilityProbe = true;
        DiscardForwardedCommands();
    }
    TSocket& Sock() {
        return Socket;
    }

private:
    enum class EQueueClass : uint8_t { Client, Vote, Replication, TimeoutNow, ConfigurationBarrier };
    static EQueueClass QueueClassFor(uint32_t type);
    static bool TryGetMessageTreeBytes(const TMessageHolder<TMessage>& message, uint64_t& bytes);
    size_t PendingMessageCount() const { return PendingDataMessages + PendingRaftMessages; }
    uint64_t PendingMessageBytes() const { return PendingDataBytes + PendingRaftBytes; }
    void RecountPendingMessages();
    void CompletePendingMessage(EQueueClass queueClass, uint64_t bytes);

    void Connect();
    void ShutdownSocket() noexcept;
    void DropUnsupportedForwardingMessages() {
        bool removed = false;
        for (auto it = Messages.begin(); it != Messages.end();) {
            const auto type = EMessageType((*it)->Type);
            const bool unsupportedV1 =
                (type == EMessageType::FORWARDED_COMMAND_REQUEST ||
                 type == EMessageType::FORWARDED_COMMAND_RESPONSE) &&
                !(PeerFeatures & TPeerCapabilitiesRequest::ExtendedForwarding);
            const bool unsupportedSessionized =
                (type == EMessageType::SESSION_FORWARDED_COMMAND_REQUEST ||
                 type == EMessageType::SESSION_FORWARDED_COMMAND_RESPONSE) &&
                !(PeerFeatures & TPeerCapabilitiesRequest::SessionizedForwarding);
            if (unsupportedV1 || unsupportedSessionized) {
                it = Messages.erase(it);
                removed = true;
            } else {
                ++it;
            }
        }
        if (removed) RecountPendingMessages();
    }
    void DiscardForwardedCommands() {
        bool removed = false;
        for (auto it = Messages.begin(); it != Messages.end();) {
            const auto type = EMessageType((*it)->Type);
            const bool forwardedClientRequest = type == EMessageType::COMMAND_REQUEST &&
                (it->Cast<TCommandRequest>()->Flags & TCommandRequest::EForwarded);
            const bool forwardedInternalRequest = type == EMessageType::FORWARDED_COMMAND_REQUEST ||
                type == EMessageType::SESSION_FORWARDED_COMMAND_REQUEST;
            if (forwardedClientRequest || forwardedInternalRequest) {
                it = Messages.erase(it);
                removed = true;
            } else {
                ++it;
            }
        }
        if (removed) RecountPendingMessages();
    }

    NNet::TVoidSuspendedTask DoDrain();
    NNet::TVoidSuspendedTask DoConnect();

    std::string Name;
    std::optional<NNet::TAddress> Address;
    std::shared_ptr<ITimeSource> TimeSource;
    std::function<TSocket(const NNet::TAddress&)> SocketFactory;
    TSocket Socket;
    bool Connected = false;
    bool Retired = false;
    bool CapabilityNegotiating = false;
    bool SkipNextCapabilityProbe = false;
    uint64_t PeerFeatures = 0;
    bool ResponseReaderActive = false;
    bool RetiredCleaned = false;
    uint64_t ConnectionGeneration = 0;

    std::coroutine_handle<> Drainer;
    std::coroutine_handle<> Connector;

    std::vector<TMessageHolder<TMessage>> Messages;
    // Includes messages in Messages and in the active writer batch.
    size_t PendingDataMessages = 0, PendingRaftMessages = 0;
    uint64_t PendingDataBytes = 0, PendingRaftBytes = 0;
    std::weak_ptr<TOutboundQueueBudget> QueueBudget;
};

template<typename TSocket>
class TRaftServer {
public:
    TRaftServer(
        typename TSocket::TPoller& poller,
        TSocket socket,
        const std::shared_ptr<TRaft>& raft,
        const std::shared_ptr<IRsm>& rsm,
        const TNodeDict& nodes,
        const std::shared_ptr<ITimeSource>& ts,
        TRequestProcessor::TAdmissionLimits admissionLimits = {},
        TOutboundQueueLimits outboundQueueLimits = {},
        TInboundMessageBudgetLimits inboundMessageBudgetLimits = {})
        : Poller(poller)
        , Socket(std::move(socket))
        , Raft(raft)
        , OutboundQueueBudget(std::make_shared<TOutboundQueueBudget>(outboundQueueLimits))
        , InboundMessageBudget(std::make_shared<TInboundMessageBudget>(inboundMessageBudgetLimits))
        , RequestProcessor(std::make_shared<TRequestProcessor>(raft, rsm, nodes, admissionLimits, ts))
        , TimeSource(ts)
    {
        try {
            for (const auto& [id, node] : nodes) {
                if (!Nodes.insert(node).second)
                    throw std::invalid_argument("Peer endpoint is registered more than once");
                if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node)) {
                    realNode->AttachQueueBudget(OutboundQueueBudget);
                    PeerNodes.emplace(id, std::move(realNode));
                }
            }
        } catch (...) {
            for (const auto& [_, node] : nodes)
                if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node))
                    realNode->DetachQueueBudget(OutboundQueueBudget);
            throw;
        }
    }

    void Serve();

    // Register local endpoints on any role so every replica can validate and
    // communicate with future voters. This does not change quorum membership.
    // Calls must be serialized with the poller loop and Raft work.
    void RegisterPeerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node);
    // Replace a local transport without changing the peer's Raft role. Active
    // response readers are retired before a reader starts for the new endpoint.
    std::shared_ptr<TNode<TSocket>> ReplacePeerTarget(
        uint32_t id, std::shared_ptr<TNode<TSocket>> node);
    std::shared_ptr<TNode<TSocket>> UnregisterPeerTarget(uint32_t id);
    // Leader-only catch-up staging and endpoint removal wrappers.
    void AddLearnerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node);
    std::shared_ptr<TNode<TSocket>> RemoveLearnerTarget(uint32_t id);
    // Local preflight for a joint voter change. Calls must be serialized with
    // the poller loop and Raft work, like endpoint registration.
    TJointConfigurationReadiness CheckJointConfigurationReadiness(
        const std::vector<uint32_t>& newVoters) const {
        return Raft->CheckJointConfigurationReadiness(newVoters);
    }
    // Start a joint-consensus voter transition after staging added peers with
    // AddLearnerTarget and satisfying the readiness report. FinalizeConfiguration
    // appends the stable successor after the joint entry commits.
    // Calls must be serialized with the poller loop and Raft work.
    uint64_t ProposeJointConfiguration(std::vector<uint32_t> newVoters) {
        return Raft->ProposeJointConfiguration(std::move(newVoters));
    }
    // Append the stable successor after the joint configuration commits and
    // leadership has transferred to a voter on its new side.
    uint64_t FinalizeConfiguration() {
        return Raft->FinalizeConfiguration();
    }
    // Transfer leadership to a voter on the new side of a committed joint
    // configuration. Calls must be serialized with the poller loop and Raft work.
    void TransferLeadership(uint32_t target) {
        if (!TimeSource) throw std::logic_error("Leadership transfer requires a time source");
        Raft->TransferLeadership(target, TimeSource->Now());
    }
    // Calls must be serialized with the poller loop and Raft work.
    TCompactionMembershipReadiness CheckCompactionMembershipReadiness() const {
        return Raft->CheckCompactionMembershipReadiness();
    }

private:
    NNet::TVoidTask InboundServe();
    NNet::TVoidTask InboundConnection(TSocket socket, std::shared_ptr<void> connectionReservation);
    NNet::TVoidTask OutboundServe(std::shared_ptr<TNode<TSocket>>);
    NNet::TVoidTask Idle();
    void ReconcilePeerReaders();
    void DrainNodes();
    void DebugPrint();
    void RegisterPeerTargetImpl(uint32_t id, std::shared_ptr<TNode<TSocket>> node, bool stageLearner);
    std::shared_ptr<TNode<TSocket>> UnregisterPeerTargetImpl(uint32_t id, bool requireLeader);

    typename TSocket::TPoller& Poller;
    TSocket Socket;
    std::shared_ptr<TRaft> Raft;
    std::shared_ptr<TOutboundQueueBudget> OutboundQueueBudget;
    std::shared_ptr<TInboundMessageBudget> InboundMessageBudget;
    std::shared_ptr<TRequestProcessor> RequestProcessor;
    std::unordered_set<std::shared_ptr<INode>> Nodes;
    std::unordered_map<uint32_t, std::shared_ptr<TNode<TSocket>>> PeerNodes;
    // Tracks readers for registered endpoint IDs. Removal retires the socket,
    // then retains the node until its read and write coroutines unwind.
    std::unordered_set<uint32_t> PeerResponseReaders;
    std::unordered_set<std::shared_ptr<TNode<TSocket>>> RetiredPeerNodes;
    std::unordered_map<std::shared_ptr<TNode<TSocket>>, ITimeSource::Time> InboundLastActivity;
    std::shared_ptr<ITimeSource> TimeSource;
    bool Serving = false;

    // for debug
    TStateFields PersistentFields;
    TVolatileState VolatileFields;
};

#include "node_impl.h"
