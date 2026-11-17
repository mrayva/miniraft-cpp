#pragma once

#include <exception>
#include <cstdint>
#include <memory>
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
}

class TNodeQueueFull : public std::runtime_error {
public:
    TNodeQueueFull() : std::runtime_error("Outbound node queue limit exceeded") {}
};

template<typename TSocket>
class TMessageReader {
public:
    TMessageReader(TSocket& socket, TMessageLimits limits = {})
        : Socket(socket), Limits(limits) { ValidateMessageLimits(Limits); }

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
        auto message = NewHoldedMessage<TMessage>(header.Type, header.Len);
        co_await NNet::TByteReader(Socket).Read(message->Value, header.Len - sizeof(TMessage));
        if (header.Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) {
            const auto count = message.template Cast<TAppendEntriesRequest>()->Nentries;
            if (count > Limits.MaxEntries || uint64_t(count) * sizeof(TLogEntry) > remaining)
                throw TMessageError("AppendEntries count exceeds limits");
            message.InitPayload(count);
            for (uint32_t i = 0; i < count; ++i)
                message.Payload[i] = co_await ReadFrame(true, remaining);
        }
        co_return message;
    }
    TSocket& Socket;
    TMessageLimits Limits;
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

    void Send(TMessageHolder<TMessage> message) override;
    bool TrySend(TMessageHolder<TMessage> message) override;
    void Drain() override;
    // Called after a failed outbound read, once its reader has unwound. A stale
    // reader cannot disconnect a newer connection or its pending connector.
    void Disconnect() { Disconnect(ConnectionGeneration); }
    void Disconnect(uint64_t generation);
    void Retire();
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
    TSocket& Sock() {
        return Socket;
    }

private:
    enum class EQueueClass : uint8_t { Client, Vote, Replication, TimeoutNow, ConfigurationBarrier };
    static EQueueClass QueueClassFor(uint32_t type);
    static bool TryGetMessageTreeBytes(const TMessageHolder<TMessage>& message, uint64_t& bytes);
    void RecountPendingMessages();
    void CompletePendingMessage(EQueueClass queueClass, uint64_t bytes);

    void Connect();
    void ShutdownSocket() noexcept;

    NNet::TVoidSuspendedTask DoDrain();
    NNet::TVoidSuspendedTask DoConnect();

    std::string Name;
    std::optional<NNet::TAddress> Address;
    std::shared_ptr<ITimeSource> TimeSource;
    std::function<TSocket(const NNet::TAddress&)> SocketFactory;
    TSocket Socket;
    bool Connected = false;
    bool Retired = false;
    bool ResponseReaderActive = false;
    bool RetiredCleaned = false;
    uint64_t ConnectionGeneration = 0;

    std::coroutine_handle<> Drainer;
    std::coroutine_handle<> Connector;

    std::vector<TMessageHolder<TMessage>> Messages;
    // Includes messages in Messages and in the active writer batch.
    size_t PendingDataMessages = 0, PendingRaftMessages = 0;
    uint64_t PendingDataBytes = 0, PendingRaftBytes = 0;
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
        TRequestProcessor::TAdmissionLimits admissionLimits = {})
        : Poller(poller)
        , Socket(std::move(socket))
        , Raft(raft)
        , RequestProcessor(std::make_shared<TRequestProcessor>(raft, rsm, nodes, admissionLimits, ts))
        , TimeSource(ts)
    {
        for (const auto& [id, node] : nodes) {
            Nodes.insert(node);
            if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node)) {
                PeerNodes.emplace(id, std::move(realNode));
            }
        }
    }

    void Serve();

    // Register local endpoints on any role so every replica can validate and
    // communicate with future voters. This does not change quorum membership.
    // Calls must be serialized with the poller loop and Raft work.
    void RegisterPeerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node);
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

private:
    NNet::TVoidTask InboundServe();
    NNet::TVoidTask InboundConnection(TSocket socket);
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
    std::shared_ptr<TRequestProcessor> RequestProcessor;
    std::unordered_set<std::shared_ptr<INode>> Nodes;
    std::unordered_map<uint32_t, std::shared_ptr<TNode<TSocket>>> PeerNodes;
    // Tracks readers for registered endpoint IDs. Removal retires the socket,
    // then retains the node until its read and write coroutines unwind.
    std::unordered_set<uint32_t> PeerResponseReaders;
    std::unordered_set<std::shared_ptr<TNode<TSocket>>> RetiredPeerNodes;
    std::shared_ptr<ITimeSource> TimeSource;
    bool Serving = false;

    // for debug
    TStateFields PersistentFields;
    TVolatileState VolatileFields;
};

#include "node_impl.h"
