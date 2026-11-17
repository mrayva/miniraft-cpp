#pragma once

// Template definitions kept visible for alternative socket implementations.
template<typename TSocket>
typename TNode<TSocket>::EQueueClass TNode<TSocket>::QueueClassFor(uint32_t type) {
    switch (EMessageType(type)) {
    case EMessageType::REQUEST_VOTE_REQUEST:
    case EMessageType::PRE_VOTE_REQUEST: return EQueueClass::Vote;
    case EMessageType::APPEND_ENTRIES_REQUEST:
    case EMessageType::INSTALL_SNAPSHOT_REQUEST: return EQueueClass::Replication;
    case EMessageType::TIMEOUT_NOW_REQUEST: return EQueueClass::TimeoutNow;
    case EMessageType::CONFIGURATION_BARRIER_REQUEST: return EQueueClass::ConfigurationBarrier;
    default: return EQueueClass::Client;
    }
}

template<typename TSocket>
bool TNode<TSocket>::TryGetMessageTreeBytes(const TMessageHolder<TMessage>& message, uint64_t& bytes) {
    bytes = message->Len;
    for (uint32_t i = 0; i < message.PayloadSize; ++i) {
        const auto childBytes = message.Payload[i]->Len;
        if (childBytes > UINT64_MAX - bytes) return false;
        bytes += childBytes;
    }
    return true;
}

template<typename TSocket>
void TNode<TSocket>::RecountPendingMessages() {
    PendingDataMessages = PendingRaftMessages = 0;
    PendingDataBytes = PendingRaftBytes = 0;
    for (const auto& message : Messages) {
        uint64_t bytes = 0;
        if (!TryGetMessageTreeBytes(message, bytes)) throw std::logic_error("Outbound message size overflow");
        if (QueueClassFor(message->Type) == EQueueClass::Client) {
            ++PendingDataMessages;
            PendingDataBytes += bytes;
        } else {
            ++PendingRaftMessages;
            PendingRaftBytes += bytes;
        }
    }
}

template<typename TSocket>
void TNode<TSocket>::CompletePendingMessage(EQueueClass queueClass, uint64_t bytes) {
    auto& count = queueClass == EQueueClass::Client ? PendingDataMessages : PendingRaftMessages;
    auto& totalBytes = queueClass == EQueueClass::Client ? PendingDataBytes : PendingRaftBytes;
    if (!count || totalBytes < bytes) throw std::logic_error("Outbound queue accounting underflow");
    --count;
    totalBytes -= bytes;
}

template<typename TSocket>
void TNode<TSocket>::Send(TMessageHolder<TMessage> message) {
    if (Retired) return;
    if (!TrySend(std::move(message))) throw TNodeQueueFull{};
}

template<typename TSocket>
bool TNode<TSocket>::TrySend(TMessageHolder<TMessage> message) {
    if (Retired) return false;
    ValidateMessageTree(message);

    const auto queueClass = QueueClassFor(message->Type);
    uint64_t bytes = 0;
    if (!TryGetMessageTreeBytes(message, bytes)) return false;

    size_t replacedMessages = 0;
    uint64_t replacedBytes = 0;
    if (queueClass != EQueueClass::Client) {
        for (const auto& queued : Messages) {
            if (QueueClassFor(queued->Type) != queueClass) continue;
            uint64_t oldBytes = 0;
            if (!TryGetMessageTreeBytes(queued, oldBytes) || oldBytes > UINT64_MAX - replacedBytes)
                return false;
            ++replacedMessages;
            replacedBytes += oldBytes;
        }
        if (replacedMessages > PendingRaftMessages || replacedBytes > PendingRaftBytes)
            throw std::logic_error("Outbound queue accounting mismatch");
        const auto remainingMessages = PendingRaftMessages - replacedMessages;
        const auto remainingBytes = PendingRaftBytes - replacedBytes;
        if (remainingMessages >= MaxQueuedRaftMessages ||
            remainingBytes > MaxQueuedRaftBytes || bytes > MaxQueuedRaftBytes - remainingBytes)
            return false;
    } else {
        if (PendingDataMessages >= MaxQueuedDataMessages || PendingDataBytes > MaxQueuedDataBytes ||
            bytes > MaxQueuedDataBytes - PendingDataBytes)
            return false;
    }

    // Append first so allocation failure leaves the previously queued retry intact.
    Messages.emplace_back(std::move(message));
    if (queueClass != EQueueClass::Client) {
        for (size_t i = 0; i + 1 < Messages.size();) {
            if (QueueClassFor(Messages[i]->Type) == queueClass)
                Messages.erase(Messages.begin() + i);
            else
                ++i;
        }
        PendingRaftMessages = PendingRaftMessages - replacedMessages + 1;
        PendingRaftBytes = PendingRaftBytes - replacedBytes + bytes;
    } else {
        ++PendingDataMessages;
        PendingDataBytes += bytes;
    }
    return true;
}

template<typename TSocket>
void TNode<TSocket>::Drain() {
    if (Retired) {
        if (Drainer && Drainer.done()) { Drainer.destroy(); Drainer = {}; }
        if (Connector && Connector.done()) { Connector.destroy(); Connector = {}; }
        if (!ResponseReaderActive && !Drainer && !Connector && !RetiredCleaned) {
            Disconnect(ConnectionGeneration);
            RetiredCleaned = true;
        }
        return;
    }
    if (!Connected) {
        Connect();
        return;
    }
    if (!Drainer || Drainer.done()) {
        if (Drainer && Drainer.done()) {
            Drainer.destroy();
        }
        Drainer = DoDrain();
    }
}

template<typename TSocket>
void TNode<TSocket>::Retire() {
    if (Retired) return;
    if (!SupportsRetirement())
        throw std::logic_error("Socket type does not support safe peer retirement");
    Retired = true;
    Messages.clear();
    PendingDataMessages = PendingRaftMessages = 0;
    PendingDataBytes = PendingRaftBytes = 0;
    ShutdownSocket();
}

template<typename TSocket>
void TNode<TSocket>::ShutdownSocket() noexcept {
    if constexpr (requires(TSocket& socket) { { socket.Shutdown() } noexcept; }) {
        Socket.Shutdown();
    } else if constexpr (requires(TSocket& socket) { socket.Fd(); }) {
        const auto fd = Socket.Fd();
        if (fd >= 0) {
#ifdef _WIN32
            ::shutdown(static_cast<SOCKET>(fd), SD_BOTH);
#else
            ::shutdown(fd, SHUT_RDWR);
#endif
        }
    }
}

template<typename TSocket>
NNet::TVoidSuspendedTask TNode<TSocket>::DoDrain() {
    try {
        while (!Messages.empty()) {
            auto tosend = std::move(Messages); Messages.clear();
            for (auto&& m : tosend) {
                const auto queueClass = QueueClassFor(m->Type);
                uint64_t bytes = 0;
                if (!TryGetMessageTreeBytes(m, bytes)) throw std::logic_error("Outbound message size overflow");
                co_await TMessageWriter(Socket).Write(std::move(m));
                if (!Retired) CompletePendingMessage(queueClass, bytes);
            }
        }
    } catch (const std::exception& ex) {
        std::cout << "Error on write: " << ex.what() << "\n";
        RecountPendingMessages();
        if (!Retired) Connect();
    }
    co_return;
}

template<typename TSocket>
void TNode<TSocket>::Disconnect(uint64_t generation) {
    if (generation != ConnectionGeneration || (Connector && !Connector.done())) return;
    // DoConnect may already be retrying after a write failure. Its awaiter can
    // still be registered with the poller; never destroy that live coroutine
    // because an older reader failed. Established connectors are done here.
    Connected = false;
    if (Drainer) { Drainer.destroy(); Drainer = {}; }
    RecountPendingMessages();
    if (Connector) { Connector.destroy(); Connector = {}; }
    std::destroy_at(&Socket);
    std::construct_at(&Socket);
}

template<typename TSocket>
void TNode<TSocket>::Connect() {
    if (!Retired && Address && (!Connector || Connector.done())) {
        if (ConnectionGeneration == UINT64_MAX) throw std::overflow_error("Connection generation exhausted");
        if (Connector && Connector.done()) {
            Connector.destroy();
        }
        ++ConnectionGeneration;
        Connector = DoConnect();
    }
}

template<typename TSocket>
NNet::TVoidSuspendedTask TNode<TSocket>::DoConnect() {
    std::cout << "Connecting " << Name << "\n";
    Connected = false;
    while (!Connected && !Retired) {
        try {
            Socket = SocketFactory(*Address);
            // The dependency's finite deadline can leave I/O and timer wakeups
            // registered for the same coroutine after a timeout. Use its default
            // I/O-only connect until cancellation of both sources is supported.
            // TCP connect failure timing is therefore controlled by the OS.
            co_await Socket.Connect();
            if (Retired) {
                ShutdownSocket();
                break;
            }
            std::cout << "Connected " << Name << "\n";
            Connected = true;
        } catch (const std::exception& ex) {
            std::cout << "Error on connect: " << Name << " " << ex.what() << "\n";
        }
        if (!Connected && !Retired) {
            co_await Socket.Poller()->Sleep(std::chrono::milliseconds(1000));
        }
    }
    co_return;
}

