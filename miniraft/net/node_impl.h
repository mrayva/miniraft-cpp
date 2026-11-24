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
    std::lock_guard lock(QueueMutex);
    RecountPendingMessagesLocked();
}

template<typename TSocket>
void TNode<TSocket>::RecountPendingMessagesLocked() {
    size_t dataMessages = 0, raftMessages = 0;
    uint64_t dataBytes = 0, raftBytes = 0;
    for (const auto& message : Messages) {
        uint64_t bytes = 0;
        if (!TryGetMessageTreeBytes(message, bytes)) throw std::logic_error("Outbound message size overflow");
        if (QueueClassFor(message->Type) == EQueueClass::Client) {
            if (dataMessages == SIZE_MAX || bytes > UINT64_MAX - dataBytes)
                throw std::logic_error("Outbound data queue accounting overflow");
            ++dataMessages;
            dataBytes += bytes;
        } else {
            if (raftMessages == SIZE_MAX || bytes > UINT64_MAX - raftBytes)
                throw std::logic_error("Outbound Raft queue accounting overflow");
            ++raftMessages;
            raftBytes += bytes;
        }
    }
    const auto oldMessages = PendingMessageCountLocked();
    const auto oldBytes = PendingMessageBytesLocked();
    const auto budget = QueueBudget.lock();
    if (budget && !budget->TryReplace(oldMessages, oldBytes,
            dataMessages + raftMessages, dataBytes + raftBytes))
        throw std::logic_error("Recount exceeds aggregate outbound queue budget");
    PendingDataMessages = dataMessages;
    PendingRaftMessages = raftMessages;
    PendingDataBytes = dataBytes;
    PendingRaftBytes = raftBytes;
}

template<typename TSocket>
void TNode<TSocket>::CompletePendingMessage(EQueueClass queueClass, uint64_t bytes) {
    std::lock_guard lock(QueueMutex);
    // Retire() releases all pending reservations atomically while holding this
    // same mutex. An in-flight writer can finish after retirement and must not
    // release that reservation a second time.
    if (Retired) return;
    auto& count = queueClass == EQueueClass::Client ? PendingDataMessages : PendingRaftMessages;
    auto& totalBytes = queueClass == EQueueClass::Client ? PendingDataBytes : PendingRaftBytes;
    if (!count || totalBytes < bytes) throw std::logic_error("Outbound queue accounting underflow");
    const auto budget = QueueBudget.lock();
    if (budget && !budget->TryReplace(PendingMessageCountLocked(), PendingMessageBytesLocked(),
            PendingMessageCountLocked() - 1, PendingMessageBytesLocked() - bytes))
        throw std::logic_error("Completion exceeds aggregate outbound queue budget");
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
    std::lock_guard lock(QueueMutex);
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

    const auto oldTotalMessages = PendingMessageCountLocked();
    const auto oldTotalBytes = PendingMessageBytesLocked();
    if (replacedMessages > oldTotalMessages || replacedBytes > oldTotalBytes ||
        oldTotalMessages - replacedMessages == SIZE_MAX ||
        bytes > UINT64_MAX - (oldTotalBytes - replacedBytes))
        return false;
    const auto newTotalMessages = oldTotalMessages - replacedMessages + 1;
    const auto newTotalBytes = oldTotalBytes - replacedBytes + bytes;

    // Append first so allocation failure leaves the previously queued retry intact.
    Messages.emplace_back(std::move(message));
    if (const auto budget = QueueBudget.lock()) {
        try {
            if (!budget->TryReplace(oldTotalMessages, oldTotalBytes,
                    newTotalMessages, newTotalBytes)) {
                Messages.pop_back();
                return false;
            }
        } catch (...) {
            Messages.pop_back();
            throw;
        }
    }
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
    if (CapabilityNegotiating) return;
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
    std::lock_guard lock(QueueMutex);
    if (Retired) return;
    if (const auto budget = QueueBudget.lock())
        if (!budget->TryReplace(PendingMessageCountLocked(), PendingMessageBytesLocked(), 0, 0))
            throw std::logic_error("Failed to release retired node's aggregate outbound queue usage");
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
        while (true) {
            std::vector<TMessageHolder<TMessage>> tosend;
            {
                std::lock_guard lock(QueueMutex);
                if (Messages.empty()) break;
                tosend = std::move(Messages);
                Messages.clear();
            }
            for (auto&& m : tosend) {
                const auto queueClass = QueueClassFor(m->Type);
                uint64_t bytes = 0;
                if (!TryGetMessageTreeBytes(m, bytes)) throw std::logic_error("Outbound message size overflow");
                co_await TMessageWriter(Socket).Write(std::move(m));
                CompletePendingMessage(queueClass, bytes);
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
    if (CapabilityNegotiating) FailCapabilityNegotiation(generation);
    DiscardForwardedCommands();
    Connected = false;
    PeerFeatures = 0;
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
    PeerFeatures = 0;
    CapabilityNegotiating = false;
    while (!Connected && !Retired) {
        try {
            // A timed-out probe write may have been interrupted. Each physical
            // retry starts a fresh negotiation state, even though it belongs to
            // the same connector coroutine.
            TimedOutCapabilityGeneration = 0;
            Socket = SocketFactory(*Address);
            if constexpr (requires(TSocket& socket, NNet::TTime deadline) {
                    socket.Connect(deadline);
                }) {
                co_await Socket.Connect(NNet::TClock::now() + std::chrono::seconds(5));
            } else {
                co_await Socket.Connect();
            }
            if (Retired) {
                ShutdownSocket();
                break;
            }
            if constexpr (requires(TSocket& socket, char* buffer) { socket.ReadSome(buffer, size_t{}); }) {
                const bool skipProbe = SkipNextCapabilityProbe;
                SkipNextCapabilityProbe = false;
                if (!skipProbe) {
                    CapabilityNegotiating = true;
                    CapabilityNegotiationStarted = TimeSource->Now();
                    auto probe = NewHoldedMessage<TPeerCapabilitiesRequest>();
                    probe->Features = TPeerCapabilitiesRequest::ExtendedForwarding |
                        TPeerCapabilitiesRequest::SessionizedForwarding;
                    probe->ConnectionId = ConnectionGeneration;
                    co_await TMessageWriter(Socket).Write(std::move(probe));
                }
            }
            std::cout << "Connected " << Name << "\n";
            Connected = true;
        } catch (const std::exception& ex) {
            if (CapabilityNegotiating) FailCapabilityNegotiation(ConnectionGeneration);
            std::cout << "Error on connect: " << Name << " " << ex.what() << "\n";
        }
        if (!Connected && !Retired) {
            co_await Socket.Poller()->Sleep(std::chrono::milliseconds(1000));
        }
    }
    co_return;
}

