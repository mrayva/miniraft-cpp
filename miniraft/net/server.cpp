#include <chrono>
#include <coroutine>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <miniraft/raft.h>
#include <miniraft/messages.h>

#include "server.h"

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::InboundConnection(
        TSocket socket, std::shared_ptr<void> connectionReservation) {
    TTaskGuard taskGuard(*this);
    (void)connectionReservation;
    std::shared_ptr<TNode<TSocket>> client;
    try {
        client = std::make_shared<TNode<TSocket>>(
            "client", std::move(socket), TimeSource
        );
        client->MarkResponseReaderStarted();
        struct TReaderGuard {
            std::shared_ptr<TNode<TSocket>> Node;
            ~TReaderGuard() { Node->MarkResponseReaderStopped(); }
        } readerGuard{client};
        Nodes.insert(client);
        InboundLastActivity.emplace(client, CurrentTime());
        bool peerIdentityResolved = false;
        std::optional<uint32_t> authenticatedPeerId;
        while (true) {
            auto mes = co_await TMessageReader(client->Sock(), {}, InboundMessageBudget).Read();
            InboundLastActivity[client] = CurrentTime();
            if (ResolvePeerIdentity && !peerIdentityResolved) {
                authenticatedPeerId = ResolvePeerIdentity(client->Sock());
                peerIdentityResolved = true;
            }
            if (ResolvePeerIdentity) {
                const auto messageType = EMessageType(mes->Type);
                if (messageType == EMessageType::PEER_CAPABILITIES_REQUEST && !authenticatedPeerId)
                    throw TMessageError("Unauthenticated peer capability request");
                if (mes.IsEx() && (!authenticatedPeerId ||
                        mes.template Cast<TMessageEx>()->Src != *authenticatedPeerId))
                    throw TMessageError("Peer message source does not match authenticated identity");
            }
            const auto now = CurrentTime();
            bool handledCapabilities = false;
            // Capability negotiation changes transport state and stays on the
            // poller. Raft and application callbacks below run on the worker.
            if (auto maybeCapabilities = mes.template Maybe<TPeerCapabilitiesRequest>()) {
                auto request = maybeCapabilities.Cast();
                if (!request->ConnectionId || !request->MinVersion ||
                    request->MinVersion > request->MaxVersion)
                    throw TMessageError("Invalid peer capability request");
                const bool compatible = request->MinVersion <= TPeerCapabilitiesRequest::CurrentVersion &&
                    request->MaxVersion >= TPeerCapabilitiesRequest::CurrentVersion;
                auto response = NewHoldedMessage<TPeerCapabilitiesResponse>();
                response->Version = compatible ? TPeerCapabilitiesRequest::CurrentVersion : 0;
                response->Features = compatible
                    ? request->Features & (TPeerCapabilitiesRequest::ExtendedForwarding |
                        TPeerCapabilitiesRequest::SessionizedForwarding) : 0;
                response->ConnectionId = request->ConnectionId;
                client->Send(std::move(response));
                handledCapabilities = true;
            }

            auto processTick = [this, now] {
                Raft->ProcessTimeout(now);
                RequestProcessor->CheckStateChange();
                RequestProcessor->ProcessCommitted();
                RequestProcessor->ProcessWaiting();
            };
            if (handledCapabilities) {
                co_await RunOnRaftWorker(processTick);
            } else {
                co_await RunOnRaftWorker([this, request = std::move(mes), client,
                        now, processTick = std::move(processTick)]() mutable {
                    if (auto maybe = request.template Maybe<TSessionForwardedCommandRequest>()) {
                        RequestProcessor->OnSessionForwardedCommandRequest(
                            std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TSessionForwardedCommandResponse>()) {
                        RequestProcessor->OnSessionForwardedCommandResponse(
                            std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TForwardedCommandRequest>()) {
                        RequestProcessor->OnForwardedCommandRequest(std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TForwardedCommandResponse>()) {
                        RequestProcessor->OnForwardedCommandResponse(std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TIdempotentCommandRequest>()) {
                        RequestProcessor->OnIdempotentCommandRequest(
                            std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TCommandRequest>()) {
                        RequestProcessor->OnCommandRequest(std::move(maybe.Cast()), client);
                    } else if (auto maybe = request.template Maybe<TCommandResponse>()) {
                        RequestProcessor->OnCommandResponse(std::move(maybe.Cast()), client);
                    } else {
                        Raft->Process(now, std::move(request), client);
                    }
                    processTick();
                });
            }
            ReconcilePeerReaders();
            DrainNodes();
        }
    } catch (const std::exception & ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }
    InboundLastActivity.erase(client);
    Nodes.erase(client);
    try {
        co_await RunOnRaftWorker([this, client] { RequestProcessor->CleanUp(client); });
    } catch (const std::exception& ex) {
        std::cerr << "Request cleanup exception: " << ex.what() << "\n";
    }
    if (client && client->IsRetired()) {
        client->Drain();
        if (!client->RetirementComplete()) RetiredPeerNodes.insert(client);
    }
    co_return;
}

template<typename TSocket>
void TRaftServer<TSocket>::Serve() {
    if (Serving) throw std::logic_error("Raft server is already serving");
    if (Stopping) throw std::logic_error("A stopped Raft server cannot be restarted");
    Serving = true;
    ReconcilePeerReaders();
    Idle();
    InboundServe();
}

template<typename TSocket>
void TRaftServer<TSocket>::RequestStop() {
    if (Stopping) return;
    if (!Serving) {
        Stopping = true;
        return;
    }
    if constexpr (requires(TSocket& socket, NNet::TTime deadline) {
            socket.Accept(deadline);
            socket.Close();
        }) {
        const auto validateRetirement = [](const auto& nodes) {
            for (const auto& node : nodes) {
                if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node)) {
                    if (!realNode->IsRetired() && !realNode->SupportsRetirement())
                        throw std::logic_error("A live server connection cannot be safely retired");
                }
            }
        };
        validateRetirement(Nodes);
        validateRetirement(RetiredPeerNodes);

        Stopping = true;
        Socket.Close();
        for (const auto& node : Nodes)
            if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node))
                realNode->Retire();
        for (const auto& node : RetiredPeerNodes)
            if (!node->IsRetired()) node->Retire();
    } else {
        throw std::logic_error("Server listener does not support cancellable accept and close");
    }
}

template<typename TSocket>
bool TRaftServer<TSocket>::HasPendingRetirement() const {
    for (const auto& node : Nodes) {
        if (auto realNode = std::dynamic_pointer_cast<TNode<TSocket>>(node))
            if (realNode->IsRetired() && !realNode->RetirementComplete()) return true;
    }
    for (const auto& node : RetiredPeerNodes)
        if (!node->RetirementComplete()) return true;
    return false;
}

template<typename TSocket>
NNet::TValueTask<TSocket> TRaftServer<TSocket>::AcceptWithDeadline() {
    if constexpr (requires(TSocket& socket, NNet::TTime deadline) { socket.Accept(deadline); }) {
        co_return co_await Socket.Accept(NNet::TClock::now() + AcceptPollInterval);
    } else {
        co_return co_await Socket.Accept();
    }
}

template<typename TSocket>
void TRaftServer<TSocket>::RegisterPeerTargetImpl(
        uint32_t id, std::shared_ptr<TNode<TSocket>> node, bool stageLearner) {
    WaitForPriorRaftWork();
    typename TRaftServer<TSocket>::TSerializedAccessGuard lock(*this);
    if (Stopping) throw std::logic_error("Cannot register peers on a stopped Raft server");
    if (!node || node->IsRetired()) throw std::invalid_argument("Invalid peer target endpoint");
    auto [peer, insertedPeer] = PeerNodes.emplace(id, node);
    if (!insertedPeer) throw std::invalid_argument("Duplicate network peer ID");

    bool insertedNode = false;
    bool insertedRequestPeer = false;
    bool attachedQueueBudget = false;
    try {
        attachedQueueBudget = node->AttachQueueBudget(OutboundQueueBudget);
        insertedNode = Nodes.insert(node).second;
        if (!insertedNode) throw std::invalid_argument("Peer endpoint is already registered");
        RequestProcessor->AddPeerEndpoint(id, node);
        insertedRequestPeer = true;
        if (stageLearner) Raft->AddLearnerTarget(id, node);
        else Raft->RegisterPeerTarget(id, node);
    } catch (...) {
        if (insertedRequestPeer) RequestProcessor->RemovePeerEndpoint(id, node);
        if (insertedNode) Nodes.erase(node);
        if (attachedQueueBudget) node->DetachQueueBudget(OutboundQueueBudget);
        PeerNodes.erase(peer);
        throw;
    }
    ReconcilePeerReaders();
}

template<typename TSocket>
void TRaftServer<TSocket>::RegisterPeerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node) {
    RegisterPeerTargetImpl(id, std::move(node), false);
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::ReplacePeerTarget(
        uint32_t id, std::shared_ptr<TNode<TSocket>> node) {
    WaitForPriorRaftWork();
    typename TRaftServer<TSocket>::TSerializedAccessGuard lock(*this);
    if (!node || node->IsRetired()) throw std::invalid_argument("Invalid replacement peer endpoint");
    auto peer = PeerNodes.find(id);
    if (peer == PeerNodes.end()) throw std::invalid_argument("Unknown network peer target");
    auto previous = peer->second;
    if (previous == node) throw std::invalid_argument("Peer endpoint is already registered");

    const bool readerStarted = PeerResponseReaders.contains(id);
    if (readerStarted && !previous->SupportsRetirement())
        throw std::logic_error("Socket type does not support safe response-reader retirement");
    if (Nodes.contains(node)) throw std::invalid_argument("Peer endpoint is already registered");

    // Prepare every allocating server-side update before changing the live
    // routing maps. This also retains the old node until its reader unwinds.
    auto nodes = Nodes;
    nodes.erase(previous);
    if (!nodes.insert(node).second) throw std::invalid_argument("Peer endpoint is already registered");
    auto peerNodes = PeerNodes;
    peerNodes.at(id) = node;
    auto retiredPeerNodes = RetiredPeerNodes;
    if (readerStarted) retiredPeerNodes.insert(previous);

    const bool attachedQueueBudget = node->AttachQueueBudget(OutboundQueueBudget);
    if (!RequestProcessor->ReplacePeerEndpoint(id, previous, node)) {
        if (attachedQueueBudget) node->DetachQueueBudget(OutboundQueueBudget);
        throw std::logic_error("Request-routing peer endpoint is out of sync");
    }
    try {
        Raft->ReplacePeerTarget(id, node);
    } catch (...) {
        RequestProcessor->ReplacePeerEndpoint(id, node, previous);
        if (attachedQueueBudget) node->DetachQueueBudget(OutboundQueueBudget);
        throw;
    }

    Nodes.swap(nodes);
    PeerNodes.swap(peerNodes);
    RetiredPeerNodes.swap(retiredPeerNodes);
    PeerResponseReaders.erase(id);
    previous->DetachQueueBudget(OutboundQueueBudget);
    if (readerStarted) previous->Retire();
    RequestProcessor->OnPeerDisconnected(previous);
    ReconcilePeerReaders();
    return previous;
}

template<typename TSocket>
void TRaftServer<TSocket>::AddLearnerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node) {
    RegisterPeerTargetImpl(id, std::move(node), true);
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::UnregisterPeerTargetImpl(
        uint32_t id, bool requireLeader) {
    WaitForPriorRaftWork();
    typename TRaftServer<TSocket>::TSerializedAccessGuard lock(*this);
    auto peer = PeerNodes.find(id);
    if (peer == PeerNodes.end()) throw std::invalid_argument("Unknown network peer target");
    auto endpoint = peer->second;
    const bool readerStarted = PeerResponseReaders.contains(id);
    if (readerStarted && !endpoint->SupportsRetirement())
        throw std::logic_error("Socket type does not support safe response-reader retirement");
    bool trackedForRetirement = false;
    if (readerStarted) trackedForRetirement = RetiredPeerNodes.insert(endpoint).second;
    try {
        if (requireLeader) Raft->RemoveLearnerTarget(id);
        else Raft->UnregisterPeerTarget(id);
    } catch (...) {
        if (trackedForRetirement) RetiredPeerNodes.erase(endpoint);
        throw;
    }
    PeerResponseReaders.erase(id);
    RequestProcessor->RemovePeerEndpoint(id, endpoint);
    Nodes.erase(endpoint);
    PeerNodes.erase(peer);
    endpoint->DetachQueueBudget(OutboundQueueBudget);
    if (readerStarted) endpoint->Retire();
    RequestProcessor->OnPeerDisconnected(endpoint);
    return endpoint;
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::UnregisterPeerTarget(uint32_t id) {
    return UnregisterPeerTargetImpl(id, false);
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::RemoveLearnerTarget(uint32_t id) {
    return UnregisterPeerTargetImpl(id, true);
}

template<typename TSocket>
void TRaftServer<TSocket>::ReconcilePeerReaders() {
    if (!Serving || Stopping) return;
    NNet::TDetail::ReconcilePeerResponseReaders(PeerNodes, PeerResponseReaders,
        [this](uint32_t, const auto& node) {
            // Learners need replies to prove catch-up before promotion.
            node->MarkResponseReaderStarted();
            OutboundServe(node);
        });
}

template<typename TSocket>
void TRaftServer<TSocket>::DrainNodes() {
    for (const auto& node : Nodes) {
        node->Drain();
    }
    for (auto it = RetiredPeerNodes.begin(); it != RetiredPeerNodes.end();) {
        (*it)->Drain();
        if ((*it)->RetirementComplete()) it = RetiredPeerNodes.erase(it);
        else ++it;
    }
}

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::OutboundServe(std::shared_ptr<TNode<TSocket>> node) {
    TTaskGuard taskGuard(*this);
    struct TReaderGuard {
        std::shared_ptr<TNode<TSocket>> Node;
        ~TReaderGuard() { Node->MarkResponseReaderStopped(); }
    } readerGuard{node};
    // read forwarded replies
    while (!node->IsRetired()) {
        bool error = false;
        const bool wasConnected = node->IsConnected();
        const auto generation = node->GetConnectionGeneration();
        try {
            if (!node->IsConnected()) {
                throw std::runtime_error("Not connected");
            }
            auto mes = co_await TMessageReader(node->Sock(), {}, InboundMessageBudget).Read();
            if (auto maybeCapabilities = mes.template Maybe<TPeerCapabilitiesResponse>()) {
                if (!node->CompleteCapabilityNegotiation(
                        maybeCapabilities.Cast(), generation))
                    throw TMessageError("Invalid peer capability response");
                DrainNodes();
            } else if (auto maybeSessionForwardedResponse = mes.template Maybe<TSessionForwardedCommandResponse>()) {
                co_await RunOnRaftWorker([this, response = std::move(maybeSessionForwardedResponse.Cast()),
                        node]() mutable {
                    RequestProcessor->OnSessionForwardedCommandResponse(std::move(response), node);
                });
                DrainNodes();
            } else if (auto maybeForwardedResponse = mes.template Maybe<TForwardedCommandResponse>()) {
                co_await RunOnRaftWorker([this, response = std::move(maybeForwardedResponse.Cast()),
                        node]() mutable {
                    RequestProcessor->OnForwardedCommandResponse(std::move(response), node);
                });
                DrainNodes();
            } else if (auto maybeCommandResponse = mes.template Maybe<TCommandResponse>()) {
                co_await RunOnRaftWorker([this, response = std::move(maybeCommandResponse.Cast()),
                        node]() mutable {
                    RequestProcessor->OnCommandResponse(std::move(response), node);
                });
                DrainNodes();
            } else {
                std::cerr << "Wrong message type: " << mes->Type << std::endl;
            }
        } catch (const std::exception& ex) {
            // wait for reconnection
            std::cerr << "Exception: " << ex.what() << std::endl;
            error = true;
        }
        if (error) {
            if (node->IsRetired()) break;
            if (wasConnected && node->IsCapabilityNegotiating())
                node->FailCapabilityNegotiation(generation);
            co_await RunOnRaftWorker([this, node] { RequestProcessor->OnPeerDisconnected(node); });
            if (wasConnected) node->Disconnect(generation);
            DrainNodes();
            co_await Poller.Sleep(std::chrono::milliseconds(1000));
            if (!node->IsRetired()) DrainNodes();
        }
    }
    if (node->IsRetired()) {
        co_await RunOnRaftWorker([this, node] { RequestProcessor->OnPeerDisconnected(node); });
        DrainNodes();
    }
    co_return;
}

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::InboundServe() {
    TTaskGuard taskGuard(*this);
    while (!Stopping) {
        try {
            auto client = co_await AcceptWithDeadline();
            if (Stopping) break;
            auto connectionReservation = InboundMessageBudget->TryReserveConnection();
            if (!connectionReservation) continue;
            std::cout << "Accepted\n";
            InboundConnection(std::move(client), std::move(connectionReservation));
        } catch (const std::system_error& ex) {
            if (Stopping) break;
            if (ex.code() == std::make_error_code(std::errc::timed_out)) continue;
            throw;
        }
    }
    co_return;
}

template<typename TSocket>
void TRaftServer<TSocket>::DebugPrint() {
    const TStateFields& state = *Raft->GetState();
    const TVolatileState& volatileState = *Raft->GetVolatileState();
    if (state == PersistentFields && volatileState == VolatileFields) {
        return;
    }
    if (Raft->CurrentStateName() == EState::LEADER) {
        std::cout << "Leader, "
            << "Term: " << state.CurrentTerm << ", "
            << "Index: " << state.LastLogIndex << ", "
            << "CommitIndex: " << volatileState.CommitIndex << ", ";
        std::cout << "Delay: ";
        for (auto [id, index] : volatileState.MatchIndex) {
            std::cout << id << ":" << (state.LastLogIndex - index) << " ";
        }
        std::cout << "MatchIndex: ";
        for (auto [id, index] : volatileState.MatchIndex) {
            std::cout << id << ":" << index << " ";
        }
        std::cout << "NextIndex: ";
        for (auto [id, index] : volatileState.NextIndex) {
            std::cout << id << ":" << index << " ";
        }
        std::cout << "\n";
    } else if (Raft->CurrentStateName() == EState::CANDIDATE) {
        std::cout << "Candidate, "
            << "Term: " << state.CurrentTerm << ", "
            << "Index: " << state.LastLogIndex << ", "
            << "CommitIndex: " << volatileState.CommitIndex << ", "
            << "\n";
    } else if (Raft->CurrentStateName() == EState::FOLLOWER) {
        std::cout << "Follower, "
            << "Term: " << state.CurrentTerm << ", "
            << "Index: " << state.LastLogIndex << ", "
            << "CommitIndex: " << volatileState.CommitIndex << ", "
            << "\n";
    }
    PersistentFields = state;
    VolatileFields = volatileState;
}

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::Idle() {
    TTaskGuard taskGuard(*this);
    auto t0 = CurrentTime();
    auto dt = std::chrono::milliseconds(2000);
    auto sleep = std::chrono::milliseconds(100);
    while (!Stopping) {
        try {
            const auto now = CurrentTime();
            NNet::TDetail::ExpireIdleConnections(
                InboundLastActivity, now, InboundMessageBudget->ConnectionIdleTimeout(),
                [](const auto& client) {
                    if (!client->SupportsRetirement()) return false;
                    client->Retire();
                    return true;
                });
            std::vector<std::shared_ptr<INode>> disconnectedPeers;
            for (const auto& [_, node] : PeerNodes) {
                if (node->ExpireCapabilityNegotiation(now, CapabilityNegotiationTimeout))
                    disconnectedPeers.push_back(node);
            }
            co_await RunOnRaftWorker([this, now, disconnectedPeers = std::move(disconnectedPeers)] {
                for (const auto& peer : disconnectedPeers)
                    RequestProcessor->OnPeerDisconnected(peer);
                Raft->ProcessTimeout(now);
                RequestProcessor->CheckStateChange();
                RequestProcessor->ProcessCommitted();
                RequestProcessor->ProcessWaiting();
            });
            ReconcilePeerReaders();
            DrainNodes();
        } catch (const TNodeQueueFull& ex) {
            // Periodic Raft requests are regenerated by their retry timers.
            // An outbound overload must not terminate this server task.
            std::cerr << "Raft tick: " << ex.what() << "\n";
        }
        auto t1 = CurrentTime();
        if (t1 > t0 + dt) {
            co_await RunOnRaftWorker([this] { DebugPrint(); });
            t0 = t1;
        }
        co_await Poller.Sleep(t1 + sleep);
    }
    co_return;
}

// TODO: Use TPoller::TSocket
template class TRaftServer<NNet::TSocket>;
template class TRaftServer<NNet::TSslSocket<NNet::TSocket>>;
template class TRaftServer<NNet::TRetirableSslSocket<NNet::TSocket>>;
