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
NNet::TVoidTask TRaftServer<TSocket>::InboundConnection(TSocket socket) {
    std::shared_ptr<TNode<TSocket>> client;
    try {
        client = std::make_shared<TNode<TSocket>>(
            "client", std::move(socket), TimeSource
        );
        Nodes.insert(client);
        while (true) {
            auto mes = co_await TMessageReader(client->Sock()).Read();
            // client request
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
            } else if (auto maybeSessionForwardedRequest = mes.template Maybe<TSessionForwardedCommandRequest>()) {
                RequestProcessor->OnSessionForwardedCommandRequest(
                    std::move(maybeSessionForwardedRequest.Cast()), client);
            } else if (auto maybeSessionForwardedResponse = mes.template Maybe<TSessionForwardedCommandResponse>()) {
                RequestProcessor->OnSessionForwardedCommandResponse(
                    std::move(maybeSessionForwardedResponse.Cast()), client);
            } else if (auto maybeForwardedRequest = mes.template Maybe<TForwardedCommandRequest>()) {
                RequestProcessor->OnForwardedCommandRequest(std::move(maybeForwardedRequest.Cast()), client);
            } else if (auto maybeForwardedResponse = mes.template Maybe<TForwardedCommandResponse>()) {
                RequestProcessor->OnForwardedCommandResponse(std::move(maybeForwardedResponse.Cast()), client);
            } else if (auto maybeCommandRequest = mes.template Maybe<TCommandRequest>()) {
                RequestProcessor->OnCommandRequest(std::move(maybeCommandRequest.Cast()), client);
            } else if (auto maybeCommandResponse = mes.template Maybe<TCommandResponse>()) {
                RequestProcessor->OnCommandResponse(std::move(maybeCommandResponse.Cast()), client);
            } else {
                Raft->Process(TimeSource->Now(), std::move(mes), client);
                ReconcilePeerReaders();
            }
            Raft->ProcessTimeout(TimeSource->Now());
            ReconcilePeerReaders();
            RequestProcessor->CheckStateChange();
            RequestProcessor->ProcessCommitted();
            RequestProcessor->ProcessWaiting();
            DrainNodes();
        }
    } catch (const std::exception & ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }
    Nodes.erase(client);
    RequestProcessor->CleanUp(client);
    co_return;
}

template<typename TSocket>
void TRaftServer<TSocket>::Serve() {
    if (Serving) throw std::logic_error("Raft server is already serving");
    Serving = true;
    ReconcilePeerReaders();
    Idle();
    InboundServe();
}

template<typename TSocket>
void TRaftServer<TSocket>::RegisterPeerTargetImpl(
        uint32_t id, std::shared_ptr<TNode<TSocket>> node, bool stageLearner) {
    if (!node || node->IsRetired()) throw std::invalid_argument("Invalid peer target endpoint");
    auto [peer, insertedPeer] = PeerNodes.emplace(id, node);
    if (!insertedPeer) throw std::invalid_argument("Duplicate network peer ID");

    bool insertedNode = false;
    bool insertedRequestPeer = false;
    try {
        insertedNode = Nodes.insert(node).second;
        if (!insertedNode) throw std::invalid_argument("Peer endpoint is already registered");
        RequestProcessor->AddPeerEndpoint(id, node);
        insertedRequestPeer = true;
        if (stageLearner) Raft->AddLearnerTarget(id, node);
        else Raft->RegisterPeerTarget(id, node);
    } catch (...) {
        if (insertedRequestPeer) RequestProcessor->RemovePeerEndpoint(id, node);
        if (insertedNode) Nodes.erase(node);
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
void TRaftServer<TSocket>::AddLearnerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node) {
    RegisterPeerTargetImpl(id, std::move(node), true);
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::UnregisterPeerTargetImpl(
        uint32_t id, bool requireLeader) {
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
    if (!Serving) return;
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
            auto mes = co_await TMessageReader(node->Sock()).Read();
            if (auto maybeCapabilities = mes.template Maybe<TPeerCapabilitiesResponse>()) {
                if (!node->CompleteCapabilityNegotiation(
                        maybeCapabilities.Cast(), generation))
                    throw TMessageError("Invalid peer capability response");
                DrainNodes();
            } else if (auto maybeSessionForwardedResponse = mes.template Maybe<TSessionForwardedCommandResponse>()) {
                RequestProcessor->OnSessionForwardedCommandResponse(
                    std::move(maybeSessionForwardedResponse.Cast()), node);
                DrainNodes();
            } else if (auto maybeForwardedResponse = mes.template Maybe<TForwardedCommandResponse>()) {
                RequestProcessor->OnForwardedCommandResponse(std::move(maybeForwardedResponse.Cast()), node);
                DrainNodes();
            } else if (auto maybeCommandResponse = mes.template Maybe<TCommandResponse>()) {
                RequestProcessor->OnCommandResponse(std::move(maybeCommandResponse.Cast()), node);
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
            RequestProcessor->OnPeerDisconnected(node);
            if (wasConnected) node->Disconnect(generation);
            DrainNodes();
            co_await Poller.Sleep(std::chrono::milliseconds(1000));
            if (!node->IsRetired()) DrainNodes();
        }
    }
    if (node->IsRetired()) {
        RequestProcessor->OnPeerDisconnected(node);
        DrainNodes();
    }
    co_return;
}

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::InboundServe() {
    while (true) {
        auto client = co_await Socket.Accept();
        std::cout << "Accepted\n";
        InboundConnection(std::move(client));
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
    auto t0 = TimeSource->Now();
    auto dt = std::chrono::milliseconds(2000);
    auto sleep = std::chrono::milliseconds(100);
    while (true) {
        try {
            Raft->ProcessTimeout(TimeSource->Now());
            ReconcilePeerReaders();
            RequestProcessor->CheckStateChange();
            RequestProcessor->ProcessCommitted();
            RequestProcessor->ProcessWaiting();
            DrainNodes();
        } catch (const TNodeQueueFull& ex) {
            // Periodic Raft requests are regenerated by their retry timers.
            // An outbound overload must not terminate this server task.
            std::cerr << "Raft tick: " << ex.what() << "\n";
        }
        auto t1 = TimeSource->Now();
        if (t1 > t0 + dt) {
            DebugPrint();
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
