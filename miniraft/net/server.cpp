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
            if (auto maybeCommandRequest = mes.template Maybe<TCommandRequest>()) {
                RequestProcessor->OnCommandRequest(std::move(maybeCommandRequest.Cast()), client);
            } else if (auto maybeCommandResponse = mes.template Maybe<TCommandResponse>()) {
                RequestProcessor->OnCommandResponse(std::move(maybeCommandResponse.Cast()), client);
            } else {
                Raft->Process(TimeSource->Now(), std::move(mes), client);
            }
            Raft->ProcessTimeout(TimeSource->Now());
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
    Idle();
    InboundServe();
    std::vector<NNet::TVoidTask> tasks;
    for (auto& [id, node] : PeerNodes) {
        // Learners only receive replication and reply through their own
        // configured voter connections. They cannot receive forwarded client
        // commands, so no outbound response reader is needed for them.
        if (Raft->IsVoter(id)) tasks.emplace_back(OutboundServe(node));
    }
}

template<typename TSocket>
void TRaftServer<TSocket>::AddLearnerTarget(uint32_t id, std::shared_ptr<TNode<TSocket>> node) {
    if (!node) throw std::invalid_argument("Learner target endpoint is null");
    auto [peer, insertedPeer] = PeerNodes.emplace(id, node);
    if (!insertedPeer) throw std::invalid_argument("Duplicate network peer ID");

    bool insertedNode = false;
    try {
        insertedNode = Nodes.insert(node).second;
        if (!insertedNode) throw std::invalid_argument("Learner endpoint is already registered");
        Raft->AddLearnerTarget(id, node);
    } catch (...) {
        if (insertedNode) Nodes.erase(node);
        PeerNodes.erase(peer);
        throw;
    }
}

template<typename TSocket>
std::shared_ptr<TNode<TSocket>> TRaftServer<TSocket>::RemoveLearnerTarget(uint32_t id) {
    auto peer = PeerNodes.find(id);
    if (peer == PeerNodes.end()) throw std::invalid_argument("Unknown network learner target");
    auto endpoint = peer->second;
    Raft->RemoveLearnerTarget(id); // Rejects voters and nonleaders before server state changes.
    Nodes.erase(endpoint);
    PeerNodes.erase(peer);
    return endpoint;
}

template<typename TSocket>
void TRaftServer<TSocket>::DrainNodes() {
    for (const auto& node : Nodes) {
        node->Drain();
    }
}

template<typename TSocket>
NNet::TVoidTask TRaftServer<TSocket>::OutboundServe(std::shared_ptr<TNode<TSocket>> node) {
    // read forwarded replies
    while (true) {
        bool error = false;
        const bool wasConnected = node->IsConnected();
        const auto generation = node->GetConnectionGeneration();
        try {
            if (!node->IsConnected()) {
                throw std::runtime_error("Not connected");
            }
            auto mes = co_await TMessageReader(node->Sock()).Read();
            if (auto maybeCommandResponse = mes.template Maybe<TCommandResponse>()) {
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
            RequestProcessor->OnPeerDisconnected(node);
            if (wasConnected) node->Disconnect(generation);
            DrainNodes();
            co_await Poller.Sleep(std::chrono::milliseconds(1000));
        }
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
        Raft->ProcessTimeout(TimeSource->Now());
        RequestProcessor->CheckStateChange();
        RequestProcessor->ProcessCommitted();
        RequestProcessor->ProcessWaiting();
        DrainNodes();
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
