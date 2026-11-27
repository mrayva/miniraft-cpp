#include <chrono>
#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <iostream>
#include <memory>
#include <functional>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
#include <filesystem>
#include <fstream>

#include <miniraft/configuration.h>
#include <miniraft/durable.h>
#include <miniraft/messages.h>
#include <miniraft/raft.h>
#include <miniraft/net/server.h>
#include <miniraft/net/idempotent_client_transport.h>
#include <miniraft/timesource.h>
#include <coroio/all.hpp>
#ifdef __linux__
#include <sys/wait.h>
#include <unistd.h>
#include "../coroio/tests/server.crt"
#include "../coroio/tests/server.key"
#include "../coroio/tests/mutual-auth.crt"
#include "../coroio/tests/mutual-auth.key"
#endif

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}

namespace {

TMessageHolder<TLogEntry> MakeEntry(const char* text) {
    auto mes = NewHoldedMessage<TLogEntry>(
        static_cast<uint32_t>(TLogEntry::MessageType),
        sizeof(TLogEntry) + strlen(text) + 1
    );
    strcpy(mes.Mes->Data, text);
    return mes;
}

template<typename TSocket>
NNet::TVoidTask StopServerAndMark(TRaftServer<TSocket>& server, bool& stopped) {
    co_await server.Stop();
    stopped = true;
}

NNet::TVoidTask ConnectSocketAndMark(NNet::TSocket& socket, bool& connected) {
    try {
        co_await socket.Connect();
        connected = true;
    } catch (const std::exception&) {
    }
}

} // namespace

void test_host_endpoint_parser_validates_all_fields(void**) {
    const THost uneven{"localhost:80:12345"};
    assert_string_equal(uneven.Address.c_str(), "localhost");
    assert_int_equal(uneven.Port, 80);
    assert_int_equal(uneven.Id, 12345);

    const THost maximum{"[2001:db8::1]:65535:4294967295"};
    assert_string_equal(maximum.Address.c_str(), "2001:db8::1");
    assert_int_equal(maximum.Port, 65535);
    assert_true(maximum.Id == UINT32_MAX);

    for (const auto* endpoint : {"", "localhost", ":80:1", "localhost::1",
            "localhost:0:1", "localhost:65536:1", "localhost:80:0",
            "localhost:80:4294967296", "localhost:80:1:extra", "[]:80:1",
            "[::1]80:1", "[::1]:80:1:extra", "[localhost]:80:1",
            "[not-an-ipv6-literal]:80:1", "[2001:db8::gg]:80:1",
            "[fe80::1%eth0]:80:1", "bad host:80:1"}) {
        bool rejected = false;
        try { (void)THost{endpoint}; }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
    }
}

void test_serialized_raft_worker_bounds_queue_and_recovers_after_saturation(void**) {
    using namespace std::chrono_literals;
    std::recursive_mutex accessMutex;

    bool rejectedZeroCapacity = false;
    try {
        TSerializedRaftWorker worker(accessMutex, 0);
    } catch (const std::invalid_argument&) {
        rejectedZeroCapacity = true;
    }

    TSerializedRaftWorker worker(accessMutex, 1);
    std::mutex gateMutex;
    std::condition_variable gateCondition;
    bool callbackEntered = false;
    bool releaseCallback = false;
    std::vector<int> completionOrder;
    auto first = worker.Submit([&] {
        {
            std::unique_lock lock(gateMutex);
            callbackEntered = true;
            gateCondition.notify_all();
            gateCondition.wait(lock, [&] { return releaseCallback; });
        }
        completionOrder.push_back(1);
        return 1;
    });

    bool sawCallbackEnter = false;
    {
        std::unique_lock lock(gateMutex);
        sawCallbackEnter = gateCondition.wait_for(lock, 3s, [&] { return callbackEntered; });
    }
    const auto pendingWhileRunning = worker.Pending();
    std::future<int> second;
    bool queuedSecond = false;
    bool rejectedThird = false;
    size_t pendingWhileRunningAndQueued = 0;
    if (sawCallbackEnter) {
        second = worker.Submit([&] {
            completionOrder.push_back(2);
            return 2;
        });
        queuedSecond = true;
        pendingWhileRunningAndQueued = worker.Pending();
        try {
            (void)worker.Submit([&] {
                completionOrder.push_back(3);
                return 3;
            });
        } catch (const TRaftWorkerQueueFull&) {
            rejectedThird = true;
        }
    }

    {
        std::lock_guard lock(gateMutex);
        releaseCallback = true;
    }
    gateCondition.notify_all();

    const bool firstReady = first.wait_for(3s) == std::future_status::ready;
    const bool secondReady = queuedSecond && second.wait_for(3s) == std::future_status::ready;
    worker.Stop();
    const auto pendingAfterStop = worker.Pending();
    bool rejectedAfterStop = false;
    try {
        (void)worker.Submit([] { return 4; });
    } catch (const std::logic_error&) {
        rejectedAfterStop = true;
    }

    assert_true(rejectedZeroCapacity);
    assert_true(sawCallbackEnter);
    assert_true(pendingWhileRunning == 1);
    assert_true(queuedSecond);
    assert_true(pendingWhileRunningAndQueued == 2);
    assert_true(rejectedThird);
    assert_true(firstReady);
    assert_true(secondReady);
    assert_true(pendingAfterStop == 0);
    assert_true(rejectedAfterStop);
    assert_int_equal(first.get(), 1);
    assert_int_equal(second.get(), 2);
    assert_int_equal(completionOrder.size(), 2);
    assert_int_equal(completionOrder[0], 1);
    assert_int_equal(completionOrder[1], 2);
}

#ifdef __linux__
void test_idempotent_client_times_out_and_retries_same_operation(void**) {
    using namespace std::chrono_literals;
    struct TStore : IIdempotentClientStateStore {
        std::optional<TIdempotentClientSessionState> Saved;
        std::optional<TIdempotentClientSessionState> Load() override { return Saved; }
        void Save(const TIdempotentClientSessionState& state) override { Saved = state; }
    };
    NNet::TLoop<NNet::TDefaultPoller> loop;
    loop.Poller().SetMaxDuration(1ms);
    auto store = std::make_shared<TStore>();
    TIdempotentClientSession session(store);
    session.StartSession(1, 2, 3);
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    listener.Bind();
    listener.Listen();
    sockaddr_in bound{};
    socklen_t length = sizeof(bound);
    assert_int_equal(::getsockname(listener.Fd(), reinterpret_cast<sockaddr*>(&bound), &length), 0);
    NNet::TAddress address{"127.0.0.1", ntohs(bound.sin_port)};
    TMessageHolder<TIdempotentCommandRequest> first, retried;
    auto server = [](NNet::TSocket& listener, auto& first, auto& retried) -> NNet::TFuture<void> {
        auto stalled = co_await listener.Accept();
        auto request = co_await TMessageReader(stalled).Read();
        first = request.template Cast<TIdempotentCommandRequest>();
        // Keep the first connection open without a reply while accepting a retry.
        auto active = co_await listener.Accept();
        request = co_await TMessageReader(active).Read();
        retried = request.template Cast<TIdempotentCommandRequest>();
        auto reply = NewHoldedMessage<TIdempotentCommandResponse>();
        reply->Epoch = retried->Epoch;
        reply->SessionIdHigh = retried->SessionIdHigh;
        reply->SessionIdLow = retried->SessionIdLow;
        reply->Sequence = retried->Sequence;
        reply->Cookie = retried->Cookie;
        reply->Index = 42;
        co_await TMessageWriter(active).Write(reply);
    }(listener, first, retried);
    const auto drive = [&](const auto& task) {
        const auto until = NNet::TClock::now() + 3s;
        while (!task.done() && NNet::TClock::now() < until) loop.Step();
        assert_true(task.done());
    };
    std::unique_ptr<NNet::TSocket> active;
    bool connected = false;
    NNet::TIdempotentClient shortClient(loop.Poller(), address, active, connected, session,
        NNet::TIdempotentClientRetryOptions{.MaxAttempts = 1, .ExchangeTimeout = 50ms});
    auto stalled = shortClient.Write("pending payload");
    drive(stalled);
    bool timedOut = false;
    try { (void)stalled.await_resume(); }
    catch (const std::exception& e) { timedOut = std::string_view(e.what()).find("exchange timed out") != std::string_view::npos; }
    assert_true(timedOut);
    assert_false(connected);
    assert_true(store->Saved && store->Saved->HasPending);

    NNet::TIdempotentClient retryClient(loop.Poller(), address, active, connected, session);
    auto retry = retryClient.RetryPending();
    drive(retry);
    assert_int_equal(retry.await_resume()->Index, 42);
    drive(server);
    server.await_resume();
    assert_true(first && retried);
    assert_int_equal(first->Len, retried->Len);
    assert_memory_equal(first.Mes, retried.Mes, first->Len);
    assert_false(store->Saved->HasPending);
}

void test_server_poller_progresses_while_worker_callback_is_blocked(void**) {
    using namespace std::chrono_literals;
    struct TGate {
        std::mutex Mutex;
        std::condition_variable Condition;
        bool Entered = false;
        bool Released = false;
    };
    struct TBlockingRsm final : TDummyRsm {
        std::shared_ptr<TGate> Gate;
        void Validate(TMessageHolder<TCommandRequest>) const override {
            std::unique_lock lock(Gate->Mutex);
            Gate->Entered = true;
            Gate->Condition.notify_all();
            Gate->Condition.wait(lock, [&] { return Gate->Released; });
        }
    };

    NNet::TLoop<NNet::TPoll> loop;
    loop.Poller().SetMaxDuration(1ms);
    auto timeSource = std::make_shared<TTimeSource>();
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    listener.Bind();
    listener.Listen();
    sockaddr_in bound{};
    socklen_t boundSize = sizeof(bound);
    assert_int_equal(::getsockname(listener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize), 0);
    const NNet::TAddress address(reinterpret_cast<sockaddr*>(&bound), boundSize);

    auto gate = std::make_shared<TGate>();
    auto rsm = std::make_shared<TBlockingRsm>();
    rsm->Gate = gate;
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 1, TNodeDict{});
    TRaftServer<NNet::TSocket> server(
        loop.Poller(), std::move(listener), raft, rsm, TNodeDict{}, timeSource);
    server.Serve();

    NNet::TSocket client(address, loop.Poller());
    auto sendRequest = [](NNet::TSocket& socket) -> NNet::TVoidTask {
        co_await socket.Connect();
        co_await TMessageWriter(socket).Write(NewHoldedMessage<TCommandRequest>());
    }(client);
    (void)sendRequest;

    const auto gateEntered = [&] {
        std::lock_guard lock(gate->Mutex);
        return gate->Entered;
    };
    const auto deadline = NNet::TClock::now() + 3s;
    while (!gateEntered() && NNet::TClock::now() < deadline) loop.Step();

    bool timerFired = false;
    if (gateEntered()) {
        // Keep the coroutine lambda closure alive until its suspended task
        // completes; the coroutine frame retains a pointer to that closure.
        auto timerTask = [&]() -> NNet::TVoidTask {
            co_await loop.Poller().Sleep(10ms);
            timerFired = true;
        };
        auto timer = timerTask();
        (void)timer;
        const auto timerDeadline = NNet::TClock::now() + 1s;
        while (!timerFired && NNet::TClock::now() < timerDeadline) loop.Step();
    }
    const bool callbackStillBlocked = [&] {
        std::lock_guard lock(gate->Mutex);
        return gate->Entered && !gate->Released;
    }();
    {
        std::lock_guard lock(gate->Mutex);
        gate->Released = true;
    }
    gate->Condition.notify_all();

    bool stopped = false;
    StopServerAndMark(server, stopped);
    const auto stopDeadline = NNet::TClock::now() + 3s;
    while (!stopped && NNet::TClock::now() < stopDeadline) loop.Step();
    assert_true(stopped);
    assert_true(timerFired);
    assert_true(callbackStillBlocked);
}

void test_serialized_raft_task_keeps_poller_timers_running(void**) {
    using namespace std::chrono_literals;
    struct TGate {
        std::mutex Mutex;
        std::condition_variable Condition;
        bool Entered = false;
        bool Released = false;
        bool ReceivedRaft = false;
    };

    NNet::TLoop<NNet::TPoll> loop;
    loop.Poller().SetMaxDuration(1ms);
    auto timeSource = std::make_shared<TTimeSource>();
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    listener.Bind();
    listener.Listen();
    auto gate = std::make_shared<TGate>();
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 1, TNodeDict{});
    TRaftServer<NNet::TSocket> server(
        loop.Poller(), std::move(listener), raft, std::make_shared<TDummyRsm>(), TNodeDict{}, timeSource);
    server.Serve();

    bool taskCompleted = false;
    auto taskBody = [&]() -> NNet::TVoidTask {
        co_await server.RunSerializedRaftTask([&](TRaft& serializedRaft) {
            std::unique_lock lock(gate->Mutex);
            gate->ReceivedRaft = &serializedRaft == raft.get();
            gate->Entered = true;
            gate->Condition.notify_all();
            gate->Condition.wait(lock, [&] { return gate->Released; });
        });
        taskCompleted = true;
    };
    auto task = taskBody();
    (void)task;

    const auto gateEntered = [&] {
        std::lock_guard lock(gate->Mutex);
        return gate->Entered;
    };
    const auto deadline = NNet::TClock::now() + 3s;
    while (!gateEntered() && NNet::TClock::now() < deadline) loop.Step();

    bool timerFired = false;
    if (gateEntered()) {
        auto timerBody = [&]() -> NNet::TVoidTask {
            co_await loop.Poller().Sleep(10ms);
            timerFired = true;
        };
        auto timer = timerBody();
        (void)timer;
        const auto timerDeadline = NNet::TClock::now() + 1s;
        while (!timerFired && NNet::TClock::now() < timerDeadline) loop.Step();
    }
    const bool callbackStillBlocked = [&] {
        std::lock_guard lock(gate->Mutex);
        return gate->Entered && !gate->Released;
    }();
    {
        std::lock_guard lock(gate->Mutex);
        gate->Released = true;
    }
    gate->Condition.notify_all();

    const auto taskDeadline = NNet::TClock::now() + 3s;
    while (!taskCompleted && NNet::TClock::now() < taskDeadline) loop.Step();
    bool stopped = false;
    StopServerAndMark(server, stopped);
    const auto stopDeadline = NNet::TClock::now() + 3s;
    while (!stopped && NNet::TClock::now() < stopDeadline) loop.Step();
    assert_true(stopped);
    assert_true(taskCompleted);
    assert_true(gate->ReceivedRaft);
    assert_true(timerFired);
    assert_true(callbackStillBlocked);
}

void test_server_binds_peer_rpc_source_to_authenticated_identity(void**) {
    using namespace std::chrono_literals;
    using TTlsSocket = NNet::TRetirableSslSocket<NNet::TSocket>;
    struct TRecordingPeer final : INode {
        std::atomic<uint32_t> PreVoteResponses{0};

        void Send(TMessageHolder<TMessage> message) override {
            if (message && message->Type == uint32_t(EMessageType::PRE_VOTE_RESPONSE))
                PreVoteResponses.fetch_add(1, std::memory_order_relaxed);
        }

        void Drain() override { }
    };

    const auto tempDir = std::filesystem::temp_directory_path() /
        ("miniraft-peer-mtls-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(tempDir));
    struct TDirectoryCleanup {
        std::filesystem::path Path;
        ~TDirectoryCleanup() {
            std::error_code ignored;
            std::filesystem::remove_all(Path, ignored);
        }
    } cleanup{tempDir};
    const auto certPath = tempDir / "identity.pem";
    const auto keyPath = tempDir / "identity.key";
    {
        std::ofstream cert(certPath, std::ios::binary);
        std::ofstream key(keyPath, std::ios::binary);
        assert_true(cert.good() && key.good());
        cert << testMutualAuthCert;
        key << testMutualAuthKey;
        assert_true(cert.good() && key.good());
    }
    const auto certFile = certPath.string();
    const auto keyFile = keyPath.string();
    auto clientContext = std::make_shared<NNet::TSslContext>(
        NNet::TSslContext::MutualTlsClient(certFile.c_str(), certFile.c_str(), keyFile.c_str()));
    auto serverContext = std::make_shared<NNet::TSslContext>(
        NNet::TSslContext::MutualTlsServer(certFile.c_str(), keyFile.c_str(), certFile.c_str()));

    NNet::TLoop<NNet::TPoll> loop;
    loop.Poller().SetMaxDuration(1ms);
    auto timeSource = std::make_shared<TTimeSource>();
    auto peerTwo = std::make_shared<TRecordingPeer>();
    auto peerThree = std::make_shared<TRecordingPeer>();
    TNodeDict peers{{2, peerTwo}, {3, peerThree}};
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 1, peers);

    NNet::TSocket rawListener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    rawListener.Bind();
    rawListener.Listen();
    sockaddr_in bound{};
    socklen_t boundSize = sizeof(bound);
    assert_int_equal(::getsockname(rawListener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize), 0);
    const NNet::TAddress address(reinterpret_cast<sockaddr*>(&bound), boundSize);

    std::atomic<uint32_t> identityResolutions{0};
    auto resolver = [&](const TTlsSocket& socket) -> std::optional<uint32_t> {
        identityResolutions.fetch_add(1, std::memory_order_relaxed);
        auto certificate = socket.GetPeerCertificate();
        if (!certificate) return std::nullopt;
        char commonName[16]{};
        const auto nameSize = X509_NAME_get_text_by_NID(
            X509_get_subject_name(certificate.get()), NID_commonName,
            commonName, sizeof(commonName));
        if (nameSize != 1 || commonName[0] != '2') return std::nullopt;
        return uint32_t{2};
    };
    TRaftServer<TTlsSocket> server(loop.Poller(), TTlsSocket(std::move(rawListener), *serverContext), raft,
        std::make_shared<TDummyRsm>(), peers, timeSource,
        TRequestProcessor::TAdmissionLimits{}, TOutboundQueueLimits{},
        TInboundMessageBudgetLimits{}, resolver);
    server.Serve();

    const auto makePreVote = [](uint32_t claimedSource) {
        auto request = NewHoldedMessage(
            TMessageEx{.Src = claimedSource, .Dst = 1, .Term = 0, .Seqno = 1},
            TPreVoteRequest{});
        request->ProspectiveTerm = 1;
        request->LastLogIndex = 0;
        request->LastLogTerm = 0;
        request->CandidateId = claimedSource;
        request->Padding = 0;
        return request;
    };
    const auto sendRequest = [](TTlsSocket& socket,
            TMessageHolder<TMessage> request) -> NNet::TFuture<void> {
        co_await socket.Connect(NNet::TClock::now() + 3s);
        co_await TMessageWriter(socket).Write(std::move(request));
    };
    const auto drive = [&](const auto& task, std::chrono::milliseconds timeout = 3s) {
        const auto deadline = NNet::TClock::now() + timeout;
        while (!task.done() && NNet::TClock::now() < deadline) loop.Step();
        assert_true(task.done());
    };

    // The connection is authenticated as peer 2, so a valid RPC claiming peer
    // 3 must be rejected before Raft routes it to peer 3's response endpoint.
    TTlsSocket spoofed(NNet::TSocket(address, loop.Poller()), *clientContext);
    auto spoofedSend = sendRequest(spoofed, makePreVote(3));
    drive(spoofedSend);
    spoofedSend.await_resume();
    const auto spoofedDeadline = NNet::TClock::now() + 2s;
    while (identityResolutions.load(std::memory_order_relaxed) < 1 &&
            NNet::TClock::now() < spoofedDeadline)
        loop.Step();
    assert_true(identityResolutions.load(std::memory_order_relaxed) == 1);
    const auto spoofedResponseDeadline = NNet::TClock::now() + 2s;
    while (peerThree->PreVoteResponses.load(std::memory_order_relaxed) == 0 &&
            NNet::TClock::now() < spoofedResponseDeadline)
        loop.Step();
    const bool spoofedRequestWasRejected =
        peerThree->PreVoteResponses.load(std::memory_order_relaxed) == 0;
    spoofed.Close();
    assert_true(spoofedRequestWasRejected);

    // The certificate maps to node 2, so a matching claimed source reaches Raft.
    TTlsSocket authenticated(NNet::TSocket(address, loop.Poller()), *clientContext);
    auto authenticatedSend = sendRequest(authenticated, makePreVote(2));
    drive(authenticatedSend);
    authenticatedSend.await_resume();
    const auto responseDeadline = NNet::TClock::now() + 2s;
    while (!peerTwo->PreVoteResponses.load(std::memory_order_relaxed) &&
            NNet::TClock::now() < responseDeadline)
        loop.Step();
    const auto matchingRequestReachedRaft =
        peerTwo->PreVoteResponses.load(std::memory_order_relaxed) == 1;
    const auto resolverSawBothConnections =
        identityResolutions.load(std::memory_order_relaxed) == 2;

    authenticated.Close();

    // A TLS client without a certificate must be disconnected before any Raft
    // response is produced. TLS 1.3 may let the client finish its side of the
    // handshake before the server processes the missing-certificate alert, so
    // observe the server's close instead of assuming Connect itself fails.
    auto unauthenticatedContext = std::make_shared<NNet::TSslContext>(NNet::TSslContext::Client());
    TTlsSocket unauthenticated(NNet::TSocket(address, loop.Poller()), *unauthenticatedContext);
    bool missingCertificateRejected = false;
    auto unauthenticatedExchange = [&](TTlsSocket& socket, bool& rejected) -> NNet::TFuture<void> {
        try {
            co_await socket.Connect(NNet::TClock::now() + 3s);
            co_await TMessageWriter(socket).Write(makePreVote(2));
            char response = 0;
            const auto read = co_await socket.ReadSome(&response, sizeof(response));
            rejected = read == 0;
        } catch (const std::exception&) {
            rejected = true;
        }
    }(unauthenticated, missingCertificateRejected);
    drive(unauthenticatedExchange);
    unauthenticatedExchange.await_resume();

    bool stopped = false;
    StopServerAndMark(server, stopped);
    const auto stopDeadline = NNet::TClock::now() + 3s;
    while (!stopped && NNet::TClock::now() < stopDeadline) loop.Step();
    assert_true(stopped);
    assert_true(matchingRequestReachedRaft);
    assert_true(resolverSawBothConnections);
    assert_true(missingCertificateRejected);
    assert_true(identityResolutions.load(std::memory_order_relaxed) == 2);
}
#endif

void test_node_send_queue_bounds_data_and_coalesces_raft_retries(void**) {
    NNet::TLoop<NNet::TPoll> loop;
    auto& poller = loop.Poller();
    auto timeSource = std::make_shared<TTimeSource>();
    auto makeNode = [&] {
        return TNode<NNet::TSocket>(
            [&](const NNet::TAddress& address) { return NNet::TSocket(address, poller); },
            "queue-test", NNet::TAddress{"127.0.0.1", 65534}, timeSource);
    };

    auto countBounded = makeNode();
    for (size_t i = 0; i < TNode<NNet::TSocket>::MaxQueuedDataMessages; ++i)
        assert_true(countBounded.TrySend(NewHoldedMessage<TCommandResponse>()));
    assert_false(countBounded.TrySend(NewHoldedMessage<TCommandResponse>()));
    bool sendRejected = false;
    try { countBounded.Send(NewHoldedMessage<TCommandResponse>()); }
    catch (const TNodeQueueFull&) { sendRejected = true; }
    assert_true(sendRejected);

    auto byteBounded = makeNode();
    constexpr size_t halfBudget = TNode<NNet::TSocket>::MaxQueuedDataBytes / 2;
    const auto largeReply = [&] {
        return NewHoldedMessage<TCommandResponse>(halfBudget);
    };
    assert_true(byteBounded.TrySend(largeReply()));
    assert_true(byteBounded.TrySend(largeReply()));
    assert_false(byteBounded.TrySend(largeReply()));

    auto coalesced = makeNode();
    for (uint64_t i = 1; i <= 1000; ++i) {
        auto request = NewHoldedMessage<TAppendEntriesRequest>();
        request->Term = i;
        assert_true(coalesced.TrySend(std::move(request)));
    }
    assert_true(coalesced.TrySend(NewHoldedMessage<TConfigurationBarrierRequest>()));
    assert_true(coalesced.TrySend(NewHoldedMessage<TRequestVoteRequest>()));

    auto retired = makeNode();
    retired.Retire();
    assert_false(retired.TrySend(NewHoldedMessage<TCommandResponse>()));
    retired.Send(NewHoldedMessage<TCommandResponse>()); // Preserve retired Send's no-op behavior.

    auto aggregateBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{1, 1024});
    auto aggregateFirst = makeNode();
    auto aggregateSecond = makeNode();
    assert_true(aggregateFirst.AttachQueueBudget(aggregateBudget));
    assert_true(aggregateSecond.AttachQueueBudget(aggregateBudget));
    assert_true(aggregateFirst.TrySend(NewHoldedMessage<TAppendEntriesRequest>()));
    assert_false(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    // Replacing a coalesced retry keeps the shared reservation at one message.
    assert_true(aggregateFirst.TrySend(NewHoldedMessage<TAppendEntriesRequest>()));
    assert_false(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    aggregateFirst.Retire();
    assert_true(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    aggregateSecond.Retire();

    // Independent server-local limits also reserve from one process budget.
    auto processBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{4, 1024});
    auto firstServerBudget = std::make_shared<TOutboundQueueBudget>(
        TOutboundQueueLimits{4, 1024}, processBudget);
    auto secondServerBudget = std::make_shared<TOutboundQueueBudget>(
        TOutboundQueueLimits{4, 1024}, processBudget);
    auto processFirst = makeNode();
    auto processSecond = makeNode();
    assert_true(processFirst.AttachQueueBudget(firstServerBudget));
    assert_true(processSecond.AttachQueueBudget(secondServerBudget));
    assert_true(processFirst.TrySend(NewHoldedMessage<TCommandResponse>(700)));
    // A node retains its reservation even if the server-local owner goes away.
    firstServerBudget.reset();
    assert_false(processSecond.TrySend(NewHoldedMessage<TCommandResponse>(700)));
    processFirst.Retire();
    assert_true(processSecond.TrySend(NewHoldedMessage<TCommandResponse>(700)));
    processSecond.Retire();

    auto measuredReply = NewHoldedMessage<TCommandResponse>();
    auto byteBudget = std::make_shared<TOutboundQueueBudget>(
        TOutboundQueueLimits{2, measuredReply->Len});
    auto byteFirst = makeNode();
    auto byteSecond = makeNode();
    assert_true(byteFirst.AttachQueueBudget(byteBudget));
    assert_true(byteSecond.AttachQueueBudget(byteBudget));
    assert_true(byteFirst.TrySend(std::move(measuredReply)));
    assert_false(byteSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    byteFirst.Retire();
    assert_true(byteSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    byteSecond.Retire();

    auto allocationBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{
        1, sizeof(TCommandResponse)});
    auto allocationBounded = makeNode();
    assert_true(allocationBounded.AttachQueueBudget(allocationBudget));
    auto overallocated = NewHoldedMessage<TCommandResponse>(1024);
    overallocated->Len = sizeof(TCommandResponse);
    assert_false(allocationBounded.TrySend(std::move(overallocated)));

    auto batch = NewHoldedMessage<TAppendEntriesRequest>();
    batch->Nentries = 1;
    batch.InitPayload(4);
    batch.PayloadSize = 1;
    batch.Payload[0] = NewHoldedMessage<TLogEntry>();
    const auto wireBytes = uint64_t(batch->Len) + batch.Payload[0]->Len;
    const auto holderBytes = uint64_t(batch.PayloadCapacity) * sizeof(TMessageHolder<TMessage>);
    auto holderBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{
        1, wireBytes + holderBytes - 1});
    auto holderBounded = makeNode();
    assert_true(holderBounded.AttachQueueBudget(holderBudget));
    assert_false(holderBounded.TrySend(std::move(batch)));

    struct TImmediatePoller {
        NNet::TValueTask<void> Sleep(std::chrono::milliseconds) { co_return; }
    };
    struct TImmediateSocket {
        using TPoller = TImmediatePoller;
        TPoller* Poller() { return nullptr; }
        NNet::TValueTask<void> Connect() { co_return; }
        NNet::TValueTask<std::ptrdiff_t> WriteSome(const void*, size_t size) {
            co_return static_cast<std::ptrdiff_t>(size);
        }
    };
    struct TWriteGate {
        std::mutex Mutex;
        std::condition_variable Condition;
        bool Entered = false;
        bool Released = false;
        std::atomic<bool> BlockFirstWrite{false};
    };
    struct TBlockingSocket {
        using TPoller = TImmediatePoller;
        std::shared_ptr<TWriteGate> Gate;
        TPoller* Poller() { return nullptr; }
        NNet::TValueTask<void> Connect() { co_return; }
        NNet::TValueTask<std::ptrdiff_t> WriteSome(const void*, size_t size) {
            if (!Gate->BlockFirstWrite.exchange(true)) {
                std::unique_lock lock(Gate->Mutex);
                Gate->Entered = true;
                Gate->Condition.notify_all();
                Gate->Condition.wait(lock, [&] { return Gate->Released; });
            }
            co_return static_cast<std::ptrdiff_t>(size);
        }
    };
    auto drainBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{1, 1024});
    TNode<TImmediateSocket> drainFirst("drain-first", TImmediateSocket{}, timeSource);
    TNode<TImmediateSocket> drainSecond("drain-second", TImmediateSocket{}, timeSource);
    assert_true(drainFirst.AttachQueueBudget(drainBudget));
    assert_true(drainSecond.AttachQueueBudget(drainBudget));
    assert_true(drainFirst.TrySend(NewHoldedMessage<TCommandResponse>()));
    assert_false(drainSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    drainFirst.Drain(); // Successful completion returns its aggregate reservation.
    assert_true(drainSecond.TrySend(NewHoldedMessage<TCommandResponse>()));

    // A worker may produce Raft replies while the poller owns the node's
    // writer. Concurrent retries of the same traffic classes must coalesce
    // without corrupting the queue or its shared reservation.
    auto concurrentBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{4, 1024});
    auto writeGate = std::make_shared<TWriteGate>();
    TNode<TBlockingSocket> concurrent("concurrent", TBlockingSocket{writeGate}, timeSource);
    TNode<TImmediateSocket> concurrentContender("concurrent-contender", TImmediateSocket{}, timeSource);
    assert_true(concurrent.AttachQueueBudget(concurrentBudget));
    assert_true(concurrentContender.AttachQueueBudget(concurrentBudget));
    assert_true(concurrent.TrySend(NewHoldedMessage<TAppendEntriesRequest>()));
    std::thread drainThread([&] { concurrent.Drain(); });
    std::unique_lock writerLock(writeGate->Mutex);
    const bool writerSuspended = writeGate->Condition.wait_for(
        writerLock, std::chrono::seconds(5), [&] { return writeGate->Entered; });
    writerLock.unlock();
    if (!writerSuspended) {
        {
            std::lock_guard lock(writeGate->Mutex);
            writeGate->Released = true;
        }
        writeGate->Condition.notify_all();
        drainThread.join();
        assert_true(writerSuspended);
        return;
    }

    std::atomic<bool> startProducers{false};
    std::atomic<size_t> accepted{0};
    std::mutex producerMutex;
    std::condition_variable producerCondition;
    size_t producersDone = 0;
    std::vector<std::thread> producers;
    for (size_t threadIndex = 0; threadIndex < 4; ++threadIndex) {
        producers.emplace_back([&, threadIndex] {
            while (!startProducers.load(std::memory_order_acquire)) std::this_thread::yield();
            for (size_t i = 0; i < 1000; ++i) {
                const auto trafficClass = (i + threadIndex) % 3;
                bool sent = false;
                if (trafficClass == 0)
                    sent = concurrent.TrySend(NewHoldedMessage<TAppendEntriesRequest>());
                else if (trafficClass == 1)
                    sent = concurrent.TrySend(NewHoldedMessage<TRequestVoteRequest>());
                else
                    sent = concurrent.TrySend(NewHoldedMessage<TConfigurationBarrierRequest>());
                if (sent) accepted.fetch_add(1, std::memory_order_relaxed);
            }
            {
                std::lock_guard lock(producerMutex);
                ++producersDone;
            }
            producerCondition.notify_one();
        });
    }
    startProducers.store(true, std::memory_order_release);
    std::unique_lock producerLock(producerMutex);
    const bool producersCompletedWhileWriterBlocked = producerCondition.wait_for(
        producerLock, std::chrono::seconds(5), [&] { return producersDone == producers.size(); });
    producerLock.unlock();
    const bool contenderWasRejectedWhileWriterBlocked =
        !concurrentContender.TrySend(NewHoldedMessage<TCommandResponse>());
    {
        std::lock_guard lock(writeGate->Mutex);
        writeGate->Released = true;
    }
    writeGate->Condition.notify_all();
    for (auto& producer : producers) producer.join();
    drainThread.join();
    assert_true(producersCompletedWhileWriterBlocked);
    assert_true(accepted.load(std::memory_order_relaxed) == 4000);
    // The in-flight request and one retry per class consume all four
    // aggregate slots until the writer completes both batches.
    assert_true(contenderWasRejectedWhileWriterBlocked);
    assert_true(concurrentContender.TrySend(NewHoldedMessage<TCommandResponse>()));
}

void test_message_send_recv(void**) {
    auto mes = MakeEntry("MESSAGE");

    NNet::TLoop<NNet::TPoll> loop;
    NNet::TSocket socket(NNet::TAddress{"127.0.0.1", 8890}, loop.Poller());
    socket.Bind();
    socket.Listen();

    NNet::TSocket client(NNet::TAddress{"127.0.0.1", 8890}, loop.Poller());

    NNet::TVoidSuspendedTask h1 = [](NNet::TSocket& client, TMessageHolder<TLogEntry> mes) -> NNet::TVoidSuspendedTask
    {
        co_await client.Connect();
        co_await client.WriteSome(mes.RawData.get(), mes->Len);
        co_return;
    }(client, mes);

    TMessageHolder<TMessage> received;
    NNet::TVoidSuspendedTask h2 = [](NNet::TSocket& server, TMessageHolder<TMessage>& received) -> NNet::TVoidSuspendedTask
    {
        auto client = std::move(co_await server.Accept());
        uint32_t type, len;
        co_await client.ReadSome(&type, sizeof(type));
        co_await client.ReadSome(&len, sizeof(len));
        received = NewHoldedMessage<TMessage>(type, len);
        co_await client.ReadSome(received->Value, len - sizeof(TMessage));
        co_return;
    }(socket, received);

    while (!(h1.done() && h2.done())) {
        loop.Step();
    }

    auto maybeCasted = received.Maybe<TLogEntry>();
    assert_true(maybeCasted);
    auto casted = maybeCasted.Cast();
    assert_string_equal(mes->Data, casted->Data);

    h1.destroy(); h2.destroy();
}

void test_read_write(void**) {
    auto mes = MakeEntry("MESSAGE");

    NNet::TLoop<NNet::TPoll> loop;
    NNet::TSocket socket(NNet::TAddress{"127.0.0.1", 8888}, loop.Poller());
    socket.Bind();
    socket.Listen();

    NNet::TSocket client(NNet::TAddress{"127.0.0.1", 8888}, loop.Poller());

    NNet::TVoidSuspendedTask h1 = [](NNet::TSocket& client, TMessageHolder<TLogEntry> mes) -> NNet::TVoidSuspendedTask
    {
        co_await client.Connect();
        co_await TMessageWriter(client).Write(std::move(mes));
        co_return;
    }(client, mes);

    TMessageHolder<TMessage> received;
    NNet::TVoidSuspendedTask h2 = [](NNet::TSocket& server, TMessageHolder<TMessage>& received) -> NNet::TVoidSuspendedTask
    {
        auto client = std::move(co_await server.Accept());
        received = co_await TMessageReader(client).Read();
        co_return;
    }(socket, received);

    while (!(h1.done() && h2.done())) {
        loop.Step();
    }

    auto maybeCasted = received.Maybe<TLogEntry>();
    assert_true(maybeCasted);
    auto casted = maybeCasted.Cast();
    assert_string_equal(mes->Data, casted->Data);

    h1.destroy(); h2.destroy();
}

void test_read_write_payload(void**) {
    auto mes = NewHoldedMessage<TAppendEntriesRequest>();
    char buf[1024];
    mes.InitPayload(1337);
    for (int i = 0; i < 1337; i++) {
        snprintf(buf, sizeof(buf), "message_%d_data", i);
        mes.Payload[i] = MakeEntry(buf);
    }
    mes->Nentries = mes.PayloadSize;

    NNet::TLoop<NNet::TPoll> loop;
    NNet::TSocket socket(NNet::TAddress{"127.0.0.1", 8889}, loop.Poller());
    socket.Bind();
    socket.Listen();

    NNet::TSocket client(NNet::TAddress{"127.0.0.1", 8889}, loop.Poller());

    NNet::TVoidSuspendedTask h1 = [](NNet::TSocket& client, TMessageHolder<TMessage> mes) -> NNet::TVoidSuspendedTask
    {
        co_await client.Connect();
        co_await TMessageWriter(client).Write(std::move(mes));
        co_return;
    }(client, mes);

    TMessageHolder<TMessage> received;
    NNet::TVoidSuspendedTask h2 = [](NNet::TSocket& server, TMessageHolder<TMessage>& received) -> NNet::TVoidSuspendedTask
    {
        auto client = std::move(co_await server.Accept());
        received = co_await TMessageReader(client).Read();
        co_return;
    }(socket, received);

    while (!(h1.done() && h2.done())) {
        loop.Step();
    }

    auto maybeCasted = received.Maybe<TAppendEntriesRequest>();
    assert_true(maybeCasted);
    auto casted = maybeCasted.Cast();
    assert_int_equal(mes->Nentries, casted->Nentries);

    h1.destroy(); h2.destroy();
}

void test_server_stages_and_removes_runtime_learner(void**) {
    NNet::TLoop<NNet::TPoll> loop;
    auto timeSource = std::make_shared<TTimeSource>();
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 1, TNodeDict{});
    raft->Become(EState::LEADER);
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    TRaftServer<NNet::TSocket> server(
        loop.Poller(), std::move(listener), raft, std::make_shared<TDummyRsm>(), TNodeDict{}, timeSource);
    auto makeEndpoint = [&] {
        return std::make_shared<TNode<NNet::TSocket>>(
            [&](const NNet::TAddress& addr) { return NNet::TSocket(addr, loop.Poller()); },
            "learner", NNet::TAddress{"127.0.0.1", 65534}, timeSource);
    };
    auto endpoint = makeEndpoint();

    server.AddLearnerTarget(2, endpoint);
    assert_true(raft->GetLearnerIds() == std::vector<uint32_t>({2}));
    assert_int_equal(raft->GetNpeers(), 1);
    assert_int_equal(raft->GetNservers(), 1);
    const auto readiness = server.CheckJointConfigurationReadiness({1, 2});
    assert_true(readiness.IsLeader);
    assert_false(readiness.HasDurableMembershipStorage);
    assert_true(readiness.MissingPeerTargets.empty());
    assert_true(readiness.AddedVotersWithoutCommitAck == std::vector<uint32_t>({2}));
    assert_false(readiness.Ready());

    const auto lastIndexBeforeRejectedChanges = raft->GetLastIndex();
    bool changeRejected = false;
    try { server.ProposeJointConfiguration({1, 2}); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    changeRejected = false;
    try { server.FinalizeConfiguration(); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    changeRejected = false;
    try { server.TransferLeadership(2); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    assert_int_equal(raft->GetLastIndex(), lastIndexBeforeRejectedChanges);

    bool duplicateRejected = false;
    try {
        server.AddLearnerTarget(2, makeEndpoint());
    } catch (const std::invalid_argument&) {
        duplicateRejected = true;
    }
    assert_true(duplicateRejected);
    assert_true(raft->GetLearnerIds() == std::vector<uint32_t>({2}));

    auto removed = server.RemoveLearnerTarget(2);
    assert_true(removed == endpoint);
    assert_true(raft->GetLearnerIds().empty());
    assert_int_equal(raft->GetNpeers(), 0);

    raft->Become(EState::FOLLOWER);
    bool nonleaderRejected = false;
    try {
        server.AddLearnerTarget(3, makeEndpoint());
    } catch (const std::logic_error&) {
        nonleaderRejected = true;
    }
    assert_true(nonleaderRejected);
    assert_true(raft->GetLearnerIds().empty());
    assert_int_equal(raft->GetNpeers(), 0);

    auto followerEndpoint = makeEndpoint();
    server.RegisterPeerTarget(3, followerEndpoint);
    assert_true(raft->GetLearnerIds() == std::vector<uint32_t>({3}));
    assert_int_equal(raft->GetNservers(), 1);
    assert_int_equal(raft->GetNpeers(), 1);
    assert_true(server.UnregisterPeerTarget(3) == followerEndpoint);
    assert_true(raft->GetLearnerIds().empty());
    assert_int_equal(raft->GetNpeers(), 0);

    raft->Become(EState::LEADER);
    auto retryEndpoint = makeEndpoint();
    server.AddLearnerTarget(3, retryEndpoint);
    assert_true(raft->GetLearnerIds() == std::vector<uint32_t>({3}));
    assert_true(server.RemoveLearnerTarget(3) == retryEndpoint);
    assert_true(raft->GetLearnerIds().empty());
}

void test_server_stop_joins_accept_and_timer_tasks(void**) {
    NNet::TLoop<NNet::TPoll> loop;
    auto timeSource = std::make_shared<TTimeSource>();
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 1, TNodeDict{});
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    listener.Bind();
    listener.Listen();
    sockaddr_in bound{};
    socklen_t boundSize = sizeof(bound);
    assert_int_equal(::getsockname(listener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize), 0);
    const NNet::TAddress address(reinterpret_cast<sockaddr*>(&bound), boundSize);
    TRaftServer<NNet::TSocket> server(
        loop.Poller(), std::move(listener), raft, std::make_shared<TDummyRsm>(), TNodeDict{}, timeSource);

    server.Serve();
    assert_false(server.IsStopped());
    NNet::TSocket client(address, loop.Poller());
    bool connected = false;
    ConnectSocketAndMark(client, connected);
    for (size_t step = 0; step < 1000 && !connected; ++step)
        loop.Step();
    assert_true(connected);
    loop.Step(); // Let the accepted connection enter its pending message read.

    bool stopped = false;
    StopServerAndMark(server, stopped);
    assert_false(stopped);
    for (size_t step = 0; step < 1000 && !stopped; ++step)
        loop.Step();
    assert_true(stopped);
    assert_true(server.IsStopped());
}

void test_server_forwards_successful_membership_operations(void**) {
#ifdef __linux__
    struct TMembershipRsm : IRsm {
        TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest>, uint64_t) override { return {}; }
        TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry>, uint64_t) override { return {}; }
        TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest>) override { return {}; }
        std::string SaveSnapshot() const override { return {}; }
        void ValidateSnapshot(std::string_view, uint64_t) const override { }
        void LoadSnapshot(std::string_view, uint64_t index) override { LastAppliedIndex = index; }
        bool SupportsApplicationSnapshotInstall() const override { return true; }
    };
    struct TEndpoint : INode {
        std::function<void(TMessageHolder<TMessage>)> Route;
        bool SawTimeoutNow = false;
        void Send(TMessageHolder<TMessage> message) override {
            SawTimeoutNow |= message->Type == uint32_t(EMessageType::TIMEOUT_NOW_REQUEST);
            if (Route) Route(std::move(message));
        }
        void Drain() override { }
    };
    struct TManualTimeSource : ITimeSource {
        Time Current{};
        Time Now() override { return Current; }
    };

    const auto dir = std::filesystem::temp_directory_path() / ("miniraft-server-membership-success-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        NNet::TLoop<NNet::TPoll> loop;
        auto timeSource = std::make_shared<TManualTimeSource>();
        auto makeServer = [&](const std::shared_ptr<TRaft>& raft, const TNodeDict& peers) {
            NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
            return std::make_unique<TRaftServer<NNet::TSocket>>(
                loop.Poller(), std::move(listener), raft, std::make_shared<TMembershipRsm>(), peers, timeSource);
        };
        const std::vector<uint32_t> oldVoters{1, 2};
        const std::vector<uint32_t> newVoters{2, 3};
        std::vector<std::shared_ptr<TDurableState>> states(4);
        std::vector<std::shared_ptr<TRaft>> rafts(4);
        std::vector<TNodeDict> peers(4);
        std::vector<std::vector<std::shared_ptr<TEndpoint>>> endpoints(
            4, std::vector<std::shared_ptr<TEndpoint>>(4));

        for (uint32_t id = 1; id <= 3; ++id) {
            states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
            states[id]->BindVoters(oldVoters);
            auto initial = NewHoldedMessage<TLogEntry>();
            initial->Term = 1;
            states[id]->Append(std::move(initial));
            states[id]->CommitIndex = 1;
            states[id]->Commit();
            for (uint32_t peer = 1; peer <= 3; ++peer) {
                if (peer == id) continue;
                endpoints[id][peer] = std::make_shared<TEndpoint>();
                peers[id].emplace(peer, endpoints[id][peer]);
            }
        }
        for (uint32_t id = 1; id <= 3; ++id)
            rafts[id] = std::make_shared<TRaft>(states[id], id, peers[id], std::vector<uint32_t>{3});
        for (uint32_t id = 1; id <= 3; ++id)
            rafts[id]->SetLocalApplicationSnapshotInstallSupport(true);
        for (uint32_t from = 1; from <= 3; ++from) {
            for (uint32_t to = 1; to <= 3; ++to) {
                if (from == to) continue;
                endpoints[from][to]->Route = [&, to](auto message) {
                    rafts[to]->Process(timeSource->Now(), std::move(message));
                };
            }
        }
        auto tick = [&](uint32_t leader) {
            timeSource->Current += std::chrono::seconds(1);
            rafts[leader]->ProcessTimeout(timeSource->Now());
        };

        rafts[1]->Become(EState::LEADER);
        auto oldLeaderServer = makeServer(rafts[1], peers[1]);
        auto newLeaderServer = makeServer(rafts[2], peers[2]);
        for (unsigned i = 0; i < 8 && !rafts[1]->IsLearnerCommitAcknowledged(3); ++i) tick(1);
        assert_true(rafts[1]->IsLearnerCommitAcknowledged(3));
        assert_true(oldLeaderServer->CheckJointConfigurationReadiness(newVoters).Ready());

        const auto jointIndex = oldLeaderServer->ProposeJointConfiguration(newVoters);
        for (unsigned i = 0; i < 8 && rafts[1]->GetVolatileState()->CommitIndex < jointIndex; ++i) tick(1);
        assert_int_equal(rafts[1]->GetVolatileState()->CommitIndex, jointIndex);
        for (unsigned i = 0; i < 8; ++i) {
            bool propagated = true;
            for (uint32_t id = 2; id <= 3; ++id)
                propagated = propagated && states[id]->CommitIndex >= jointIndex;
            if (propagated) break;
            tick(1);
        }
        for (uint32_t id = 1; id <= 3; ++id)
            assert_true(rafts[id]->GetVoterIds() == (std::vector<uint32_t>{1, 2, 3}));

        for (unsigned i = 0; i < 8 &&
            (rafts[1]->GetVolatileState()->MatchIndex.at(2) < states[1]->LastLogIndex ||
             rafts[1]->GetVolatileState()->CommitAckIndex.at(2) < rafts[1]->GetVolatileState()->CommitIndex); ++i)
            tick(1);
        oldLeaderServer->TransferLeadership(2);
        assert_true(rafts[1]->CurrentStateName() == EState::FOLLOWER);
        assert_true(endpoints[1][2]->SawTimeoutNow);
        for (unsigned i = 0; i < 4 && rafts[2]->CurrentStateName() != EState::LEADER; ++i) tick(2);
        assert_true(rafts[2]->CurrentStateName() == EState::LEADER);

        for (unsigned i = 0; i < 8 &&
            rafts[2]->GetVolatileState()->CommitIndex < states[2]->LastLogIndex; ++i)
            tick(2);
        assert_true(rafts[2]->GetVolatileState()->CommitIndex > jointIndex);
        const auto stableIndex = newLeaderServer->FinalizeConfiguration();
        for (unsigned i = 0; i < 8 && rafts[2]->GetVolatileState()->CommitIndex < stableIndex; ++i) tick(2);
        assert_int_equal(rafts[2]->GetVolatileState()->CommitIndex, stableIndex);
        for (unsigned i = 0; i < 8; ++i) {
            bool propagated = true;
            for (uint32_t id = 1; id <= 3; ++id)
                propagated = propagated && states[id]->CommitIndex >= stableIndex;
            if (propagated) break;
            tick(2);
        }
        for (uint32_t id = 1; id <= 3; ++id) {
            assert_true(rafts[id]->GetVoterIds() == newVoters);
            assert_true(rafts[id]->IsVoter(id) == (id != 1));
            assert_true(states[id]->GetBoundVoters() == oldVoters);
        }
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
}

#ifdef __linux__
int RunSocketJointSnapshotMembershipTest(const std::filesystem::path& dir) {
    struct TManualTimeSource : ITimeSource {
        Time Current = std::chrono::steady_clock::now();
        Time Now() override { return Current; }
    };
    struct TSnapshotRsm : IRsm {
        std::string Value;
        size_t Loads = 0;
        TMessageHolder<TMessage> Read(TMessageHolder<TCommandRequest>, uint64_t) override { return {}; }
        TMessageHolder<TMessage> Write(TMessageHolder<TLogEntry> entry, uint64_t) override {
            const std::string_view payload(entry->Data, entry->Len - sizeof(TLogEntry));
            constexpr std::string_view snapshotPayload = "joint-snapshot-payload";
            const bool isSnapshotPayload = payload == snapshotPayload ||
                (payload.size() == snapshotPayload.size() + 1 && payload.back() == '\0' &&
                    payload.substr(0, snapshotPayload.size()) == snapshotPayload);
            if (isSnapshotPayload)
                Value.assign(256 * 1024, 'x');
            else
                Value.assign(payload);
            return {};
        }
        TMessageHolder<TLogEntry> Prepare(TMessageHolder<TCommandRequest>) override {
            return NewHoldedMessage<TLogEntry>();
        }
        std::string SaveSnapshot() const override { return Value; }
        void ValidateSnapshot(std::string_view bytes, uint64_t) const override {
            if (bytes.size() != 256 * 1024 || bytes.find_first_not_of('x') != std::string_view::npos)
                throw std::runtime_error("Unexpected joint snapshot application image");
        }
        void LoadSnapshot(std::string_view bytes, uint64_t index) override {
            ValidateSnapshot(bytes, index);
            Value.assign(bytes);
            LastAppliedIndex = index;
            ++Loads;
        }
        bool SupportsApplicationSnapshotInstall() const override { return true; }
    };
    struct TDummyPeer : INode {
        void Send(TMessageHolder<TMessage>) override { }
        void Drain() override { }
    };

    if (!std::filesystem::create_directory(dir)) return 1;

    NNet::TLoop<NNet::TPoll> loop;
    loop.Poller().SetMaxDuration(std::chrono::milliseconds(1));
    auto timeSource = std::make_shared<TManualTimeSource>();
    const std::vector<uint32_t> oldVoters{1, 2};
    const std::vector<uint32_t> newVoters{2, 3};
    std::vector<NNet::TSocket> listeners;
    std::vector<NNet::TAddress> addresses(5);
    listeners.reserve(4);
    for (uint32_t id = 1; id <= 4; ++id) {
        listeners.emplace_back(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
        auto& listener = listeners.back();
        listener.Bind();
        listener.Listen();
        sockaddr_storage bound{};
        socklen_t boundSize = sizeof(bound);
        if (::getsockname(listener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize) != 0) return 2;
        addresses[id] = NNet::TAddress(reinterpret_cast<sockaddr*>(&bound), boundSize);
    }

    std::vector<TNodeDict> peers(5);
    std::vector<std::vector<std::shared_ptr<TNode<NNet::TSocket>>>> transports(5,
        std::vector<std::shared_ptr<TNode<NNet::TSocket>>>(5));
    for (uint32_t id = 1; id <= 3; ++id) {
        for (uint32_t peer = 1; peer <= 3; ++peer) {
            if (peer == id) continue;
            auto node = std::make_shared<TNode<NNet::TSocket>>(
                [&](const NNet::TAddress& address) { return NNet::TSocket(address, loop.Poller()); },
                "membership-peer-" + std::to_string(id) + "-" + std::to_string(peer),
                addresses[peer], timeSource);
            transports[id][peer] = node;
            peers[id].emplace(peer, std::move(node));
        }
    }

    std::vector<std::shared_ptr<TDurableState>> states(5);
    std::vector<std::shared_ptr<TRaft>> rafts(5);
    std::vector<std::shared_ptr<TSnapshotRsm>> applications(5);
    std::vector<std::unique_ptr<TRaftServer<NNet::TSocket>>> servers(5);
    for (uint32_t id = 1; id <= 3; ++id) {
        states[id] = std::make_shared<TDurableState>((dir / "raft").string(), id);
        states[id]->BindVoters(oldVoters);
        auto initial = NewHoldedMessage<TLogEntry>();
        initial->Term = 1;
        states[id]->Append(std::move(initial));
        states[id]->CommitIndex = 1;
        states[id]->Commit();
        rafts[id] = std::make_shared<TRaft>(states[id], id, peers[id], std::vector<uint32_t>{3});
        applications[id] = std::make_shared<TSnapshotRsm>();
        auto* volatileState = const_cast<TVolatileState*>(rafts[id]->GetVolatileState());
        volatileState->ElectionDue = timeSource->Now() + std::chrono::hours(1);
    }
    for (uint32_t id = 1; id <= 3; ++id) {
        servers[id] = std::make_unique<TRaftServer<NNet::TSocket>>(
            loop.Poller(), std::move(listeners[id - 1]), rafts[id], applications[id],
            peers[id], timeSource);
    }

    auto drainPeers = [&] {
        for (uint32_t id = 1; id <= 4; ++id)
            for (uint32_t peer = 1; peer <= 4; ++peer)
                if (transports[id][peer]) transports[id][peer]->Drain();
    };
    auto pumpUntil = [&](const auto& ready, size_t maxSteps) {
        for (size_t step = 0; step < maxSteps && !ready(); ++step) {
            drainPeers();
            loop.Step();
        }
        return ready();
    };

    // Establish the outbound TCP connections before starting response readers,
    // so the server readers begin directly on the capability handshake.
    for (size_t step = 0; step < 1000; ++step) {
        drainPeers();
        loop.Step();
        bool connected = true;
        for (uint32_t id = 1; id <= 3; ++id)
            for (uint32_t peer = 1; peer <= 3; ++peer)
                if (transports[id][peer]) connected = connected && transports[id][peer]->IsConnected();
        if (connected) break;
        if (step == 999) return 3;
    }

    for (uint32_t id = 1; id <= 3; ++id) servers[id]->Serve();
    const auto capabilitiesReady = [&] {
        for (uint32_t id = 1; id <= 3; ++id)
            for (uint32_t peer = 1; peer <= 3; ++peer)
                if (transports[id][peer] && transports[id][peer]->IsCapabilityNegotiating()) return false;
        return true;
    };
    if (!pumpUntil(capabilitiesReady, 500)) return 4;

    rafts[1]->Become(EState::LEADER);
    auto tickLeader = [&] {
        timeSource->Current += std::chrono::seconds(1);
        rafts[1]->ProcessTimeout(timeSource->Now());
        drainPeers();
    };
    const auto learnerReady = [&] {
        return rafts[1]->IsLearnerCommitAcknowledged(3) &&
            servers[1]->CheckJointConfigurationReadiness(newVoters).Ready();
    };
    for (unsigned retry = 0; retry < 4 && !learnerReady(); ++retry) {
        tickLeader();
        if (pumpUntil(learnerReady, 500)) break;
    }
    if (!learnerReady()) {
        const auto readiness = servers[1]->CheckJointConfigurationReadiness(newVoters);
        const auto* volatileState = rafts[1]->GetVolatileState();
        std::cerr << "socket membership: commit=" << volatileState->CommitIndex
            << " last=" << states[1]->LastLogIndex << " learner-ack="
            << rafts[1]->IsLearnerCommitAcknowledged(3) << '\n';
        for (uint32_t peer = 2; peer <= 3; ++peer) {
            std::cerr << "peer " << peer << " connected=" << transports[1][peer]->IsConnected()
                << " capability-negotiating=" << transports[1][peer]->IsCapabilityNegotiating()
                << " match=" << (volatileState->MatchIndex.contains(peer) ?
                    volatileState->MatchIndex.at(peer) : 0)
                << " commit-ack=" << (volatileState->CommitAckIndex.contains(peer) ?
                    volatileState->CommitAckIndex.at(peer) : 0)
                << " last-sent=" << (volatileState->LastSentSeqno.contains(peer) ?
                    volatileState->LastSentSeqno.at(peer) : 0)
                << " last-ack=" << (volatileState->ReadAckSeqno.contains(peer) ?
                    volatileState->ReadAckSeqno.at(peer) : 0)
                << " capabilities=" << volatileState->PeerCapabilities.contains(peer) << '\n';
        }
        auto printMissing = [](const char* name, const std::vector<uint32_t>& ids) {
            std::cerr << name << ':';
            for (auto id : ids) std::cerr << ' ' << id;
            std::cerr << '\n';
        };
        printMissing("missing targets", readiness.MissingPeerTargets);
        printMissing("added voter commit ACKs", readiness.AddedVotersWithoutCommitAck);
        printMissing("snapshot support", readiness.AddedVotersWithoutSnapshotInstallSupport);
        printMissing("application snapshot install support",
            readiness.ProposedVotersWithoutApplicationSnapshotInstallSupport);
        printMissing("joint support", readiness.VotersWithoutJointConsensusSupport);
        return 5;
    }

    const auto jointIndex = servers[1]->ProposeJointConfiguration(newVoters);
    tickLeader();
    const auto jointCommitted = [&] { return rafts[1]->GetVolatileState()->CommitIndex >= jointIndex; };
    if (!pumpUntil(jointCommitted, 500)) return 6;

    // Send the newly committed index over the same sockets and ensure every
    // follower applies the joint voter union from its received AppendEntries.
    const auto jointPropagated = [&] {
        for (uint32_t id = 1; id <= 3; ++id)
            if (states[id]->CommitIndex < jointIndex ||
                rafts[id]->GetVoterIds() != std::vector<uint32_t>({1, 2, 3})) return false;
        return true;
    };
    for (unsigned retry = 0; retry < 4 && !jointPropagated(); ++retry) {
        tickLeader();
        if (pumpUntil(jointPropagated, 500)) break;
    }
    if (!jointPropagated()) {
        std::cerr << "socket membership: leader commit=" << rafts[1]->GetVolatileState()->CommitIndex
            << " joint index=" << jointIndex << '\n';
        for (uint32_t id = 1; id <= 3; ++id)
            std::cerr << "node " << id << " durable commit=" << states[id]->CommitIndex
                << " last=" << states[id]->LastLogIndex << " voters="
                << rafts[id]->GetVoterIds().size() << '\n';
        return 7;
    }
    for (uint32_t id = 1; id <= 3; ++id)
        if (states[id]->GetBoundVoters() != oldVoters) return 8;

    const auto transferReady = [&] {
        const auto* leaderState = rafts[1]->GetVolatileState();
        return leaderState->MatchIndex.contains(2) &&
            leaderState->MatchIndex.at(2) >= states[1]->LastLogIndex &&
            leaderState->CommitAckIndex.contains(2) &&
            leaderState->CommitAckIndex.at(2) >= leaderState->CommitIndex;
    };
    for (unsigned retry = 0; retry < 4 && !transferReady(); ++retry) {
        tickLeader();
        if (pumpUntil(transferReady, 500)) break;
    }
    if (!transferReady()) return 9;

    servers[1]->TransferLeadership(2);
    drainPeers();
    if (!pumpUntil([&] { return rafts[2]->CurrentStateName() == EState::LEADER; }, 2000)) return 10;

    const auto currentTermEntryCommitted = [&] {
        return rafts[2]->GetVolatileState()->CommitIndex > jointIndex &&
            states[2]->LogTerm(rafts[2]->GetVolatileState()->CommitIndex) == states[2]->CurrentTerm;
    };
    for (unsigned retry = 0; retry < 4 && !currentTermEntryCommitted(); ++retry) {
        timeSource->Current += std::chrono::seconds(1);
        rafts[2]->ProcessTimeout(timeSource->Now());
        drainPeers();
        if (pumpUntil(currentTermEntryCommitted, 500)) break;
    }
    if (!currentTermEntryCommitted()) return 11;

    auto tickNewLeader = [&] {
        timeSource->Current += std::chrono::seconds(1);
        rafts[2]->ProcessTimeout(timeSource->Now());
        drainPeers();
    };
    const auto payloadIndex = rafts[2]->Append(MakeEntry("joint-snapshot-payload"));
    const auto payloadApplied = [&] {
        if (rafts[2]->GetVolatileState()->CommitIndex < payloadIndex) return false;
        for (uint32_t id = 1; id <= 3; ++id)
            if (states[id]->CommitIndex < payloadIndex ||
                applications[id]->LastAppliedIndex < payloadIndex) return false;
        return true;
    };
    for (unsigned retry = 0; retry < 6 && !payloadApplied(); ++retry) {
        tickNewLeader();
        if (pumpUntil(payloadApplied, 1000)) break;
    }
    if (!payloadApplied()) return 14;

    const auto compactionReady = [&] {
        const auto readiness = servers[2]->CheckCompactionMembershipReadiness();
        return readiness.ActiveVotersWithoutSnapshotInstallSupport.empty() &&
            readiness.ActiveVotersWithoutApplicationSnapshotInstallSupport.empty() &&
            readiness.LocalApplicationSnapshotInstallSupported &&
            readiness.ActiveVotersWithoutConfigurationAck.empty() &&
            (!readiness.RequiresLeaderAcknowledgementBarrier || readiness.HasLeaderAcknowledgementBarrier);
    };
    for (unsigned retry = 0; retry < 6 && !compactionReady(); ++retry) {
        tickNewLeader();
        if (pumpUntil(compactionReady, 1000)) break;
    }
    if (!compactionReady()) return 15;

    const auto jointSnapshot = rafts[2]->CompactShared();
    if (!jointSnapshot->IsJoint() || jointSnapshot->Index != payloadIndex ||
        jointSnapshot->Data.size() <= TInstallSnapshotRequest::MaxChunkBytes) return 16;

    const std::vector<uint32_t> jointBootstrapVoters{1, 2, 3};
    TNodeDict learnerPeers;
    learnerPeers.emplace(1, std::make_shared<TDummyPeer>());
    auto learnerToLeader = std::make_shared<TNode<NNet::TSocket>>(
        [&](const NNet::TAddress& address) { return NNet::TSocket(address, loop.Poller()); },
        "joint-snapshot-learner-4-to-2", addresses[2], timeSource);
    learnerPeers.emplace(2, learnerToLeader);
    learnerPeers.emplace(3, std::make_shared<TDummyPeer>());
    states[4] = std::make_shared<TDurableState>((dir / "raft").string(), 4);
    states[4]->BindVoters(jointBootstrapVoters);
    applications[4] = std::make_shared<TSnapshotRsm>();
    rafts[4] = std::make_shared<TRaft>(states[4], 4, learnerPeers, std::vector<uint32_t>{4});
    servers[4] = std::make_unique<TRaftServer<NNet::TSocket>>(
        loop.Poller(), std::move(listeners[3]), rafts[4], applications[4], learnerPeers, timeSource);
    transports[4][2] = learnerToLeader;
    auto leaderToLearner = std::make_shared<TNode<NNet::TSocket>>(
        [&](const NNet::TAddress& address) { return NNet::TSocket(address, loop.Poller()); },
        "joint-snapshot-leader-2-to-4", addresses[4], timeSource);
    transports[2][4] = leaderToLearner;
    servers[2]->AddLearnerTarget(4, leaderToLearner);
    servers[4]->Serve();

    const auto learnerTransportsReady = [&] {
        return learnerToLeader->IsConnected() && !learnerToLeader->IsCapabilityNegotiating() &&
            leaderToLearner->IsConnected() && !leaderToLearner->IsCapabilityNegotiating();
    };
    if (!pumpUntil(learnerTransportsReady, 2000)) return 17;

    const auto learnerInstalledJointSnapshot = [&] {
        const auto snapshot = states[4]->GetSnapshot();
        return applications[4]->Loads == 1 && snapshot && snapshot->Index == jointSnapshot->Index &&
            snapshot->IsJoint() && applications[4]->Value == applications[2]->Value &&
            applications[4]->LastAppliedIndex == jointSnapshot->Index &&
            rafts[4]->GetVoterIds() == std::vector<uint32_t>({1, 2, 3}) &&
            rafts[4]->GetConfigurationState().Committed.IsJoint();
    };
    for (unsigned retry = 0; retry < 8 && !learnerInstalledJointSnapshot(); ++retry) {
        tickNewLeader();
        if (pumpUntil(learnerInstalledJointSnapshot, 1000)) break;
    }
    if (!learnerInstalledJointSnapshot()) {
        std::cerr << "joint snapshot socket install: learner->leader connected="
            << learnerToLeader->IsConnected() << " leader->learner connected="
            << leaderToLearner->IsConnected() << " loads=" << applications[4]->Loads
            << " snapshot=" << states[4]->SnapshotIndex << " expected=" << jointSnapshot->Index
            << " commit=" << states[4]->CommitIndex << " last=" << states[4]->LastLogIndex
            << " app-index=" << applications[4]->LastAppliedIndex << " voters=";
        for (auto voter : rafts[4]->GetVoterIds()) std::cerr << voter << ' ';
        const auto* leaderVolatile = rafts[2]->GetVolatileState();
        std::cerr << " leader-state=" << int(rafts[2]->CurrentStateName())
            << " leader-commit=" << leaderVolatile->CommitIndex << " leader-last=" << states[2]->LastLogIndex
            << " learner-next=" << (leaderVolatile->NextIndex.contains(4) ? leaderVolatile->NextIndex.at(4) : 0)
            << " learner-match=" << (leaderVolatile->MatchIndex.contains(4) ? leaderVolatile->MatchIndex.at(4) : 0)
            << " learner-capabilities=" << leaderVolatile->PeerCapabilities.contains(4)
            << " learner-snapshot-rpc=" << leaderVolatile->SnapshotRpcs.contains(4);
        if (leaderVolatile->SnapshotRpcs.contains(4)) {
            const auto& rpc = leaderVolatile->SnapshotRpcs.at(4);
            std::cerr << " snapshot-offset=" << rpc.Offset << " snapshot-sent-end=" << rpc.SentEnd
                << " snapshot-seq=" << rpc.Seqno;
        }
        std::cerr << '\n';
        return 18;
    }

    const auto stableIndex = servers[2]->FinalizeConfiguration();
    const auto stablePropagated = [&] {
        for (uint32_t id = 1; id <= 3; ++id)
            if (states[id]->CommitIndex < stableIndex || rafts[id]->GetVoterIds() != newVoters ||
                rafts[id]->IsVoter(id) != (id != 1)) return false;
        return true;
    };
    for (unsigned retry = 0; retry < 6 && !stablePropagated(); ++retry) {
        tickNewLeader();
        if (pumpUntil(stablePropagated, 500)) break;
    }
    if (!stablePropagated()) return 12;
    for (uint32_t id = 1; id <= 3; ++id)
        if (states[id]->GetBoundVoters() != oldVoters) return 13;
    const auto learnerAppliedStableConfiguration = [&] {
        return states[4]->CommitIndex >= stableIndex && applications[4]->LastAppliedIndex >= stableIndex &&
            rafts[4]->GetVoterIds() == newVoters &&
            !rafts[4]->GetConfigurationState().Committed.IsJoint();
    };
    for (unsigned retry = 0; retry < 6 && !learnerAppliedStableConfiguration(); ++retry) {
        tickNewLeader();
        if (pumpUntil(learnerAppliedStableConfiguration, 1000)) break;
    }
    if (!learnerAppliedStableConfiguration() || applications[4]->Loads != 1 ||
        applications[4]->Value != applications[2]->Value ||
        states[4]->GetBoundVoters() != jointBootstrapVoters)
        return 19;
    return 0;
}

void test_server_transitions_membership_and_installs_joint_snapshot_over_socket_transport(void**) {
    const auto dir = std::filesystem::temp_directory_path() /
        ("miniraft-socket-joint-snapshot-membership-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto child = ::fork();
    assert_true(child >= 0);
    if (child == 0) _exit(RunSocketJointSnapshotMembershipTest(dir));

    int status = 0;
    assert_int_equal(::waitpid(child, &status, 0), child);
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
    assert_true(WIFEXITED(status));
    if (WEXITSTATUS(status) != 0)
        std::cerr << "Socket membership integration failed at stage " << WEXITSTATUS(status) << '\n';
    assert_int_equal(WEXITSTATUS(status), 0);
}

int RunServingServerMembershipReaderTest(const std::filesystem::path& dir) {
    using TTlsSocket = NNet::TRetirableSslSocket<NNet::TSocket>;
    if (!std::filesystem::create_directory(dir)) return 1;

    NNet::TLoop<NNet::TPoll> loop;
    auto timeSource = std::make_shared<TTimeSource>();
    auto clientContext = NNet::TSslContext::Client();
    auto serverContext = NNet::TSslContext::ServerFromMem(testMemCert, testMemKey);
    auto makePeer = [&](const char* name) {
        return std::make_shared<TNode<TTlsSocket>>(
            [&](const NNet::TAddress& address) {
                return TTlsSocket(NNet::TSocket(address, loop.Poller()), clientContext);
            }, name, NNet::TAddress{"127.0.0.1", 1}, timeSource);
    };

    auto state = std::make_shared<TDurableState>((dir / "raft").string(), 1);
    TNodeDict peers{{2, makePeer("old-voter")}};
    auto raft = std::make_shared<TRaft>(state, 1, peers);
    NNet::TSocket rawListener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    rawListener.Bind();
    rawListener.Listen();
    TRaftServer<TTlsSocket> server(loop.Poller(), TTlsSocket(std::move(rawListener), serverContext),
        raft, std::make_shared<TDummyRsm>(), peers, timeSource,
        TRequestProcessor::TAdmissionLimits{}, TOutboundQueueLimits{0, 0});

    auto joiningPeer = makePeer("joining-voter");
    server.RegisterPeerTarget(3, joiningPeer);
    server.Serve(); // Starts readers for voters 1/2 and staged target 3.
    return server.WithSerializedRaft([&](TRaft&) -> int {
    if (!joiningPeer->HasResponseReader()) return 8;

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
    volatileState->HasCompactionSafeInitialConfigurationBarrier = true;
    auto replacedLearner = makePeer("replaced-learner");
    if (server.ReplacePeerTarget(3, replacedLearner) != joiningPeer ||
        !joiningPeer->IsRetired() || !replacedLearner->HasResponseReader() || raft->IsVoter(3) ||
        !volatileState->HasCompactionSafeInitialConfigurationBarrier ||
        volatileState->CompactionSafeConfigurationTerm != state->CurrentTerm) return 14;
    joiningPeer = replacedLearner;

    // A replacement endpoint must not inherit replication or capability proof
    // established by the old transport, and must keep the configured voter role.
    volatileState->PeerCapabilities[2].InstallSnapshot = true;
    volatileState->MatchIndex[2] = 1;
    volatileState->CommitAckIndex[2] = 1;
    volatileState->CompactionSafeConfigurationIndex = 1;
    volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
    auto oldVoter = std::dynamic_pointer_cast<TNode<TTlsSocket>>(peers.at(2));
    auto movedVoter = makePeer("moved-voter");
    if (!oldVoter || !oldVoter->HasResponseReader()) return 10;

    // A queue-budget rejection occurs before routing or Raft state changes.
    auto blockedReplacement = makePeer("blocked-replacement");
    blockedReplacement->Send(NewHoldedMessage<TCommandResponse>());
    bool replacementRejected = false;
    try {
        server.ReplacePeerTarget(2, blockedReplacement);
    } catch (const TNodeQueueFull&) {
        replacementRejected = true;
    }
    if (!replacementRejected || oldVoter->IsRetired() || !oldVoter->HasResponseReader() ||
        !raft->GetVolatileState()->PeerCapabilities.contains(2) ||
        !raft->GetVolatileState()->CommitAckIndex.contains(2)) return 13;

    if (server.ReplacePeerTarget(2, movedVoter) != oldVoter || !oldVoter->IsRetired() ||
        !movedVoter->HasResponseReader() || !raft->IsVoter(2) ||
        raft->GetVoterIds() != std::vector<uint32_t>({1, 2})) return 11;
    const auto* replacedState = raft->GetVolatileState();
    if (replacedState->PeerCapabilities.contains(2) || replacedState->MatchIndex.contains(2) ||
        replacedState->CommitAckIndex.contains(2) || replacedState->CompactionSafeConfigurationIndex ||
        replacedState->CompactionSafeConfigurationTerm ||
        replacedState->HasCompactionSafeInitialConfigurationBarrier) return 12;

    TConfigurationRecord addJoint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2}, {1, 2, 3}};
    auto addRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.LeaderCommit = 1 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 2, .Nentries = 1});
    addRequest.InitPayload(1);
    addRequest.Payload[0] = EncodeConfigurationEntry(addJoint);
    raft->Process({}, std::move(addRequest));
    if (!raft->IsVoter(3)) return 2;
    bool jointVoterRetirementRejected = false;
    try {
        (void)server.UnregisterPeerTarget(3);
    } catch (const std::invalid_argument&) {
        jointVoterRetirementRejected = true;
    }
    if (!jointVoterRetirementRejected || joiningPeer->IsRetired() ||
        !joiningPeer->HasResponseReader() || !raft->IsVoter(3)) return 15;

    // Registering another endpoint invokes the production reconciliation path
    // while the server is serving, without a timer wait or socket traffic.
    auto unrelatedLearner = makePeer("unrelated-learner");
    server.RegisterPeerTarget(4, unrelatedLearner);
    if (!unrelatedLearner->HasResponseReader()) return 9;

    TConfigurationRecord addStable{EConfigurationPhase::Stable, 2, 1, 2, {1, 2}, {1, 2, 3}};
    TConfigurationRecord removeJoint{EConfigurationPhase::Joint, 2, 2, 2, {1, 2, 3}, {1, 2}};
    TConfigurationRecord removeStable{EConfigurationPhase::Stable, 2, 3, 2, {1, 2, 3}, {1, 2}};
    auto removeRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 2,
            .LeaderCommit = 3 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 2, .Nentries = 2});
    removeRequest.InitPayload(2);
    removeRequest.Payload[0] = EncodeConfigurationEntry(addStable);
    removeRequest.Payload[1] = EncodeConfigurationEntry(removeJoint);
    raft->Process({}, std::move(removeRequest));
    if (!raft->IsVoter(3)) return 17;
    bool pendingRemovalRejected = false;
    try {
        (void)server.UnregisterPeerTarget(3);
    } catch (const std::invalid_argument&) {
        pendingRemovalRejected = true;
    }
    if (!pendingRemovalRejected || joiningPeer->IsRetired() ||
        !joiningPeer->HasResponseReader() || !raft->IsVoter(3)) return 16;

    auto finalizeRemoval = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.PrevLogIndex = 3, .PrevLogTerm = 2,
            .LeaderCommit = 4 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 2, .Nentries = 1});
    finalizeRemoval.InitPayload(1);
    finalizeRemoval.Payload[0] = EncodeConfigurationEntry(removeStable);
    raft->Process({}, std::move(finalizeRemoval));
    if (raft->IsVoter(3)) return 18;

    // Successful retirement proves the serving server started and tracked the
    // reader when peer 3 was promoted, then kept it until demotion completed.
    auto removed = server.UnregisterPeerTarget(3);
    if (removed != joiningPeer || !removed->IsRetired() ||
        raft->GetLearnerIds() != std::vector<uint32_t>({4})) return 4;

    // Reuse the peer ID before the retired endpoint's reader has necessarily
    // unwound. The replacement must get a fresh response reader when promoted.
    auto replacementPeer = makePeer("replacement-voter");
    server.RegisterPeerTarget(3, replacementPeer);
    TConfigurationRecord readdJoint{EConfigurationPhase::Joint, 2, 4, 2, {1, 2}, {1, 2, 3}};
    auto readdRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.PrevLogIndex = 4, .PrevLogTerm = 2,
            .LeaderCommit = 5 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 2, .Nentries = 1});
    readdRequest.InitPayload(1);
    readdRequest.Payload[0] = EncodeConfigurationEntry(readdJoint);
    raft->Process({}, std::move(readdRequest));
    if (!raft->IsVoter(3)) return 5;

    // Reconciliation observes the replacement while it is in the active voter union.
    server.RegisterPeerTarget(5, makePeer("reconciliation-trigger"));
    TConfigurationRecord readdStable{EConfigurationPhase::Stable, 2, 5, 2, {1, 2}, {1, 2, 3}};
    TConfigurationRecord removeAgainJoint{EConfigurationPhase::Joint, 2, 6, 2, {1, 2, 3}, {1, 2}};
    TConfigurationRecord removeAgainStable{EConfigurationPhase::Stable, 2, 7, 2, {1, 2, 3}, {1, 2}};
    auto reuseRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.PrevLogIndex = 5, .PrevLogTerm = 2,
            .LeaderCommit = 8 | TAppendEntriesRequest::JointElectionFenceLeader,
            .LeaderId = 2, .Nentries = 3});
    reuseRequest.InitPayload(3);
    reuseRequest.Payload[0] = EncodeConfigurationEntry(readdStable);
    reuseRequest.Payload[1] = EncodeConfigurationEntry(removeAgainJoint);
    reuseRequest.Payload[2] = EncodeConfigurationEntry(removeAgainStable);
    raft->Process({}, std::move(reuseRequest));
    if (raft->IsVoter(3)) return 6;

    // The demoted replacement still has a reader, so unregister must retire it.
    server.RegisterPeerTarget(6, makePeer("reconciliation-trigger"));
    auto removedReplacement = server.UnregisterPeerTarget(3);
    if (removedReplacement != replacementPeer || !removedReplacement->IsRetired() ||
        raft->GetLearnerIds() != std::vector<uint32_t>({4, 5, 6})) return 7;
    return 0;
    });
}

void test_serving_server_retires_demoted_joint_voter_reader(void**) {
    const auto dir = std::filesystem::temp_directory_path() /
        ("miniraft-serving-membership-reader-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    const auto child = ::fork();
    assert_true(child >= 0);
    if (child == 0) _exit(RunServingServerMembershipReaderTest(dir));

    int status = 0;
    assert_int_equal(::waitpid(child, &status, 0), child);
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 0);
}
#endif

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_host_endpoint_parser_validates_all_fields),
        cmocka_unit_test(test_serialized_raft_worker_bounds_queue_and_recovers_after_saturation),
        cmocka_unit_test(test_node_send_queue_bounds_data_and_coalesces_raft_retries),
        cmocka_unit_test(test_message_send_recv),
        cmocka_unit_test(test_read_write),
        cmocka_unit_test(test_read_write_payload),
        cmocka_unit_test(test_server_stages_and_removes_runtime_learner),
        cmocka_unit_test(test_server_stop_joins_accept_and_timer_tasks),
        cmocka_unit_test(test_server_forwards_successful_membership_operations),
#ifdef __linux__
        cmocka_unit_test(test_idempotent_client_times_out_and_retries_same_operation),
        cmocka_unit_test(test_server_poller_progresses_while_worker_callback_is_blocked),
        cmocka_unit_test(test_serialized_raft_task_keeps_poller_timers_running),
        cmocka_unit_test(test_server_binds_peer_rpc_source_to_authenticated_identity),
        cmocka_unit_test(test_server_transitions_membership_and_installs_joint_snapshot_over_socket_transport),
        cmocka_unit_test(test_serving_server_retires_demoted_joint_voter_reader),
#endif
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
};
