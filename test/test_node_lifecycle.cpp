#include <miniraft/net/server.h>
#include <cerrno>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#include "../coroio/tests/server.crt"
#include "../coroio/tests/server.key"
#endif
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}

namespace {
unsigned UnhandledCoroutineExceptionCount = 0;
std::exception_ptr LastUnhandledCoroutineException;

void CaptureUnhandledCoroutineException(std::exception_ptr error) noexcept {
    ++UnhandledCoroutineExceptionCount;
    LastUnhandledCoroutineException = std::move(error);
}

NNet::TVoidTask ThrowFromDetachedCoroutine() {
    throw std::runtime_error("detached coroutine failure");
    co_return;
}

NNet::TVoidSuspendedTask ThrowFromSuspendedCoroutine() {
    throw std::runtime_error("suspended coroutine failure");
    co_return;
}

struct TPoller : NNet::TPollerBase {
    std::vector<std::coroutine_handle<>> Ready;
    void Tick() { ProcessTimers(); }
    void QueueReadyWrite(int fd, std::coroutine_handle<> handle) {
        ReadyEvents_.emplace_back(NNet::TEvent{fd, NNet::TEvent::WRITE, handle});
    }
    void WakePollerReady() { WakeupReadyHandles(); }
    void ResumeReady() {
        auto ready = std::move(Ready);
        Ready.clear();
        for (auto handle : ready) handle.resume();
    }
    void TickOne() {
        assert_false(Timers_.empty());
        auto timer = Timers_.top(); Timers_.pop();
        LastFiredTimer_ = timer.Id;
        if (timer.Handle) timer.Handle.resume();
    }
};
struct TManualTimeSource : ITimeSource {
    Time Current{};
    Time Now() override { return Current; }
};
struct TAsyncSocketState {
    TPoller* Poller = nullptr;
    bool Shutdown = false;
    std::vector<std::coroutine_handle<>> Pending;
};
struct TAsyncSocket {
    struct TAwaiter {
        std::shared_ptr<TAsyncSocketState> State;
        bool await_ready() const { return false; }
        void await_suspend(std::coroutine_handle<> h) { State->Pending.push_back(h); }
        ssize_t await_resume() {
            if (State->Shutdown) throw std::runtime_error("socket shut down");
            return 1;
        }
    };
    std::shared_ptr<TAsyncSocketState> State;
    TAsyncSocket() = default;
    explicit TAsyncSocket(std::shared_ptr<TAsyncSocketState> state) : State(std::move(state)) {}
    TPoller* Poller() { return State ? State->Poller : nullptr; }
    NNet::TValueTask<void> Connect() { co_return; }
    void Shutdown() noexcept {
        State->Shutdown = true;
        for (auto handle : State->Pending) State->Poller->Ready.push_back(handle);
        State->Pending.clear();
    }
    NNet::TValueTask<ssize_t> ReadSome(void*, size_t) {
        co_await TAwaiter{State};
        co_return 1;
    }
    NNet::TValueTask<ssize_t> WriteSome(const void*, size_t) {
        co_await TAwaiter{State};
        co_return 1;
    }
};
// Model the dependency's connect awaiter: a pending timer retains the coroutine
// handle, and destroying the awaiter does not unregister that timer. The timer
// is already due, so the schedule needs neither sockets nor wall-clock waits.
struct TSocket {
    TPoller* Owner = nullptr;
    bool FailWrite = false;
    auto Connect() {
        struct TAwaiter {
            TPoller* Owner;
            bool await_ready() { return false; }
            void await_suspend(std::coroutine_handle<> h) {
                Owner->AddTimer({}, h);
            }
            void await_resume() {}
        };
        return TAwaiter{Owner};
    }
    TPoller* Poller() { return Owner; }
    void Shutdown() noexcept {}
    NNet::TValueTask<ssize_t> WriteSome(const void*, size_t length) {
        if (FailWrite) throw std::runtime_error("injected write failure");
        co_return length;
    }
};
struct TDeadlineSocket {
    struct TConnectAwaiter {
        TPoller* Owner;
        NNet::TTime Deadline;
        unsigned TimerId = 0;
        static constexpr int Fd = 77;
        bool await_ready() const { return false; }
        void await_suspend(std::coroutine_handle<> handle) {
            Owner->AddWrite(Fd, handle);
            TimerId = Owner->AddTimer(Deadline, handle);
            Owner->QueueReadyWrite(Fd, handle);
        }
        void await_resume() {
            if (Owner->RemoveTimer(TimerId, Deadline)) {
                Owner->RemoveEvent(Fd);
                throw std::system_error(std::make_error_code(std::errc::timed_out));
            }
        }
    };
    TPoller* Owner = nullptr;
    TConnectAwaiter Connect(NNet::TTime deadline) { return {Owner, deadline}; }
    TPoller* Poller() { return Owner; }
    void Shutdown() noexcept { }
    NNet::TValueTask<ssize_t> WriteSome(const void*, size_t size) { co_return ssize_t(size); }
};
struct TTlsHandshakeDeadlineState {
    NNet::TTime ReadDeadline{};
    size_t TimedReadCalls = 0;
    size_t TimedWriteCalls = 0;
    bool StallWrites = false;
};
struct TTlsHandshakeDeadlineSocket {
    using TPoller = ::TPoller;

    struct TReadAwaiter {
        TPoller* Owner;
        std::shared_ptr<TTlsHandshakeDeadlineState> State;
        NNet::TTime Deadline;
        unsigned TimerId = 0;

        bool await_ready() const { return false; }
        void await_suspend(std::coroutine_handle<> handle) {
            TimerId = Owner->AddTimer(Deadline, handle);
        }
        ssize_t await_resume() {
            if (Owner->RemoveTimer(TimerId, Deadline)) {
                throw std::system_error(std::make_error_code(std::errc::timed_out));
            }
            return -1;
        }
    };

    TPoller* Owner;
    std::shared_ptr<TTlsHandshakeDeadlineState> State;

    NNet::TValueTask<TTlsHandshakeDeadlineSocket> Accept();
    std::suspend_never Connect(NNet::TTime) { return {}; }
    TReadAwaiter ReadSome(void*, size_t, NNet::TTime deadline) {
        State->ReadDeadline = deadline;
        ++State->TimedReadCalls;
        return {Owner, State, deadline};
    }
    NNet::TValueTask<ssize_t> WriteSome(const void*, size_t size, NNet::TTime deadline) {
        if (State->StallWrites) {
            ++State->TimedWriteCalls;
            co_await TReadAwaiter{Owner, State, deadline};
        }
        co_return ssize_t(size);
    }
};
NNet::TValueTask<TTlsHandshakeDeadlineSocket> TTlsHandshakeDeadlineSocket::Accept() {
    co_return TTlsHandshakeDeadlineSocket{Owner, State};
}
struct TTlsHandshakeDeadlineResult {
    bool Completed = false;
    bool TimedOut = false;
};
NNet::TVoidTask ConnectTlsWithDeadline(NNet::TSslSocket<TTlsHandshakeDeadlineSocket>& socket,
        NNet::TTime deadline, TTlsHandshakeDeadlineResult& result) {
    try {
        co_await socket.Connect(deadline);
    } catch (const std::system_error& ex) {
        result.TimedOut = ex.code() == std::make_error_code(std::errc::timed_out);
    }
    result.Completed = true;
}
using TTestTlsSocket = NNet::TSslSocket<TTlsHandshakeDeadlineSocket>;
NNet::TVoidTask AcceptFakeTlsSocket(TTestTlsSocket& listener,
        std::optional<TTestTlsSocket>& accepted) {
    accepted.emplace(co_await listener.Accept());
}
struct TTlsReadResult {
    bool Completed = false;
    bool TimedOut = false;
};
NNet::TVoidTask ReadFromTlsSocket(TTestTlsSocket& socket, TTlsReadResult& result) {
    char byte;
    try {
        co_await socket.ReadSome(&byte, 1);
    } catch (const std::system_error& ex) {
        result.TimedOut = ex.code() == std::make_error_code(std::errc::timed_out);
    }
    result.Completed = true;
}
struct TSocketDeadlineResult {
    bool Completed = false;
    bool TimedOut = false;
};
NNet::TVoidTask ExerciseSocketIODeadline(NNet::TSocket& socket, bool write,
        TSocketDeadlineResult& result) {
    char byte = 'x';
    try {
        if (write) {
            co_await socket.WriteSome(&byte, 1, NNet::TTime{});
        } else {
            co_await socket.ReadSome(&byte, 1, NNet::TTime{});
        }
    } catch (const std::system_error& ex) {
        result.TimedOut = ex.code() == std::make_error_code(std::errc::timed_out);
    }
    result.Completed = true;
}
struct TCapabilitySocketState {
    TPoller* Owner = nullptr;
    size_t ProbeWrites = 0;
    uint64_t ProbeFeatures = 0;
    size_t SessionForwardWrites = 0;
    size_t CommandWrites = 0;
};
struct TCapabilitySocket {
    struct TConnectAwaiter {
        TPoller* Owner;
        bool await_ready() const { return false; }
        void await_suspend(std::coroutine_handle<> handle) { Owner->AddTimer({}, handle); }
        void await_resume() { }
    };
    std::shared_ptr<TCapabilitySocketState> State;
    TCapabilitySocket() = default;
    explicit TCapabilitySocket(std::shared_ptr<TCapabilitySocketState> state) : State(std::move(state)) { }
    TConnectAwaiter Connect() { return {State->Owner}; }
    TPoller* Poller() { return State->Owner; }
    NNet::TValueTask<ssize_t> WriteSome(const void* data, size_t size) {
        const auto type = static_cast<const TMessage*>(data)->Type;
        if (type == uint32_t(EMessageType::PEER_CAPABILITIES_REQUEST)) {
            ++State->ProbeWrites;
            State->ProbeFeatures = static_cast<const TPeerCapabilitiesRequest*>(data)->Features;
        }
        if (type == uint32_t(EMessageType::SESSION_FORWARDED_COMMAND_REQUEST))
            ++State->SessionForwardWrites;
        if (type == uint32_t(EMessageType::COMMAND_REQUEST)) ++State->CommandWrites;
        co_return ssize_t(size);
    }
    NNet::TValueTask<ssize_t> ReadSome(void*, size_t) { co_return 0; }
};
void test_disconnect_during_pending_connect_does_not_destroy_timer_target(void**) {
    TPoller poller;
    TNode<TSocket> node([&](const auto&) { return TSocket{&poller}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());
    node.Drain(); assert_false(node.IsConnected());
    node.Disconnect(); // Must preserve the active reconnect until it completes.
    poller.Tick();
    assert_true(node.IsConnected());
    node.Disconnect(); assert_false(node.IsConnected());
}
void test_peer_capability_probe_and_legacy_fallback(void**) {
    TPoller poller;
    auto state = std::make_shared<TCapabilitySocketState>();
    state->Owner = &poller;
    TNode<TCapabilitySocket> node([&](const auto&) { return TCapabilitySocket{state}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());

    node.Drain(); poller.Tick();
    assert_true(node.IsConnected());
    assert_true(node.IsCapabilityNegotiating());
    assert_int_equal(state->ProbeWrites, 1);
    assert_true(state->ProbeFeatures & TPeerCapabilitiesRequest::SnapshotTransferManifest);
    assert_true(state->ProbeFeatures & TPeerCapabilitiesRequest::SnapshotTransferResume);
    auto response = NewHoldedMessage<TPeerCapabilitiesResponse>();
    response->Features = TPeerCapabilitiesRequest::ExtendedForwarding |
        TPeerCapabilitiesRequest::SessionizedForwarding;
    response->ConnectionId = node.GetConnectionGeneration() - 1;
    assert_false(node.CompleteCapabilityNegotiation(response, node.GetConnectionGeneration()));
    assert_true(node.IsCapabilityNegotiating());
    response->ConnectionId = node.GetConnectionGeneration();
    response->Features = uint64_t(1) << 4;
    assert_false(node.CompleteCapabilityNegotiation(response, node.GetConnectionGeneration()));
    assert_true(node.IsCapabilityNegotiating());
    response->Features = TPeerCapabilitiesRequest::ExtendedForwarding |
        TPeerCapabilitiesRequest::SessionizedForwarding |
        TPeerCapabilitiesRequest::SnapshotTransferManifest |
        TPeerCapabilitiesRequest::SnapshotTransferResume;
    response->ConnectionId = node.GetConnectionGeneration();
    assert_true(node.CompleteCapabilityNegotiation(response, node.GetConnectionGeneration()));
    assert_true(node.SupportsExtendedForwarding());
    assert_true(node.SupportsSessionizedForwarding());
    assert_true(node.SupportsSnapshotTransferManifest());
    assert_true(node.SupportsSnapshotTransferResume());

    auto generation = node.GetConnectionGeneration();
    node.Disconnect(generation);
    assert_false(node.SupportsExtendedForwarding());
    assert_false(node.SupportsSessionizedForwarding());
    assert_false(node.SupportsSnapshotTransferManifest());
    assert_false(node.SupportsSnapshotTransferResume());
    node.Drain(); poller.Tick();
    generation = node.GetConnectionGeneration();
    assert_true(node.IsCapabilityNegotiating());
    assert_int_equal(state->ProbeWrites, 2);

    // A missing response causes one legacy reconnect. A later reconnect probes
    // again so peer upgrades and downgrades are observed.
    node.FailCapabilityNegotiation(generation);
    node.Disconnect(generation);
    node.Drain(); poller.Tick();
    generation = node.GetConnectionGeneration();
    assert_true(node.IsConnected());
    assert_false(node.IsCapabilityNegotiating());
    assert_false(node.SupportsExtendedForwarding());
    assert_false(node.SupportsSessionizedForwarding());
    assert_int_equal(state->ProbeWrites, 2);

    node.Disconnect(generation);
    node.Drain(); poller.Tick();
    assert_true(node.IsCapabilityNegotiating());
    assert_int_equal(state->ProbeWrites, 3);
    node.Disconnect();
}
void test_capability_negotiation_timeout_unblocks_queue_and_accepts_late_response(void**) {
    TPoller poller;
    auto state = std::make_shared<TCapabilitySocketState>();
    state->Owner = &poller;
    auto clock = std::make_shared<TManualTimeSource>();
    TNode<TCapabilitySocket> node([&](const auto&) { return TCapabilitySocket{state}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), clock);

    node.Drain(); poller.Tick();
    assert_true(node.IsConnected());
    assert_true(node.IsCapabilityNegotiating());
    const auto generation = node.GetConnectionGeneration();

    auto request = NewHoldedMessage<TCommandRequest>();
    request->Flags = TCommandRequest::EWrite;
    assert_true(node.TrySend(request));
    node.Drain();
    assert_int_equal(state->CommandWrites, 0);

    constexpr auto timeout = std::chrono::seconds(5);
    assert_false(node.ExpireCapabilityNegotiation(clock->Now(), timeout));
    clock->Current += timeout;
    assert_true(node.ExpireCapabilityNegotiation(clock->Now(), timeout));
    assert_false(node.IsCapabilityNegotiating());
    assert_false(node.SupportsExtendedForwarding());

    node.Drain();
    assert_int_equal(state->CommandWrites, 1);

    // A response that arrives after fallback is still bound to this connection
    // generation and may safely enable capabilities for future requests.
    auto lateResponse = NewHoldedMessage<TPeerCapabilitiesResponse>();
    lateResponse->ConnectionId = generation;
    lateResponse->Version = TPeerCapabilitiesRequest::CurrentVersion;
    lateResponse->Features = TPeerCapabilitiesRequest::ExtendedForwarding |
        TPeerCapabilitiesRequest::SessionizedForwarding;
    assert_true(node.CompleteCapabilityNegotiation(lateResponse, generation));
    assert_true(node.SupportsExtendedForwarding());
    assert_true(node.SupportsSessionizedForwarding());
    node.Disconnect();
}
void test_queued_sessionized_request_is_dropped_on_legacy_fallback(void**) {
    TPoller poller;
    auto state = std::make_shared<TCapabilitySocketState>();
    state->Owner = &poller;
    TNode<TCapabilitySocket> node([&](const auto&) { return TCapabilitySocket{state}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());
    node.Drain(); poller.Tick();
    assert_true(node.IsCapabilityNegotiating());

    auto request = NewHoldedMessage<TSessionForwardedCommandRequest>();
    request->SessionId = 1;
    request->ForwardCookie = 1;
    request->Flags = TCommandRequest::EWrite | TCommandRequest::EForwarded;
    assert_true(node.TrySend(request));
    node.Drain(); // Queue remains held while the peer capability is unknown.
    assert_int_equal(state->SessionForwardWrites, 0);

    const auto generation = node.GetConnectionGeneration();
    node.FailCapabilityNegotiation(generation);
    node.Disconnect(generation);
    node.Drain(); poller.Tick(); // Legacy retry must not replay the v2 request.
    node.Drain();
    assert_true(node.IsConnected());
    assert_int_equal(state->SessionForwardWrites, 0);
    node.Disconnect();
}
void test_old_reader_failure_cannot_disconnect_writer_reconnect(void**) {
    TPoller poller;
    TNode<TSocket> node([&](const auto&) { return TSocket{&poller}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());
    node.Drain(); poller.Tick(); assert_true(node.IsConnected());
    const auto old = node.GetConnectionGeneration();
    node.Sock().FailWrite = true;
    node.Send(NewHoldedMessage<TCommandRequest>()); node.Drain();
    assert_false(node.IsConnected()); assert_true(node.GetConnectionGeneration() != old);
    node.Disconnect(old); // Old reader unwinds while the new connect is pending.
    poller.Tick(); assert_true(node.IsConnected());
    node.Disconnect(old); // It is stale even after the replacement connected.
    assert_true(node.IsConnected());
    node.Send(NewHoldedMessage<TCommandRequest>()); node.Drain();
    node.Disconnect(node.GetConnectionGeneration()); assert_false(node.IsConnected());
}
void test_connect_timeout_cancels_a_ready_event_before_retry(void**) {
    TPoller poller;
    TNode<TDeadlineSocket> node([&](const auto&) { return TDeadlineSocket{&poller}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());
    node.Drain();
    poller.TickOne(); // Timeout wins after readiness has already been queued.
    assert_false(node.IsConnected());
    poller.WakePollerReady(); // The ready queue must no longer contain the timed-out handle.
    assert_false(node.IsConnected());
    node.Retire();
    poller.TickOne(); // Let the retry sleep observe retirement and unwind.
    node.Drain();
    assert_true(node.RetirementComplete());
}
void test_peer_response_readers_start_once_for_replication_targets(void**) {
    std::unordered_map<uint32_t, std::string> peers{{1, "old voter"}, {2, "staged learner"}};
    std::unordered_set<uint32_t> started;
    std::unordered_set<uint32_t> calls;
    unsigned startCount = 0;
    const auto reconcile = [&] {
        NNet::TDetail::ReconcilePeerResponseReaders(peers, started,
            [&](uint32_t id, const std::string&) { calls.insert(id); ++startCount; });
    };

    reconcile();
    assert_true(started == std::unordered_set<uint32_t>({1, 2}));
    assert_true(calls == started); // The staged learner can return catch-up ACKs.
    assert_int_equal(startCount, 2);

    peers.emplace(3, "newly staged learner");
    reconcile();
    assert_true(started == std::unordered_set<uint32_t>({1, 2, 3}));
    assert_true(calls == started);
    assert_int_equal(startCount, 3);

    reconcile(); // Repeated processing must not launch a duplicate reader.
    assert_int_equal(startCount, 3);
}
void test_peer_response_reader_start_failure_can_retry(void**) {
    std::unordered_map<uint32_t, std::string> peers{{1, "peer"}};
    std::unordered_set<uint32_t> started;
    unsigned startCount = 0;
    bool failStart = true;
    const auto reconcile = [&] {
        NNet::TDetail::ReconcilePeerResponseReaders(peers, started,
            [&](uint32_t, const std::string&) {
                ++startCount;
                if (failStart) throw std::runtime_error("reader launch failed");
            });
    };

    assert_false(NNet::TDetail::TryPeerResponseReaderReconciliation(reconcile));
    assert_true(started.empty());
    assert_int_equal(startCount, 1);

    failStart = false;
    assert_true(NNet::TDetail::TryPeerResponseReaderReconciliation(reconcile));
    assert_true(started == std::unordered_set<uint32_t>({1}));
    assert_int_equal(startCount, 2);
    reconcile();
    assert_int_equal(startCount, 2);
}
void test_unhandled_coroutine_exception_is_reported(void**) {
    const auto previousHandler = NNet::SetUnhandledCoroutineExceptionHandler(
        &CaptureUnhandledCoroutineException);
    UnhandledCoroutineExceptionCount = 0;
    LastUnhandledCoroutineException = {};

    ThrowFromDetachedCoroutine();
    auto suspended = ThrowFromSuspendedCoroutine();
    const bool suspendedTaskCompleted = suspended.done();
    if (suspended) suspended.destroy();

    const auto reportedCount = UnhandledCoroutineExceptionCount;
    const auto reportedException = LastUnhandledCoroutineException;
    NNet::SetUnhandledCoroutineExceptionHandler(previousHandler);

    assert_true(suspendedTaskCompleted);
    assert_int_equal(reportedCount, 2);
    bool reportedExpectedError = false;
    try {
        std::rethrow_exception(reportedException);
    } catch (const std::runtime_error& ex) {
        reportedExpectedError = std::string(ex.what()) == "suspended coroutine failure";
    }
    assert_true(reportedExpectedError);
}
NNet::TVoidTask WaitForRetiredResponseReader(
        std::shared_ptr<TNode<TAsyncSocket>> node, bool& stopped) {
    char byte = 0;
    try {
        co_await node->Sock().ReadSome(&byte, 1);
    } catch (const std::exception&) {
    }
    node->MarkResponseReaderStopped();
    stopped = true;
}
void test_retirement_wakes_reader_and_writer_before_socket_cleanup(void**) {
    TPoller poller;
    auto state = std::make_shared<TAsyncSocketState>(); state->Poller = &poller;
    auto node = std::make_shared<TNode<TAsyncSocket>>(
        "peer", TAsyncSocket{state}, std::make_shared<TTimeSource>());
    node->MarkResponseReaderStarted();
    bool readerStopped = false;
    WaitForRetiredResponseReader(node, readerStopped);
    node->Send(NewHoldedMessage<TCommandRequest>());
    node->Drain(); // The writer is also blocked on this connection.

    node->Retire();
    assert_false(readerStopped);
    assert_false(node->RetirementComplete());
    poller.ResumeReady();

    assert_true(readerStopped);
    node->Drain(); // Reap both completed coroutines, then close the socket.
    assert_true(node->RetirementComplete());
}
#ifdef __linux__
NNet::TVoidTask WaitForSocketShutdown(
        std::shared_ptr<TNode<NNet::TSocket>> node, bool& stopped) {
    char byte = 0;
    try {
        const auto bytes = co_await node->Sock().ReadSome(&byte, 1);
        stopped = bytes == 0;
    } catch (const std::exception&) {
        stopped = true;
    }
    node->MarkResponseReaderStopped();
}
void test_socket_shutdown_wakes_a_real_pending_read(void**) {
    int sockets[2]{};
    assert_int_equal(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    NNet::TPoll poller;
    NNet::TSocket socket(NNet::TAddress{}, sockets[0], poller);
    auto node = std::make_shared<TNode<NNet::TSocket>>(
        "peer", std::move(socket), std::make_shared<TTimeSource>());
    node->MarkResponseReaderStarted();
    bool stopped = false;
    WaitForSocketShutdown(node, stopped);
    assert_false(stopped);

    node->Retire();
    poller.Poll();
    poller.WakeupReadyHandles();
    assert_true(stopped);
    node->Drain();
    assert_true(node->RetirementComplete());
    ::close(sockets[1]);

    TNode<NNet::TSslSocket<NNet::TSocket>> tlsNode(
        "tls-peer", NNet::TSslSocket<NNet::TSocket>{}, std::make_shared<TTimeSource>());
    assert_false(tlsNode.SupportsRetirement());
}
struct TConnectFailureResult {
    bool Completed = false;
    bool Succeeded = false;
    int Error = 0;
};
NNet::TVoidTask ConnectToRejectedPeer(NNet::TPoll& poller, const NNet::TAddress& address,
        TConnectFailureResult& result) {
    NNet::TSocket socket(address, poller);
    try {
        co_await socket.Connect(NNet::TClock::now() + std::chrono::seconds(5));
        result.Succeeded = true;
    } catch (const std::system_error& ex) {
        result.Error = ex.code().value();
    }
    result.Completed = true;
}
void test_connect_readiness_reports_socket_error(void**) {
    int reserved = ::socket(AF_INET, SOCK_STREAM, 0);
    assert_true(reserved >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    assert_int_equal(::bind(reserved, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    socklen_t addressSize = sizeof(address);
    assert_int_equal(::getsockname(reserved, reinterpret_cast<sockaddr*>(&address), &addressSize), 0);

    NNet::TPoll poller;
    TConnectFailureResult result;
    const NNet::TAddress peer(reinterpret_cast<sockaddr*>(&address), addressSize);
    ConnectToRejectedPeer(poller, peer, result);
    for (size_t step = 0; step < 100 && !result.Completed; ++step) {
        poller.Poll();
        poller.WakeupReadyHandles();
    }
    assert_true(result.Completed);
    assert_false(result.Succeeded);
    assert_int_equal(result.Error, std::make_error_code(std::errc::connection_refused).value());
    ::close(reserved);
}
void test_tls_handshake_obeys_connect_deadline(void**) {
    TPoller poller;
    auto clientContext = NNet::TSslContext::Client();
    auto state = std::make_shared<TTlsHandshakeDeadlineState>();
    NNet::TSslSocket<TTlsHandshakeDeadlineSocket> socket(
        TTlsHandshakeDeadlineSocket{&poller, state}, clientContext);
    const auto deadline = NNet::TTime{};
    TTlsHandshakeDeadlineResult result;

    auto connection = ConnectTlsWithDeadline(socket, deadline, result);
    assert_false(result.Completed);
    assert_int_equal(state->TimedReadCalls, 1);
    assert_true(state->ReadDeadline == deadline);

    poller.TickOne();
    assert_true(result.Completed);
    assert_true(result.TimedOut);
}
void test_tls_handshake_write_obeys_connect_deadline(void**) {
    TPoller poller;
    auto clientContext = NNet::TSslContext::Client();
    auto state = std::make_shared<TTlsHandshakeDeadlineState>();
    state->StallWrites = true;
    NNet::TSslSocket<TTlsHandshakeDeadlineSocket> socket(
        TTlsHandshakeDeadlineSocket{&poller, state}, clientContext);
    TTlsHandshakeDeadlineResult result;

    auto connection = ConnectTlsWithDeadline(socket, NNet::TTime{}, result);
    assert_false(result.Completed);
    assert_int_equal(state->TimedWriteCalls, 1);
    assert_int_equal(state->TimedReadCalls, 0);

    poller.TickOne();
    assert_true(result.Completed);
    assert_true(result.TimedOut);
}
void test_tls_move_assignment_releases_replaced_ssl_session(void**) {
    size_t contextFreeCalls = 0;
    {
        auto clientContext = NNet::TSslContext::Client();
        const int freeIndex = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr,
            [](void*, void* value, CRYPTO_EX_DATA*, int, long, void*) {
                if (value) ++*static_cast<size_t*>(value);
            });
        assert_true(freeIndex >= 0);
        assert_int_equal(SSL_CTX_set_ex_data(clientContext.Ctx, freeIndex, &contextFreeCalls), 1);

        TPoller poller;
        auto oldState = std::make_shared<TTlsHandshakeDeadlineState>();
        auto newState = std::make_shared<TTlsHandshakeDeadlineState>();
        NNet::TSslSocket<TTlsHandshakeDeadlineSocket> socket(
            TTlsHandshakeDeadlineSocket{&poller, oldState}, clientContext);
        NNet::TSslSocket<TTlsHandshakeDeadlineSocket> replacement(
            TTlsHandshakeDeadlineSocket{&poller, newState}, clientContext);
        socket = std::move(replacement);
    }
    assert_true(contextFreeCalls == 1);
}
void test_failed_background_tls_handshake_wakes_all_readers(void**) {
    TPoller poller;
    auto serverContext = NNet::TSslContext::ServerFromMem(testMemCert, testMemKey);
    auto state = std::make_shared<TTlsHandshakeDeadlineState>();
    TTestTlsSocket listener(TTlsHandshakeDeadlineSocket{&poller, state}, serverContext);
    std::optional<TTestTlsSocket> accepted;
    auto accept = AcceptFakeTlsSocket(listener, accepted);
    assert_true(accepted.has_value());

    TTlsReadResult firstResult;
    TTlsReadResult secondResult;
    auto first = ReadFromTlsSocket(*accepted, firstResult);
    auto second = ReadFromTlsSocket(*accepted, secondResult);
    assert_false(firstResult.Completed);
    assert_false(secondResult.Completed);
    assert_int_equal(state->TimedReadCalls, 1);

    poller.TickOne();
    assert_true(firstResult.Completed);
    assert_true(firstResult.TimedOut);
    assert_true(secondResult.Completed);
    assert_true(secondResult.TimedOut);

    TTlsReadResult lateResult;
    auto late = ReadFromTlsSocket(*accepted, lateResult);
    assert_true(lateResult.Completed);
    assert_true(lateResult.TimedOut);
}
void test_socket_read_and_write_deadlines_cancel_waits(void**) {
    for (const bool write : {false, true}) {
        int sockets[2]{};
        assert_int_equal(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
        TPoller poller;
        NNet::TSocket socket(NNet::TAddress{}, sockets[0], poller);

        if (write) {
            char fill[4096]{};
            ssize_t sent;
            do {
                sent = ::send(sockets[0], fill, sizeof(fill), MSG_DONTWAIT);
            } while (sent > 0 || (sent < 0 && errno == EINTR));
            assert_true(sent < 0);
            assert_true(errno == EAGAIN || errno == EWOULDBLOCK);
        }

        TSocketDeadlineResult result;
        auto operation = ExerciseSocketIODeadline(socket, write, result);
        assert_false(result.Completed);
        poller.TickOne();
        assert_true(result.Completed);
        assert_true(result.TimedOut);
        ::close(sockets[1]);
    }
}

using TRetirableTlsSocket = NNet::TRetirableSslSocket<NNet::TSocket>;
struct TTlsAcceptState {
    std::unique_ptr<TRetirableTlsSocket> Connection;
    bool HandshakeComplete = false;
};
NNet::TVoidTask AcceptTlsPeer(TRetirableTlsSocket& listener, NNet::TSslContext& context, TTlsAcceptState& state) {
    auto socket = co_await listener.Accept();
    state.Connection = std::make_unique<TRetirableTlsSocket>(std::move(socket));
    co_await state.Connection->AcceptHandshake();
    state.HandshakeComplete = true;
}
struct TTlsRecordPeerState {
    std::unique_ptr<NNet::TSslSocket<NNet::TSocket>> Connection;
    int NativeFd = -1;
    bool HandshakeComplete = false;
};
NNet::TVoidTask AcceptTlsRecordPeer(NNet::TSocket& listener, NNet::TSslContext& context,
        TTlsRecordPeerState& state) {
    auto socket = co_await listener.Accept();
    state.NativeFd = socket.Fd();
    state.Connection = std::make_unique<NNet::TSslSocket<NNet::TSocket>>(
        std::move(socket), context);
    co_await state.Connection->AcceptHandshake();
    state.HandshakeComplete = true;
}
NNet::TVoidTask ConnectTlsRecordPeer(NNet::TSslSocket<NNet::TSocket>& socket, bool& connected) {
    try {
        co_await socket.Connect();
        connected = true;
    } catch (const std::exception&) {
    }
}
struct TTlsRecordReadResult {
    bool Completed = false;
    bool Threw = false;
    ssize_t Bytes = -1;
};
NNet::TVoidTask ReadTlsRecord(NNet::TSslSocket<NNet::TSocket>& socket,
        TTlsRecordReadResult& result) {
    char byte;
    try {
        result.Bytes = co_await socket.ReadSome(&byte, 1);
    } catch (const std::exception&) {
        result.Threw = true;
    }
    result.Completed = true;
}
template<typename TSocket>
NNet::TVoidTask ReadTlsUntilShutdown(std::shared_ptr<TNode<TSocket>> node, bool& stopped) {
    char byte = 0;
    try {
        co_await node->Sock().ReadSome(&byte, 1);
    } catch (const std::exception&) {
    }
    node->MarkResponseReaderStopped();
    stopped = true;
}
void test_tls_shutdown_wakes_retired_peer_reader(void**) {
    using TTlsSocket = NNet::TSslSocket<NNet::TSocket>;
    NNet::TLoop<NNet::TPoll> loop;
    NNet::TSocket rawListener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    rawListener.Bind(); rawListener.Listen();
    sockaddr_in bound{};
    socklen_t boundSize = sizeof(bound);
    assert_int_equal(::getsockname(rawListener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize), 0);
    const NNet::TAddress peerAddress(reinterpret_cast<sockaddr*>(&bound), boundSize);

    auto clientContext = NNet::TSslContext::Client();
    auto serverContext = NNet::TSslContext::ServerFromMem(testMemCert, testMemKey);
    TTlsRecordPeerState accepted;
    AcceptTlsRecordPeer(rawListener, serverContext, accepted);
    auto node = std::make_shared<TNode<TTlsSocket>>(
        [&](const NNet::TAddress& address) {
            return TTlsSocket(NNet::TSocket(address, loop.Poller()), clientContext);
        }, "tls-peer", peerAddress, std::make_shared<TTimeSource>());
    node->Drain();
    for (size_t step = 0; step < 10000 && (!node->IsConnected() || !accepted.HandshakeComplete); ++step)
        loop.Step();
    assert_true(node->IsConnected());
    assert_true(accepted.HandshakeComplete);
    assert_true(node->Sock().Fd() >= 0);
    assert_true(node->SupportsRetirement());

    node->MarkResponseReaderStarted();
    bool readerStopped = false;
    ReadTlsUntilShutdown(node, readerStopped);
    assert_false(readerStopped);
    node->Retire();
    for (size_t step = 0; step < 100 && !readerStopped; ++step) loop.Step();
    assert_true(readerStopped);
    node->Drain();
    assert_true(node->RetirementComplete());
}
void test_tls_read_throws_on_malformed_record(void**) {
    NNet::TLoop<NNet::TPoll> loop;
    NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
    listener.Bind(); listener.Listen();
    sockaddr_in bound{};
    socklen_t boundSize = sizeof(bound);
    assert_int_equal(::getsockname(listener.Fd(), reinterpret_cast<sockaddr*>(&bound), &boundSize), 0);
    const NNet::TAddress peerAddress(reinterpret_cast<sockaddr*>(&bound), boundSize);

    auto clientContext = NNet::TSslContext::Client();
    auto serverContext = NNet::TSslContext::ServerFromMem(testMemCert, testMemKey);
    TTlsRecordPeerState peer;
    AcceptTlsRecordPeer(listener, serverContext, peer);
    NNet::TSslSocket<NNet::TSocket> client(
        NNet::TSocket(peerAddress, loop.Poller()), clientContext);
    bool connected = false;
    ConnectTlsRecordPeer(client, connected);
    for (size_t step = 0; step < 10000 && (!connected || !peer.HandshakeComplete); ++step)
        loop.Step();
    assert_true(connected);
    assert_true(peer.HandshakeComplete);

    TTlsRecordReadResult result;
    ReadTlsRecord(client, result);
    assert_false(result.Completed);
    const unsigned char malformedRecord[] = {0xff, 0x03, 0x03, 0x00, 0x00};
    assert_int_equal(::send(peer.NativeFd, malformedRecord, sizeof(malformedRecord), MSG_NOSIGNAL),
        sizeof(malformedRecord));
    for (size_t step = 0; step < 100 && !result.Completed; ++step)
        loop.Step();
    assert_true(result.Completed);
    assert_true(result.Threw);
}
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_disconnect_during_pending_connect_does_not_destroy_timer_target),
        cmocka_unit_test(test_peer_capability_probe_and_legacy_fallback),
        cmocka_unit_test(test_capability_negotiation_timeout_unblocks_queue_and_accepts_late_response),
        cmocka_unit_test(test_queued_sessionized_request_is_dropped_on_legacy_fallback),
        cmocka_unit_test(test_old_reader_failure_cannot_disconnect_writer_reconnect),
        cmocka_unit_test(test_connect_timeout_cancels_a_ready_event_before_retry),
        cmocka_unit_test(test_peer_response_readers_start_once_for_replication_targets),
        cmocka_unit_test(test_peer_response_reader_start_failure_can_retry),
        cmocka_unit_test(test_unhandled_coroutine_exception_is_reported),
        cmocka_unit_test(test_retirement_wakes_reader_and_writer_before_socket_cleanup),
#ifdef __linux__
        cmocka_unit_test(test_socket_shutdown_wakes_a_real_pending_read),
        cmocka_unit_test(test_connect_readiness_reports_socket_error),
        cmocka_unit_test(test_tls_handshake_obeys_connect_deadline),
        cmocka_unit_test(test_tls_handshake_write_obeys_connect_deadline),
        cmocka_unit_test(test_tls_move_assignment_releases_replaced_ssl_session),
        cmocka_unit_test(test_failed_background_tls_handshake_wakes_all_readers),
        cmocka_unit_test(test_socket_read_and_write_deadlines_cancel_waits),
        cmocka_unit_test(test_tls_shutdown_wakes_retired_peer_reader),
        cmocka_unit_test(test_tls_read_throws_on_malformed_record),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
