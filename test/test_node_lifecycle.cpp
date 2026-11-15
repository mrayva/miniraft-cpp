#include <miniraft/net/server.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#ifdef __linux__
#include <sys/socket.h>
#include <unistd.h>
#endif
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}

namespace {
struct TPoller : NNet::TPollerBase {
    std::vector<std::coroutine_handle<>> Ready;
    void Tick() { ProcessTimers(); }
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
    bool DuplicateDeadline = false;
    auto Connect(NNet::TTime deadline = NNet::TTime::max()) {
        struct TAwaiter {
            TPoller* Owner;
            bool DuplicateDeadline;
            bool await_ready() { return false; }
            void await_suspend(std::coroutine_handle<> h) {
                Owner->AddTimer({}, h); // Controllable stand-in for I/O readiness.
                if (DuplicateDeadline) Owner->AddTimer({}, h);
            }
            void await_resume() {}
        };
        return TAwaiter{Owner, DuplicateDeadline && deadline != NNet::TTime::max()};
    }
    TPoller* Poller() { return Owner; }
    void Shutdown() noexcept {}
    NNet::TValueTask<ssize_t> WriteSome(const void*, size_t length) {
        if (FailWrite) throw std::runtime_error("injected write failure");
        co_return length;
    }
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
void test_connect_cannot_leave_a_second_deadline_wakeup_after_completion(void**) {
    TPoller poller;
    // The dependency's finite-deadline connect registers the same coroutine for
    // both I/O and a timer. Model a second wakeup surviving the first, as seen
    // in CI when timeout/I/O completion races with the retry sleep.
    TNode<TSocket> node([&](const auto&) { return TSocket{&poller, false, true}; }, "peer",
        NNet::TAddress("127.0.0.1", 1), std::make_shared<TTimeSource>());
    node.Drain(); poller.TickOne(); assert_true(node.IsConnected());
    node.Disconnect();
    poller.Tick(); // A leftover deadline must not resume the destroyed connector.
    assert_false(node.IsConnected());
}
void test_peer_response_readers_start_once_when_voters_are_added(void**) {
    const std::unordered_map<uint32_t, std::string> peers{{1, "old voter"}, {2, "staged learner"}, {3, "other"}};
    std::unordered_set<uint32_t> voters{1};
    std::unordered_set<uint32_t> started;
    std::vector<uint32_t> calls;
    const auto reconcile = [&] {
        NNet::TDetail::ReconcilePeerResponseReaders(peers,
            [&](uint32_t id) { return voters.contains(id); }, started,
            [&](uint32_t id, const std::string&) { calls.push_back(id); });
    };

    reconcile();
    assert_true(started == std::unordered_set<uint32_t>{1});
    assert_true(calls == std::vector<uint32_t>{1});

    voters.insert(2); // Joint configuration activates the staged peer.
    reconcile();
    assert_true(started == std::unordered_set<uint32_t>({1, 2}));
    assert_true(calls == std::vector<uint32_t>({1, 2}));

    reconcile(); // Repeated processing must not launch a duplicate reader.
    assert_true(calls == std::vector<uint32_t>({1, 2}));
    voters.erase(1); // A demoted peer's in-flight reader remains tracked.
    reconcile();
    assert_true(started == std::unordered_set<uint32_t>({1, 2}));
    assert_true(calls == std::vector<uint32_t>({1, 2}));
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
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_disconnect_during_pending_connect_does_not_destroy_timer_target),
        cmocka_unit_test(test_old_reader_failure_cannot_disconnect_writer_reconnect),
        cmocka_unit_test(test_connect_cannot_leave_a_second_deadline_wakeup_after_completion),
        cmocka_unit_test(test_peer_response_readers_start_once_when_voters_are_added),
        cmocka_unit_test(test_retirement_wakes_reader_and_writer_before_socket_cleanup),
#ifdef __linux__
        cmocka_unit_test(test_socket_shutdown_wakes_a_real_pending_read),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
