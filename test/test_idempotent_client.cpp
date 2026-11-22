#include "../miniraft/idempotent_client.h"

#include <stdexcept>
#include <string>
#include <cstring>
#include <cmocka.h>

namespace {
struct TMemoryClientStore final : IIdempotentClientStateStore {
    std::optional<TIdempotentClientSessionState> Saved;
    bool FailNextSave = false;

    std::optional<TIdempotentClientSessionState> Load() override { return Saved; }
    void Save(const TIdempotentClientSessionState& state) override {
        if (FailNextSave) {
            FailNextSave = false;
            throw std::runtime_error("injected state-store failure");
        }
        Saved = state;
    }
};

template<class F> void Reject(F&& f) {
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}

TMessageHolder<TIdempotentCommandResponse> Response(
        const TMessageHolder<TIdempotentCommandRequest>& request, uint32_t error) {
    auto response = NewHoldedMessage<TIdempotentCommandResponse>();
    response->Epoch = request->Epoch;
    response->SessionIdHigh = request->SessionIdHigh;
    response->SessionIdLow = request->SessionIdLow;
    response->Sequence = request->Sequence;
    response->Index = error == TCommandResponse::Success ? 42 : 0;
    response->ErrorCode = error;
    return response;
}
}

void test_client_persists_pending_identity_and_retries_after_restart(void**) {
    auto store = std::make_shared<TMemoryClientStore>();
    TIdempotentClientSession client(store);
    Reject([&] { client.BeginWrite("work"); });

    client.StartSession(1, 0xabc, 0xdef);
    assert_true(store->Saved && store->Saved->Initialized);
    store->FailNextSave = true;
    Reject([&] { client.BeginWrite("work", 9); });
    assert_false(client.State().HasPending);
    assert_false(store->Saved->HasPending);

    auto request = client.BeginWrite("work", 9);
    assert_true(client.State().HasPending);
    assert_true(store->Saved->HasPending);
    assert_true(store->Saved->PendingPayload == "work");
    auto retry = client.BeginWrite("work", 10);
    assert_int_equal(retry->Epoch, request->Epoch);
    assert_int_equal(retry->SessionIdHigh, request->SessionIdHigh);
    assert_int_equal(retry->SessionIdLow, request->SessionIdLow);
    assert_int_equal(retry->Sequence, request->Sequence);
    assert_int_equal(retry->Cookie, 10);
    Reject([&] { client.BeginWrite("other", 11); });

    auto unrelated = Response(request, TCommandResponse::Success);
    unrelated->Sequence++;
    assert_false(client.Complete(unrelated));
    assert_false(client.Complete(Response(request, TCommandResponse::TimedOut)));
    assert_true(client.State().HasPending);

    TIdempotentClientSession restarted(store);
    auto afterRestart = restarted.RetryPending(12);
    assert_int_equal(afterRestart->Epoch, request->Epoch);
    assert_int_equal(afterRestart->SessionIdHigh, request->SessionIdHigh);
    assert_int_equal(afterRestart->SessionIdLow, request->SessionIdLow);
    assert_int_equal(afterRestart->Sequence, request->Sequence);
    assert_true(std::string(afterRestart->Data,
        afterRestart->Len - sizeof(TIdempotentCommandRequest)) == "work");

    store->FailNextSave = true;
    Reject([&] { restarted.Complete(Response(afterRestart, TCommandResponse::Success)); });
    assert_true(restarted.State().HasPending);
    assert_true(store->Saved->HasPending);

    TIdempotentClientSession recovered(store);
    auto finalRetry = recovered.RetryPending(13);
    assert_true(recovered.Complete(Response(finalRetry, TCommandResponse::Success)));
    assert_false(recovered.State().HasPending);
    assert_int_equal(recovered.State().NextSequence, 2);
    assert_false(store->Saved->HasPending);
    assert_int_equal(store->Saved->NextSequence, 2);

    auto second = recovered.BeginWrite("next", 14);
    assert_int_equal(second->Sequence, 2);
    Reject([&] { recovered.StartSession(1, 0xabc, 0xdef); });
    assert_true(recovered.Complete(Response(second, TCommandResponse::Success)));
    recovered.StartSession(2, 0xabc, 0xdef);
    assert_int_equal(recovered.State().Epoch, 2);
    assert_int_equal(recovered.State().NextSequence, 1);
}

void test_client_replaces_only_a_matching_terminal_session(void**) {
    auto store = std::make_shared<TMemoryClientStore>();
    TIdempotentClientSession client(store);
    client.StartSession(1, 2, 3);
    auto pending = client.BeginWrite("pending");
    auto closed = Response(pending, TCommandResponse::SessionClosed);
    assert_false(client.ReplaceTerminalSession(closed, 1, 2, 4)); // No committed index.
    closed->Index = 10;
    ++closed->Sequence;
    assert_false(client.ReplaceTerminalSession(closed, 1, 2, 4));
    --closed->Sequence;
    auto timedOut = Response(pending, TCommandResponse::TimedOut);
    assert_false(client.ReplaceTerminalSession(timedOut, 1, 2, 4));
    Reject([&] { client.ReplaceTerminalSession(closed, 1, 2, 3); });

    store->FailNextSave = true;
    Reject([&] { client.ReplaceTerminalSession(closed, 1, 2, 4); });
    assert_true(client.State().HasPending);
    assert_true(store->Saved == client.State());
    assert_true(client.ReplaceTerminalSession(closed, 1, 2, 4));

    TIdempotentClientSession recovered(store);
    assert_false(recovered.State().HasPending);
    auto next = recovered.BeginWrite("new payload");
    assert_int_equal(next->Sequence, 1);
    assert_int_equal(next->SessionIdLow, 4);
    auto stale = Response(next, TCommandResponse::StaleEpoch);
    stale->Index = 12;
    assert_true(recovered.ReplaceTerminalSession(stale, 2, 2, 5));
    assert_int_equal(store->Saved->Epoch, 2);
}

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_client_persists_pending_identity_and_retries_after_restart),
        cmocka_unit_test(test_client_replaces_only_a_matching_terminal_session),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
