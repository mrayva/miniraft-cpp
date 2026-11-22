#include "../miniraft/idempotent_client.h"

#include <cmocka.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <stdexcept>
#include <system_error>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;

struct TFiles {
    fs::path Directory;
    fs::path State;

    TFiles() {
        char pattern[] = "/tmp/miniraft-client-state-XXXXXX";
        char* directory = mkdtemp(pattern);
        assert_non_null(directory);
        Directory = directory;
        State = Directory / "client.state";
    }
    ~TFiles() { std::error_code error; fs::remove_all(Directory, error); }
};

TIdempotentClientSessionState InitialState() {
    TIdempotentClientSessionState state;
    state.Initialized = true;
    state.Epoch = 7;
    state.SessionIdHigh = 0x1234;
    state.SessionIdLow = 0x5678;
    state.NextSequence = 1;
    return state;
}

TMessageHolder<TIdempotentCommandResponse> Success(
        const TMessageHolder<TIdempotentCommandRequest>& request) {
    auto response = NewHoldedMessage<TIdempotentCommandResponse>();
    response->Epoch = request->Epoch;
    response->SessionIdHigh = request->SessionIdHigh;
    response->SessionIdLow = request->SessionIdLow;
    response->Sequence = request->Sequence;
    response->Index = 9;
    response->ErrorCode = TCommandResponse::Success;
    return response;
}

void test_durable_client_session_survives_restart_and_completion(void**) {
    TFiles files;
    {
        auto store = std::make_shared<TDurableIdempotentClientStateStore>(files.State.string());
        assert_false(store->Load().has_value());
        TIdempotentClientSession client(store);
        client.StartSession(7, 0x1234, 0x5678);
        auto request = client.BeginWrite("persist-before-send");
        assert_int_equal(request->Sequence, 1);
        assert_true(client.State().HasPending);
    }

    {
        auto store = std::make_shared<TDurableIdempotentClientStateStore>(files.State.string());
        const auto recovered = store->Load();
        assert_true(recovered.has_value());
        assert_true(recovered->HasPending);
        assert_string_equal(recovered->PendingPayload.c_str(), "persist-before-send");

        TIdempotentClientSession client(store);
        auto retry = client.RetryPending(44);
        assert_int_equal(retry->Epoch, 7);
        assert_int_equal(retry->SessionIdHigh, 0x1234);
        assert_int_equal(retry->SessionIdLow, 0x5678);
        assert_int_equal(retry->Sequence, 1);
        assert_int_equal(retry->Cookie, 44);
        assert_true(client.Complete(Success(retry)));
    }

    TDurableIdempotentClientStateStore recovered(files.State.string());
    const auto completed = recovered.Load();
    assert_true(completed.has_value());
    assert_false(completed->HasPending);
    assert_int_equal(completed->NextSequence, 2);
}

void test_durable_client_state_publication_is_old_or_new_on_failure(void**) {
    const std::vector<TDurableIdempotentClientStateStore::EStage> stages = {
        TDurableIdempotentClientStateStore::EStage::TempCreated,
        TDurableIdempotentClientStateStore::EStage::Written,
        TDurableIdempotentClientStateStore::EStage::FileSynced,
        TDurableIdempotentClientStateStore::EStage::Renamed,
        TDurableIdempotentClientStateStore::EStage::DirectorySynced,
    };
    for (const auto failureStage : stages) {
        TFiles files;
        auto previous = InitialState();
        auto next = previous;
        next.HasPending = true;
        next.PendingPayload = "new operation";
        {
            TDurableIdempotentClientStateStore store(files.State.string());
            store.Save(previous);
        }
        {
            TDurableIdempotentClientStateStore store(files.State.string(), [failureStage](auto stage) {
                if (stage == failureStage) throw std::runtime_error("injected publish failure");
            });
            bool threw = false;
            try { store.Save(next); } catch (const std::runtime_error&) { threw = true; }
            assert_true(threw);
        }

        TDurableIdempotentClientStateStore recovered(files.State.string());
        const auto state = recovered.Load();
        assert_true(state.has_value());
        const bool afterRename = failureStage == TDurableIdempotentClientStateStore::EStage::Renamed ||
            failureStage == TDurableIdempotentClientStateStore::EStage::DirectorySynced;
        assert_true(*state == (afterRename ? next : previous));
        recovered.Save(next); // A pre-rename failure leaves no stale temp blocking recovery.
    }
}

void test_durable_client_state_rejects_corruption_and_second_writer(void**) {
    TFiles files;
    auto store = std::make_unique<TDurableIdempotentClientStateStore>(files.State.string());
    store->Save(InitialState());
    bool locked = false;
    try {
        TDurableIdempotentClientStateStore second(files.State.string());
    } catch (const std::system_error&) {
        locked = true;
    }
    assert_true(locked);
    store.reset();

    assert_int_equal(chmod(files.State.c_str(), 0644), 0);
    {
        TDurableIdempotentClientStateStore permissions(files.State.string());
        bool rejected = false;
        try { (void)permissions.Load(); } catch (const std::runtime_error&) { rejected = true; }
        assert_true(rejected);
    }
    assert_int_equal(chmod(files.State.c_str(), 0600), 0);

    std::fstream file(files.State, std::ios::in | std::ios::out | std::ios::binary);
    assert_true(file.good());
    file.seekg(-1, std::ios::end);
    char byte = 0;
    file.read(&byte, 1);
    assert_true(file.good());
    byte ^= 0x40;
    file.seekp(-1, std::ios::end);
    file.write(&byte, 1);
    file.close();

    TDurableIdempotentClientStateStore corrupted(files.State.string());
    bool rejected = false;
    try { (void)corrupted.Load(); } catch (const std::runtime_error&) { rejected = true; }
    assert_true(rejected);
}
}

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_durable_client_session_survives_restart_and_completion),
        cmocka_unit_test(test_durable_client_state_publication_is_old_or_new_on_failure),
        cmocka_unit_test(test_durable_client_state_rejects_corruption_and_second_writer),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
