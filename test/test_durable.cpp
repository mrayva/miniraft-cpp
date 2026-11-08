#include <miniraft/durable.h>
#include <miniraft/persist.h>
#include <miniraft/raft.h>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <iterator>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <csignal>
#include <unistd.h>
extern "C" {
#include <cmocka.h>
}

namespace {
namespace fs = std::filesystem;
using TStage = TDurableState::EStage;
struct TFiles {
    fs::path Directory;
    std::string Name;
    TFiles() {
        static unsigned next = 0;
        Directory = fs::temp_directory_path() / ("miniraft-durable-" +
            std::to_string(getpid()) + "-" + std::to_string(next++));
        assert_true(fs::create_directory(Directory));
        Name = (Directory / "raft").string();
    }
    ~TFiles() { std::error_code ignored; fs::remove_all(Directory, ignored); }
    fs::path Image() const { return Name + ".durable.1"; }
    void Seed();
};
TMessageHolder<TLogEntry> Entry(uint64_t term, const std::string& data) {
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + data.size());
    entry->Term = term;
    memcpy(entry->Data, data.data(), data.size());
    return entry;
}
void TFiles::Seed() {
    TDurableState state(Name, 1);
    state.CurrentTerm = 3;
    state.VotedFor = 2;
    state.Append(Entry(1, "first"));
    state.Append(Entry(2, "second"));
}
std::string Data(TDurableState& state, int64_t index) {
    auto entry = state.Get(index);
    return std::string(entry->Data, entry->Len - sizeof(TLogEntry));
}
enum class EOperation { Append, Truncate, Vote, Replace };
void Operate(TDurableState& state, EOperation operation) {
    if (operation == EOperation::Truncate) state.RemoveLast();
    else if (operation == EOperation::Vote) {
        state.CurrentTerm = 4;
        state.VotedFor = 3;
        state.Commit();
    } else state.Append(Entry(3, "replacement"));
}
void Check(TDurableState& state, EOperation operation, bool updated) {
    assert_int_equal(state.CurrentTerm, updated && operation == EOperation::Vote ? 4 : 3);
    assert_int_equal(state.VotedFor, updated && operation == EOperation::Vote ? 3 : 2);
    const auto oldCount = operation == EOperation::Replace ? 1 : 2;
    const auto count = !updated ? oldCount : operation == EOperation::Truncate ? 1 :
        operation == EOperation::Vote ? 2 : oldCount + 1;
    assert_int_equal(state.LastLogIndex, count);
    assert_string_equal(Data(state, 1).c_str(), "first");
    if (count >= 2) {
        const bool replaced = updated && operation == EOperation::Replace;
        assert_string_equal(Data(state, 2).c_str(), replaced ? "replacement" : "second");
        assert_int_equal(state.Get(2)->Term, replaced ? 3 : 2);
    }
    if (count == 3) assert_string_equal(Data(state, 3).c_str(), "replacement");
    assert_int_equal(state.LastLogTerm, state.Get(count)->Term);
}
void Wait(pid_t child, int expected) {
    int status;
    assert_int_equal(waitpid(child, &status, 0), child);
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), expected);
}

void test_crash_at_every_publication_boundary(void**) {
    for (auto operation : {EOperation::Append, EOperation::Truncate, EOperation::Vote, EOperation::Replace}) {
        for (auto stage : {TStage::TempCreated, TStage::Written, TStage::FileSynced,
                           TStage::Renamed, TStage::DirectorySynced}) {
            TFiles files;
            files.Seed();
            const auto child = fork();
            assert_true(child >= 0);
            if (child == 0) {
                try {
                    bool armed = false;
                    TDurableState state(files.Name, 1, [&](auto point) {
                        if (armed && point == stage) _exit(90);
                    });
                    if (operation == EOperation::Replace) state.RemoveLast();
                    armed = true;
                    Operate(state, operation);
                } catch (...) { _exit(2); }
                _exit(3);
            }
            Wait(child, 90);
            TDurableState recovered(files.Name, 1);
            Check(recovered, operation, stage >= TStage::Renamed);
        }
    }
}

void test_acknowledged_updates_survive_immediate_exit(void**) {
    for (auto operation : {EOperation::Append, EOperation::Truncate, EOperation::Vote, EOperation::Replace}) {
        TFiles files;
        files.Seed();
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDurableState state(files.Name, 1);
                if (operation == EOperation::Replace) state.RemoveLast();
                Operate(state, operation);
                _exit(0); // No destructors or stream flushing after acknowledgement.
            } catch (...) { _exit(2); }
        }
        Wait(child, 0);
        TDurableState recovered(files.Name, 1);
        Check(recovered, operation, true);
    }
}

void test_publication_order(void**) {
    TFiles files;
    files.Seed();
    std::vector<TStage> stages;
    TDurableState state(files.Name, 1, [&](auto stage) { stages.push_back(stage); });
    state.CurrentTerm = 4;
    state.Commit();
    const std::vector<TStage> expected{TStage::TempCreated, TStage::Written, TStage::FileSynced,
                                      TStage::Renamed, TStage::DirectorySynced};
    assert_true(stages == expected);
}

void test_live_publication_failure_is_fatal(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDurableState state(files.Name, 1, [](auto stage) {
                if (stage == TStage::FileSynced) throw std::runtime_error("injected failure");
            });
            state.CurrentTerm = 4;
            state.Commit();
        } catch (...) { _exit(74); }
        _exit(75);
    }
    Wait(child, 73);
    TDurableState recovered(files.Name, 1);
    assert_int_equal(recovered.CurrentTerm, 3);
    assert_int_equal(recovered.VotedFor, 2);
}

void test_write_error_is_fatal_and_preserves_image(void**) {
    TFiles files;
    files.Seed();
    const auto child = fork();
    assert_true(child >= 0);
    if (child == 0) {
        std::set_terminate([] { _exit(73); });
        try {
            TDurableState state(files.Name, 1);
            struct rlimit limit;
            if (getrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(76);
            limit.rlim_cur = 0;
            if (setrlimit(RLIMIT_FSIZE, &limit) != 0) _exit(76);
            std::signal(SIGXFSZ, SIG_IGN);
            state.Append(Entry(3, "must not be acknowledged"));
        } catch (...) { _exit(74); }
        _exit(75);
    }
    Wait(child, 73);
    TDurableState recovered(files.Name, 1);
    Check(recovered, EOperation::Append, false);
}

void test_interrupted_initialization_is_not_reset(void**) {
    for (auto stage : {TStage::TempCreated, TStage::Written, TStage::FileSynced,
                       TStage::Renamed, TStage::DirectorySynced}) {
        TFiles files;
        const auto child = fork();
        assert_true(child >= 0);
        if (child == 0) {
            try {
                TDurableState state(files.Name, 1, [&](auto point) {
                    if (point == stage) _exit(90);
                });
            } catch (...) { _exit(2); }
            _exit(3);
        }
        Wait(child, 90);
        bool rejected = false;
        try {
            TDurableState recovered(files.Name, 1);
            assert_int_equal(recovered.CurrentTerm, 1);
            assert_int_equal(recovered.VotedFor, 0);
            assert_int_equal(recovered.LastLogIndex, 0);
        } catch (const std::runtime_error&) { rejected = true; }
        assert_int_equal(rejected, stage < TStage::Renamed);
    }
}

void Reject(const TFiles& files) {
    bool rejected = false;
    try { TDurableState state(files.Name, 1); }
    catch (const std::runtime_error&) { rejected = true; }
    assert_true(rejected);
}
void test_corrupt_images_are_rejected(void**) {
    for (const auto offset : {0, 8, 16, 20, 30, 45}) {
        TFiles files;
        files.Seed();
        std::fstream file(files.Image(), std::ios::in | std::ios::out | std::ios::binary);
        file.seekg(offset);
        char byte;
        file.read(&byte, 1);
        byte ^= 1;
        file.seekp(offset);
        file.write(&byte, 1);
        file.close();
        Reject(files);
    }
    for (const auto size : {0, 7, 19, 21, 40}) {
        TFiles files;
        files.Seed();
        fs::resize_file(files.Image(), size);
        Reject(files);
    }
}
void test_single_writer_lock(void**) {
    TFiles files;
    files.Seed();
    {
        TDurableState writer(files.Name, 1);
        Reject(files);
    }
    TDurableState afterClose(files.Name, 1);
    assert_int_equal(afterClose.CurrentTerm, 3);
}
void test_missing_image_is_not_recreated(void**) {
    TFiles files;
    files.Seed();
    fs::remove(files.Image());
    Reject(files);
    assert_false(fs::exists(files.Image()));
}
void test_legacy_store_is_not_silently_migrated(void**) {
    TFiles files;
    { TDiskState legacy(files.Name, 1); }
    Reject(files);
    assert_false(fs::exists(files.Image()));
}
void test_get_and_append_do_not_share_mutable_entries(void**) {
    TFiles files;
    {
        TDurableState state(files.Name, 1);
        auto entry = Entry(1, "data");
        state.Append(entry);
        entry->Term = 100;
        auto result = state.Get(1);
        assert_int_equal(result->Term, 1);
        result->Term = 99;
        assert_int_equal(state.Get(1)->Term, 1);
        state.Commit();
    }
    TDurableState state(files.Name, 1);
    assert_int_equal(state.Get(1)->Term, 1);
}
void test_raft_election_and_application_replay_after_restart(void**) {
    TFiles files;
    uint64_t term = 0, index = 0;
    for (int restart = 0; restart < 2; ++restart) {
        auto state = std::make_shared<TDurableState>(files.Name, 1);
        auto raft = std::make_shared<TRaft>(state, 1, TNodeDict{});
        raft->ProcessTimeout(ITimeSource::Time{});
        assert_int_equal(raft->CurrentStateName(), EState::LEADER);
        assert_true(state->CurrentTerm > term);
        term = state->CurrentTerm;
        if (restart == 0) {
            index = raft->Append(Entry(term, "application-command"));
            raft->ProcessTimeout(ITimeSource::Time{});
        }
        assert_true(raft->GetVolatileState()->CommitIndex >= index);
        assert_string_equal(Data(*state, index).c_str(), "application-command");
        auto rsm = std::make_shared<TDummyRsm>();
        TRequestProcessor processor(raft, rsm, {});
        processor.ProcessCommitted();
        assert_int_equal(rsm->LastAppliedIndex, state->LastLogIndex);
    }
}
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_crash_at_every_publication_boundary),
        cmocka_unit_test(test_acknowledged_updates_survive_immediate_exit),
        cmocka_unit_test(test_publication_order),
        cmocka_unit_test(test_live_publication_failure_is_fatal),
        cmocka_unit_test(test_write_error_is_fatal_and_preserves_image),
        cmocka_unit_test(test_interrupted_initialization_is_not_reset),
        cmocka_unit_test(test_corrupt_images_are_rejected),
        cmocka_unit_test(test_single_writer_lock),
        cmocka_unit_test(test_missing_image_is_not_recreated),
        cmocka_unit_test(test_legacy_store_is_not_silently_migrated),
        cmocka_unit_test(test_get_and_append_do_not_share_mutable_entries),
        cmocka_unit_test(test_raft_election_and_application_replay_after_restart),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
