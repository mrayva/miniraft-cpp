#include <miniraft/raft.h>
#include <functional>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}

namespace {
void test_typed_allocations_reject_undersized_storage(void**) {
    for (uint32_t size : {0u, 7u, 8u, uint32_t(sizeof(TRequestVoteRequest) - 1)}) {
        for (bool explicitType : {false, true}) {
            bool rejected = false;
            try {
                if (explicitType) NewHoldedMessage<TRequestVoteRequest>(uint32_t(EMessageType::REQUEST_VOTE_REQUEST), size);
                else NewHoldedMessage<TRequestVoteRequest>(size);
            } catch (const std::invalid_argument&) { rejected = true; }
            assert_true(rejected);
        }
    }
}
void test_short_wrong_type_and_empty_casts_are_rejected(void**) {
    auto shortFrame = NewHoldedMessage<TMessage>(uint32_t(EMessageType::REQUEST_VOTE_REQUEST), sizeof(TMessage));
    auto wrongType = NewHoldedMessage<TMessage>(uint32_t(EMessageType::LOG_ENTRY), sizeof(TRequestVoteRequest));
    for (auto message : {shortFrame, wrongType, TMessageHolder<TMessage>{}}) {
        assert_false(message.IsEx());
        assert_false(message.Maybe<TRequestVoteRequest>());
        bool rejected = false;
        try { message.Cast<TRequestVoteRequest>(); } catch (const std::bad_cast&) { rejected = true; }
        assert_true(rejected);
    }
}
struct TEndpoint : INode {
    std::vector<TMessageHolder<TAppendEntriesRequest>> Sent;
    void Send(TMessageHolder<TMessage> message) override {
        if (message->Type == uint32_t(EMessageType::APPEND_ENTRIES_REQUEST))
            Sent.push_back(message.Cast<TAppendEntriesRequest>());
    }
    void Drain() override {}
};
struct TCountingState : TState {
    mutable unsigned Reads = 0;
    TMessageHolder<TLogEntry> Get(int64_t index) const override {
        ++Reads; return TState::Get(index);
    }
};
void test_memory_term_lookup_avoids_copying_full_entry(void**) {
    TCountingState state;
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1024 * 1024);
    entry->Term = 1; entry->Data[0] = 'x'; state.Append(entry);
    for (int i = 0; i < 20; ++i) assert_int_equal(state.LogTerm(1), 1);
    assert_int_equal(state.Reads, 0);
    auto copy = state.Get(1); copy->Term = 9; copy->Data[0] = 'y';
    assert_int_equal(state.LogTerm(1), 1);
    assert_int_equal(state.Get(1)->Data[0], 'x');
}
void test_memory_log_does_not_alias_proposed_or_read_entries(void**) {
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    entry->Data[0] = 'a';
    TState state;
    state.Append(entry);
    entry->Term = 9; entry->Data[0] = 'b';
    auto read = state.Get(1);
    assert_int_equal(read->Term, 1); assert_int_equal(read->Data[0], 'a');
    read->Term = 7; read->Data[0] = 'c';
    assert_int_equal(state.Get(1)->Term, 1); assert_int_equal(state.Get(1)->Data[0], 'a');
    assert_int_equal(state.LastLogTerm, 1);
}
void test_memory_log_constructor_owns_initial_entries(void**) {
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + 1);
    entry->Data[0] = 'x';
    TState state(1, 0, {entry});
    entry->Data[0] = 'y'; entry->Term = 2;
    assert_int_equal(state.Get(1)->Data[0], 'x'); assert_int_equal(state.LogTerm(1), 1);
}
void test_memory_state_copies_do_not_share_mutable_log_entries(void**) {
    TState source; source.Append(NewHoldedMessage<TLogEntry>());
    TState copied(source), assigned; assigned = source;
    source.Log[0]->Term = 4;
    assert_int_equal(copied.LogTerm(1), 1);
    assert_int_equal(assigned.LogTerm(1), 1);
    copied.Log[0]->Term = 5;
    assert_int_equal(assigned.LogTerm(1), 1);
}
void test_raft_batches_respect_budget_and_continue_replication(void**) {
    auto state = std::make_shared<TState>();
    state->CurrentTerm = 2;
    auto entry = NewHoldedMessage<TLogEntry>(1024 * 1024);
    entry->Term = 2;
    for (int i = 0; i < 100; ++i) state->Append(entry);
    auto peer = std::make_shared<TEndpoint>();
    TRaft raft(state, 1, {{2, peer}});
    raft.Become(EState::LEADER);
    auto* progress = const_cast<TVolatileState*>(raft.GetVolatileState());
    progress->BatchSize[2] = 1024;
    raft.ProcessTimeout(ITimeSource::Time{});
    assert_int_equal(peer->Sent.size(), 1);
    auto first = peer->Sent[0];
    ValidateMessageTree(first);
    assert_true(first->Nentries > 0);
    assert_true(first->Nentries < 100);
    assert_int_equal(first->Nentries, first.PayloadSize);
    auto reply = NewHoldedMessage(TMessageEx{.Src = 2, .Dst = 1, .Term = 2, .Seqno = first->Seqno},
                                 TAppendEntriesResponse{.MatchIndex = first->Nentries, .Success = 1});
    raft.Process(ITimeSource::Time{}, reply);
    raft.ProcessTimeout(ITimeSource::Time{});
    assert_int_equal(peer->Sent.size(), 2);
    auto second = peer->Sent[1];
    ValidateMessageTree(second);
    assert_int_equal(second->PrevLogIndex, first->Nentries);
    assert_int_equal(first->Nentries + second->Nentries, 100);
    assert_true(second->Nentries > 0);
}
void test_invalid_proposal_does_not_mutate_persistent_state(void**) {
    auto state = std::make_shared<TState>();
    TRaft raft(state, 1, {});
    raft.Become(EState::LEADER);
    for (uint32_t length : {0u, uint32_t(sizeof(TLogEntry) - 1), TMessageLimits{}.MaxFrameBytes + 1}) {
        auto entry = NewHoldedMessage<TLogEntry>();
        entry->Len = length;
        entry->Term = 99;
        bool rejected = false;
        try { raft.Append(entry); } catch (const TMessageError&) { rejected = true; }
        assert_true(rejected);
        assert_int_equal(state->LastLogIndex, 0);
        assert_int_equal(entry->Term, 99);
    }
}
void test_declared_length_cannot_exceed_actual_proposal_buffer(void**) {
    auto state = std::make_shared<TState>();
    TRaft raft(state, 1, {}); raft.Become(EState::LEADER);
    auto entry = NewHoldedMessage<TLogEntry>();
    entry->Len = sizeof(TLogEntry) + 8;
    bool rejected = false;
    try { raft.Append(entry); } catch (const TMessageError&) { rejected = true; }
    assert_true(rejected);
    assert_int_equal(state->LastLogIndex, 0);
}
void test_invalid_holder_extents_are_rejected_before_batch_access(void**) {
    auto shortEntry = NewHoldedMessage<TLogEntry>();
    shortEntry->Len += 8;
    assert_false(shortEntry.Maybe<TLogEntry>());
    bool rejected = false;
    try { ValidateMessageTree(shortEntry); } catch (const TMessageError&) { rejected = true; }
    assert_true(rejected);
    auto append = NewHoldedMessage(TAppendEntriesRequest{.Nentries = 2});
    append.InitPayload(1); append.Payload[0] = NewHoldedMessage<TLogEntry>();
    append.PayloadSize = 2;
    rejected = false;
    try { ValidateMessageTree(append); } catch (const TMessageError&) { rejected = true; }
    assert_true(rejected);
    append.PayloadSize = 1; append->Nentries = 1; append.Payload[0] = shortEntry;
    rejected = false;
    try { ValidateMessageTree(append); } catch (const TMessageError&) { rejected = true; }
    assert_true(rejected);
}
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_memory_term_lookup_avoids_copying_full_entry),
        cmocka_unit_test(test_memory_log_does_not_alias_proposed_or_read_entries),
        cmocka_unit_test(test_memory_log_constructor_owns_initial_entries),
        cmocka_unit_test(test_memory_state_copies_do_not_share_mutable_log_entries),
        cmocka_unit_test(test_typed_allocations_reject_undersized_storage),
        cmocka_unit_test(test_short_wrong_type_and_empty_casts_are_rejected),
        cmocka_unit_test(test_raft_batches_respect_budget_and_continue_replication),
        cmocka_unit_test(test_invalid_proposal_does_not_mutate_persistent_state),
        cmocka_unit_test(test_declared_length_cannot_exceed_actual_proposal_buffer),
        cmocka_unit_test(test_invalid_holder_extents_are_rejected_before_batch_access),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
