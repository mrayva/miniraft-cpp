#include <miniraft/configuration.h>
#include <miniraft/durable.h>
#include <miniraft/raft.h>
#include <miniraft/persist.h>
#include <miniraft/snapshot_codec.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}
namespace {
template<class F> void Reject(F f) {
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    assert_true(rejected);
}
TConfigurationRecord Joint() {
    return {EConfigurationPhase::Joint, 3, 2, 2, {1, 2, 3}, {3, 4, 5}};
}
TMessageHolder<TLogEntry> Copy(TMessageHolder<TLogEntry> entry) {
    auto copy = NewHoldedMessage<TLogEntry>(entry->Len);
    memcpy(copy.Mes, entry.Mes, entry->Len); return copy;
}
void FixChecksum(TMessageHolder<TLogEntry> entry) {
    const auto length = entry->Len - sizeof(TLogEntry);
    const auto crc = NSnapshotCodec::Checksum(std::string_view(entry->Data, length - 4));
    for (int i = 0; i < 4; ++i) entry->Data[length - 4 + i] = char(crc >> (8 * i));
}
struct TEndpoint : INode {
    unsigned Sent = 0;
    void Send(TMessageHolder<TMessage>) override { ++Sent; }
    void Drain() override {}
};
TMessageHolder<TLogEntry> Ordinary(uint64_t term) {
    auto entry = NewHoldedMessage<TLogEntry>(); entry->Term = term; return entry;
}
void test_history_validates_joint_stable_chain_and_independent_rollback(void**) {
    auto initial = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    auto prefix = initial.Observe(1, Ordinary(1));
    auto joint = Joint(); joint.Term = 2; joint.PreviousIndex = joint.PreviousTerm = 0;
    auto pending = prefix.Advance(2, EncodeConfigurationEntry(joint));
    assert_true(pending.IsJoint());
    assert_true(pending.OldVoters() == joint.OldVoters); assert_true(pending.NewVoters() == joint.NewVoters);
    assert_int_equal(pending.ConfigurationIndex(), 2); assert_int_equal(pending.ConfigurationTerm(), 2);
    auto scanned = pending.Observe(3, Ordinary(2));
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    auto stable = scanned.Advance(4, EncodeConfigurationEntry(final));
    assert_false(stable.IsJoint()); assert_true(stable.OldVoters() == joint.NewVoters);
    assert_int_equal(stable.ConfigurationIndex(), 4); assert_int_equal(stable.ThroughTerm(), 3);
    // A replacement branch starts from the unchanged prior prefix.
    auto replacement = joint; replacement.Term = 3; replacement.NewVoters = {3, 5, 6};
    auto alternate = prefix.Advance(2, EncodeConfigurationEntry(replacement));
    assert_true(alternate.NewVoters() == replacement.NewVoters);
    assert_true(pending.NewVoters() == joint.NewVoters);
    assert_false(prefix.IsJoint()); assert_int_equal(prefix.ThroughIndex(), 1);
}
void test_history_from_joint_snapshot_requires_a_consistent_identity(void**) {
    TSnapshot snapshot{8, 4, {1, 2, 3}, "opaque"};
    snapshot.JointVoters = {3, 4, 5}; snapshot.ConfigurationIndex = 5; snapshot.ConfigurationTerm = 3;
    auto history = TConfigurationHistory::FromJointSnapshot(snapshot);
    assert_true(history.IsJoint()); assert_int_equal(history.ConfigurationIndex(), 5);
    assert_int_equal(history.ConfigurationTerm(), 3); assert_int_equal(history.ThroughIndex(), 8);
    assert_int_equal(history.ThroughTerm(), 4);
    auto final = Joint(); final.Phase = EConfigurationPhase::Stable; final.Term = 4;
    final.PreviousIndex = 5; final.PreviousTerm = 3;
    auto stable = history.Advance(9, EncodeConfigurationEntry(final));
    assert_false(stable.IsJoint()); assert_true(stable.OldVoters() == final.NewVoters);
    auto fixed = snapshot; fixed.JointVoters.clear(); fixed.ConfigurationIndex = fixed.ConfigurationTerm = 0;
    Reject([&] { TConfigurationHistory::FromJointSnapshot(fixed); });
    auto inconsistent = snapshot; inconsistent.ConfigurationIndex = inconsistent.Index;
    Reject([&] { TConfigurationHistory::FromJointSnapshot(inconsistent); });
    Reject([&] { TConfigurationHistory::Stable({1}, 4, 2, 4, 3); });
    Reject([&] { TConfigurationHistory::Stable({1}, 5, 1, 4, 3); });
    Reject([&] { TConfigurationHistory::Stable({1}, 0, 0, 1, 0); });
}
void test_history_rejects_skips_wrong_predecessors_and_term_regression(void**) {
    auto base = TConfigurationHistory::Stable({1, 2, 3}, 0, 0, 0, 0);
    auto joint = Joint(); joint.Term = 2; joint.PreviousIndex = joint.PreviousTerm = 0;
    auto entry = EncodeConfigurationEntry(joint);
    Reject([&] { base.Advance(2, entry); });
    Reject([&] { base.Observe(2, Ordinary(1)); });
    Reject([&] { base.Observe(1, entry); });
    Reject([&] { base.Advance(1, Ordinary(1)); });
    auto wrong = joint; wrong.OldVoters = {1, 2, 4};
    Reject([&] { base.Advance(1, EncodeConfigurationEntry(wrong)); });
    auto prefix = base.Observe(1, Ordinary(3));
    Reject([&] { prefix.Advance(2, entry); });
    Reject([&] { prefix.Observe(2, Ordinary(2)); });
    auto valid = joint; valid.Term = 3;
    auto pending = prefix.Advance(2, EncodeConfigurationEntry(valid));
    wrong = valid; wrong.Phase = EConfigurationPhase::Stable; wrong.PreviousIndex = 2; wrong.PreviousTerm = 3;
    wrong.NewVoters = {3, 4, 6};
    Reject([&] { pending.Advance(3, EncodeConfigurationEntry(wrong)); });
    wrong = valid; wrong.PreviousIndex = 2; wrong.PreviousTerm = 3;
    Reject([&] { pending.Advance(3, EncodeConfigurationEntry(wrong)); });
    auto corrupt = EncodeConfigurationEntry(valid); corrupt->Data[0] ^= 1;
    Reject([&] { prefix.Advance(2, corrupt); });
    auto exhausted = TConfigurationHistory::Stable({1}, 0, 0, uint64_t(INT64_MAX), 1);
    Reject([&] { exhausted.Observe(UINT64_MAX, Ordinary(1)); });
    Reject([&] { exhausted.Advance(UINT64_MAX, entry); });
    assert_false(prefix.IsJoint()); assert_int_equal(prefix.ThroughIndex(), 1);
    assert_true(pending.IsJoint()); assert_int_equal(pending.ThroughIndex(), 2);
}
void test_configuration_roundtrips_both_phases_and_bounded_maximum(void**) {
    auto record = Joint();
    for (auto phase : {EConfigurationPhase::Joint, EConfigurationPhase::Stable}) {
        record.Phase = phase;
        auto entry = EncodeConfigurationEntry(record);
        assert_int_equal(entry->Flags, TLogEntry::EConfiguration);
        assert_true(DecodeConfigurationEntry(entry) == record);
    }
    record = {EConfigurationPhase::Joint, 1, 0, 0, {1}, {1, 2}};
    assert_true(DecodeConfigurationEntry(EncodeConfigurationEntry(record)) == record);
    record.OldVoters.clear(); record.NewVoters.clear();
    for (uint32_t id = 1; id <= TSnapshot::MaxVoters; ++id) {
        record.OldVoters.push_back(id); record.NewVoters.push_back(id + TSnapshot::MaxVoters);
    }
    auto max = EncodeConfigurationEntry(record);
    assert_int_equal(max->Len, sizeof(TLogEntry) + 48 + 8 * TSnapshot::MaxVoters);
    assert_true(DecodeConfigurationEntry(max) == record);
}
void test_configuration_rejects_invalid_fields_and_valid_crc_corruption(void**) {
    for (int mode = 0; mode < 12; ++mode) {
        auto record = Joint();
        if (mode == 0) record.Phase = EConfigurationPhase(0);
        if (mode == 1) record.Term = 0;
        if (mode == 2) record.PreviousIndex = UINT64_MAX;
        if (mode == 3) record.PreviousIndex = 0;
        if (mode == 4) record.PreviousTerm = 0;
        if (mode == 5) record.PreviousTerm = 4;
        if (mode == 6) record.OldVoters.clear();
        if (mode == 7) record.NewVoters = {0, 1};
        if (mode == 8) record.NewVoters = {1, 1};
        if (mode == 9) record.NewVoters = {3, 2};
        if (mode == 10) { record.Phase = EConfigurationPhase::Stable; record.PreviousIndex = record.PreviousTerm = 0; }
        if (mode == 11) record.OldVoters.resize(TSnapshot::MaxVoters + 1, 1);
        Reject([&] { EncodeConfigurationEntry(record); });
    }
    auto original = EncodeConfigurationEntry(Joint());
    const auto payload = original->Len - sizeof(TLogEntry);
    for (size_t i = 0; i < payload; ++i) {
        auto bad = Copy(original); bad->Data[i] ^= 1;
        Reject([&] { DecodeConfigurationEntry(bad); });
        bad = Copy(original); bad->Len = sizeof(TLogEntry) + i;
        Reject([&] { DecodeConfigurationEntry(bad); });
    }
    for (size_t offset : {size_t(8), size_t(12), size_t(20), size_t(28), size_t(36), size_t(40), size_t(44), size_t(56)}) {
        auto bad = Copy(original);
        const auto width = offset == 12 || offset == 20 || offset == 28 ? 8 : 4;
        for (size_t i = offset; i < offset + width; ++i) bad->Data[i] = offset >= 44 ? 0 : char(0xff);
        FixChecksum(bad); Reject([&] { DecodeConfigurationEntry(bad); });
    }
    auto bad = Copy(original); bad->Term++; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad->Flags |= TLogEntry::EStub; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad->Len += 1; Reject([&] { DecodeConfigurationEntry(bad); });
    bad = Copy(original); bad.InitPayload(1); Reject([&] { DecodeConfigurationEntry(bad); });
}
void test_configuration_rpc_and_proposal_are_rejected_before_mutation(void**) {
    auto peer = std::make_shared<TEndpoint>(); auto state = std::make_shared<TState>();
    TRaft follower(state, 2, {{1, peer}});
    auto record = Joint(); record.Term = 2; record.PreviousIndex = record.PreviousTerm = 0;
    auto config = EncodeConfigurationEntry(record);
    auto request = NewHoldedMessage(TMessageEx{.Src = 1, .Dst = 2, .Term = 2},
        TAppendEntriesRequest{.LeaderId = 1, .Nentries = 1});
    request.InitPayload(1); request.Payload[0] = config;
    const auto before = static_cast<TStateFields>(*state);
    follower.Process({}, request);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(peer->Sent, 0);
    config->Flags |= TLogEntry::EStub; follower.Process({}, request);
    assert_true(static_cast<TStateFields>(*state) == before); assert_int_equal(peer->Sent, 0);
    auto normal = NewHoldedMessage<TLogEntry>(); normal->Term = 2; request.Payload[0] = normal;
    follower.Process({}, request); assert_int_equal(state->CurrentTerm, 2); assert_int_equal(state->LastLogIndex, 1);
    assert_int_equal(peer->Sent, 1);
    auto leaderState = std::make_shared<TState>(); TRaft leader(leaderState, 1, {});
    leader.ProcessTimeout({}); assert_int_equal(leader.CurrentStateName(), EState::LEADER);
    const auto leaderBefore = static_cast<TStateFields>(*leaderState);
    auto proposed = EncodeConfigurationEntry(record);
    Reject([&] { leader.Append(proposed); });
    assert_true(static_cast<TStateFields>(*leaderState) == leaderBefore); assert_int_equal(proposed->Term, 2);
}
void test_unsupported_configuration_never_reaches_application_or_binds_voters(void**) {
    auto state = std::make_shared<TState>(); auto record = Joint(); record.Term = 1;
    record.PreviousIndex = record.PreviousTerm = 0;
    state->Append(EncodeConfigurationEntry(record));
    Reject([&] { TRaft invalid(state, 1, {}); });
    assert_true(state->GetBoundVoters().empty());
    auto live = std::make_shared<TState>(); auto raft = std::make_shared<TRaft>(live, 1, TNodeDict{});
    auto app = std::make_shared<TDummyRsm>(); TRequestProcessor processor(raft, app, {});
    live->Append(EncodeConfigurationEntry(record));
    const_cast<TVolatileState*>(raft->GetVolatileState())->CommitIndex = 1;
    Reject([&] { processor.ProcessCommitted(); });
    assert_int_equal(app->LastAppliedIndex, 0);
    auto compacted = std::make_shared<TState>();
    { TRaft bootstrap(compacted, 1, {}); }
    compacted->Append(NewHoldedMessage<TLogEntry>());
    compacted->InstallSnapshot({1, 1, {1}, "fixed image"});
    compacted->Append(EncodeConfigurationEntry(record));
    const auto fields = static_cast<TStateFields>(*compacted);
    Reject([&] { TRaft invalid(compacted, 1, {}); });
    assert_true(static_cast<TStateFields>(*compacted) == fields);
    assert_true(compacted->GetSnapshot()->Data == "fixed image");
}
#ifdef __linux__
void test_durable_restart_rejects_reserved_entry_without_rewriting(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-config-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string(); const auto path = name + ".durable.1";
    auto record = Joint(); record.Term = 1; record.PreviousIndex = record.PreviousTerm = 0;
    { auto state = std::make_shared<TDurableState>(name, 1);
      { TRaft bootstrap(state, 1, {}); }
      state->Append(EncodeConfigurationEntry(record)); }
    std::ifstream input(path, std::ios::binary); std::string before(std::istreambuf_iterator<char>(input), {}); input.close();
    { auto state = std::make_shared<TDurableState>(name, 1);
      const auto fields = static_cast<TStateFields>(*state);
      Reject([&] { TRaft invalid(state, 1, {}); });
      assert_true(static_cast<TStateFields>(*state) == fields); }
    std::ifstream after(path, std::ios::binary);
    assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == before);
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_legacy_restart_uses_safe_flag_lookup_and_preserves_files(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-legacy-config-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string();
    auto record = Joint(); record.Term = 1; record.PreviousIndex = record.PreviousTerm = 0;
    { TDiskState state(name, 2); state.Append(EncodeConfigurationEntry(record)); }
    std::vector<std::string> before;
    for (auto suffix : {".entries.2", ".index.2", ".state.2"}) {
        std::ifstream input(name + suffix, std::ios::binary);
        before.emplace_back(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    { auto state = std::make_shared<TDiskState>(name, 2);
      const auto fields = static_cast<TStateFields>(*state);
      Reject([&] { TRaft invalid(state, 2, {}); });
      assert_true(static_cast<TStateFields>(*state) == fields); }
    size_t i = 0;
    for (auto suffix : {".entries.2", ".index.2", ".state.2"}) {
        std::ifstream input(name + suffix, std::ios::binary);
        assert_true(std::string(std::istreambuf_iterator<char>(input), {}) == before[i++]);
    }
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
void test_offline_history_scan_after_durable_snapshot_and_restart(void**) {
    auto dir = std::filesystem::temp_directory_path() / ("miniraft-history-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    const auto name = (dir / "raft").string(); const auto path = name + ".durable.1";
    TConfigurationRecord joint{EConfigurationPhase::Joint, 2, 0, 0, {1}, {1, 2}};
    auto final = joint; final.Phase = EConfigurationPhase::Stable; final.Term = 3;
    final.PreviousIndex = 2; final.PreviousTerm = 2;
    {
        auto state = std::make_shared<TDurableState>(name, 1);
        { TRaft bootstrap(state, 1, {}); }
        state->Append(Ordinary(1)); state->InstallSnapshot({1, 1, {1}, "fixed image"});
        state->CurrentTerm = 3; state->Commit();
        state->Append(EncodeConfigurationEntry(joint)); state->Append(Ordinary(2));
        state->Append(EncodeConfigurationEntry(final));
    }
    std::ifstream input(path, std::ios::binary); std::string before(std::istreambuf_iterator<char>(input), {}); input.close();
    {
        auto state = std::make_shared<TDurableState>(name, 1);
        // This fixture explicitly knows that index 1 precedes any config entry.
        auto history = TConfigurationHistory::Stable({1}, 0, 0, state->SnapshotIndex, state->SnapshotTerm);
        history = history.Advance(2, state->Get(2));
        assert_true(history.IsJoint());
        history = history.Observe(3, state->Get(3));
        history = history.Advance(4, state->Get(4));
        assert_false(history.IsJoint()); assert_true(history.OldVoters() == final.NewVoters);
        assert_int_equal(history.ConfigurationIndex(), 4);
        Reject([&] { TRaft disabled(state, 1, {}); });
        assert_true(state->GetSnapshot()->Data == "fixed image");
    }
    std::ifstream after(path, std::ios::binary);
    assert_true(std::string(std::istreambuf_iterator<char>(after), {}) == before);
    std::error_code ignored; std::filesystem::remove_all(dir, ignored);
}
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_history_validates_joint_stable_chain_and_independent_rollback),
        cmocka_unit_test(test_history_from_joint_snapshot_requires_a_consistent_identity),
        cmocka_unit_test(test_history_rejects_skips_wrong_predecessors_and_term_regression),
        cmocka_unit_test(test_configuration_roundtrips_both_phases_and_bounded_maximum),
        cmocka_unit_test(test_configuration_rejects_invalid_fields_and_valid_crc_corruption),
        cmocka_unit_test(test_configuration_rpc_and_proposal_are_rejected_before_mutation),
        cmocka_unit_test(test_unsupported_configuration_never_reaches_application_or_binds_voters),
#ifdef __linux__
        cmocka_unit_test(test_durable_restart_rejects_reserved_entry_without_rewriting),
        cmocka_unit_test(test_legacy_restart_uses_safe_flag_lookup_and_preserves_files),
        cmocka_unit_test(test_offline_history_scan_after_durable_snapshot_and_restart),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
