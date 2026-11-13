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
#endif
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_configuration_roundtrips_both_phases_and_bounded_maximum),
        cmocka_unit_test(test_configuration_rejects_invalid_fields_and_valid_crc_corruption),
        cmocka_unit_test(test_configuration_rpc_and_proposal_are_rejected_before_mutation),
        cmocka_unit_test(test_unsupported_configuration_never_reaches_application_or_binds_voters),
#ifdef __linux__
        cmocka_unit_test(test_durable_restart_rejects_reserved_entry_without_rewriting),
        cmocka_unit_test(test_legacy_restart_uses_safe_flag_lookup_and_preserves_files),
#endif
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
