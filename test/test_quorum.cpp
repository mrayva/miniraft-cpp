#include <miniraft/quorum.h>
#include <miniraft/raft.h>
#include <bit>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}
namespace {
std::vector<uint32_t> Members(unsigned mask) {
    std::vector<uint32_t> out;
    for (uint32_t id = 1; id <= 5; ++id) if (mask & (1u << (id - 1))) out.push_back(id);
    return out;
}
bool Majority(unsigned members, unsigned votes) {
    return std::popcount(members & votes) > std::popcount(members) / 2;
}
void test_stable_quorum_matches_strict_majority_order_statistic(void**) {
    for (uint32_t count = 1; count <= 8; ++count) {
        std::vector<uint32_t> voters;
        for (uint32_t id = 1; id <= count; ++id) voters.push_back(id);
        TQuorum quorum(voters);
        for (uint64_t seed = 0; seed < 64; ++seed) {
            auto value = [&](uint32_t id) { return (seed * 7 + id * id * 3) % 17; };
            std::vector<uint64_t> expected;
            for (auto id : voters) expected.push_back(value(id));
            std::sort(expected.begin(), expected.end());
            assert_int_equal(quorum.Value(value), expected[(count - 1) / 2]);
        }
    }
}
void test_all_small_joint_election_quorums(void**) {
    for (unsigned old = 1; old < 32; ++old) for (unsigned next = 1; next < 32; ++next) {
        TQuorum quorum(Members(old), Members(next));
        for (unsigned mask = 0; mask < 32; ++mask) {
            std::unordered_set<uint32_t> votes{99};
            for (auto id : Members(mask)) votes.insert(id);
            assert_int_equal(quorum.HasQuorum(votes), Majority(old, mask) && Majority(next, mask));
            for (uint32_t self = 1; self <= 6; ++self) {
                const auto withSelf = self <= 5 ? mask | (1u << (self - 1)) : mask;
                assert_int_equal(quorum.HasQuorum(votes, self), Majority(old, withSelf) && Majority(next, withSelf));
            }
        }
    }
}
void test_joint_commit_and_read_watermarks_need_both_majorities(void**) {
    for (unsigned old = 1; old < 32; ++old) for (unsigned next = 1; next < 32; ++next) {
        TQuorum quorum(Members(old), Members(next));
        for (unsigned pattern = 0; pattern < 32; ++pattern) {
            const auto value = [&](uint32_t id) -> uint64_t {
                return pattern & (1u << (id - 1)) ? 11 - id : id % 3;
            };
            uint64_t expected = 0;
            for (uint64_t threshold = 1; threshold <= 10; ++threshold) {
                unsigned matched = 0;
                for (uint32_t id = 1; id <= 5; ++id) if (value(id) >= threshold) matched |= 1u << (id - 1);
                if (Majority(old, matched) && Majority(next, matched)) expected = threshold;
            }
            assert_int_equal(quorum.Value(value), expected);
        }
    }
}
void test_invalid_configurations_are_rejected(void**) {
    for (auto bad : {std::vector<uint32_t>{}, std::vector<uint32_t>{0},
                     std::vector<uint32_t>{1, 1}, std::vector<uint32_t>{2, 1}}) {
        bool rejected = false;
        try { TQuorum invalid(bad); } catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected); rejected = false;
        try { TQuorum invalid({1}, bad); } catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
    }
}
void test_joint_commit_respects_both_majorities_and_current_term(void**) {
    TState state; state.CurrentTerm = 2;
    auto old = NewHoldedMessage<TLogEntry>(); old->Term = 1; state.Append(old);
    auto current = NewHoldedMessage<TLogEntry>(); current->Term = 2; state.Append(current);
    TQuorum quorum({1, 2, 3}, {3, 4, 5});
    TVolatileState progress;
    progress.MatchIndex[2] = 2; progress.MatchIndex[99] = 2;
    progress.CommitAdvance(quorum, 1, state); assert_int_equal(progress.CommitIndex, 0);
    progress.MatchIndex[3] = 1; progress.MatchIndex[4] = 1;
    progress.CommitAdvance(quorum, 1, state); assert_int_equal(progress.CommitIndex, 0);
    progress.MatchIndex[3] = 2;
    progress.CommitAdvance(quorum, 1, state); assert_int_equal(progress.CommitIndex, 0);
    progress.MatchIndex[4] = 2;
    progress.CommitAdvance(quorum, 1, state); assert_int_equal(progress.CommitIndex, 2);
    progress.MatchIndex.clear();
    progress.CommitAdvance(quorum, 1, state); assert_int_equal(progress.CommitIndex, 2);
}
}
int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_stable_quorum_matches_strict_majority_order_statistic),
        cmocka_unit_test(test_all_small_joint_election_quorums),
        cmocka_unit_test(test_joint_commit_and_read_watermarks_need_both_majorities),
        cmocka_unit_test(test_invalid_configurations_are_rejected),
        cmocka_unit_test(test_joint_commit_respects_both_majorities_and_current_term),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
