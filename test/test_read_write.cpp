#include <chrono>
#include <coroutine>
#include <cstdint>
#include <iostream>
#include <memory>
#include <functional>
#include <stdexcept>
#include <vector>
#include <filesystem>

#include <miniraft/configuration.h>
#include <miniraft/durable.h>
#include <miniraft/messages.h>
#include <miniraft/raft.h>
#include <miniraft/net/server.h>
#include <miniraft/timesource.h>
#include <coroio/all.hpp>
#ifdef __linux__
#include <sys/wait.h>
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

TMessageHolder<TLogEntry> MakeEntry(const char* text) {
    auto mes = NewHoldedMessage<TLogEntry>(
        static_cast<uint32_t>(TLogEntry::MessageType),
        sizeof(TLogEntry) + strlen(text) + 1
    );
    strcpy(mes.Mes->Data, text);
    return mes;
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
            "[::1]80:1", "[::1]:80:1:extra"}) {
        bool rejected = false;
        try { (void)THost{endpoint}; }
        catch (const std::invalid_argument&) { rejected = true; }
        assert_true(rejected);
    }
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

#ifdef __linux__
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
        raft, std::make_shared<TDummyRsm>(), peers, timeSource);

    auto joiningPeer = makePeer("joining-voter");
    server.RegisterPeerTarget(3, joiningPeer);
    server.Serve(); // Starts readers for voters 1/2 and staged target 3.
    if (!joiningPeer->HasResponseReader()) return 8;

    TConfigurationRecord addJoint{EConfigurationPhase::Joint, 2, 0, 0, {1, 2}, {1, 2, 3}};
    auto addRequest = NewHoldedMessage(
        TMessageEx{.Src = 2, .Dst = 1, .Term = 2},
        TAppendEntriesRequest{.LeaderCommit = 1, .LeaderId = 2, .Nentries = 1});
    addRequest.InitPayload(1);
    addRequest.Payload[0] = EncodeConfigurationEntry(addJoint);
    raft->Process({}, std::move(addRequest));
    if (!raft->IsVoter(3)) return 2;

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
        TAppendEntriesRequest{.PrevLogIndex = 1, .PrevLogTerm = 2, .LeaderCommit = 4,
            .LeaderId = 2, .Nentries = 3});
    removeRequest.InitPayload(3);
    removeRequest.Payload[0] = EncodeConfigurationEntry(addStable);
    removeRequest.Payload[1] = EncodeConfigurationEntry(removeJoint);
    removeRequest.Payload[2] = EncodeConfigurationEntry(removeStable);
    raft->Process({}, std::move(removeRequest));
    if (raft->IsVoter(3)) return 3;

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
        TAppendEntriesRequest{.PrevLogIndex = 4, .PrevLogTerm = 2, .LeaderCommit = 5,
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
        TAppendEntriesRequest{.PrevLogIndex = 5, .PrevLogTerm = 2, .LeaderCommit = 8,
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
        cmocka_unit_test(test_message_send_recv),
        cmocka_unit_test(test_read_write),
        cmocka_unit_test(test_read_write_payload),
        cmocka_unit_test(test_server_stages_and_removes_runtime_learner),
#ifdef __linux__
        cmocka_unit_test(test_serving_server_retires_demoted_joint_voter_reader),
#endif
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
};
