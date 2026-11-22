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

void test_node_send_queue_bounds_data_and_coalesces_raft_retries(void**) {
    NNet::TLoop<NNet::TPoll> loop;
    auto& poller = loop.Poller();
    auto timeSource = std::make_shared<TTimeSource>();
    auto makeNode = [&] {
        return TNode<NNet::TSocket>(
            [&](const NNet::TAddress& address) { return NNet::TSocket(address, poller); },
            "queue-test", NNet::TAddress{"127.0.0.1", 65534}, timeSource);
    };

    auto countBounded = makeNode();
    for (size_t i = 0; i < TNode<NNet::TSocket>::MaxQueuedDataMessages; ++i)
        assert_true(countBounded.TrySend(NewHoldedMessage<TCommandResponse>()));
    assert_false(countBounded.TrySend(NewHoldedMessage<TCommandResponse>()));
    bool sendRejected = false;
    try { countBounded.Send(NewHoldedMessage<TCommandResponse>()); }
    catch (const TNodeQueueFull&) { sendRejected = true; }
    assert_true(sendRejected);

    auto byteBounded = makeNode();
    constexpr size_t halfBudget = TNode<NNet::TSocket>::MaxQueuedDataBytes / 2;
    const auto largeReply = [&] {
        return NewHoldedMessage<TCommandResponse>(halfBudget);
    };
    assert_true(byteBounded.TrySend(largeReply()));
    assert_true(byteBounded.TrySend(largeReply()));
    assert_false(byteBounded.TrySend(largeReply()));

    auto coalesced = makeNode();
    for (uint64_t i = 1; i <= 1000; ++i) {
        auto request = NewHoldedMessage<TAppendEntriesRequest>();
        request->Term = i;
        assert_true(coalesced.TrySend(std::move(request)));
    }
    assert_true(coalesced.TrySend(NewHoldedMessage<TConfigurationBarrierRequest>()));
    assert_true(coalesced.TrySend(NewHoldedMessage<TRequestVoteRequest>()));

    auto retired = makeNode();
    retired.Retire();
    assert_false(retired.TrySend(NewHoldedMessage<TCommandResponse>()));
    retired.Send(NewHoldedMessage<TCommandResponse>()); // Preserve retired Send's no-op behavior.

    auto aggregateBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{1, 1024});
    auto aggregateFirst = makeNode();
    auto aggregateSecond = makeNode();
    assert_true(aggregateFirst.AttachQueueBudget(aggregateBudget));
    assert_true(aggregateSecond.AttachQueueBudget(aggregateBudget));
    assert_true(aggregateFirst.TrySend(NewHoldedMessage<TAppendEntriesRequest>()));
    assert_false(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    // Replacing a coalesced retry keeps the shared reservation at one message.
    assert_true(aggregateFirst.TrySend(NewHoldedMessage<TAppendEntriesRequest>()));
    assert_false(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    aggregateFirst.Retire();
    assert_true(aggregateSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    aggregateSecond.Retire();

    auto measuredReply = NewHoldedMessage<TCommandResponse>();
    auto byteBudget = std::make_shared<TOutboundQueueBudget>(
        TOutboundQueueLimits{2, measuredReply->Len});
    auto byteFirst = makeNode();
    auto byteSecond = makeNode();
    assert_true(byteFirst.AttachQueueBudget(byteBudget));
    assert_true(byteSecond.AttachQueueBudget(byteBudget));
    assert_true(byteFirst.TrySend(std::move(measuredReply)));
    assert_false(byteSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    byteFirst.Retire();
    assert_true(byteSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    byteSecond.Retire();

    struct TImmediatePoller {
        NNet::TValueTask<void> Sleep(std::chrono::milliseconds) { co_return; }
    };
    struct TImmediateSocket {
        using TPoller = TImmediatePoller;
        TPoller* Poller() { return nullptr; }
        NNet::TValueTask<void> Connect() { co_return; }
        NNet::TValueTask<std::ptrdiff_t> WriteSome(const void*, size_t size) {
            co_return static_cast<std::ptrdiff_t>(size);
        }
    };
    auto drainBudget = std::make_shared<TOutboundQueueBudget>(TOutboundQueueLimits{1, 1024});
    TNode<TImmediateSocket> drainFirst("drain-first", TImmediateSocket{}, timeSource);
    TNode<TImmediateSocket> drainSecond("drain-second", TImmediateSocket{}, timeSource);
    assert_true(drainFirst.AttachQueueBudget(drainBudget));
    assert_true(drainSecond.AttachQueueBudget(drainBudget));
    assert_true(drainFirst.TrySend(NewHoldedMessage<TCommandResponse>()));
    assert_false(drainSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
    drainFirst.Drain(); // Successful completion returns its aggregate reservation.
    assert_true(drainSecond.TrySend(NewHoldedMessage<TCommandResponse>()));
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

    const auto lastIndexBeforeRejectedChanges = raft->GetLastIndex();
    bool changeRejected = false;
    try { server.ProposeJointConfiguration({1, 2}); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    changeRejected = false;
    try { server.FinalizeConfiguration(); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    changeRejected = false;
    try { server.TransferLeadership(2); }
    catch (const std::logic_error&) { changeRejected = true; }
    assert_true(changeRejected);
    assert_int_equal(raft->GetLastIndex(), lastIndexBeforeRejectedChanges);

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

void test_server_forwards_successful_membership_operations(void**) {
#ifdef __linux__
    struct TEndpoint : INode {
        std::vector<TMessageHolder<TMessage>> Messages;
        void Send(TMessageHolder<TMessage> message) override { Messages.push_back(std::move(message)); }
        void Drain() override { }
    };
    struct TManualTimeSource : ITimeSource {
        Time Now() override { return {}; }
    };

    const auto dir = std::filesystem::temp_directory_path() / ("miniraft-server-membership-success-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    assert_true(std::filesystem::create_directory(dir));
    {
        NNet::TLoop<NNet::TPoll> loop;
        auto timeSource = std::make_shared<TManualTimeSource>();
        auto makeServer = [&](const std::shared_ptr<TRaft>& raft, const TNodeDict& peers) {
            NNet::TSocket listener(NNet::TAddress{"127.0.0.1", 0}, loop.Poller());
            return std::make_unique<TRaftServer<NNet::TSocket>>(
                loop.Poller(), std::move(listener), raft, std::make_shared<TDummyRsm>(), peers, timeSource);
        };
        auto ordinary = [] {
            auto entry = NewHoldedMessage<TLogEntry>();
            entry->Term = 1;
            return entry;
        };

        // A stable cluster can propose a removal through the server facade once
        // its current-term peer capability has been observed.
        auto proposalState = std::make_shared<TDurableState>((dir / "proposal").string(), 1);
        proposalState->BindVoters({1, 2});
        proposalState->Append(ordinary());
        proposalState->CommitIndex = 1;
        proposalState->Commit();
        auto proposalPeer = std::make_shared<TEndpoint>();
        TNodeDict proposalPeers{{2, proposalPeer}};
        auto proposalRaft = std::make_shared<TRaft>(proposalState, 1, proposalPeers);
        proposalRaft->Become(EState::LEADER);
        auto* proposalVolatile = const_cast<TVolatileState*>(proposalRaft->GetVolatileState());
        proposalVolatile->PeerCapabilities[2].Term = proposalState->CurrentTerm;
        proposalVolatile->PeerCapabilities[2].JointConsensus = true;
        auto proposalServer = makeServer(proposalRaft, proposalPeers);
        assert_true(proposalServer->CheckJointConfigurationReadiness({1}).Ready());
        const auto jointIndex = proposalServer->ProposeJointConfiguration({1});
        assert_int_equal(jointIndex, 2);
        const auto proposed = proposalRaft->GetConfigurationState().Latest;
        assert_true(proposed.IsJoint());
        assert_true(proposed.OldVoters() == std::vector<uint32_t>({1, 2}));
        assert_true(proposed.NewVoters() == std::vector<uint32_t>({1}));

        // A committed joint configuration with this leader on the new side can
        // append its stable successor through the same server API.
        auto finalizeState = std::make_shared<TDurableState>((dir / "finalize").string(), 1);
        finalizeState->BindVoters({1, 2});
        finalizeState->Append(ordinary());
        finalizeState->Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 1, 0, 0, {1, 2}, {1}}));
        finalizeState->CommitIndex = 2;
        finalizeState->Commit();
        TNodeDict finalizePeers{{2, std::make_shared<TEndpoint>()}};
        auto finalizeRaft = std::make_shared<TRaft>(finalizeState, 1, finalizePeers);
        finalizeRaft->Become(EState::LEADER);
        auto finalizeServer = makeServer(finalizeRaft, finalizePeers);
        const auto stableIndex = finalizeServer->FinalizeConfiguration();
        assert_int_equal(stableIndex, 3);
        const auto stable = DecodeConfigurationEntry(finalizeState->Get(stableIndex));
        assert_int_equal(stable.Phase, EConfigurationPhase::Stable);
        assert_true(stable.NewVoters == std::vector<uint32_t>({1}));

        // The wrapper supplies its time source and sends TimeoutNow to a caught-up
        // voter on the new side of a committed joint configuration.
        auto transferState = std::make_shared<TDurableState>((dir / "transfer").string(), 1);
        transferState->BindVoters({1, 2});
        transferState->Append(ordinary());
        transferState->Append(EncodeConfigurationEntry({
            EConfigurationPhase::Joint, 1, 0, 0, {1, 2}, {2, 3}}));
        transferState->CommitIndex = 2;
        transferState->Commit();
        auto target = std::make_shared<TEndpoint>();
        TNodeDict transferPeers{{2, std::make_shared<TEndpoint>()}, {3, target}};
        auto transferRaft = std::make_shared<TRaft>(transferState, 1, transferPeers, std::vector<uint32_t>{3});
        transferRaft->Become(EState::LEADER);
        auto* transferVolatile = const_cast<TVolatileState*>(transferRaft->GetVolatileState());
        transferVolatile->MatchIndex[3] = transferState->LastLogIndex;
        transferVolatile->CommitAckIndex[3] = transferState->CommitIndex;
        auto transferServer = makeServer(transferRaft, transferPeers);
        transferServer->TransferLeadership(3);
        assert_true(transferRaft->CurrentStateName() == EState::FOLLOWER);
        assert_false(target->Messages.empty());
        const auto timeoutNow = target->Messages.back().Cast<TTimeoutNowRequest>();
        assert_int_equal(timeoutNow->TargetId, 3);
    }
    std::error_code ignored;
    std::filesystem::remove_all(dir, ignored);
#endif
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
        raft, std::make_shared<TDummyRsm>(), peers, timeSource,
        TRequestProcessor::TAdmissionLimits{}, TOutboundQueueLimits{0, 0});

    auto joiningPeer = makePeer("joining-voter");
    server.RegisterPeerTarget(3, joiningPeer);
    server.Serve(); // Starts readers for voters 1/2 and staged target 3.
    if (!joiningPeer->HasResponseReader()) return 8;

    auto* volatileState = const_cast<TVolatileState*>(raft->GetVolatileState());
    volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
    volatileState->HasCompactionSafeInitialConfigurationBarrier = true;
    auto replacedLearner = makePeer("replaced-learner");
    if (server.ReplacePeerTarget(3, replacedLearner) != joiningPeer ||
        !joiningPeer->IsRetired() || !replacedLearner->HasResponseReader() || raft->IsVoter(3) ||
        !volatileState->HasCompactionSafeInitialConfigurationBarrier ||
        volatileState->CompactionSafeConfigurationTerm != state->CurrentTerm) return 14;
    joiningPeer = replacedLearner;

    // A replacement endpoint must not inherit replication or capability proof
    // established by the old transport, and must keep the configured voter role.
    volatileState->PeerCapabilities[2].InstallSnapshot = true;
    volatileState->MatchIndex[2] = 1;
    volatileState->CommitAckIndex[2] = 1;
    volatileState->CompactionSafeConfigurationIndex = 1;
    volatileState->CompactionSafeConfigurationTerm = state->CurrentTerm;
    auto oldVoter = std::dynamic_pointer_cast<TNode<TTlsSocket>>(peers.at(2));
    auto movedVoter = makePeer("moved-voter");
    if (!oldVoter || !oldVoter->HasResponseReader()) return 10;

    // A queue-budget rejection occurs before routing or Raft state changes.
    auto blockedReplacement = makePeer("blocked-replacement");
    blockedReplacement->Send(NewHoldedMessage<TCommandResponse>());
    bool replacementRejected = false;
    try {
        server.ReplacePeerTarget(2, blockedReplacement);
    } catch (const TNodeQueueFull&) {
        replacementRejected = true;
    }
    if (!replacementRejected || oldVoter->IsRetired() || !oldVoter->HasResponseReader() ||
        !raft->GetVolatileState()->PeerCapabilities.contains(2) ||
        !raft->GetVolatileState()->CommitAckIndex.contains(2)) return 13;

    if (server.ReplacePeerTarget(2, movedVoter) != oldVoter || !oldVoter->IsRetired() ||
        !movedVoter->HasResponseReader() || !raft->IsVoter(2) ||
        raft->GetVoterIds() != std::vector<uint32_t>({1, 2})) return 11;
    const auto* replacedState = raft->GetVolatileState();
    if (replacedState->PeerCapabilities.contains(2) || replacedState->MatchIndex.contains(2) ||
        replacedState->CommitAckIndex.contains(2) || replacedState->CompactionSafeConfigurationIndex ||
        replacedState->CompactionSafeConfigurationTerm ||
        replacedState->HasCompactionSafeInitialConfigurationBarrier) return 12;

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
        cmocka_unit_test(test_node_send_queue_bounds_data_and_coalesces_raft_retries),
        cmocka_unit_test(test_message_send_recv),
        cmocka_unit_test(test_read_write),
        cmocka_unit_test(test_read_write_payload),
        cmocka_unit_test(test_server_stages_and_removes_runtime_learner),
        cmocka_unit_test(test_server_forwards_successful_membership_operations),
#ifdef __linux__
        cmocka_unit_test(test_serving_server_retires_demoted_joint_voter_reader),
#endif
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
};
