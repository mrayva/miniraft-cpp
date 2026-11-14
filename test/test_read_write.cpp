#include <chrono>
#include <coroutine>
#include <cstdint>
#include <iostream>
#include <memory>
#include <functional>
#include <stdexcept>
#include <vector>

#include <miniraft/messages.h>
#include <miniraft/raft.h>
#include <miniraft/net/server.h>
#include <miniraft/timesource.h>
#include <coroio/all.hpp>

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

    raft->Become(EState::LEADER);
    auto retryEndpoint = makeEndpoint();
    server.AddLearnerTarget(3, retryEndpoint);
    assert_true(raft->GetLearnerIds() == std::vector<uint32_t>({3}));
    assert_true(server.RemoveLearnerTarget(3) == retryEndpoint);
    assert_true(raft->GetLearnerIds().empty());
}

int main() {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_message_send_recv),
        cmocka_unit_test(test_read_write),
        cmocka_unit_test(test_read_write_payload),
        cmocka_unit_test(test_server_stages_and_removes_runtime_learner),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
};
