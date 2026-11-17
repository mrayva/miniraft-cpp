#include <miniraft/raft.h>
#include <functional>
#include <string>
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
extern "C" {
#include <cmocka.h>
}

namespace {
struct TEndpoint : INode {
    std::function<void(TMessageHolder<TMessage>)> Callback;
    std::vector<TMessageHolder<TMessage>> Rejected;
    bool Accept = true;
    explicit TEndpoint(std::function<void(TMessageHolder<TMessage>)> cb) : Callback(std::move(cb)) {}
    void Send(TMessageHolder<TMessage> message) override { Callback(std::move(message)); }
    bool TrySend(TMessageHolder<TMessage> message) override {
        if (!Accept) { Rejected.push_back(std::move(message)); return false; }
        Send(std::move(message));
        return true;
    }
    void Drain() override {}
};
struct TManualTimeSource : ITimeSource {
    Time Current{};
    Time Now() override { return Current; }
};
struct TClient {
    struct TReply { uint32_t Cookie, Error; std::string Data; };
    std::vector<TReply> Replies;
    std::shared_ptr<TEndpoint> Endpoint = std::make_shared<TEndpoint>([this](auto message) {
        auto response = message.template Cast<TCommandResponse>();
        Replies.push_back({response->Cookie, response->ErrorCode,
            std::string(response->Data, response->Len - sizeof(TCommandResponse))});
    });
};
struct TFollower {
    std::vector<TMessageHolder<TCommandRequest>> Sent;
    std::shared_ptr<TEndpoint> Leader = std::make_shared<TEndpoint>([this](auto message) {
        if (message->Type == uint32_t(EMessageType::COMMAND_REQUEST))
            Sent.push_back(message.template Cast<TCommandRequest>());
    });
    std::shared_ptr<TEndpoint> Other = std::make_shared<TEndpoint>([this](auto message) {
        if (message->Type == uint32_t(EMessageType::COMMAND_REQUEST))
            Sent.push_back(message.template Cast<TCommandRequest>());
    });
    TNodeDict Peers{{1, Leader}, {3, Other}};
    std::shared_ptr<TRaft> Raft = std::make_shared<TRaft>(std::make_shared<TState>(), 2, Peers);
    std::shared_ptr<TDummyRsm> Rsm = std::make_shared<TDummyRsm>();
    TRequestProcessor Processor{Raft, Rsm, Peers};
    explicit TFollower(bool known = true,
            TRequestProcessor::TAdmissionLimits limits = TRequestProcessor::TAdmissionLimits{},
            std::shared_ptr<ITimeSource> timeSource = {})
        : Processor{Raft, Rsm, Peers, limits, std::move(timeSource)} {
        if (known) LearnLeader(1, 3);
    }
    void LearnLeader(uint32_t id, uint64_t term) {
        auto message = NewHoldedMessage(TMessageEx{.Src = id, .Dst = 2, .Term = term},
                                       TAppendEntriesRequest{.LeaderId = id});
        Raft->Process(ITimeSource::Time{}, message);
    }
    TMessageHolder<TCommandRequest> Request(TClient& client, uint32_t cookie, uint32_t extra = 0) {
        auto request = NewHoldedMessage<TCommandRequest>(sizeof(TCommandRequest) + 5);
        request->Cookie = cookie;
        request->Flags = TCommandRequest::EWrite | extra;
        memcpy(request->Data, "value", 5);
        Processor.OnCommandRequest(request, client.Endpoint);
        return request;
    }
    TMessageHolder<TCommandResponse> Response(size_t sent, uint32_t error = 0) {
        auto response = NewHoldedMessage<TCommandResponse>(sizeof(TCommandResponse) + 6);
        response->Cookie = Sent.at(sent)->Cookie;
        response->ErrorCode = error;
        memcpy(response->Data, "answer", 6);
        return response;
    }
    void Reply(size_t sent, uint32_t error = 0) {
        Processor.OnCommandResponse(Response(sent, error), Leader);
    }
};

void test_malformed_forwarded_reply_does_not_consume_route(void**) {
    TFollower follower; TClient client;
    follower.Request(client, 42);
    auto response = follower.Response(0);
    response->Len += 8;
    bool rejected = false;
    try { follower.Processor.OnCommandResponse(response, follower.Leader); }
    catch (const TMessageError&) { rejected = true; }
    assert_true(rejected); assert_true(client.Replies.empty());
    auto wrong = follower.Response(0);
    wrong->Type = uint32_t(EMessageType::LOG_ENTRY);
    rejected = false;
    try { follower.Processor.OnCommandResponse(wrong, follower.Leader); }
    catch (const TMessageError&) { rejected = true; }
    assert_true(rejected); assert_true(client.Replies.empty());
    follower.Reply(0);
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 42);
    assert_string_equal(client.Replies[0].Data.c_str(), "answer");
}

void test_forwarded_send_rejection_fails_client_and_discards_route(void**) {
    TFollower follower;
    TClient client;
    follower.Leader->Accept = false;
    follower.Request(client, 42);

    assert_int_equal(follower.Leader->Rejected.size(), 1);
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 42);
    assert_int_equal(client.Replies[0].Error, TCommandResponse::Overloaded);

    const auto forwarded = follower.Leader->Rejected[0].Cast<TCommandRequest>();
    auto lateReply = NewHoldedMessage(TCommandResponse{.Cookie = forwarded->Cookie});
    follower.Processor.OnCommandResponse(lateReply, follower.Leader);
    assert_int_equal(client.Replies.size(), 1);
}

void test_forwarded_requests_share_admission_budget_and_release_it(void**) {
    TFollower follower(true, TRequestProcessor::TAdmissionLimits{
        .MaxPendingRequests = 1,
        .MaxPendingRequestBytes = 1024,
        .MaxPendingReplyBytes = 1024,
    });
    TClient first, second;
    follower.Request(first, 11);
    assert_int_equal(follower.Sent.size(), 1);
    follower.Request(second, 22);
    assert_int_equal(follower.Sent.size(), 1);
    assert_int_equal(second.Replies.size(), 1);
    assert_int_equal(second.Replies[0].Cookie, 22);
    assert_int_equal(second.Replies[0].Error, TCommandResponse::Overloaded);

    follower.Reply(0);
    assert_int_equal(first.Replies.size(), 1);
    follower.Request(second, 23); // Completing the route releases its reservation.
    assert_int_equal(follower.Sent.size(), 2);
    follower.Processor.CleanUp(second.Endpoint);
    follower.Request(second, 24); // Disconnect cleanup releases the replacement route.
    assert_int_equal(follower.Sent.size(), 3);
}

void test_forwarded_request_deadline_removes_route_and_ignores_late_reply(void**) {
    auto clock = std::make_shared<TManualTimeSource>();
    TFollower follower(true, TRequestProcessor::TAdmissionLimits{
        .MaxPendingRequests = 2,
        .MaxPendingRequestBytes = 1024,
        .MaxPendingReplyBytes = 1024,
        .RequestTimeout = std::chrono::milliseconds(10),
    }, clock);
    TClient client;
    follower.Request(client, 71);
    assert_int_equal(follower.Sent.size(), 1);
    const auto forwardedCookie = follower.Sent[0]->Cookie;

    clock->Current += std::chrono::milliseconds(10);
    follower.Processor.ProcessWaiting();
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 71);
    assert_int_equal(client.Replies[0].Error, TCommandResponse::TimedOut);

    auto late = NewHoldedMessage(TCommandResponse{.Cookie = forwardedCookie});
    follower.Processor.OnCommandResponse(late, follower.Leader);
    assert_int_equal(client.Replies.size(), 1);
    follower.Request(client, 72); // Expiration released the shared admission slot.
    assert_int_equal(follower.Sent.size(), 2);
}

void test_nonzero_client_cookies_are_forwarded(void**) {
    for (uint32_t cookie : {42u, UINT32_MAX}) {
        TFollower follower;
        TClient client;
        follower.Request(client, cookie);
        assert_int_equal(follower.Sent.size(), 1);
        follower.Reply(0);
        assert_int_equal(client.Replies.size(), 1);
        assert_int_equal(client.Replies[0].Cookie, cookie);
        assert_int_equal(client.Replies[0].Error, 0);
    }
}
void test_zero_client_cookie_is_restored(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0);
    assert_int_equal(follower.Sent.size(), 1);
    follower.Reply(0);
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 0);
    assert_string_equal(client.Replies[0].Data.c_str(), "answer");
}
void test_forwarding_copies_request_and_marks_one_hop(void**) {
    TFollower follower;
    TClient client;
    auto original = follower.Request(client, 0);
    assert_int_equal(original->Cookie, 0);
    assert_int_equal(original->Flags, TCommandRequest::EWrite);
    assert_true(follower.Sent[0]->Flags & TCommandRequest::EForwarded);
    assert_true(follower.Sent[0].RawData != original.RawData);
}
void test_out_of_order_and_duplicate_replies_preserve_client_routes(void**) {
    TFollower follower;
    TClient first, second;
    follower.Request(first, 0);
    follower.Request(second, 0);
    assert_int_not_equal(follower.Sent[0]->Cookie, follower.Sent[1]->Cookie);
    follower.Reply(1);
    follower.Reply(1);
    assert_true(first.Replies.empty());
    assert_int_equal(second.Replies.size(), 1);
    assert_int_equal(second.Replies[0].Cookie, 0);
    follower.Reply(0);
    assert_int_equal(first.Replies.size(), 1);
    assert_int_equal(first.Replies[0].Cookie, 0);
}
void test_forwarded_request_is_rejected_on_term_change(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0);
    follower.LearnLeader(3, 4);
    follower.Processor.CheckStateChange();
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 0);
    assert_int_equal(client.Replies[0].Error, 1);
    follower.Reply(0);
    assert_int_equal(client.Replies.size(), 1);
}
void test_candidate_transition_rejects_forwarded_requests(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0);
    follower.Raft->ProcessTimeout(ITimeSource::Time{} + std::chrono::seconds(20));
    assert_int_equal(follower.Raft->CurrentStateName(), EState::CANDIDATE);
    follower.Processor.CheckStateChange();
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Error, 1);
}
void test_late_reply_prunes_stale_route_before_application_poll(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0);
    follower.LearnLeader(3, 4);
    follower.Reply(0); // No CheckStateChange/ProcessWaiting in between.
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Error, 1);
    assert_int_equal(client.Replies[0].Cookie, 0);
}
void test_client_disconnect_releases_only_its_forwarded_route(void**) {
    TFollower follower;
    TClient first, second;
    follower.Request(first, 0);
    follower.Request(second, 0);
    std::weak_ptr<INode> endpoint = first.Endpoint;
    follower.Processor.CleanUp(first.Endpoint);
    first.Endpoint.reset();
    assert_true(endpoint.expired());
    follower.Reply(0);
    assert_true(first.Replies.empty());
    follower.Reply(1);
    assert_int_equal(second.Replies.size(), 1);
    assert_int_equal(second.Replies[0].Error, 0);
}
void test_already_forwarded_request_is_not_relayed_again(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0, TCommandRequest::EForwarded);
    assert_true(follower.Sent.empty());
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Error, 1);
}
void test_nonzero_cookie_waits_for_leader_discovery(void**) {
    TFollower follower(false);
    TClient client;
    follower.Request(client, 77);
    assert_true(client.Replies.empty());
    assert_true(follower.Sent.empty());
    follower.LearnLeader(1, 3);
    follower.Processor.CheckStateChange();
    assert_int_equal(follower.Sent.size(), 1);
    follower.Reply(0);
    assert_int_equal(client.Replies[0].Cookie, 77);
}
void test_response_requires_original_peer_and_preserves_wire_message(void**) {
    TFollower follower;
    TClient client;
    follower.Request(client, 0);
    auto response = follower.Response(0, 77);
    const auto cookie = response->Cookie;
    follower.Processor.OnCommandResponse(response);
    follower.Processor.OnCommandResponse(response, follower.Other);
    assert_true(client.Replies.empty());
    follower.Processor.OnCommandResponse(response, follower.Leader);
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 0);
    assert_int_equal(client.Replies[0].Error, 77);
    assert_int_equal(response->Cookie, cookie);
}
void test_peer_disconnect_cancels_once_and_does_not_reuse_cookies(void**) {
    TFollower follower;
    TClient first, second;
    follower.Request(first, 42);
    follower.Request(second, 43);
    follower.Processor.OnPeerDisconnected(follower.Other);
    assert_true(first.Replies.empty());
    assert_true(second.Replies.empty());
    follower.Processor.OnPeerDisconnected(follower.Leader);
    follower.Processor.OnPeerDisconnected(follower.Leader);
    assert_int_equal(first.Replies.size(), 1);
    assert_int_equal(second.Replies.size(), 1);
    assert_int_equal(first.Replies[0].Cookie, 42);
    assert_int_equal(second.Replies[0].Cookie, 43);
    assert_int_equal(first.Replies[0].Error, 1);
    assert_int_equal(second.Replies[0].Error, 1);
    std::weak_ptr<INode> endpoint = first.Endpoint;
    first.Endpoint.reset();
    assert_true(endpoint.expired());
    TClient afterReconnect;
    follower.Request(afterReconnect, 44);
    assert_true(follower.Sent[2]->Cookie > follower.Sent[1]->Cookie);
    follower.Reply(0); // Late old-connection reply cannot redeem the new route.
    assert_true(afterReconnect.Replies.empty());
    follower.Reply(2);
    assert_int_equal(afterReconnect.Replies.size(), 1);
    assert_int_equal(afterReconnect.Replies[0].Cookie, 44);
    assert_int_equal(afterReconnect.Replies[0].Error, 0);
}
void test_runtime_peer_endpoint_updates_forwarding_routes(void**) {
    auto originalPeer = std::make_shared<TEndpoint>([](auto) {});
    std::vector<TMessageHolder<TCommandRequest>> sent;
    auto addedPeer = std::make_shared<TEndpoint>([&](auto message) {
        if (message->Type == uint32_t(EMessageType::COMMAND_REQUEST))
            sent.push_back(message.template Cast<TCommandRequest>());
    });
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), 2,
        TNodeDict{{1, originalPeer}, {3, addedPeer}});
    // Model a transport target added after the request processor was created.
    TRequestProcessor processor{raft, std::make_shared<TDummyRsm>(), TNodeDict{{1, originalPeer}}};
    auto contact = NewHoldedMessage(TMessageEx{.Src = 3, .Dst = 2, .Term = 2},
        TAppendEntriesRequest{.LeaderId = 3});
    raft->Process(ITimeSource::Time{}, contact);
    assert_int_equal(raft->GetLeaderId(), 3);

    TClient client;
    auto request = NewHoldedMessage<TCommandRequest>(sizeof(TCommandRequest) + 5);
    request->Cookie = 17;
    request->Flags = TCommandRequest::EWrite;
    memcpy(request->Data, "value", 5);
    processor.OnCommandRequest(request, client.Endpoint);
    assert_true(sent.empty()); // The new leader is not in the startup route map yet.

    processor.AddPeerEndpoint(3, addedPeer);
    processor.CheckStateChange();
    assert_int_equal(sent.size(), 1);
    assert_true(sent[0]->Flags & TCommandRequest::EForwarded);
    processor.OnCommandResponse(NewHoldedMessage(TCommandResponse{.Cookie = sent[0]->Cookie}), addedPeer);
    assert_int_equal(client.Replies.size(), 1);
    assert_int_equal(client.Replies[0].Cookie, 17);
    assert_int_equal(client.Replies[0].Error, 0);

    processor.RemovePeerEndpoint(3, addedPeer);
    TClient afterRemoval;
    request->Cookie = 18;
    processor.OnCommandRequest(request, afterRemoval.Endpoint);
    assert_int_equal(sent.size(), 1); // A removed endpoint is no longer a forwarding route.
}
}

int main() {
    const CMUnitTest tests[] = {
        cmocka_unit_test(test_malformed_forwarded_reply_does_not_consume_route),
        cmocka_unit_test(test_forwarded_send_rejection_fails_client_and_discards_route),
        cmocka_unit_test(test_forwarded_requests_share_admission_budget_and_release_it),
        cmocka_unit_test(test_forwarded_request_deadline_removes_route_and_ignores_late_reply),
        cmocka_unit_test(test_nonzero_client_cookies_are_forwarded),
        cmocka_unit_test(test_zero_client_cookie_is_restored),
        cmocka_unit_test(test_forwarding_copies_request_and_marks_one_hop),
        cmocka_unit_test(test_out_of_order_and_duplicate_replies_preserve_client_routes),
        cmocka_unit_test(test_forwarded_request_is_rejected_on_term_change),
        cmocka_unit_test(test_candidate_transition_rejects_forwarded_requests),
        cmocka_unit_test(test_late_reply_prunes_stale_route_before_application_poll),
        cmocka_unit_test(test_client_disconnect_releases_only_its_forwarded_route),
        cmocka_unit_test(test_already_forwarded_request_is_not_relayed_again),
        cmocka_unit_test(test_nonzero_cookie_waits_for_leader_discovery),
        cmocka_unit_test(test_response_requires_original_peer_and_preserves_wire_message),
        cmocka_unit_test(test_peer_disconnect_cancels_once_and_does_not_reuse_cookies),
        cmocka_unit_test(test_runtime_peer_endpoint_updates_forwarding_routes),
    };
    return cmocka_run_group_tests(tests, nullptr, nullptr);
}
