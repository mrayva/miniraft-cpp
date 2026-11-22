#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <miniraft/idempotent_client.h>
#include <miniraft/net/server.h>

namespace NNet {

struct TIdempotentClientRetryOptions {
    // Counts the initial send as one attempt when a connected socket is supplied.
    size_t MaxAttempts = 4;
    std::chrono::milliseconds InitialBackoff{50};
    std::chrono::milliseconds MaxBackoff{200};
    std::chrono::milliseconds ConnectTimeout{2000};
    std::chrono::milliseconds ExchangeTimeout{35000};
};

template<typename TSocket>
TFuture<TMessageHolder<TIdempotentCommandResponse>> SendIdempotentRequest(
        TSocket& socket, TMessageHolder<TIdempotentCommandRequest> request) {
    co_await TMessageWriter(socket).Write(request);
    auto reply = co_await TMessageReader(socket).Read();
    auto response = reply.template Cast<TIdempotentCommandResponse>();
    ValidateIdempotentCommandResponse(response);
    if (response->Epoch != request->Epoch || response->SessionIdHigh != request->SessionIdHigh ||
        response->SessionIdLow != request->SessionIdLow || response->Sequence != request->Sequence ||
        response->Cookie != request->Cookie)
        throw TMessageError("Idempotent reply did not match the request");
    co_return response;
}

namespace TDetail {
template<typename TPoller, typename TSocket>
TFuture<void> ShutdownAtExchangeDeadline(TPoller& poller, TSocket& socket, TTime due, bool& expired) {
    co_await poller.Sleep(due);
    expired = true;
    // Shutdown wakes the outstanding read/write while leaving its coroutine
    // and descriptor alive until the poller finishes the operation.
    if constexpr (requires { { socket.Shutdown() } noexcept; }) {
        socket.Shutdown();
    } else {
        const auto fd = socket.Fd();
#ifdef _WIN32
        ::shutdown(static_cast<SOCKET>(fd), SD_BOTH);
#else
        ::shutdown(fd, SHUT_RDWR);
#endif
    }
}

template<typename TPoller, typename TSocket>
TFuture<TMessageHolder<TIdempotentCommandResponse>> ExchangeIdempotentRequest(
        TPoller& poller, TSocket& socket, TMessageHolder<TIdempotentCommandRequest> request,
        std::chrono::milliseconds timeout) {
    const auto start = TClock::now();
    const auto duration = std::chrono::duration_cast<TTime::duration>(timeout);
    const auto due = start > TTime::max() - duration ? TTime::max() : start + duration;
    bool expired = false;
    // Destroying the sleeping task removes its timer on normal completion.
    auto timer = ShutdownAtExchangeDeadline(poller, socket, due, expired);
    try {
        auto response = co_await SendIdempotentRequest(socket, std::move(request));
        if (!expired && TClock::now() < due) co_return response;
    } catch (...) {
        if (!expired) throw;
    }
    throw std::runtime_error("Idempotent write exchange timed out");
}
}

// Retries the durable session's one pending operation after transport errors.
// A non-success response is returned for the caller to interpret. A fresh
// connection is used after a failed attempt; the operation identity and payload
// always come from the session's saved state.
template<typename TPoller, typename TSocket>
TFuture<TMessageHolder<TIdempotentCommandResponse>> SendPendingIdempotentWriteWithRetry(
        TPoller& poller, const TAddress& address,
        std::unique_ptr<TSocket>& activeSocket, bool& activeSocketConnected,
        TIdempotentClientSession& client,
        TIdempotentClientRetryOptions options = {}) {
    if (!options.MaxAttempts)
        throw std::invalid_argument("Idempotent client retry attempts must be nonzero");
    if (options.InitialBackoff < std::chrono::milliseconds::zero() ||
        options.MaxBackoff < options.InitialBackoff)
        throw std::invalid_argument("Invalid idempotent client retry backoff");
    if (options.ConnectTimeout <= std::chrono::milliseconds::zero())
        throw std::invalid_argument("Idempotent client connect timeout must be positive");
    if (options.ExchangeTimeout <= std::chrono::milliseconds::zero() ||
        options.ExchangeTimeout > std::chrono::duration_cast<std::chrono::milliseconds>(TTime::duration::max()))
        throw std::invalid_argument("Invalid idempotent client exchange timeout");

    std::string lastError;
    auto backoff = options.InitialBackoff;
    for (size_t attempt = 0; attempt < options.MaxAttempts; ++attempt) {
        try {
            if (attempt == 0 && activeSocketConnected) {
                auto response = co_await TDetail::ExchangeIdempotentRequest(
                    poller, *activeSocket, client.RetryPending(), options.ExchangeTimeout);
                co_return response;
            }
            auto retrySocket = std::make_unique<TSocket>(address, poller);
            co_await retrySocket->Connect(TClock::now() + options.ConnectTimeout);
            auto response = co_await TDetail::ExchangeIdempotentRequest(
                poller, *retrySocket, client.RetryPending(), options.ExchangeTimeout);
            activeSocket = std::move(retrySocket);
            activeSocketConnected = true;
            co_return response;
        } catch (const std::exception& error) {
            lastError = error.what();
            if (attempt == 0 && activeSocketConnected) {
                activeSocketConnected = false;
                activeSocket.reset();
            }
        }
        if (attempt + 1 < options.MaxAttempts && backoff > std::chrono::milliseconds::zero())
            co_await poller.Sleep(backoff);
        if (backoff < options.MaxBackoff)
            backoff += std::min(backoff, options.MaxBackoff - backoff);
    }
    throw std::runtime_error("idempotent write transport retry exhausted: " + lastError);
}

// Couples durable operation state to the retrying transport so callers cannot
// accidentally send before persisting, or forget to acknowledge a successful
// response. The poller, socket pointer, connection flag, and session must outlive
// this facade. Like TIdempotentClientSession, it is intended for serialized use.
template<typename TPoller, typename TSocket>
class TIdempotentClient {
public:
    TIdempotentClient(TPoller& poller, TAddress address,
            std::unique_ptr<TSocket>& activeSocket, bool& activeSocketConnected,
            TIdempotentClientSession& session,
            TIdempotentClientRetryOptions options = {})
        : Poller(poller)
        , Address(std::move(address))
        , ActiveSocket(activeSocket)
        , ActiveSocketConnected(activeSocketConnected)
        , Session(session)
        , Options(options) {
    }

    const TIdempotentClientSessionState& State() const {
        return Session.State();
    }

    // BeginWrite durably records the operation before any network I/O. If a
    // prior operation is pending, only resending that same payload is allowed.
    // The value parameter keeps temporary payloads alive across coroutine
    // suspension.
    TFuture<TMessageHolder<TIdempotentCommandResponse>> Write(
            std::string payload) {
        (void)Session.BeginWrite(payload);
        co_return co_await SendAndAcknowledge();
    }

    // Retries the operation recovered from the durable session after restart or
    // after a transport failure. A successful matching reply clears it durably.
    TFuture<TMessageHolder<TIdempotentCommandResponse>> RetryPending() {
        if (!Session.State().HasPending)
            throw std::logic_error("idempotent client has no pending operation to retry");
        co_return co_await SendAndAcknowledge();
    }

private:
    TFuture<TMessageHolder<TIdempotentCommandResponse>> SendAndAcknowledge() {
        auto response = co_await SendPendingIdempotentWriteWithRetry(
            Poller, Address, ActiveSocket, ActiveSocketConnected, Session, Options);
        ValidateIdempotentCommandResponse(response);
        if (response->ErrorCode == TCommandResponse::Success && !Session.Complete(response))
            throw std::runtime_error(
                "successful idempotent response did not match the pending operation");
        co_return response;
    }

    TPoller& Poller;
    TAddress Address;
    std::unique_ptr<TSocket>& ActiveSocket;
    bool& ActiveSocketConnected;
    TIdempotentClientSession& Session;
    TIdempotentClientRetryOptions Options;
};

} // namespace NNet
