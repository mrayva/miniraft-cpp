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
};

template<typename TSocket>
TFuture<TMessageHolder<TIdempotentCommandResponse>> SendIdempotentRequest(
        TSocket& socket, TMessageHolder<TIdempotentCommandRequest> request) {
    co_await TMessageWriter(socket).Write(std::move(request));
    auto reply = co_await TMessageReader(socket).Read();
    co_return reply.template Cast<TIdempotentCommandResponse>();
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

    std::string lastError;
    auto backoff = options.InitialBackoff;
    for (size_t attempt = 0; attempt < options.MaxAttempts; ++attempt) {
        try {
            if (attempt == 0 && activeSocketConnected) {
                auto response = co_await SendIdempotentRequest(
                    *activeSocket, client.RetryPending());
                co_return response;
            }
            auto retrySocket = std::make_unique<TSocket>(address, poller);
            co_await retrySocket->Connect(TClock::now() + options.ConnectTimeout);
            auto response = co_await SendIdempotentRequest(
                *retrySocket, client.RetryPending());
            activeSocket = std::move(retrySocket);
            activeSocketConnected = true;
            co_return response;
        } catch (const std::exception& error) {
            lastError = error.what();
            if (attempt == 0 && activeSocketConnected)
                activeSocketConnected = false;
        }
        if (attempt + 1 < options.MaxAttempts && backoff > std::chrono::milliseconds::zero())
            co_await poller.Sleep(backoff);
        if (backoff < options.MaxBackoff)
            backoff += std::min(backoff, options.MaxBackoff - backoff);
    }
    throw std::runtime_error("idempotent write transport retry exhausted: " + lastError);
}

} // namespace NNet
