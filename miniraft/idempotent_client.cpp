#include "idempotent_client.h"

#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace {
void ValidateClientSessionState(const TIdempotentClientSessionState& state) {
    if (state.Version != TIdempotentClientSessionState::CurrentVersion)
        throw std::invalid_argument("Unsupported idempotent client session state version");
    if (!state.Initialized) {
        if (state.Epoch || state.SessionIdHigh || state.SessionIdLow || state.NextSequence ||
            state.HasPending || !state.PendingPayload.empty())
            throw std::invalid_argument("Uninitialized idempotent client state contains session data");
        return;
    }
    if (!state.Epoch || (!state.SessionIdHigh && !state.SessionIdLow) || !state.NextSequence)
        throw std::invalid_argument("Invalid idempotent client session identity");
    if (!state.HasPending && !state.PendingPayload.empty())
        throw std::invalid_argument("Idempotent client state has an orphan pending payload");
    if (state.PendingPayload.size() > TMessageLimits{}.MaxFrameBytes -
            sizeof(TIdempotentCommandRequest))
        throw std::invalid_argument("Pending idempotent client payload exceeds the frame limit");
}
}

TIdempotentClientSession::TIdempotentClientSession(
        std::shared_ptr<IIdempotentClientStateStore> store)
    : Store(std::move(store)) {
    if (!Store) throw std::invalid_argument("Idempotent client requires a state store");
    if (auto loaded = Store->Load()) {
        ValidateClientSessionState(*loaded);
        if (!loaded->Initialized)
            throw std::invalid_argument("Client state store returned an uninitialized session");
        Session = std::move(*loaded);
    }
}

void TIdempotentClientSession::StartSession(
        uint64_t epoch, uint64_t sessionIdHigh, uint64_t sessionIdLow) {
    if (Session.HasPending)
        throw std::logic_error("Cannot replace a client session with an unresolved write");
    if (Session.Initialized && Session.Epoch == epoch &&
        Session.SessionIdHigh == sessionIdHigh && Session.SessionIdLow == sessionIdLow)
        throw std::invalid_argument("Cannot restart a client session with the same identity and epoch");
    TIdempotentClientSessionState candidate{
        .Initialized = true,
        .Epoch = epoch,
        .SessionIdHigh = sessionIdHigh,
        .SessionIdLow = sessionIdLow,
        .NextSequence = 1,
    };
    ValidateClientSessionState(candidate);
    Store->Save(candidate);
    Session = std::move(candidate);
}

TMessageHolder<TIdempotentCommandRequest> TIdempotentClientSession::BeginWrite(
        std::string_view payload, uint32_t cookie) {
    RequireInitialized();
    if (payload.size() > TMessageLimits{}.MaxFrameBytes - sizeof(TIdempotentCommandRequest))
        throw TMessageError("Idempotent client payload exceeds the frame limit");
    if (Session.HasPending) {
        if (payload != Session.PendingPayload)
            throw std::logic_error("An idempotent client write is already unresolved");
        return BuildRequest(Session, cookie);
    }
    if (Session.NextSequence == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("Idempotent client sequence exhausted");

    auto candidate = Session;
    candidate.HasPending = true;
    candidate.PendingPayload.assign(payload);
    auto request = BuildRequest(candidate, cookie);
    Store->Save(candidate);
    Session = std::move(candidate);
    return request;
}

TMessageHolder<TIdempotentCommandRequest> TIdempotentClientSession::RetryPending(
        uint32_t cookie) const {
    RequireInitialized();
    if (!Session.HasPending) throw std::logic_error("No idempotent client write is pending");
    return BuildRequest(Session, cookie);
}

bool TIdempotentClientSession::Complete(
        const TMessageHolder<TIdempotentCommandResponse>& response) {
    ValidateIdempotentCommandResponse(response);
    if (!Session.HasPending || response->Epoch != Session.Epoch ||
        response->SessionIdHigh != Session.SessionIdHigh ||
        response->SessionIdLow != Session.SessionIdLow ||
        response->Sequence != Session.NextSequence ||
        response->ErrorCode != TCommandResponse::Success)
        return false;

    auto candidate = Session;
    candidate.HasPending = false;
    candidate.PendingPayload.clear();
    ++candidate.NextSequence;
    ValidateClientSessionState(candidate);
    Store->Save(candidate);
    Session = std::move(candidate);
    return true;
}

TMessageHolder<TIdempotentCommandRequest> TIdempotentClientSession::BuildRequest(
        const TIdempotentClientSessionState& state, uint32_t cookie) const {
    auto request = NewHoldedMessage<TIdempotentCommandRequest>(
        sizeof(TIdempotentCommandRequest) + state.PendingPayload.size());
    request->Epoch = state.Epoch;
    request->SessionIdHigh = state.SessionIdHigh;
    request->SessionIdLow = state.SessionIdLow;
    request->Sequence = state.NextSequence;
    request->Cookie = cookie;
    if (!state.PendingPayload.empty())
        std::memcpy(request->Data, state.PendingPayload.data(), state.PendingPayload.size());
    return request;
}

void TIdempotentClientSession::RequireInitialized() const {
    if (!Session.Initialized) throw std::logic_error("Idempotent client session is not initialized");
}
