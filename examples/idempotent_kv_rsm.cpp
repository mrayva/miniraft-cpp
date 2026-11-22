#include "idempotent_kv_rsm.h"
#include "kv_messages.h"
#include "payload_validation.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace {
struct TParsedKvWrite {
    std::string_view Key;
    std::string_view Value;
};

TParsedKvWrite ParseKvWrite(std::string_view payload) {
    if (payload.size() < sizeof(TKvEntry)) throw TMessageError("Short idempotent KV write");
    uint16_t keySize = 0, valueSize = 0;
    std::memcpy(&keySize, payload.data(), sizeof(keySize));
    std::memcpy(&valueSize, payload.data() + sizeof(keySize), sizeof(valueSize));
    const uint64_t dataSize = uint64_t(keySize) + valueSize;
    if (dataSize != payload.size() - sizeof(TKvEntry))
        throw TMessageError("Invalid idempotent KV write extent");
    const auto data = payload.substr(sizeof(TKvEntry));
    return {data.substr(0, keySize), data.substr(keySize, valueSize)};
}

std::string_view RequestPayload(const TIdempotentCommandRequest& request) {
    return {request.Data, request.Len - sizeof(TIdempotentCommandRequest)};
}

std::string_view LogPayload(const TLogEntry& entry) {
    return {entry.Data, entry.Len - sizeof(TLogEntry)};
}

auto FindSession(std::vector<TIdempotencySessionRecord>& sessions,
        uint64_t high, uint64_t low) {
    return std::find_if(sessions.begin(), sessions.end(), [&](const auto& session) {
        return session.SessionIdHigh == high && session.SessionIdLow == low;
    });
}

auto FindSession(const std::vector<TIdempotencySessionRecord>& sessions,
        uint64_t high, uint64_t low) {
    return std::find_if(sessions.begin(), sessions.end(), [&](const auto& session) {
        return session.SessionIdHigh == high && session.SessionIdLow == low;
    });
}

size_t ActiveSessions(const std::vector<TIdempotencySessionRecord>& sessions) {
    return static_cast<size_t>(std::count_if(sessions.begin(), sessions.end(),
        [](const auto& session) { return !session.Closed; }));
}

size_t ClosedSessions(const std::vector<TIdempotencySessionRecord>& sessions) {
    return sessions.size() - ActiveSessions(sessions);
}

uint64_t CachedResponseBytes(const std::vector<TIdempotencySessionRecord>& sessions) {
    uint64_t total = 0;
    for (const auto& session : sessions) {
        if (session.Closed) continue;
        if (session.ResponseData.size() > UINT64_MAX - total)
            throw std::overflow_error("Idempotent KV response cache size overflow");
        total += session.ResponseData.size();
    }
    return total;
}

void SortSessions(std::vector<TIdempotencySessionRecord>& sessions) {
    std::sort(sessions.begin(), sessions.end(), [](const auto& left, const auto& right) {
        if (left.SessionIdHigh != right.SessionIdHigh)
            return left.SessionIdHigh < right.SessionIdHigh;
        return left.SessionIdLow < right.SessionIdLow;
    });
}
}

TIdempotentKvRsm::TIdempotentKvRsm(TIdempotencySnapshotLimits limits)
    : IIdempotentRsm(std::move(limits)), Kv(std::make_shared<TKv>()) { }

void TIdempotentKvRsm::Validate(TMessageHolder<TCommandRequest> message) const {
    Kv->Validate(std::move(message));
}

TMessageHolder<TMessage> TIdempotentKvRsm::Read(
        TMessageHolder<TCommandRequest> message, uint64_t index) {
    return Kv->Read(std::move(message), index);
}

TMessageHolder<TMessage> TIdempotentKvRsm::Write(
        TMessageHolder<TLogEntry> message, uint64_t index) {
    if (index <= LastAppliedIndex) return {};
    auto candidate = std::make_shared<TKv>(*Kv);
    auto response = candidate->Write(std::move(message), index);
    if (candidate->LastAppliedIndex != index)
        throw std::logic_error("KV application did not advance to the committed index");
    Kv.swap(candidate);
    LastAppliedIndex = index;
    return response;
}

TMessageHolder<TLogEntry> TIdempotentKvRsm::Prepare(
        TMessageHolder<TCommandRequest> message) {
    return Kv->Prepare(std::move(message));
}

void TIdempotentKvRsm::ValidateIdempotent(
        TMessageHolder<TIdempotentCommandRequest> request) const {
    ValidateIdempotentCommandRequest(request);
    const auto parsed = ParseKvWrite(RequestPayload(*request.Mes));
    if (parsed.Value.size() > SnapshotLimits.MaxResponseBytes)
        throw TMessageError("Idempotent KV response exceeds its configured limit");
    if (request->Epoch != Epoch) throw std::invalid_argument("Stale idempotent KV epoch");

    const auto session = FindSession(Sessions, request->SessionIdHigh, request->SessionIdLow);
    if (session == Sessions.end()) {
        if (request->Sequence != 1)
            throw std::invalid_argument("New idempotent KV session must start at sequence one");
        if (ActiveSessions(Sessions) >= SnapshotLimits.MaxActiveSessions)
            throw std::invalid_argument("Idempotent KV active-session limit reached");
        const auto cached = CachedResponseBytes(Sessions);
        if (cached > SnapshotLimits.MaxCachedResponseBytes ||
            parsed.Value.size() > SnapshotLimits.MaxCachedResponseBytes - cached)
            throw std::invalid_argument("Idempotent KV response cache limit reached");
        return;
    }
    if (session->Closed) throw std::invalid_argument("Idempotent KV session is closed");

    const auto digest = ComputeIdempotentPayloadDigest(RequestPayload(*request.Mes));
    if (request->Sequence == session->HighestSequence) {
        if (digest != session->PayloadDigest)
            throw std::invalid_argument("Idempotent KV sequence reused with another payload");
        return;
    }
    if (request->Sequence < session->HighestSequence)
        throw std::invalid_argument("Idempotent KV sequence is already completed");
    if (request->Sequence - session->HighestSequence != 1)
        throw std::invalid_argument("Gap in idempotent KV sequence");
    const auto cached = CachedResponseBytes(Sessions);
    const auto previousBytes = session->ResponseData.size();
    if (cached < previousBytes || cached - previousBytes > SnapshotLimits.MaxCachedResponseBytes ||
        parsed.Value.size() > SnapshotLimits.MaxCachedResponseBytes - (cached - previousBytes))
        throw std::invalid_argument("Idempotent KV response cache limit reached");
}

uint64_t TIdempotentKvRsm::CachedResponseDataUpperBound(
        TMessageHolder<TIdempotentCommandRequest> request) const {
    return ParseKvWrite(RequestPayload(*request.Mes)).Value.size();
}

TMessageHolder<TLogEntry> TIdempotentKvRsm::PrepareIdempotent(
        TMessageHolder<TIdempotentCommandRequest> request) {
    ValidateIdempotent(request);
    const auto payload = RequestPayload(*request.Mes);
    auto entry = NewHoldedMessage<TLogEntry>(sizeof(TLogEntry) + payload.size());
    if (!payload.empty()) std::memcpy(entry->Data, payload.data(), payload.size());
    return entry;
}

TIdempotentWriteResult TIdempotentKvRsm::WriteIdempotent(
        TMessageHolder<TLogEntry> command, uint64_t index,
        const TIdempotentOperationIdentity& identity) {
    if (!command.HasValidExtent() || command->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        command->Flags != TLogEntry::ENone || command->Len < sizeof(TLogEntry))
        throw TMessageError("Invalid prepared idempotent KV write");
    const auto parsed = ParseKvWrite(LogPayload(*command.Mes));
    if (identity.PayloadDigest != ComputeIdempotentPayloadDigest(LogPayload(*command.Mes)))
        throw TMessageError("Idempotent KV payload digest mismatch");
    const auto existing = FindSession(Sessions, identity.SessionIdHigh, identity.SessionIdLow);

    auto advanceWithoutMutation = [&](uint32_t error, uint64_t responseIndex = 0) {
        Kv->LastAppliedIndex = index;
        LastAppliedIndex = index;
        return TIdempotentWriteResult{.Index = responseIndex ? responseIndex : index,
            .ErrorCode = error};
    };

    if (identity.Epoch != Epoch)
        return advanceWithoutMutation(TCommandResponse::StaleEpoch);
    if (existing != Sessions.end()) {
        if (existing->Closed) return advanceWithoutMutation(TCommandResponse::SessionClosed);
        if (identity.Sequence == existing->HighestSequence) {
            if (identity.PayloadDigest == existing->PayloadDigest) {
                Kv->LastAppliedIndex = index;
                LastAppliedIndex = index;
                return {.Index = existing->LogIndex, .ErrorCode = existing->ErrorCode,
                    .Data = existing->ResponseData};
            }
            return advanceWithoutMutation(TCommandResponse::IdempotencyConflict);
        }
        if (identity.Sequence < existing->HighestSequence)
            return advanceWithoutMutation(TCommandResponse::AlreadyCompleted,
                existing->LogIndex);
        if (identity.Sequence - existing->HighestSequence != 1)
            return advanceWithoutMutation(TCommandResponse::SequenceGap);
    } else if (identity.Sequence != 1) {
        return advanceWithoutMutation(TCommandResponse::SequenceGap);
    }

    if (existing == Sessions.end() &&
        ActiveSessions(Sessions) >= SnapshotLimits.MaxActiveSessions)
        return advanceWithoutMutation(TCommandResponse::Overloaded);

    const std::string response(parsed.Value);
    const auto cached = CachedResponseBytes(Sessions);
    const auto previousBytes = existing == Sessions.end() ? 0 : existing->ResponseData.size();
    if (cached < previousBytes || cached - previousBytes > SnapshotLimits.MaxCachedResponseBytes ||
        response.size() > SnapshotLimits.MaxCachedResponseBytes - (cached - previousBytes))
        return advanceWithoutMutation(TCommandResponse::Overloaded);

    auto candidateKv = std::make_shared<TKv>(*Kv);
    auto candidateSessions = Sessions;
    auto candidateSession = FindSession(candidateSessions,
        identity.SessionIdHigh, identity.SessionIdLow);
    TIdempotencySessionRecord record{
        .SessionIdHigh = identity.SessionIdHigh,
        .SessionIdLow = identity.SessionIdLow,
        .HighestSequence = identity.Sequence,
        .PayloadDigest = identity.PayloadDigest,
        .LogIndex = index,
        .ErrorCode = TCommandResponse::Success,
        .ResponseData = response,
    };
    if (candidateSession == candidateSessions.end())
        candidateSessions.push_back(std::move(record));
    else
        *candidateSession = std::move(record);
    SortSessions(candidateSessions);

    auto applicationResponse = candidateKv->Write(std::move(command), index);
    (void)applicationResponse;
    if (candidateKv->LastAppliedIndex != index)
        throw std::logic_error("Idempotent KV application did not advance its progress");

    Kv.swap(candidateKv);
    Sessions.swap(candidateSessions);
    LastAppliedIndex = index;
    return {.Index = index, .ErrorCode = TCommandResponse::Success, .Data = response};
}

void TIdempotentKvRsm::ValidateIdempotencyControl(
        const TIdempotencyControlOperation& operation) const {
    ValidateIdempotencyControlOperation(operation);
    if (operation.Epoch != Epoch) throw std::invalid_argument("Unexpected idempotent KV epoch");
    if (operation.Kind == EIdempotencyControlKind::CloseSession) {
        const auto session = FindSession(Sessions, operation.SessionIdHigh, operation.SessionIdLow);
        if (session == Sessions.end() || session->Closed)
            throw std::invalid_argument("Cannot close an unknown or closed idempotent KV session");
        if (ClosedSessions(Sessions) >= SnapshotLimits.MaxClosedSessions)
            throw std::invalid_argument("Idempotent KV closed-session limit reached");
    }
}

TMessageHolder<TLogEntry> TIdempotentKvRsm::PrepareIdempotencyControl(
        const TIdempotencyControlOperation& operation) {
    ValidateIdempotencyControl(operation);
    return NewHoldedMessage<TLogEntry>();
}

void TIdempotentKvRsm::ApplyIdempotencyControl(
        const TIdempotencyControlOperation& operation,
        TMessageHolder<TLogEntry> applicationEntry, uint64_t index) {
    ValidateIdempotencyControl(operation);
    if (!applicationEntry.HasValidExtent() ||
        applicationEntry->Type != uint32_t(EMessageType::LOG_ENTRY) ||
        applicationEntry->Len != sizeof(TLogEntry) ||
        applicationEntry->Flags != TLogEntry::ENone)
        throw TMessageError("Invalid idempotent KV control payload");

    if (operation.Kind == EIdempotencyControlKind::CloseSession) {
        auto session = FindSession(Sessions, operation.SessionIdHigh, operation.SessionIdLow);
        session->Closed = true;
        session->ResponseData.clear();
        session->ErrorCode = TCommandResponse::Success;
    } else {
        Sessions.clear();
        Epoch = operation.NewEpoch;
    }
    Kv->LastAppliedIndex = index;
    LastAppliedIndex = index;
}

TIdempotencySnapshot TIdempotentKvRsm::CaptureIdempotencySnapshot() const {
    if (Kv->LastAppliedIndex != LastAppliedIndex)
        throw std::logic_error("KV and idempotency progress diverged");
    return {.LastAppliedIndex = LastAppliedIndex,
        .Epoch = Epoch,
        .ApplicationData = Kv->SaveSnapshot(),
        .Sessions = Sessions};
}

void TIdempotentKvRsm::ValidateIdempotencySnapshot(
        const TIdempotencySnapshot& snapshot, uint64_t index) const {
    ::ValidateIdempotencySnapshot(snapshot, SnapshotLimits);
    if (snapshot.LastAppliedIndex != index || !snapshot.Epoch)
        throw std::runtime_error("Invalid idempotent KV snapshot progress or epoch");
    TKv candidate;
    candidate.ValidateSnapshot(snapshot.ApplicationData, index);
}

void TIdempotentKvRsm::RestoreIdempotencySnapshot(
        TIdempotencySnapshot snapshot, uint64_t index) {
    ValidateIdempotencySnapshot(snapshot, index);
    auto candidate = std::make_shared<TKv>();
    candidate->LoadSnapshot(snapshot.ApplicationData, index);
    Kv.swap(candidate);
    Sessions.swap(snapshot.Sessions);
    Epoch = snapshot.Epoch;
    LastAppliedIndex = index;
}
