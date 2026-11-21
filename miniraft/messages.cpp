#include "messages.h"

void ValidateMessageLimits(const TMessageLimits& limits) {
    if (limits.MaxFrameBytes < sizeof(TMessage) || limits.MaxBatchBytes < sizeof(TMessage))
        throw std::invalid_argument("Message byte limits cannot hold a header");
}

void ValidateMessageHeader(uint32_t type, uint32_t length, const TMessageLimits& limits) {
    uint32_t minimum;
    bool fixed = true;
    switch (EMessageType(type)) {
    case EMessageType::LOG_ENTRY: minimum = sizeof(TLogEntry); fixed = false; break;
    case EMessageType::REQUEST_VOTE_REQUEST: minimum = sizeof(TRequestVoteRequest); break;
    case EMessageType::REQUEST_VOTE_RESPONSE: minimum = sizeof(TRequestVoteResponse); break;
    case EMessageType::PRE_VOTE_REQUEST: minimum = sizeof(TPreVoteRequest); break;
    case EMessageType::PRE_VOTE_RESPONSE: minimum = sizeof(TPreVoteResponse); break;
    case EMessageType::APPEND_ENTRIES_REQUEST: minimum = sizeof(TAppendEntriesRequest); break;
    case EMessageType::APPEND_ENTRIES_RESPONSE: minimum = sizeof(TAppendEntriesResponse); break;
    case EMessageType::INSTALL_SNAPSHOT_REQUEST:
        minimum = sizeof(TInstallSnapshotRequest) + 1;
        if (length > sizeof(TInstallSnapshotRequest) + TInstallSnapshotRequest::MaxChunkBytes)
            throw TMessageError("Snapshot chunk exceeds byte limit");
        fixed = false; break;
    case EMessageType::INSTALL_SNAPSHOT_RESPONSE: minimum = sizeof(TInstallSnapshotResponse); break;
    case EMessageType::TIMEOUT_NOW_REQUEST: minimum = sizeof(TTimeoutNowRequest); break;
    case EMessageType::CONFIGURATION_BARRIER_REQUEST: minimum = sizeof(TConfigurationBarrierRequest); break;
    case EMessageType::CONFIGURATION_BARRIER_RESPONSE: minimum = sizeof(TConfigurationBarrierResponse); break;
    case EMessageType::COMMAND_REQUEST: minimum = sizeof(TCommandRequest); fixed = false; break;
    case EMessageType::COMMAND_RESPONSE: minimum = sizeof(TCommandResponse); fixed = false; break;
    case EMessageType::IDEMPOTENT_COMMAND_REQUEST:
        minimum = sizeof(TIdempotentCommandRequest); fixed = false; break;
    case EMessageType::IDEMPOTENT_COMMAND_RESPONSE:
        minimum = sizeof(TIdempotentCommandResponse); fixed = false; break;
    case EMessageType::FORWARDED_COMMAND_REQUEST:
        minimum = sizeof(TForwardedCommandRequest); fixed = false; break;
    case EMessageType::FORWARDED_COMMAND_RESPONSE:
        minimum = sizeof(TForwardedCommandResponse); fixed = false; break;
    case EMessageType::PEER_CAPABILITIES_REQUEST: minimum = sizeof(TPeerCapabilitiesRequest); break;
    case EMessageType::PEER_CAPABILITIES_RESPONSE: minimum = sizeof(TPeerCapabilitiesResponse); break;
    case EMessageType::SESSION_FORWARDED_COMMAND_REQUEST:
        minimum = sizeof(TSessionForwardedCommandRequest); fixed = false; break;
    case EMessageType::SESSION_FORWARDED_COMMAND_RESPONSE:
        minimum = sizeof(TSessionForwardedCommandResponse); fixed = false; break;
    default: throw TMessageError("Unknown or unsupported message type");
    }
    if (length < minimum || (fixed && length != minimum) || length > limits.MaxFrameBytes)
        throw TMessageError("Invalid message length");
}

void ValidateMessageTree(const TMessageHolder<TMessage>& message, const TMessageLimits& limits) {
    ValidateMessageLimits(limits);
    uint64_t remaining = limits.MaxBatchBytes;
    auto validate = [&](const auto& frame, bool child) {
        if (!frame) throw TMessageError("Missing message");
        if (!frame.HasValidExtent()) throw TMessageError("Message length exceeds its buffer");
        ValidateMessageHeader(frame->Type, frame->Len, limits);
        if (frame->Len > remaining) throw TMessageError("Message batch exceeds byte limit");
        remaining -= frame->Len;
        if (child && frame->Type != uint32_t(EMessageType::LOG_ENTRY))
            throw TMessageError("AppendEntries children must be log entries");
        if (frame->Type != uint32_t(EMessageType::APPEND_ENTRIES_REQUEST) && frame.PayloadSize)
            throw TMessageError("Unexpected message payload");
    };
    validate(message, false);
    if (message->Type != uint32_t(EMessageType::APPEND_ENTRIES_REQUEST)) return;
    auto copy = message;
    const auto count = copy.Cast<TAppendEntriesRequest>()->Nentries;
    if (count > limits.MaxEntries || count != message.PayloadSize || count > message.PayloadCapacity || (count && !message.Payload))
        throw TMessageError("Invalid AppendEntries entry count");
    for (uint32_t i = 0; i < count; ++i) validate(message.Payload[i], true);
}

void ValidateIdempotentCommandRequest(
        const TMessageHolder<TIdempotentCommandRequest>& message,
        const TMessageLimits& limits) {
    if (!message.HasValidExtent() ||
        message->Type != uint32_t(EMessageType::IDEMPOTENT_COMMAND_REQUEST))
        throw TMessageError("Invalid idempotent command request");
    ValidateMessageTree(message, limits);
    if (message->Version != TIdempotentCommandRequest::CurrentVersion || message->Reserved != 0 ||
        message->Epoch == 0 || (!message->SessionIdHigh && !message->SessionIdLow) ||
        message->Sequence == 0 || message->Flags != TCommandRequest::EWrite)
        throw TMessageError("Invalid idempotent command metadata");
}

void ValidateIdempotentCommandResponse(
        const TMessageHolder<TIdempotentCommandResponse>& message,
        const TMessageLimits& limits) {
    if (!message.HasValidExtent() ||
        message->Type != uint32_t(EMessageType::IDEMPOTENT_COMMAND_RESPONSE))
        throw TMessageError("Invalid idempotent command response");
    ValidateMessageTree(message, limits);
    if (message->Version != TIdempotentCommandRequest::CurrentVersion || message->Reserved != 0 ||
        message->Epoch == 0 || (!message->SessionIdHigh && !message->SessionIdLow) ||
        message->Sequence == 0 || message->ErrorCode > TCommandResponse::AlreadyCompleted ||
        (message->ErrorCode == TCommandResponse::Success && message->Index == 0))
        throw TMessageError("Invalid idempotent command response metadata");
}
