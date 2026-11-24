#pragma once

#include <coroutine>
#include <utility>

#include <coroio/all.hpp>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

namespace NNet {

// TSslSocket owns its transport privately and does not expose cancellation.
// This adapter remembers the native handle before moving it into TSslSocket,
// allowing TNode::Retire to interrupt pending TLS reads and writes safely.
template<class THandle>
class TRetirableSslSocket final : public TSslSocket<THandle> {
    using TBase = TSslSocket<THandle>;

    struct TInit {
        THandle Socket;
        int Fd;
    };

    struct TAcceptedTag {};

    static TInit Capture(THandle&& socket) {
        static_assert(requires(THandle& handle) { handle.Fd(); },
            "TRetirableSslSocket requires a handle with Fd()");
        const auto fd = socket.Fd();
        return {std::move(socket), fd};
    }

    TRetirableSslSocket(TInit init, TSslContext& context)
        : TBase(std::move(init.Socket), context)
        , Fd_(init.Fd)
    { }

    // Accepted sockets also retain their native handle so server shutdown can
    // interrupt a TLS reader blocked on that client connection.
    TRetirableSslSocket(TBase&& socket, TAcceptedTag, int fd)
        : TBase(std::move(socket))
        , Fd_(fd)
    { }

public:
    using TPoller = typename TBase::TPoller;

    TRetirableSslSocket() = default;
    TRetirableSslSocket(THandle&& socket, TSslContext& context)
        : TRetirableSslSocket(Capture(std::move(socket)), context)
    { }

    TRetirableSslSocket(TRetirableSslSocket&& other)
        : TBase(std::move(other))
        , Fd_(std::exchange(other.Fd_, -1))
    { }

    TRetirableSslSocket& operator=(TRetirableSslSocket&& other) {
        if (this != &other) {
            TBase::operator=(std::move(other));
            Fd_ = std::exchange(other.Fd_, -1);
        }
        return *this;
    }

    bool SupportsRetirement() const noexcept { return Fd_ >= 0; }

    void Shutdown() noexcept {
        if (Fd_ < 0) return;
#ifdef _WIN32
        ::shutdown(static_cast<SOCKET>(Fd_), SD_BOTH);
#else
        ::shutdown(Fd_, SHUT_RDWR);
#endif
    }

    void Close() {
        if (Fd_ < 0) return;
        TBase::Close();
        Fd_ = -1;
    }

    TValueTask<TRetirableSslSocket> Accept() {
        auto socket = co_await TBase::Accept();
        const auto fd = socket.Fd();
        co_return TRetirableSslSocket(std::move(socket), TAcceptedTag{}, fd);
    }

    TValueTask<TRetirableSslSocket> Accept(TTime deadline)
        requires requires(TBase& socket, TTime until) { socket.Accept(until); }
    {
        auto socket = co_await TBase::Accept(deadline);
        const auto fd = socket.Fd();
        co_return TRetirableSslSocket(std::move(socket), TAcceptedTag{}, fd);
    }

private:
    int Fd_ = -1;
};

} // namespace NNet
