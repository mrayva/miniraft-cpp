#pragma once

// Template definitions kept visible for alternative socket implementations.
template<typename TSocket>
void TNode<TSocket>::Send(TMessageHolder<TMessage> message) {
    if (Retired) return;
    Messages.emplace_back(std::move(message));
}

template<typename TSocket>
void TNode<TSocket>::Drain() {
    if (Retired) {
        if (Drainer && Drainer.done()) { Drainer.destroy(); Drainer = {}; }
        if (Connector && Connector.done()) { Connector.destroy(); Connector = {}; }
        if (!ResponseReaderActive && !Drainer && !Connector && !RetiredCleaned) {
            Disconnect(ConnectionGeneration);
            RetiredCleaned = true;
        }
        return;
    }
    if (!Connected) {
        Connect();
        return;
    }
    if (!Drainer || Drainer.done()) {
        if (Drainer && Drainer.done()) {
            Drainer.destroy();
        }
        Drainer = DoDrain();
    }
}

template<typename TSocket>
void TNode<TSocket>::Retire() {
    if (Retired) return;
    if (!SupportsRetirement())
        throw std::logic_error("Socket type does not support safe peer retirement");
    Retired = true;
    Messages.clear();
    ShutdownSocket();
}

template<typename TSocket>
void TNode<TSocket>::ShutdownSocket() noexcept {
    if constexpr (requires(TSocket& socket) { { socket.Shutdown() } noexcept; }) {
        Socket.Shutdown();
    } else if constexpr (requires(TSocket& socket) { socket.Fd(); }) {
        const auto fd = Socket.Fd();
        if (fd >= 0) {
#ifdef _WIN32
            ::shutdown(static_cast<SOCKET>(fd), SD_BOTH);
#else
            ::shutdown(fd, SHUT_RDWR);
#endif
        }
    }
}

template<typename TSocket>
NNet::TVoidSuspendedTask TNode<TSocket>::DoDrain() {
    try {
        while (!Messages.empty()) {
            auto tosend = std::move(Messages); Messages.clear();
            for (auto&& m : tosend) {
                co_await TMessageWriter(Socket).Write(std::move(m));
            }
        }
    } catch (const std::exception& ex) {
        std::cout << "Error on write: " << ex.what() << "\n";
        if (!Retired) Connect();
    }
    co_return;
}

template<typename TSocket>
void TNode<TSocket>::Disconnect(uint64_t generation) {
    if (generation != ConnectionGeneration || (Connector && !Connector.done())) return;
    // DoConnect may already be retrying after a write failure. Its awaiter can
    // still be registered with the poller; never destroy that live coroutine
    // because an older reader failed. Established connectors are done here.
    Connected = false;
    if (Drainer) { Drainer.destroy(); Drainer = {}; }
    if (Connector) { Connector.destroy(); Connector = {}; }
    std::destroy_at(&Socket);
    std::construct_at(&Socket);
}

template<typename TSocket>
void TNode<TSocket>::Connect() {
    if (!Retired && Address && (!Connector || Connector.done())) {
        if (ConnectionGeneration == UINT64_MAX) throw std::overflow_error("Connection generation exhausted");
        if (Connector && Connector.done()) {
            Connector.destroy();
        }
        ++ConnectionGeneration;
        Connector = DoConnect();
    }
}

template<typename TSocket>
NNet::TVoidSuspendedTask TNode<TSocket>::DoConnect() {
    std::cout << "Connecting " << Name << "\n";
    Connected = false;
    while (!Connected && !Retired) {
        try {
            Socket = SocketFactory(*Address);
            // The dependency's finite deadline can leave I/O and timer wakeups
            // registered for the same coroutine after a timeout. Use its default
            // I/O-only connect until cancellation of both sources is supported.
            // TCP connect failure timing is therefore controlled by the OS.
            co_await Socket.Connect();
            if (Retired) {
                ShutdownSocket();
                break;
            }
            std::cout << "Connected " << Name << "\n";
            Connected = true;
        } catch (const std::exception& ex) {
            std::cout << "Error on connect: " << Name << " " << ex.what() << "\n";
        }
        if (!Connected && !Retired) {
            co_await Socket.Poller()->Sleep(std::chrono::milliseconds(1000));
        }
    }
    co_return;
}

