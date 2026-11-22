#include <string_view>
#include <algorithm>
#include <chrono>
#include <charconv>
#include <csignal>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

#include <miniraft/net/server.h>
#include <miniraft/persist.h>
#include <miniraft/durable.h>
#include <miniraft/snapshot.h>

#include "kv.h"

#include "kv_messages.h"

#include "idempotent_kv_rsm.h"
#include <miniraft/idempotent_client.h>
#include <miniraft/net/idempotent_client_transport.h>

#include <random>

static std::pair<uint64_t, uint64_t> NewClientSessionId() {
    std::random_device random;
    const auto random64 = [&] { return (uint64_t(random()) << 32) | uint64_t(random()); };
    const auto high = random64();
    auto low = random64();
    if (!high && !low) low = 1;
    return {high, low};
}

std::string EncodeIdempotentKvWrite(std::string_view key, std::string_view value) {
    if (key.size() > UINT16_MAX || value.size() > UINT16_MAX)
        throw std::length_error("KV key or value is too large");
    const auto keySize = static_cast<uint16_t>(key.size());
    const auto valueSize = static_cast<uint16_t>(value.size());
    std::string payload(sizeof(TKvEntry) + key.size() + value.size(), '\0');
    memcpy(payload.data(), &keySize, sizeof(keySize));
    memcpy(payload.data() + sizeof(keySize), &valueSize, sizeof(valueSize));
    if (!key.empty()) memcpy(payload.data() + sizeof(TKvEntry), key.data(), key.size());
    if (!value.empty())
        memcpy(payload.data() + sizeof(TKvEntry) + key.size(), value.data(), value.size());
    return payload;
}

void ReportIdempotentReply(const TMessageHolder<TIdempotentCommandResponse>& response) {
    ValidateIdempotentCommandResponse(response);
    if (response->ErrorCode != TCommandResponse::Success) {
        std::cout << "Idempotent write unresolved (error " << response->ErrorCode
                  << "); its operation remains saved for retry.\n";
        return;
    }
    std::cout << "Idempotent write committed at " << response->Index << "\n";
    const auto size = response->Len - sizeof(TIdempotentCommandResponse);
    if (size) std::cout << std::string_view(response->Data, size) << "\n";
}

template<typename TPoller, typename TSocket>
NNet::TFuture<void> Client(TPoller& poller, std::unique_ptr<TSocket> socket,
        NNet::TAddress address, uint32_t flags,
        std::shared_ptr<TIdempotentClientSession> idempotentClient) {
    using TFileHandle = typename TPoller::TFileHandle;
    TFileHandle input{0, poller}; // stdin
    bool socketConnected = false;
    try {
        co_await socket->Connect(NNet::TClock::now() + std::chrono::seconds(2));
        socketConnected = true;
        std::cout << "Connected\n";
    } catch (const std::exception& error) {
        if (!idempotentClient) throw;
        std::cout << "Initial connection failed; idempotent writes can retry: "
                  << error.what() << "\n";
    }

    using TIdempotentTransport = NNet::TIdempotentClient<TPoller, TSocket>;
    std::unique_ptr<TIdempotentTransport> idempotentTransport;
    if (idempotentClient) {
        idempotentTransport = std::make_unique<TIdempotentTransport>(
            poller, address, socket, socketConnected, *idempotentClient);
    }
    TMessageHolder<TIdempotentCommandResponse> terminalReply;
    const auto report = [&](const TMessageHolder<TIdempotentCommandResponse>& response) {
        ReportIdempotentReply(response);
        if (response->Index && (response->ErrorCode == TCommandResponse::StaleEpoch ||
                response->ErrorCode == TCommandResponse::SessionClosed)) {
            terminalReply = response;
            std::cout << "After resolving the old outcome, use replace-terminal-session <epoch>.\n";
        } else if (response->ErrorCode == TCommandResponse::Success) {
            terminalReply = {};
        }
    };

    NNet::TLine line;
    TCommandRequest header;
    header.Flags = TCommandRequest::EWrite;
    header.Type = static_cast<uint32_t>(TCommandRequest::MessageType);
    auto lineReader = NNet::TLineReader(input, 2*1024);
    const char* sep = " \t\r\n";

    try {
        if (idempotentTransport && idempotentTransport->State().HasPending) {
            try {
                auto response = co_await idempotentTransport->RetryPending();
                report(response);
            } catch (const std::exception& error) {
                std::cout << "Pending idempotent write remains saved after reconnect retries: "
                          << error.what() << "\n";
            }
        }
        std::cout << "> " << std::flush;
        while ((line = co_await lineReader.Read())) {
            std::string strLine;
            strLine += std::string_view(line.Part1.data(), line.Part1.size());
            strLine += std::string_view(line.Part2.data(), line.Part2.size());
            auto prefix = strtok((char*)strLine.data(), sep);
            if (!prefix) { std::cout << "> " << std::flush; continue; }
            TMessageHolder<TMessage> req;
            bool isWrite = false;
            std::string idempotentPayload;

            if (!strcmp(prefix, "replace-terminal-session") && idempotentClient) {
                const auto* argument = strtok(nullptr, sep);
                uint64_t epoch = 0;
                if (argument) {
                    const auto* end = argument + strlen(argument);
                    const auto result = std::from_chars(argument, end, epoch);
                    if (result.ec != std::errc{} || result.ptr != end) epoch = 0;
                }
                if (!epoch || strtok(nullptr, sep)) {
                    std::cout << "Expected replace-terminal-session <positive epoch>\n";
                } else if (!terminalReply) {
                    std::cout << "A matching stale epoch or closed session reply is required.\n";
                } else {
                    try {
                        const auto [high, low] = NewClientSessionId();
                        if (idempotentClient->ReplaceTerminalSession(terminalReply, epoch, high, low)) {
                            terminalReply = {};
                            std::cout << "Started a new session. The previous operation can have applied earlier.\n";
                        }
                    } catch (const std::exception& error) {
                        std::cout << "Could not replace the session: " << error.what() << "\n";
                    }
                }
                std::cout << "> " << std::flush;
                continue;
            } else if (!strcmp(prefix, "set")) {
                auto key = strtok(nullptr, sep);
                auto val = strtok(nullptr, sep);
                if (!key || !val) { std::cout << "Expected set <key> <value>\n"; continue; }
                auto keySize = strlen(key);
                auto valSize = strlen(val);
                isWrite = true;
                if (idempotentClient) idempotentPayload = EncodeIdempotentKvWrite(
                    std::string_view(key, keySize), std::string_view(val, valSize));
                auto mes = NewHoldedMessage<TWriteKv>(sizeof(TWriteKv) + keySize + valSize);
                mes->Flags = TCommandRequest::EWrite;
                mes->KeySize = keySize;
                mes->ValSize = valSize;
                memcpy(mes->TKvEntry::Data, key, keySize);
                memcpy(mes->TKvEntry::Data+keySize, val, valSize);
                req = mes;
            } else if (!strcmp(prefix, "get")) {
                auto key = strtok(nullptr, sep);
                if (!key) { std::cout << "Expected a key\n"; continue; }
                auto size = strlen(key);
                auto mes = NewHoldedMessage<TReadKv>(sizeof(TReadKv) + size);
                mes->Flags = flags;
                mes->KeySize = size;
                memcpy(mes->Data, key, size);
                req = mes;
            } else if (!strcmp(prefix, "list")) {
                auto mes = NewHoldedMessage<TReadKv>(sizeof(TReadKv));
                mes->Flags = flags;
                req = mes;
            } else if (!strcmp(prefix, "del")) {
                auto key = strtok(nullptr, sep);
                if (!key) { std::cout << "Expected a key\n"; continue; }
                auto size = strlen(key);
                isWrite = true;
                if (idempotentClient) idempotentPayload = EncodeIdempotentKvWrite(
                    std::string_view(key, size), {});
                auto mes = NewHoldedMessage<TWriteKv>(sizeof(TWriteKv) + size);
                mes->Flags = TCommandRequest::EWrite;
                mes->KeySize = size;
                mes->ValSize = 0;
                memcpy(mes->TKvEntry::Data, key, size);
                req = mes;
            } else {
                std::cout << "Cannot parse command: " << strLine << "\n";
                continue;
            }

            if (idempotentTransport && isWrite) {
                if (idempotentTransport->State().HasPending &&
                    idempotentTransport->State().PendingPayload != idempotentPayload) {
                    TMessageHolder<TIdempotentCommandResponse> previous;
                    try {
                        previous = co_await idempotentTransport->RetryPending();
                    } catch (const std::exception& error) {
                        std::cout << "Pending idempotent write remains saved after reconnect retries: "
                                  << error.what() << "\n";
                        std::cout << "> " << std::flush;
                        continue;
                    }
                    report(previous);
                    if (idempotentTransport->State().HasPending) {
                        std::cout << "Resolve the pending write before starting another write.\n";
                        std::cout << "> " << std::flush;
                        continue;
                    }
                }
                try {
                    auto response = co_await idempotentTransport->Write(idempotentPayload);
                    report(response);
                } catch (const std::exception& error) {
                    std::cout << "Pending idempotent write remains saved after reconnect retries: "
                              << error.what() << "\n";
                }
            } else {
                if (!socketConnected) {
                    socket = std::make_unique<TSocket>(address, poller);
                    co_await socket->Connect();
                    socketConnected = true;
                }
                co_await TMessageWriter(*socket).Write(std::move(req));
                auto reply = co_await TMessageReader(*socket).Read();
                auto res = reply.template Cast<TCommandResponse>();
                auto len = res->Len - sizeof(TCommandResponse);
                std::string_view data(res->Data, len);
                if (res->ErrorCode) {
                    std::cout << "Request failed (error " << res->ErrorCode
                              << "). A write may still have committed.\n";
                } else {
                    std::cout << "Ok, commitIndex: " << res->Index << "\n";
                    if (!data.empty()) std::cout << data << "\n";
                }
            }
            std::cout << "> " << std::flush;
        }
    } catch (const std::exception& ex) {
        std::cout << "Exception: " << ex.what() << "\n";
    }
    co_return;
}

template<typename TPoller>
NNet::TVoidTask CheckpointLoop(TPoller& poller, std::shared_ptr<TRaft> raft,
    std::shared_ptr<IRsm> rsm, std::shared_ptr<TSnapshotStore> store, uint64_t interval, uint64_t saved) {
    while (true) {
        co_await poller.Sleep(std::chrono::seconds(1));
        if (rsm->LastAppliedIndex <= saved || rsm->LastAppliedIndex - saved < interval) continue;
        try {
            auto snapshot = raft->CompactShared();
            store->Save(*snapshot);
            saved = snapshot->Index;
        } catch (const std::exception& error) {
            // The durable state image already contains the authoritative snapshot.
            // A failed standalone checkpoint export can be retried safely.
            std::cerr << "Checkpoint failed: " << error.what() << "\n";
        }
    }
}

static void LoadPeerConfig(const std::string& path, std::vector<THost>& hosts,
        std::vector<uint32_t>& learners) {
    std::ifstream file(path);
    if (!file) throw std::runtime_error("Cannot open peer config: " + path);
    std::unordered_set<uint32_t> ids;
    std::set<std::pair<std::string, int>> addresses;
    std::string line;
    size_t lineNumber = 0;
    bool hasVoter = false;
    while (std::getline(file, line)) {
        ++lineNumber;
        std::istringstream fields(line);
        std::string role, endpoint, extra;
        if (!(fields >> role) || role.starts_with('#')) continue;
        try {
            if (line.find('\0') != std::string::npos || (role != "voter" && role != "learner") || !(fields >> endpoint) ||
                (fields >> extra && !extra.starts_with('#')))
                throw std::invalid_argument("Expected voter or learner followed by address:port:id");
            THost host(endpoint);
            if (!ids.insert(host.Id).second) throw std::invalid_argument("Duplicate node ID");
            if (!addresses.emplace(host.Address, host.Port).second)
                throw std::invalid_argument("Duplicate node address and port");
            if (role == "learner") learners.push_back(host.Id);
            else hasVoter = true;
            hosts.push_back(std::move(host));
        } catch (const std::invalid_argument& error) {
            throw std::invalid_argument(path + ":" + std::to_string(lineNumber) + ": " + error.what());
        }
    }
    if (!file.eof() || file.bad()) throw std::runtime_error("Cannot read peer config: " + path);
    if (!hasVoter) throw std::invalid_argument("Peer config requires at least one voter: " + path);
}

#ifdef __linux__
static volatile std::sig_atomic_t PeerConfigReloadRequested = 0;

static void RequestPeerConfigReload(int) {
    PeerConfigReloadRequested = 1;
}

template<typename TSocket, typename TMakeEndpoint>
static void ReloadPeerConfig(const std::string& path, uint32_t localId,
        std::vector<THost>& hosts, std::vector<uint32_t>& learners,
        TRaftServer<TSocket>& server, TMakeEndpoint makeEndpoint) {
    std::vector<THost> requested;
    std::vector<uint32_t> requestedLearners;
    LoadPeerConfig(path, requested, requestedLearners);
    const std::unordered_set<uint32_t> learnerIds(learners.begin(), learners.end());
    const std::unordered_set<uint32_t> newLearnerIds(requestedLearners.begin(), requestedLearners.end());
    std::unordered_map<uint32_t, size_t> currentById;
    std::map<std::pair<std::string, int>, uint32_t> currentByAddress;
    for (size_t i = 0; i < hosts.size(); ++i) {
        currentById.emplace(hosts[i].Id, i);
        currentByAddress.emplace(std::pair{hosts[i].Address, hosts[i].Port}, hosts[i].Id);
    }

    struct TChange {
        size_t Requested;
        std::optional<size_t> Current;
        std::shared_ptr<TNode<TSocket>> Endpoint;
    };
    std::vector<TChange> changes;
    size_t existing = 0;
    for (size_t i = 0; i < requested.size(); ++i) {
        const auto& host = requested[i];
        std::optional<size_t> current;
        if (const auto it = currentById.find(host.Id); it != currentById.end()) {
            ++existing;
            current = it->second;
            const auto& previous = hosts[*current];
            if (learnerIds.contains(host.Id) != newLearnerIds.contains(host.Id))
                throw std::invalid_argument("Peer config reload cannot change bootstrap roles");
            if (previous.Address == host.Address && previous.Port == host.Port) continue;
            if (host.Id == localId)
                throw std::invalid_argument("Peer config reload cannot change the local listener");
        } else if (!newLearnerIds.contains(host.Id)) {
            throw std::invalid_argument("Peer config reload can only add learners");
        }
        const auto owner = currentByAddress.find({host.Address, host.Port});
        if (owner != currentByAddress.end() && owner->second != host.Id)
            throw std::invalid_argument("Peer config reload cannot reassign an existing address to another node ID");
        changes.push_back({i, current, {}});
    }
    if (existing != hosts.size()) throw std::invalid_argument("Peer config reload cannot remove endpoints");

    // Resolve and allocate every new endpoint before changing any live routes.
    for (auto& change : changes) change.Endpoint = makeEndpoint(requested[change.Requested]);
    hosts.reserve(requested.size());
    learners.reserve(requestedLearners.size());
    for (auto& change : changes) {
        auto& host = requested[change.Requested];
        if (change.Current) {
            server.ReplacePeerTarget(host.Id, std::move(change.Endpoint));
            hosts[*change.Current] = std::move(host);
        } else {
            server.RegisterPeerTarget(host.Id, std::move(change.Endpoint));
            learners.push_back(host.Id);
            hosts.push_back(std::move(host));
        }
    }
    std::cerr << "Reloaded peer config (" << changes.size() << " endpoint changes)\n";
}
#endif

void usage(const char* prog) {
    std::cerr << prog << " --client|--server --id myid "
        "(--node ip:port:id [--node ip:port:id ...] [--learner id ...] | --peer-config path) "
        "[--persist|--durable] [--snapshots entries] [--idempotent [--client-state path]] [--stale] [--consistent]\n";
    exit(0);
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    std::vector<THost> hosts;
    THost myHost;
    TNodeDict nodes;
    std::vector<uint32_t> learners;
    uint32_t id = 0;
    bool server = false;
    bool persist = false;
    bool durable = false;
    bool idempotent = false;
    std::string peerConfig;
    std::string clientStatePath;
    uint64_t snapshotInterval = 0;
    uint32_t flags = 0;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--server")) {
            server = true;
        } else if (!strcmp(argv[i], "--node")) {
            if (i + 1 >= argc) { std::cerr << "--node requires address:port:id\n"; return 1; }
            try { hosts.emplace_back(argv[++i]); }
            catch (const std::invalid_argument& error) { std::cerr << error.what() << "\n"; return 1; }
        } else if (!strcmp(argv[i], "--peer-config")) {
            if (!peerConfig.empty() || i + 1 >= argc || !argv[i + 1][0]) {
                std::cerr << "--peer-config requires one path\n"; return 1;
            }
            peerConfig = argv[++i];
        } else if (!strcmp(argv[i], "--id")) {
            if (i + 1 >= argc) { std::cerr << "--id requires a positive node ID\n"; return 1; }
            const std::string_view value(argv[++i]);
            const auto result = std::from_chars(value.data(), value.data() + value.size(), id);
            if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || !id) {
                std::cerr << "--id requires a positive node ID\n"; return 1;
            }
        } else if (!strcmp(argv[i], "--persist")) {
            persist = true;
        } else if (!strcmp(argv[i], "--durable")) {
            persist = true;
            durable = true;
        } else if (!strcmp(argv[i], "--idempotent")) {
            idempotent = true;
        } else if (!strcmp(argv[i], "--client-state")) {
            if (i + 1 >= argc) { std::cerr << "--client-state requires a path\n"; return 1; }
            clientStatePath = argv[++i];
        } else if (!strcmp(argv[i], "--snapshots")) {
            if (i + 1 >= argc) { std::cerr << "--snapshots requires a positive entry interval\n"; return 1; }
            const std::string_view value(argv[++i]);
            const auto result = std::from_chars(value.data(), value.data() + value.size(), snapshotInterval);
            if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || !snapshotInterval) {
                std::cerr << "--snapshots requires a positive entry interval\n"; return 1;
            }
        } else if (!strcmp(argv[i], "--learner")) {
            if (i + 1 >= argc) { std::cerr << "--learner requires a positive node ID\n"; return 1; }
            const std::string_view value(argv[++i]);
            uint32_t learner = 0;
            const auto result = std::from_chars(value.data(), value.data() + value.size(), learner);
            if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || !learner) {
                std::cerr << "--learner requires a positive node ID\n"; return 1;
            }
            learners.push_back(learner);
        } else if (!strcmp(argv[i], "--stale")) {
            flags |= TCommandRequest::EStale;
        } else if (!strcmp(argv[i], "--consistent")) {
            flags |= TCommandRequest::EConsistent;
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
        }
    }

    if (!peerConfig.empty()) {
        if (!server || !durable || !hosts.empty() || !learners.empty()) {
            std::cerr << "--peer-config requires --server --durable and replaces --node and --learner\n";
            return 1;
        }
        try {
            LoadPeerConfig(peerConfig, hosts, learners);
            if (std::none_of(hosts.begin(), hosts.end(), [id](const THost& host) { return host.Id == id; }))
                throw std::invalid_argument("Local node ID is missing from peer config");
        } catch (const std::exception& error) {
            std::cerr << error.what() << "\n";
            return 1;
        }
    }
    if (!learners.empty() && (!server || !durable)) {
        std::cerr << "--learner requires --server --durable\n"; return 1;
    }
    if (snapshotInterval && (!server || !durable)) {
        std::cerr << "--snapshots requires --server --durable\n"; return 1;
    }
    if (idempotent && server && !durable) {
        std::cerr << "--idempotent server mode requires --durable\n"; return 1;
    }
    if (idempotent && !server && clientStatePath.empty()) {
        std::cerr << "--idempotent client mode requires --client-state\n"; return 1;
    }
    if (!idempotent && !clientStatePath.empty()) {
        std::cerr << "--client-state requires --idempotent\n"; return 1;
    }
    if (server && !clientStatePath.empty()) {
        std::cerr << "--client-state is only valid in client mode\n"; return 1;
    }
    if (hosts.empty()) { std::cerr << "No node endpoints configured\n"; return 1; }
    using TPoller = NNet::TDefaultPoller;
    std::shared_ptr<ITimeSource> timeSource = std::make_shared<TTimeSource>();
    NNet::TLoop<TPoller> loop;

    if (server) {
        const auto makeEndpoint = [&](const THost& host) {
            return std::make_shared<TNode<TPoller::TSocket>>(
                [&](const NNet::TAddress& addr) { return TPoller::TSocket(addr, loop.Poller()); },
                std::to_string(host.Id), NNet::TAddress{host.Address, host.Port}, timeSource);
        };
        std::unordered_set<uint32_t> hostIds;
        for (auto& host : hosts) {
            if (!hostIds.insert(host.Id).second) { std::cerr << "Duplicate node ID\n"; return 1; }
            if (!host) {
                std::cerr << "Empty host\n"; return 1;
            }
            if (host.Id == id) {
                myHost = host;
            } else {
                nodes[host.Id] = makeEndpoint(host);
            }
        }

        if (!myHost) {
            std::cerr << "Host not found\n"; return 1;
        }

        std::shared_ptr<IRsm> rsm = idempotent
            ? std::static_pointer_cast<IRsm>(std::make_shared<TIdempotentKvRsm>())
            : std::static_pointer_cast<IRsm>(std::make_shared<TKv>());
        std::shared_ptr<IState> state = std::make_shared<TState>();
        if (persist) {
            if (durable) state = std::make_shared<TDurableState>("state", myHost.Id);
            else state = std::make_shared<TDiskState>("state", myHost.Id);
        }
        auto raft = std::make_shared<TRaft>(state, myHost.Id, nodes, learners);
        std::shared_ptr<TSnapshotStore> snapshots;
        uint64_t savedSnapshot = state->SnapshotIndex;
        if (snapshotInterval) {
            snapshots = std::make_shared<TSnapshotStore>("state", myHost.Id);
            if (auto snapshot = snapshots->Load()) {
                if (snapshot->Index > state->SnapshotIndex) {
                    RestoreSnapshot(*snapshot, *raft, *rsm);
                    savedSnapshot = snapshot->Index;
                }
            }
        }
        TPoller::TSocket socket(NNet::TAddress{myHost.Address, myHost.Port}, loop.Poller());
#ifdef __linux__
        if (!peerConfig.empty()) {
            struct sigaction action{};
            action.sa_handler = RequestPeerConfigReload;
            sigemptyset(&action.sa_mask);
            if (sigaction(SIGHUP, &action, nullptr) != 0) {
                std::cerr << "Cannot install peer config reload handler\n";
                return 1;
            }
        }
#endif
        socket.Bind();
        socket.Listen();
        TRaftServer server(loop.Poller(), std::move(socket), raft, rsm, nodes, timeSource);
        server.Serve();
        if (snapshots) CheckpointLoop(loop.Poller(), raft, rsm, snapshots, snapshotInterval, savedSnapshot);
#ifdef __linux__
        if (!peerConfig.empty()) {
            for (;;) {
                loop.Step();
                if (!PeerConfigReloadRequested) continue;
                PeerConfigReloadRequested = 0;
                try {
                    ReloadPeerConfig(peerConfig, myHost.Id, hosts, learners, server, makeEndpoint);
                } catch (const std::exception& error) {
                    std::cerr << "Peer config reload failed: " << error.what() << "\n";
                }
            }
        }
#endif
        loop.Loop();
    } else {
        NNet::TAddress addr{hosts[0].Address, hosts[0].Port};
        auto socket = std::make_unique<TPoller::TSocket>(addr, loop.Poller());

        std::shared_ptr<TIdempotentClientSession> idempotentClient;
        if (idempotent) {
            auto store = std::make_shared<TDurableIdempotentClientStateStore>(clientStatePath);
            idempotentClient = std::make_shared<TIdempotentClientSession>(store);
            if (!idempotentClient->State().Initialized) {
                const auto [high, low] = NewClientSessionId();
                idempotentClient->StartSession(1, high, low);
            }
        }
        auto h = Client(loop.Poller(), std::move(socket), std::move(addr), flags,
            std::move(idempotentClient));
        while (!h.done()) {
            loop.Step();
        }
    }

    return 0;
}
