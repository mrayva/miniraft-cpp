#include <coroio/all.hpp>
#include <csignal>
#include <miniraft/timesource.h>
#include <miniraft/raft.h>
#include <miniraft/net/server.h>
#include <charconv>
#include <optional>

void usage(const char* prog) {
    std::cerr << prog << " --id myid --node ip:port:id [--node ip:port:id ...]" << "\n";
    std::cerr << "  [--ssl] enables legacy TLS without peer authentication\n";
    std::cerr << "  [--mtls-ca ca.pem --mtls-cert node.pem --mtls-key node.key] enables mutual TLS; certificate CN must be the decimal node ID\n";
    exit(0);
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    std::vector<THost> hosts;
    THost myHost;
    TNodeDict nodes;
    uint32_t id = 0;
    bool ssl = false;
    std::string mtlsCa;
    std::string mtlsCert;
    std::string mtlsKey;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--node") && i < argc - 1) {
            // address:port:id
            hosts.push_back(THost{argv[++i]});
        } else if (!strcmp(argv[i], "--id") && i < argc - 1) {
            id = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--mtls-ca") && i < argc - 1) {
            mtlsCa = argv[++i];
        } else if (!strcmp(argv[i], "--mtls-cert") && i < argc - 1) {
            mtlsCert = argv[++i];
        } else if (!strcmp(argv[i], "--mtls-key") && i < argc - 1) {
            mtlsKey = argv[++i];
        } else if (!strcmp(argv[i], "--ssl")) {
            ssl = true;
        } else if (!strcmp(argv[i], "--help")) {
            usage(argv[0]);
        }
    }

    const bool hasMtlsCa = !mtlsCa.empty();
    const bool hasMtlsCert = !mtlsCert.empty();
    const bool hasMtlsKey = !mtlsKey.empty();
    const bool mtls = hasMtlsCa && hasMtlsCert && hasMtlsKey;
    if ((hasMtlsCa || hasMtlsCert || hasMtlsKey) && !mtls) {
        std::cerr << "Mutual TLS requires --mtls-ca, --mtls-cert, and --mtls-key\n";
        return 2;
    }
    ssl = ssl || mtls;

    using TPoller = NNet::TDefaultPoller;
    using TPeerSocket = NNet::TRetirableSslSocket<TPoller::TSocket>;

    std::shared_ptr<ITimeSource> timeSource = std::make_shared<TTimeSource>();
    NNet::TLoop<TPoller> loop;
    std::shared_ptr<NNet::TSslContext> clientContext;
    std::shared_ptr<NNet::TSslContext> serverContext;
    std::function<void(const char*)> sslDebugLogFunc = [](const char* s) { std::cerr << s << "\n"; };

    if (mtls) {
        clientContext = std::make_shared<NNet::TSslContext>(
            NNet::TSslContext::MutualTlsClient(mtlsCa.c_str(), mtlsCert.c_str(), mtlsKey.c_str(), sslDebugLogFunc));
        serverContext = std::make_shared<NNet::TSslContext>(
            NNet::TSslContext::MutualTlsServer(mtlsCert.c_str(), mtlsKey.c_str(), mtlsCa.c_str(), sslDebugLogFunc));
    } else if (ssl) {
        clientContext = std::make_shared<NNet::TSslContext>(NNet::TSslContext::Client(sslDebugLogFunc));
        serverContext = std::make_shared<NNet::TSslContext>(NNet::TSslContext::Server("server.crt", "server.key", sslDebugLogFunc));
    }

    for (auto& host : hosts) {
        if (!host) {
            std::cerr << "Empty host\n"; return 1;
        }
        if (host.Id == id) {
            myHost = host;
        } else {
            if (ssl) {
                nodes[host.Id] = std::make_shared<TNode<TPeerSocket>>(
                    [&](const NNet::TAddress& addr) {
                        return TPeerSocket(TPoller::TSocket(addr, loop.Poller()), *clientContext.get());
                    },
                    std::to_string(host.Id),
                    NNet::TAddress{host.Address, host.Port},
                    timeSource);
            } else {
                nodes[host.Id] = std::make_shared<TNode<TPoller::TSocket>>(
                    [&](const NNet::TAddress& addr) { return TPoller::TSocket(addr, loop.Poller()); },
                    std::to_string(host.Id),
                    NNet::TAddress{host.Address, host.Port},
                    timeSource);
            }
        }
    }

    if (!myHost) {
        std::cerr << "Host not found\n"; return 1;
    }

    std::function<std::optional<uint32_t>(const TPeerSocket&)> peerIdentityResolver;
    if (mtls) {
        // The trusted certificate's Common Name is the authenticated peer ID.
        // Reject ambiguous names and malformed IDs; outbound Raft connections
        // also require the certificate ID to match the configured endpoint.
        // The CA is the identity issuer, so later learner IDs are supported.
        peerIdentityResolver = [](const TPeerSocket& socket) -> std::optional<uint32_t> {
            auto certificate = socket.GetPeerCertificate();
            if (!certificate) return std::nullopt;
            X509_NAME* subject = X509_get_subject_name(certificate.get());
            const int nameIndex = X509_NAME_get_index_by_NID(subject, NID_commonName, -1);
            if (nameIndex < 0 || X509_NAME_get_index_by_NID(subject, NID_commonName, nameIndex) >= 0)
                return std::nullopt;
            X509_NAME_ENTRY* commonName = X509_NAME_get_entry(subject, nameIndex);
            ASN1_STRING* value = X509_NAME_ENTRY_get_data(commonName);
            const auto* text = reinterpret_cast<const char*>(ASN1_STRING_get0_data(value));
            const int length = ASN1_STRING_length(value);
            if (!text || length <= 0) return std::nullopt;
            uint32_t peerId = 0;
            const auto [end, error] = std::from_chars(text, text + length, peerId);
            if (error != std::errc{} || end != text + length || !peerId)
                return std::nullopt;
            return peerId;
        };
    }

    std::shared_ptr<IRsm> rsm = std::make_shared<TDummyRsm>();
    auto raft = std::make_shared<TRaft>(std::make_shared<TState>(), myHost.Id, nodes);
    TPoller::TSocket socket(NNet::TAddress{myHost.Address, myHost.Port}, loop.Poller());
    socket.Bind();
    socket.Listen();
    if (ssl) {
        auto sslSocket = TPeerSocket(std::move(socket), *serverContext.get());
        TRaftServer server(loop.Poller(), std::move(sslSocket), raft, rsm, nodes, timeSource,
            TRequestProcessor::TAdmissionLimits{}, TOutboundQueueLimits{},
            TInboundMessageBudgetLimits{}, peerIdentityResolver);
        server.Serve();
        loop.Loop();
    } else {
        TRaftServer server(loop.Poller(), std::move(socket), raft, rsm, nodes, timeSource);
        server.Serve();
        loop.Loop();
    }
    return 0;
}
