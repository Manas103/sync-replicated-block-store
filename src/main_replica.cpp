#include <csignal>
#include <cstdio>
#include <grpcpp/grpcpp.h>

#include "replica_node.h"

using namespace syncrepl;

namespace {
std::unique_ptr<grpc::Server> g_server;
void on_signal(int) {
    if (g_server) g_server->Shutdown();
}
}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr, "usage: %s <node_id> <listen_address> <peer_address> <mediator_address>\n", argv[0]);
        return 2;
    }
    std::string node_id = argv[1];
    std::string listen_address = argv[2];
    std::string peer_address = argv[3];
    std::string mediator_address = argv[4];

    ReplicaNode node(node_id, peer_address, mediator_address);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&node);
    g_server = builder.BuildAndStart();
    if (!g_server) {
        std::fprintf(stderr, "failed to start on %s\n", listen_address.c_str());
        return 1;
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    node.start_healing_thread();
    std::fprintf(stderr, "replica %s listening on %s (peer %s, mediator %s)\n", node_id.c_str(),
                 listen_address.c_str(), peer_address.c_str(), mediator_address.c_str());
    g_server->Wait();
    node.stop();
    std::fprintf(stderr, "replica %s clean shutdown\n", node_id.c_str());
    return 0;
}
