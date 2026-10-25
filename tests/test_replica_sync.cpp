#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <grpcpp/grpcpp.h>
#include <thread>

#include "mediator_service.h"
#include "replica_node.h"

using namespace syncrepl;
using namespace std::chrono_literals;

namespace {

bool wait_until(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

// Hosts a mediator and two replica nodes as real grpc::Server instances
// on loopback ports within this test process: real RPCs, real sockets,
// just not real separate OS processes (those are reserved for the
// fault_harness measurement tool, where literal SIGKILL matters).
struct Pod {
    MediatorServiceImpl mediator_impl;
    std::unique_ptr<ReplicaNode> a, b;
    std::unique_ptr<grpc::Server> mediator_server, server_a, server_b;
    std::unique_ptr<ReplicaService::Stub> stub_a, stub_b;

    Pod(int mediator_port, int port_a, int port_b) {
        std::string mediator_addr = "127.0.0.1:" + std::to_string(mediator_port);
        std::string addr_a = "127.0.0.1:" + std::to_string(port_a);
        std::string addr_b = "127.0.0.1:" + std::to_string(port_b);

        grpc::ServerBuilder mb;
        mb.AddListeningPort(mediator_addr, grpc::InsecureServerCredentials());
        mb.RegisterService(&mediator_impl);
        mediator_server = mb.BuildAndStart();

        a = std::make_unique<ReplicaNode>("A", addr_b, mediator_addr);
        b = std::make_unique<ReplicaNode>("B", addr_a, mediator_addr);

        grpc::ServerBuilder ab;
        ab.AddListeningPort(addr_a, grpc::InsecureServerCredentials());
        ab.RegisterService(a.get());
        server_a = ab.BuildAndStart();

        grpc::ServerBuilder bb;
        bb.AddListeningPort(addr_b, grpc::InsecureServerCredentials());
        bb.RegisterService(b.get());
        server_b = bb.BuildAndStart();

        a->start_healing_thread();
        b->start_healing_thread();

        stub_a = ReplicaService::NewStub(grpc::CreateChannel(addr_a, grpc::InsecureChannelCredentials()));
        stub_b = ReplicaService::NewStub(grpc::CreateChannel(addr_b, grpc::InsecureChannelCredentials()));
    }

    ~Pod() {
        a->stop();
        b->stop();
        mediator_server->Shutdown();
        server_a->Shutdown();
        server_b->Shutdown();
    }

    bool write(ReplicaService::Stub* stub, uint32_t key, const std::string& value, WriteReply* reply) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 2s);
        WriteRequest req;
        req.set_key(key);
        req.set_value(value);
        return stub->Write(&ctx, req, reply).ok();
    }
    bool read(ReplicaService::Stub* stub, uint32_t key, ReadReply* reply) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 2s);
        ReadRequest req;
        req.set_key(key);
        return stub->Read(&ctx, req, reply).ok();
    }
};

}  // namespace

TEST(ReplicaSync, WriteReplicatesSynchronouslyToBothNodes) {
    Pod pod(23801, 23802, 23803);
    ASSERT_TRUE(wait_until([&] { return !pod.a->is_fenced() && !pod.b->is_fenced(); }, 3000ms));

    WriteReply wr;
    ASSERT_TRUE(pod.write(pod.stub_a.get(), 7, "hello", &wr));
    ASSERT_TRUE(wr.ok());

    ReadReply rr;
    ASSERT_TRUE(pod.read(pod.stub_b.get(), 7, &rr));
    EXPECT_TRUE(rr.ok());
    EXPECT_EQ(rr.value(), "hello");
}

TEST(ReplicaSync, PartitionedNodeFencesAndSurvivorServesAlone) {
    Pod pod(23811, 23812, 23813);
    ASSERT_TRUE(wait_until([&] { return !pod.a->is_fenced() && !pod.b->is_fenced(); }, 3000ms));

    // Isolate A: its own calls to B, and B's calls to it, both start
    // failing, exactly like a real link cut.
    {
        grpc::ClientContext ctx;
        FaultModeRequest req;
        req.set_drop_peer_calls(true);
        FaultModeReply reply;
        pod.stub_a->SetFaultMode(&ctx, req, &reply);
    }

    WriteReply wr;
    ASSERT_TRUE(pod.write(pod.stub_b.get(), 9, "solo-write", &wr));
    EXPECT_TRUE(wr.ok());
    ASSERT_TRUE(wait_until([&] { return pod.b->is_solo_survivor(); }, 3000ms));

    // A should lose the race and fence itself once it also notices it
    // cannot reach B.
    WriteReply wr_a;
    pod.write(pod.stub_a.get(), 10, "should-fence", &wr_a);
    ASSERT_TRUE(wait_until([&] { return pod.a->is_fenced(); }, 3000ms));

    // Heal the partition: A's healing thread should resync from B and
    // both should resume normal dual-write mode.
    {
        grpc::ClientContext ctx;
        FaultModeRequest req;
        req.set_drop_peer_calls(false);
        FaultModeReply reply;
        pod.stub_a->SetFaultMode(&ctx, req, &reply);
    }
    ASSERT_TRUE(wait_until([&] { return !pod.a->is_fenced() && !pod.b->is_solo_survivor(); }, 3000ms));

    ReadReply rr;
    ASSERT_TRUE(pod.read(pod.stub_a.get(), 9, &rr));
    EXPECT_EQ(rr.value(), "solo-write") << "A did not pick up the write B took while A was isolated";
}
