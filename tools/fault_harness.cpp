// The measurement program behind every number in this project's README:
// spawns a mediator and two replica nodes as real, separate OS processes,
// drives a client write/read workload against them, injects 300 random
// partitions and process kills, times how long each takes to recover
// from, and checks the entire observed history with an independent
// linearizability checker.
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "linearizability_checker.h"
#include "mediator.grpc.pb.h"
#include "replica.grpc.pb.h"

using namespace syncrepl;
using Clock = std::chrono::steady_clock;

namespace {

uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

// A child OS process this harness started, tracked by the PID it holds in
// this variable (never a PID discovered from a process list).
class Child {
public:
    void start(const std::vector<std::string>& argv) {
        pid_t pid = fork();
        if (pid == 0) {
            std::vector<char*> cargv;
            for (auto& s : argv) cargv.push_back(const_cast<char*>(s.c_str()));
            cargv.push_back(nullptr);
            execvp(cargv[0], cargv.data());
            _exit(127);
        }
        pid_ = pid;
    }
    void kill_now() {
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            int status;
            waitpid(pid_, &status, 0);
            pid_ = -1;
        }
    }
    void stop_gracefully() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status;
            waitpid(pid_, &status, 0);
            pid_ = -1;
        }
    }
    pid_t pid() const { return pid_; }

private:
    pid_t pid_ = -1;
};

double percentile(std::vector<double> v, double p) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    size_t idx = static_cast<size_t>(p / 100.0 * (v.size() - 1));
    return v[idx];
}

struct Client {
    std::unique_ptr<ReplicaService::Stub> stub_a, stub_b;
    std::string addr_a, addr_b;

    Client(const std::string& a, const std::string& b) : addr_a(a), addr_b(b) {
        stub_a = ReplicaService::NewStub(grpc::CreateChannel(a, grpc::InsecureChannelCredentials()));
        stub_b = ReplicaService::NewStub(grpc::CreateChannel(b, grpc::InsecureChannelCredentials()));
    }

    // Tries node A, then node B, matching how a real initiator would fail
    // over between two paths to the same pod. Returns true and fills
    // *op on the first node that accepts the write.
    bool write(uint32_t key, const std::string& value, Op* op, int timeout_ms = 1500) {
        op->key = key;
        op->is_write = true;
        op->value = value;
        op->start_ns = now_ns();
        for (auto* stub : {stub_a.get(), stub_b.get()}) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
            WriteRequest req;
            req.set_key(key);
            req.set_value(value);
            WriteReply reply;
            grpc::Status status = stub->Write(&ctx, req, &reply);
            if (status.ok() && reply.ok()) {
                op->end_ns = now_ns();
                op->success = true;
                return true;
            }
        }
        op->end_ns = now_ns();
        op->success = false;
        return false;
    }

    bool read(uint32_t key, Op* op, int timeout_ms = 1500) {
        op->key = key;
        op->is_write = false;
        op->start_ns = now_ns();
        for (auto* stub : {stub_a.get(), stub_b.get()}) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
            ReadRequest req;
            req.set_key(key);
            ReadReply reply;
            grpc::Status status = stub->Read(&ctx, req, &reply);
            if (status.ok() && reply.ok()) {
                op->end_ns = now_ns();
                op->success = true;
                op->value = reply.value();
                return true;
            }
        }
        op->end_ns = now_ns();
        op->success = false;
        return false;
    }
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 6) {
        std::fprintf(stderr, "usage: %s <num_faults> <writes_per_round> <mediator_port> <portA> <portB> [report.json]\n",
                     argv[0]);
        return 2;
    }
    int num_faults = std::atoi(argv[1]);
    int writes_per_round = std::atoi(argv[2]);
    std::string mediator_addr = std::string("127.0.0.1:") + argv[3];
    std::string addr_a = std::string("127.0.0.1:") + argv[4];
    std::string addr_b = std::string("127.0.0.1:") + argv[5];
    std::string report_path = argc > 6 ? argv[6] : "";

    std::string self = "./build/replica_node_main";
    std::string mediator_bin = "./build/mediator_main";

    Child mediator_proc, node_a, node_b;
    mediator_proc.start({mediator_bin, mediator_addr});
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    node_a.start({self, "A", addr_a, addr_b, mediator_addr});
    node_b.start({self, "B", addr_b, addr_a, mediator_addr});
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // let initial pairing complete

    Client client(addr_a, addr_b);
    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<uint32_t> key_dist(0, 63);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::uniform_int_distribution<int> fault_type_dist(0, 1);
    std::uniform_int_distribution<int> target_dist(0, 1);
    std::uniform_int_distribution<int> hold_ms_dist(200, 1500);

    std::vector<Op> history;
    std::vector<double> failover_ms;
    int write_failures_outside_faults = 0;

    auto random_value = [&] {
        std::string v(16, '\0');
        for (auto& c : v) c = static_cast<char>(byte_dist(rng));
        return v;
    };

    for (int round = 0; round < num_faults; ++round) {
        for (int i = 0; i < writes_per_round; ++i) {
            Op op;
            bool ok = client.write(key_dist(rng), random_value(), &op);
            history.push_back(op);
            if (!ok) write_failures_outside_faults++;
            if (i % 3 == 0) {
                Op rop;
                client.read(key_dist(rng), &rop);
                history.push_back(rop);
            }
        }

        bool do_kill = fault_type_dist(rng) == 1;
        bool target_a = target_dist(rng) == 0;
        Child& target_proc = target_a ? node_a : node_b;
        auto* target_stub = target_a ? client.stub_a.get() : client.stub_b.get();

        auto fault_start = Clock::now();
        if (do_kill) {
            target_proc.kill_now();
            std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms_dist(rng)));
            target_proc.start({self, target_a ? "A" : "B", target_a ? addr_a : addr_b, target_a ? addr_b : addr_a,
                                mediator_addr});
        } else {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
            FaultModeRequest freq;
            freq.set_drop_peer_calls(true);
            FaultModeReply frep;
            target_stub->SetFaultMode(&ctx, freq, &frep);
            std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms_dist(rng)));
            grpc::ClientContext ctx2;
            ctx2.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
            FaultModeRequest freq2;
            freq2.set_drop_peer_calls(false);
            FaultModeReply frep2;
            target_stub->SetFaultMode(&ctx2, freq2, &frep2);
        }

        // Probe with real writes until one succeeds (or an 8s cap elapses),
        // the same way an application would naturally retry.
        bool recovered = false;
        auto deadline = Clock::now() + std::chrono::seconds(8);
        while (Clock::now() < deadline) {
            Op op;
            bool ok = client.write(key_dist(rng), random_value(), &op, 500);
            history.push_back(op);
            if (ok) {
                recovered = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        double elapsed_ms =
            std::chrono::duration<double, std::milli>(Clock::now() - fault_start).count();
        failover_ms.push_back(recovered ? elapsed_ms : 8000.0);

        if ((round + 1) % 50 == 0) {
            std::fprintf(stderr, "round %d/%d done (last failover %.1f ms)\n", round + 1, num_faults, elapsed_ms);
        }
    }

    LinearizabilityChecker checker;
    CheckResult result = checker.check(history);

    double p50 = percentile(failover_ms, 50);
    double p99 = percentile(failover_ms, 99);
    double max_ms = *std::max_element(failover_ms.begin(), failover_ms.end());

    std::fprintf(stderr,
                 "\n=== results ===\nfaults injected: %d\nfailover p50=%.1fms p99=%.1fms max=%.1fms\n"
                 "history ops: %zu, checked_keys=%d skipped_keys=%d total_ops_checked=%d linearizable=%s\n",
                 num_faults, p50, p99, max_ms, history.size(), result.checked_keys, result.skipped_keys,
                 result.total_ops_checked, result.linearizable ? "true" : "false");
    if (!result.linearizable) std::fprintf(stderr, "VIOLATION: %s\n", result.violation_detail.c_str());

    if (!report_path.empty()) {
        std::ofstream out(report_path, std::ios::trunc);
        out << "{\"faults_injected\":" << num_faults << ",\"failover_p50_ms\":" << p50
            << ",\"failover_p99_ms\":" << p99 << ",\"failover_max_ms\":" << max_ms
            << ",\"history_ops\":" << history.size() << ",\"checked_keys\":" << result.checked_keys
            << ",\"skipped_keys\":" << result.skipped_keys << ",\"total_ops_checked\":" << result.total_ops_checked
            << ",\"linearizable\":" << (result.linearizable ? "true" : "false") << "}";
    }

    node_a.stop_gracefully();
    node_b.stop_gracefully();
    mediator_proc.stop_gracefully();
    return result.linearizable ? 0 : 1;
}
