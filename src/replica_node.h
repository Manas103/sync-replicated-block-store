#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "mediator.grpc.pb.h"
#include "replica.grpc.pb.h"

namespace syncrepl {

struct StoredEntry {
    std::string value;
    uint64_t seq = 0;
};

// One array node of a two-node, ActiveCluster-style pod. Both nodes are
// symmetric: either can serve a client write, which is replicated to the
// peer synchronously (zero RPO: a write is never acknowledged unless it is
// durable on both nodes, or the peer has been confirmed unreachable and an
// external mediator has granted this node sole-survivor status so there is
// only ever one writable copy at a time).
class ReplicaNode final : public ReplicaService::Service {
public:
    ReplicaNode(std::string node_id, std::string peer_address, std::string mediator_address);
    ~ReplicaNode() override;

    grpc::Status Write(grpc::ServerContext*, const WriteRequest*, WriteReply*) override;
    grpc::Status Read(grpc::ServerContext*, const ReadRequest*, ReadReply*) override;
    grpc::Status PeerReplicate(grpc::ServerContext*, const PeerReplicateRequest*, PeerReplicateReply*) override;
    grpc::Status Heartbeat(grpc::ServerContext*, const HeartbeatRequest*, HeartbeatReply*) override;
    grpc::Status SetFaultMode(grpc::ServerContext*, const FaultModeRequest*, FaultModeReply*) override;
    grpc::Status Snapshot(grpc::ServerContext*, const SnapshotRequest*, SnapshotReply*) override;
    grpc::Status AdoptSnapshot(grpc::ServerContext*, const AdoptSnapshotRequest*, AdoptSnapshotReply*) override;
    grpc::Status NotifyRejoined(grpc::ServerContext*, const NotifyRejoinedRequest*, NotifyRejoinedReply*) override;

    // Starts the background thread that notices a reachable peer again
    // (after this node was fenced, or on first bring-up) and drives the
    // resync handshake. Must be called once the gRPC server is up so the
    // peer can answer Heartbeat/Snapshot calls back.
    void start_healing_thread();
    void stop();

    bool is_fenced() const { return fenced_.load(); }
    bool is_solo_survivor() const { return solo_survivor_.load(); }
    const std::string& node_id() const { return node_id_; }

private:
    bool call_peer_replicate(uint32_t key, const std::string& value, uint64_t seq, uint64_t epoch);
    bool request_mediator_survivor();
    void release_mediator_survivor();
    void heal_loop();
    void do_resync_as_rejoiner();

    std::string node_id_;
    std::string peer_address_;
    std::string mediator_address_;

    std::shared_ptr<grpc::Channel> peer_channel_;
    std::unique_ptr<ReplicaService::Stub> peer_stub_;
    std::shared_ptr<grpc::Channel> mediator_channel_;
    std::unique_ptr<MediatorService::Stub> mediator_stub_;

    mutable std::mutex mutex_;
    std::unordered_map<uint32_t, StoredEntry> store_;
    uint64_t next_seq_ = 1;

    std::atomic<bool> fenced_{true};  // cannot serve client writes until paired/resynced
    std::atomic<bool> solo_survivor_{false};
    std::atomic<uint64_t> epoch_{0};
    std::atomic<bool> drop_peer_calls_{false};  // test-only fault injection, see SetFaultMode
    std::atomic<bool> stop_{false};
    std::thread heal_thread_;
};

}  // namespace syncrepl
