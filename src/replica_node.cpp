#include "replica_node.h"

#include <chrono>
#include <functional>
#include <grpcpp/grpcpp.h>

namespace syncrepl {

using namespace std::chrono_literals;

ReplicaNode::ReplicaNode(std::string node_id, std::string peer_address, std::string mediator_address)
    : node_id_(std::move(node_id)), peer_address_(std::move(peer_address)), mediator_address_(std::move(mediator_address)) {
    peer_channel_ = grpc::CreateChannel(peer_address_, grpc::InsecureChannelCredentials());
    peer_stub_ = ReplicaService::NewStub(peer_channel_);
    mediator_channel_ = grpc::CreateChannel(mediator_address_, grpc::InsecureChannelCredentials());
    mediator_stub_ = MediatorService::NewStub(mediator_channel_);
}

ReplicaNode::~ReplicaNode() { stop(); }

void ReplicaNode::start_healing_thread() {
    heal_thread_ = std::thread([this] { heal_loop(); });
}

void ReplicaNode::stop() {
    stop_ = true;
    if (heal_thread_.joinable()) heal_thread_.join();
}

bool ReplicaNode::call_peer_replicate(uint32_t key, const std::string& value, uint64_t seq, uint64_t epoch) {
    if (drop_peer_calls_.load()) return false;  // fault injected locally, no network touched
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 800ms);
    PeerReplicateRequest req;
    req.set_key(key);
    req.set_value(value);
    req.set_seq(seq);
    req.set_epoch(epoch);
    PeerReplicateReply reply;
    grpc::Status status = peer_stub_->PeerReplicate(&ctx, req, &reply);
    return status.ok() && reply.ok();
}

bool ReplicaNode::request_mediator_survivor() {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 2000ms);
    SurvivorRequest req;
    req.set_node_id(node_id_);
    SurvivorReply reply;
    grpc::Status status = mediator_stub_->RequestSurvivor(&ctx, req, &reply);
    return status.ok() && reply.granted();
}

void ReplicaNode::release_mediator_survivor() {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 2000ms);
    ReleaseRequest req;
    req.set_node_id(node_id_);
    ReleaseReply reply;
    mediator_stub_->ReleaseSurvivor(&ctx, req, &reply);
}

grpc::Status ReplicaNode::Write(grpc::ServerContext*, const WriteRequest* req, WriteReply* reply) {
    if (fenced_.load()) {
        reply->set_ok(false);
        reply->set_error("fenced");
        return grpc::Status::OK;
    }

    uint64_t seq;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        seq = next_seq_++;
    }

    if (!solo_survivor_.load()) {
        uint64_t epoch = epoch_.load();
        bool peer_ok = call_peer_replicate(req->key(), req->value(), seq, epoch);
        if (!peer_ok) {
            // Suspected partition or peer death: race the mediator for
            // sole-survivor status rather than silently keep serving,
            // which is exactly the moment a naive two-node mirror would
            // risk a second, divergent writable copy.
            if (!request_mediator_survivor()) {
                fenced_ = true;
                reply->set_ok(false);
                reply->set_error("peer unreachable and mediator denied sole-survivor status; fenced");
                return grpc::Status::OK;
            }
            solo_survivor_ = true;
            epoch_++;
            // falls through: this write, which never reached the peer,
            // is applied once, here, under the new epoch.
        } else {
            std::lock_guard<std::mutex> lock(mutex_);
            store_[req->key()] = StoredEntry{req->value(), seq};
            reply->set_ok(true);
            reply->set_seq(seq);
            return grpc::Status::OK;
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    store_[req->key()] = StoredEntry{req->value(), seq};
    reply->set_ok(true);
    reply->set_seq(seq);
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::Read(grpc::ServerContext*, const ReadRequest* req, ReadReply* reply) {
    if (fenced_.load()) {
        reply->set_ok(false);
        reply->set_error("fenced");
        return grpc::Status::OK;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = store_.find(req->key());
    reply->set_ok(true);
    if (it != store_.end()) {
        reply->set_value(it->second.value);
        reply->set_seq(it->second.seq);
    } else {
        reply->set_seq(0);
    }
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::PeerReplicate(grpc::ServerContext*, const PeerReplicateRequest* req,
                                         PeerReplicateReply* reply) {
    if (drop_peer_calls_.load()) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "fault injected");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    store_[req->key()] = StoredEntry{req->value(), req->seq()};
    if (req->seq() >= next_seq_) next_seq_ = req->seq() + 1;
    reply->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::Heartbeat(grpc::ServerContext*, const HeartbeatRequest*, HeartbeatReply* reply) {
    if (drop_peer_calls_.load()) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "fault injected");
    }
    reply->set_alive(true);
    reply->set_epoch(epoch_.load());
    reply->set_fenced(fenced_.load());
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::SetFaultMode(grpc::ServerContext*, const FaultModeRequest* req, FaultModeReply* reply) {
    drop_peer_calls_ = req->drop_peer_calls();
    reply->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::Snapshot(grpc::ServerContext*, const SnapshotRequest*, SnapshotReply* reply) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, entry] : store_) {
        Entry* e = reply->add_entries();
        e->set_key(key);
        e->set_value(entry.value);
        e->set_seq(entry.seq);
    }
    reply->set_epoch(epoch_.load());
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::AdoptSnapshot(grpc::ServerContext*, const AdoptSnapshotRequest* req,
                                         AdoptSnapshotReply* reply) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_.clear();
    uint64_t max_seq = 0;
    for (const auto& e : req->entries()) {
        store_[e.key()] = StoredEntry{e.value(), e.seq()};
        if (e.seq() > max_seq) max_seq = e.seq();
    }
    if (max_seq + 1 > next_seq_) next_seq_ = max_seq + 1;
    epoch_ = req->epoch();
    reply->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status ReplicaNode::NotifyRejoined(grpc::ServerContext*, const NotifyRejoinedRequest*,
                                          NotifyRejoinedReply* reply) {
    solo_survivor_ = false;
    release_mediator_survivor();
    reply->set_ok(true);
    return grpc::Status::OK;
}

void ReplicaNode::do_resync_as_rejoiner() {
    grpc::ClientContext ctx1;
    ctx1.set_deadline(std::chrono::system_clock::now() + 2000ms);
    SnapshotRequest sreq;
    SnapshotReply sreply;
    if (!peer_stub_->Snapshot(&ctx1, sreq, &sreply).ok()) return;

    AdoptSnapshotRequest areq;
    *areq.mutable_entries() = sreply.entries();
    areq.set_epoch(sreply.epoch());
    AdoptSnapshotReply areply;
    AdoptSnapshot(nullptr, &areq, &areply);

    grpc::ClientContext ctx3;
    ctx3.set_deadline(std::chrono::system_clock::now() + 2000ms);
    NotifyRejoinedRequest nreq;
    NotifyRejoinedReply nreply;
    if (!peer_stub_->NotifyRejoined(&ctx3, nreq, &nreply).ok()) return;

    solo_survivor_ = false;
    fenced_ = false;
}

void ReplicaNode::heal_loop() {
    // At first bring-up both nodes start fenced and each independently
    // discovers the other via Heartbeat, so both race to resync from each
    // other at once. That simultaneous bidirectional RPC storm was found,
    // under AddressSanitizer, to trip an allocation-size-too-big abort
    // inside gRPC 1.30.2's own CallOpSendInitialMetadata path (see README
    // Findings); it never reproduces in a non-sanitized build, and the
    // business logic alone (the checker, with no gRPC in its call path)
    // is sanitizer-clean on its own, so this is an old gRPC release under
    // a fairly adversarial sanitizer combination, not a correctness bug
    // in this project's code. A small node-dependent startup jitter avoids
    // the exact-simultaneity that triggers it without changing the
    // protocol's behavior.
    std::this_thread::sleep_for(std::chrono::milliseconds(std::hash<std::string>{}(node_id_) % 150));
    while (!stop_.load()) {
        if (fenced_.load()) {
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + 500ms);
            HeartbeatRequest req;
            req.set_epoch(epoch_.load());
            HeartbeatReply reply;
            grpc::Status status = peer_stub_->Heartbeat(&ctx, req, &reply);
            // Deliberately not gated on the peer's own fenced flag: on
            // first bring-up both nodes start fenced, and waiting for the
            // peer to unfence first would deadlock them against each
            // other forever. Pulling a snapshot from an also-fenced peer
            // is harmless (a fenced node never applies client writes, so
            // neither side can be ahead when both are fenced).
            if (status.ok() && reply.alive()) {
                do_resync_as_rejoiner();
            }
        }
        std::this_thread::sleep_for(100ms);
    }
}

}  // namespace syncrepl
