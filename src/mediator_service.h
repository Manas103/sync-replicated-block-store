#pragma once

#include <mutex>
#include <string>

#include "mediator.grpc.pb.h"

namespace syncrepl {

// The independent arbiter. Deliberately the simplest state machine that
// prevents split brain: at most one node_id holds sole-survivor status at
// any time, first request wins, and a re-request from the current holder
// is idempotent (handles two concurrent Write calls on the same node both
// discovering the peer is gone and both asking).
class MediatorServiceImpl final : public MediatorService::Service {
public:
    grpc::Status RequestSurvivor(grpc::ServerContext*, const SurvivorRequest*, SurvivorReply*) override;
    grpc::Status ReleaseSurvivor(grpc::ServerContext*, const ReleaseRequest*, ReleaseReply*) override;
    grpc::Status Status(grpc::ServerContext*, const StatusRequest*, StatusReply*) override;

private:
    std::mutex mutex_;
    std::string current_holder_;
};

}  // namespace syncrepl
