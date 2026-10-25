#include "mediator_service.h"

namespace syncrepl {

grpc::Status MediatorServiceImpl::RequestSurvivor(grpc::ServerContext*, const SurvivorRequest* req,
                                                   SurvivorReply* reply) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_holder_.empty() || current_holder_ == req->node_id()) {
        current_holder_ = req->node_id();
        reply->set_granted(true);
    } else {
        reply->set_granted(false);
    }
    reply->set_current_holder(current_holder_);
    return grpc::Status::OK;
}

grpc::Status MediatorServiceImpl::ReleaseSurvivor(grpc::ServerContext*, const ReleaseRequest* req,
                                                   ReleaseReply* reply) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_holder_ == req->node_id()) current_holder_.clear();
    reply->set_ok(true);
    return grpc::Status::OK;
}

grpc::Status MediatorServiceImpl::Status(grpc::ServerContext*, const StatusRequest*, StatusReply* reply) {
    std::lock_guard<std::mutex> lock(mutex_);
    reply->set_current_holder(current_holder_);
    return grpc::Status::OK;
}

}  // namespace syncrepl
