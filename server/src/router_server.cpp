/*
 * It's already deprecated. Not part of our design any more.
 */
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "myproto/kvstorage.grpc.pb.h"
#include "myproto/coordinator.grpc.pb.h"


using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using kvstorage::KvStorage;
using kvstorage::PutRequest;
using kvstorage::PutResponse;
using kvstorage::GetRequest;
using kvstorage::GetResponse;
using kvstorage::CputRequest;
using kvstorage::CputResponse;
using kvstorage::DeleteRequest;
using kvstorage::DeleteResponse;
using kvstorage::Coordinator;
using kvstorage::GlobalView;
using kvstorage::PartitionInfo;
using kvstorage::HeartbeatRequest;
using kvstorage::HeartbeatResponse;

const int MAX_MESSAGE_SIZE = 20 * 1024 * 1024;

struct ShardRange {
    std::string start;
    std::string end;
    std::string primary_addr;  // primary server address
};

bool InRange(const std::string& key, const ShardRange& range) {
    bool ge_start = range.start.empty() || key >= range.start;
    bool lt_end   = range.end.empty()   || key <  range.end;
    return ge_start && lt_end;
}

void Trim(std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        start++;
    }
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        end--;
    }
    s = s.substr(start, end - start);
}

std::vector<ShardRange> LoadConfig(const std::string& path) {
    std::vector<ShardRange> ranges;
    std::ifstream fin(path);
    if (!fin.is_open()) {
        throw std::runtime_error("Cannot open shard config: " + path);
    }
    std::string line;
    while (std::getline(fin, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::istringstream ss(line);
        std::string start;
        std::string end;
        std::string primary;
        std::string backup1;
        std::string backup2;

        if (!std::getline(ss, start, ',')) {
            continue;
        }
        if (!std::getline(ss, end, ',')) {
            continue;
        }
        if (!std::getline(ss, primary, ',')) {
            continue;
        }
        // Optional backups (ignored by router but consumed to match CSV format)
        std::getline(ss, backup1, ',');
        std::getline(ss, backup2, ',');

        Trim(start);
        Trim(end);
        Trim(primary);

        ranges.push_back({start, end, primary});
    }

    return ranges;
}

class RouterService final : public KvStorage::Service {
public:
    explicit RouterService(std::vector<ShardRange> shards,
                           const std::string& router_addr,
                           const std::string& coordinator_addr)
        : shards_(std::move(shards)), router_addr_(router_addr) 
    {

        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(MAX_MESSAGE_SIZE);
        args.SetMaxSendMessageSize(MAX_MESSAGE_SIZE);

        for (const auto& s : shards_) {
            if (!stubs_.count(s.primary_addr)) {
                auto channel = grpc::CreateCustomChannel(
                    s.primary_addr,
                    grpc::InsecureChannelCredentials(),
                    args
                );
                stubs_[s.primary_addr] = KvStorage::NewStub(channel);
            }
        }

        std::cout << "[Router] Loaded " << shards_.size() << " shard ranges..." << std::endl;
        
        for (const auto& s : shards_) {
            std::cout << "  ["
                      << (s.start.empty() ? "-inf" : s.start)
                      << ", "
                      << (s.end.empty() ? "+inf" : s.end)
                      << ") -> "
                      << s.primary_addr
                      << std::endl;
        }
        if (!coordinator_addr.empty()){
            auto channel = grpc::CreateChannel(coordinator_addr, grpc::InsecureChannelCredentials());
            coordinator_stub_ = Coordinator::NewStub(channel);
            std::thread([this]() { this->HeartbeatLoop(); }).detach();
        }
    }

private:
    KvStorage::Stub* ChooseByRow(const std::string& row) {
        for (const auto& s : shards_) {
            if (InRange(row, s)) {
                auto it = stubs_.find(s.primary_addr);
                if (it != stubs_.end()){
                    return it->second.get();
                }
            }
        }

        return nullptr;
    }

    void UpdateFromGlobalView(const GlobalView& view){
        std::lock_guard<std::mutex> lock(mu_);
        shards_.clear();

        for (const auto& p : view.partitions()){
            ShardRange r;
            r.start = p.start();
            r.end = p.end();
            r.primary_addr = p.primary();

            shards_.push_back(r);
            if (!stubs_.count(r.primary_addr)){
                stubs_[r.primary_addr] = KvStorage::NewStub(
                    grpc::CreateChannel(r.primary_addr, grpc::InsecureChannelCredentials())
                );
            }
        }

        // std::cout << "[Router] Updated shard mapping from coordinator view:" << std::endl;
        // for (const auto& s : shards_){
        //     std::cout << "  ["
        //             << (s.start.empty() ? "-inf" : s.start)
        //             << ", "
        //             << (s.end.empty() ? "+inf" : s.end)
        //             << ") -> "
        //             << s.primary_addr
        //             << std::endl;
        // }
    }

    void HeartbeatLoop(){
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            if (!coordinator_stub_) continue;

            HeartbeatRequest req;
            req.mutable_node()->set_address(router_addr_);
            HeartbeatResponse resp;
            grpc::ClientContext ctx;
            Status s = coordinator_stub_->Heartbeat(&ctx, req, &resp);
            if (!s.ok()) {
                std::cerr << "[Router] Heartbeat to coordinator failed: "
                        << s.error_message() << std::endl;
                continue;
            }
            if (resp.has_new_view()) {
                UpdateFromGlobalView(resp.view());
            }
        }
    }




    Status Put(ServerContext*, const PutRequest* req, PutResponse* resp) override {
        auto* stub = ChooseByRow(req->r());

        if (stub == nullptr) {
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "No shard found");
        }
        ClientContext ctx;

        return stub->Put(&ctx, *req, resp);
    }
    Status Get(ServerContext*, const GetRequest* req, GetResponse* resp) override {
        auto* stub = ChooseByRow(req->r());

        if (stub == nullptr) {
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "No shard found");
        }
        ClientContext ctx;

        return stub->Get(&ctx, *req, resp);
    }
    Status Cput(ServerContext*, const CputRequest* req, CputResponse* resp) override {
        auto* stub = ChooseByRow(req->r());

        if (stub == nullptr) {
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "No shard found");
        }
        ClientContext ctx;

        return stub->Cput(&ctx, *req, resp);
    }
    Status Delete(ServerContext*, const DeleteRequest* req, DeleteResponse* resp) override {
        auto* stub = ChooseByRow(req->r());

        if (stub == nullptr) {
            return Status(grpc::StatusCode::FAILED_PRECONDITION, "No shard found");
        }

        ClientContext ctx;
        return stub->Delete(&ctx, *req, resp);
    }

private:
    std::vector<ShardRange> shards_;
    std::unordered_map<std::string, std::unique_ptr<KvStorage::Stub>> stubs_;
    std::unique_ptr<Coordinator::Stub> coordinator_stub_; 
    std::string router_addr_;                             
    std::mutex mu_;     
};

int main(int argc, char** argv) {
    std::string cfg_file = "cluster_config.csv";
    std::string router_addr = "0.0.0.0:2600";
    std::string coordinator_addr = "localhost:6000"; 

    if (argc >= 2) {
        cfg_file = argv[1];
    }

    if (argc >= 3) {
        router_addr = argv[2];
    }
    if (argc >= 4){
        coordinator_addr = argv[3];
    }

    try {
        auto shards = LoadConfig(cfg_file);

        if (shards.empty()) {
            std::cerr << "No shard ranges configured." << std::endl;
            return 1;
        }

        RouterService service(std::move(shards), router_addr, coordinator_addr);

        grpc::ServerBuilder builder;
        builder.SetMaxReceiveMessageSize(MAX_MESSAGE_SIZE);
        builder.SetMaxSendMessageSize(MAX_MESSAGE_SIZE);

        builder.AddListeningPort(router_addr, grpc::InsecureServerCredentials());

        builder.RegisterService(&service);

        std::unique_ptr<Server> server = builder.BuildAndStart();


        std::cout << "==================================================" << std::endl;
        std::cout << "KV Router listening on " << router_addr
              << " (max message size " << MAX_MESSAGE_SIZE << " bytes)" << std::endl;
        std::cout << "==================================================" << std::endl;
        std::cout << "Forwarding Put/Get/Cput/Delete by row-key range" << std::endl;
        std::cout << "Press Ctrl+C to stop" << std::endl;
        std::cout << "==================================================" << std::endl;

        server->Wait();
    } 

    catch (const std::exception& e) {
        std::cerr << "Router error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
