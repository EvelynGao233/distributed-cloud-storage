#include "kv_client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <sstream>


using grpc::Channel;
using grpc::ClientContext;
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
using kvstorage::GetClusterStatusRequest;
using kvstorage::ClusterStatusResponse;
using kvstorage::NodeStatus;
using kvstorage::PartitionStatus;

static std::string VecToStr(const std::vector<std::string> &v)
{
    std::ostringstream oss;
    for (size_t i = 0; i < v.size(); i++) {
        if (i) oss << ", ";
        oss << v[i];
    }
    return oss.str();
}

namespace {
const int kMaxMessageSize = 25 * 1024 * 1024;

bool RowInRange(const std::string &row,
                const std::string &start,
                const std::string &end) {
    if (!start.empty() && row < start) return false;
    if (!end.empty() && row >= end)   return false;
    return true;
}
} // namespace


KvStorageClient::KvStorageClient(const std::string &coordinator_addr) {
    auto coord_channel = grpc::CreateChannel(
        coordinator_addr,
        grpc::InsecureChannelCredentials()
    );
    coord_stub_ = Coordinator::NewStub(coord_channel);

    std::cout << "[KVClient] Connecting to Coordinator at "
              << coordinator_addr << std::endl;

    RefreshRouting();
}


void KvStorageClient::BuildRoutingFromClusterStatus(const ClusterStatusResponse &resp) {
    std::lock_guard<std::mutex> lock(mu_);

    partitions_.clear();
    node_alive_.clear();

    for (const auto &node : resp.nodes()) {
        node_alive_[node.address()] = node.is_alive();
    }

    for (const auto &p : resp.partitions()) {
        PartitionRouting pr;
        pr.start   = p.start();
        pr.end     = p.end();
        pr.primary = p.primary();

        // replicas: primary + backups
        pr.replicas.push_back(p.primary());
        for (int i = 0; i < p.backups_size(); ++i) {
            pr.replicas.push_back(p.backups(i));
        }

        partitions_.push_back(std::move(pr));
    }

    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(kMaxMessageSize);
    args.SetMaxSendMessageSize(kMaxMessageSize);

    for (const auto &pr : partitions_) {
        for (const auto &addr : pr.replicas) {
            if (kv_stubs_.find(addr) == kv_stubs_.end()) {
                auto ch = grpc::CreateCustomChannel(
                    addr,
                    grpc::InsecureChannelCredentials(),
                    args
                );
                kv_stubs_[addr] = KvStorage::NewStub(ch);
                std::cout << "[KVClient] Prepared stub to KV node " << addr << std::endl;
            }
        }
    }

    std::cout << "[KVClient] Routing table built: "
              << partitions_.size() << " partitions" << std::endl;
}

void KvStorageClient::RefreshRouting() {
    ClusterStatusResponse resp;
    ClientContext ctx;
    GetClusterStatusRequest req;

    Status s = coord_stub_->GetClusterStatus(&ctx, req, &resp);
    if (!s.ok()) {
        std::cerr << "[KVClient] GetClusterStatus RPC failed: "
                  << s.error_message() << std::endl;
        return;
    }

    BuildRoutingFromClusterStatus(resp);
}


KvStorage::Stub *KvStorageClient::GetStubFor(const std::string &addr) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = kv_stubs_.find(addr);
    if (it == kv_stubs_.end()) {
        return nullptr;
    }
    return it->second.get();
}

KvStorage::Stub *KvStorageClient::ChoosePrimaryForRow(const std::string &row) {
    std::lock_guard<std::mutex> lock(mu_);

    for (const auto &pr : partitions_) {
        if (RowInRange(row, pr.start, pr.end)) {
            auto it = kv_stubs_.find(pr.primary);
            if (it != kv_stubs_.end()) {
                return it->second.get();
            }
        }
    }
    return nullptr;
}

KvStorage::Stub *KvStorageClient::ChooseReplicaForRow(const std::string &row)
{
    std::lock_guard<std::mutex> lock(mu_);

    std::cerr << "[KVClient][ChooseReplica] row = " << row << std::endl;

    for (const auto &pr : partitions_) {
        if (!RowInRange(row, pr.start, pr.end)) {
            continue;
        }
        std::cerr << "[KVClient][ChooseReplica] Matched partition: "
                  << "[" << (pr.start.empty() ? "-inf" : pr.start)
                  << ", " << (pr.end.empty() ? "+inf" : pr.end)
                  << ")" << std::endl;

        std::cerr << "[KVClient][ChooseReplica] Primary = "
                  << pr.primary << std::endl;

        std::cerr << "[KVClient][ChooseReplica] Replicas = "
                  << VecToStr(pr.replicas) << std::endl;

        for (const auto &addr : pr.replicas) {
            bool alive = true;
            auto alive_it = node_alive_.find(addr);
            if (alive_it != node_alive_.end()) {
                alive = alive_it->second;
            }

            std::cerr << "[KVClient][ChooseReplica]   Check replica "
                      << addr << " alive=" << alive << std::endl;

            if (!alive) continue;

            auto stub_it = kv_stubs_.find(addr);
            if (stub_it != kv_stubs_.end()) {
                std::cerr << "[KVClient][ChooseReplica] --> CHOSEN (alive) "
                          << addr << std::endl;
                return stub_it->second.get();
            }
        }

        for (const auto &addr : pr.replicas) {
            auto stub_it = kv_stubs_.find(addr);
            if (stub_it != kv_stubs_.end()) {
                std::cerr << "[KVClient][ChooseReplica] --> FALLBACK CHOSEN "
                          << addr << std::endl;
                return stub_it->second.get();
            }
        }

        std::cerr << "[KVClient][ChooseReplica] No stub found for replicas"
                  << std::endl;
    }

    std::cerr << "[KVClient][ChooseReplica] No partition matched for row "
              << row << std::endl;

    return nullptr;
}


bool KvStorageClient::Put(const std::string &row,
                          const std::string &col,
                          const std::string &value) {
    const int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; retry++) {
        KvStorage::Stub *stub = ChoosePrimaryForRow(row);
        if (!stub) {
            std::cerr << "[KVClient] Put: no primary found for row " << row
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
            continue;
        }

        PutRequest request;
        request.set_r(row);
        request.set_c(col);
        request.set_v(value);

        PutResponse response;
        ClientContext context;

        Status status = stub->Put(&context, request, &response);
        if (status.ok()) {
            if (response.success()) {
                return true;  // Success
            } else {
                // logic failure (e.g sent to backup instead of primary)
                std::cerr << "[KVClient] Put returned success=false for row " << row
                          << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
                RefreshRouting();
                if (retry < MAX_RETRIES - 1) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
                }
            }
        } else {
            std::cerr << "[KVClient] Put RPC failed: " << status.error_message()
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            // Refresh routing on RPC failure
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
        }
    }

    std::cerr << "[KVClient] Put: giving up after " << MAX_RETRIES << " attempts" << std::endl;
    return false;
}

bool KvStorageClient::Get(const std::string &row,
                          const std::string &col,
                          std::string &value) {
    const int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; retry++) {
        KvStorage::Stub *stub = ChooseReplicaForRow(row);
        if (!stub) {
            std::cerr << "[KVClient] Get: no replica found for row " << row
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
            continue;
        }

        GetRequest request;
        request.set_r(row);
        request.set_c(col);

        GetResponse response;
        ClientContext context;

        Status status = stub->Get(&context, request, &response);
        if (status.ok()) {
            if (response.found()) {
                value = response.v();
                return true;
            }
            return false;  // Key not found
        } else {
            std::cerr << "[KVClient] Get RPC failed: " << status.error_message()
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            // Refresh routing on RPC failure
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
        }
    }

    std::cerr << "[KVClient] Get: giving up after " << MAX_RETRIES << " attempts" << std::endl;
    return false;
}

bool KvStorageClient::Cput(const std::string &row,
                           const std::string &col,
                           const std::string &v_old,
                           const std::string &v_new) {
    const int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; retry++) {
        KvStorage::Stub *stub = ChoosePrimaryForRow(row);
        if (!stub) {
            std::cerr << "[KVClient] Cput: no primary found for row " << row
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
            continue;
        }

        CputRequest request;
        request.set_r(row);
        request.set_c(col);
        request.set_v_old(v_old);
        request.set_v_new(v_new);

        CputResponse response;
        ClientContext context;

        Status status = stub->Cput(&context, request, &response);
        if (status.ok()) {
            if (response.success()) {
                return true;  // Success
            } else {
                // logic failure (e.g sent to backup instead of primary)
                std::cerr << "[KVClient] Cput returned success=false for row " << row
                          << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
                RefreshRouting();
                if (retry < MAX_RETRIES - 1) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
                }
            }
        } else {
            std::cerr << "[KVClient] Cput RPC failed: " << status.error_message()
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            // Refresh routing on RPC failure
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
        }
    }

    std::cerr << "[KVClient] Cput: giving up after " << MAX_RETRIES << " attempts" << std::endl;
    return false;
}

bool KvStorageClient::Delete(const std::string &row,
                             const std::string &col) {
    const int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; retry++) {
        KvStorage::Stub *stub = ChoosePrimaryForRow(row);
        if (!stub) {
            std::cerr << "[KVClient] Delete: no primary found for row " << row
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
            continue;
        }

        DeleteRequest request;
        request.set_r(row);
        request.set_c(col);

        DeleteResponse response;
        ClientContext context;

        Status status = stub->Delete(&context, request, &response);
        if (status.ok()) {
            if (response.success()) {
                return true;  // Success
            } else {
                // Business logic failure (e.g., sent to backup instead of primary)
                std::cerr << "[KVClient] Delete returned success=false for row " << row
                          << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
                RefreshRouting();
                if (retry < MAX_RETRIES - 1) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
                }
            }
        } else {
            std::cerr << "[KVClient] Delete RPC failed: " << status.error_message()
                      << " (attempt " << (retry + 1) << "/" << MAX_RETRIES << ")" << std::endl;
            // Refresh routing on RPC failure
            RefreshRouting();
            if (retry < MAX_RETRIES - 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (retry + 1)));
            }
        }
    }

    std::cerr << "[KVClient] Delete: giving up after " << MAX_RETRIES << " attempts" << std::endl;
    return false;
}