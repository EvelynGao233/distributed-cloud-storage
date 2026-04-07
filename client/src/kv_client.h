// kv_client.h
#pragma once

#include <grpcpp/grpcpp.h>
#include <memory>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "myproto/kvstorage.grpc.pb.h"
#include "myproto/coordinator.grpc.pb.h"

namespace kvstorage {
    class KvStorage;
    class Coordinator;
}

class KvStorageClient {
public:
    explicit KvStorageClient(const std::string &coordinator_addr);

    bool Put(const std::string &row, const std::string &col, const std::string &value);
    bool Get(const std::string &row, const std::string &col, std::string &value);
    bool Cput(const std::string &row, const std::string &col,
              const std::string &v_old, const std::string &v_new);
    bool Delete(const std::string &row, const std::string &col);

    void RefreshRouting();

private:
    struct PartitionRouting {
        std::string start;
        std::string end;
        std::string primary;
        std::vector<std::string> replicas;
    };

    std::unique_ptr<kvstorage::Coordinator::Stub> coord_stub_;

    std::map<std::string, std::unique_ptr<kvstorage::KvStorage::Stub>> kv_stubs_;

    std::vector<PartitionRouting> partitions_;
    std::map<std::string, bool> node_alive_;

    std::mutex mu_;
    void BuildRoutingFromClusterStatus(const kvstorage::ClusterStatusResponse &resp);

    kvstorage::KvStorage::Stub *GetStubFor(const std::string &addr);

    kvstorage::KvStorage::Stub *ChoosePrimaryForRow(const std::string &row);

    kvstorage::KvStorage::Stub *ChooseReplicaForRow(const std::string &row);
};

void RunTests(KvStorageClient &client);
