#include <grpcpp/grpcpp.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <thread>

#include "myproto/kvstorage.grpc.pb.h"
#include "myproto/coordinator.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstorage::Coordinator;
using kvstorage::RegisterRequest;
using kvstorage::RegisterResponse;
using kvstorage::HeartbeatRequest;
using kvstorage::HeartbeatResponse;
using kvstorage::GlobalView;
using kvstorage::PartitionInfo;
using kvstorage::ClusterStatusResponse;
using kvstorage::GetClusterStatusRequest;
using kvstorage::NodeStatus;
using kvstorage::PartitionStatus;

namespace {

struct RangeKey {
    std::string start;
    std::string end;

    bool operator==(const RangeKey& other) const {
        return start == other.start && end == other.end;
    }
};

struct RangeKeyHash {
    std::size_t operator()(const RangeKey& k) const noexcept {
        return std::hash<std::string>()(k.start) ^ (std::hash<std::string>()(k.end) << 1);
    }
};

// Simple trim function reused from other components.
void Trim(std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    s = s.substr(start, end - start);
}

/**
 * GlobalState - Coordinator's in-memory cluster state
 *
 * - Holds the current GlobalView (list of PartitionInfo)
 * - Tracks last heartbeat time per node for failure detection
 */
class GlobalState {
public:
    GlobalState() : global_version_(1) {}

    /**
     * LoadFromCsv - Initialize partition layout from cluster_config.csv
     *
     * CSV format (per line):
     *   start,end,primary,backup1,backup2
     */
    void LoadFromCsv(const std::string& path) {
        std::ifstream fin(path);
        if (!fin.is_open()) {
            throw std::runtime_error("Cannot open cluster config: " + path);
        }

        std::string line;
        while (std::getline(fin, line)) {
            if (line.empty() || line[0] == '#') {
                continue;
            }

            std::istringstream ss(line);
            std::string start, end, primary, backup1, backup2;

            if (!std::getline(ss, start, ',')) continue;
            if (!std::getline(ss, end, ',')) continue;
            if (!std::getline(ss, primary, ',')) continue;
            std::getline(ss, backup1, ',');
            std::getline(ss, backup2, ',');

            Trim(start);
            Trim(end);
            Trim(primary);
            Trim(backup1);
            Trim(backup2);

            PartitionInfo p;
            p.set_view_id(0);  // initial view id
            p.set_start(start);
            p.set_end(end);
            p.set_primary(primary);
            if (!backup1.empty()) {
                p.add_backups(backup1);
            }
            if (!backup2.empty()) {
                p.add_backups(backup2);
            }

            int current_partition_idx = static_cast<int>(partitions_.size());
            partitions_.push_back(p);

            RangeKey key{start, end};
            last_view_id_[key] = 0;

            if (!primary.empty()) {
                static_node_mapping_[primary].push_back(current_partition_idx);
            }
            if (!backup1.empty()) {
                static_node_mapping_[backup1].push_back(current_partition_idx);
            }
            if (!backup2.empty()) {
                static_node_mapping_[backup2].push_back(current_partition_idx);
            }
        }

        std::cout << "[Coordinator] Loaded " << partitions_.size()
                  << " partitions from config" << std::endl;
        for (const auto& p : partitions_) {
            std::cout << "  ["
                      << (p.start().empty() ? "-inf" : p.start())
                      << ", "
                      << (p.end().empty() ? "+inf" : p.end())
                      << ") primary=" << p.primary()
                      << " backups=";
            for (const auto& b : p.backups()) {
                std::cout << b << " ";
            }
            std::cout << std::endl;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            global_version_++;
        }
    }

    /**
     * BuildView - Construct a GlobalView proto from current partitions
     */
    GlobalView BuildViewLocked() const {
        GlobalView view;
        for (const auto& p : partitions_) {
            *view.add_partitions() = p;
        }
        view.set_version(global_version_);
        return view;
    }

    int64_t GetVersion() const {
        std::lock_guard<std::mutex> lock(mu_);
        return global_version_;
    }

    /**
     * GetCurrentView - Thread-safe snapshot of GlobalView
     */
    GlobalView GetCurrentView() const {
        std::lock_guard<std::mutex> lock(mu_);
        return BuildViewLocked();
    }

    ClusterStatusResponse BuildClusterStatus(std::chrono::milliseconds timeout) const {
        ClusterStatusResponse resp;
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mu_);

        for (const auto& entry : last_heartbeat_) {
            auto* node_status = resp.add_nodes();
            node_status->set_address(entry.first);
            const auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(now - entry.second);
            node_status->set_last_heartbeat_ms(delta.count());
            node_status->set_is_alive(delta <= timeout);
        }

        for (const auto& partition : partitions_) {
            auto* partition_status = resp.add_partitions();
            partition_status->set_view_id(partition.view_id());
            partition_status->set_start(partition.start());
            partition_status->set_end(partition.end());
            partition_status->set_primary(partition.primary());
            for (const auto& backup : partition.backups()) {
                partition_status->add_backups(backup);
            }
        }

        return resp;
    }

    void TryRecoverNode(const std::string& addr) {
        std::lock_guard<std::mutex> lock(mu_);
        bool changed = false;
        for (const auto& p : partitions_) {
            if (p.primary() == addr) {
                return;
            }
            for (const auto& b : p.backups()) {
                if (b == addr) {
                    return;
                }
            }
        }

        auto it = static_node_mapping_.find(addr);
        if (it == static_node_mapping_.end()) {
            return;
        }

        for (int p_idx : it->second) {
            if (p_idx < 0 || p_idx >= static_cast<int>(partitions_.size())) {
                continue;
            }
            auto& partition = partitions_[p_idx];

            bool already_in_backups = false;
            for (const auto& existing_backup : partition.backups()) {
                if (existing_backup == addr) {
                    already_in_backups = true;
                    break;
                }
            }
            if (already_in_backups || partition.primary() == addr) {
                continue;
            }

            partition.add_backups(addr);
            RangeKey key{partition.start(), partition.end()};
            partition.set_view_id(++last_view_id_[key]);
            changed = true;

            std::cout << "[Coordinator] REJOIN: Node " << addr
                      << " re-admitted to partition ["
                      << (partition.start().empty() ? "-inf" : partition.start()) << ", "
                      << (partition.end().empty() ? "+inf" : partition.end())
                      << ") as BACKUP. New ViewID: " << partition.view_id() << std::endl;
        }

        if (changed) {
            ++global_version_;
            std::cout << "[Coordinator] GlobalView updated (v" << global_version_
                      << ") due to node rejoin: " << addr << std::endl;
        }
    }

    /**
     * TouchNode - Update last heartbeat time for the given node
     */
    void TouchNode(const std::string& addr) {
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mu_);
        last_heartbeat_[addr] = now;
        node_alive_[addr] = true;
    }

    /**
     * ScanForDeadNodes - Log nodes that have not sent heartbeats
     * within the given timeout.
     *
     * This does not yet modify the GlobalView; it only detects failures.
     */
    void ScanForDeadNodes(std::chrono::milliseconds timeout) {
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mu_);

        for (auto& entry : last_heartbeat_) {
            const auto& addr = entry.first;
            auto last = entry.second;
            auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(now - last);
            if (delta > timeout) {
                std::cerr << "[Coordinator] Node considered dead: " << addr
                          << " (last heartbeat " << delta.count() << " ms ago)" << std::endl;
                node_alive_[addr] = false;
                HandleNodeFailureLocked(addr);

            }
        }
    }

    bool IsNodeAliveLocked(const std::string& addr) const{
        auto it = node_alive_.find(addr);
        if (it == node_alive_.end()){
            return false;
        }
        return it->second;
    }

    /**
     * HandleNodeFailureLocked - Process a detected node failure
     *
     * 1. Marks the node as dead.
     * 2. If the node was a Primary, promotes the first available Backup.
     * 3. Removes the node from all Backup lists.
     * 4. Increments view IDs for affected partitions and updates global version.
     *
     * Must be called with mu_ held.
     */
    void HandleNodeFailureLocked(const std::string& addr){
        bool any_changed = false;

        for (auto& p : partitions_){
            bool updated = false;
            if (p.primary() == addr){
                std::string new_primary;
                int new_primary_backup_index = -1;

                auto* backups = p.mutable_backups();
                for (int i = 0; i < backups->size(); ++i){
                    const std::string& cand = backups->Get(i);
                    if (IsNodeAliveLocked(cand)){
                        new_primary = cand;
                        new_primary_backup_index = i;
                        break;
                    }
                }

                if (!new_primary.empty()){
                    std::cout << "[DEBUG-COORD] Node " << addr << " is dead."
                    << " PROMOTING " << new_primary << " to PRIMARY." << std::endl;
                    std::cout << "[Coordinator] Promoting backup " << new_primary
                            << " to primary for range ["
                            << (p.start().empty() ? "-inf" : p.start()) << ", "
                            << (p.end().empty() ? "+inf" : p.end()) << ")"
                            << std::endl;
                    p.set_primary(new_primary);
                    if (new_primary_backup_index >= 0){
                        backups->DeleteSubrange(new_primary_backup_index, 1);
                    }

                    updated = true;
                } else{
                    std::cerr << "[Coordinator] WARNING: partition ["
                            << (p.start().empty() ? "-inf" : p.start()) << ", "
                            << (p.end().empty() ? "+inf" : p.end()) << ")"
                            << " lost all replicas (no alive backup for failed primary "
                            << addr << ")"
                            << std::endl;
                }
            }

            {
                auto* backups = p.mutable_backups();
                for (int i = 0; i < backups->size();){
                    if (backups->Get(i) == addr){
                        backups->DeleteSubrange(i, 1);
                        updated = true;
                    } else{
                        ++i;
                    }
                }
            }

            if (updated){
                RangeKey key{p.start(), p.end()};
                int64_t new_view_id = ++last_view_id_[key];
                p.set_view_id(new_view_id);
                any_changed = true;
            }
        }

        if (any_changed){
            ++global_version_;
            std::cout << "[Coordinator] GlobalView updated (v" << global_version_
                    << ") due to failure of node "
                    << addr << std::endl;
        }
    }


private:
    mutable std::mutex mu_;
    std::vector<PartitionInfo> partitions_;
    std::unordered_map<RangeKey, int64_t, RangeKeyHash> last_view_id_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_heartbeat_;
    std::unordered_map<std::string, bool> node_alive_;
    std::unordered_map<std::string, std::vector<int>> static_node_mapping_;
    int64_t global_version_;
};

} // namespace

/**
 * CoordinatorServiceImpl - gRPC service implementation for Coordinator
 *
 * Handles Register and Heartbeat from KV servers and returns GlobalView.
 */
class CoordinatorServiceImpl final : public Coordinator::Service {
public:
    explicit CoordinatorServiceImpl(GlobalState& state)
        : state_(state) {}

    Status Register(ServerContext* /*context*/, const RegisterRequest* req,
                    RegisterResponse* resp) override {
        const std::string addr = req->node().address();
        std::cout << "[Coordinator] Register from " << addr << std::endl;
        state_.TouchNode(addr);
        state_.TryRecoverNode(addr);
        resp->set_success(true);
        return Status::OK;
    }

    Status Heartbeat(ServerContext* /*context*/, const HeartbeatRequest* req,
                     HeartbeatResponse* resp) override {
        const std::string addr = req->node().address();
        state_.TouchNode(addr);
        state_.TryRecoverNode(addr);

        int64_t current_ver = state_.GetVersion();
        int64_t client_ver = req->known_view_version();
        if (client_ver < current_ver) {
            // Client's view is outdated; send new view
            GlobalView view = state_.GetCurrentView();
            *resp->mutable_view() = view;
            resp->set_has_new_view(true);
        } else {
            // Client's view is up-to-date
            resp->set_has_new_view(false);
        }
        return Status::OK;
    }

    Status GetClusterStatus(ServerContext* /*context*/, const GetClusterStatusRequest* /*req*/,
                            ClusterStatusResponse* resp) override {
        const auto timeout = std::chrono::milliseconds(500);
        *resp = state_.BuildClusterStatus(timeout);
        return Status::OK;
    }

private:
    GlobalState& state_;
};

/**
 * RunCoordinator - Entry point for coordinator binary
 */
void RunCoordinator(const std::string& cfg_path, const std::string& listen_addr) {
    GlobalState state;
    state.LoadFromCsv(cfg_path);

    CoordinatorServiceImpl service(state);

    ServerBuilder builder;
    builder.AddListeningPort(listen_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server = builder.BuildAndStart();

    std::cout << "==================================================" << std::endl;
    std::cout << "Coordinator listening on " << listen_addr << std::endl;
    std::cout << "Using cluster config: " << cfg_path << std::endl;
    std::cout << "==================================================" << std::endl;

    // Background failure detector thread
    std::thread detector([&state]() {
        const auto interval = std::chrono::milliseconds(300);
        const auto timeout = std::chrono::milliseconds(500);
        while (true) {
            std::this_thread::sleep_for(interval);
            state.ScanForDeadNodes(timeout);
        }
    });
    detector.detach();

    server->Wait();
}

int main(int argc, char** argv) {
    std::string cfg_path = "cluster_config.csv";
    std::string listen_addr = "0.0.0.0:5051";  // default coordinator port

    if (argc > 1) {
        cfg_path = argv[1];
    }
    if (argc > 2) {
        listen_addr = argv[2];
    }

    try {
        RunCoordinator(cfg_path, listen_addr);
    } catch (const std::exception& e) {
        std::cerr << "Coordinator error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
