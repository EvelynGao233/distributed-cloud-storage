#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <future>
#include <atomic>
#include <thread>
#include <chrono>

#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "myproto/kvstorage.grpc.pb.h"
#include "myproto/coordinator.grpc.pb.h"
#include "tablet.h"

#include "wal.h"
#include "checkpoint.h"
#include "recovery.cpp"

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
using kvstorage::LogEntry;
using kvstorage::Ack;
using kvstorage::Coordinator;
using kvstorage::PartitionInfo;
using kvstorage::GlobalView;
using kvstorage::RegisterRequest;
using kvstorage::RegisterResponse;
using kvstorage::HeartbeatRequest;
using kvstorage::HeartbeatResponse;
using kvstorage::DumpTabletRequest;
using kvstorage::DumpTabletResponse;
using kvstorage::RawDataRequest;
using kvstorage::RawDataResponse;
using kvstorage::ControlRequest;
using kvstorage::ControlResponse;
using kvstorage::FetchSnapshotRequest;
using kvstorage::FetchSnapshotResponse;
using kvstorage::FetchWALRequest;
using kvstorage::FetchWALResponse;

struct ShardReplicaInfo {
    std::string start;
    std::string end;
    bool is_primary = false;
    bool is_backup = false;
    std::string primary_addr;
    std::vector<std::string> backups;  // only valid when is_primary == true
};

static void Trim(std::string& s) {
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
 * KvStorageImpl - gRPC service implementation for KV Storage
 *
 * This class uses the thread-safe Tablet data structure to handle
 * concurrent requests from gRPC's built-in thread pool.
 *
 * Thread Safety:
 * - All thread safety is handled by the Tablet class
 * - No additional locking is needed at this layer
 * - gRPC automatically manages the thread pool
 */
class KvStorageImpl final : public KvStorage::Service {
    // Thread-safe Tablet data structure
    Tablet tablet;

    // Shard/replication metadata for this server instance (updated via heartbeats)
    std::vector<ShardReplicaInfo> shard_infos_;
    std::unordered_map<std::string, std::unique_ptr<KvStorage::Stub>> replica_stubs_;
    std::atomic<int64_t> current_term_{0};
    int64_t current_view_version_ = 0;

    // Coordinator client
    std::unique_ptr<Coordinator::Stub> coordinator_stub_;
    std::string my_address_;
    mutable std::mutex view_mu_;
    std::atomic<bool> is_running_{true};
    std::atomic<bool> heartbeat_running_{true};

    CheckpointManager checkpoint_mgr_;
    WriteAheadLog wal_;
    std::mutex checkpoint_protection_mu_;

public:

    Tablet& GetTablet() { return tablet; }
    CheckpointManager& GetCheckpointMgr() { return checkpoint_mgr_; }
    WriteAheadLog& GetWal() { return wal_;}
    /**
     * Constructor - Initializes the KvStorageImpl instance
     *
     * @param my_address The address of this server instance
     * @param coordinator_addr The address of the coordinator server
     */

    KvStorageImpl(const std::string& my_address,
                  const std::string& coordinator_addr)
        : my_address_(my_address),
    checkpoint_mgr_(GetSafeFileName(my_address, "checkpoint", ".bin")),
    wal_(GetSafeFileName(my_address, "wal", ".log")){
        auto channel = grpc::CreateChannel(coordinator_addr, grpc::InsecureChannelCredentials());
        coordinator_stub_ = Coordinator::NewStub(channel);

        recovery_manager_ = std::make_unique<RecoveryManager>(
        tablet, checkpoint_mgr_, wal_, checkpoint_protection_mu_);
        auto start_local = std::chrono::steady_clock::now();
        recovery_manager_->LocalRecover();
        auto end_local = std::chrono::steady_clock::now();
        std::cout << "[DEBUG-TIME] LocalRecover took "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end_local - start_local).count()
                  << " ms" << std::endl;
        int64_t snapshot_lsn = checkpoint_mgr_.GetCheckpointLSN();
        wal_.SetBaseLSN(snapshot_lsn);
    }

    void Start() {
        // Initial registration
        RegisterWithCoordinator();
        std::cout << "[DEBUG-TIME] Registered with Coordinator." << std::endl;

        // Start heartbeat thread
        std::thread([this]() { this->HeartbeatLoop(); }).detach();
        // Start periodic checkpoint thread
        std::thread([this]() { this->CheckpointLoop(); }).detach();
    }


private:
    std::atomic<bool> remote_recovered_{false};
    std::unique_ptr<RecoveryManager> recovery_manager_;

    std::string GetSafeFileName(const std::string& addr, const std::string& prefix, const std::string& suffix) {
        std::string safe_addr = addr;
        size_t colon = safe_addr.find(':');
        if (colon != std::string::npos) {
            safe_addr = safe_addr.substr(colon + 1);
        }
        return prefix + "-" + safe_addr + suffix;
    }

    Status CheckRunning() const {
        if (!is_running_) {
            std::cout << "[WARN] Request rejected by CheckRunning() (is_running_ == false)" << std::endl;
            return Status(grpc::StatusCode::UNAVAILABLE, "Server is simulated DOWN");
        }
        return Status::OK;
    }

    void RegisterWithCoordinator() {
        RegisterRequest req;
        req.mutable_node()->set_address(my_address_);
        RegisterResponse resp;
        grpc::ClientContext ctx;
        Status s = coordinator_stub_->Register(&ctx, req, &resp);
        if (!s.ok() || !resp.success()) {
            std::cerr << "[KV] Failed to register with coordinator at startup: "
                      << s.error_message() << std::endl;
        } else {
            std::cout << "[KV] Registered with coordinator as " << my_address_ << std::endl;
        }
    }

    void HeartbeatLoop() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!heartbeat_running_) {
                continue;
            }

            HeartbeatRequest req;
            req.mutable_node()->set_address(my_address_);
            {
                std::lock_guard<std::mutex> lock(view_mu_);
                req.set_known_view_version(current_view_version_);
            }
            HeartbeatResponse resp;
            grpc::ClientContext ctx;
            Status s = coordinator_stub_->Heartbeat(&ctx, req, &resp);
            if (!s.ok()) {
                std::cerr << "[KV] Heartbeat to coordinator failed: "
                          << s.error_message() << std::endl;
                continue;
            }

            if (resp.has_new_view()) {
                UpdateViewFromGlobal(resp.view());
                // MaybeDoRemoteRecovery();
            }
            if (!remote_recovered_) {
                std::cout << "[DEBUG-TIME] Starting MaybeDoRemoteRecovery..." << std::endl;
                auto start = std::chrono::steady_clock::now();
                MaybeDoRemoteRecovery();
                auto end = std::chrono::steady_clock::now();
                std::cout << "[DEBUG-TIME] MaybeDoRemoteRecovery took "
                          << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
                          << " ms" << std::endl;
            }
        }
    }

    void UpdateViewFromGlobal(const GlobalView& view) {
        std::lock_guard<std::mutex> lock(view_mu_);
        current_view_version_ = view.version();

        shard_infos_.clear();
        replica_stubs_.clear();

        for (const auto& p : view.partitions()) {
            ShardReplicaInfo info;
            info.start = p.start();
            info.end = p.end();
            info.primary_addr = p.primary();

            if (p.primary() == my_address_) {
                info.is_primary = true;
                for (const auto& b : p.backups()) {
                    info.backups.push_back(b);
                }
            } else {
                for (const auto& b : p.backups()) {
                    if (b == my_address_) {
                        info.is_backup = true;
                        break;
                    }
                }
            }

            if (info.is_primary || info.is_backup) {
                shard_infos_.push_back(info);
            }
        }

        // Rebuild stubs for backups
        for (const auto& info : shard_infos_) {
            std::string role = info.is_primary ? "PRIMARY" : (info.is_backup ? "BACKUP" : "NONE");
            std::cout << "[DEBUG-VIEW-UPDATE] Partition [" << info.start << ", " << info.end
                      << "] My Role: " << role
                      << " | Current Primary in View: " << info.primary_addr << std::endl;
            if (!info.is_primary) continue;
            for (const auto& addr : info.backups) {
                if (!replica_stubs_.count(addr)) {
                    replica_stubs_[addr] = KvStorage::NewStub(
                        grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
                }
            }
        }
        std::cout << "[KV] Updated view to version " << current_view_version_ << std::endl;

        // std::cout << "[KV] Updated view from coordinator. Participating ranges: "
        //           << shard_infos_.size() << std::endl;
    }

    /**
     * MaybeDoRemoteRecovery - Check if this node needs to sync from a primary
     *
     * Called periodically or after view updates.
     * If this node is a backup and hasn't recovered yet, it attempts to
     * fetch the latest state from the primary server.
     */
    void MaybeDoRemoteRecovery() {
        if (remote_recovered_) return;
        std::string primary_addr;

        bool is_backup = false;
        bool has_valid_view = false;
        {
            std::lock_guard<std::mutex> lock(view_mu_);
            if (current_view_version_ > 0) {
                has_valid_view = true;
                for (const auto& info : shard_infos_) {
                    if (info.is_backup) {
                        primary_addr = info.primary_addr;
                        is_backup = true;
                        break;
                    }
                }
            }
        }

        if (!has_valid_view) {
            std::cout << "[DEBUG-RECOVERY] has_valid_view is false, no remote recovery." << std::endl;
            return;
        }

        if (!is_backup) {
            std::cout << "[RECOVERY] This node has no backup role, skip." << std::endl;
            remote_recovered_ = true;
            return;
        }

        std::cout << "[RECOVERY] Found primary at " << primary_addr << ", starting remote recovery..." << std::endl;
        bool success = recovery_manager_->RemoteRecoverFromPrimary(primary_addr);
        if (success) {
            remote_recovered_ = true;
        } else {
            std::cerr << "[KV] Remote recovery failed, will retry later." << std::endl;
        }
    }

    /**
     * IsPrimaryForRow - Check if this server is the primary for the given row
     *
     * @param row The row key
     * @return true if this server is primary for the row, false otherwise
     */
    bool IsPrimaryForRow(const std::string& row) const {
        std::lock_guard<std::mutex> lock(view_mu_);
        for (const auto& info : shard_infos_) {
            if (!info.is_primary) continue;
            bool ge_start = info.start.empty() || row >= info.start;
            bool lt_end = info.end.empty() || row < info.end;
            if (ge_start && lt_end) return true;
        }
        return false;
    }

    /**
     * GetBackupsForRow - Get the backup addresses for the given row
     *
     * @param row The row key
     * @return A vector of backup addresses
     */
    std::vector<std::string> GetBackupsForRow(const std::string& row) const {
        std::lock_guard<std::mutex> lock(view_mu_);
        for (const auto& info : shard_infos_) {
            if (!info.is_primary) continue;
            bool ge_start = info.start.empty() || row >= info.start;
            bool lt_end = info.end.empty() || row < info.end;
            if (ge_start && lt_end) {
                return info.backups;
            }
        }
        return {};
    }

    /**
     * [Placeholder] PerformFullRecovery
     * * TODO (Full Solution)
     */
    void PerformFullRecovery() {
        if (recovery_manager_) {
            recovery_manager_->LocalRecover();
        }
    }

    /**
     * Replicate - gRPC handler for replication requests from primary to backups
     *
     * This method applies the log entry (PUT/DELETE) to the local tablet
     * and sends an acknowledgment response.
     */
    Status Replicate(ServerContext* context, const LogEntry* request, Ack* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        std::string display_val_size;
        if (request->op_type() == LogEntry::OP_CPUT) {
            display_val_size = std::to_string(request->v_new().size());
        } else {
            display_val_size = std::to_string(request->v().size());
        }

        const std::string& row = request->r();
        const std::string& col = request->c();
        const std::string value(request->v().begin(), request->v().end());
        const std::string old_value(request->v_old().begin(), request->v_old().end());
        const std::string new_value(request->v_new().begin(), request->v_new().end());
        const std::string& request_type = request->op_type() == 1 ?  "PUT" :
                                            request->op_type() == 2 ?  "DELETE" :
                                            request -> op_type() == 3 ? "CPUT" :"UNKNOWN";


        std::cout << "[" << "REPLICATE&" << request_type << "] "
                  << "row=" << row << ", col=" << col
                  << ", value_size=" << display_val_size << " bytes" << std::endl;

        int64_t local_lsn = wal_.GetLastLSN();
        int64_t request_lsn = request->lsn();
        std::cout << "local_lsn: " << local_lsn << std::endl;
        std::cout << "request_lsn: " << request_lsn << std::endl;
        if (request_lsn <= local_lsn && request_lsn > 0) {
            response->set_success(true);
            return Status::OK;
        }

        WriteAheadLog::WalRecord rec;
        rec.op = request->op_type();
        rec.row = request->r();
        rec.col = request->c();
        rec.lsn = request->lsn();

        if (request->op_type() == LogEntry::OP_PUT) {
            rec.v_new = std::string(request->v().begin(), request->v().end());
        } else if (request->op_type() == LogEntry::OP_CPUT) {
            rec.v_old = std::string(request->v_old().begin(), request->v_old().end());
            rec.v_new = std::string(request->v_new().begin(), request->v_new().end());
        }

        wal_.Append(rec);

        std::shared_ptr<RowData> row_data = tablet.FindOrCreateRow(row);
        std::unique_lock<std::shared_mutex> guard(row_data->lock);

        if (request->op_type() == LogEntry::OP_PUT) {
            row_data->cols[col] = value;
        } else if (request->op_type() == LogEntry::OP_DELETE) {
            auto it = row_data->cols.find(col);
            if (it != row_data->cols.end()) {
                row_data->cols.erase(it);
                std::cout << "[DEBUG-REPL] Deleted existing key: " << row << " " << col << std::endl;
            } else {
                std::cout << "[WARN-REPL] Received DELETE for non-existent key: " << row << " " << col
                          << ". Replica might be out of sync or key never existed." << std::endl;
            }
        } else if (request->op_type() == LogEntry::OP_CPUT) {
            auto it = row_data->cols.find(col);
            if (it == row_data->cols.end()) {
                std::cout << "[WARN-REPL-CPUT] Key NOT FOUND on Backup. Row=" << row
                          << " Col=" << col << ". Skipping update." << std::endl;
                response->set_success(false);
                return Status::OK;
            }
            if (it->second != old_value) {
                std::cout << "[WARN-REPL-CPUT] Old Value MISMATCH on Backup. Row=" << row
                          << " Col=" << col
                          << ". Expected hash/len: " << std::hash<std::string>{}(old_value) << "/" << old_value.size()
                          << ". Actual hash/len: " << std::hash<std::string>{}(it->second) << "/" << it->second.size()
                          << ". Skipping update." << std::endl;
                response->set_success(false);
                return Status::OK;
            }

            row_data->cols[col] = new_value;
        }

        response->set_success(true);
        return Status::OK;
    }

    /**
     * CheckpointLoop - Periodic background thread for persistence
     *
     * Wakes up every 120 seconds to save a snapshot of the tablet to disk.
     * Also truncates the WAL to free up space and speed up future recovery.
     * Uses checkpoint_protection_mu_ to ensure consistency with concurrent ops.
     */
    void CheckpointLoop(){
        while (true){
            std::this_thread::sleep_for(std::chrono::seconds(120));
            if (!is_running_) continue;
            std::cout << "[CHECKPOINT] Starting atomic checkpoint..." << std::endl;
            std::unique_lock<std::mutex> cp_lock(checkpoint_protection_mu_);
            checkpoint_mgr_.Save(tablet, wal_.GetLastLSN());
            wal_.Clear();
            std::cout << "[CHECKPOINT] Done. WAL truncated." << std::endl;
        }
    }

    Status DumpTablet(ServerContext* /*context*/,
                    const DumpTabletRequest* /*request*/,
                    DumpTabletResponse* response) override {
        auto snap = tablet.Snapshot();

        for (const auto& [row_key, cols] : snap) {
            for (const auto& [col_key, val] : cols) {
                auto* cell = response->add_cells();
                cell->set_r(row_key);
                cell->set_c(col_key);
                cell->set_v(val);
            }
        }

        std::cout << "[DUMP] DumpTablet called, cells=" << response->cells_size() << std::endl;
        return Status::OK;
    }
    /**
     * FetchSnapshot - Stream the entire tablet state to a requesting backup
     *
     * Used during remote recovery.
     * Batches rows into chunks (approx 4MB) to avoid oversized gRPC messages.
     * Includes the snapshot LSN so the backup knows where to start WAL replay.
     */
    Status FetchSnapshot(ServerContext*,
                        const FetchSnapshotRequest* request,
                        grpc::ServerWriter<FetchSnapshotResponse>* writer) override {

        constexpr size_t kSnapshotBatchBytes = 4 * 1024 * 1024;  // 4MB per chunk
        std::map<std::string, std::map<std::string, std::string>> snap;
        int64_t snapshot_lsn = 0;

        {
            std::unique_lock<std::mutex> cp_lock(checkpoint_protection_mu_);
            snap = tablet.Snapshot();
            snapshot_lsn = wal_.GetLastLSN();
        }

        FetchSnapshotResponse resp;
        resp.set_snapshot_lsn(snapshot_lsn);

        auto flush_if_needed = [&](bool force) -> bool {
            if (resp.cells_size() == 0) {
                return true;
            }
            if (!force && resp.ByteSizeLong() < static_cast<int64_t>(kSnapshotBatchBytes)) {
                return true;
            }
            if (!writer->Write(resp)) {
                return false;
            }
            resp.Clear();
            resp.set_snapshot_lsn(snapshot_lsn);
            return true;
        };

        for (const auto& [row, cols] : snap) {
            for (const auto& [col, val] : cols) {
                auto* cell = resp.add_cells();
                cell->set_r(row);
                cell->set_c(col);
                cell->set_v(val);

                if (!flush_if_needed(false)) {
                    return Status::CANCELLED;
                }
            }
        }

        if (!flush_if_needed(true)) {
            return Status::CANCELLED;
        }

        return Status::OK;
    }

    /**
     * FetchWAL - Stream WAL entries to a requesting backup
     *
     * Used during remote recovery (after snapshot) or for catching up.
     * Reads from the local WAL starting at the requested LSN.
     * Batches entries (approx 4MB soft limit) and supports pagination via next_lsn.
     */
    Status FetchWAL(ServerContext*,
                    const FetchWALRequest* req,
                    FetchWALResponse* resp) override {

        constexpr size_t kWalBatchBytes = 4 * 1024 * 1024;  // 4MB soft cap per response
        int64_t from_lsn = req->from_lsn();
        auto entries = wal_.LoadAfter(from_lsn);

        int64_t last_lsn = from_lsn;
        size_t emitted = 0;
        for (size_t i = 0; i < entries.size(); ++i) {
            auto& r = entries[i];
            LogEntry* e = resp->add_entries();
            e->set_op_type(static_cast<LogEntry::OpType>(r.op));
            e->set_r(r.row);
            e->set_c(r.col);
            e->set_v(r.v_new);
            e->set_v_old(r.v_old);
            e->set_v_new(r.v_new);
            e->set_lsn(r.lsn);
            last_lsn = r.lsn;
            emitted++;

            if (resp->ByteSizeLong() >= static_cast<int64_t>(kWalBatchBytes)) {
                break;
            }
        }

        if (emitted == 0) {
            resp->set_has_more(false);
        } else {
            resp->set_next_lsn(last_lsn + 1);
            resp->set_has_more(emitted < entries.size());
        }

        return Status::OK;
    }

public:
    /**
     * Put - Store or update a key-value pair
     *
     * Implementation steps (gRPC worker thread):
     * 1. Find or create the row in the tablet
     * 2. Acquire exclusive lock on the row
     * 3. (Future: Write to WAL and fsync - gRPC thread will block here)
     * 4. Update the in-memory tablet
     * 5. Release lock automatically (RAII)
     */
    Status Put(ServerContext* context, const PutRequest* request,
               PutResponse* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        std::unique_lock<std::mutex> cp_lock(checkpoint_protection_mu_);
        // Retrieve the request parameters
        const std::string& row = request->r();
        const std::string& col = request->c();
        const std::string value(request->v().begin(), request->v().end());

        // backup cannot put value
        if (!IsPrimaryForRow(row)) {
            std::cout << "[ERROR] Backup cannot accept PUT for row=" << row << std::endl;
            response->set_success(false);
            return Status::OK;
        }

        // Step 1: Find or create the row
        std::shared_ptr<RowData> row_data = tablet.FindOrCreateRow(row);

        // Step 2: Acquire exclusive lock on this row
        std::unique_lock<std::shared_mutex> guard(row_data->lock);

        // Step 3: TODO - Write to WAL and fsync
        // wal.append_and_fsync(row, col, value);
        WriteAheadLog::WalRecord rec;
        rec.op = 1;
        rec.row = row;
        rec.col = col;
        rec.v_old = "";
        rec.v_new = value;
        wal_.Append(rec);
        // Note: gRPC thread will block here during disk I/O

        int64_t new_lsn = wal_.GetLastLSN();

        // Step 4: Update the in-memory tablet
        row_data->cols[col] = value;

        // Step 5: Lock is automatically released when guard goes out of scope

        response->set_success(true);

        std::cout << "[PUT] row=" << row << ", col=" << col
                  << ", value_size=" << value.size() << " bytes" << std::endl;

        // Replicate to backups if this server is primary for the row
        if (IsPrimaryForRow(row)) {
            std::vector<std::string> backups = GetBackupsForRow(row);
            if (!backups.empty()) {
                const int total_nodes = static_cast<int>(backups.size()) + 1;
                const int quorum = total_nodes / 2 + 1;

                LogEntry entry;
                entry.set_op_type(LogEntry::OP_PUT);
                entry.set_r(row);
                entry.set_c(col);
                entry.set_v(request->v());
                entry.set_term(current_term_.load());
                entry.set_lsn(new_lsn);

                std::atomic<int> success_count(1);  // self already applied

                std::vector<std::future<void>> futures;
                futures.reserve(backups.size());

                for (const auto& addr : backups) {
                    std::cout << "Primary node replicating(put) row=" << row << ", col=" << col << ", value_size=" << value.size() << " bytes" << " to " << backups.size() << " backups: " << addr << std::endl;
                    auto* stub = replica_stubs_[addr].get();
                    futures.push_back(std::async(std::launch::async, [stub, entry, &success_count]() {
                        grpc::ClientContext ctx;
                        Ack ack;
                        Status s = stub->Replicate(&ctx, entry, &ack);
                        if (s.ok() && ack.success()) {
                            success_count.fetch_add(1);
                        }
                    }));
                }

                for (auto& f : futures) {
                    f.wait();
                }

                if (success_count.load() < quorum) {
                    response->set_success(false);
                }

                std::cout << "[DEBUG-PUT] Quorum check: " << success_count.load() << "/" << total_nodes << " for " << row << " " << col << std::endl;

                if (success_count.load() < total_nodes) {
                    std::cout << "[WARN-PUT] Partial replication failure! Some backups did not ack." << std::endl;
                }
            }
        }

        return Status::OK;
    }

    /**
     * Delete - Remove a key-value pair
     *
     * Implementation steps (gRPC worker thread):
     * 1. Find the row (do NOT create if not exists)
     * 2. Acquire exclusive lock on the row
     * 3. Write to WAL (future) and delete from in-memory tablet
     * 4. Release lock automatically (RAII)
     */
    Status Delete(ServerContext* context, const DeleteRequest* request,
                  DeleteResponse* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        std::unique_lock<std::mutex> cp_lock(checkpoint_protection_mu_);
        const std::string& row = request->r();
        const std::string& col = request->c();

        // backup cannot delete value
        if (!IsPrimaryForRow(row)) {
            std::cout << "[ERROR] Backup cannot accept DELETE for row=" << row << std::endl;
            response->set_success(false);
            return Status::OK;
        }
        // Step 1: Find the row (returns nullptr if not found)
        std::shared_ptr<RowData> row_data = tablet.FindRow(row);

        if (!row_data) {
            // Row doesn't exist
            response->set_success(false);
            std::cout << "[DELETE] row=" << row << ", col=" << col
                      << " - NOT FOUND (row doesn't exist)" << std::endl;
            return Status::OK;
        }

        // Step 2: Acquire exclusive lock for deletion
        std::unique_lock<std::shared_mutex> guard(row_data->lock);

        int64_t new_lsn = 0;
        // Step 3: Check if column exists and delete
        auto it = row_data->cols.find(col);
        if (it != row_data->cols.end()) {
            // TODO - Write to WAL and fsync
            WriteAheadLog::WalRecord rec;
            rec.op = 2;
            rec.row = row;
            rec.col = col;
            rec.v_old = "";
            rec.v_new = "";
            wal_.Append(rec);

            new_lsn = wal_.GetLastLSN();
            // wal.append_delete_and_fsync(row, col);

            row_data->cols.erase(it);

            response->set_success(true);
            std::cout << "[DELETE] row=" << row << ", col=" << col
                      << " - SUCCESS" << std::endl;
        } else {
            // Column doesn't exist
            response->set_success(false);
            std::cout << "[DELETE] row=" << row << ", col=" << col
                      << " - NOT FOUND (column doesn't exist)" << std::endl;
            return Status::OK;
        }

        // Step 4: Lock is automatically released

        // Replicate to backups if this server is primary for the row
        if (IsPrimaryForRow(row)) {
            std::vector<std::string> backups = GetBackupsForRow(row);
            if (!backups.empty()) {
                const int total_nodes = static_cast<int>(backups.size()) + 1;
                const int quorum = total_nodes / 2 + 1;

                LogEntry entry;
                entry.set_op_type(LogEntry::OP_DELETE);
                entry.set_r(row);
                entry.set_c(col);
                entry.set_term(current_term_.load());
                entry.set_lsn(new_lsn);

                std::atomic<int> success_count(1);
                std::vector<std::future<void>> futures;
                futures.reserve(backups.size());

                for (const auto& addr : backups) {
                    std::cout << "Primary node replicating(delete) row=" << row << ", col=" << col << " to " << backups.size() << " backups: " << addr << std::endl;
                    auto* stub = replica_stubs_[addr].get();
                    futures.push_back(std::async(std::launch::async, [stub, entry, &success_count]() {
                        grpc::ClientContext ctx;
                        Ack ack;
                        Status s = stub->Replicate(&ctx, entry, &ack);
                        if (s.ok() && ack.success()) {
                            success_count.fetch_add(1);
                        }
                    }));
                }

                for (auto& f : futures) {
                    f.wait();
                }

                if (success_count.load() < quorum) {
                    response->set_success(false);
                }

                std::cout << "[DEBUG-DELETE] Quorum check: " << success_count.load() << "/" << total_nodes << " for " << row << " " << col << std::endl;

                if (success_count.load() < total_nodes) {
                    std::cout << "[WARN-DELETE] Partial replication failure! Some backups did not ack." << std::endl;
                }
            }
        }

        return Status::OK;
    }

    Status GetRawData(ServerContext* /*context*/, const RawDataRequest* request,
                  RawDataResponse* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        int limit = request->limit();
        if (limit <= 0) {
            limit = 10;
        }

        RowKey next_start_key;
        auto rows = tablet.GetRowsPaged(request->start_key(), limit, next_start_key);

        for (const auto& row_pair : rows) {
            const auto& row_key = row_pair.first;
            const auto& columns = row_pair.second;
            for (const auto& col_entry : columns) {
                auto* entry = response->add_entries();
                entry->set_key(row_key);
                entry->set_col(col_entry.first);

                const std::string& raw_val = col_entry.second;
                std::string display_val;

                // filedata
                bool is_filedata_row =
                    (row_key.size() >= std::string("filedata").size() &&
                    row_key.rfind("-filedata") ==
                        row_key.size() - std::string("-filedata").size());

                if (is_filedata_row) {
                    std::ostringstream oss;
                    oss << "[binary data, " << raw_val.size() << " bytes]";
                    display_val = oss.str();
                }
                else if (raw_val.size() > 512) {
                    std::ostringstream oss;
                    oss << raw_val.substr(0, 512)
                        << " … [truncated, total " << raw_val.size() << " bytes]";
                    display_val = oss.str();
                } else {
                    display_val = raw_val;
                }

                entry->set_val(display_val);
            }
        }

        const bool has_more = !next_start_key.empty();
        response->set_has_more(has_more);
        response->set_next_start_key(next_start_key);

        return Status::OK;
    }


    /**
     * Get - Retrieve the value for a given key
     *
     * Implementation steps (gRPC worker thread):
     * 1. Find the row (do NOT create if not exists)
     * 2. Acquire shared lock on the row (allows multiple concurrent readers)
     * 3. Read from in-memory tablet
     * 4. Release lock automatically (RAII)
     */
    Status Get(ServerContext* context, const GetRequest* request,
               GetResponse* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        const std::string& row = request->r();
        const std::string& col = request->c();

        // Step 1: Find the row (returns nullptr if not found)
        std::shared_ptr<RowData> row_data = tablet.FindRow(row);

        if (!row_data) {
            // Row doesn't exist
            response->set_found(false);
            std::cout << "[GET] row=" << row << ", col=" << col
                      << " - NOT FOUND (row doesn't exist)" << std::endl;
            return Status::OK;
        }

        // Step 2: Acquire shared lock (allows concurrent reads)
        std::shared_lock<std::shared_mutex> guard(row_data->lock);

        // Step 3: Read from in-memory tablet
        auto it = row_data->cols.find(col);
        if (it != row_data->cols.end()) {
            response->set_v(it->second);
            response->set_found(true);

            std::cout << "[GET] row=" << row << ", col=" << col
          << " - FOUND, value_size=" << it->second.size() << " bytes"
          << std::endl;
        } else {
            // Column doesn't exist
            response->set_found(false);
            std::cout << "[GET] row=" << row << ", col=" << col
                      << " - NOT FOUND (column doesn't exist)" << std::endl;
        }

        // Step 4: Lock is automatically released

        return Status::OK;
    }

    /**
     * Cput - Compare-and-Put operation for atomic conditional updates
     *
     * Implementation steps (gRPC worker thread):
     * 1. Find the row (do NOT create if not exists)
     * 2. Acquire exclusive lock on the row
     * 3. Compare the old value atomically
     * 4. If match, write to WAL (future) and update in-memory tablet
     * 5. Release lock automatically (RAII)
     *
     * Critical: The "compare" and "set" must happen within the SAME lock
     * to guarantee atomicity!
     */
    Status Cput(ServerContext* context, const CputRequest* request,
                CputResponse* response) override {
        Status running = CheckRunning();
        if (!running.ok()) {
            return running;
        }
        std::unique_lock<std::mutex> cp_lock(checkpoint_protection_mu_);
        const std::string& row = request->r();
        const std::string& col = request->c();
        const std::string v_old(request->v_old().begin(), request->v_old().end());
        const std::string v_new(request->v_new().begin(), request->v_new().end());

        // backup cannot change value
        if (!IsPrimaryForRow(row)) {
            std::cout << "[ERROR] Backup cannot accept CPUT for row=" << row << std::endl;
            response->set_success(false);
            return Status::OK;
        }
        // Step 1: Find the row (returns nullptr if not found)
        std::shared_ptr<RowData> row_data = tablet.FindRow(row);

        if (!row_data) {
            // Row doesn't exist
            response->set_success(false);
            std::cout << "[CPUT] row=" << row << ", col=" << col
                      << " - FAILED (row doesn't exist)" << std::endl;
            return Status::OK;
        }

        // Step 2: Acquire exclusive lock for compare-and-swap
        std::unique_lock<std::shared_mutex> guard(row_data->lock);

        // Step 3: Compare the old value (within the lock!)
        auto it = row_data->cols.find(col);
        if (it == row_data->cols.end()) {
            // Column doesn't exist
            response->set_success(false);
            std::cout << "[CPUT] row=" << row << ", col=" << col
                      << " - FAILED (column doesn't exist)" << std::endl;
            return Status::OK;
        }

        if (it->second != v_old) {
            // Old value doesn't match
            response->set_success(false);
            std::cout << "[CPUT] row=" << row << ", col=" << col
                      << " - FAILED (old value mismatch)" << std::endl;
            return Status::OK;
        }

        // Step 4: Old value matches, perform the update
        WriteAheadLog::WalRecord rec;
        rec.op = 3;
        rec.row = row;
        rec.col = col;
        rec.v_old = v_old;
        rec.v_new = v_new;
        wal_.Append(rec);
        it->second = v_new;

        int64_t new_lsn = wal_.GetLastLSN();

        it->second = v_new;

        // Step 5: Lock is automatically released

        response->set_success(true);
        std::cout << "[CPUT] row=" << row << ", col=" << col
                  << " - SUCCESS (old value matched, updated to new value)" << std::endl;

        if (IsPrimaryForRow(row)) {
            std::vector<std::string> backups = GetBackupsForRow(row);
            if (!backups.empty()) {
                const int total_nodes = static_cast<int>(backups.size()) + 1;
                const int quorum = total_nodes / 2 + 1;

                LogEntry entry;
                entry.set_op_type(LogEntry::OP_CPUT);
                entry.set_r(row);
                entry.set_c(col);
                entry.set_v_old(request->v_old());
                entry.set_v_new(request->v_new());
                entry.set_term(current_term_.load());
                entry.set_lsn(new_lsn);

                std::atomic<int> success_count(1);  // self already applied

                std::vector<std::future<void>> futures;
                futures.reserve(backups.size());

                for (const auto& addr : backups) {
                    std::cout << "Primary node replicating(CPUT) row=" << row << ", col=" << col << " to " << backups.size() << " backups: " << addr << std::endl;
                    auto* stub = replica_stubs_[addr].get();
                    futures.push_back(std::async(std::launch::async, [stub, entry, &success_count]() {
                        grpc::ClientContext ctx;
                        Ack ack;
                        Status s = stub->Replicate(&ctx, entry, &ack);
                        if (s.ok() && ack.success()) {
                            success_count.fetch_add(1);
                        }
                    }));
                }

                for (auto& f : futures) {
                    f.wait();
                }

                if (success_count.load() < quorum) {
                    response->set_success(false);
                }

                std::cout << "[DEBUG-CPUT] Quorum check: " << success_count.load() << "/" << total_nodes << " for " << row << " " << col << std::endl;

                if (success_count.load() < total_nodes) {
                    std::cout << "[WARN-CPUT] Partial replication failure! Some backups did not ack." << std::endl;
                }
            }
        }

        return Status::OK;
    }

    Status AdminControl(ServerContext* /*context*/, const ControlRequest* request,
                        ControlResponse* response) override {
        if (request->cmd() == ControlRequest::SHUTDOWN) {
            std::cout << "[Admin] SHUTDOWN - stopping service" << std::endl;

            heartbeat_running_ = false;
            is_running_ = false;
            remote_recovered_ = false;
            tablet.Clear();

            std::cout << "[Admin] Server DOWN" << std::endl;
            response->set_message("Server is DOWN (Memory Cleared)");
        } else if (request->cmd() == ControlRequest::RESTART) {
            std::cout << "[Admin] RESTART request received." << std::endl;
            std::cout << "[DEBUG-ADMIN] remote_recovered_ flag is: " << remote_recovered_ << " (Should be 0 to trigger sync)" << std::endl;

            PerformFullRecovery();
            is_running_ = true;
            heartbeat_running_ = true;

            HeartbeatRequest hb_req;
            hb_req.mutable_node()->set_address(my_address_);
            HeartbeatResponse hb_resp;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            Status hb_status = coordinator_stub_->Heartbeat(&ctx, hb_req, &hb_resp);
            if (hb_status.ok() && hb_resp.has_new_view()) {
                UpdateViewFromGlobal(hb_resp.view());
            } else if (!hb_status.ok()) {
                std::cerr << "[Admin] Restart heartbeat failed: " << hb_status.error_message() << std::endl;
            }

            std::cout << "[Admin] Server RUNNING" << std::endl;
            response->set_message("Server is RUNNING (Data Recovered, Coordinator Notified)");
        } else {
            response->set_success(false);
            response->set_message("Unknown admin command");
            return Status::OK;
        }
        response->set_success(true);
        return Status::OK;
    }

    /**
     * PrintStats - Get statistics about the tablet (for debugging/monitoring)
     */
    void PrintStats() const {
        std::cout << "[STATS] Rows: " << tablet.GetRowCount()
                  << ", Total Columns: " << tablet.GetColumnCount() << std::endl;
    }

    RecoveryManager* GetRecoveryManager() { return recovery_manager_.get(); }

};

void RunServer(const std::string& server_address,
               const std::string& coordinator_addr) {
    KvStorageImpl service(server_address, coordinator_addr);
    std::cout << "[DEBUG-TIME] KvStorageImpl constructed. Preparing to bind port..." << std::endl;

    ServerBuilder builder;

    const int MAX_MESSAGE_SIZE = 25 * 1024 * 1024;
    builder.SetMaxReceiveMessageSize(MAX_MESSAGE_SIZE);
    builder.SetMaxSendMessageSize(MAX_MESSAGE_SIZE);

    // Listen on the given address
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    const std::unique_ptr<Server> server(builder.BuildAndStart());

    std::cout << "[DEBUG-TIME] Server BuildAndStart() finished. RPC Server is NOW listening on " << server_address << std::endl;

    std::cout << "==================================================" << std::endl;
    std::cout << "===== KV Storage listening on " << server_address
              << " (max message size " << MAX_MESSAGE_SIZE << " bytes) ====="
              << std::endl;
    std::cout << "Coordinator at: " << coordinator_addr << std::endl;
    std::cout << "==================================================" << std::endl;
    std::cout << "Server is using:" << std::endl;
    std::cout << "  - gRPC's built-in thread pool for concurrency" << std::endl;
    std::cout << "  - Thread-safe Tablet data structure" << std::endl;
    std::cout << "  - Fine-grained row-level locking" << std::endl;
    std::cout << "  - Deadlock prevention via ordered lock acquisition" << std::endl;
    std::cout << "==================================================" << std::endl;
    std::cout << "Press Ctrl+C to stop the server." << std::endl;
    std::cout << "==================================================" << std::endl;

    service.Start();

    server->Wait();
}

int main(int argc, char** argv) {
    std::string server_address = "0.0.0.0:8000";
    std::string coordinator_addr = "localhost:5051";

    if (argc > 1) {
        server_address = argv[1];
    }
    if (argc > 2) {
        coordinator_addr = argv[2];
    }

    try {
        RunServer(server_address, coordinator_addr);
    } catch (const std::exception& e) {
        std::cerr << "Server error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
