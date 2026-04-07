#include <iostream>
#include <shared_mutex>
#include <mutex>
#include "tablet.h"
#include "checkpoint.h"
#include "wal.h"
#include "myproto/kvstorage.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using kvstorage::LogEntry;
using kvstorage::FetchSnapshotRequest;
using kvstorage::FetchSnapshotResponse;
using kvstorage::FetchWALRequest;
using kvstorage::FetchWALResponse;

class RecoveryManager {
public:
    RecoveryManager(Tablet& tablet,
                    CheckpointManager& ckpt,
                    WriteAheadLog& wal,
                    std::mutex& checkpoint_mu)
        : tablet_(tablet), ckpt_(ckpt), wal_(wal), checkpoint_mu_(checkpoint_mu){}

    /**
     * LocalRecover - Restore state from local checkpoint and WAL
     *
     * 1. Loads the last valid checkpoint into the tablet.
     * 2. Replays WAL entries with LSN > checkpoint LSN.
     *
     * This brings the node up to the state it was in before shutdown/crash.
     */
    void LocalRecover() {
        std::cout << "[RECOVERY] ===== Local Recovery Start =====" << std::endl;

        std::cout << "[RECOVERY] Loading checkpoint..." << std::endl;
        ckpt_.Load(tablet_);
        int64_t base_lsn = ckpt_.GetCheckpointLSN();
        std::cout << "[RECOVERY] Checkpoint load complete." << std::endl;

        std::cout << "[RECOVERY] Replaying WAL entries..." << std::endl;

        auto wal_entries = wal_.LoadAll();
        std::cout << "[RECOVERY] WAL entries found: " << wal_entries.size() << std::endl;

        for (auto& r : wal_entries) {
            if (r.lsn <= base_lsn) continue;
            auto row_ptr = tablet_.FindOrCreateRow(r.row);

            std::unique_lock<std::shared_mutex> guard(row_ptr->lock);

            if (r.op == 1) {
                row_ptr->cols[r.col] = r.v_new;
            }
            else if (r.op == 2) {
                row_ptr->cols.erase(r.col);
            }
            else if (r.op == 3) {
                auto it = row_ptr->cols.find(r.col);
                if (it != row_ptr->cols.end() && it->second == r.v_old) {
                    it->second = r.v_new;
                }
            }
        }

        std::cout << "[RECOVERY] WAL replay complete." << std::endl;
        std::cout << "[RECOVERY] ===== Local Recovery Finished =====" << std::endl;
    }
    /**
     * RemoteRecoverFromPrimary - Fetch state from the primary node
     *
     * Used when a node joins as a backup or needs to resync after being out of date.
     * 1. Fetches a full snapshot from the primary.
     * 2. Clears local state and applies the snapshot.
     * 3. Fetches and replays WAL entries from the primary starting after the snapshot LSN.
     * 4. Saves a new local checkpoint to persist the recovered state.
     *
     * @param primary_addr Address of the primary node to sync from
     * @return true if recovery succeeded, false otherwise
     */
    bool RemoteRecoverFromPrimary(const std::string& primary_addr) {
        std::cout << "[DEBUG-RECOVERY] Attempting remote recovery from " << primary_addr << std::endl;

        std::cout << "[RECOVERY] Remote recovery from primary " << primary_addr << "\n";

        grpc::ChannelArguments args;
        const int MAX_MESSAGE_SIZE = 25 * 1024 * 1024; // 25 MB
        args.SetMaxReceiveMessageSize(MAX_MESSAGE_SIZE);
        args.SetMaxSendMessageSize(MAX_MESSAGE_SIZE);
        auto stub = kvstorage::KvStorage::NewStub(
            grpc::CreateCustomChannel(primary_addr,grpc::InsecureChannelCredentials(), args));

        FetchSnapshotRequest snap_req;
        grpc::ClientContext snap_ctx;

        std::unique_ptr<grpc::ClientReader<FetchSnapshotResponse>> reader(
            stub->FetchSnapshot(&snap_ctx, snap_req)
        );

        FetchSnapshotResponse snap_resp;
        int64_t snapshot_lsn = 0;

        // Buffer snapshot data to avoid clearing local data on failure
        std::map<std::string, std::map<std::string, std::string>> temp_snapshot;

        while (reader->Read(&snap_resp)) {
            for (const auto& cell : snap_resp.cells()) {
                temp_snapshot[cell.r()][cell.c()] = cell.v();
            }
            snapshot_lsn = snap_resp.snapshot_lsn();
        }

        grpc::Status status = reader->Finish();
        if (!status.ok()) {
            std::cerr << "[RECOVERY] FetchSnapshot RPC failed: "
                      << status.error_message() << ". Retaining local data." << std::endl;
            return false;
        }

        // Snapshot fetch successful. Now safe to overwrite local data.
        std::cout << "[RECOVERY] Snapshot fetched successfully. Applying to local tablet..." << std::endl;
        std::cout << "[RECOVERY] Clearing local WAL" << std::endl;
        std::unique_lock<std::mutex> cp_lock(checkpoint_mu_);
        wal_.Clear();
        wal_.ResetToLSN(snapshot_lsn);
        tablet_.Clear();

        for (const auto& [row_key, cols] : temp_snapshot) {
            auto row_ptr = tablet_.FindOrCreateRow(row_key);
            std::unique_lock<std::shared_mutex> lock(row_ptr->lock);
            for (const auto& [col_key, val] : cols) {
                row_ptr->cols[col_key] = val;
            }
        }

        int64_t next_lsn = snapshot_lsn + 1;

        while (true) {
            FetchWALRequest wreq;
            wreq.set_from_lsn(next_lsn);

            FetchWALResponse wresp;
            grpc::ClientContext wctx;

            auto status = stub->FetchWAL(&wctx, wreq, &wresp);
            if (!status.ok()) {
                std::cerr << "[RECOVERY] FetchWAL RPC failed: "
                        << status.error_message() << "\n";
                return false;
            }

            for (const auto& e : wresp.entries()) {
                auto row_ptr = tablet_.FindOrCreateRow(e.r());
                std::unique_lock<std::shared_mutex> lock(row_ptr->lock);
                WriteAheadLog::WalRecord rec;
                rec.op = e.op_type();
                rec.row = e.r();
                rec.col = e.c();

                if (e.op_type() == LogEntry::OP_PUT) {
                    row_ptr->cols[e.c()] = e.v();
                    rec.v_new = e.v();
                } else if (e.op_type() == LogEntry::OP_DELETE) {
                    row_ptr->cols.erase(e.c());
                } else if (e.op_type() == LogEntry::OP_CPUT) {
                    auto it = row_ptr->cols.find(e.c());
                    if (it != row_ptr->cols.end() &&
                        it->second == e.v_old()) {
                        it->second = e.v_new();
                        rec.v_old = e.v_old();
                        rec.v_new = e.v();
                    }
                }

                rec.lsn = e.lsn();
                wal_.Append(rec);
                next_lsn = e.lsn() + 1;
            }

            if (!wresp.has_more()) {
                break;
            }
        }

        wal_.SetBaseLSN(next_lsn - 1);
        ckpt_.Save(tablet_, wal_.GetLastLSN());
        cp_lock.unlock();

        std::cout << "[RECOVERY] Remote recovery finished. Checkpoint saved." << std::endl;
        return true;
    }

private:

        void ApplyWalEntry(const kvstorage::LogEntry& e) {
            auto row_ptr = tablet_.FindOrCreateRow(e.r());
            std::unique_lock<std::shared_mutex> lock(row_ptr->lock);

            if (e.op_type() == kvstorage::LogEntry::OP_PUT) {
                row_ptr->cols[e.c()] = std::string(e.v().begin(), e.v().end());
            }
            else if (e.op_type() == kvstorage::LogEntry::OP_DELETE) {
                row_ptr->cols.erase(e.c());
            }
            else if (e.op_type() == kvstorage::LogEntry::OP_CPUT) {
                auto it = row_ptr->cols.find(e.c());
                if (it != row_ptr->cols.end()) {
                    std::string oldv(e.v_old().begin(), e.v_old().end());
                    if (it->second == oldv) {
                        it->second = std::string(e.v_new().begin(), e.v_new().end());
                    }
                }
            }
        }


        void FetchAndApplyWAL(kvstorage::KvStorage::Stub* stub, int64_t from_lsn) {

            kvstorage::FetchWALRequest req;
            req.set_from_lsn(from_lsn);

            while (true) {
                kvstorage::FetchWALResponse resp;
                grpc::ClientContext ctx;

                auto status = stub->FetchWAL(&ctx, req, &resp);
                if (!status.ok()) {
                    std::cerr << "[RECOVERY] FetchWAL RPC failed.\n";
                    return;
                }

                for (const auto& e : resp.entries()) {
                    ApplyWalEntry(e);
                }

                if (!resp.has_more()) break;

                req.set_from_lsn(resp.next_lsn());
            }
        }


    private:
        Tablet& tablet_;
        CheckpointManager& ckpt_;
        WriteAheadLog& wal_;
        std::mutex& checkpoint_mu_;
    };
