#ifndef WAL_H
#define WAL_H
#include <fstream>
#include <mutex>
#include <string>
#include <vector>
#include <unistd.h>
#include "tablet.h"

class WriteAheadLog {
public:
    struct WalRecord {
        int op;
        std::string row;
        std::string col;
        std::string v_old;
        std::string v_new;
        int64_t lsn = 0; 
    };

    explicit WriteAheadLog(const std::string& path);

    /**
     * Append - Write a new record to the WAL
     *
     * Assigns a new LSN if not provided (lsn=0), writes the record to disk,
     * and fsyncs to ensure durability.
     *
     * @param r The record to append
     */
    void Append(const WalRecord& r);

    /**
     * LoadAll - Read all records from the WAL file
     *
     * Used during local recovery to replay operations not yet checkpointed.
     * Also updates last_lsn_ to the highest LSN found in the log.
     */
    std::vector<WalRecord> LoadAll();

    // Clears WAL file so checkpoint can restart from clean state
    void Clear();

    /**
     * ResetToLSN - Truncate WAL and set the base LSN
     *
     * Used during remote recovery to align the WAL state with a received snapshot.
     */
    void ResetToLSN(int64_t lsn);
    int64_t GetLastLSN() const { return last_lsn_; }

    /**
     * LoadAfter - Retrieve WAL records with LSN greater than the given value
     *
     * Used by backups to fetch missing updates from the primary.
     */
    std::vector<WalRecord> LoadAfter(int64_t lsn) {
        std::vector<WalRecord> result;
        auto all = LoadAll();
        for (auto& r : all) {
            if (r.lsn > lsn)
                result.push_back(r);
        }
        return result;
    }

    void SetBaseLSN(int64_t lsn) {
        std::lock_guard<std::mutex> lk(mu_);
        if (lsn > last_lsn_) {
            last_lsn_ = lsn;
        }
    }

private:
    std::string path_;
    std::ofstream log_;
    std::mutex mu_;
    int64_t last_lsn_ = 0;
    void TruncateAndReopenLocked();
};
#endif