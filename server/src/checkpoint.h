#pragma once
#include <fstream>
#include <mutex>
#include <string>
#include "tablet.h"

class CheckpointManager {
public:
    explicit CheckpointManager(const std::string& path);

    /**
     * Save - Persist the current tablet state to disk
     *
     * Writes a snapshot of the tablet and the associated LSN to a temporary file,
     * then atomically renames it to the target checkpoint path.
     *
     * @param tablet The in-memory tablet to save
     * @param snapshot_lsn The LSN corresponding to this snapshot state
     */
    void Save(const Tablet& tablet, int64_t snapshot_lsn);

    /**
     * Load - Restore tablet state from disk
     *
     * Reads the checkpoint file and populates the tablet.
     * Also updates the last_snapshot_lsn_ for WAL replay.
     *
     * @param tablet The tablet to populate
     */
    void Load(Tablet& tablet);
    int64_t GetCheckpointLSN() const {
        return last_snapshot_lsn_;
    }
private:
    std::string path_;
    std::mutex mu_;
    int64_t last_snapshot_lsn_ = 0;

    void WriteString(std::ofstream& out, const std::string& s);
    std::string ReadString(std::ifstream& in);
};
