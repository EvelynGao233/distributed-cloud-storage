#include "checkpoint.h"
#include <cstdio>
#include <iostream>

CheckpointManager::CheckpointManager(const std::string& path)
    : path_(path) {}

/**
 * Save - Persist the current tablet state to disk
 *
 * Writes a snapshot of the tablet and the associated LSN to a temporary file,
 * then atomically renames it to the target checkpoint path.
 *
 * @param tablet The in-memory tablet to save
 * @param snapshot_lsn The LSN corresponding to this snapshot state
 */
void CheckpointManager::Save(const Tablet& tablet, int64_t snapshot_lsn) {
    std::lock_guard<std::mutex> lk(mu_);
    std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            std::cout << "[Checkpoint] Failed to open temp file: " << tmp_path << std::endl;
            return;
        }
        out.write(reinterpret_cast<char*>(&snapshot_lsn), sizeof(int64_t));
        auto snapshot = tablet.Snapshot();
        uint32_t row_count = snapshot.size();
        out.write((char*)&row_count, sizeof(uint32_t));
        for (auto& [row, cols] : snapshot) {
            WriteString(out, row);
            uint32_t col_count = cols.size();
            out.write((char*)&col_count, sizeof(uint32_t));
            for (auto& [c, v] : cols) {
                WriteString(out, c);
                WriteString(out, v);
            }
        }
        out.flush();
    }
    if (std::rename(tmp_path.c_str(), path_.c_str()) != 0) {
        std::cerr << "[Checkpoint] Failed to rename checkpoint file!" << std::endl;
        std::remove(tmp_path.c_str());
    }
}

/**
 * Load - Restore tablet state from disk
 *
 * Reads the checkpoint file and populates the tablet.
 * Also updates the last_snapshot_lsn_ for WAL replay.
 *
 * @param tablet The tablet to populate
 */
void CheckpointManager::Load(Tablet& tablet) {
    std::lock_guard<std::mutex> lk(mu_);

    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return;

    if (!in.read(reinterpret_cast<char*>(&last_snapshot_lsn_), sizeof(int64_t))) {
        last_snapshot_lsn_ = 0;
        return;
    }
    uint32_t row_count;
    if (!in.read((char*)&row_count, sizeof(uint32_t))) return;

    for (uint32_t i = 0; i < row_count; i++) {
        std::string row = ReadString(in);
        auto row_ptr = tablet.FindOrCreateRow(row);

        uint32_t col_count;
        in.read((char*)&col_count, sizeof(uint32_t));

        for (uint32_t j = 0; j < col_count; j++) {
            std::string col = ReadString(in);
            std::string val = ReadString(in);

            std::unique_lock<std::shared_mutex> lock(row_ptr->lock);
            row_ptr->cols[col] = val;
        }
    }
}

void CheckpointManager::WriteString(std::ofstream& out, const std::string& s) {
    uint32_t len = s.size();
    out.write((char*)&len, sizeof(uint32_t));
    out.write(s.data(), len);
}

std::string CheckpointManager::ReadString(std::ifstream& in) {
    uint32_t len;
    in.read((char*)&len, sizeof(uint32_t));
    std::string s(len, '\0');
    in.read(&s[0], len);
    return s;
}
