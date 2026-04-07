#include "wal.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

WriteAheadLog::WriteAheadLog(const std::string& path)
    : path_(path)
{
    log_.open(path_, std::ios::binary | std::ios::app);
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return;

    while (true) {
        WalRecord r;

        if (!in.read(reinterpret_cast<char*>(&r.op), sizeof(int)))
            break;

        auto read_str = [&](std::string& s) -> bool {
            uint32_t len;
            if (!in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) return false;
            s.resize(len);
            return (bool)in.read(&s[0], len);
        };

        if (!read_str(r.row)) break;
        if (!read_str(r.col)) break;
        if (!read_str(r.v_old)) break;
        if (!read_str(r.v_new)) break;

        if (!in.read(reinterpret_cast<char*>(&r.lsn), sizeof(int64_t)))
            break;

        if (r.lsn > last_lsn_) last_lsn_ = r.lsn;
    }
}


/**
 * Append - Write a new record to the WAL
 *
 * Assigns a new LSN if not provided (lsn=0), writes the record to disk,
 * and fsyncs to ensure durability.
 *
 * @param r The record to append
 */
void WriteAheadLog::Append(const WalRecord& r) {
    std::lock_guard<std::mutex> lk(mu_);
    WalRecord rin = r;
    if (rin.lsn != 0) {
        if (rin.lsn > last_lsn_) {
            last_lsn_ = rin.lsn;
        }
    } else {
        rin.lsn = ++last_lsn_;
    }

    auto write_str = [&](const std::string& s) {
        uint32_t len = s.size();
        log_.write(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        log_.write(s.data(), len);
    };

    log_.write(reinterpret_cast<const char*>(&rin.op), sizeof(int));
    write_str(r.row);
    write_str(r.col);
    write_str(r.v_old);
    write_str(r.v_new);
    log_.write(reinterpret_cast<const char*>(&rin.lsn), sizeof(int64_t));

    log_.flush();

    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

/**
 * LoadAll - Read all records from the WAL file
 *
 * Used during local recovery to replay operations not yet checkpointed.
 * Also updates last_lsn_ to the highest LSN found in the log.
 */
std::vector<WriteAheadLog::WalRecord> WriteAheadLog::LoadAll() {
    std::vector<WalRecord> out;

    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return out;

    while (true) {
        WalRecord r;

        if (!in.read(reinterpret_cast<char*>(&r.op), sizeof(int))) break;

        auto read_str = [&](std::string& s) -> bool {
            uint32_t len;
            if (!in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) return false;
            s.resize(len);
            return (bool)in.read(&s[0], len);
        };

        if (!read_str(r.row)) break;
        if (!read_str(r.col)) break;
        if (!read_str(r.v_old)) break;
        if (!read_str(r.v_new)) break;
        if (!in.read(reinterpret_cast<char*>(&r.lsn), sizeof(int64_t)))
            break;
        if (r.lsn > last_lsn_) {
            last_lsn_ = r.lsn;
        }
        out.push_back(std::move(r));
    }
    return out;
}

/**
 * Clear - Truncate the WAL file
 *
 * Used after a successful checkpoint to free up disk space.
 * Does NOT reset the in-memory LSN counter (last_lsn_).
 */
void WriteAheadLog::Clear() {
    std::lock_guard<std::mutex> lk(mu_);
    TruncateAndReopenLocked();
}

/**
 * ResetToLSN - Truncate WAL and set the base LSN
 *
 * Used during remote recovery to align the WAL state with a received snapshot.
 */
void WriteAheadLog::ResetToLSN(int64_t lsn) {
    std::lock_guard<std::mutex> lk(mu_);
    last_lsn_ = lsn;
    TruncateAndReopenLocked();
}

void WriteAheadLog::TruncateAndReopenLocked() {
    if (log_.is_open()) {
        log_.close();
    }
    {
        std::ofstream trunc(path_, std::ios::binary | std::ios::trunc);
    }
    log_.open(path_, std::ios::binary | std::ios::app);
}
