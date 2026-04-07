#ifndef TABLET_H
#define TABLET_H

#include <string>
#include <map>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <vector>
#include <algorithm>

using RowKey = std::string;
using ColKey = std::string;
using Value = std::string;

/**
 * RowData - Encapsulates column data and a read-write lock for a single row
 *
 * This structure provides fine-grained locking at the row level, allowing:
 * - Multiple concurrent reads on the same row (shared lock)
 * - Exclusive writes on the same row (unique lock)
 */
struct RowData {
    std::map<ColKey, Value> cols;  // Ordered map for column data
    std::shared_mutex lock;        // Read-write lock for this row

    RowData() = default;

    // Prevent copying to avoid accidental lock duplication
    RowData(const RowData&) = delete;
    RowData& operator=(const RowData&) = delete;
};

/**
 * Tablet - Thread-safe in-memory data structure for KV storage
 *
 * Architecture:
 * - Uses std::map for row index: O(log n) lookup, ordered keys for admin pagination / range scans
 * - Uses std::map for columns within each row (stable iteration order)
 * - Implements two-level locking:
 *   1. map_lock: Protects the main hash table structure
 *   2. per-row locks: Protect individual row data
 *
 * Thread Safety:
 * - Race condition prevention: map_lock ensures atomic row creation
 * - Deadlock prevention: Operations requiring multiple row locks must
 *   acquire locks in lexicographic order of row keys
 *
 * Performance:
 * - Fine-grained locking minimizes contention
 * - Read operations on different rows can proceed concurrently
 * - Multiple readers can access the same row simultaneously
 */
class Tablet {
private:
    // Row index: row key -> row data (ordered map; see class comment)
    std::map<RowKey, std::shared_ptr<RowData>> rows;

    // Mutex to protect the main hash table structure
    std::mutex map_lock;

public:
    Tablet() = default;

    // Prevent copying
    Tablet(const Tablet&) = delete;
    Tablet& operator=(const Tablet&) = delete;

    /**
     * FindOrCreateRow - Find an existing row or create a new one atomically
     *
     * This method ensures thread-safe row creation:
     * - If multiple threads try to create the same row simultaneously,
     *   only one will create it and all will get the same shared_ptr
     *
     * @param r The row key to find or create
     * @return A shared_ptr to the RowData
     *
     * Thread Safety:
     * - map_lock protects against race conditions during row creation
     * - After returning, callers must acquire the row's lock before accessing cols
     */
    std::shared_ptr<RowData> FindOrCreateRow(const RowKey& r) {
        std::lock_guard<std::mutex> lock(map_lock);

        // Try to find existing row
        auto it = rows.find(r);
        if (it != rows.end()) {
            return it->second;
        }

        // Row doesn't exist, create a new one
        auto row_data = std::make_shared<RowData>();
        rows[r] = row_data;

        return row_data;
    }

    /**
     * FindRow - Find an existing row without creating it
     *
     * This method is used for read operations (Get) where we don't want
     * to create a new row if it doesn't exist.
     *
     * @param r The row key to find
     * @return A shared_ptr to the RowData, or nullptr if not found
     *
     * Thread Safety:
     * - map_lock protects against race conditions during row lookup
     * - Returns nullptr instead of creating a new row
     */
    std::shared_ptr<RowData> FindRow(const RowKey& r) {
        std::lock_guard<std::mutex> lock(map_lock);

        auto it = rows.find(r);
        if (it != rows.end()) {
            return it->second;
        }

        return nullptr;  // Row not found
    }

    /**
     * Put - Insert or update a key-value pair
     *
     * @param r Row key
     * @param c Column key
     * @param v Value
     */
    void Put(const RowKey& r, const ColKey& c, const Value& v) {
        auto row_data = FindOrCreateRow(r);

        // Acquire exclusive lock on this row
        std::unique_lock<std::shared_mutex> lock(row_data->lock);
        row_data->cols[c] = v;
    }

    /**
     * Get - Retrieve a value for a given key
     *
     * @param r Row key
     * @param c Column key
     * @param v Output parameter for the value
     * @return true if found, false otherwise
     */
    bool Get(const RowKey& r, const ColKey& c, Value& v) {
        std::shared_ptr<RowData> row_data;

        // First check if row exists
        {
            std::lock_guard<std::mutex> lock(map_lock);
            auto it = rows.find(r);
            if (it == rows.end()) {
                return false;  // Row doesn't exist
            }
            row_data = it->second;
        }

        // Acquire shared lock for reading
        std::shared_lock<std::shared_mutex> lock(row_data->lock);

        auto col_it = row_data->cols.find(c);
        if (col_it != row_data->cols.end()) {
            v = col_it->second;
            return true;
        }

        return false;  // Column doesn't exist
    }

    /**
     * Cput - Compare-and-Put (atomic conditional update)
     *
     * Only updates the value if the current value matches v_old
     *
     * @param r Row key
     * @param c Column key
     * @param v_old Expected current value
     * @param v_new New value to set
     * @return true if update succeeded, false otherwise
     */
    bool Cput(const RowKey& r, const ColKey& c, const Value& v_old, const Value& v_new) {
        std::shared_ptr<RowData> row_data;

        // First check if row exists
        {
            std::lock_guard<std::mutex> lock(map_lock);
            auto it = rows.find(r);
            if (it == rows.end()) {
                return false;  // Row doesn't exist
            }
            row_data = it->second;
        }

        // Acquire exclusive lock for compare-and-swap
        std::unique_lock<std::shared_mutex> lock(row_data->lock);

        auto col_it = row_data->cols.find(c);
        if (col_it != row_data->cols.end()) {
            // Column exists, check if value matches
            if (col_it->second == v_old) {
                // Value matches, update it
                col_it->second = v_new;
                return true;
            }
        }

        return false;  // Column doesn't exist or value mismatch
    }

    /**
     * Delete - Remove a key-value pair
     *
     * @param r Row key
     * @param c Column key
     * @return true if deletion succeeded, false if key didn't exist
     */
    bool Delete(const RowKey& r, const ColKey& c) {
        std::shared_ptr<RowData> row_data;

        // First check if row exists
        {
            std::lock_guard<std::mutex> lock(map_lock);
            auto it = rows.find(r);
            if (it == rows.end()) {
                return false;  // Row doesn't exist
            }
            row_data = it->second;
        }

        // Acquire exclusive lock for deletion
        std::unique_lock<std::shared_mutex> lock(row_data->lock);

        auto col_it = row_data->cols.find(c);
        if (col_it != row_data->cols.end()) {
            row_data->cols.erase(col_it);
            return true;
        }

        return false;  // Column doesn't exist
    }

    /**
     * AcquireRowsInOrder - Helper function to acquire locks on multiple rows
     * in lexicographic order to prevent deadlocks
     *
     * This is a utility function for operations that need to lock multiple rows.
     * It ensures locks are always acquired in the same order (sorted by row key)
     * to prevent circular wait conditions that could cause deadlocks.
     *
     * Example usage:
     *   auto locked_rows = tablet.AcquireRowsInOrder({"row2", "row1", "row3"});
     *   // Now you can safely access locked_rows[0], locked_rows[1], etc.
     *
     * @param row_keys Vector of row keys to lock
     * @return Vector of pairs: (RowKey, locked RowData with unique_lock)
     */
    struct LockedRow {
        RowKey key;
        std::shared_ptr<RowData> data;
        std::unique_lock<std::shared_mutex> lock;

        LockedRow(const RowKey& k, std::shared_ptr<RowData> d)
            : key(k), data(d), lock(d->lock) {}
    };

    std::vector<LockedRow> AcquireRowsInOrder(std::vector<RowKey> row_keys) {
        // Sort row keys to ensure consistent lock ordering
        std::sort(row_keys.begin(), row_keys.end());

        // Remove duplicates
        row_keys.erase(std::unique(row_keys.begin(), row_keys.end()), row_keys.end());

        std::vector<LockedRow> locked_rows;
        locked_rows.reserve(row_keys.size());

        // Acquire locks in sorted order
        for (const auto& row_key : row_keys) {
            auto row_data = FindOrCreateRow(row_key);
            locked_rows.emplace_back(row_key, row_data);
        }

        return locked_rows;
    }

    /**
     * GetRowCount - Get the number of rows in the tablet (for debugging/monitoring)
     */
    size_t GetRowCount() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(map_lock));
        return rows.size();
    }

    /**
     * GetColumnCount - Get the total number of columns across all rows
     */
    size_t GetColumnCount() const {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(map_lock));
        size_t total = 0;
        for (const auto& [row_key, row_data] : rows) {
            std::shared_lock<std::shared_mutex> row_lock(row_data->lock);
            total += row_data->cols.size();
        }
        return total;
    }

    /**
     * Clear - Remove all data from the tablet
     *
     * Used during recovery or shutdown to reset the state.
     * Acquires map_lock to ensure no concurrent accesses during clearing.
     */
    void Clear() {
        std::lock_guard<std::mutex> lock(map_lock);
        rows.clear();
    }

    /**
     * Snapshot - Create a consistent copy of the entire tablet
     *
     * Iterates over all rows and copies their content.
     * Note: This is not a strictly atomic snapshot of the whole database,
     * but rather row-by-row atomic copies.
     */
    std::map<RowKey, std::map<ColKey, Value>> Snapshot() const {
        std::map<RowKey, std::map<ColKey, Value>> snap;
        std::vector<RowKey> row_keys;
        {
            std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(map_lock));
            for (const auto& [row, _] : rows) {
                row_keys.push_back(row);
            }
        }
        for (const auto& row : row_keys) {
            std::shared_ptr<RowData> row_data;

            {
                std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(map_lock));
                auto it = rows.find(row);
                if (it == rows.end()) continue;
                row_data = it->second;
            }
            std::shared_lock<std::shared_mutex> row_lock(row_data->lock);

            snap[row] = row_data->cols;
        }

        return snap;
    }

    /**
 * GetRowsPaged - Get a batch of rows starting from start_key (inclusive)
 * Used by Admin Console for pagination.
 * * @param start_key The row key to start from. "" means start from beginning.
 * @param limit Maximum number of rows to return.
 * @param next_start_key Output parameter for the start key of the next page.
 * @return Vector of pairs (RowKey, RowData snapshot)
 */
    std::vector<std::pair<RowKey, std::map<ColKey, Value>>>
    GetRowsPaged(const RowKey& start_key, int limit, RowKey& next_start_key) {
        std::lock_guard<std::mutex> lock(map_lock);

        std::vector<std::pair<RowKey, std::map<ColKey, Value>>> result;
        result.reserve(limit);

        auto it = start_key.empty() ? rows.begin() : rows.lower_bound(start_key);

        for (int i = 0; i < limit && it != rows.end(); ++i, ++it) {
            std::shared_lock<std::shared_mutex> row_lock(it->second->lock);
            result.push_back({it->first, it->second->cols});
        }

        if (it != rows.end()) {
            next_start_key = it->first;
        } else {
            next_start_key = "";
        }

        return result;
    }
};

#endif // TABLET_H
