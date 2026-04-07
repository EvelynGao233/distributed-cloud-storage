/**
 *
 * g++ -std=c++17 -pthread -o tablet_test server/src/tablet_test.cpp
 *
 * ./tablet_test
 */

#include "tablet.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>
#include <chrono>

#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"

void assert_true(bool condition, const std::string& message) {
    if (condition) {
        std::cout << GREEN << "✓ " << message << RESET << std::endl;
    } else {
        std::cout << RED << "✗ " << message << RESET << std::endl;
        exit(1);
    }
}

void test_basic_put_get() {
    std::cout << BLUE << "\n[Test 1] Basic Put/Get" << RESET << std::endl;

    Tablet tablet;

    // Put
    auto row_data = tablet.FindOrCreateRow("user1");
    {
        std::unique_lock<std::shared_mutex> lock(row_data->lock);
        row_data->cols["name"] = "Alice";
        row_data->cols["age"] = "25";
    }

    // Get
    auto found_row = tablet.FindRow("user1");
    assert_true(found_row != nullptr, "Row should exist");

    {
        std::shared_lock<std::shared_mutex> lock(found_row->lock);
        assert_true(found_row->cols["name"] == "Alice", "Name should be Alice");
        assert_true(found_row->cols["age"] == "25", "Age should be 25");
    }

    // Get non-existent row
    auto not_found = tablet.FindRow("user2");
    assert_true(not_found == nullptr, "Non-existent row should return nullptr");
}

void test_concurrent_create_row() {
    std::cout << BLUE << "\n[Test 2] Concurrent Row Creation (Race Condition Test)" << RESET << std::endl;

    Tablet tablet;
    std::vector<std::thread> threads;
    std::vector<std::shared_ptr<RowData>> results(10);

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&tablet, &results, i]() {
            results[i] = tablet.FindOrCreateRow("shared_row");
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    for (int i = 1; i < 10; ++i) {
        assert_true(results[i] == results[0], "All threads should get the same RowData instance");
    }

    assert_true(tablet.GetRowCount() == 1, "Should only have 1 row created");
}

void test_cput_atomicity() {
    std::cout << BLUE << "\n[Test 3] Cput Atomicity (Compare-and-Swap)" << RESET << std::endl;

    Tablet tablet;

    auto row_data = tablet.FindOrCreateRow("counter_row");
    {
        std::unique_lock<std::shared_mutex> lock(row_data->lock);
        row_data->cols["counter"] = "0";
    }

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < 100; ++i) {
        threads.emplace_back([&tablet, &success_count, &failure_count]() {
            bool succeeded = false;

            for (int retry = 0; retry < 100 && !succeeded; ++retry) {
                auto row = tablet.FindRow("counter_row");
                if (!row) continue;

                std::string current_value;
                {
                    std::shared_lock<std::shared_mutex> lock(row->lock);
                    auto it = row->cols.find("counter");
                    if (it == row->cols.end()) continue;
                    current_value = it->second;
                }

                int current = std::stoi(current_value);
                int new_value = current + 1;

                {
                    std::unique_lock<std::shared_mutex> lock(row->lock);
                    auto it = row->cols.find("counter");

                    if (it != row->cols.end() && it->second == current_value) {
                        it->second = std::to_string(new_value);
                        succeeded = true;
                        success_count++;
                    } else {
                        failure_count++;
                    }
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto final_row = tablet.FindRow("counter_row");
    std::string final_value;
    {
        std::shared_lock<std::shared_mutex> lock(final_row->lock);
        final_value = final_row->cols["counter"];
    }

    assert_true(final_value == "100", "Counter should be exactly 100");
    assert_true(success_count == 100, "Should have exactly 100 successful updates");

    std::cout << YELLOW << "  Success: " << success_count
              << ", Failures: " << failure_count
              << " (retries due to contention)" << RESET << std::endl;
}

void test_concurrent_readers_writers() {
    std::cout << BLUE << "\n[Test 4] Concurrent Readers and Writers" << RESET << std::endl;

    Tablet tablet;

    auto row_data = tablet.FindOrCreateRow("shared_data");
    {
        std::unique_lock<std::shared_mutex> lock(row_data->lock);
        row_data->cols["value"] = "initial";
    }

    std::atomic<int> read_count{0};
    std::atomic<int> write_count{0};
    std::atomic<bool> stop{false};

    std::vector<std::thread> threads;

    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&tablet, &read_count, &stop]() {
            while (!stop) {
                auto row = tablet.FindRow("shared_data");
                if (row) {
                    std::shared_lock<std::shared_mutex> lock(row->lock);
                    std::string value = row->cols["value"];
                    read_count++;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }

    threads.emplace_back([&tablet, &write_count, &stop]() {
        int counter = 0;
        while (!stop) {
            auto row = tablet.FindRow("shared_data");
            if (row) {
                std::unique_lock<std::shared_mutex> lock(row->lock);
                row->cols["value"] = "update_" + std::to_string(counter++);
                write_count++;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(1));
    stop = true;

    for (auto& t : threads) {
        t.join();
    }

    std::cout << YELLOW << "  Reads: " << read_count
              << ", Writes: " << write_count << RESET << std::endl;

    assert_true(read_count > 1000, "Should have many concurrent reads");
    assert_true(write_count > 100, "Should have some writes");
}

void test_deadlock_prevention() {
    std::cout << BLUE << "\n[Test 5] Deadlock Prevention (Ordered Lock Acquisition)" << RESET << std::endl;

    Tablet tablet;

    {
        auto acc_a = tablet.FindOrCreateRow("account_A");
        std::unique_lock<std::shared_mutex> lock_a(acc_a->lock);
        acc_a->cols["balance"] = "1000";
    }
    {
        auto acc_b = tablet.FindOrCreateRow("account_B");
        std::unique_lock<std::shared_mutex> lock_b(acc_b->lock);
        acc_b->cols["balance"] = "1000";
    }

    std::atomic<int> transfer_count{0};
    std::vector<std::thread> threads;

    threads.emplace_back([&tablet, &transfer_count]() {
        for (int i = 0; i < 50; ++i) {
            auto locked = tablet.AcquireRowsInOrder({"account_A", "account_B"});

            auto& acc_a = locked[0].data;
            auto& acc_b = locked[1].data;

            int bal_a = std::stoi(acc_a->cols["balance"]);
            int bal_b = std::stoi(acc_b->cols["balance"]);

            if (bal_a >= 10) {
                acc_a->cols["balance"] = std::to_string(bal_a - 10);
                acc_b->cols["balance"] = std::to_string(bal_b + 10);
                transfer_count++;
            }

            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    threads.emplace_back([&tablet, &transfer_count]() {
        for (int i = 0; i < 50; ++i) {
            auto locked = tablet.AcquireRowsInOrder({"account_B", "account_A"});

            auto& acc_a = locked[0].data;
            auto& acc_b = locked[1].data;

            int bal_a = std::stoi(acc_a->cols["balance"]);
            int bal_b = std::stoi(acc_b->cols["balance"]);

            if (bal_b >= 5) {
                acc_b->cols["balance"] = std::to_string(bal_b - 5);
                acc_a->cols["balance"] = std::to_string(bal_a + 5);
                transfer_count++;
            }

            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    for (auto& t : threads) {
        t.join();
    }

    auto acc_a = tablet.FindRow("account_A");
    auto acc_b = tablet.FindRow("account_B");

    int final_a, final_b;
    {
        std::shared_lock<std::shared_mutex> lock_a(acc_a->lock);
        final_a = std::stoi(acc_a->cols["balance"]);
    }
    {
        std::shared_lock<std::shared_mutex> lock_b(acc_b->lock);
        final_b = std::stoi(acc_b->cols["balance"]);
    }

    int total = final_a + final_b;

    std::cout << YELLOW << "  Final balances: A=" << final_a
              << ", B=" << final_b
              << ", Total=" << total << RESET << std::endl;

    assert_true(total == 2000, "Total balance should remain 2000 (conservation of money)");
    assert_true(transfer_count > 0, "Should have completed some transfers");

    std::cout << GREEN << "  No deadlock detected!" << RESET << std::endl;
}

void test_performance_benchmark() {
    std::cout << BLUE << "\n[Test 6] Performance Benchmark" << RESET << std::endl;

    Tablet tablet;
    const int num_threads = 10;
    const int ops_per_thread = 10000;

    std::atomic<int> total_ops{0};
    std::vector<std::thread> threads;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&tablet, &total_ops, i, ops_per_thread]() {
            for (int j = 0; j < ops_per_thread; ++j) {
                std::string row_key = "row_" + std::to_string(i);
                std::string col_key = "col_" + std::to_string(j % 100);
                std::string value = "value_" + std::to_string(j);

                // Put
                auto row_data = tablet.FindOrCreateRow(row_key);
                {
                    std::unique_lock<std::shared_mutex> lock(row_data->lock);
                    row_data->cols[col_key] = value;
                }

                // Get
                auto found = tablet.FindRow(row_key);
                if (found) {
                    std::shared_lock<std::shared_mutex> lock(found->lock);
                    std::string v = found->cols[col_key];
                }

                total_ops += 2;  // Put + Get
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    double ops_per_second = (total_ops.load() * 1000.0) / duration.count();

    std::cout << YELLOW << "  Total operations: " << total_ops << std::endl;
    std::cout << "  Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << static_cast<int>(ops_per_second) << " ops/sec" << RESET << std::endl;

    assert_true(tablet.GetRowCount() == num_threads, "Should have created correct number of rows");
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "  Tablet Thread-Safety Test Suite" << std::endl;
    std::cout << "========================================" << std::endl;

    try {
        test_basic_put_get();
        test_concurrent_create_row();
        test_cput_atomicity();
        test_concurrent_readers_writers();
        test_deadlock_prevention();
        test_performance_benchmark();

        std::cout << GREEN << "\n========================================" << std::endl;
        std::cout << "  ✓ All tests passed!" << std::endl;
        std::cout << "========================================" << RESET << std::endl;

        return 0;
    } catch (const std::exception& e) {
        std::cerr << RED << "\n✗ Test failed with exception: " << e.what() << RESET << std::endl;
        return 1;
    }
}

