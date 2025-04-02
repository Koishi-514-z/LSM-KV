#include <cassert>
#include <iostream>
#include <map>
#include <chrono> 
#include <iostream>
#include <iomanip> 
#include <string>
#include <random>
#include <vector>
#include <algorithm>

#include "kvstore.h"

using namespace std;
using namespace std::chrono;

const uint64_t TEST_MAX = 1024 * 32; 
const uint64_t KEY_RANGE = 1024 * 36;

std::random_device rd;
std::mt19937_64 gen(rd());

void printHeader(const string& title) {
    cout << "\n" << string(60, '=') << endl;
    cout << "  " << title << endl;
    cout << string(60, '=') << endl;
}

void printResult(const string& opName, uint64_t opCount, milliseconds duration, 
                uint64_t success = 0, uint64_t total = 0) {
    double throughput = (double)opCount / duration.count() * 1000;
    double latency = (double)duration.count() / opCount;
    
    cout << "  " << left << setw(15) << opName << ": " 
         << right << setw(9) << opCount << " ops in " 
         << setw(7) << duration.count() << " ms" << endl;
    cout << "  " << left << setw(15) << "Throughput" << ": " 
         << fixed << setprecision(2) << right << setw(9) << throughput << " ops/sec" << endl;
    cout << "  " << left << setw(15) << "Avg Latency" << ": " 
         << fixed << setprecision(3) << right << setw(9) << latency << " ms/op" << endl;
    
    if (success > 0 || total > 0) {
        double successRate = (double)success / total * 100;
        cout << "  " << left << setw(15) << "Success Rate" << ": " 
             << fixed << setprecision(1) << right << setw(9) << successRate << "% (" 
             << success << "/" << total << ")" << endl;
    }
    
    cout << endl;
}

uint64_t random_key() {
    std::uniform_int_distribution<uint64_t> dis(1, KEY_RANGE);
    return dis(gen);
}

string generate_value(uint64_t len) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    string result;
    result.reserve(len);
    for (uint64_t i = 0; i < len; ++i) {
        result += alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return result;
}

void test_put(KVStore& store, const vector<uint64_t>& keys, bool sequential = false) {
    printHeader("PUT PERFORMANCE (" + string(sequential ? "SEQUENTIAL" : "RANDOM") + " KEYS)");
    
    vector<string> values(TEST_MAX);
    
    cout << "  Generating test values..." << endl;
    for (uint64_t i = 0; i < TEST_MAX; i++) {
        values[i] = generate_value(keys[i]);
    }
    
    auto start = high_resolution_clock::now();
    
    for (uint64_t i = 0; i < TEST_MAX; i++) {
        store.put(keys[i], values[i]);
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    printResult("PUT", TEST_MAX, duration);
}

void test_get(KVStore& store, const vector<uint64_t>& keys) {
    printHeader("GET PERFORMANCE");
    
    auto start = high_resolution_clock::now();
    
    uint64_t found = 0;
    for (const auto& key : keys) {
        string value = store.get(key);
        if (!value.empty()) {
            found++;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    printResult("GET", keys.size(), duration, found, keys.size());
}

void test_del(KVStore& store, const vector<uint64_t>& keys) {
    printHeader("DELETE PERFORMANCE");
    
    auto start = high_resolution_clock::now();
    
    uint64_t deleted = 0;
    for (const auto& key : keys) {
        if (store.del(key)) {
            deleted++;
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    printResult("DELETE", keys.size(), duration, deleted, keys.size());
}

void test_mixed_workload(KVStore& store) {
    printHeader("MIXED WORKLOAD PERFORMANCE");
    
    vector<pair<int, uint64_t>> operations; // (op_type, key)
    operations.reserve(TEST_MAX);
    
    vector<uint64_t> existing_keys;
    
    cout << "  Generating mixed operations (40% PUT, 40% GET, 20% DEL)..." << endl;
    for (uint64_t i = 0; i < TEST_MAX; i++) {
        int op_type;
        if (i < TEST_MAX * 0.4) {
            op_type = 0; // PUT
        } else if (i < TEST_MAX * 0.8) {
            op_type = 1; // GET
        } else {
            op_type = 2; // DEL
        }
        
        uint64_t key = random_key();
        operations.push_back({op_type, key});
        
        if (op_type == 0) {
            existing_keys.push_back(key);
        }
    }
    
    cout << "  Shuffling operations..." << endl;
    shuffle(operations.begin(), operations.end(), gen);
    
    auto start = high_resolution_clock::now();
    
    uint64_t puts = 0, gets = 0, dels = 0;
    for (const auto& op : operations) {
        switch (op.first) {
            case 0: { // PUT
                store.put(op.second, generate_value(op.second));
                puts++;
                break;
            }
            case 1: { // GET
                store.get(op.second);
                gets++;
                break;
            }
            case 2: { // DEL
                store.del(op.second);
                dels++;
                break;
            }
        }
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);
    
    printResult("MIXED", TEST_MAX, duration);
    cout << "  Operations breakdown:" << endl;
    cout << "    PUT: " << puts << " operations (" << fixed << setprecision(1) << (double)puts/TEST_MAX*100 << "%)" << endl;
    cout << "    GET: " << gets << " operations (" << fixed << setprecision(1) << (double)gets/TEST_MAX*100 << "%)" << endl;
    cout << "    DEL: " << dels << " operations (" << fixed << setprecision(1) << (double)dels/TEST_MAX*100 << "%)" << endl;
}

int main() {
    printHeader("LSM-TREE PERFORMANCE TEST");
    cout << "  Data size: " << TEST_MAX << " entries" << endl;
    cout << "  Key range: 1 to " << KEY_RANGE << endl << endl;
    
    KVStore store("./data");
    store.reset();
    
    cout << "  Generating test data..." << endl;
    vector<uint64_t> sequential_keys(TEST_MAX);
    vector<uint64_t> random_keys(TEST_MAX);
    
    for (uint64_t i = 0; i < TEST_MAX; i++) {
        sequential_keys[i] = i + 1;
        random_keys[i] = random_key();
    }
    
    // Test with sequential keys
    test_put(store, sequential_keys, true);
    test_get(store, sequential_keys);
    test_del(store, sequential_keys);
    
    store.reset();
    
    // Test with random keys
    test_put(store, random_keys, false);
    test_get(store, random_keys);
    test_del(store, random_keys);
    
    store.reset();
    
    // Test mixed workload
    test_mixed_workload(store);
    
    printHeader("PERFORMANCE TESTING SUMMARY");
    cout << "  All tests completed successfully." << endl;
    cout << "  Total test data size: " << (TEST_MAX * 3) << " entries" << endl;
    cout << "  Test database directory: ./data" << endl;
    cout << string(60, '=') << endl;
    
    return 0;
}