#ifndef LSM_KV_BLOOM_H
#define LSM_KV_BLOOM_H
#include "MurmurHash3.h"

#include <bitset>
#include <cstdint>

const uint32_t M = 10240;

class bloom {
private:
    std::bitset<8 * M> s;
    uint32_t hashV[4];

public:
    bloom() {}

    ~bloom() {
        s.reset();
    }

    void reset() {
        s.reset();
    }

    void setBitset(std::bitset<8 * M> t) {
        s = t;
    }

    std::bitset<8 * M> getBitset() {
        return s;
    }

    bool getBit(uint32_t p) {
        return s[p];
    }

    void setBit(uint32_t p) {
        s[p] = true;
    }

    void insert(uint64_t key);
    bool search(uint64_t key);
};

#endif // LSM_KV_BLOOM_H
