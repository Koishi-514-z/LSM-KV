#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H

#include <cstdint>
#include <limits>
#include <list>
#include <string>
#include <vector>

enum TYPE {
    HEAD,
    NORMAL,
    TAIL
};

const int MAX_LEVEL = 18;

static const std::string DEL = "~DELETED~";

const uint64_t INF = std::numeric_limits<uint64_t>::max();


struct ele {
    uint64_t key;
    std::string value;
    uint64_t time;
    int level;

    ele(uint64_t k, const std::string &v, uint64_t t, int l) : key(k), value(v), time(t), level(l) {}
    ele() : key(0), value(""), time(0), level(0) {}

    bool operator< (const ele &other) const {
        if(key == other.key) {
            if(level == other.level) {
                return time > other.time;
            }
            return level < other.level;
        }
        return key < other.key;
    }
};

struct vecele {
    uint64_t key;
    std::vector<float> vec;

    vecele(uint64_t k, std::vector<float> v) : key(k), vec(v) {}
    vecele() : key(0), vec(std::vector<float>()) {}

    bool operator== (uint64_t otherKey) {
        return key == otherKey;
    }
};


class slnode {
public:
    uint64_t key;
    std::string val;
    TYPE type;
    std::vector<slnode *> nxt;

    slnode(uint64_t key, const std::string &val, TYPE type) {
        this->key  = key;
        this->val  = val;
        this->type = type;
        for (int i = 0; i < MAX_LEVEL; ++i)
            nxt.push_back(nullptr);
    }
};

class skiplist {
private:
    double p;
    uint32_t bytes = 0x0; // bytes表示index + data区域的字节数
    int curMaxL    = 1;
    slnode *head   = new slnode(0, "", HEAD);
    slnode *tail   = new slnode(INF, "", TAIL);

public:
    skiplist(double p) { // p 表示增长概率
        bytes   = 0x0;
        curMaxL = 1;
        this->p = p;
        for (int i = 0; i < MAX_LEVEL; ++i)
            head->nxt[i] = tail;
    }

    slnode *getFirst() {
        return head->nxt[0];
    }

    double my_rand();
    int randLevel();
    void insert(uint64_t key, const std::string &str);
    std::string search(uint64_t key);
    bool del(uint64_t key);
    void scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list);
    slnode *lowerBound(uint64_t key);
    void reset();
    uint32_t getBytes();
};

#endif // LSM_KV_SKIPLIST_H
