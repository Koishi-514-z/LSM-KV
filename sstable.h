#pragma once

#ifndef LSM_KV_SSTABLE_H
#define LSM_KV_SSTABLE_H
#include "bloom.h"
#include "skiplist.h"
#include "sstablehead.h"

#include <cstdint>
#include <vector>
#include <limits>
static uint64_t TIME = 0;                     // 全局时间戳

class sstable : public sstablehead { // 储存sstable的软数据结构
private:
    std::vector<std::string> data;

public:
    void reset() { // 这里不reset time, namesuf
        cnt    = 0;
        curpos = 0;
        minV   = INF;
        maxV   = 0;
        bytes  = 10240 + 32;
        filter.reset();
        index.clear();
        data.clear();
    }

    sstable() {
        time   = 0;
        cnt    = 0;
        curpos = 0;
        minV   = INF;
        maxV   = 0;
        bytes  = 10240 + 32;
        filter.reset();
        index.clear();
        data.clear();
    }

    sstable(skiplist *s) { // 将一个memtable转成sstable， 这里时间戳加1
        reset();
        curpos      = 0;
        bytes       = 10240 + 32 + s->getBytes();
        time        = ++TIME;
        filename    = "./data/level-0/" + std::to_string(TIME) + ".sst"; // 初始的文件名就是时间戳
        cnt         = 0;
        minV        = INF;
        maxV        = 0;
        slnode *cur = s->getFirst();
        while (cur->type != TAIL) { // curpos 为这个串的终止地址
            cnt++;
            curpos += cur->val.length();
            minV = std::min(minV, cur->key);
            maxV = std::max(maxV, cur->key);
            filter.insert(cur->key);
            index.emplace_back(cur->key, curpos);
            data.push_back(cur->val);
            cur = cur->nxt[0];
        }
    }

    sstable(int level) { // 将一个eleArr转成数个sstable
        reset();
        curpos      = 0;
        time        = ++TIME;
        filename    = "./data/level-" + std::to_string(level) + "/" + std::to_string(TIME) + ".sst"; 
        cnt         = 0;
        minV        = INF;
        maxV        = 0;
    }

    bool checkSize(std::string val, int curLevel);        // 检查大小，如果不够加val, 返回true
    void addNewSst(int curLevel);
    void putFile(const char *path);  //  将sstable输出到路径
    void loadFile(const char *path); // 从路径载入一个sstable

    void insert(uint64_t key, const std::string &val);

    bloom copyFilter();
    std::vector<Index> copyIndexs();

    std::string getData(int p) {
        return data[p];
    }

    sstablehead getHead(); // 取出头部
};

#endif // LSM_KV_SSTABLE_H
