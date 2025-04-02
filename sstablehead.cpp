#include "sstablehead.h"

#include <cstring>
#include <iostream>

void sstablehead::loadFileHead(const char *path) { // 只读取文件头
    FILE *file = fopen(path, "rb+");               // 注意格式为二进制
    filename   = path;
    int len = std::strlen(path), c = 0;
    std::string suf;
    for (int i = 0; i < len; ++i) {
        if (c == 2)
            suf += path[i];
        if (path[i] == '-') {
            c++;
        }
        if (path[i] == '.')
            c = 0;
    }
    if (suf.size())
        nameSuffix = std::stoi(suf);
    else
        nameSuffix = 0;
    fseek(file, 0, SEEK_SET);
    reset();

    fread(&time, 8, 1, file);
    fread(&cnt, 8, 1, file);
    fread(&minV, 8, 1, file);
    fread(&maxV, 8, 1, file);
    for (int i = 0; i < M * 8; i += 8) { // bloom
        unsigned char cur = 0x0;
        fread(&cur, 1, 1, file);
        for (int j = 0; j < 8; ++j) {
            if ((cur >> j) & 1)
                filter.setBit(i + j);
        }
    }
    Index temp;
    bytes = 10240 + 32 + 12 * cnt;
    for (int i = 0; i < cnt; ++i) { // index
        fread(&temp.key, 8, 1, file);
        fread(&temp.offset, 4, 1, file);
        index.push_back(temp);
    }
    bytes += temp.offset;
    fflush(file);
    fclose(file);
}

void sstablehead::reset() {
    filter.reset();
    index.clear();
}

int sstablehead::search(uint64_t key) {
    int res = filter.search(key);
    if (!res)
        return -1; // bloom 说没有 确实没有
    auto it = std::lower_bound(index.begin(), index.end(), Index(key, 0));
    if (it == index.end())
        return -1; // 没找到
    if ((*it).key == key)
        return it - index.begin(); // 在这一块二分找到了，返回第几个字符串
    return -1;
}

int sstablehead::searchOffset(uint64_t key, uint32_t &len) {
    int res = filter.search(key);
    if (!res)
        return -1; // bloom 说没有 确实没有
    auto it = std::lower_bound(index.begin(), index.end(), Index(key, 0));
    if (it == index.end())
        return -1; // 没找到
    if ((*it).key == key) {
        if (it == index.begin()) {
            len = (*it).offset;
            return 0;
        } else {
            len = (*it).offset - (*(it - 1)).offset;
            return (*(it - 1)).offset;
        }
    }
    return -1;
}

int sstablehead::lowerBound(uint64_t key) {
    auto it = std::lower_bound(index.begin(), index.end(), Index(key, 0));
    return it - index.begin(); // found
}
