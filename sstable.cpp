#include "sstable.h"

#include "sstablehead.h"
#include "utils.h"

#include <iostream>
const uint32_t MAXSIZE = 2 * 1024 * 1024; // 2MB

/*
 *  在path路径下创建一个新的sstable，时间戳为缓存sstable的时间戳
 * */
void sstable::putFile(const char *path) { // 将内存中的输出到二进制文件中
    // std::cout << "output path" << path << std::endl;
    FILE *file = fopen(path, "wb");
    fseek(file, 0, SEEK_SET);
    // 4个u64变量
    fwrite(&time, 8, 1, file);
    fwrite(&cnt, 8, 1, file);
    fwrite(&minV, 8, 1, file);
    fwrite(&maxV, 8, 1, file);
    for (int i = 0; i < 8 * M; i += 8) { // bloom
        unsigned char cur = 0x0;
        for (int j = 0; j < 8; ++j)
            cur |= (filter.getBit(i + j) << j);
        fwrite(&cur, 1, 1, file);
    }
    int size = index.size();
    for (int i = 0; i < size; ++i) { // index
        uint64_t key    = index[i].key;
        uint32_t offset = index[i].offset;
        fwrite(&key, 8, 1, file);
        fwrite(&offset, 4, 1, file);
    }
    size = data.size();
    for (int i = 0; i < size; ++i) { // datas
        fwrite(data[i].data(), 1, data[i].length(), file);
    }
    fflush(file); // 清空缓冲区
    fclose(file);
}

char buf[2097152];

void sstable::loadFile(const char *path) { // load file from the path
    filename = path;
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
    FILE *file = fopen(path, "rb+");
    fseek(file, 0, SEEK_SET); // 移动到开头
    reset();
    fread(&time, 8, 1, file);
    fread(&cnt, 8, 1, file);
    fread(&minV, 8, 1, file);
    fread(&maxV, 8, 1, file);
    for (int i = 0; i < 8 * M; i += 8) { // bloom
        unsigned char cur = 0x0;
        fread(&cur, 1, 1, file);
        for (int j = 0; j < 8; ++j) {
            if ((cur >> j) & 1)
                filter.setBit(i + j);
        }
    }
    bytes = 10240 + 32 + 12 * cnt;
    Index temp;
    for (int i = 0; i < cnt; ++i) { // index
        fread(&temp.key, 8, 1, file);
        fread(&temp.offset, 4, 1, file);
        index.push_back(temp);
    }
    bytes += temp.offset;
    std::string cur; // data
    fread(buf, 1, index[0].offset, file);
    buf[index[0].offset] = '\0';
    cur                  = buf;
    data.push_back(cur);
    for (int i = 1; i < cnt; ++i) {
        fread(buf, 1, index[i].offset - index[i - 1].offset, file);
        buf[index[i].offset - index[i - 1].offset] = '\0';
        cur                                        = buf;
        data.push_back(cur);
    }
    fflush(file);
    fclose(file);
}

bloom sstable::copyFilter() {
    bloom *res = new bloom;
    res->setBitset(filter.getBitset());
    return *res;
}

std::vector<Index> sstable::copyIndexs() {
    std::vector<Index> *res = new std::vector<Index>(index);
    return *res;
}

sstablehead sstable::getHead() {
    auto *res = new sstablehead;
    res->setFilename(filename);
    res->setNamesuffix(nameSuffix);
    res->setTime(time);
    res->setCnt(cnt);
    res->setMinV(minV);
    res->setMaxV(maxV);
    res->setBytes(bytes);
    res->setFilter(filter);
    res->setIndex(index);
    return *res;
}

// 向sstable尾部插一个key-val对，同时修改头和bloom filter
void sstable::insert(uint64_t key, const std::string &val) {
    cnt++;
    curpos += val.length();
    minV = std::min(minV, key);
    maxV = std::max(maxV, key);
    bytes += 12 + val.length();
    index.emplace_back(key, curpos);
    filter.insert(key);
    data.push_back(val);
}

void sstable::addNewSst(int curLevel) {
    std::string url = std::string("./data/level-") + std::to_string(curLevel) + "/";
    url += std::to_string(time) + "-" + std::to_string(++nameSuffix) + ".sst";
    filename = url;
    putFile(url.data());
}

bool sstable::checkSize(std::string val, int curLevel) {
    uint32_t nxtBytes = bytes + 12 + val.length();
    if (nxtBytes > MAXSIZE) {
        return true;
    }
    return false;
}
