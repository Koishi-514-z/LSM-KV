#include "kvstore.h"

#include "skiplist.h"
#include "sstable.h"
#include "utils.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <cmath>

const uint32_t MAXSIZE       = 2 * 1024 * 1024;


KVStore::KVStore(const std::string &dir) :
    KVStoreAPI(dir) // read from sstables
{
    for (totalLevel = 0;; ++totalLevel) {
        std::string path = dir + "/level-" + std::to_string(totalLevel) + "/";
        std::vector<std::string> files;
        if (!utils::dirExists(path)) {
            totalLevel--;
            break; // stop read
        }
        int nums = utils::scanDir(path, files);
        sstablehead cur;
        for (int i = 0; i < nums; ++i) {       // 读每一个文件头
            std::string url = path + files[i]; // url, 每一个文件名
            cur.loadFileHead(url.data());
            sstableIndex[totalLevel].push_back(cur);
            TIME = std::max(TIME, cur.getTime()); // 更新时间戳
        }
    }
}

KVStore::~KVStore()
{
    sstable ss(s);
    if (!ss.getCnt())
        return; // empty sstable
    std::string path = std::string("./data/level-0/");
    if (!utils::dirExists(path)) {
        utils::_mkdir(path.data());
        totalLevel = 0;
    }
    ss.putFile(ss.getFilename().data());
    compaction(); // 从0层开始尝试合并
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &val) {
    uint32_t nxtsize = s->getBytes();
    std::string res  = s->search(key);
    if (!res.length()) { // new add
        nxtsize += 12 + val.length();
    } else
        nxtsize = nxtsize - res.length() + val.length(); // change string
    if (nxtsize + 10240 + 32 <= MAXSIZE)
        s->insert(key, val); // 小于等于（不超过） 2MB
    else {
        sstable ss(s);
        s->reset();
        std::string url  = ss.getFilename();
        std::string path = "./data/level-0";
        if (!utils::dirExists(path)) {
            utils::mkdir(path.data());
            totalLevel = 0;
        }
        addsstable(ss, 0);      // 加入缓存
        ss.putFile(url.data()); // 加入磁盘
        compaction();
        s->insert(key, val);
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) //
{
    uint64_t time = 0;
    int goalOffset;
    uint32_t goalLen;
    std::string goalUrl;
    std::string res = s->search(key);
    if (res.length()) { // 在memtable中找到, 或者是deleted，说明最近被删除过，
                        // 不用查sstable
        if (res == DEL)
            return "";
        return res;
    }
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key < it.getMinV() || key > it.getMaxV())
                continue;
            uint32_t len;
            int offset = it.searchOffset(key, len);
            if (offset == -1) {
                if (level == 0)
                    continue;
                else
                    break;
            }
            // sstable ss;
            // ss.loadFile(it.getFilename().data());
            if (it.getTime() > time) { // find the latest head
                time       = it.getTime();
                goalUrl    = it.getFilename();
                goalOffset = offset + 32 + 10240 + 12 * it.getCnt();
                goalLen    = len;
            }
        }
        if (time)
            break; // only a test for found
    }
    if (!goalUrl.length())
        return ""; // not found a sstable
    res = fetchString(goalUrl, goalOffset, goalLen);
    if (res == DEL)
        return "";
    return res;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    std::string res = get(key);
    if (!res.length())
        return false; // not exist
    put(key, DEL);    // put a del marker
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    s->reset(); // 先清空memtable
    std::vector<std::string> files;
    for (int level = 0; level <= totalLevel; ++level) { // 依层清空每一层的sstables
        std::string path = std::string("./data/level-") + std::to_string(level);
        int size         = utils::scanDir(path, files);
        for (int i = 0; i < size; ++i) {
            std::string file = path + "/" + files[i];
            utils::rmfile(file.data());
        }
        utils::rmdir(path.data());
        sstableIndex[level].clear();
    }
    totalLevel = -1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */

struct myPair {
    uint64_t key, time;
    int id, index;
    std::string filename;

    myPair(uint64_t key, uint64_t time, int index, int id,
           std::string file) { // construct function
        this->time     = time;
        this->key      = key;
        this->id       = id;
        this->index    = index;
        this->filename = file;
    }
};

struct cmp {
    bool operator()(myPair &a, myPair &b) {
        if (a.key == b.key)
            return a.time < b.time;
        return a.key > b.key;
    }
};


void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    std::vector<std::pair<uint64_t, std::string>> mem;
    // std::set<myPair> heap; // 维护一个指针最小堆
    std::priority_queue<myPair, std::vector<myPair>, cmp> heap;
    // std::vector<sstable> ssts;
    std::vector<sstablehead> sshs;
    s->scan(key1, key2, mem);   // add in mem
    std::vector<int> head, end; // [head, end)
    int cnt = 0;
    if (mem.size())
        heap.push(myPair(mem[0].first, INF, 0, -1, "qwq"));
    for (int level = 0; level <= totalLevel; ++level) {
        for (sstablehead it : sstableIndex[level]) {
            if (key1 > it.getMaxV() || key2 < it.getMinV())
                continue; // 无交集
            int hIndex = it.lowerBound(key1);
            int tIndex = it.lowerBound(key2);
            if (hIndex < it.getCnt()) { // 此sstable可用
                // sstable ss; // 读sstable
                std::string url = it.getFilename();
                // ss.loadFile(url.data());

                heap.push(myPair(it.getKey(hIndex), it.getTime(), hIndex, cnt++, url));
                head.push_back(hIndex);
                if (it.search(key2) == tIndex)
                    tIndex++; // tIndex为第一个不可的
                end.push_back(tIndex);
                // ssts.push_back(ss); // 加入ss
                sshs.push_back(it);
            }
        }
    }
    uint64_t lastKey = INF; // only choose the latest key
    while (!heap.empty()) { // 维护堆
        myPair cur = heap.top();
        heap.pop();
        if (cur.id >= 0) { // from sst
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                uint32_t start  = sshs[cur.id].getOffset(cur.index - 1);
                uint32_t len    = sshs[cur.id].getOffset(cur.index) - start;
                uint32_t scnt   = sshs[cur.id].getCnt();
                std::string res = fetchString(cur.filename, 10240 + 32 + scnt * 12 + start, len);
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, res);
            }
            if (cur.index + 1 < end[cur.id]) { // add next one to heap
                heap.push(myPair(sshs[cur.id].getKey(cur.index + 1), cur.time, cur.index + 1, cur.id, cur.filename));
            }
        } else { // from mem
            if (cur.key != lastKey) {
                lastKey         = cur.key;
                std::string res = mem[cur.index].second;
                if (res.length() && res != DEL)
                    list.emplace_back(cur.key, mem[cur.index].second);
            }
            if (cur.index < mem.size() - 1) {
                heap.push(myPair(mem[cur.index + 1].first, cur.time, cur.index + 1, -1, cur.filename));
            }
        }
    }
}


int maxLimit(int level) {
    return pow(2, level + 1);
}

void Merge(std::vector<ele> &eleArr, std::vector<ele> &tmpArr, int startIndex, int midIndex, int endIndex){
    int i = startIndex, j = midIndex + 1, k = startIndex;
    while(i <= midIndex && j <= endIndex) {
        if(eleArr[j] < eleArr[i]) {
            tmpArr[k++] = eleArr[j++];
        }
        else {
            tmpArr[k++] = eleArr[i++];
        }
    }

    while(i <= midIndex) {
        tmpArr[k++] = eleArr[i++];
    }
    while(j <= endIndex) {
        tmpArr[k++] = eleArr[j++];
    }

    for(i = startIndex; i <= endIndex; ++i) {
        eleArr[i] = tmpArr[i];
    }  
}

void MergeSort(std::vector<ele> &eleArr, std::vector<ele> &tmpArr, int startIndex, int endIndex) {
    int midIndex;
    if(startIndex < endIndex) {
        midIndex = startIndex + (endIndex - startIndex) / 2;
        MergeSort(eleArr, tmpArr, startIndex, midIndex);
        MergeSort(eleArr, tmpArr, midIndex + 1, endIndex);
        Merge(eleArr, tmpArr, startIndex, midIndex, endIndex);
    }
}

void removeDup(std::vector<ele> &eleArr) {
    for(auto it = eleArr.begin(); it != eleArr.end();) {
        auto it2 = it + 1;
        while(it2 != eleArr.end() && it->key == it2->key) {
            it2 = eleArr.erase(it2);
        }
        it = it2;
    }
}

void removeDel(std::vector<ele> &eleArr) {
    for(auto it = eleArr.begin(); it != eleArr.end();) {
        if(it->value == DEL) {
            it = eleArr.erase(it);
        }
        else {
            ++it;
        }
    }
}

std::vector<sstablehead>::iterator findMin(std::vector<sstablehead> &ssh) {
    auto minSst = ssh.begin();
    for (auto it = ssh.begin(); it != ssh.end(); ++it) {
        if(*it < *minSst) {
            minSst = it;
        }
    }
    return minSst;
}


void KVStore::generateSST(const std::vector<ele> &eleArr, int level) {
    sstable ss(level);
    for(ele it : eleArr) { 
        if(ss.checkSize(it.value, level)) {
            ss.addNewSst(level);
            addsstable(ss, level);
            ss.reset();
        }
        ss.insert(it.key, it.value);
    }
    if(ss.getCnt()) {
        ss.addNewSst(level);
        addsstable(ss, level);
    }
}


void KVStore::compaction() {
    for(int level = 0; level <= totalLevel; ++level) {
        if(sstableIndex[level].size() <= maxLimit(level)) {
            break;
        }
        uint64_t minKey = INF, maxKey = -1;
        std::vector<ele> eleArr;

        if(level == totalLevel) {
            ++totalLevel;
            std::string path = "./data/level-" + std::to_string(totalLevel);
            utils::mkdir(path.data());
        }

        if(level == 0) {
            for (auto it = sstableIndex[0].begin(); it != sstableIndex[0].end();) {
                minKey = std::min(minKey, it->getMinV());
                maxKey = std::max(maxKey, it->getMaxV());
                sstable ss;
                ss.loadFile(it->getFilename().data());
                for(int i = 0; i < ss.getCnt(); ++i) {
                    ele e(ss.getKey(i), ss.getData(i), ss.getTime(), 0);
                    eleArr.emplace_back(e);
                }
                it = delsstable(it->getFilename());
            }
        }
        else {
            for(int i = 0; i < sstableIndex[level].size() - maxLimit(level); ++i) {
                auto it = findMin(sstableIndex[level]);
                minKey = std::min(minKey, it->getMinV());
                maxKey = std::max(maxKey, it->getMaxV());
                sstable ss;
                ss.loadFile(it->getFilename().data());
                for(int i = 0; i < ss.getCnt(); ++i) {
                    ele e(ss.getKey(i), ss.getData(i), ss.getTime(), level);
                    eleArr.emplace_back(e);
                }
                delsstable(it->getFilename());
            }
        }

        for (auto it = sstableIndex[level+1].begin(); it != sstableIndex[level+1].end();) {
            if(!(it->getMaxV() < minKey || it->getMinV() > maxKey)) {
                sstable ss;
                ss.loadFile(it->getFilename().data());
                for(int i = 0; i < ss.getCnt(); ++i) {
                    ele e(ss.getKey(i), ss.getData(i), ss.getTime(), level+1);
                    eleArr.emplace_back(e);
                }
                it = delsstable(it->getFilename());
            }
            else {
                ++it;
            }
        }

        //printf("finish read\n");
        std::vector<ele> tmpArr(eleArr.size());
        MergeSort(eleArr, tmpArr, 0, eleArr.size() - 1);
        //printf("finish sort\n");
        removeDup(eleArr);
        //printf("finish removeDup\n");
        if(level + 1 == totalLevel) {
            removeDel(eleArr);
        }
        //printf("finish removeDel\n");
    
        generateSST(eleArr, level + 1);
        //printf("finish generateSST\n");
    }
}

std::vector<sstablehead>::iterator KVStore::delsstable(std::string filename) {
    std::vector<sstablehead>::iterator it;
    for (int level = 0; level <= totalLevel; ++level) {
        int size = sstableIndex[level].size(), flag = 0;
        for (int i = 0; i < size; ++i) {
            if (sstableIndex[level][i].getFilename() == filename) {
                it = sstableIndex[level].erase(sstableIndex[level].begin() + i);
                flag = 1;
                break;
            }
        }
        if (flag)
            break;
    }
    int flag = utils::rmfile(filename.data());
    if (flag != 0) {
        std::cout << "delete fail!" << std::endl;
        std::cout << strerror(errno) << std::endl;
    }
    return it;
}

void KVStore::addsstable(sstable ss, int level) {
    sstableIndex[level].push_back(ss.getHead());
}

char strBuf[2097152];

/**
 * @brief Fetches a substring from a file starting at a given offset.
 *
 * This function opens a file in binary read mode, seeks to the specified start offset,
 * reads a specified number of bytes into a buffer, and returns the buffer as a string.
 *
 * @param file The path to the file from which to read the substring.
 * @param startOffset The offset in the file from which to start reading.
 * @param len The number of bytes to read from the file.
 * @return A string containing the read bytes.
 */
std::string KVStore::fetchString(std::string file, int startOffset, uint32_t len) {
    FILE *fp = fopen(file.data(), "rb");
    if (fp == nullptr) {
        std::cerr << "Error: Unable to open file " << file << std::endl;
        return "";
    }
    fseek(fp, startOffset, SEEK_SET);
    fread(strBuf, 1, len, fp);
    fclose(fp);
    return std::string(strBuf, len);
}

std::vector<std::pair<std::uint64_t, std::string>> KVStore::search_knn(std::string query, int k) {
    
}
