#include "skiplist.h"

#include <iostream>
#include <algorithm>
#include <random>

double skiplist::my_rand() {
    static std::default_random_engine e;
    static std::uniform_real_distribution<double> u(0, 1);
    return u(e);
}

int skiplist::randLevel() {
    int level = 1;
    while (my_rand() < p && level < MAX_LEVEL)
        level++;
    return level;
}

void skiplist::insert(uint64_t key, const std::string &str) {
    slnode *update[MAX_LEVEL];
    slnode *cur = head;
    for (int i = curMaxL - 1; i >= 0; --i) {
        while (cur->nxt[i]->key < key)
            cur = cur->nxt[i];
        update[i] = cur;
    }

    if (cur->nxt[0]->key == key) {
        cur->nxt[0]->val = str;
        return;
    }

    int level = randLevel();
    if (level > curMaxL) {
        for (int i = curMaxL; i < level; ++i)
            update[i] = head;
        curMaxL = level;
    }
    slnode *nnode = new slnode(key, str, NORMAL);
    for (int i = 0; i < level; ++i) {
        nnode->nxt[i] = update[i]->nxt[i];
        update[i]->nxt[i] = nnode;
    }

    bytes += 12;            // Index
    bytes += str.length();  // Data
}

std::string skiplist::search(uint64_t key) {
    slnode *cur = lowerBound(key);
    if (cur->key == key) {
        return cur->val;
    }
    return "";
}

bool skiplist::del(uint64_t key) {
    slnode *update[MAX_LEVEL];
    slnode *cur = head;
    for (int i = curMaxL - 1; i >= 0; --i) {
        while (cur->nxt[i]->key < key)
            cur = cur->nxt[i];
        update[i] = cur;
    }

    if (cur->nxt[0]->key != key)
        return false;

    slnode *delNode = cur->nxt[0];
    bytes -= 12;                        // Index
    bytes -= delNode->val.length();     // Data
    for (int i = 0; i < curMaxL; ++i) {
        if (update[i]->nxt[i] == delNode)
            update[i]->nxt[i] = delNode->nxt[i];
    }
    delete delNode;
    while (curMaxL > 1 && head->nxt[curMaxL - 1] == tail)
        curMaxL--;

    insert(key, DEL);
    
    return true;
}

void skiplist::scan(uint64_t key1, uint64_t key2, std::vector<std::pair<uint64_t, std::string>> &list) {
    slnode *cur1 = lowerBound(key1);
    slnode *cur2 = lowerBound(key2);
    if(cur2->key == key2) {
        cur2 = cur2->nxt[0];
    } 

    while (cur1 != cur2) {
        list.push_back(std::make_pair(cur1->key, cur1->val));
        cur1 = cur1->nxt[0];
    }
}

slnode* skiplist::lowerBound(uint64_t key) {
    slnode *cur = head;
    for (int i = curMaxL - 1; i >= 0; --i) {
        while (cur->nxt[i]->key < key)
            cur = cur->nxt[i];
    }
    return cur->nxt[0];
}

void skiplist::reset() {
    slnode *cur = head->nxt[0];
    while (cur != tail) {
        slnode *tmp = cur;
        cur = cur->nxt[0];
        delete tmp;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
        head->nxt[i] = tail;
    curMaxL = 1;
    bytes = 0;
}

uint32_t skiplist::getBytes() {
    return bytes;
}