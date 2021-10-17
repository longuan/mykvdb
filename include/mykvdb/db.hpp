

#pragma once

#include <cstdint>
#include <vector>

#include "leveldb/skiplist.hpp"

const int kItemLen = 12; // 每个k-v item是12个字节（int64_t + uint32_t）

struct KVItem {
    int64_t key;
    uint32_t value;
};

class KeyComparator {
public:
    int operator()(KVItem* const& a, KVItem* const& b) const {
        if(a->key == b->key) {
            return 0;
        } else if (a->key > b->key) {
            return 1;
        } else {
            return -1;
        }
    }
};

class DB {

public:

    DB(const KeyComparator& cmp) ;
    ~DB() ;

    DB(const DB&) =delete;
    DB& operator=(const DB&) =delete;

    bool insert(int64_t key, uint32_t value);
    bool update(int64_t key, uint32_t value);
    bool del(int64_t key);
    uint32_t get(int64_t key);
    std::vector<uint32_t> range(int64_t begin, int64_t end);

private:
    // 因为key和value都是整型，因此每条key-value所占用内存都是固定的。
    // item_cnt_用来记录存储的k-v数量，一次得到内存占用量
    int item_cnt_;

    // 只支持key为int64_t类型，value是uint32_t类型
    typedef SkipList<KVItem*, KeyComparator> internalDB;
    internalDB datas_;

    Arena arena_;

};

