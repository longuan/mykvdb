
#include <iostream>
#include <cstring>
#include "mykvdb/db.hpp"

DB::DB(const KeyComparator& cmp): item_cnt_(0), datas_(cmp, &arena_) {}

DB::~DB() {}

bool DB::insert(int64_t key, uint32_t value) {
    KVItem* item = (KVItem *)arena_.Allocate(kItemLen);
    item->key = key;
    item->value = value;
    datas_.Insert(item);
    return true;
}

uint32_t DB::get(int64_t key) {
    internalDB::Iterator iter(&datas_);
    KVItem tmpKVItem = KVItem{key, 0};
    iter.Seek(&tmpKVItem);

    if (iter.Valid()) {
        KVItem *const entry = iter.key();
        return entry->value;
    }
    return 0;
}

bool DB::update(int64_t key, uint32_t value) {return true;};
bool DB::del(int64_t key) {return true;};
std::vector<uint32_t> DB::range(int64_t begin, int64_t end) {return std::vector<uint32_t>{};};


int main(void) {
    KeyComparator cmp;
    DB db = DB(cmp);
    
    std::cout << std::boolalpha << db.insert(1, 1) << std::endl;
    std::cout << std::boolalpha << db.insert(2, 2) << std::endl;
    std::cout << db.get(1) << std::endl;
    std::cout << db.get(2) << std::endl;
    std::cout << db.get(3) << std::endl;

    return 0;
}
