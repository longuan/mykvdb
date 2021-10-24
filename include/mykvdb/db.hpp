

#pragma once

#include <cstdint>
#include <shared_mutex>
#include <vector>

#include "leveldb/skiplist.hpp"

const uint64_t kItemDataLen =
    12;  // 每个item中data是12个字节（int64_t + uint32_t）
const uint64_t kMaxItemCount = 357913941;  // 4*1024*1024*1024 4 GB

struct KVItem {
  explicit KVItem(int64_t k, uint32_t v = 0) : key(k), value(v), del_flag(0){};
  int64_t key;
  uint32_t value;

  // 用于存储item的元信息，目前只有一个del_flag，用来表示此item是否被删除
  uint32_t : 31;
  uint32_t del_flag : 1;
};

class KeyComparator {
 public:
  int operator()(KVItem* const& a, KVItem* const& b) const {
    if (a->key == b->key) {
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
  explicit DB(const KeyComparator& cmp);
  ~DB();

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  bool insert(int64_t key, uint32_t value);
  bool update(int64_t key, uint32_t value);
  bool del(int64_t key);
  uint32_t get(int64_t key);
  std::vector<uint32_t> range(int64_t begin, int64_t end);

 private:
  // 因为key和value都是整型，因此每条key-value所占用内存都是固定的。
  // item_cnt_用来记录存储的k-v数量，一次得到内存占用量
  int item_cnt_;
  std::shared_mutex rw_mutex_;

  Arena arena_;

  KeyComparator comparator_;

  // 只支持key为int64_t类型，value是uint32_t类型
  typedef SkipList<KVItem*, KeyComparator> internalDB;
  internalDB datas_;

  KVItem* FindEqual(int64_t key);
  KVItem* FindGreaterOrEqual(int64_t key);
};
