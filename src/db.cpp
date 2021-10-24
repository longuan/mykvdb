
#include "mykvdb/db.hpp"

DB::DB(const KeyComparator& cmp)
    : item_cnt_(0), comparator_(cmp), datas_(cmp, &arena_) {}

DB::~DB() {}

KVItem* DB::FindEqual(int64_t key) {
  KVItem* item = FindGreaterOrEqual(key);
  if (item == nullptr) {
    return nullptr;
  } else if (item->key != key) {
    return nullptr;
  } else {
    return item;
  }
}

KVItem* DB::FindGreaterOrEqual(int64_t key) {
  internalDB::Iterator iter(&datas_);
  KVItem tmpKVItem = KVItem(key);
  tmpKVItem.key = key;
  iter.Seek(&tmpKVItem);

  if (iter.Valid()) {
    KVItem* entry = const_cast<KVItem*>(iter.key());
    if (entry->del_flag == 0) {
      return entry;
    } else {
      // entry is in the deletion status
      return nullptr;
    }
  } else {
    return nullptr;
  }
}

bool DB::insert(int64_t key, uint32_t value) {
  std::unique_lock<std::shared_mutex> lck(rw_mutex_);
  if (FindEqual(key) != nullptr) {
    // already have this key
    return false;
  } else {
    KVItem* item = (KVItem*)arena_.Allocate(sizeof(KVItem));
    item->key = key;
    item->value = value;
    item->del_flag = 0;
    if ((item_cnt_ + 1) * kItemDataLen > kMaxItemCount) {
      return false;
    }
    datas_.Insert(item);
    item_cnt_++;
    return true;
  }
}

uint32_t DB::get(int64_t key) {
  std::shared_lock<std::shared_mutex> lck(rw_mutex_);
  KVItem* target = FindEqual(key);
  if (target == nullptr) {
    // key is not exist or mark as delete
    return 0;
  } else {
    return target->value;
  }
}

bool DB::update(int64_t key, uint32_t value) {
  std::unique_lock<std::shared_mutex> lck(rw_mutex_);
  KVItem* target = FindEqual(key);
  if (target == nullptr) {
    // key is not exist or mark as delete
    return false;
  } else {
    target->value = value;
    return true;
  }
};

bool DB::del(int64_t key) {
  std::unique_lock<std::shared_mutex> lck(rw_mutex_);
  KVItem* target = FindEqual(key);
  if (target == nullptr) {
    // key is not exist or mark as delete
    return false;
  } else {
    target->del_flag = true;
    item_cnt_--;
    return true;
  }
};

std::vector<uint32_t> DB::range(int64_t begin, int64_t end) {
  std::shared_lock<std::shared_mutex> lck(rw_mutex_);

  internalDB::Iterator iter(&datas_);
  KVItem beginKVItem = KVItem(begin);
  iter.Seek(&beginKVItem);

  std::vector<uint32_t> rangeResult = {};
  if (iter.Valid()) {
    for (; iter.Valid(); iter.Next()) {
      KVItem* entry = const_cast<KVItem*>(iter.key());
      if (entry->key > end) {
        break;
      } else {
        if (entry->del_flag == 0) {
          rangeResult.push_back(entry->value);
        }
      }
    }
  }

  return rangeResult;
};
