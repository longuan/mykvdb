#include <chrono>
#include <iostream>
#include <thread>
#include "mykvdb/db.hpp"

void TestSingleThread() {
  KeyComparator cmp;
  DB db = DB(cmp);

  // test for insert
  assert(db.insert(1, 1) == true);
  assert(db.insert(3, 3) == true);
  assert(db.get(1) == 1);
  assert(db.get(2) == 0);
  assert(db.get(3) == 3);

  // test for update
  assert(db.update(1, 3) == true);
  assert(db.get(1) == 3);
  assert(db.update(1, 4) == true);
  assert(db.get(1) == 4);
  assert(db.update(3, 33) == true);
  assert(db.get(3) == 33);
  assert(db.update(3, 34) == true);
  assert(db.get(3) == 34);

  // test for delete
  assert(db.del(1) == true);
  assert(db.get(1) == 0);
  assert(db.del(3) == true);
  assert(db.get(3) == 0);

  // test for range
  for (int i = 100; i < 10000; i++) {
    if (i % 4 == 0) {
      assert(db.insert(i, i) == true);
    }
  }
  std::vector<uint32_t> range1 = db.range(100, 110);
  assert(range1.size() == 3);
  assert(range1[0] == 100);
  assert(range1[1] == 104);
  assert(range1[2] == 108);
  std::vector<uint32_t> range2 = db.range(101, 10001);
  assert(range2.size() == 2474);
  assert(range2[0] == 104);
  assert(range2[range2.size() - 1] == 9996);
  assert(db.del(100) == true);
  assert(db.del(9996) == true);
  std::vector<uint32_t> range3 = db.range(100, 10000);
  assert(range3.size() == 2473);
  assert(range3[0] == 104);
  assert(range3[range3.size() - 1] == 9992);

  // other test
  assert(db.get(4) == 0);
  assert(db.update(4, 4) == false);
  assert(db.del(4) == false);
  assert(db.get(4) == 0);
}

// 每秒开始时所有insertThread都尝试去insert当前的unix
// timestamp，理论上只有一个thread会插入成功，其他所有thread都会插入失败
// 如果两个insertThread都insert了同一个unix
// timestamp，skiplist.hpp:343行的assert会失败
void insertThread(int threadIndex, DB& db) {
  auto nowTime = std::chrono::system_clock::now();

  for (; nowTime < nowTime + std::chrono::seconds(100);) {
    auto nowTime = std::chrono::system_clock::now();
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(
        nowTime.time_since_epoch());
    auto unixTS = secs.count();
    if (unixTS % 1 == 0) {
      if (db.insert(unixTS, 33) == true) {
        std::cout << "Thread " << threadIndex << " insert success " << unixTS
                  << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
}

void TestMultiThreadInsert() {
  KeyComparator cmp;
  DB db = DB(cmp);

  std::vector<std::thread> thread_group;
  for (int i = 0; i < 15; i++) {
    auto t = std::thread(insertThread, i, std::ref(db));
    thread_group.push_back(std::move(t));
  }

  for (auto& t : thread_group) {
    t.join();
  }
}

int main() {
  std::cout << "start to test database using single thread" << std::endl;
  TestSingleThread();
  std::cout << "test database using single thread ALL PASS" << std::endl;

  std::cout << "start to test database using multi thread" << std::endl;
  TestMultiThreadInsert();
  std::cout << "test database using multi thread ALL PASS" << std::endl;
  return 0;
}
