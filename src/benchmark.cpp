
#include <chrono>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <thread>
#include "leveldb/random.hpp"
#include "mykvdb/db.hpp"

KeyComparator cmp;
DB db = DB(cmp);

// 插入两千万行记录
std::chrono::milliseconds InsertBenchmark() {
  LCG value_generator(
      pow(2, 31), 1103515245, 12345,
      2021 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      20000000);

  auto start_time = std::chrono::system_clock::now();
  for (int i = 1; i <= 20000000; i++) {
    db.insert(i, value_generator.Next());
  }
  auto end_time = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
  return duration;
}

// 查询2KW次
void GetFunc() {
  LCG key_generator(
      pow(2, 31), 1103515245, 12345,
      1001 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      20000000);

  for (int i = 0; i < 20000000; i++) {
    db.get(key_generator.Next());
  }
}

// 范围查询2KW次
void RangeFunc() {
  LCG key_generator(
      pow(2, 31), 1103515245, 12345,
      1001 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      20000000);

  for (int i = 0; i < 20000000; i++) {
    int64_t begin = key_generator.Next();
    int64_t end = key_generator.Next();
    if (begin >= end) {
      db.range(end, begin);
    } else {
      db.range(begin, end);
    }
  }
}

std::chrono::milliseconds GetBenchmark(int thread_num, bool is_range) {
  std::vector<std::thread> thread_group;

  auto start_time = std::chrono::system_clock::now();

  for (int i = 0; i < thread_num; i++) {
    if (is_range) {
      thread_group.push_back(std::thread(RangeFunc));

    } else {
      thread_group.push_back(std::thread(GetFunc));
    }
  }
  for (auto& t : thread_group) {
    t.join();
  }
  auto end_time = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
  return duration;
}

int main(void) {
  auto insert_time = InsertBenchmark();
  std::cout << "insert 2KW record: " << insert_time.count() << "ms"
            << std::endl;

  auto get1_time = GetBenchmark(1, false);
  std::cout << "get 2KW record using 1 thread: " << get1_time.count() << "ms"
            << std::endl;

  auto get4_time = GetBenchmark(4, false);
  std::cout << "get 8KW record using 4 thread: " << get4_time.count() << "ms"
            << std::endl;

  auto get8_time = GetBenchmark(8, false);
  std::cout << "get 16KW record using 8 thread: " << get8_time.count() << "ms"
            << std::endl;

  auto get16_time = GetBenchmark(16, false);
  std::cout << "get 32KW record using 16 thread: " << get16_time.count() << "ms"
            << std::endl;

  auto range1_time = GetBenchmark(1, true);
  std::cout << "range 2KW record using 1 thread: " << range1_time.count()
            << "ms" << std::endl;

  auto range4_time = GetBenchmark(4, true);
  std::cout << "range 8KW record using 4 thread: " << range4_time.count()
            << "ms" << std::endl;

  auto range8_time = GetBenchmark(8, true);
  std::cout << "range 16KW record using 8 thread: " << range8_time.count()
            << "ms" << std::endl;

  auto range16_time = GetBenchmark(16, true);
  std::cout << "range 32KW record using 16 thread: " << range16_time.count()
            << "ms" << std::endl;
}