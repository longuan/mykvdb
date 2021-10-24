#include <unistd.h>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include "leveldb/random.hpp"
#include "mykvdb/db.hpp"

KeyComparator cmp;
DB db = DB(cmp);

struct CURDResult {
  int insert_num;
  int update_num;
  int delete_num;
  int get_num;
  int range_num;
};

std::vector<struct CURDResult*> threads_curd_result;

void PrintUsage() {
  std::cout << "Usage: ./mykvdb -t <thread_num> -i <insert ratio> -u <update "
               "ratio> -d <delete ratio> -g <get ratio> -r <range ratio> -l "
               "<log_interval_time (seconds)>"
            << std::endl;
}

struct CURDRatio {
  int insert_ratio;
  int update_ratio;
  int delete_ratio;
  int get_ratio;
  int range_ratio;
};

int CURDFunc(int thread_index, struct CURDRatio r) {
  LCG value_generator(
      pow(2, 31), 1103515245, 12345,
      2021 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      1100000000);
  LCG curd_generator(
      pow(2, 31), 1103515245, 12345,
      1202 + std::hash<std::thread::id>()(std::this_thread::get_id()), 10);
  LCG key_generator(
      pow(2, 31), 1103515245, 12345,
      1001 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      20000000);

  for (;;) {
    int operation = curd_generator.Next();

    if (operation < r.insert_ratio) {
      auto k = key_generator.Next();
      db.insert(k, value_generator.Next());
      threads_curd_result[thread_index]->insert_num++;
      continue;
    }
    operation -= r.insert_ratio;
    if (operation < r.update_ratio) {
      db.update(key_generator.Next(), value_generator.Next());
      threads_curd_result[thread_index]->update_num++;
      continue;
    }
    operation -= r.update_ratio;
    if (operation < r.delete_ratio) {
      db.del(key_generator.Next());
      threads_curd_result[thread_index]->delete_num++;
      continue;
    }
    operation -= r.delete_ratio;
    if (operation < r.get_ratio) {
      db.get(key_generator.Next());
      threads_curd_result[thread_index]->get_num++;
      continue;
    }
    operation -= r.get_ratio;
    if (operation < r.range_ratio) {
      int64_t begin = key_generator.Next();
      int64_t end = key_generator.Next();
      if (begin >= end) {
        db.range(end, begin);
      } else {
        db.range(begin, end);
      }
      threads_curd_result[thread_index]->range_num++;
      continue;
    }
  }
  return 0;
}

std::vector<std::thread> GetThreadGroup(int thread_num, struct CURDRatio r) {
  std::vector<std::thread> thread_group;
  for (int i = 0; i < thread_num; i++) {
    struct CURDResult* result = new (struct CURDResult);
    result->insert_num = 0;
    result->update_num = 0;
    result->delete_num = 0;
    result->get_num = 0;
    result->range_num = 0;
    threads_curd_result.push_back(result);
    std::thread t = std::thread(CURDFunc, i, r);
    thread_group.push_back(std::move(t));
  }
  return thread_group;
}

int main(int argc, char* argv[]) {
  int opt;
  const char* opt_string = "t:i:u:d:g:r:l:";
  int thread_num, insert_ratio, update_ratio, delete_ratio, get_ratio,
      range_ratio, log_interval = 0;
  while ((opt = getopt(argc, argv, opt_string)) != -1) {
    switch (opt) {
      case 't':
        thread_num = std::stoi(optarg);
        if (thread_num <= 0) {
          std::cout << "thread num cannot less than 0" << std::endl;
        }
        break;
      case 'i':
        insert_ratio = std::stoi(optarg);
        break;
      case 'u':
        update_ratio = std::stoi(optarg);
        break;
      case 'd':
        delete_ratio = std::stoi(optarg);
        break;
      case 'g':
        get_ratio = std::stoi(optarg);
        break;
      case 'r':
        range_ratio = std::stoi(optarg);
        break;
      case 'l':
        log_interval = std::stoi(optarg);
        if (log_interval == 0) {
          std::cout << "log_interval cannot be 0" << std::endl;
        }
        break;
      default:
        PrintUsage();
        return -1;
    }
  }

  if ((insert_ratio + update_ratio + delete_ratio + get_ratio + range_ratio) !=
      10) {
    std::cout << "the sum of ratio should equal to 10" << std::endl;
    PrintUsage();
    return -1;
  }

  if (log_interval == 0) {
    std::cout << "the default of log interval is 1s" << std::endl;
    log_interval = 1;
  }

  // 预先插入2KW条数据
  std::cout << "first insert 2KW record into database, key is [1, 20000000]"
            << std::endl;
  LCG value_generator(
      pow(2, 31), 1103515245, 12345,
      2021 + std::hash<std::thread::id>()(std::this_thread::get_id()),
      20000000);

  for (int i = 1; i <= 20000000; i++) {
    db.insert(i, value_generator.Next());
  }

  struct CURDRatio ratio = CURDRatio{insert_ratio, update_ratio, delete_ratio,
                                     get_ratio, range_ratio};

  std::vector<std::thread> thread_group = GetThreadGroup(thread_num, ratio);

  std::vector<struct CURDResult> prev_curd_result;
  for (int i = 0; i < thread_group.size(); i++) {
    auto r = CURDResult{0, 0, 0, 0, 0};
    prev_curd_result.push_back(r);
  }
  for (;;) {
    printf(
        "thread index  insert num  update num  delete num  get num  range num "
        "\n");
    for (int i = 0; i < threads_curd_result.size(); i++) {
      auto this_thread_curd = threads_curd_result[i];
      auto& prev_interval_curd = prev_curd_result[i];
      printf("%12d %10d %10d %10d %10d %9d\n", i, this_thread_curd->insert_num,
             this_thread_curd->update_num, this_thread_curd->delete_num,
             this_thread_curd->get_num, this_thread_curd->range_num);
      printf("%12d %8d/s %8d/s %8d/s %8d/s %7d/s\n", i,
             (this_thread_curd->insert_num - prev_interval_curd.insert_num) /
                 log_interval,
             (this_thread_curd->update_num - prev_interval_curd.update_num) /
                 log_interval,
             (this_thread_curd->delete_num - prev_interval_curd.delete_num) /
                 log_interval,
             (this_thread_curd->get_num - prev_interval_curd.get_num) /
                 log_interval,
             (this_thread_curd->range_num - prev_interval_curd.range_num) /
                 log_interval);
      prev_interval_curd.insert_num = this_thread_curd->insert_num;
      prev_interval_curd.update_num = this_thread_curd->update_num;
      prev_interval_curd.delete_num = this_thread_curd->delete_num;
      prev_interval_curd.get_num = this_thread_curd->get_num;
      prev_interval_curd.range_num = this_thread_curd->range_num;
    }
    std::this_thread::sleep_for(std::chrono::seconds(log_interval));
  }

  return 0;
}
