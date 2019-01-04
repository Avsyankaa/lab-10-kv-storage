
// Copyright 2018 Avsyankaa <Avsyankaa@gmail.com> 

#ifndef INCLUDE_DB_HPP_
#define INCLUDE_DB_HPP_
#include <cstdio>
#include <string>
#include <queue>
#include <utility>
#include <mutex>
#include <condition_variable>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

class kv_storage {
private:
    std::recursive_mutex m;
    std::condition_variable_any cv;
    unsigned read_column;
    bool done;
    bool read;
    DB* db;
    std::queue<std::pair<std::string,std::string>> start_data_queue;
    std::queue<unsigned> column_number;
    std::string kDBPath_read;
    std::string kDBPath_write;
    std::vector<std::string> names_column;
    std::string log_level;
    unsigned thread_count;
    std::vector<Iterator*> open_old_bd();
    std::vector<ColumnFamilyHandle*> open_new_bd();
    void reader_thread(Iterator* it, unsigned k);
    std::string make_ssh(std::pair<std::string, std::string> pair);
    void writer_thread(std::vector<ColumnFamilyHandle*> handles);

public:
    kv_storage(): read_column(100),
        done(false),
        read(false),
        kDBPath_read ("/home/vagrant/Projects/lab-10-kv-storage/tmp"),
        kDBPath_write ("/home/vagrant/Projects/lab-10-kv-storage/hash"),
        log_level ("error"),
        thread_count(0)
    {}
    void make_process();
    void set_log_level (std::string input);
    void set_thread_count (unsigned input);
    void set_path_write (std::string input);
};

#endif // INCLUDE_DB_HPP_
