// Copyright 2018 Avsyankaa <Avsyankaa@gmail.com>

#include <gtest/gtest.h>

#include <db.hpp>

TEST(Kv_storage, Test1) {
    kv_storage kv;
    kv.set_log_level("warning");
    kv.set_thread_count(4);
    kv.set_path_write("/home/vagrant/Projects/lab-10-kv-storage/hash");
    kv.make_process();
    SUCCEED();
}
