
// Copyright 2018 Avsyankaa <Avsyankaa@gmail.com>

#include <iostream>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;
std::string kDBPath_read = "/home/vagrant/Projects/lab-10-kv-storage/tmp";
int main() {
    Options options;
    options.create_if_missing = true;
    DB* db;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    ColumnFamilyHandle* cf1;
    ColumnFamilyHandle* cf2;
    s = db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf1", &cf1);
    s = db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf2", &cf2);
    assert(s.ok());
    delete cf1;
    delete cf2;
    delete db;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(ColumnFamilyDescriptor(
                                  kDefaultColumnFamilyName, ColumnFamilyOptions()));
    column_families.push_back(ColumnFamilyDescriptor(
                                  "new_cf1", ColumnFamilyOptions()));
    column_families.push_back(ColumnFamilyDescriptor(
                                  "new_cf2", ColumnFamilyOptions()));
    std::vector<ColumnFamilyHandle*> handles;
    s = DB::Open(DBOptions(), kDBPath, column_families, &handles, &db);
    assert(s.ok());
    WriteBatch batch;
    batch.Put(handles[0], Slice("key1"), Slice("value1"));
    batch.Put(handles[0], Slice("key2"), Slice("value2"));
    batch.Put(handles[0], Slice("key3"), Slice("value3"));
    batch.Put(handles[1], Slice("key4"), Slice("value4"));
    batch.Put(handles[1], Slice("key5"), Slice("value5"));
    batch.Put(handles[2], Slice("key6"), Slice("value6"));
    batch.Put(handles[2], Slice("key7"), Slice("value7"));
    batch.Put(handles[2], Slice("key8"), Slice("value8"));
    batch.Put(handles[2], Slice("key9"), Slice("value9"));
    s = db->Write(WriteOptions(), &batch);
    assert(s.ok());

    for (auto handle : handles) {

        delete handle;

    }

    delete db;
    return 0;
}
