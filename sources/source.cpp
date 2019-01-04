#include <db.hpp>

#include <ThreadPool.h>
#include <picosha2.h>

#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/trivial.hpp>

namespace logging = boost::log;

std::vector<ColumnFamilyHandle*> kv_storage::open_new_bd() {
    Options options;
    options.create_if_missing = true;
    Status s = DB::Open(options, kDBPath_write, &db);
    if (!s.ok()) {
        if (log_level == "warning")
            BOOST_LOG_TRIVIAL(warning) << "something wrong in opening new DB" << std::endl;
        if (log_level == "info")
            BOOST_LOG_TRIVIAL(info) << "something wrong in opening new DB" << std::endl;
        if (log_level == "error")
            BOOST_LOG_TRIVIAL(error) << "something wrong in opening new DB" << std::endl;
    }
    std::vector<ColumnFamilyHandle*> col_handles;
    for (unsigned i = 1; i < names_column.size(); i++) {
        ColumnFamilyHandle* cf1;
        col_handles.push_back(cf1);
    }
    for (unsigned i = 1; i < names_column.size(); i++)
        s = db->CreateColumnFamily(ColumnFamilyOptions(), names_column[i], &col_handles[i-1]);
    if (!s.ok()) {
        if (log_level == "warning")
            BOOST_LOG_TRIVIAL(warning) << "something wrong in creating column family" << std::endl;
        if (log_level == "info")
            BOOST_LOG_TRIVIAL(info) << "something wrong in creating column family" << std::endl;
        if (log_level == "error")
            BOOST_LOG_TRIVIAL(error) << "something wrong in creating column family" << std::endl;
    }

    for (unsigned i = 1; i < names_column.size(); i++) {
        delete col_handles[i-1];
    }
    delete db;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(ColumnFamilyDescriptor(
                                  kDefaultColumnFamilyName, ColumnFamilyOptions()));
    for (unsigned i = 1; i < names_column.size(); i++)
        column_families.push_back(ColumnFamilyDescriptor(
                                      names_column[i], ColumnFamilyOptions()));
    std::vector<ColumnFamilyHandle*> handles;
    s = DB::Open(DBOptions(), kDBPath_write, column_families, &handles, &db);
    if (!s.ok()) {
        if (log_level == "warning")
            BOOST_LOG_TRIVIAL(warning) << "something wrong in writing into DB" << std::endl;
        if (log_level == "info")
            BOOST_LOG_TRIVIAL(info) << "something wrong in writing into DB" << std::endl;
        if (log_level == "error")
            BOOST_LOG_TRIVIAL(error) << "something wrong in writing into DB" << std::endl;
    }

    return handles;
}


std::vector<Iterator*> kv_storage::open_old_bd() {
    DBOptions options;
    DB* db_old;
    Status s = DB::ListColumnFamilies(options, kDBPath_read, &names_column);
    if (!s.ok()) {
        if (log_level == "warning")
            BOOST_LOG_TRIVIAL(warning) << "something wrong in opening old DB" << std::endl;
        if (log_level == "info")
            BOOST_LOG_TRIVIAL(info) << "something wrong in opening old DB" << std::endl;
        if (log_level == "error")
            BOOST_LOG_TRIVIAL(error) << "something wrong in opening old DB" << std::endl;
    }
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.push_back(ColumnFamilyDescriptor(
                                  kDefaultColumnFamilyName, ColumnFamilyOptions()));
    for (unsigned i = 1; i < names_column.size(); i++)
        column_families.push_back(ColumnFamilyDescriptor(
                                      names_column[i], ColumnFamilyOptions()));
    std::vector<ColumnFamilyHandle*> handles;
    s = DB::OpenForReadOnly(DBOptions(), kDBPath_read, column_families, &handles, &db_old);
    if (!s.ok()) {
        if (log_level == "warning")
            BOOST_LOG_TRIVIAL(warning) << "something wrong in reading from old DB" << std::endl;
        if (log_level == "info")
            BOOST_LOG_TRIVIAL(info) << "something wrong in reading from old DB" << std::endl;
        if (log_level == "error")
            BOOST_LOG_TRIVIAL(error) << "something wrong in reading from old DB" << std::endl;
    }
    std::vector<Iterator*> iters;
    for (unsigned i = 0; i < handles.size(); i++) {
        iters.push_back(db_old->NewIterator(ReadOptions()));
    }
    s = db_old->NewIterators(ReadOptions(), handles, &iters);
    return iters;
}

void kv_storage::make_process() {
    logging::add_file_log("logs.log");
    std::vector<Iterator*> iters = open_old_bd();
    if (thread_count == 0) thread_count = iters.size();
    ThreadPool pool_reader(thread_count);
    ThreadPool pool_writer(thread_count);
    read_column = iters.size();
    std::vector<ColumnFamilyHandle*> handles = open_new_bd();
    for (unsigned i = 0; i < iters.size(); i++) {
        pool_reader.enqueue( &kv_storage::reader_thread, this, iters[i], i);
        if (done!=true) pool_writer.enqueue(&kv_storage::writer_thread, this, handles);
    }
    while (done != true) {
        pool_writer.enqueue(&kv_storage::writer_thread, this, handles);
    }
}

void kv_storage::reader_thread(Iterator* it, unsigned k) {
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        m.lock();
        start_data_queue.push({it->key().ToString(), it->value().ToString()});
        column_number.push(k);
        std::cout << it->key().ToString()<< ":" << it->value().ToString()<< std::endl;
        read = true;
        m.unlock();
        cv.notify_one();
    }
    m.lock();
    read_column--;
    if (read_column == 0) cv.notify_all();
    m.unlock();
}

std::string kv_storage::make_ssh(std::pair<std::string, std::string> pair) {
    std::string src_str = pair.first + pair.second;
    std::string hash_hex_str = picosha2::hash256_hex_string(src_str);
    return hash_hex_str;
}

void kv_storage::set_log_level (std::string input) {
    if ((input == "warning") || (input == "info"))
        log_level = input;
}

void kv_storage::set_thread_count (unsigned input) {
    if (input != 0)
        thread_count  = input;
}

void kv_storage::set_path_write (std::string input) {
    if (input != "")
        kDBPath_write  = input;
}

void kv_storage::writer_thread(std::vector<ColumnFamilyHandle*> handles) {
    std::unique_lock<std::recursive_mutex> lk(m);
    while ((read_column != 0) && (read == false))
    {
        cv.wait(lk);
    }
    if ((start_data_queue.size() == 0) && (read_column == 0)) {
        read = false;
        done = true;
        return;
    }
    if (start_data_queue.size() == 0) {
        read = false;
        return;
    }
    read = false;
    std::string new_value = make_ssh(start_data_queue.front());
    WriteBatch batch;
    batch.Put(handles[column_number.front()], Slice(start_data_queue.front().first),
              Slice(new_value));
    Status s = db->Write(WriteOptions(), &batch);
    assert(s.ok());
    start_data_queue.pop();
    column_number.pop();
}

