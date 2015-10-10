/*
 * rocksdb_engine.cpp
 *
 *  Created on: 2015Äê9ÔÂ21ÈÕ
 *      Author: wangqiying
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"

OP_NAMESPACE_BEGIN
    rocksdb::ColumnFamilyHandle* RocksDBEngine::GetDB(Context& ctx, const KeyObject& key, bool create_if_notexist)
    {
        if (ctx.cf != NULL)
        {
            return ctx.cf;
        }
        const char* ns = key.ns;
        uint32 db = key.db;
        rocksdb::ColumnFamilyOptions cf_options(m_options);
        std::string name;
        rocksdb::ColumnFamilyHandle* cfh = NULL;
        rocksdb::Status ret = m_db->CreateColumnFamily(cf_options, name, &cfh);
        if (NULL != cfh)
        {
            return cfh;
        }
        ERROR_LOG("Failed to create column family:%s for reason:%s", name.c_str(), ret.ToString().c_str());
        return NULL;
    }

    int RocksDBEngine::Init(const std::string& dir, const std::string& conf)
    {
        rocksdb::Status s = rocksdb::GetDBOptionsFromString(m_options, conf, &m_options);
        s = rocksdb::DB::Open(m_options, "/tmp/mydb_rocks", &m_db);
        return -1;
    }

    int RocksDBEngine::Put(Context& ctx,const KeyObject& key, const ValueObject& value)
    {
        rocksdb::ColumnFamilyHandle* db = GetDB(ctx, key, true);
        if (NULL == db)
        {
            return ERR_DB_NOT_EXIST;
        }
        rocksdb::WriteOptions opt;
        Buffer keybuf, valuebuf;
        key.Encode(keybuf);
        //m_db->Put(opt, db, )
    }
    bool RocksDBEngine::Exists(Context& ctx,const KeyObject& key, bool exact_check)
    {
        rocksdb::ColumnFamilyHandle* db = NULL;
        rocksdb::ReadOptions opt;
        rocksdb::Slice k;
        bool exist = m_db->KeyMayExist(opt, db, k, NULL);
        if (!exist)
        {
            return false;
        }
        if (!exact_check)
        {
            return exist;
        }
        return m_db->Get(opt, db, k, NULL).ok();
    }
    int RocksDBEngine::BeginTransc()
    {
        rocksdb::WriteBatch batch;
        batch.Count();
    }
OP_NAMESPACE_END

