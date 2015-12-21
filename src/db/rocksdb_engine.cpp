/*
 * rocksdb_engine.cpp
 *
 *  Created on: 2015Äê9ÔÂ21ÈÕ
 *      Author: wangqiying
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"

OP_NAMESPACE_BEGIN

    class MergeOperator: public rocksdb::AssociativeMergeOperator
    {
        private:
            RocksDBEngine* m_engine;
        public:
            MergeOperator(RocksDBEngine* engine) :
                    m_engine(engine)
            {
            }
            bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const rocksdb::Slice& value, std::string* new_value,
                    rocksdb::Logger* logger) const override
            {
                MergeOperation op;
                Buffer mergeBuffer(const_cast<char*>(value.data()), 0, value.size());
                if (!op.Decode(mergeBuffer))
                {

                }
                switch (op.op)
                {
                    case REDIS_CMD_APPEND:
                    {
                        break;
                    }
                    case REDIS_CMD_INCRBY:
                    {
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                return true;        // always return true for this, since we treat all errors as "zero".
            }

            const char* Name() const override
            {
                return "ArdbMergeOperator";
            }
    };
    rocksdb::ColumnFamilyHandle* RocksDBEngine::GetColumnFamilyHandle(Context& ctx, const Data& name)
    {
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
        //m_db->ListColumnFamilies();
        ERROR_LOG("Failed to create column family:%s for reason:%s", name.c_str(), ret.ToString().c_str());
        return NULL;
    }

    int RocksDBEngine::Init(const std::string& dir, const std::string& conf)
    {
        rocksdb::Status s = rocksdb::GetDBOptionsFromString(m_options, conf, &m_options);
        s = rocksdb::DB::Open(m_options, dir, &m_db);
        return -1;
    }

    int RocksDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.ns);
        if (NULL == cf)
        {
            return ERR_DB_NOT_EXIST;
        }
        rocksdb::WriteOptions opt;
        rocksdb::Slice key_slice, value_slice;
        int err = m_db->Put(opt, cf, key_slice, value_slice);
        return err;
    }
    bool RocksDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ctx.flags.create_if_notexist = false;
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.ns);
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::ReadOptions opt;
        rocksdb::Slice k;
        bool exist = m_db->KeyMayExist(opt, cf, k, NULL);
        if (!exist)
        {
            return false;
        }
        if (ctx.flags.fuzzy_check)
        {
            return exist;
        }
        return m_db->Get(opt, cf, k, NULL).ok();
    }
OP_NAMESPACE_END

