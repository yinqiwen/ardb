/*
 * rocksdb_engine.cpp
 *
 *  Created on: 2015Äê9ÔÂ21ÈÕ
 *      Author: wangqiying
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"
#include "thread/lock_guard.hpp"

OP_NAMESPACE_BEGIN

    class RocksDBComparator: public rocksdb::Comparator
    {
        public:
            // Three-way comparison.  Returns value:
            //   < 0 iff "a" < "b",
            //   == 0 iff "a" == "b",
            //   > 0 iff "a" > "b"
            int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const
            {
                KeyObject ak, bk;
                //decode
                return ak.Compare(bk);
            }

            // Compares two slices for equality. The following invariant should always
            // hold (and is the default implementation):
            //   Equal(a, b) iff Compare(a, b) == 0
            // Overwrite only if equality comparisons can be done more efficiently than
            // three-way comparisons.
            bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const
            {
                return Compare(a, b) == 0;
            }

            // The name of the comparator.  Used to check for comparator
            // mismatches (i.e., a DB created with one comparator is
            // accessed using a different comparator.
            //
            // The client of this package should switch to a new name whenever
            // the comparator implementation changes in a way that will cause
            // the relative ordering of any two keys to change.
            //
            // Names starting with "rocksdb." are reserved and should not be used
            // by any clients of this package.
            const char* Name() const
            {
                return "ArdbComparator";
            }

            // Advanced functions: these are used to reduce the space requirements
            // for internal data structures like index blocks.

            // If *start < limit, changes *start to a short string in [start,limit).
            // Simple comparator implementations may return with *start unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            void FindShortestSeparator(std::string* start, const Slice& limit) const
            {
            }
            // Changes *key to a short string >= *key.
            // Simple comparator implementations may return with *key unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            void FindShortSuccessor(std::string* key)
            {
            }
    };

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
                return "ArdbMerger";
            }
    };

    RocksDBEngine::RocksDBEngine() :
            m_db(NULL)
    {
        Data deleted_ns, empty_ns;
        deleted_ns.SetInt64(-1);
        empty_ns.SetInt64(-2);
        m_handlers.set_deleted_key(deleted_ns);
        m_handlers.set_empty_key(empty_ns);
    }

    rocksdb::ColumnFamilyHandle* RocksDBEngine::GetColumnFamilyHandle(Context& ctx, const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !ctx.flags.create_if_notexist);
        ColumnFamilyHandleTable::iterator found = m_handlers.find(ns);
        if (found != m_handlers.end())
        {
            return found->second;
        }
        if (!ctx.flags.create_if_notexist)
        {
            return NULL;
        }
        rocksdb::ColumnFamilyOptions cf_options(m_options);
        std::string name;
        ns.ToString(name);
        rocksdb::ColumnFamilyHandle* cfh = NULL;
        rocksdb::Status s = m_db->CreateColumnFamily(cf_options, name, &cfh);
        if (s.ok())
        {
            m_handlers[ns] = cfh;
            INFO_LOG("Create ColumnFamilyHandle with name:%s success.", name.c_str());
            return cfh;
        }
        ERROR_LOG("Failed to create column family:%s for reason:%s", name.c_str(), s.ToString().c_str());
        return NULL;
    }

    int RocksDBEngine::Init(const std::string& dir, const std::string& conf)
    {
        rocksdb::Status s = rocksdb::GetDBOptionsFromString(m_options, conf, &m_options);
        if (!s.ok())
        {
            ERROR_LOG("Invalid rocksdb's options:%s with error reson:%s", conf.c_str(), s.ToString().c_str());
            return -1;
        }
        static RocksDBComparator comparator;
        m_options.comparator = &comparator;
        m_options.merge_operator.reset(new MergeOperator(this));
        std::vector<std::string> column_families;
        s = rocksdb::DB::ListColumnFamilies(m_options, dir, &column_families);
        if (!s.OK())
        {
            ERROR_LOG("No column families found by reason:%s", s.ToString().c_str());
            return -1;
        }
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families_descs(column_families.size());
        for (size_t i = 0; i < column_families.size(); i++)
        {
            column_families_descs[i] = ColumnFamilyDescriptor(column_families[i], ColumnFamilyOptions(m_options));
        }
        std::vector<rocksdb::ColumnFamilyHandle*> handlers;
        s = rocksdb::DB::Open(m_options, dir, column_families_descs, &handlers, &m_db);
        if (!s.OK())
        {
            ERROR_LOG("Failed to open db:%s by reason:%s", dir.c_str(), s.ToString().c_str());
            return -1;
        }
        for (size_t i = 0; i < handlers.size(); i++)
        {
            rocksdb::ColumnFamilyHandle* handler = handlers[i];
            Data ns;
            ns.SetString(column_families_descs[i].name, true);
            m_handlers[ns] = handler;
        }
        return 0;
    }

    int RocksDBEngine::Put(Context& ctx, KeyObject& key, const ValueObject& value)
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

