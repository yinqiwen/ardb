/*
 * rocksdb_engine.cpp
 *
 *  Created on: 2015��9��21��
 *      Author: wangqiying
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/compaction_filter.h"
#include "thread/lock_guard.hpp"
#include "db/db.hpp"

#define ROCKSDB_SLICE(slice) rocksdb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())
#define ROCKSDB_ERR(err)  (0 == err.code()? 0: (rocksdb::Status::kNotFound == err.code() ? ERR_ENTRY_NOT_EXIST:(0 -err.code()-2000)))

//

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
                return compare_keys(a.data(), a.size(), b.data(), b.size(), false);
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
            void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const
            {
            }
            // Changes *key to a short string >= *key.
            // Simple comparator implementations may return with *key unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            void FindShortSuccessor(std::string* key) const
            {

            }
    };

    class RocksDBCompactionFilter: public rocksdb::CompactionFilter
	{
    	const char* Name() const
    	{
    		return "ArdbCompactionFilter";
    	}
    	bool FilterMergeOperand(int level, const rocksdb::Slice& key,
    	                                  const rocksdb::Slice& operand) const
    	{
    	    return false;
        }
    	bool Filter(int level,
    	                      const rocksdb::Slice& key,
    	                      const rocksdb::Slice& existing_value,
    	                      std::string* new_value,
    	                      bool* value_changed) const
    	{
    		if(existing_value.size() == 0)
    		{
    			return false;
    		}
    		return true;
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
                KeyObject key_obj;
                ValueObject val_obj, merge_op;

                Buffer keyBuffer(const_cast<char*>(key.data()), 0, key.size());
                key_obj.Decode(keyBuffer, false);

                Buffer mergeBuffer(const_cast<char*>(value.data()), 0, value.size());
                if(!merge_op.Decode(mergeBuffer, false))
                {
                	std::string ks;
                	key_obj.GetKey().ToString(ks);
                	WARN_LOG("Invalid merge key:%s op string with size:%u & op:%u", ks.c_str(), value.size(), merge_op.GetMergeOp());
                	return false;
                }
                if (NULL != existing_value)
                {
                    Buffer valueBuffer(const_cast<char*>(existing_value->data()), 0, existing_value->size());
                    if (!val_obj.Decode(valueBuffer, false))
                    {
                    	std::string ks;
                    	key_obj.GetKey().ToString(ks);
                    	WARN_LOG("Invalid key:%s existing value string with size:%llu, while merge op:%u", ks.c_str(),existing_value->size(), merge_op.GetMergeOp());
                        return true;
                    }
                }
                if(val_obj.GetType() == KEY_MERGE_OP)
                {
                	int merge_oprand = g_db->MergeOperand(val_obj, merge_op);
                	if(merge_oprand < 0)
                	{
                		return false;
                	}
                	else if(0 == merge_oprand)
                	{
                		new_value->assign(value.data(), value.size());
                		return true;
                	}else
                	{
                		Buffer tmp;
                		val_obj.Encode(tmp);
                		new_value->assign(tmp.GetRawReadBuffer(), tmp.ReadableBytes());
                		return true;
                	}
                }
                int err = g_db->MergeOperation(key_obj, val_obj, merge_op.GetMergeOp(), merge_op.GetMergeArgs());
                if (0 == err)
                {
                    Buffer encode_buffer;
                    Slice encode_slice = val_obj.Encode(encode_buffer);
                    new_value->assign(encode_slice.data(), encode_slice.size());
                }
                else
                {
                    if (NULL != existing_value && !existing_value->empty())
                    {
                        new_value->assign(existing_value->data(), existing_value->size());
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
    }

    RocksDBEngine::~RocksDBEngine()
    {
        DELETE(m_db);
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
        static RocksDBComparator comparator;
        m_options.comparator = &comparator;
        m_options.merge_operator.reset(new MergeOperator(this));
        rocksdb::Status s = rocksdb::GetOptionsFromString(m_options, conf, &m_options);
        if (!s.ok())
        {
            ERROR_LOG("Invalid rocksdb's options:%s with error reson:%s", conf.c_str(), s.ToString().c_str());
            return -1;
        }

        std::vector<std::string> column_families;
        s = rocksdb::DB::ListColumnFamilies(m_options, dir, &column_families);
        if (column_families.empty())
        {
            s = rocksdb::DB::Open(m_options, dir, &m_db);
        }
        else
        {
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families_descs(column_families.size());
            for (size_t i = 0; i < column_families.size(); i++)
            {
                column_families_descs[i] = rocksdb::ColumnFamilyDescriptor(column_families[i], rocksdb::ColumnFamilyOptions(m_options));
            }
            std::vector<rocksdb::ColumnFamilyHandle*> handlers;
            s = rocksdb::DB::Open(m_options, dir, column_families_descs, &handlers, &m_db);
            if (s.ok())
            {
                for (size_t i = 0; i < handlers.size(); i++)
                {
                    rocksdb::ColumnFamilyHandle* handler = handlers[i];
                    Data ns;
                    ns.SetString(column_families_descs[i].name, true);
                    m_handlers[ns] = handler;
                    INFO_LOG("RocksDB open column family:%s success.", column_families_descs[i].name.c_str());
                }
            }
        }

        if (s != rocksdb::Status::OK())
        {
            ERROR_LOG("Failed to open db:%s by reason:%s", dir.c_str(), s.ToString().c_str());
            return -1;
        }
        return 0;
    }

    int RocksDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::WriteOptions opt;
        rocksdb::Slice key_slice = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
        Buffer value_buffer;
        rocksdb::Slice value_slice = ROCKSDB_SLICE(const_cast<ValueObject&>(value).Encode(value_buffer));
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = m_transc.GetValue().Ref();
        if (NULL != batch)
        {
            batch->Put(cf, key_slice, value_slice);
        }
        else
        {
            s = m_db->Put(opt, cf, key_slice, value_slice);
        }
        return ROCKSDB_ERR(s);
    }
    int RocksDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, ctx.ns);
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        std::vector<rocksdb::ColumnFamilyHandle*> cfs;
        std::vector<rocksdb::Slice> ks;
        for (size_t i = 0; i < keys.size(); i++)
        {
            ks.push_back(ROCKSDB_SLICE(const_cast<KeyObject&>(keys[i]).Encode()));
        }
        for (size_t i = 0; i < keys.size(); i++)
        {
            cfs.push_back(cf);
        }
        std::vector<std::string> vs;
        rocksdb::ReadOptions opt;
        opt.snapshot = PeekSnpashot();
        std::vector<rocksdb::Status> ss = m_db->MultiGet(opt, cfs, ks, &vs);
        errs.resize(ss.size());
        values.resize(ss.size());
        for (size_t i = 0; i < ss.size(); i++)
        {
            if (ss[i].ok())
            {
                Buffer valBuffer(const_cast<char*>(vs[i].data()), 0, vs[i].size());
                values[i].Decode(valBuffer, true);
            }
            errs[i] = ROCKSDB_ERR(ss[i]);
        }
        return 0;
    }
    int RocksDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::ReadOptions opt;
        opt.snapshot = PeekSnpashot();
        std::string valstr;
        rocksdb::Slice key_slice = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
        rocksdb::Status s = m_db->Get(opt, cf, key_slice, &valstr);
        int err = ROCKSDB_ERR(s);
        if (0 != err)
        {
            return err;
        }
        Buffer valBuffer(const_cast<char*>(valstr.data()), 0, valstr.size());
        value.Decode(valBuffer, true);
        return 0;
    }
    int RocksDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::WriteOptions opt;
        rocksdb::Slice key_slice = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = m_transc.GetValue().Ref();
        if (NULL != batch)
        {
            batch->Delete(cf, key_slice);
        }
        else
        {
            s = m_db->Delete(opt, cf, key_slice);
        }
        return ROCKSDB_ERR(s);
    }

    int RocksDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::WriteOptions opt;
        rocksdb::Slice key_slice = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
        Buffer merge_buffer;
        encode_merge_operation(merge_buffer, op, args);
        rocksdb::Slice merge_slice(merge_buffer.GetRawReadBuffer(), merge_buffer.ReadableBytes());
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = m_transc.GetValue().Ref();
        if (NULL != batch)
        {
            batch->Merge(cf, key_slice, merge_slice);
        }
        else
        {
            s = m_db->Merge(opt, cf, key_slice, merge_slice);
        }
        return ROCKSDB_ERR(s);
    }

    bool RocksDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ctx.flags.create_if_notexist = false;
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::ReadOptions opt;
        opt.snapshot = PeekSnpashot();
        rocksdb::Slice k = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
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

    const rocksdb::Snapshot* RocksDBEngine::GetSnpashot()
    {
        RocksSnapshot& snapshot = m_snapshot.GetValue();
        snapshot.ref++;
        if (snapshot.snapshot == NULL)
        {
            snapshot.snapshot = m_db->GetSnapshot();
        }
        return snapshot.snapshot;
    }
    const rocksdb::Snapshot* RocksDBEngine::PeekSnpashot()
    {
        RocksSnapshot& snapshot = m_snapshot.GetValue();
        return snapshot.snapshot;
    }
    void RocksDBEngine::ReleaseSnpashot()
    {
        RocksSnapshot& snapshot = m_snapshot.GetValue();
        snapshot.ref--;
        if (0 == snapshot.ref)
        {
            m_db->ReleaseSnapshot(snapshot.snapshot);
            snapshot.snapshot = NULL;
        }
    }

    Iterator* RocksDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, key.GetNameSpace());
        if (NULL == cf)
        {
            return NULL;
        }
        rocksdb::ReadOptions opt;
        opt.snapshot = GetSnpashot();
        rocksdb::Iterator* rocksiter = m_db->NewIterator(opt, cf);
        Iterator* iter = NULL;
        NEW(iter, RocksDBIterator(this,rocksiter));
        if (key.GetType() > 0)
        {
            iter->Jump(key);
        }
        return iter;
    }

    int RocksDBEngine::BeginTransaction()
    {
        m_transc.GetValue().AddRef();
        return 0;
    }
    int RocksDBEngine::CommitTransaction()
    {
        if (m_transc.GetValue().ReleaseRef(false) == 0)
        {
            rocksdb::WriteOptions opt;
            m_db->Write(opt, &m_transc.GetValue().GetBatch());
            m_transc.GetValue().Clear();
        }
        return 0;
    }
    int RocksDBEngine::DiscardTransaction()
    {
        if (m_transc.GetValue().ReleaseRef(true) == 0)
        {
            m_transc.GetValue().Clear();
        }
        return 0;
    }

    bool RocksDBIterator::Valid()
    {
        return NULL != m_iter && m_iter->Valid();
    }
    void RocksDBIterator::Next()
    {
        assert(m_iter != NULL);
        m_iter->Next();
    }
    void RocksDBIterator::Prev()
    {
        assert(m_iter != NULL);
        m_iter->Prev();
    }
    void RocksDBIterator::Jump(const KeyObject& next)
    {
        assert(m_iter != NULL);
        Slice key_slice = const_cast<KeyObject&>(next).Encode();
        m_iter->Seek(ROCKSDB_SLICE(key_slice));
    }
    KeyObject& RocksDBIterator::Key()
    {
        rocksdb::Slice key = m_iter->key();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        assert(m_key.Decode(kbuf, true));
        return m_key;
    }
    ValueObject& RocksDBIterator::Value()
    {
        rocksdb::Slice key = m_iter->value();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        assert(m_value.Decode(kbuf, true));
        return m_value;
    }
    RocksDBIterator::~RocksDBIterator()
    {
        m_engine->ReleaseSnpashot();
        DELETE(m_iter);
    }
OP_NAMESPACE_END

