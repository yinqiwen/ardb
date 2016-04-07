/*
 * rocksdb_engine.cpp
 *
 *  Created on: 2015��9��21��
 *      Author: wangqiying
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice_transform.h"
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
                return "ardb.comparator";
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

    class RocksDBPrefixExtractor: public rocksdb::SliceTransform
    {
            // Return the name of this transformation.
            const char* Name() const
            {
                return "ardb.prefix_extractor";
            }

            // transform a src in domain to a dst in the range
            rocksdb::Slice Transform(const rocksdb::Slice& src) const
            {
                Buffer buffer(const_cast<char*>(src.data()), 0, src.size());
                KeyObject k;
                if (!k.DecodeKey(buffer, false))
                {
                    abort();
                }
                return rocksdb::Slice(src.data(), src.size() - buffer.ReadableBytes());
            }

            // determine whether this is a valid src upon the function applies
            bool InDomain(const rocksdb::Slice& src) const
            {
                return true;
            }

            // determine whether dst=Transform(src) for some src
            bool InRange(const rocksdb::Slice& dst) const
            {
                return true;
            }

            // Transform(s)=Transform(`prefix`) for any s with `prefix` as a prefix.
            //
            // This function is not used by RocksDB, but for users. If users pass
            // Options by string to RocksDB, they might not know what prefix extractor
            // they are using. This function is to help users can determine:
            //   if they want to iterate all keys prefixing `prefix`, whetherit is
            //   safe to use prefix bloom filter and seek to key `prefix`.
            // If this function returns true, this means a user can Seek() to a prefix
            // using the bloom filter. Otherwise, user needs to skip the bloom filter
            // by setting ReadOptions.total_order_seek = true.
            //
            // Here is an example: Suppose we implement a slice transform that returns
            // the first part of the string after spliting it using deimiter ",":
            // 1. SameResultWhenAppended("abc,") should return true. If aplying prefix
            //    bloom filter using it, all slices matching "abc:.*" will be extracted
            //    to "abc,", so any SST file or memtable containing any of those key
            //    will not be filtered out.
            // 2. SameResultWhenAppended("abc") should return false. A user will not
            //    guaranteed to see all the keys matching "abc.*" if a user seek to "abc"
            //    against a DB with the same setting. If one SST file only contains
            //    "abcd,e", the file can be filtered out and the key will be invisible.
            //
            // i.e., an implementation always returning false is safe.
            virtual bool SameResultWhenAppended(const rocksdb::Slice& prefix) const
            {
                return false;
            }
    };

    class RocksDBCompactionFilter: public rocksdb::CompactionFilter
    {
        private:
            uint32_t cf_id;
            std::string delete_key;
        public:
            RocksDBCompactionFilter(uint32 id) :
                    cf_id(id)
            {
            }
            const char* Name() const
            {
                return "ardb.compact_filter";
            }
            bool FilterMergeOperand(int level, const rocksdb::Slice& key, const rocksdb::Slice& operand) const
            {
                return false;
            }
            bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value, std::string* new_value,
            bool* value_changed) const
            {
                if (existing_value.size() == 0)
                {
                    return true;
                }
                Buffer buffer(const_cast<char*>(key.data()), 0, key.size());
                KeyObject k;
                if (!k.DecodeKey(buffer, false))
                {
                    abort();
                }
                if (k.GetType() == KEY_META)
                {
                    ValueObject meta;
                    Buffer val_buffer(const_cast<char*>(existing_value.data()), 0, existing_value.size());
                    if (!meta.Decode(val_buffer, false))
                    {
                        abort();
                    }
                    if (meta.GetTTL() <= get_current_epoch_millis())
                    {
                        if (meta.GetType() != KEY_STRING)
                        {
                            const_cast<RocksDBCompactionFilter*>(this)->delete_key.assign(k.GetKey().CStr(), k.GetKey().StringLength());
                        }
                        return true;
                    }
                    const_cast<RocksDBCompactionFilter*>(this)->delete_key.clear();
                }
                else
                {
                    if (!delete_key.empty())
                    {
                        if (delete_key.size() == k.GetKey().StringLength() && !strncmp(delete_key.data(), k.GetKey().CStr(), delete_key.size()))
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
    };

    class RocksDBCompactionFilterFactory: public rocksdb::CompactionFilterFactory
    {
            std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context)
            {
                return std::unique_ptr < rocksdb::CompactionFilter > (new RocksDBCompactionFilter(context.column_family_id));
            }

            const char* Name() const
            {
                return "ardb.compact_filter_factory";
            }
    };

    class MergeOperator: public rocksdb::MergeOperator
    {
        private:
            RocksDBEngine* m_engine;
        public:
            MergeOperator(RocksDBEngine* engine) :
                    m_engine(engine)
            {
            }
            // Gives the client a way to express the read -> modify -> write semantics
            // key:      (IN)    The key that's associated with this merge operation.
            //                   Client could multiplex the merge operator based on it
            //                   if the key space is partitioned and different subspaces
            //                   refer to different types of data which have different
            //                   merge operation semantics
            // existing: (IN)    null indicates that the key does not exist before this op
            // operand_list:(IN) the sequence of merge operations to apply, front() first.
            // new_value:(OUT)   Client is responsible for filling the merge result here.
            // The string that new_value is pointing to will be empty.
            // logger:   (IN)    Client could use this to log errors during merge.
            //
            // Return true on success.
            // All values passed in will be client-specific values. So if this method
            // returns false, it is because client specified bad data or there was
            // internal corruption. This will be treated as an error by the library.
            //
            // Also make use of the *logger for error messages.
            bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const std::deque<std::string>& operand_list, std::string* new_value,
                    rocksdb::Logger* logger) const
            {
                KeyObject key_obj;
                Buffer keyBuffer(const_cast<char*>(key.data()), 0, key.size());
                key_obj.Decode(keyBuffer, false);

                ValueObject val_obj;
                if (NULL != existing_value)
                {
                    Buffer valueBuffer(const_cast<char*>(existing_value->data()), 0, existing_value->size());
                    if (!val_obj.Decode(valueBuffer, false))
                    {
                        std::string ks;
                        key_obj.GetKey().ToString(ks);
                        WARN_LOG("Invalid key:%s existing value string with size:%llu", ks.c_str(), existing_value->size());
                        return false;
                    }
                }
                bool value_changed = false;
                for (size_t i = 0; i < operand_list.size(); i++)
                {
                    uint16 op = 0;
                    DataArray args;
                    Buffer mergeBuffer(const_cast<char*>(operand_list[i].data()), 0, operand_list[i].size());
                    if (!decode_merge_operation(mergeBuffer, op, args))
                    {
                        std::string ks;
                        key_obj.GetKey().ToString(ks);
                        WARN_LOG("Invalid merge op which decode faild for key:%s", ks.c_str());
                        continue;
                    }
                    if (0 == g_db->MergeOperation(key_obj, val_obj, op, args))
                    {
                        value_changed = true;
                    }
                }
                if (value_changed)
                {
                    Buffer encode_buffer;
                    Slice encode_slice = val_obj.Encode(encode_buffer);
                    new_value->assign(encode_slice.data(), encode_slice.size());
                }
                else
                {
                    if (NULL != existing_value)
                    {
                        new_value->assign(existing_value->data(), existing_value->size());
                    }
                }
                return true;
            }
            // This function performs merge(left_op, right_op)
            // when both the operands are themselves merge operation types
            // that you would have passed to a DB::Merge() call in the same order
            // (i.e.: DB::Merge(key,left_op), followed by DB::Merge(key,right_op)).
            //
            // PartialMerge should combine them into a single merge operation that is
            // saved into *new_value, and then it should return true.
            // *new_value should be constructed such that a call to
            // DB::Merge(key, *new_value) would yield the same result as a call
            // to DB::Merge(key, left_op) followed by DB::Merge(key, right_op).
            //
            // The string that new_value is pointing to will be empty.
            //
            // The default implementation of PartialMergeMulti will use this function
            // as a helper, for backward compatibility.  Any successor class of
            // MergeOperator should either implement PartialMerge or PartialMergeMulti,
            // although implementing PartialMergeMulti is suggested as it is in general
            // more effective to merge multiple operands at a time instead of two
            // operands at a time.
            //
            // If it is impossible or infeasible to combine the two operations,
            // leave new_value unchanged and return false. The library will
            // internally keep track of the operations, and apply them in the
            // correct order once a base-value (a Put/Delete/End-of-Database) is seen.
            //
            // TODO: Presently there is no way to differentiate between error/corruption
            // and simply "return false". For now, the client should simply return
            // false in any case it cannot perform partial-merge, regardless of reason.
            // If there is corruption in the data, handle it in the FullMerge() function,
            // and return false there.  The default implementation of PartialMerge will
            // always return false.
            bool PartialMergeMulti(const rocksdb::Slice& key, const std::deque<rocksdb::Slice>& operand_list, std::string* new_value,
                    rocksdb::Logger* logger) const
            {
                if (operand_list.size() < 2)
                {
                    return false;
                }
                uint16 ops[2];
                DataArray ops_args[2];
                size_t left_pos = 0;
                Buffer first_op_buffer(const_cast<char*>(operand_list[0].data()), 0, operand_list[0].size());
                if (!decode_merge_operation(first_op_buffer, ops[0], ops_args[0]))
                {
                    WARN_LOG("Invalid first merge op.");
                    return false;
                }
                for (size_t i = 1; i < operand_list.size(); i += 2)
                {
                    Buffer op_buffer(const_cast<char*>(operand_list[i].data()), 0, operand_list[i].size());
                    if (!decode_merge_operation(op_buffer, ops[1 - left_pos], ops_args[1 - left_pos]))
                    {
                        WARN_LOG("Invalid merge op at:%u", i);
                        return false;
                    }
                    if (0 != g_db->MergeOperands(ops[left_pos], ops_args[left_pos], ops[1 - left_pos], ops_args[1 - left_pos]))
                    {
                        return false;
                    }
                    left_pos = 1 - left_pos;
                }
                Buffer merge;
                encode_merge_operation(merge, ops[left_pos], ops_args[left_pos]);
                new_value->assign(merge.GetRawReadBuffer(), merge.ReadableBytes());
                return true;
            }
            // This function performs merge when all the operands are themselves merge
            // operation types that you would have passed to a DB::Merge() call in the
            // same order (front() first)
            // (i.e. DB::Merge(key, operand_list[0]), followed by
            //  DB::Merge(key, operand_list[1]), ...)
            //
            // PartialMergeMulti should combine them into a single merge operation that is
            // saved into *new_value, and then it should return true.  *new_value should
            // be constructed such that a call to DB::Merge(key, *new_value) would yield
            // the same result as subquential individual calls to DB::Merge(key, operand)
            // for each operand in operand_list from front() to back().
            //
            // The string that new_value is pointing to will be empty.
            //
            // The PartialMergeMulti function will be called only when the list of
            // operands are long enough. The minimum amount of operands that will be
            // passed to the function are specified by the "min_partial_merge_operands"
            // option.
            //
            // In the default implementation, PartialMergeMulti will invoke PartialMerge
            // multiple times, where each time it only merges two operands.  Developers
            // should either implement PartialMergeMulti, or implement PartialMerge which
            // is served as the helper function of the default PartialMergeMulti.
            bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand, const rocksdb::Slice& right_operand, std::string* new_value,
                    rocksdb::Logger* logger) const
            {
                uint16 left_op = 0, right_op = 0;
                DataArray left_args, right_args;
                Buffer left_mergeBuffer(const_cast<char*>(left_operand.data()), 0, left_operand.size());
                Buffer right_mergeBuffer(const_cast<char*>(right_operand.data()), 0, right_operand.size());
                if (!decode_merge_operation(left_mergeBuffer, left_op, left_args))
                {
                    WARN_LOG("Invalid left merge op.");
                    return false;
                }
                if (!decode_merge_operation(right_mergeBuffer, right_op, right_args))
                {
                    WARN_LOG("Invalid right merge op.");
                    return false;
                }
                int err = g_db->MergeOperands(left_op, left_args, right_op, right_args);
                if (0 == err)
                {
                    Buffer merge;
                    encode_merge_operation(merge, right_op, right_args);
                    new_value->assign(merge.GetRawReadBuffer(), merge.ReadableBytes());
                    return true;
                }
                return false;
            }

            const char* Name() const override
            {
                return "ardb.merger";
            }
    };

    RocksDBEngine::RocksDBEngine() :
            m_db(NULL)
    {
    }

    RocksDBEngine::~RocksDBEngine()
    {
        ColumnFamilyHandleTable::iterator it = m_handlers.begin();
        while (it != m_handlers.end())
        {
            DELETE(it->second);
            it++;
        }
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
            m_idmapping[cfh->GetID()] = ns;
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
        m_options.prefix_extractor.reset(new RocksDBPrefixExtractor);
        m_options.compaction_filter_factory.reset(new RocksDBCompactionFilterFactory);
        rocksdb::Status s = rocksdb::GetOptionsFromString(m_options, conf, &m_options);
        if (!s.ok())
        {
            ERROR_LOG("Invalid rocksdb's options:%s with error reason:%s", conf.c_str(), s.ToString().c_str());
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
                    m_idmapping[handlers[i]->GetID()] = ns;
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
        std::string tmp;
        rocksdb::Slice k = ROCKSDB_SLICE(const_cast<KeyObject&>(key).Encode());
        bool exist = m_db->KeyMayExist(opt, cf, k, &tmp, NULL);
        if (!exist)
        {
            return false;
        }
        if (ctx.flags.fuzzy_check)
        {
            return exist;
        }
        return m_db->Get(opt, cf, k, &tmp).ok();
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
        RocksDBIterator* iter = NULL;
        NEW(iter, RocksDBIterator(this,key.GetNameSpace()));
        rocksdb::ReadOptions opt;
        opt.snapshot = GetSnpashot();
        if (key.GetType() > 0)
        {
            opt.prefix_same_as_start = true;
            if(!ctx.flags.iterate_multi_keys)
            {
                iter->IterateUpperBoundKey().SetNameSpce(ctx.ns);
                if (key.GetType() == KEY_META)
                {
                    iter->IterateUpperBoundKey().SetType(KEY_END);
                }
                else
                {
                    iter->IterateUpperBoundKey().SetType(key.GetType() + 1);
                }
                iter->IterateUpperBoundKey().SetKey(key.GetKey());
                iter->SaveIterateUpperBoundSlice();
                opt.iterate_upper_bound = &(iter->IterateUpperBoundSlice());
            }
        }
        rocksdb::Iterator* rocksiter = m_db->NewIterator(opt, cf);
        iter->SetIterator(rocksiter);
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

    int RocksDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        rocksdb::ColumnFamilyHandle* cf = GetColumnFamilyHandle(ctx, ctx.ns);
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::Slice start_key = ROCKSDB_SLICE(const_cast<KeyObject&>(start).Encode());
        rocksdb::Slice end_key = ROCKSDB_SLICE(const_cast<KeyObject&>(end).Encode());
        rocksdb::CompactRangeOptions opt;
        rocksdb::Status s = m_db->CompactRange(opt, cf, start.IsValid() ? &start_key : NULL, end.IsValid() ? &end_key : NULL);
        return ROCKSDB_ERR(s);
    }
    void RocksDBIterator::SaveIterateUpperBoundSlice()
    {
        m_iterate_upper_bound_slice = ROCKSDB_SLICE(m_iterate_upper_bound_key.Encode(false));
    }
    bool RocksDBIterator::RocksDBIterator::Valid()
    {
        return NULL != m_iter && m_iter->Valid();
    }
    void RocksDBIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
    }
    void RocksDBIterator::Next()
    {
        ClearState();
        m_iter->Next();
    }
    void RocksDBIterator::Prev()
    {
        ClearState();
        m_iter->Prev();
    }
    void RocksDBIterator::Jump(const KeyObject& next)
    {
        ClearState();
        Slice key_slice = const_cast<KeyObject&>(next).Encode();
        m_iter->Seek(ROCKSDB_SLICE(key_slice));
    }
    void RocksDBIterator::JumpToFirst()
    {
        ClearState();
        m_iter->SeekToFirst();
    }
    void RocksDBIterator::JumpToLast()
    {
        ClearState();
        m_iter->SeekToLast();
    }
    KeyObject& RocksDBIterator::Key()
    {
        if (m_key.GetType() > 0)
        {
            return m_key;
        }
        rocksdb::Slice key = m_iter->key();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_key.Decode(kbuf, true);
        m_key.SetNameSpce(m_ns);
        return m_key;
    }
    ValueObject& RocksDBIterator::Value()
    {
        if (m_value.GetType() > 0)
        {
            return m_value;
        }
        rocksdb::Slice key = m_iter->value();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_value.Decode(kbuf, true);
        return m_value;
    }
    RocksDBIterator::~RocksDBIterator()
    {
        m_engine->ReleaseSnpashot();
        DELETE(m_iter);
    }
OP_NAMESPACE_END

