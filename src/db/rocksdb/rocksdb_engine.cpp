/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "rocksdb_engine.hpp"
#include "rocksdb/utilities/convenience.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/table.h"
#include "thread/lock_guard.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "db/db.hpp"
#include "util/string_helper.hpp"

OP_NAMESPACE_BEGIN

    static inline rocksdb::Slice to_rocksdb_slice(const Slice& slice)
    {
        return rocksdb::Slice(slice.data(), slice.size());
    }
    static inline Slice to_ardb_slice(const rocksdb::Slice& slice)
    {
        return Slice(slice.data(), slice.size());
    }

    static rocksdb::DB* g_rocksdb = NULL;

    class RocksWriteBatch
    {
        private:
            rocksdb::WriteBatch batch;
            uint32_t ref;
        public:
            RocksWriteBatch() :
                    ref(0)
            {
            }
            rocksdb::WriteBatch& GetBatch()
            {
                return batch;
            }
            rocksdb::WriteBatch* Ref()
            {
                if (ref > 0)
                {
                    return &batch;
                }
                return NULL;
            }
            uint32 AddRef()
            {
                ref++;
                batch.SetSavePoint();
                return ref;
            }
            uint32 ReleaseRef(bool rollback)
            {
                ref--;
                if (rollback)
                {
                    batch.RollbackToSavePoint();
                }
                return ref;
            }
            void Clear()
            {
                batch.Clear();
                ref = 0;
            }
    };

    struct RocksIterData
    {
            Data ns;
            rocksdb::Iterator* iter;
            rocksdb::SequenceNumber dbseq;
            time_t create_time;
            bool iter_total_order_seek;
            bool iter_prefix_same_as_start;
            bool delete_after_finish;
            RocksIterData() :
                    iter(NULL), dbseq(0), create_time(0), iter_total_order_seek(false), iter_prefix_same_as_start(
                            false), delete_after_finish(false)
            {
            }
            bool EqualOptions(const rocksdb::ReadOptions& a)
            {
                return a.total_order_seek == iter_total_order_seek
                        && a.prefix_same_as_start == iter_prefix_same_as_start;
            }
            ~RocksIterData()
            {
                DELETE(iter);
            }
    };

    struct RocksDBLocalContext
    {
            RocksWriteBatch transc;
            Buffer encode_buffer_cache;
            std::string string_cache;
            std::vector<std::string> multi_string_cache;
            typedef TreeMap<int, rocksdb::Status>::Type ErrMap;
            ErrMap err_map;
            Buffer& GetEncodeBuferCache()
            {
                encode_buffer_cache.Clear();
                return encode_buffer_cache;
            }
            std::string& GetStringCache()
            {
                string_cache.clear();
                return string_cache;
            }
            std::vector<string>& GetMultiStringCache()
            {
                return multi_string_cache;
            }
    };

    static ThreadLocal<RocksDBLocalContext> g_rocks_context;
    //static RocksIteratorCache g_iter_cache;

    static inline int rocksdb_err(const rocksdb::Status& s)
    {
        if (s.code() == 0)
        {
            return 0;
        }
        if (rocksdb::Status::kNotFound == s.code())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        int err = (int) (s.code()) << 8 + s.subcode();
        g_rocks_context.GetValue().err_map[err] = s;
        return err + STORAGE_ENGINE_ERR_OFFSET;
    }

    class RocksDBLogger: public rocksdb::Logger
    {
            // Write an entry to the log file with the specified format.
            virtual void Logv(const char* format, va_list ap)
            {
                Logv(rocksdb::INFO_LEVEL, format, ap);
            }
            // Write an entry to the log file with the specified log level
            // and format.  Any log with level under the internal log level
            // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
            // printed.
            void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap)
            {
                LogLevel level = INFO_LOG_LEVEL;
                switch (log_level)
                {
                    case rocksdb::INFO_LEVEL:
                    {
                        level = INFO_LOG_LEVEL;
                        break;
                    }
                    case rocksdb::DEBUG_LEVEL:
                    {
                        level = DEBUG_LOG_LEVEL;
                        break;
                    }
                    case rocksdb::WARN_LEVEL:
                    {
                        level = WARN_LOG_LEVEL;
                        break;
                    }
                    case rocksdb::ERROR_LEVEL:
                    {
                        level = ERROR_LOG_LEVEL;
                        break;
                    }
                    case rocksdb::FATAL_LEVEL:
                    {
                        level = ERROR_LOG_LEVEL;
                        break;
                    }
                    case rocksdb::HEADER_LEVEL:
                    {
                        level = INFO_LOG_LEVEL;
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                if (LOG_ENABLED(level))
                {
                    char* buffer = NULL;
                    size_t buf_len = 1024;
                    NEW(buffer, char[buf_len]);
                    int n = vsnprintf(buffer, buf_len - 1, format, ap);
                    if (n > 0 && NULL != buffer)
                    {
                        if (n < buf_len)
                        {
                            buffer[n] = 0;
                        }
                        else
                        {
                            buffer[buf_len - 1] = 0;
                        }
                        LOG_WITH_LEVEL(level, "[RocksDB]%s", buffer);
                    }
                    DELETE_A(buffer);
                }
            }
    };

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
                if (src.size() == 0)
                {
                    return src;
                }
                Buffer buffer(const_cast<char*>(src.data()), 0, src.size());
                KeyObject k;
                if (!k.DecodeKey(buffer, false))
                {
                    FATAL_LOG("Not a valid key slice in PrefixExtractor with len:%d", src.size());
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
            Data ns;
        public:
            RocksDBCompactionFilter(RocksDBEngine* engine, const rocksdb::CompactionFilter::Context& context)
            {
                ns = engine->GetNamespaceByColumnFamilyId(context.column_family_id);
            }
            const char* Name() const
            {
                return "ardb.compact_filter";
            }
            bool FilterMergeOperand(int level, const rocksdb::Slice& key, const rocksdb::Slice& operand) const
            {
                return false;
            }
            bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& existing_value,
                    std::string* new_value, bool* value_changed) const
            {
                /*
                 * do not do filter for slave
                 */
                if (!g_db->GetConf().master_host.empty())
                {
                    return false;
                }
                if (existing_value.size() == 0)
                {
                    return true;
                }
                if (ns.IsNil())
                {
                    return false;
                }
                Buffer buffer(const_cast<char*>(key.data()), 0, key.size());
                KeyObject k;
                if (!k.DecodePrefix(buffer, false))
                {
                    FATAL_LOG("Failed to decode prefix in compact filter.");
                }
                if (k.GetType() == KEY_META)
                {
                    ValueObject meta;
                    Buffer val_buffer(const_cast<char*>(existing_value.data()), 0, existing_value.size());
                    if (!meta.DecodeMeta(val_buffer))
                    {
                        ERROR_LOG("Failed to decode value of key:%s with type:%u %u", k.GetKey().AsString().c_str(),
                                meta.GetType(), existing_value.size());
                        return false;
                    }
                    if (meta.GetType() == 0)
                    {
                        ERROR_LOG("Invalid value for key:%s with type:%u %u", k.GetKey().AsString().c_str(),
                                k.GetType(), existing_value.size());
                        return false;
                    }
                    if (meta.GetMergeOp() != 0)
                    {
                        return false;
                    }
                    if (meta.GetTTL() > 0 && meta.GetTTL() <= get_current_epoch_millis())
                    {
                        g_db->AddExpiredKey(ns, k.GetKey());
                        if (meta.GetType() != KEY_STRING)
                        {
                            return false;
                        }
                        else
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
    };

    struct RocksDBCompactionFilterFactory: public rocksdb::CompactionFilterFactory
    {
            RocksDBEngine* engine;
            RocksDBCompactionFilterFactory(RocksDBEngine* e) :
                    engine(e)
            {
            }
            std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
                    const rocksdb::CompactionFilter::Context& context)
            {
                return std::unique_ptr < rocksdb::CompactionFilter > (new RocksDBCompactionFilter(engine, context));
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
            bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                    const std::deque<std::string>& operand_list, std::string* new_value, rocksdb::Logger* logger) const
            {

                KeyObject key_obj;
                Buffer keyBuffer(const_cast<char*>(key.data()), 0, key.size());
                key_obj.Decode(keyBuffer, false);

                //INFO_LOG("Do merge for key:%s in thread %d", key_obj.GetKey().AsString().c_str(), pthread_self());

                ValueObject val_obj;
                if (NULL != existing_value)
                {
                    Buffer valueBuffer(const_cast<char*>(existing_value->data()), 0, existing_value->size());
                    if (!val_obj.Decode(valueBuffer, false))
                    {
                        std::string ks;
                        key_obj.GetKey().ToString(ks);
                        WARN_LOG("Invalid key:%s existing value string with size:%llu", ks.c_str(),
                                existing_value->size());
                        return false;
                    }
                }
                bool value_changed = false;
                for (size_t i = 0; i < operand_list.size(); i++)
                {
                    Buffer mergeBuffer(const_cast<char*>(operand_list[i].data()), 0, operand_list[i].size());
                    ValueObject mergeValue;
                    if (!mergeValue.Decode(mergeBuffer, false))
                    {
                        std::string ks;
                        key_obj.GetKey().ToString(ks);
                        WARN_LOG("Invalid merge op which decode faild for key:%s", ks.c_str());
                        continue;
                    }
                    if (0 == g_db->MergeOperation(key_obj, val_obj, mergeValue.GetMergeOp(), mergeValue.GetMergeArgs()))
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
            bool PartialMergeMulti(const rocksdb::Slice& key, const std::deque<rocksdb::Slice>& operand_list,
                    std::string* new_value, rocksdb::Logger* logger) const
            {
                if (operand_list.size() < 2)
                {
                    return false;
                }
                ValueObject ops[2];
                size_t left_pos = 0;
                Buffer first_op_buffer(const_cast<char*>(operand_list[0].data()), 0, operand_list[0].size());
                if (!ops[0].Decode(first_op_buffer, false))
                {
                    WARN_LOG("Invalid first merge op.");
                    return false;
                }
                for (size_t i = 1; i < operand_list.size(); i++)
                {
                    Buffer op_buffer(const_cast<char*>(operand_list[i].data()), 0, operand_list[i].size());
                    if (!ops[1 - left_pos].Decode(op_buffer, false))
                    {
                        WARN_LOG("Invalid merge op at:%u", i);
                        return false;
                    }
                    if (0
                            != g_db->MergeOperands(ops[left_pos].GetMergeOp(), ops[left_pos].GetMergeArgs(),
                                    ops[1 - left_pos].GetMergeOp(), ops[1 - left_pos].GetMergeArgs()))
                    {
                        return false;
                    }
                    left_pos = 1 - left_pos;
                }
                Buffer merge;
                encode_merge_operation(merge, ops[left_pos].GetMergeOp(), ops[left_pos].GetMergeArgs());
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
            bool PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand, std::string* new_value, rocksdb::Logger* logger) const
            {
                ValueObject left_op, right_op;
                Buffer left_mergeBuffer(const_cast<char*>(left_operand.data()), 0, left_operand.size());
                Buffer right_mergeBuffer(const_cast<char*>(right_operand.data()), 0, right_operand.size());
                if (!left_op.Decode(left_mergeBuffer, false))
                {
                    WARN_LOG("Invalid left merge op.");
                    return false;
                }
                if (!right_op.Decode(right_mergeBuffer, false))
                {
                    WARN_LOG("Invalid right merge op.");
                    return false;
                }
                int err = g_db->MergeOperands(left_op.GetMergeOp(), left_op.GetMergeArgs(), right_op.GetMergeOp(),
                        right_op.GetMergeArgs());
                if (0 == err)
                {
                    Buffer merge;
                    encode_merge_operation(merge, right_op.GetMergeOp(), right_op.GetMergeArgs());
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
            m_db(NULL), m_bulk_loading(false)
    {
    }

    RocksDBEngine::~RocksDBEngine()
    {
        Close();
    }

    RocksDBEngine::ColumnFamilyHandlePtr RocksDBEngine::GetColumnFamilyHandle(Context& ctx, const Data& ns,
            bool create_if_noexist)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !ctx.flags.create_if_notexist);
        ColumnFamilyHandleTable::iterator found = m_handlers.find(ns);
        if (found != m_handlers.end())
        {
            return found->second;
        }
        if (!create_if_noexist)
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
            m_handlers[ns].reset(cfh);
            INFO_LOG("Create ColumnFamilyHandle with name:%s success.", name.c_str());
            return m_handlers[ns];
        }
        ERROR_LOG("Failed to create column family:%s for reason:%s", name.c_str(), s.ToString().c_str());
        return NULL;
    }

    void RocksDBEngine::Close()
    {
        //g_iter_cache.Clear();
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        m_handlers.clear(); //handlers MUST be deleted before m_db
        DELETE(m_db);
    }

    int RocksDBEngine::Backup(Context& ctx, const std::string& dir)
    {
        LockGuard<ThreadMutex> guard(m_backup_lock);
        rocksdb::BackupableDBOptions opt(dir);
        rocksdb::BackupEngine* backup_engine = NULL;
        rocksdb::Status s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), opt, &backup_engine);
        if (s.ok())
        {
            s = backup_engine->CreateNewBackup(m_db, true);
            if (s.ok())
            {
                backup_engine->PurgeOldBackups(1);
            }
        }
        if (!s.ok())
        {
            ERROR_LOG("Failed to backup rocksdb for reason:%s", s.ToString().c_str());
        }
        DELETE(backup_engine);
        return rocksdb_err(s);
    }

    int RocksDBEngine::Restore(Context& ctx, const std::string& dir)
    {
        LockGuard<ThreadMutex> guard(m_backup_lock);
        m_bulk_loading = true;
        Close();
        rocksdb::BackupEngineReadOnly* backup_engine = NULL;
        rocksdb::BackupableDBOptions opt(dir);
        rocksdb::Status s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(), opt, &backup_engine);
        if (s.ok())
        {
            std::vector<rocksdb::BackupInfo> backup_info;
            backup_engine->GetBackupInfo(&backup_info);
            s = backup_engine->RestoreDBFromBackup(backup_info[0].backup_id, m_dbdir, m_dbdir);
        }
        if (!s.ok())
        {
            ERROR_LOG("Failed to backup rocksdb for reason:%s", s.ToString().c_str());
        }
        DELETE(backup_engine);
        ReOpen(m_options);
        m_bulk_loading = false;
        return 0;
    }

    int RocksDBEngine::ReOpen(rocksdb::Options& options)
    {
        Close();
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        std::vector<std::string> column_families;
        rocksdb::Status s = rocksdb::DB::ListColumnFamilies(options, m_dbdir, &column_families);
        if (column_families.empty())
        {
            s = rocksdb::DB::Open(options, m_dbdir, &m_db);
        }
        else
        {
            std::vector<rocksdb::ColumnFamilyDescriptor> column_families_descs(column_families.size());
            for (size_t i = 0; i < column_families.size(); i++)
            {
                column_families_descs[i] = rocksdb::ColumnFamilyDescriptor(column_families[i],
                        rocksdb::ColumnFamilyOptions(m_options));
            }
            std::vector<rocksdb::ColumnFamilyHandle*> handlers;
            s = rocksdb::DB::Open(options, m_dbdir, column_families_descs, &handlers, &m_db);
            if (s.ok())
            {
                for (size_t i = 0; i < handlers.size(); i++)
                {
                    rocksdb::ColumnFamilyHandle* handler = handlers[i];
                    Data ns;
                    ns.SetString(column_families_descs[i].name, false);
                    m_handlers[ns].reset(handler);
                    INFO_LOG("RocksDB open column family:%s success.", column_families_descs[i].name.c_str());
                }
            }
        }

        if (s != rocksdb::Status::OK())
        {
            ERROR_LOG("Failed to open db:%s by reason:%s", m_dbdir.c_str(), s.ToString().c_str());
            return -1;
        }
        g_rocksdb = m_db;
        return 0;
    }

    int RocksDBEngine::Init(const std::string& dir, const std::string& conf)
    {
        //g_iter_cache.Init();

        static RocksDBComparator comparator;
        m_options.comparator = &comparator;
        m_options.merge_operator.reset(new MergeOperator(this));
        m_options.prefix_extractor.reset(new RocksDBPrefixExtractor);
        m_options.compaction_filter_factory.reset(new RocksDBCompactionFilterFactory(this));
        m_options.info_log.reset(new RocksDBLogger);
        m_options.create_if_missing = true;
        if (DEBUG_ENABLED())
        {
            m_options.info_log_level = rocksdb::DEBUG_LEVEL;
        }
        else
        {
            m_options.info_log_level = rocksdb::INFO_LEVEL;
        }

        rocksdb::Status s = rocksdb::GetOptionsFromString(m_options, conf, &m_options);
        if (!s.ok())
        {
            ERROR_LOG("Invalid rocksdb's options:%s with error reason:%s", conf.c_str(), s.ToString().c_str());
            return -1;
        }
        if (strcasecmp(g_db->GetConf().rocksdb_compaction.c_str(), "OptimizeLevelStyleCompaction") == 0)
        {
            m_options.OptimizeLevelStyleCompaction();

        }
        else if (strcasecmp(g_db->GetConf().rocksdb_compaction.c_str(), "OptimizeUniversalStyleCompaction") == 0)
        {
            m_options.OptimizeUniversalStyleCompaction();
        }

        if (g_db->GetConf().rocksdb_disablewal)
        {
            disablewal = true;
        }

        m_options.IncreaseParallelism();
        m_options.stats_dump_period_sec = (unsigned int) g_db->GetConf().statistics_log_period;
        m_dbdir = dir;
        return ReOpen(m_options);
    }

    int RocksDBEngine::Routine()
    {
        //g_iter_cache.Routine();
        return 0;
    }

    int RocksDBEngine::Repair(const std::string& dir)
    {
        static RocksDBComparator comparator;
        m_options.comparator = &comparator;
        m_options.merge_operator.reset(new MergeOperator(this));
        m_options.prefix_extractor.reset(new RocksDBPrefixExtractor);
        m_options.compaction_filter_factory.reset(new RocksDBCompactionFilterFactory(this));
        m_options.info_log.reset(new RocksDBLogger);
        m_options.info_log_level = rocksdb::INFO_LEVEL;
        return rocksdb_err(rocksdb::RepairDB(dir, m_options));
    }

    Data RocksDBEngine::GetNamespaceByColumnFamilyId(uint32 id)
    {
        Data ns;
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        ColumnFamilyHandleTable::iterator it = m_handlers.begin();
        while (it != m_handlers.end())
        {
            if (it->second->GetID() == id)
            {
                ns.SetString(it->second->GetName(), false);
                return ns;
            }
            it++;
        }
        return ns;
    }

    int RocksDBEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ns, ctx.flags.create_if_notexist);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::WriteOptions opt;
        if (disablewal || ctx.flags.bulk_loading)
        {
            opt.disableWAL = true;
        }
        rocksdb::Slice key_slice = to_rocksdb_slice(key);
        rocksdb::Slice value_slice = to_rocksdb_slice(value);
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = rocks_ctx.transc.Ref();
        if (NULL != batch)
        {
            batch->Put(cf, key_slice, value_slice);
        }
        else
        {
            s = m_db->Put(opt, cf, key_slice, value_slice);
        }
        return rocksdb_err(s);
    }

    int RocksDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        rocksdb::Status s;
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::WriteOptions opt;
        if (disablewal || ctx.flags.bulk_loading)
        {
            opt.disableWAL = true;
        }
        Buffer& encode_buffer = rocks_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        rocksdb::Slice key_slice(encode_buffer.GetRawBuffer(), key_len);
        rocksdb::Slice value_slice(encode_buffer.GetRawBuffer() + key_len, value_len);
        rocksdb::WriteBatch* batch = rocks_ctx.transc.Ref();
        if (NULL != batch)
        {
            batch->Put(cf, key_slice, value_slice);
        }
        else
        {
            s = m_db->Put(opt, cf, key_slice, value_slice);
        }
        return rocksdb_err(s);
    }
    int RocksDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        values.resize(keys.size());
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ctx.ns, false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            errs.assign(keys.size(), ERR_ENTRY_NOT_EXIST);
            return ERR_ENTRY_NOT_EXIST;
        }
        errs.resize(keys.size());
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        std::vector<rocksdb::ColumnFamilyHandle*> cfs;
        std::vector<rocksdb::Slice> ks;
        std::vector<size_t> positions;
        Buffer& key_encode_buffers = rocks_ctx.GetEncodeBuferCache();
        ks.resize(keys.size());
        std::vector<std::string>& vs = rocks_ctx.GetMultiStringCache();
        for (size_t i = 0; i < keys.size(); i++)
        {
            size_t mark = key_encode_buffers.GetWriteIndex();
            keys[i].Encode(key_encode_buffers);
            positions.push_back((size_t) (key_encode_buffers.GetWriteIndex() - mark));
        }
        for (size_t i = 0; i < keys.size(); i++)
        {
            cfs.push_back(cf);
            ks[i] = rocksdb::Slice(key_encode_buffers.GetRawReadBuffer(), positions[i]);
            key_encode_buffers.AdvanceReadIndex(positions[i]);
        }

        rocksdb::ReadOptions opt;
        //opt.snapshot = rocks_ctx.PeekSnapshot();
        std::vector<rocksdb::Status> ss = m_db->MultiGet(opt, cfs, ks, &vs);

        for (size_t i = 0; i < ss.size(); i++)
        {
            if (ss[i].ok())
            {
                Buffer valBuffer(const_cast<char*>(vs[i].data()), 0, vs[i].size());
                values[i].Decode(valBuffer, true);
            }
            errs[i] = rocksdb_err(ss[i]);
        }
        return 0;
    }
    int RocksDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::ReadOptions opt;
        //opt.snapshot = rocks_ctx.PeekSnapshot();
        std::string& valstr = rocks_ctx.GetStringCache();
        Buffer& key_encode_buffer = rocks_ctx.GetEncodeBuferCache();
        rocksdb::Slice key_slice = to_rocksdb_slice(key.Encode(key_encode_buffer));
        rocksdb::Status s = m_db->Get(opt, cf, key_slice, &valstr);
        int err = rocksdb_err(s);
        if (0 != err)
        {
            return err;
        }
        Buffer valBuffer(const_cast<char*>(valstr.data()), 0, valstr.size());
        value.Decode(valBuffer, true);

        return 0;
    }

    int RocksDBEngine::DelRange(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ctx.ns, false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::WriteOptions opt;
        Buffer& key_encode_buffer = rocks_ctx.GetEncodeBuferCache();
        start.Encode(key_encode_buffer, false);
        size_t start_len = key_encode_buffer.ReadableBytes();
        end.Encode(key_encode_buffer, false);
        size_t end_len = key_encode_buffer.ReadableBytes() - start_len;
        rocksdb::Slice start_slice(key_encode_buffer.GetRawBuffer(), start_len);
        rocksdb::Slice end_slice(key_encode_buffer.GetRawBuffer() + start_len, end_len);
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = rocks_ctx.transc.Ref();
        if (NULL != batch)
        {
            batch->DeleteRange(cf, start_slice, end_slice);
        }
        else
        {
            s = m_db->DeleteRange(opt, cf, start_slice, end_slice);
        }
        return rocksdb_err(s);
    }
    int RocksDBEngine::DelKeySlice(rocksdb::WriteBatch* batch, rocksdb::ColumnFamilyHandle* cf, const rocksdb::Slice& key_slice)
    {
        rocksdb::WriteOptions opt;
        rocksdb::Status s;
        //rocksdb::WriteBatch* batch = rocks_ctx.transc.Ref();
        if (NULL != batch)
        {
            batch->Delete(cf, key_slice);
        }
        else
        {
            s = m_db->Delete(opt, cf, key_slice);
        }
        return rocksdb_err(s);
    }
    int RocksDBEngine::DelKey(Context& ctx, const rocksdb::Slice& key_slice)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ctx.ns, false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        return DelKeySlice(rocks_ctx.transc.Ref(), cf, key_slice);
    }

    int RocksDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::WriteOptions opt;
        Buffer& key_encode_buffer = rocks_ctx.GetEncodeBuferCache();
        rocksdb::Slice key_slice = to_rocksdb_slice(key.Encode(key_encode_buffer));
        return DelKeySlice(rocks_ctx.transc.Ref(), cf, key_slice);
    }

    int RocksDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::WriteOptions opt;
        Buffer& encode_buffer = rocks_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        encode_merge_operation(encode_buffer, op, args);
        size_t merge_len = encode_buffer.ReadableBytes() - key_len;
        rocksdb::Slice key_slice(encode_buffer.GetRawBuffer(), key_len);
        rocksdb::Slice merge_slice(encode_buffer.GetRawBuffer() + key_len, merge_len);
        rocksdb::Status s;
        rocksdb::WriteBatch* batch = rocks_ctx.transc.Ref();
        if (NULL != batch)
        {
            batch->Merge(cf, key_slice, merge_slice);
        }
        else
        {
            s = m_db->Merge(opt, cf, key_slice, merge_slice);
        }
        return rocksdb_err(s);
    }

    bool RocksDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return false;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocksdb::ReadOptions opt;
        //opt.snapshot = rocks_ctx.PeekSnapshot();
        Buffer& key_encode_buffer = rocks_ctx.GetEncodeBuferCache();
        std::string& tmp = rocks_ctx.GetStringCache();
        rocksdb::Slice k = to_rocksdb_slice(key.Encode(key_encode_buffer));
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

    Iterator* RocksDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        rocksdb::ReadOptions opt;
        opt.snapshot = (const rocksdb::Snapshot*) ctx.engine_snapshot;
        //opt.snapshot = GetSnpashot();
        RocksDBIterator* iter = NULL;
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, key.GetNameSpace(), false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        NEW(iter, RocksDBIterator(this,cf, key.GetNameSpace()));
        if (NULL == cf)
        {
            iter->MarkValid(false);
            return iter;
        }
        if (key.GetType() > 0)
        {
            if (!ctx.flags.iterate_multi_keys)
            {
                //opt.prefix_same_as_start = true;
                if (!ctx.flags.iterate_no_upperbound)
                {
                    KeyObject& upperbound_key = iter->IterateUpperBoundKey();
                    upperbound_key.SetNameSpace(key.GetNameSpace());
                    if (key.GetType() == KEY_META)
                    {
                        upperbound_key.SetType(KEY_END);
                    }
                    else
                    {
                        upperbound_key.SetType(key.GetType() + 1);
                    }
                    upperbound_key.SetKey(key.GetKey());
                    upperbound_key.CloneStringPart();
                }
            }
            else
            {
                //opt.total_order_seek = true;
            }
        }
        if (ctx.flags.iterate_total_order)
        {
            opt.total_order_seek = true;
        }
        //RocksIterData* rocksiter = g_iter_cache.Get(key.GetNameSpace(), opt);
        RocksIterData* rocksiter = NULL;
        if (NULL == rocksiter)
        {
            NEW(rocksiter, RocksIterData);
            rocksiter->iter = m_db->NewIterator(opt, cf);
            rocksiter->dbseq = m_db->GetLatestSequenceNumber();
            rocksiter->create_time = time(NULL);
            rocksiter->iter_prefix_same_as_start = opt.prefix_same_as_start;
            rocksiter->iter_total_order_seek = opt.total_order_seek;
            rocksiter->ns.Clone(key.GetNameSpace());
            rocksiter->ns.ToMutableStr();
            //g_iter_cache.AddRunningIter(rocksiter);
        }
        iter->SetIterator(rocksiter);
        if (key.GetType() > 0)
        {
            iter->Jump(key);
        }
        else
        {
            iter->JumpToFirst();
        }
        return iter;
    }

    int RocksDBEngine::BeginWriteBatch(Context& ctx)
    {
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        rocks_ctx.transc.AddRef();
        return 0;
    }
    int RocksDBEngine::CommitWriteBatch(Context& ctx)
    {
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        if (rocks_ctx.transc.ReleaseRef(false) == 0)
        {
            rocksdb::WriteOptions opt;
            if (disablewal || ctx.flags.bulk_loading)
            {
                opt.disableWAL = true;
            }
            m_db->Write(opt, &rocks_ctx.transc.GetBatch());
            rocks_ctx.transc.Clear();
        }
        return 0;
    }
    int RocksDBEngine::DiscardWriteBatch(Context& ctx)
    {
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        if (rocks_ctx.transc.ReleaseRef(true) == 0)
        {
            rocks_ctx.transc.Clear();
        }
        return 0;
    }

    int RocksDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, start.GetNameSpace(), false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Buffer start_buffer, end_buffer;
        rocksdb::Slice start_key;
        if (start.IsValid())
        {
            start_key = to_rocksdb_slice(start.Encode(start_buffer));
        }
        rocksdb::Slice end_key;
        if (end.IsValid())
        {
            end_key = to_rocksdb_slice(end.Encode(end_buffer));
        }
        rocksdb::CompactRangeOptions opt;
        rocksdb::Status s = m_db->CompactRange(opt, cf, start.IsValid() ? &start_key : NULL,
                end.IsValid() ? &end_key : NULL);
        return rocksdb_err(s);
    }

    int RocksDBEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        ColumnFamilyHandleTable::iterator it = m_handlers.begin();
        while (it != m_handlers.end())
        {
            if (it->first.AsString() != m_db->DefaultColumnFamily()->GetName())
            {
                nss.push_back(it->first);
            }
            it++;
        }
        return 0;
    }

    int RocksDBEngine::Flush(Context& ctx, const Data& ns)
    {
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ns, false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        rocksdb::FlushOptions opt;
        rocksdb::Status s = m_db->Flush(opt, cf);
        return rocksdb_err(s);
    }

    int RocksDBEngine::BeginBulkLoad(Context& ctx)
    {
        rocksdb::Options load_options = m_options;
        load_options.PrepareForBulkLoad();
        m_bulk_loading = true;
        return ReOpen(load_options);
    }
    int RocksDBEngine::EndBulkLoad(Context& ctx)
    {
        int ret = ReOpen(m_options);
        m_bulk_loading = false;
        return ret;
    }

    const std::string RocksDBEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        RocksDBLocalContext::ErrMap& err_map = g_rocks_context.GetValue().err_map;
        if (err_map.count(err) > 0)
        {
            return err_map[err].ToString();
        }
        return "";
    }

    int RocksDBEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        //g_iter_cache.Clear();
        ColumnFamilyHandleTable::iterator found = m_handlers.find(ns);
        if (found != m_handlers.end())
        {
            INFO_LOG("RocksDB drop column family:%s.", found->second->GetName().c_str());
            m_db->DropColumnFamily(found->second.get());
            //m_droped_handlers.push_back(found->second);
            m_handlers.erase(found);
            return 0;
        }
        return ERR_ENTRY_NOT_EXIST;
    }

    int64_t RocksDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        std::string cf_stat;
        ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, ns, false);
        rocksdb::ColumnFamilyHandle* cf = cfp.get();
        if (NULL == cf) return 0;
        uint64 value = 0;
        m_db->GetIntProperty(cf, "rocksdb.estimate-num-keys", &value);
        return (int64) value;
    }

    int RocksDBEngine::MaxOpenFiles()
    {
        return (int) (m_options.max_open_files);
    }

    const FeatureSet RocksDBEngine::GetFeatureSet()
    {
        FeatureSet features;
        features.support_compactfilter = 1;
        features.support_namespace = 1;
        features.support_merge = 1;
        features.support_backup = 1;
        features.support_delete_range = 1;
        return features;
    }

    void RocksDBEngine::Stats(Context& ctx, std::string& all)
    {
        std::string str, version_info;
        version_info.append("rocksdb_version:").append(stringfromll(rocksdb::kMajorVersion)).append(".").append(
                stringfromll(rocksdb::kMinorVersion)).append(".").append(stringfromll(ROCKSDB_PATCH)).append("\r\n");
        all.append(version_info);
        if (m_bulk_loading || NULL == m_db)
        {
            return;
        }
        // all.append("rocksdb_iterator_cache:").append(stringfromll(g_iter_cache.cache_size)).append("\r\n");
        std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;
        std::unordered_set<const rocksdb::Cache*> cache_set;
        std::vector<rocksdb::DB*> dbs(1, m_db);
        cache_set.insert(m_db->GetDBOptions().row_cache.get());
        rocksdb::MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set, &usage_by_type);
        for (size_t i = 0; i < rocksdb::MemoryUtil::kNumUsageTypes; ++i)
        {
            if (usage_by_type.count((rocksdb::MemoryUtil::UsageType) i) > 0)
            {
                std::string name;
                switch (i)
                {
                    case rocksdb::MemoryUtil::kMemTableTotal:
                    {
                        name = "rocksdb_memtable_total";
                        break;
                    }
                    case rocksdb::MemoryUtil::kMemTableUnFlushed:
                    {
                        name = "rocksdb_memtable_unflushed";
                        break;
                    }
                    case rocksdb::MemoryUtil::kTableReadersTotal:
                    {
                        name = "rocksdb_table_readers_total";
                        break;
                    }
                    case rocksdb::MemoryUtil::kCacheTotal:
                    {
                        name = "rocksdb_cache_total";
                        break;
                    }
                    default:
                    {
                        continue;
                    }
                }
                all.append(name).append(":").append(stringfromll(usage_by_type[(rocksdb::MemoryUtil::UsageType) i])).append(
                        "\r\n");
            }
        }
        DataArray nss;
        ListNameSpaces(ctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            std::string cf_stat;
            ColumnFamilyHandlePtr cfp = GetColumnFamilyHandle(ctx, nss[i], false);
            rocksdb::ColumnFamilyHandle* cf = cfp.get();
            if (NULL == cf) continue;
            m_db->GetProperty(cf, "rocksdb.stats", &cf_stat);
            all.append(cf_stat).append("\r\n");
        }
    }

    EngineSnapshot RocksDBEngine::CreateSnapshot()
    {
        return m_db->GetSnapshot();
    }
    void RocksDBEngine::ReleaseSnapshot(EngineSnapshot s)
    {
        if (NULL == s)
        {
            return;
        }
        m_db->ReleaseSnapshot((rocksdb::Snapshot*) s);
    }

    void RocksDBIterator::SetIterator(RocksIterData* iter)
    {
        m_iter = iter;
        m_rocks_iter = m_iter->iter;
    }

    bool RocksDBIterator::Valid()
    {
        return m_valid && NULL != m_rocks_iter && m_rocks_iter->Valid();
    }
    void RocksDBIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void RocksDBIterator::CheckBound()
    {
        if (NULL != m_rocks_iter && m_iterate_upper_bound_key.GetType() > 0)
        {
            if (m_rocks_iter->Valid())
            {
                if (Key(false).Compare(m_iterate_upper_bound_key) >= 0)
                {
                    m_valid = false;
                }
            }
        }
    }
    void RocksDBIterator::Next()
    {
        ClearState();
        if (NULL == m_rocks_iter)
        {
            return;
        }
        m_rocks_iter->Next();
        CheckBound();
    }
    void RocksDBIterator::Prev()
    {
        ClearState();
        if (NULL == m_rocks_iter)
        {
            return;
        }
        m_rocks_iter->Prev();
    }
    void RocksDBIterator::Jump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_rocks_iter)
        {
            return;
        }
        RocksDBLocalContext& rocks_ctx = g_rocks_context.GetValue();
        Slice key_slice = next.Encode(rocks_ctx.GetEncodeBuferCache(), false);
        m_rocks_iter->Seek(to_rocksdb_slice(key_slice));
        CheckBound();
    }
    void RocksDBIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_rocks_iter)
        {
            return;
        }
        m_rocks_iter->SeekToFirst();
    }
    void RocksDBIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_rocks_iter)
        {
            return;
        }

        if (m_iterate_upper_bound_key.GetType() > 0)
        {
            Jump(m_iterate_upper_bound_key);
            if (!m_rocks_iter->Valid())
            {
                m_rocks_iter->SeekToLast();
            }
            else
            {
                Prev();
            }
        }
        else
        {
            m_rocks_iter->SeekToLast();
        }
    }

    KeyObject& RocksDBIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
        rocksdb::Slice key = m_rocks_iter->key();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_key.Decode(kbuf, clone_str);
        m_key.SetNameSpace(m_ns);
        return m_key;
    }
    ValueObject& RocksDBIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            if (clone_str)
            {
                m_value.CloneStringPart();
            }
            return m_value;
        }
        rocksdb::Slice key = m_rocks_iter->value();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice RocksDBIterator::RawKey()
    {
        return to_ardb_slice(m_rocks_iter->key());
    }
    Slice RocksDBIterator::RawValue()
    {
        return to_ardb_slice(m_rocks_iter->value());
    }
    void RocksDBIterator::Del()
    {
        if (NULL != m_rocks_iter)
        {
            Context tmpctx;
            tmpctx.ns = m_ns;
            m_engine->DelKey(tmpctx, m_rocks_iter->key());
            //rocksdb::WriteOptions opt;
            //m_engine->m_db->Delete(opt, m_cf, m_rocks_iter->key());
        }

    }
    RocksDBIterator::~RocksDBIterator()
    {
        if (NULL != m_iter)
        {
            DELETE(m_iter);
            //g_iter_cache.Recycle(m_iter);
        }
    }
OP_NAMESPACE_END

