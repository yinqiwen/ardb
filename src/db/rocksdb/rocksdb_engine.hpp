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

#ifndef ROCKSDB_HPP_
#define ROCKSDB_HPP_

#include "common/common.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/thread_local.hpp"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/comparator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/backupable_db.h"
#include "db/engine.hpp"
#include <vector>
#include <sparsehash/dense_hash_map>
#include <memory>

OP_NAMESPACE_BEGIN

    class RocksIterData;
    class RocksDBEngine;
    class RocksDBIterator: public Iterator
    {
        private:
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            RocksDBEngine* m_engine;
            rocksdb::ColumnFamilyHandle* m_cf;
            RocksIterData* m_iter;
            rocksdb::Iterator* m_rocks_iter;
            KeyObject m_iterate_upper_bound_key;
            bool m_valid;
            void ClearState();
            void CheckBound();
        public:
            RocksDBIterator(RocksDBEngine* engine, rocksdb::ColumnFamilyHandle* cf, const Data& ns) :
                    m_ns(ns), m_engine(engine), m_cf(cf), m_iter(NULL), m_rocks_iter(NULL),m_valid(true)
            {
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
            void SetIterator(RocksIterData* iter);
            KeyObject& IterateUpperBoundKey()
            {
                return m_iterate_upper_bound_key;
            }
            bool Valid();
            void Next();
            void Prev();
            void Jump(const KeyObject& next);
            void JumpToFirst();
            void JumpToLast();
            KeyObject& Key(bool clone_str);
            ValueObject& Value(bool clone_str);
            Slice RawKey();
            Slice RawValue();
            void Del();
            ~RocksDBIterator();
    };

    class RocksDBCompactionFilter;
    class RocksDBEngine: public Engine
    {
        private:
            typedef std::shared_ptr<rocksdb::ColumnFamilyHandle> ColumnFamilyHandlePtr;
            typedef TreeMap<Data, ColumnFamilyHandlePtr>::Type ColumnFamilyHandleTable;
            typedef TreeMap<uint32_t, Data>::Type ColumnFamilyHandleIDTable;
            rocksdb::DB* m_db;

            rocksdb::Options m_options;
            std::string m_dbdir;
            ColumnFamilyHandleTable m_handlers;
            SpinRWLock m_lock;
            ThreadMutex m_backup_lock;
            bool m_bulk_loading;
            bool disablewal;

            ColumnFamilyHandlePtr GetColumnFamilyHandle(Context& ctx, const Data& name, bool create_if_noexist);

            Data GetNamespaceByColumnFamilyId(uint32 id);
            int ReOpen(rocksdb::Options& options);
            void Close();
            friend class RocksDBIterator;
            friend class RocksDBCompactionFilter;
            int DelKeySlice(rocksdb::WriteBatch* batch, rocksdb::ColumnFamilyHandle* cf, const rocksdb::Slice& key);
        public:
            RocksDBEngine();
            ~RocksDBEngine();
            int Init(const std::string& dir, const std::string& options);
            int Repair(const std::string& dir);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
            int DelKey(Context& ctx, const rocksdb::Slice& key);
            int DelRange(Context& ctx, const KeyObject& start, const KeyObject& end);
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args);
            bool Exists(Context& ctx, const KeyObject& key);
            int BeginWriteBatch(Context& ctx);
            int CommitWriteBatch(Context& ctx);
            int DiscardWriteBatch(Context& ctx);
            int Compact(Context& ctx, const KeyObject& start, const KeyObject& end);
            int ListNameSpaces(Context& ctx, DataArray& nss);
            int DropNameSpace(Context& ctx, const Data& ns);
            void Stats(Context& ctx, std::string& str);
            int64_t EstimateKeysNum(Context& ctx, const Data& ns);
            Iterator* Find(Context& ctx, const KeyObject& key);
            int Flush(Context& ctx, const Data& ns);
            int BeginBulkLoad(Context& ctx);
            int EndBulkLoad(Context& ctx);
            const std::string GetErrorReason(int err);
            int Backup(Context& ctx, const std::string& dir);
            int Restore(Context& ctx, const std::string& dir);
            const FeatureSet GetFeatureSet();
            int Routine();
            int MaxOpenFiles();
            EngineSnapshot CreateSnapshot();
            void ReleaseSnapshot(EngineSnapshot s);
    };

OP_NAMESPACE_END
#endif /* SRC_ROCKSDB_HPP_ */
