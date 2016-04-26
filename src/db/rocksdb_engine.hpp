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
#include "thread/thread_local.hpp"
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/comparator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/statistics.h"
#include "rocksdb/merge_operator.h"
#include "engine.hpp"
#include <vector>
#include <sparsehash/dense_hash_map>

OP_NAMESPACE_BEGIN

    class RocksDBEngine;
    class RocksDBIterator: public Iterator
    {
        private:
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            RocksDBEngine* m_engine;
            rocksdb::Iterator* m_iter;
            KeyObject m_iterate_upper_bound_key;bool m_valid;
            void ClearState();
            void CheckBound();
        public:
            RocksDBIterator(RocksDBEngine* engine, const Data& ns) :
                    m_ns(ns), m_engine(engine), m_iter(NULL), m_valid(true)
            {
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
            void SetIterator(rocksdb::Iterator* iter)
            {
                m_iter = iter;
            }
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
            KeyObject& Key();
            ValueObject& Value();
            Slice RawKey();
            Slice RawValue();
            ~RocksDBIterator();
    };

    class RocksDBCompactionFilter;
    class RocksDBEngine: public Engine
    {
        private:
            class RocksTransaction
            {
                private:
                    rocksdb::WriteBatch batch;
                    uint32_t ref;
                public:
                    RocksTransaction() :
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
            struct RocksSnapshot
            {
                    const rocksdb::Snapshot* snapshot;
                    uint32_t ref;
                    RocksSnapshot() :
                            snapshot(NULL), ref(0)
                    {
                    }
            };

            struct ColumnFamilyHandleContext
            {
                    Data name;
                    rocksdb::ColumnFamilyHandle* handler;
                    time_t create_time;
                    time_t droped_time;
                    ColumnFamilyHandleContext() :
                            handler(0), create_time(0), droped_time(0)
                    {
                    }
            };

            typedef TreeMap<Data, rocksdb::ColumnFamilyHandle*>::Type ColumnFamilyHandleTable;
            typedef std::vector<ColumnFamilyHandleContext> ColumnFamilyHandleArray;
            typedef TreeMap<uint32_t, Data>::Type ColumnFamilyHandleIDTable;
            rocksdb::DB* m_db;
            rocksdb::Options m_options;
            ColumnFamilyHandleTable m_handlers;
            ColumnFamilyHandleArray m_droped_handlers;
            SpinRWLock m_lock;
            ThreadLocal<RocksTransaction> m_transc;
            ThreadLocal<RocksSnapshot> m_snapshot;

            rocksdb::ColumnFamilyHandle* GetColumnFamilyHandle(Context& ctx, const Data& name);
            const rocksdb::Snapshot* GetSnpashot();
            const rocksdb::Snapshot* PeekSnpashot();
            void ReleaseSnpashot();
            Data GetNamespaceByColumnFamilyId(uint32 id);
            friend class RocksDBIterator;
            friend class RocksDBCompactionFilter;
        public:
            RocksDBEngine();
            ~RocksDBEngine();
            int Init(const std::string& dir, const std::string& conf);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args);bool Exists(Context& ctx, const KeyObject& key);
            int BeginTransaction();
            int CommitTransaction();
            int DiscardTransaction();
            int Compact(Context& ctx, const KeyObject& start, const KeyObject& end);
            int ListNameSpaces(Context& ctx, DataArray& nss);
            int DropNameSpace(Context& ctx, const Data& ns);
            void Stats(Context& ctx, std::string& str);
            int64_t EstimateKeysNum(Context& ctx, const Data& ns);

            Iterator* Find(Context& ctx, const KeyObject& key);
    };

OP_NAMESPACE_END
#endif /* SRC_ROCKSDB_HPP_ */
