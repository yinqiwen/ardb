/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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
#ifndef LEVELDB_ENGINE_HPP_
#define LEVELDB_ENGINE_HPP_

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "db/engine.hpp"
#include "util/config_helper.hpp"
#include "thread/thread_local.hpp"
#include "thread/spin_rwlock.hpp"
#include <stack>

namespace ardb
{
    class LevelDBEngine;
    class LevelDBIterator: public Iterator
    {
        private:
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            LevelDBEngine* m_engine;
            leveldb::Iterator* m_iter;
            KeyObject m_iterate_upper_bound_key;
            bool m_valid;
            void ClearState();
            void CheckBound();
            Slice GetRawKey(Data& ns);
            friend class LevelDBEngine;
        public:
            LevelDBIterator(LevelDBEngine* engine, const Data& ns) :
                    m_ns(ns), m_engine(engine), m_iter(NULL), m_valid(true)
            {
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
            void SetIterator(leveldb::Iterator* iter)
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
            KeyObject& Key(bool clone_str);
            ValueObject& Value(bool clone_str);
            Slice RawKey();
            Slice RawValue();
            void Del();
            ~LevelDBIterator();
    };

    struct LevelDBConfig
    {
            std::string path;
            int64 block_cache_size;
            int64 write_buffer_size;
            int64 max_open_files;
            int64 block_size;
            int64 block_restart_interval;
            int64 bloom_bits;
            int64 batch_commit_watermark;
            std::string compression;
            bool logenable;
            LevelDBConfig() :
                    block_cache_size(0), write_buffer_size(0), max_open_files(10240), block_size(0), block_restart_interval(0), bloom_bits(10), batch_commit_watermark(1024), compression("snappy"), logenable(false)
            {
            }
    };
    class LevelDBEngine: public Engine
    {
        private:
            DataSet m_nss;
            SpinRWLock m_lock;
            leveldb::DB* m_db;
            LevelDBConfig m_cfg;
            leveldb::Options m_options;
            friend class LevelDBIterator;
            bool GetNamespace(const Data& ns, bool create_if_missing);
            Iterator* Find(Context& ctx, const KeyObject& key, bool check_ns);
            int DelKeySlice(leveldb::WriteBatch* batch, const leveldb::Slice& key);
        public:
            LevelDBEngine();
            ~LevelDBEngine();
            int Init(const std::string& dir, const std::string& options);
            int Repair(const std::string& dir);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
            int DelKey(Context& ctx, const leveldb::Slice& key);
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
            const std::string GetErrorReason(int err);
            Iterator* Find(Context& ctx, const KeyObject& key);
            const FeatureSet GetFeatureSet()
            {
                FeatureSet features;
                features.support_compactfilter = 0;
                features.support_namespace = 0;
                features.support_merge = 0;
                return features;
            }
            int MaxOpenFiles();
            EngineSnapshot CreateSnapshot();
            void ReleaseSnapshot(EngineSnapshot s);
    };

}
#endif
