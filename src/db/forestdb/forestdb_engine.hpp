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

#ifndef FORESTDB_ENGINE_HPP_
#define FORESTDB_ENGINE_HPP_

#include "libforestdb/forestdb.h"
#include "db/engine.hpp"
#include "channel/all_includes.hpp"
#include "util/config_helper.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/event_condition.hpp"
#include "util/concurrent_queue.hpp"
#include <stack>

namespace ardb
{
    class ForestDBEngine;
    class ForestDBIterator: public Iterator
    {
        private:
            fdb_kvs_handle *m_kv;
            fdb_iterator* m_iter;
            fdb_doc* m_raw;
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            std::string min_key;
            std::string max_key;
            bool m_valid;

            void ClearRawDoc();
            void DoJump(const KeyObject& next);

            bool Valid();
            void Next();
            void Prev();
            void Jump(const KeyObject& next);
            void JumpToFirst();
            void JumpToLast();
            KeyObject& Key(bool clone_str);
            ValueObject& Value(bool clone_str);
            void GetRaw();
            Slice RawKey();
            Slice RawValue();
            void Del();


            void SetMin(const void* data, size_t len)
            {
                if(NULL != data)
                {
                    min_key.assign((const char*)data, len);
                }
            }
            void SetMax(const void* data, size_t len)
            {
                if(NULL != data)
                {
                    max_key.assign((const char*)data, len);
                }
            }
            void SetIterator(fdb_iterator *cursor)
            {
                m_iter = cursor;
            }
            void ClearState();
            void CheckBound();
            friend class ForestDBEngine;
        public:
            ForestDBIterator(fdb_kvs_handle* kv, const Data& ns) :
                    m_kv(kv), m_iter(NULL), m_raw(NULL), m_ns(ns), m_valid(true)
            {
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
//            void DelKey(const KeyObject& key);
            ~ForestDBIterator();
    };
    class ForestDBLocalContext;
    class ForestDBEngine: public Engine
    {
        private:
            DataSet m_nss;
            SpinRWLock m_lock;
            fdb_kvs_handle* GetKVStore(Context& ctx, const Data& name, bool create_if_noexist);
            void AddNamespace(const Data& ns);
            bool HaveNamespace(const Data& ns);
            friend class ForestDBLocalContext;
        public:
            ForestDBEngine();
            ~ForestDBEngine();
            int Init(const std::string& dir, const std::string& options);
            int Repair(const std::string& dir);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
            int Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args);bool Exists(Context& ctx, const KeyObject& key);
            int BeginWriteBatch(Context& ctx);
            int CommitWriteBatch(Context& ctx);
            int DiscardWriteBatch(Context& ctx);
            int Compact(Context& ctx, const KeyObject& start, const KeyObject& end);
            int ListNameSpaces(Context& ctx, DataArray& nss);
            int DropNameSpace(Context& ctx, const Data& ns);
            void Stats(Context& ctx, std::string& str);
            int64_t EstimateKeysNum(Context& ctx, const Data& ns);
            Iterator* Find(Context& ctx, const KeyObject& key);
            const std::string GetErrorReason(int err);
            const FeatureSet GetFeatureSet()
            {
                FeatureSet features;
                features.support_compactfilter = 1;
                features.support_namespace = 1;
                features.support_merge = 0;
                return features;
            }
            int MaxOpenFiles()
            {
                return 10;
            }
    };
}

#endif /* LMDB_ENGINE_HPP_ */
