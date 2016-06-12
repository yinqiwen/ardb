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
#ifndef WIREDTIGER_ENGINE_HPP_
#define WIREDTIGER_ENGINE_HPP_

#include "db/engine.hpp"
#include "util/config_helper.hpp"
#include "thread/thread.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/spin_rwlock.hpp"
#include <stack>
#include "tokudb.h"

namespace ardb
{
    class PerconaFTEngine;
    class PerconaFTIterator: public Iterator
    {
        private:
            PerconaFTEngine* m_engine;
            DB* m_db;
            DB_TXN* m_txn;
            DBC* m_cursor;
            Data m_ns;
            KeyObject m_key;
            ValueObject m_value;
            DBT m_raw_key;
            DBT m_raw_val;
            KeyObject m_iterate_upper_bound_key;
            bool m_valid;

            void DoJump(const KeyObject& next);

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

            void ClearState();
            void CheckBound();
            void SetCursor(DB* db, DB_TXN* txn, DBC* c)
            {
                m_db = db;
                m_txn = txn;
                m_cursor = c;
            }
            KeyObject& IterateUpperBoundKey()
            {
                return m_iterate_upper_bound_key;
            }
            friend class PerconaFTEngine;
        public:
            PerconaFTIterator(PerconaFTEngine * e, const Data& ns) :
                m_engine(e),m_db(NULL),m_txn(NULL), m_cursor(NULL), m_ns(ns), m_valid(true)
            {
                ClearState();
            }
            void MarkValid(bool valid)
            {
                m_valid = valid;
            }
            ~PerconaFTIterator();
    };

    class PerconaFTEngine: public Engine
    {
        private:
            DB_ENV* m_env;
            typedef TreeMap<Data, DB*>::Type FTDBTable;
            FTDBTable m_dbs;
            SpinRWLock m_lock;
            friend class PerconaFTIterator;
            friend class WiredTigerLocalContext;
            DB* GetFTDB(Context& ctx,const Data& ns, bool create_if_missing);
            void Close();
        public:
            PerconaFTEngine();
            ~PerconaFTEngine();
            int Init(const std::string& dir, const std::string& options);
            int Repair(const std::string& dir);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
            int Get(Context& ctx, const KeyObject& key, ValueObject& value);
            int MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs);
            int Del(Context& ctx, const KeyObject& key);
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
            const std::string GetErrorReason(int err);
            const FeatureSet GetFeatureSet()
            {
                FeatureSet features;
                features.support_compactfilter = 0;
                features.support_namespace = 1;
                features.support_merge = 0;
                return features;
            }
            int MaxOpenFiles();
    };
}
#endif
