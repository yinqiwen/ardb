/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#include "engine.hpp"
#include "util/config_helper.hpp"
#include "thread/thread.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "util/concurrent_queue.hpp"
#include <stack>
#include <wiredtiger.h>

namespace ardb
{
    class WiredTigerEngine;
    class WiredTigerIterator: public Iterator
    {
        private:
            WiredTigerEngine* m_engine;
            WT_CURSOR* m_iter;
            WT_ITEM m_key_item, m_value_item;
            int m_iter_ret;
            void Next();
            void Prev();
            Slice Key() const;
            Slice Value() const;
            bool Valid();
            void SeekToFirst();
            void SeekToLast();
            void Seek(const Slice& target);
        public:
            WiredTigerIterator(WiredTigerEngine* engine, WT_CURSOR* iter) :
                    m_engine(engine), m_iter(iter), m_iter_ret(0)
            {

            }
            ~WiredTigerIterator();
    };

    struct WiredTigerConfig
    {
            std::string path;
            int64 batch_commit_watermark;
            std::string init_options;
            std::string init_table_options;
            bool logenable;
            WiredTigerConfig() :
                    batch_commit_watermark(1024), logenable(false)
            {
            }
    };
    class WiredTigerEngine: public Engine
    {
        private:
            WT_CONNECTION* m_db;
            std::string m_db_path;
            friend class WiredTigerIterator;
            void Close();
        public:
            WiredTigerEngine();
            ~WiredTigerEngine();
            int Init(const std::string& dir, const Properties& props);
            int Put(Context& ctx, const KeyObject& key, const ValueObject& value);
            int PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value);
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
}
#endif
