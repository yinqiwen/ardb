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
#include "operation.hpp"
#include "util/config_helper.hpp"
#include "thread/thread.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/event_condition.hpp"
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
    class WiredTigerEngineFactory;
    class WiredTigerEngine: public KeyValueEngine
    {
        private:
            WT_CONNECTION* m_db;
            struct ContextHolder
            {
                    uint32 trasc_ref;
                    uint32 batch_ref;
                    bool readonly_transc;
                    WT_SESSION* session;
                    WT_CURSOR* batch;
                    uint32 batch_write_count;
                    EventCondition cond;
                    WiredTigerEngine* engine;
                    void ReleaseTranscRef();
                    void AddTranscRef();
                    void AddBatchRef();
                    void ReleaseBatchRef();
                    void IncBatchWriteCount();
                    void RestartBatchWrite();
                    bool EmptyBatchRef()
                    {
                        return batch_ref == 0;
                    }
                    ContextHolder() :
                            trasc_ref(0), batch_ref(0), readonly_transc(true),session(NULL), batch(NULL), batch_write_count(0),engine(NULL)
                    {
                    }
            };
            ThreadLocal<ContextHolder> m_context;
            std::string m_db_path;

            WiredTigerConfig m_cfg;

            //MPSCQueue<WriteOperation*> m_write_queue;
            //ThreadMutexLock m_queue_cond;
            //volatile bool m_running;
            //Thread* m_background;

            friend class WiredTigerEngineFactory;
            friend class WiredTigerIterator;

            //void Run();
            //void NotifyBackgroundThread();
            //void WaitWriteComplete(ContextHolder& holder);
            ContextHolder& GetContextHolder();
            void Close();
        public:
            WiredTigerEngine();
            ~WiredTigerEngine();
            int Init(const WiredTigerConfig& cfg);
            int Put(const Slice& key, const Slice& value, const Options& options);
            int Get(const Slice& key, std::string* value, const Options& options);
            int Del(const Slice& key, const Options& options);
            int BeginBatchWrite();
            int CommitBatchWrite();
            int DiscardBatchWrite();
            Iterator* Find(const Slice& findkey, const Options& options);
            const std::string Stats();
            void CompactRange(const Slice& begin, const Slice& end);
            int MaxOpenFiles();
    };

    class WiredTigerEngineFactory: public KeyValueEngineFactory
    {
        private:
            WiredTigerConfig m_cfg;
            static void ParseConfig(const Properties& props, WiredTigerConfig& cfg);
        public:
            WiredTigerEngineFactory(const Properties& cfg);
            KeyValueEngine* CreateDB(const std::string& name);
            void DestroyDB(KeyValueEngine* engine);
            void CloseDB(KeyValueEngine* engine);
            const std::string GetName()
            {
                return "WiredTiger";
            }
    };
}
#endif
