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

#ifndef LMDB_ENGINE_HPP_
#define LMDB_ENGINE_HPP_

#include "lmdb.h"
#include "engine.hpp"
#include "channel/all_includes.hpp"
#include "util/config_helper.hpp"
#include "thread/thread_local.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "thread/event_condition.hpp"
#include "util/concurrent_queue.hpp"
#include <stack>

namespace ardb
{
    class LMDBEngine;
    class LMDBIterator: public Iterator
    {
        private:
            LMDBEngine *m_engine;
            MDB_cursor * m_cursor;
            MDB_val m_key;
            MDB_val m_value;
            bool m_valid;
            void Next();
            void Prev();
            Slice Key() const;
            Slice Value() const;
            bool Valid();
            void SeekToFirst();
            void SeekToLast();
            void Seek(const Slice& target);
            friend class LMDBEngine;
        public:
            LMDBIterator(LMDBEngine * e, MDB_cursor* iter, bool valid = true) :
                    m_engine(e), m_cursor(iter), m_valid(valid)
            {
                if (valid)
                {
                    int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_GET_CURRENT);
                    m_valid = rc == 0;
                }
            }
            ~LMDBIterator();
    };

    struct LMDBConfig
    {
            std::string path;
            int64 max_db_size;
            int64 batch_commit_watermark;
            bool readahead;
            LMDBConfig() :
                    max_db_size(10 * 1024 * 1024 * 1024LL), batch_commit_watermark(1024),readahead(false)
            {
            }
    };

    enum WriteOperationType
    {
        PUT_OP = 1, DEL_OP, CKP_OP
    };

    struct WriteOperation
    {
            WriteOperationType type;
            WriteOperation(WriteOperationType t) :
                    type(t)
            {
            }
            virtual ~WriteOperation()
            {
            }
    };

    struct PutOperation: public WriteOperation
    {
            std::string key;
            std::string value;
            PutOperation() :
                    WriteOperation(PUT_OP)
            {
            }
    };

    struct DelOperation: public WriteOperation
    {
            std::string key;
            DelOperation() :
                    WriteOperation(DEL_OP)
            {
            }
    };

    struct CheckPointOperation: public WriteOperation
    {
            EventCondition& cond;
            bool execed;
            CheckPointOperation(EventCondition& c) :
                    WriteOperation(CKP_OP),cond(c),execed(false)
            {
            }
            void Notify()
            {
                execed = true;
                cond.Notify();
            }
            void Wait()
            {
                cond.Wait();
            }
    };

    class LMDBEngineFactory;
    class LMDBEngine: public KeyValueEngine, public Runnable
    {
        private:
            MDB_env *m_env;
            MDB_dbi m_dbi;
            struct LMDBContext
            {
                    MDB_txn *readonly_txn;
                    uint32 batch_write;
                    uint32 readonly_txn_ref;
                    EventCondition cond;
                    LMDBContext() :
                            readonly_txn(NULL),batch_write(0),readonly_txn_ref(0)
                    {
                    }
            };
            ThreadLocal<LMDBContext> m_ctx_local;
            std::string m_db_path;

            LMDBConfig m_cfg;

            MPSCQueue<WriteOperation*> m_write_queue;
            ThreadMutexLock m_queue_cond;
            volatile bool m_running;
            Thread* m_background;
            friend class LMDBIterator;
            void Run();
            void CloseTransaction();
            void NotifyBackgroundThread();
        public:
            LMDBEngine();
            ~LMDBEngine();
            int Init(const LMDBConfig& cfg, MDB_env *env, const std::string& name);
            int Put(const Slice& key, const Slice& value, const Options& options);
            int Get(const Slice& key, std::string* value, const Options& options);
            int Del(const Slice& key, const Options& options);
            int BeginBatchWrite();
            int CommitBatchWrite();
            int DiscardBatchWrite();
            const std::string Stats();
            Iterator* Find(const Slice& findkey, const Options& options);
            void Close();
            void Clear();
            int MaxOpenFiles();

    };

    class LMDBEngineFactory: public KeyValueEngineFactory
    {
        private:
            LMDBConfig m_cfg;
            MDB_env *m_env;
            bool m_env_opened;
            static void ParseConfig(const Properties& props, LMDBConfig& cfg);
        public:
            LMDBEngineFactory(const Properties& cfg);
            KeyValueEngine* CreateDB(const std::string& name);
            void DestroyDB(KeyValueEngine* engine);
            void CloseDB(KeyValueEngine* engine);
            const std::string GetName()
            {
                return "LMDB";
            }
            ~LMDBEngineFactory();
    };
}

#endif /* LMDB_ENGINE_HPP_ */
