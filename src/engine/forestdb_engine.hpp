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
#ifndef FORESTDB_ENGINE_HPP_
#define FORESTDB_ENGINE_HPP_

#include "forestdb.h"
#include "engine.hpp"
#include "util/config_helper.hpp"
#include "thread/thread_local.hpp"
#include <stack>

namespace ardb
{
    class ForestDBEngine;
    class ForestDBIterator: public Iterator
    {
        private:
            ForestDBEngine* m_engine;
            fdb_iterator* m_iter;
            fdb_status m_iter_status;
            fdb_doc* m_iter_doc;
            void Next();
            void Prev();
            Slice Key() const;
            Slice Value() const;
            bool Valid();
            void SeekToFirst();
            void SeekToLast();
            void Seek(const Slice& target);
        public:
            ForestDBIterator(ForestDBEngine* engine, fdb_iterator* iter) :
                    m_engine(engine), m_iter(iter), m_iter_status(FDB_RESULT_SUCCESS), m_iter_doc(
                    NULL)
            {

            }
            ~ForestDBIterator();
    };

    struct ForestDBConfig
    {
            std::string path;
            int64 buffercache_size;
            int64 purging_interval;
            int64 wal_threshold;
            int64 compaction_threshold;bool compress_enable;bool logenable;
            ForestDBConfig() :
                    buffercache_size(1024 * 1024 * 1024), purging_interval(
                            3600), wal_threshold(4096), compaction_threshold(
                            30), compress_enable(true), logenable(false)
            {
            }
    };
    class ForestDBEngineFactory;
    class ForestDBEngine: public KeyValueEngine
    {
        private:
            struct ContextHolder
            {
                    ForestDBEngine* engine;
                    fdb_file_handle* file;
                    fdb_kvs_handle* kv;bool inited;
                    uint32 batch_ref;
                    fdb_kvs_handle* snapshot;
                    uint32 snapshot_ref;

                    void ReleaseBatchRef();
                    void AddBatchRef();
                    void CommitBatch();bool EmptyBatchRef()
                    {
                        return batch_ref == 0;
                    }
                    ContextHolder() :
                            engine(NULL), file(NULL), kv(NULL), inited(false), batch_ref(
                                    0), snapshot(
                            NULL), snapshot_ref(0)
                    {
                    }
            };
            ThreadLocal<ContextHolder> m_context;
            std::string m_db_path;

            ForestDBConfig m_cfg;
            fdb_config m_options;
            friend class ForestDBEngineFactory;
            int FlushWriteBatch(ContextHolder& holder);
            ContextHolder& GetContextHolder();
        public:
            ForestDBEngine();
            ~ForestDBEngine();
            int Init(const ForestDBConfig& cfg);
            int Put(const Slice& key, const Slice& value,
                    const Options& options);
            int Get(const Slice& key, std::string* value,
                    const Options& options);
            int Del(const Slice& key, const Options& options);
            int BeginBatchWrite();
            int CommitBatchWrite();
            int DiscardBatchWrite();
            Iterator* Find(const Slice& findkey, const Options& options);
            const std::string Stats();
            void CompactRange(const Slice& begin, const Slice& end);
            void ReleaseContextSnapshot();
            int MaxOpenFiles();
    };

    class ForestDBEngineFactory: public KeyValueEngineFactory
    {
        private:
            ForestDBConfig m_cfg;
            static void ParseConfig(const Properties& props,
                    ForestDBConfig& cfg);
        public:
            ForestDBEngineFactory(const Properties& cfg);
            KeyValueEngine* CreateDB(const std::string& name);
            void DestroyDB(KeyValueEngine* engine);
            void CloseDB(KeyValueEngine* engine);
            const std::string GetName()
            {
                return "ForestDB";
            }
    };
}
#endif
