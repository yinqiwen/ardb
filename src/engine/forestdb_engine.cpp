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

#include "forestdb_engine.hpp"
#include "codec.hpp"
#include "util/file_helper.hpp"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

#define CHECK_FDB_RETURN(ret)         do{\
                                       if (0 != ret) \
                                        {             \
                                           ERROR_LOG("ForestDB Error: %s at %s:%d", fdb_error_msg(ret), __FILE__, __LINE__);\
                                         }\
                                       }while(0)
#define ARDB_KV  "ardb_data"

namespace ardb
{
    ForestDBEngineFactory::ForestDBEngineFactory(const Properties& props)
    {
        ParseConfig(props, m_cfg);
    }
    void ForestDBEngineFactory::ParseConfig(const Properties& props,
            ForestDBConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
        conf_get_int64(props, "forestdb.buffercache_size",
                cfg.buffercache_size);
        conf_get_int64(props, "forestdb.purging_interval",
                cfg.purging_interval);
        conf_get_int64(props, "forestdb.wal_threshold", cfg.wal_threshold);
        conf_get_int64(props, "forestdb.compaction_threshold",
                cfg.compaction_threshold);
        conf_get_bool(props, "forestdb.compress_enable", cfg.compress_enable);
    }

    KeyValueEngine* ForestDBEngineFactory::CreateDB(const std::string& name)
    {
        ForestDBEngine* engine = new ForestDBEngine();
        ForestDBConfig cfg = m_cfg;
        char tmp[cfg.path.size() + name.size() + 10];
        sprintf(tmp, "%s/%s", cfg.path.c_str(), name.c_str());
        cfg.path = tmp;
        if (engine->Init(cfg) != 0)
        {
            DELETE(engine);
            ERROR_LOG("Failed to create DB:%s", name.c_str());
            return NULL;
        }
        return engine;
    }
    void ForestDBEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }

    void ForestDBEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }

    void ForestDBIterator::SeekToFirst()
    {
        m_iter_status = fdb_iterator_seek_to_min(m_iter);
    }
    void ForestDBIterator::SeekToLast()
    {
        m_iter_status = fdb_iterator_seek_to_max(m_iter);
    }
    void ForestDBIterator::Seek(const Slice& target)
    {
        m_iter_status = fdb_iterator_seek(m_iter, target.data(), target.size(),
                FDB_ITR_SEEK_HIGHER);
    }
    void ForestDBIterator::Next()
    {
        m_iter_status = fdb_iterator_next(m_iter);
    }
    void ForestDBIterator::Prev()
    {
        m_iter_status = fdb_iterator_prev(m_iter);
    }
    Slice ForestDBIterator::Key() const
    {
        return Slice((const char*)m_iter_doc->key, m_iter_doc->keylen);
    }
    Slice ForestDBIterator::Value() const
    {
        return Slice((const char*)m_iter_doc->body, m_iter_doc->bodylen);
    }
    bool ForestDBIterator::Valid()
    {
        if (NULL != m_iter_doc)
        {
            fdb_doc_free(m_iter_doc);
            m_iter_doc = NULL;
        }
        if (0 == m_iter_status)
        {
            m_iter_status = fdb_iterator_get(m_iter, &m_iter_doc);
            return 0 == m_iter_status;
        }
        return false;
    }

    ForestDBIterator::~ForestDBIterator()
    {
        if (NULL != m_iter_doc)
        {
            fdb_doc_free(m_iter_doc);
        }
        fdb_iterator_close(m_iter);
        m_engine->ReleaseContextSnapshot();
    }

    ForestDBEngine::ForestDBEngine()
    {

    }
    ForestDBEngine::~ForestDBEngine()
    {
    }

    static int __ardb_compare_keys(void *a, size_t len_a, void *b, size_t len_b)
    {
        const char *s1 = (const char *) a;
        const char *s2 = (const char *) b;

        return CommonComparator::Compare(s1, len_a, s2, len_b);
    }
    int ForestDBEngine::Init(const ForestDBConfig& cfg)
    {
        m_options = fdb_get_default_config();
        m_cfg = cfg;
        make_dir(cfg.path);
        m_db_path = cfg.path;

        fdb_status ret = fdb_init(&m_options);
        if (0 != ret)
        {
            ERROR_LOG("Failed to init fdb for reason:%s", fdb_error_msg(ret));
            return -1;
        }
        m_options.buffercache_size = m_cfg.buffercache_size;
        m_options.compaction_mode = FDB_COMPACTION_AUTO;
        m_options.compress_document_body = m_cfg.compress_enable;
        m_options.purging_interval = m_cfg.purging_interval;
        m_options.wal_threshold = m_cfg.wal_threshold;
        m_options.compaction_threshold = m_cfg.compaction_threshold;
        m_options.wal_flush_before_commit = true;

        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return -1;
        }
        return 0;
    }

    ForestDBEngine::ContextHolder& ForestDBEngine::GetContextHolder()
    {
        ContextHolder& holder = m_context.GetValue();
        if (holder.inited)
        {
            return holder;
        }
        holder.engine = this;
        fdb_status ret = FDB_RESULT_SUCCESS;
        if (NULL == holder.file)
        {
            std::string fname = m_cfg.path + "/ardb.data";
            char* kvs_names[1];
            kvs_names[0] = ARDB_KV;
            fdb_custom_cmp_variable cmp = __ardb_compare_keys;
            ret = fdb_open_custom_cmp(&holder.file, fname.c_str(), &m_options,
                    1, kvs_names, &cmp);
            CHECK_FDB_RETURN(ret);
        }
        if (NULL == holder.kv && FDB_RESULT_SUCCESS == ret)
        {
            fdb_kvs_config kvs_config;
            kvs_config = fdb_get_default_kvs_config();
            kvs_config.custom_cmp = __ardb_compare_keys;
            ret = fdb_kvs_open(holder.file, &holder.kv, ARDB_KV, &kvs_config);
            CHECK_FDB_RETURN(ret);
        }
        if (ret == FDB_RESULT_SUCCESS)
        {
            holder.inited = true;
        }
        return holder;
    }

    int ForestDBEngine::BeginBatchWrite()
    {
        GetContextHolder().AddBatchRef();
        return 0;
    }
    int ForestDBEngine::CommitBatchWrite()
    {
        ContextHolder& holder = GetContextHolder();
        holder.ReleaseBatchRef();
        return 0;
    }
    int ForestDBEngine::DiscardBatchWrite()
    {
        ContextHolder& holder = GetContextHolder();
        holder.ReleaseBatchRef();
        return 0;
    }

    int ForestDBEngine::FlushWriteBatch(ContextHolder& holder)
    {
        GetContextHolder().CommitBatch();
        return 0;
    }

    void ForestDBEngine::CompactRange(const Slice& begin, const Slice& end)
    {
        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return;
        }
        fdb_compact(holder.file, NULL);
    }

    void ForestDBEngine::ContextHolder::ReleaseBatchRef()
    {
        if (batch_ref > 0)
        {
            batch_ref--;
            if (batch_ref == 0)
            {
                CommitBatch();
            }
        }
    }
    void ForestDBEngine::ContextHolder::AddBatchRef()
    {
        batch_ref++;
        if (batch_ref == 1)
        {
        }
    }
    void ForestDBEngine::ContextHolder::CommitBatch()
    {
        fdb_status ret;
        ret = fdb_commit(file, FDB_COMMIT_NORMAL);
        CHECK_FDB_RETURN(ret);
    }

    int ForestDBEngine::Put(const Slice& key, const Slice& value,
            const Options& options)
    {
        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return -1;
        }
        fdb_status ret;
        ret = fdb_set_kv(holder.kv, (void *) (key.data()), key.size(),
                (void *) (value.data()), value.size());
        CHECK_FDB_RETURN(ret);
        if (holder.EmptyBatchRef() && 0 == ret)
        {
            ret = fdb_commit(holder.file, FDB_COMMIT_NORMAL);
            CHECK_FDB_RETURN(ret);
        }
        return 0 == ret ? 0 :-1;
    }
    int ForestDBEngine::Get(const Slice& key, std::string* value,
            const Options& options)
    {
        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return -1;
        }
        fdb_kvs_handle* read_kv =
                holder.snapshot != NULL ? holder.snapshot : holder.kv;
        char* val_buf;
        size_t val_len;
        fdb_status ret = fdb_get_kv(read_kv, (void *) (key.data()), key.size(),
                (void**) (&val_buf), &val_len);
        if (0 == ret)
        {
            if (NULL != value)
            {
                value->assign(val_buf, val_len);
            }
            free(val_buf);
        }
        return ret != 0 ? -1 : 0;
    }
    int ForestDBEngine::Del(const Slice& key, const Options& options)
    {
        fdb_status ret;
        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return -1;
        }
        ret = fdb_del_kv(holder.kv, (void *) (key.data()), key.size());
        CHECK_FDB_RETURN(ret);
        if (holder.EmptyBatchRef() && 0 == ret)
        {
            ret = fdb_commit(holder.file, FDB_COMMIT_NORMAL);
            CHECK_FDB_RETURN(ret);
        }
        return 0 == ret ? 0 :-1;
    }

    Iterator* ForestDBEngine::Find(const Slice& findkey, const Options& options)
    {
        fdb_status ret;
        ContextHolder& holder = GetContextHolder();
        if (!holder.inited)
        {
            return NULL;
        }
        if (NULL == holder.snapshot)
        {
            fdb_seqnum_t seq;
            ret = fdb_get_kvs_seqnum(holder.kv, &seq);
            CHECK_FDB_RETURN(ret);
            if (0 != ret)
            {
                return NULL;
            }
            ret = fdb_snapshot_open(holder.kv, &holder.snapshot, seq);
            CHECK_FDB_RETURN(ret);
            if (0 != ret)
            {
                return NULL;
            }
        }
        holder.snapshot_ref++;
        fdb_iterator* iter = NULL;
        ret = fdb_iterator_init(holder.snapshot, &iter, findkey.data(),
                findkey.size(), NULL, 0, FDB_ITR_NONE | FDB_ITR_NO_DELETES);
        CHECK_FDB_RETURN(ret);
        if (0 != ret)
        {
            ReleaseContextSnapshot();
            return NULL;
        }
        return new ForestDBIterator(this, iter);
    }

    void ForestDBEngine::ReleaseContextSnapshot()
    {
        ContextHolder& holder = GetContextHolder();
        if (NULL != holder.snapshot)
        {
            holder.snapshot_ref--;
            if (holder.snapshot_ref == 0)
            {
                fdb_status ret = fdb_kvs_close(holder.snapshot);
                holder.snapshot = NULL;
                CHECK_FDB_RETURN(ret);
            }
        }
    }

    const std::string ForestDBEngine::Stats()
    {
        return "";
    }

    int ForestDBEngine::MaxOpenFiles()
    {
        return 2;
    }

}

