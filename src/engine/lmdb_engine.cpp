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

#include "lmdb_engine.hpp"
#include "codec.hpp"
#include "util/helpers.hpp"
#include <string.h>
#include <unistd.h>

namespace ardb
{
    static int LMDBCompareFunc(const MDB_val *a, const MDB_val *b)
    {
        return CommonComparator::Compare((const char*) a->mv_data, a->mv_size, (const char*) b->mv_data, b->mv_size);
    }
    LMDBEngineFactory::LMDBEngineFactory(const Properties& props) :
            m_env(NULL), m_env_opened(false)
    {
        ParseConfig(props, m_cfg);
    }

    LMDBEngineFactory::~LMDBEngineFactory()
    {
        mdb_env_close(m_env);
    }

    void LMDBEngineFactory::ParseConfig(const Properties& props, LMDBConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
        conf_get_int64(props, "lmdb.database_max_size", cfg.max_db_size);
        conf_get_int64(props, "lmdb.batch_commit_watermark", cfg.batch_commit_watermark);
        conf_get_bool(props, "lmdb.readahead", cfg.readahead);
    }

    KeyValueEngine* LMDBEngineFactory::CreateDB(const std::string& name)
    {
        if (!m_env_opened)
        {
            mdb_env_create(&m_env);
            int page_size = sysconf(_SC_PAGE_SIZE);
            int rc = mdb_env_set_mapsize(m_env, (m_cfg.max_db_size / page_size) * page_size);
            if (rc != MDB_SUCCESS)
            {
                ERROR_LOG("Invalid db size:%llu for reason:%s", m_cfg.max_db_size, mdb_strerror(rc));
                return NULL;
            }
            char tmp[m_cfg.path.size() + name.size() + 10];
            sprintf(tmp, "%s/%s", m_cfg.path.c_str(), name.c_str());
            m_cfg.path = tmp;
            make_dir(m_cfg.path);
            int env_opt = MDB_NOSYNC | MDB_NOMETASYNC | MDB_WRITEMAP | MDB_MAPASYNC ;
            if(!m_cfg.readahead)
            {
                env_opt |= MDB_NORDAHEAD;
            }
            rc = mdb_env_open(m_env, m_cfg.path.c_str(), env_opt, 0664);
            if (rc != MDB_SUCCESS)
            {
                ERROR_LOG("Failed to open mdb:%s", mdb_strerror(rc));
                return NULL;
            }
            m_env_opened = true;
        }
        LMDBEngine* engine = new LMDBEngine();
        LMDBConfig cfg = m_cfg;
        if (engine->Init(cfg, m_env, name) != 0)
        {
            DELETE(engine);
            return NULL;
        }
        DEBUG_LOG("Create DB:%s at path:%s success", name.c_str(), cfg.path.c_str());
        return engine;
    }

    void LMDBEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }

    void LMDBEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        LMDBEngine* kcdb = (LMDBEngine*) engine;
        if (NULL != kcdb)
        {
            kcdb->Clear();
        }
        DELETE(engine);
    }

    LMDBEngine::LMDBEngine() :
            m_env(NULL), m_dbi(0),m_running(false),m_background(NULL)
    {
    }

    LMDBEngine::~LMDBEngine()
    {
        Close();
    }

    const std::string LMDBEngine::Stats()
    {
        MDB_stat stat;
        MDB_txn* txn;
        int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
        if (0 == rc)
        {
            mdb_stat(txn, m_dbi, &stat);
            mdb_txn_commit(txn);
            std::string stat_info;
            stat_info.append("lmdb version:").append(mdb_version(NULL, NULL, NULL)).append("\r\n");
            stat_info.append("db page size:").append(stringfromll(stat.ms_psize)).append("\r\n");
            stat_info.append("b-tree depath:").append(stringfromll(stat.ms_depth)).append("\r\n");
            stat_info.append("branch pages:").append(stringfromll(stat.ms_branch_pages)).append("\r\n");
            stat_info.append("leaf pages:").append(stringfromll(stat.ms_leaf_pages)).append("\r\n");
            stat_info.append("overflow oages:").append(stringfromll(stat.ms_overflow_pages)).append("\r\n");
            stat_info.append("data items:").append(stringfromll(stat.ms_entries));
            return stat_info;
        }
        else
        {
            return "Failed to get lmdb's stats";
        }

    }

    void LMDBEngine::Run()
    {
        while (m_running)
        {
            MDB_txn* txn;
            int rc = mdb_txn_begin(m_env, NULL, 0, &txn);
            if (rc != 0)
            {
                Thread::Sleep(10, MILLIS);
                continue;
            }
            WriteOperation* op = NULL;
            int count = 0;
            while (m_write_queue.Pop(op))
            {
                switch (op->type)
                {
                    case PUT_OP:
                    {
                        PutOperation* pop = (PutOperation*) op;
                        MDB_val k, v;
                        k.mv_data = const_cast<char*>(pop->key.data());
                        k.mv_size = pop->key.size();
                        v.mv_data = const_cast<char*>(pop->value.data());
                        v.mv_size = pop->value.size();
                        int rc = mdb_put(txn, m_dbi, &k, &v, 0);
                        if(0 != rc)
                        {
                            ERROR_LOG("Write error:%s", mdb_strerror(rc));
                        }
                        DELETE(op);
                        break;
                    }
                    case DEL_OP:
                    {
                        DelOperation* pop = (DelOperation*) op;
                        MDB_val k;
                        k.mv_data = const_cast<char*>(pop->key.data());
                        k.mv_size = pop->key.size();
                        mdb_del(txn, m_dbi, &k, NULL);
                        DELETE(op);
                        break;
                    }
                    case CKP_OP:
                    {
                        mdb_txn_commit(txn);
                        txn = NULL;
                        CheckPointOperation* cop = (CheckPointOperation*) op;
                        cop->Notify();
                        break;
                    }
                    default:
                    {
                        break;
                    }
                }
                count++;
                if (count == m_cfg.batch_commit_watermark || NULL == txn)
                {
                    break;
                }
            }
            if (NULL != txn)
            {
                mdb_txn_commit(txn);
            }
            if (count == 0)
            {
                m_queue_cond.Lock();
                m_queue_cond.Wait(5);
                m_queue_cond.Unlock();
            }
        }
    }

    void LMDBEngine::Clear()
    {
        if (0 != m_dbi)
        {
            LMDBContext& holder = m_ctx_local.GetValue();
            MDB_txn* txn = holder.readonly_txn;
            if (NULL == holder.readonly_txn)
            {
                mdb_txn_begin(m_env, NULL, 0, &txn);
            }
            mdb_drop(txn, m_dbi, 1);
            if (NULL == holder.readonly_txn)
            {
                mdb_txn_commit(txn);
            }
        }
    }
    void LMDBEngine::Close()
    {
        m_running = false;
        DELETE(m_background);
        if (0 != m_dbi)
        {
            mdb_dbi_close(m_env, m_dbi);
            m_dbi = 0;
        }
    }

    int LMDBEngine::Init(const LMDBConfig& cfg, MDB_env *env, const std::string& name)
    {
        m_env = env;
        MDB_txn *txn;
        int rc = mdb_txn_begin(env, NULL, 0, &txn);
        rc = mdb_open(txn, NULL, MDB_CREATE, &m_dbi);
        if (rc != 0)
        {
            ERROR_LOG("Failed to open mdb:%s for reason:%s\n", name.c_str(), mdb_strerror(rc));
            return -1;
        }
        mdb_set_compare(txn, m_dbi, LMDBCompareFunc);
        mdb_txn_commit(txn);
        m_running = true;
        m_background = new Thread(this);
        m_background->Start();
        return 0;
    }

    int LMDBEngine::BeginBatchWrite()
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        holder.batch_write++;
        return 0;
    }
    int LMDBEngine::CommitBatchWrite()
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        holder.batch_write--;
        if (holder.batch_write == 0)
        {
            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
            m_write_queue.Push(ck);
            NotifyBackgroundThread();
            ck->Wait();
            DELETE(ck);
        }
        return 0;
    }
    int LMDBEngine::DiscardBatchWrite()
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        holder.batch_write--;
        if (holder.batch_write == 0)
        {
            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
            m_write_queue.Push(ck);
            NotifyBackgroundThread();
            ck->Wait();
            DELETE(ck);
        }
        return 0;
    }

    void LMDBEngine::CloseTransaction()
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        holder.readonly_txn_ref--;
        if (NULL != holder.readonly_txn && 0 == holder.readonly_txn_ref)
        {
            mdb_txn_commit(holder.readonly_txn);
            holder.readonly_txn = NULL;
        }
    }

    void LMDBEngine::NotifyBackgroundThread()
    {
        m_queue_cond.Lock();
        m_queue_cond.Notify();
        m_queue_cond.Unlock();
    }

    int LMDBEngine::Put(const Slice& key, const Slice& value, const Options& options)
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        MDB_txn *txn = holder.readonly_txn;
        if (NULL == txn || holder.batch_write == 0)
        {
            MDB_val k, v;
            k.mv_data = const_cast<char*>(key.data());
            k.mv_size = key.size();
            v.mv_data = const_cast<char*>(value.data());
            v.mv_size = value.size();
            mdb_txn_begin(m_env, NULL, 0, &txn);
            mdb_put(txn, m_dbi, &k, &v, 0);
            mdb_txn_commit(txn);
            return 0;
        }
        PutOperation* op = new PutOperation;
        op->key.assign((const char*) key.data(), key.size());
        op->value.assign((const char*) value.data(), value.size());
        m_write_queue.Push(op);
        if (holder.batch_write == 0)
        {
            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
            m_write_queue.Push(ck);
            NotifyBackgroundThread();
            ck->Wait();
            DELETE(ck);
            return 0;
        }
        return 0;
    }
    int LMDBEngine::Get(const Slice& key, std::string* value, const Options& options)
    {
        MDB_val k, v;
        k.mv_data = const_cast<char*>(key.data());
        k.mv_size = key.size();
        int rc;
        LMDBContext& holder = m_ctx_local.GetValue();
        MDB_txn *txn = holder.readonly_txn;
        if (NULL == holder.readonly_txn)
        {
            rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
            if (rc != 0)
            {
                ERROR_LOG("Failed to create txn for get for reason:%s", mdb_strerror(rc));
                return -1;
            }
        }
        rc = mdb_get(txn, m_dbi, &k, &v);
        if (NULL == holder.readonly_txn)
        {
            mdb_txn_commit(txn);
        }
        if (0 == rc && NULL != value && NULL != v.mv_data)
        {
            value->assign((const char*) v.mv_data, v.mv_size);
        }
        return rc;
    }
    int LMDBEngine::Del(const Slice& key, const Options& options)
    {
        LMDBContext& holder = m_ctx_local.GetValue();
        MDB_txn *txn = holder.readonly_txn;
        if (NULL == txn || holder.batch_write == 0)
        {
            MDB_val k;
            k.mv_data = const_cast<char*>(key.data());
            k.mv_size = key.size();
            mdb_txn_begin(m_env, NULL, 0, &txn);
            mdb_del(txn, m_dbi, &k, NULL);
            mdb_txn_commit(txn);
            return 0;
        }
        DelOperation* op = new DelOperation;
        op->key.assign((const char*) key.data(), key.size());
        m_write_queue.Push(op);
        if (holder.batch_write == 0)
        {
            CheckPointOperation* ck = new CheckPointOperation(holder.cond);
            m_write_queue.Push(ck);
            NotifyBackgroundThread();
            ck->Wait();
            DELETE(ck);
            return 0;
        }
        return 0;
    }

    Iterator* LMDBEngine::Find(const Slice& findkey, const Options& options)
    {
        MDB_val k, data;
        k.mv_data = const_cast<char*>(findkey.data());
        k.mv_size = findkey.size();
        MDB_cursor *cursor = NULL;
        int rc = 0;
        LMDBContext& holder = m_ctx_local.GetValue();
        if (NULL == holder.readonly_txn)
        {
            rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &holder.readonly_txn);
            if (rc != 0)
            {
                ERROR_LOG("Failed to create txn for iterator for reason:%s", mdb_strerror(rc));
                holder.readonly_txn = NULL;
                return NULL;
            }
        }
        holder.readonly_txn_ref++;
        rc = mdb_cursor_open(holder.readonly_txn, m_dbi, &cursor);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create cursor for reason:%s", mdb_strerror(rc));
            CloseTransaction();
            return NULL;
        }
        rc = mdb_cursor_get(cursor, &k, &data, MDB_SET_RANGE);
        LMDBIterator* iter = new LMDBIterator(this, cursor, rc == 0);
        return iter;
    }

    int LMDBEngine::MaxOpenFiles()
    {
        return 2;
    }

    void LMDBIterator::SeekToFirst()
    {
        int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_FIRST);
        m_valid = rc == 0;
    }
    void LMDBIterator::SeekToLast()
    {
        int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_LAST);
        m_valid = rc == 0;
    }
    void LMDBIterator::Seek(const Slice& target)
    {
        m_key.mv_data = const_cast<char*>(target.data());
        m_key.mv_size = target.size();
        int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_SET_RANGE);
        m_valid = rc == 0;
    }

    void LMDBIterator::Next()
    {
        int rc;
        rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_NEXT);
        m_valid = rc == 0;
    }
    void LMDBIterator::Prev()
    {
        int rc = mdb_cursor_get(m_cursor, &m_key, &m_value, MDB_PREV);
        m_valid = rc == 0;
    }
    Slice LMDBIterator::Key() const
    {
        return Slice((const char*) m_key.mv_data, m_key.mv_size);
    }
    Slice LMDBIterator::Value() const
    {
        return Slice((const char*) m_value.mv_data, m_value.mv_size);
    }
    bool LMDBIterator::Valid()
    {
        return m_valid;
    }

    LMDBIterator::~LMDBIterator()
    {
        mdb_cursor_close(m_cursor);
        m_engine->CloseTransaction();
    }
}

