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
#include "db/codec.hpp"
#include "db/db.hpp"
#include "util/helpers.hpp"
#include "thread/lock_guard.hpp"
#include <string.h>
#include <unistd.h>

#define CHECK_RET(expr, fail)  do{\
    int __rc__ = expr; \
    if (MDB_SUCCESS != __rc__)\
    {           \
       ERROR_LOG("LMDB operation:%s error:%s at line:%u", #expr,  mdb_strerror(__rc__), __LINE__); \
       return fail;\
    }\
}while(0)

#define DEFAULT_LMDB_LOCAL_MULTI_CACHE_SIZE 10
#define LMDB_PUT_OP 1
#define LMDB_DEL_OP 2
#define LMDB_CKP_OP 3

#define LMDB_ERR(err)  (MDB_NOTFOUND == err ? ERR_ENTRY_NOT_EXIST:err)

namespace ardb
{
    static int LMDBCompareFunc(const MDB_val *a, const MDB_val *b)
    {
        return compare_keys((const char*) a->mv_data, a->mv_size, (const char*) b->mv_data, b->mv_size, false);
    }

    struct WriteOperation
    {
            std::string key;
            std::string value;
            MDB_dbi dbi;
            uint8 type;
    };

    class BGWriteThread: public Thread
    {
        private:
            MDB_env *env;
            bool running;
            MPSCQueue<WriteOperation*> write_queue;
            ThreadMutexLock queue_cond;
            EventCondition event_cond;
            void Run()
            {
                running = true;
                while (running)
                {
                    MDB_txn* txn;
                    int rc = mdb_txn_begin(env, NULL, 0, &txn);
                    if (rc != 0)
                    {
                        Thread::Sleep(10, MILLIS);
                        continue;
                    }
                    WriteOperation* op = NULL;
                    int count = 0;
                    while (write_queue.Pop(op))
                    {
                        switch (op->type)
                        {
                            case LMDB_PUT_OP:
                            {
                                MDB_val k, v;
                                k.mv_data = const_cast<char*>(op->key.data());
                                k.mv_size = op->key.size();
                                v.mv_data = const_cast<char*>(op->value.data());
                                v.mv_size = op->value.size();
                                mdb_put(txn, op->dbi, &k, &v, 0);
                                DELETE(op);
                                break;
                            }
                            case LMDB_DEL_OP:
                            {
                                MDB_val k;
                                k.mv_data = const_cast<char*>(op->key.data());
                                k.mv_size = op->key.size();
                                mdb_del(txn, op->dbi, &k, NULL);
                                DELETE(op);
                                break;
                            }
                            case LMDB_CKP_OP:
                            {
                                mdb_txn_commit(txn);
                                txn = NULL;
                                event_cond.Notify();
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                        count++;
                    }
                    if (NULL != txn)
                    {
                        int rc = mdb_txn_commit(txn);
                        if (0 != rc)
                        {
                            ERROR_LOG("Transaction commit error:%s", mdb_strerror(rc));
                        }
                    }
                    if (count == 0)
                    {
                        queue_cond.Lock();
                        queue_cond.Wait(5);
                        queue_cond.Unlock();
                    }
                }
            }
            void PushWriteOp(WriteOperation* op)
            {
                write_queue.Push(op);
                queue_cond.Lock();
                queue_cond.Notify();
                queue_cond.Unlock();
            }
        public:
            BGWriteThread() :
                    env(NULL), running(false)
            {
            }
            bool IsRunning()
            {
                return running;
            }
            void AdviceStop()
            {
                running = false;
            }
            void Put(MDB_dbi dbi, MDB_val k, MDB_val v)
            {
                WriteOperation* op = new WriteOperation;
                op->dbi = dbi;
                op->type = LMDB_PUT_OP;
                op->key.assign((const char*) k.mv_data, k.mv_size);
                op->value.assign((const char*) v.mv_data, v.mv_size);
                PushWriteOp(op);
            }
            void Del(MDB_dbi dbi, MDB_val k)
            {
                WriteOperation* op = new WriteOperation;
                op->dbi = dbi;
                op->key.assign((const char*) k.mv_data, k.mv_size);
                op->type = LMDB_DEL_OP;
                PushWriteOp(op);
            }
            void WaitWriteComplete()
            {
                WriteOperation* op = new WriteOperation;
                op->type = LMDB_CKP_OP;
                PushWriteOp(op);
                event_cond.Wait();
            }

    };

    struct LMDBLocalContext
    {
            MDB_txn *txn;
            uint32 txn_ref;
            uint32 iter_ref;
            bool txn_abort;
            bool iter_writable;
            EventCondition cond;
            Buffer encode_buffer_cache;
            std::string string_cache;
            std::vector<std::string> multi_string_cache;
            BGWriteThread bgwriter;
            LMDBLocalContext() :
                    txn(NULL), txn_ref(0), iter_ref(0), txn_abort(false), iter_writable(false)
            //, iter_txn(NULL),iter_txn_ref(0)
            {
            }
            BGWriteThread& GetBGWriter()
            {
                if (!bgwriter.IsRunning())
                {
                    bgwriter.Start();
                    /*
                     * wait until it started
                     */
                    while (!bgwriter.IsRunning())
                    {
                        usleep(10);
                    }
                }
                return bgwriter;
            }
            int AcquireTransanction(MDB_env *env)
            {
                int rc = 0;
                if (NULL == txn)
                {
                    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
                    txn_abort = false;
                }
                if (0 == rc)
                {
                    txn_ref++;
                }
                return rc;
            }
            int TryReleaseTransanction(bool success, bool from_iterator)
            {
                int rc = 0;
                if (NULL != txn)
                {
                    txn_ref--;
                    //printf("#### %d %d\n", txn_ref, txn_abort);
                    if (!txn_abort)
                    {
                        txn_abort = !success;
                    }
                    if (txn_ref == 0)
                    {
                        if (txn_abort)
                        {
                            mdb_txn_abort(txn);
                        }
                        else
                        {
                            rc = mdb_txn_commit(txn);
                        }
                        txn = NULL;
                    }
                }
                if (from_iterator && iter_ref > 0)
                {
                    iter_ref--;
                    if (iter_ref == 0 && iter_writable)
                    {
                        GetBGWriter().WaitWriteComplete();
                    }
                }
                return rc;
            }
            Buffer& GetEncodeBuferCache()
            {
                encode_buffer_cache.Clear();
                return encode_buffer_cache;
            }
            ~LMDBLocalContext()
            {
                bgwriter.AdviceStop();
                bgwriter.Join();
            }
    };
    static ThreadLocal<LMDBLocalContext> g_ctx_local;

    struct LMDBTransactionGuard
    {
            MDB_env *env;
            MDB_txn *txn;
            int rc;
            int& err;
            LMDBTransactionGuard(MDB_env* e, int& errcode) :
                    env(e), txn(NULL), rc(0), err(errcode)
            {
                rc = mdb_txn_begin(env, NULL, 0, &txn);
            }
            ~LMDBTransactionGuard()
            {
                if (NULL != txn)
                {
                    if (0 == rc)
                    {
                        rc = mdb_txn_commit(txn);
                    }
                    else
                    {
                        mdb_txn_abort(txn);
                    }
                }
                err = rc;
            }
    };

    LMDBEngine::LMDBEngine() :
            m_env(NULL), m_meta_dbi(0)
    {
    }

    LMDBEngine::~LMDBEngine()
    {
        //Close();
    }

    bool LMDBEngine::GetDBI(Context& ctx, const Data& ns, bool create_if_noexist, MDB_dbi& dbi)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !ctx.flags.create_if_notexist);
        DBITable::iterator found = m_dbis.find(ns);
        if (found != m_dbis.end())
        {
            dbi = found->second;
            return true;
        }
        if (!create_if_noexist)
        {
            return false;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        MDB_txn *txn = local_ctx.txn;
        if (NULL == txn)
        {
            CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &txn), false);
        }
        CHECK_RET(mdb_open(txn, ns.AsString().c_str(), MDB_CREATE, &dbi), false);
        mdb_set_compare(txn, dbi, LMDBCompareFunc);
        CHECK_RET(mdb_txn_commit(txn), false);
        if (txn == local_ctx.txn)
        {
            CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &local_ctx.txn), false);
        }
        m_dbis[ns] = dbi;
        return true;
    }

    int LMDBEngine::Init(const std::string& dir, const Properties& props)
    {
        conf_get_int64(props, "lmdb.database_maxsize", m_cfg.max_dbsize);
        conf_get_int64(props, "lmdb.database_maxdbs", m_cfg.max_dbs);
        conf_get_int64(props, "lmdb.batch_commit_watermark", m_cfg.batch_commit_watermark);
        conf_get_bool(props, "lmdb.readahead", m_cfg.readahead);

        mdb_env_create(&m_env);
        mdb_env_set_maxdbs(m_env, m_cfg.max_dbs);
        int page_size = sysconf(_SC_PAGE_SIZE);
        int rc = mdb_env_set_mapsize(m_env, (m_cfg.max_dbsize / page_size) * page_size);
        if (rc != MDB_SUCCESS)
        {
            ERROR_LOG("Invalid db size:%llu for reason:%s", m_cfg.max_dbsize, mdb_strerror(rc));
            return -1;
        }
        int env_opt = MDB_NOSYNC | MDB_NOMETASYNC | MDB_WRITEMAP | MDB_MAPASYNC;
        if (!m_cfg.readahead)
        {
            env_opt |= MDB_NORDAHEAD;
        }
        rc = mdb_env_open(m_env, dir.c_str(), env_opt, 0664);
        if (rc != MDB_SUCCESS)
        {
            ERROR_LOG("Failed to open mdb:%s", mdb_strerror(rc));
            return -1;
        }
        MDB_txn *txn;
        CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &txn), -1);
        CHECK_RET(mdb_open(txn, "__lmdb_meta__", MDB_CREATE, &m_meta_dbi), -1);
        MDB_cursor* cursor;
        CHECK_RET(mdb_cursor_open(txn, m_meta_dbi, &cursor), -1);
        MDB_val key, data;
        while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0)
        {
            std::string name((const char*) key.mv_data, key.mv_size);
            if (has_prefix(name, "ns:"))
            {
                Data ns;
                ns.SetString((const char*) data.mv_data, data.mv_size, true);
                MDB_dbi dbi;
                CHECK_RET(mdb_open(txn, ns.AsString().c_str(), MDB_CREATE, &dbi), -1);
                m_dbis[ns] = dbi;
            }
        }
        mdb_cursor_close(cursor);
        CHECK_RET(mdb_txn_commit(txn), false);
        INFO_LOG("Success to open lmdb at %s", dir.c_str());
        return 0;
    }

    int LMDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        MDB_val k, v;
        k.mv_data = const_cast<char*>(encode_buffer.GetRawBuffer());
        k.mv_size = key_len;
        v.mv_data = const_cast<char*>(encode_buffer.GetRawBuffer() + key_len);
        v.mv_size = value_len;
        MDB_txn *txn = local_ctx.txn;
        if (NULL == txn)
        {
            CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &txn), -1);
            CHECK_RET(mdb_put(txn, dbi, &k, &v, 0), -1);
            CHECK_RET(mdb_txn_commit(txn), -1);
            return 0;
        }
        else
        {
            /*
             * write operation MUST spread to write thread if there is exiting iterators,
             * because write operation would invalid current iterator in the same thread.
             */
            if (local_ctx.iter_ref > 0)
            {
                local_ctx.iter_writable = true;
                local_ctx.GetBGWriter().Put(dbi, k, v);
                return 0;
            }
            CHECK_RET(mdb_put(local_ctx.txn, dbi, &k, &v, 0), -1);
            return 0;
        }
    }
    int LMDBEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, ns, ctx.flags.create_if_notexist, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }

        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        MDB_val k, v;
        k.mv_data = const_cast<char*>(key.data());
        k.mv_size = key.size();
        v.mv_data = const_cast<char*>(value.data());
        v.mv_size = value.size();
        MDB_txn *txn = local_ctx.txn;
        if (NULL == txn)
        {
            CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &txn), -1);
            CHECK_RET(mdb_put(txn, dbi, &k, &v, 0), -1);
            CHECK_RET(mdb_txn_commit(txn), -1);
            return 0;
        }
        else
        {
            if (local_ctx.iter_ref > 0)
            {
                local_ctx.iter_writable = true;
                local_ctx.GetBGWriter().Put(dbi, k, v);
                return 0;
            }
            CHECK_RET(mdb_put(local_ctx.txn, dbi, &k, &v, 0), -1);
            return 0;
        }
    }

    int LMDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, key.GetNameSpace(), false, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        MDB_val k, v;
        k.mv_data = const_cast<char*>(encode_buffer.GetRawBuffer());
        k.mv_size = key_len;
        MDB_txn *txn = local_ctx.txn;
        int rc = 0;
        if (NULL == txn)
        {
            rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
        }
        if (0 == rc)
        {
            rc = mdb_get(txn, dbi, &k, &v);
            if (0 == rc)
            {
                Buffer valBuffer((char*) (v.mv_data), 0, v.mv_size);
                value.Decode(valBuffer, true);
            }
            if (NULL == local_ctx.txn)
            {
                mdb_txn_commit(txn);
            }
        }
        return LMDB_ERR(rc);
    }

    int LMDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, ctx.ns, false, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& key_encode_buffers = local_ctx.GetEncodeBuferCache();
        std::vector<size_t> positions;
        std::vector<MDB_val> ks;
        ks.resize(keys.size());
        values.resize(keys.size());
        errs.resize(keys.size());
        for (size_t i = 0; i < keys.size(); i++)
        {
            size_t mark = key_encode_buffers.GetWriteIndex();
            keys[i].Encode(key_encode_buffers);
            positions.push_back(key_encode_buffers.GetWriteIndex() - mark);
        }
        for (size_t i = 0; i < keys.size(); i++)
        {
            ks[i].mv_data = (void*) key_encode_buffers.GetRawReadBuffer();
            ks[i].mv_size = positions[i];
            key_encode_buffers.AdvanceReadIndex(positions[i]);
        }

        MDB_txn *txn = local_ctx.txn;
        int rc = 0;
        if (NULL == txn)
        {
            rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
        }
        if (0 == rc)
        {
            for (size_t i = 0; i < keys.size(); i++)
            {
                MDB_val v;
                rc = mdb_get(txn, dbi, &ks[i], &v);
                if (0 == rc)
                {
                    Buffer valBuffer((char*) (v.mv_data), 0, v.mv_size);
                    values[i].Decode(valBuffer, true);
                }
                else
                {
                    errs[i] = LMDB_ERR(rc);
                }
            }
            if (NULL == local_ctx.txn)
            {
                mdb_txn_commit(txn);
            }
        }
        return rc;
    }
    int LMDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, key.GetNameSpace(), false, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        MDB_val k, v;
        k.mv_data = const_cast<char*>(encode_buffer.GetRawBuffer());
        k.mv_size = key_len;
        int rc = 0;
        MDB_txn *txn = local_ctx.txn;
        if (local_ctx.iter_ref > 0)
        {
            local_ctx.iter_writable = true;
            local_ctx.GetBGWriter().Del(dbi, k);
            return 0;
        }
        if (NULL == txn)
        {
            rc = mdb_txn_begin(m_env, NULL, 0, &txn);
        }
        if (0 == rc)
        {
            rc = mdb_del(txn, dbi, &k, NULL);
            if (NULL == local_ctx.txn)
            {
                mdb_txn_commit(txn);
            }
        }
        return rc;
    }

    int LMDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
        ValueObject current;
        int err = Get(ctx, key, current);
        if(0 == err || ERR_ENTRY_NOT_EXIST == err)
        {
            err = g_db->MergeOperation(key, current, op, const_cast<DataArray&>(args));
            if(0 == err)
            {
                return Put(ctx, key, current);
            }
        }
        return err;
    }

    bool LMDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    int LMDBEngine::BeginTransaction()
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        if (local_ctx.txn == NULL)
        {
            int rc = mdb_txn_begin(m_env, NULL, 0, &local_ctx.txn);
            if (0 != rc)
            {
                return rc;
            }
        }
        local_ctx.txn_ref++;
        return 0;
    }
    int LMDBEngine::CommitTransaction()
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.TryReleaseTransanction(true, false);
    }
    int LMDBEngine::DiscardTransaction()
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.TryReleaseTransanction(false, false);
    }
    int LMDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        return ERR_NOTSUPPORTED;
    }
    int LMDBEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        DBITable::iterator it = m_dbis.begin();
        while (it != m_dbis.end())
        {
            nss.push_back(it->first);
            it++;
        }
        return 0;
    }
    int LMDBEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, ns, false, dbi))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        if (NULL != local_ctx.txn)
        {
            return -1;
        }
        int rc = 0;
        {
            LMDBTransactionGuard guard(m_env, rc);
            if (NULL != guard.txn)
            {
                mdb_drop(guard.txn, dbi, 0);
            }
        }
        return rc;
    }
    int64_t LMDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return 0;
    }

    Iterator* LMDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        LMDBIterator* iter = NULL;
        NEW(iter, LMDBIterator(this,key.GetNameSpace()));
        MDB_dbi dbi;
        if (!GetDBI(ctx, key.GetNameSpace(), false, dbi))
        {
            iter->MarkValid(false);
            return iter;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        int rc = local_ctx.AcquireTransanction(m_env);
        if (0 != rc)
        {
            iter->MarkValid(false);
            return iter;
        }
        MDB_cursor* cursor;
        rc = mdb_cursor_open(local_ctx.txn, dbi, &cursor);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create cursor for reason:%s", mdb_strerror(rc));
            //CloseTransaction();
            iter->MarkValid(false);
            return iter;
        }
        iter->SetCursor(cursor);
        if (key.GetType() > 0)
        {
            if (!ctx.flags.iterate_multi_keys)
            {
                if (!ctx.flags.iterate_no_upperbound)
                {
                    KeyObject& upperbound_key = iter->IterateUpperBoundKey();
                    upperbound_key.SetNameSpace(key.GetNameSpace());
                    if (key.GetType() == KEY_META)
                    {
                        upperbound_key.SetType(KEY_END);
                    }
                    else
                    {
                        upperbound_key.SetType(key.GetType() + 1);
                    }
                    upperbound_key.SetKey(key.GetKey());
                    upperbound_key.CloneStringPart();
                }
            }
            iter->Jump(key);
        }
        else
        {
            iter->JumpToFirst();
        }
        return iter;
    }

    void LMDBEngine::Stats(Context& ctx, std::string& stat_info)
    {
        MDB_stat stat;
        MDB_txn* txn;
        int rc = mdb_txn_begin(m_env, NULL, MDB_RDONLY, &txn);
        if (0 == rc)
        {
            //mdb_stat(txn, m_dbi, &stat);
            mdb_txn_commit(txn);
            stat_info.append("lmdb version:").append(mdb_version(NULL, NULL, NULL)).append("\r\n");
            stat_info.append("db page size:").append(stringfromll(stat.ms_psize)).append("\r\n");
            stat_info.append("b-tree depath:").append(stringfromll(stat.ms_depth)).append("\r\n");
            stat_info.append("branch pages:").append(stringfromll(stat.ms_branch_pages)).append("\r\n");
            stat_info.append("leaf pages:").append(stringfromll(stat.ms_leaf_pages)).append("\r\n");
            stat_info.append("overflow oages:").append(stringfromll(stat.ms_overflow_pages)).append("\r\n");
            stat_info.append("data items:").append(stringfromll(stat.ms_entries));
        }
    }

    void LMDBIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void LMDBIterator::CheckBound()
    {
        if (m_valid && NULL != m_cursor && m_iterate_upper_bound_key.GetType() > 0)
        {
            if (m_valid)
            {
                if (Key(false).Compare(m_iterate_upper_bound_key) >= 0)
                {
                    m_valid = false;
                }
            }
        }
    }
    bool LMDBIterator::Valid()
    {
        return m_valid && NULL != m_cursor;
    }
    void LMDBIterator::Next()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int rc;
        rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_NEXT);
        m_valid = rc == 0;
        CheckBound();
    }
    void LMDBIterator::Prev()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int rc;
        rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_PREV);
        m_valid = rc == 0;
    }
    void LMDBIterator::DoJump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Slice key_slice = next.Encode(local_ctx.GetEncodeBuferCache());
        m_raw_key.mv_data = (void *) key_slice.data();
        m_raw_key.mv_size = key_slice.size();
        int rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_SET_RANGE);
        m_valid = rc == 0;
    }
    void LMDBIterator::Jump(const KeyObject& next)
    {
        DoJump(next);
        CheckBound();
    }
    void LMDBIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_FIRST);
        m_valid = rc == 0;
    }
    void LMDBIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int rc;
        if (m_iterate_upper_bound_key.GetType() > 0)
        {
            DoJump(m_iterate_upper_bound_key);
            if (!m_valid)
            {
                rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_LAST);
                if (0 == rc)
                {
                    Prev();
                }
                else
                {
                    m_valid = false;
                    return;
                }
            }
            else
            {
                Prev();
            }
            CheckBound();
        }
        else
        {
            rc = mdb_cursor_get(m_cursor, &m_raw_key, &m_raw_val, MDB_LAST);
            m_valid = rc == 0;
        }
    }
    KeyObject& LMDBIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
        Buffer kbuf((char*) (m_raw_key.mv_data), 0, m_raw_key.mv_size);
        m_key.Decode(kbuf, clone_str);
        m_key.SetNameSpace(m_ns);
        return m_key;
    }
    ValueObject& LMDBIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            return m_value;
        }
        Buffer kbuf((char*) (m_raw_val.mv_data), 0, m_raw_val.mv_size);
        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice LMDBIterator::RawKey()
    {
        return Slice((const char*) m_raw_key.mv_data, m_raw_key.mv_size);
    }
    Slice LMDBIterator::RawValue()
    {
        return Slice((const char*) m_raw_val.mv_data, m_raw_val.mv_size);
    }

    LMDBIterator::~LMDBIterator()
    {
        mdb_cursor_close(m_cursor);
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        local_ctx.TryReleaseTransanction(true, true);
    }
}

