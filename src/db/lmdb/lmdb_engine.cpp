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
#include "db/db_utils.hpp"
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

#define CHECK_EXPR(expr)  do{\
    int __rc__ = expr; \
    if (MDB_SUCCESS != __rc__)\
    {           \
       ERROR_LOG("LMDB operation:%s error:%s at line:%u", #expr,  mdb_strerror(__rc__), __LINE__); \
    }\
}while(0)

#define DEFAULT_LMDB_LOCAL_MULTI_CACHE_SIZE 10
#define LMDB_PUT_OP 1
#define LMDB_DEL_OP 2
#define LMDB_CKP_OP 3

#define LMDB_META_NAMESPACE "__LMDB_META__"

namespace ardb
{
    static int kEngineNotFound = MDB_NOTFOUND;
    static int LMDBCompareFunc(const MDB_val *a, const MDB_val *b)
    {
        return compare_keys((const char*) a->mv_data, a->mv_size, (const char*) b->mv_data, b->mv_size, false);
    }

    class BGWriteThread;
    static MDB_env *g_mdb_env = NULL;
    static bool g_bgwriter_running = true;
    static ThreadMutex g_bgwriters_mutex;
    static std::vector<BGWriteThread*> g_bgwriters;

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
            bool running;
            MPSCQueue<WriteOperation*> write_queue;
            ThreadMutexLock queue_cond;
            EventCondition event_cond;
            void Run()
            {
                running = true;
                g_bgwriters_mutex.Lock();
                g_bgwriters.push_back(this);
                g_bgwriters_mutex.Unlock();

                MDB_txn* txn = NULL;
                while (g_bgwriter_running)
                {
                    if (NULL == txn)
                    {
                        if (NULL == g_mdb_env || 0 != mdb_txn_begin(g_mdb_env, NULL, 0, &txn))
                        {
                            Thread::Sleep(10, MILLIS);
                            continue;
                        }
                    }
                    WriteOperation* op = NULL;
                    int count = 0;
                    while (write_queue.Pop(op))
                    {
                        count++;
                        switch (op->type)
                        {
                            case LMDB_PUT_OP:
                            {
                                MDB_val k, v;
                                k.mv_data = const_cast<char*>(op->key.data());
                                k.mv_size = op->key.size();
                                v.mv_data = const_cast<char*>(op->value.data());
                                v.mv_size = op->value.size();
                                CHECK_EXPR(mdb_put(txn, op->dbi, &k, &v, 0));
                                DELETE(op);
                                break;
                            }
                            case LMDB_DEL_OP:
                            {
                                MDB_val k;
                                k.mv_data = const_cast<char*>(op->key.data());
                                k.mv_size = op->key.size();
                                mdb_del(txn, op->dbi, &k, NULL);
                                //CHECK_EXPR(mdb_del(txn, op->dbi, &k, NULL));
                                DELETE(op);
                                break;
                            }
                            case LMDB_CKP_OP:
                            {
                                CHECK_EXPR(mdb_txn_commit(txn));
                                txn = NULL;
                                event_cond.Notify();
                                DELETE(op);
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
                        if (NULL == txn)
                        {
                            break;
                        }
                    }

                    if (count == 0)
                    {
                        if (NULL != txn)
                        {
                            CHECK_EXPR(mdb_txn_commit(txn));
                            txn = NULL;
                        }
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
                    running(false)
            {
            }
            bool IsRunning()
            {
                return running;
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
            bool write_dispatched;
            EventCondition cond;
            Buffer encode_buffer_cache;
            /*
             * If there is active cursors, al write op must be cached & write latter.
             */
            std::vector<WriteOperation*> delayed_write_ops;
            BGWriteThread bgwriter;
            LMDBLocalContext() :
                    txn(NULL), txn_ref(0), iter_ref(0), txn_abort(false), write_dispatched(false)
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
            int AcquireTransanction(bool from_iterator = false)
            {
                int rc = 0;
                if (NULL == txn)
                {
                    rc = mdb_txn_begin(g_mdb_env, NULL, 0, &txn);
                    txn_abort = false;
                    txn_ref = 0;
                    iter_ref = 0;
                    write_dispatched = false;
                }
                if (0 == rc)
                {
                    txn_ref++;
                    if (from_iterator)
                    {
                        iter_ref++;
                    }
                }
                return rc;
            }
            int TryReleaseTransanction(bool success, bool from_iterator)
            {
                int rc = 0;
                if (NULL != txn)
                {
                    txn_ref--;
                    //printf("#### %d %d %d \n", txn_ref, txn_abort, write_dispatched);
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

                        if (write_dispatched)
                        {
                            GetBGWriter().WaitWriteComplete();
                        }
                        write_dispatched = false;
                    }
                }
                if (from_iterator && iter_ref > 0)
                {
                    iter_ref--;
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
            }
    };
    static ThreadLocal<LMDBLocalContext> g_ctx_local;

    LMDBEngine::LMDBEngine() :
            m_env(NULL), m_meta_dbi(0)
    {
    }

    LMDBEngine::~LMDBEngine()
    {
        g_bgwriter_running = false;
        for (size_t i = 0; i < g_bgwriters.size(); i++)
        {
            g_bgwriters[i]->Join();
        }
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
        bool recreate_local_txn = false;
        MDB_txn *txn = local_ctx.txn;
        if (NULL == txn)
        {
            CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &txn), false);
        }
        else
        {
            recreate_local_txn = true;
        }
        CHECK_RET(mdb_open(txn, ns.AsString().c_str(), create_if_noexist ? MDB_CREATE : 0, &dbi), false);
        mdb_set_compare(txn, dbi, LMDBCompareFunc);

        std::string ns_key = "ns:" + ns.AsString();
        std::string ns_val = ns.AsString();
        MDB_val ns_key_val, ns_val_val;
        ns_key_val.mv_data = (void *) ns_key.data();
        ns_key_val.mv_size = ns_key.size();
        ns_val_val.mv_data = (void *) ns_val.data();
        ns_val_val.mv_size = ns_val.size();
        mdb_put(txn, m_meta_dbi, &ns_key_val, &ns_val_val, 0);
        bool success = true;
        if (0 != mdb_txn_commit(txn))
        {
            success = false;
        }

        if (success)
        {
            m_dbis[ns] = dbi;
        }

        if (recreate_local_txn)
        {
            local_ctx.txn = NULL;
            int err = mdb_txn_begin(m_env, NULL, 0, &local_ctx.txn);
            if (0 != err)
            {
                FATAL_LOG("Can NOT create transaction for reason:(%d)%s", err, mdb_strerror(err));
            }
        }
        return success;
    }
    int LMDBEngine::Close()
    {
        if (NULL != m_env)
        {
            DBITable::iterator it = m_dbis.begin();
            while (it != m_dbis.end())
            {
                mdb_close(m_env, it->second);
                it++;
            }
            mdb_close(m_env, m_meta_dbi);
            mdb_env_close(m_env);
            m_env = NULL;
        }
        return 0;
    }
    int LMDBEngine::Reopen(const LMDBConfig& cfg)
    {
        Close();
        mdb_env_create(&m_env);
        mdb_env_set_maxdbs(m_env, cfg.max_dbs);
        int page_size = sysconf(_SC_PAGE_SIZE);
        int rc = mdb_env_set_mapsize(m_env, (cfg.max_dbsize / page_size) * page_size);
        if (rc != MDB_SUCCESS)
        {
            ERROR_LOG("Invalid db size:%llu for reason:%s", cfg.max_dbsize, mdb_strerror(rc));
            return -1;
        }
        g_mdb_env = m_env;
        int env_opt = MDB_NOSYNC | MDB_NOMETASYNC | MDB_WRITEMAP | MDB_MAPASYNC;
        if (!cfg.readahead)
        {
            env_opt |= MDB_NORDAHEAD;
        }
        rc = mdb_env_open(m_env, m_dbdir.c_str(), env_opt, 0664);
        if (rc != MDB_SUCCESS)
        {
            ERROR_LOG("Failed to open mdb:%s", mdb_strerror(rc));
            return -1;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        CHECK_RET(mdb_txn_begin(m_env, NULL, 0, &local_ctx.txn), false);
        rc = mdb_open(local_ctx.txn, LMDB_META_NAMESPACE, MDB_CREATE, &m_meta_dbi);
        if (0 != rc)
        {
            ERROR_LOG("Ailed to open meta error:%s", mdb_strerror(rc));
            return -1;
        }
        Context init_ctx;
        MDB_cursor* cursor;
        rc = mdb_cursor_open(local_ctx.txn, m_meta_dbi, &cursor);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create meta cursor for reason:%s", mdb_strerror(rc));
            return -1;
        }
        do
        {
            MDB_val key, val;
            rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
            if (0 == rc)
            {
                std::string ns_key((const char*) key.mv_data, key.mv_size);
                //printf("####%s\n", ns_key.c_str());
                if (has_prefix(ns_key, "ns:"))
                {
                    Data ns;
                    ns.SetString((const char*) val.mv_data, val.mv_size, true);
                    MDB_dbi tmp;
                    //GetDBI(init_ctx, ns, false, tmp);
                    rc = mdb_open(local_ctx.txn, ns.AsString().c_str(), 0, &tmp);
                    if (0 == rc)
                    {
                        m_dbis[ns] = tmp;
                        INFO_LOG("Open db:%s success.", ns.AsString().c_str());
                    }
                    else
                    {
                        ERROR_LOG("Failed to open db:%s with reason:%s", ns.AsString().c_str(), mdb_strerror(rc));
                    }
                }
            }
            else
            {
                WARN_LOG("Read DB ID faile with %s", mdb_strerror(rc));
            }
        }
        while (rc == 0);
        mdb_txn_commit(local_ctx.txn);
        local_ctx.txn = NULL;
        INFO_LOG("Success to open lmdb at %s", m_dbdir.c_str());
        m_cfg = cfg;
        return 0;
    }

    int LMDBEngine::Init(const std::string& dir, const std::string& options)
    {
        Properties props;
        parse_conf_content(options, props);
        LMDBConfig cfg;
        conf_get_int64(props, "database_maxsize", cfg.max_dbsize);
        conf_get_int64(props, "database_maxdbs", cfg.max_dbs);
        conf_get_bool(props, "readahead", cfg.readahead);

        m_dbdir = dir;
        return Reopen(cfg);
    }

    int LMDBEngine::Repair(const std::string& dir)
    {
        ERROR_LOG("Repair not supported in lmdb");
        return ERR_NOTSUPPORTED;
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

        /*
         * write operation MUST dispatch to background write thread if there is exiting iterators,
         * because write operation would invalid current iterator in the same thread.
         */
        if (local_ctx.iter_ref > 0)
        {
            local_ctx.write_dispatched = true;
            local_ctx.GetBGWriter().Put(dbi, k, v);
            return 0;
        }
        int err = local_ctx.AcquireTransanction(false);
        if (0 == err)
        {
            err = mdb_put(local_ctx.txn, dbi, &k, &v, 0);
            local_ctx.TryReleaseTransanction(err == 0, false);
        }
        return ENGINE_ERR(err);
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
        if (local_ctx.iter_ref > 0)
        {
            local_ctx.write_dispatched = true;
            local_ctx.GetBGWriter().Put(dbi, k, v);
            return 0;
        }
        int err = local_ctx.AcquireTransanction(false);
        if (0 == err)
        {
            err = mdb_put(local_ctx.txn, dbi, &k, &v, 0);
            local_ctx.TryReleaseTransanction(err == 0, false);
        }
        return ENGINE_ERR(err);
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
        return ENGINE_ERR(rc);
    }

    int LMDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        MDB_dbi dbi;
        values.resize(keys.size());
        if (!GetDBI(ctx, ctx.ns, false, dbi))
        {
            errs.assign(keys.size(), ERR_ENTRY_NOT_EXIST);
            return 0;
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& key_encode_buffers = local_ctx.GetEncodeBuferCache();
        std::vector<size_t> positions;
        std::vector<MDB_val> ks;
        ks.resize(keys.size());
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
                    errs[i] = ENGINE_ERR(rc);
                }
            }
            if (NULL == local_ctx.txn)
            {
                mdb_txn_commit(txn);
            }
        }
        return ENGINE_NERR(rc);
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
        MDB_val k;
        k.mv_data = const_cast<char*>(encode_buffer.GetRawBuffer());
        k.mv_size = key_len;
        int rc = 0;
        if (local_ctx.iter_ref > 0)
        {
            local_ctx.write_dispatched = true;
            local_ctx.GetBGWriter().Del(dbi, k);
            return 0;
        }
        rc = local_ctx.AcquireTransanction(false);
        if (0 == rc)
        {
            rc = mdb_del(local_ctx.txn, dbi, &k, NULL);
            rc = ENGINE_NERR(rc);
            local_ctx.TryReleaseTransanction(0 == rc, false);
        }
        else
        {
            rc = ENGINE_ERR(rc);
        }
        return rc;
    }

    int LMDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
    {
        ValueObject current;
        int err = Get(ctx, key, current);
        if (0 == err || ERR_ENTRY_NOT_EXIST == err)
        {
            err = g_db->MergeOperation(key, current, op, const_cast<DataArray&>(args));
            if (0 == err)
            {
                return Put(ctx, key, current);
            }
            if (err == ERR_NOTPERFORMED)
            {
                err = 0;
            }
        }
        return err;
    }

    const std::string LMDBEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        return mdb_strerror(err);
    }

    int LMDBEngine::Backup(Context& ctx, const std::string& dir)
    {
        LockGuard<ThreadMutex> guard(m_backup_lock);
        int err = mdb_env_copy(m_env, dir.c_str());
        return ENGINE_ERR(err);
    }
    int LMDBEngine::Restore(Context& ctx, const std::string& dir)
    {
        LockGuard<ThreadMutex> guard(m_backup_lock);
        Close();
        dir_copy(dir, m_dbdir);
        Reopen(m_cfg);
        return 0;
    }

    bool LMDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    int LMDBEngine::BeginWriteBatch(Context& ctx)
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.AcquireTransanction(false);
    }
    int LMDBEngine::CommitWriteBatch(Context& ctx)
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.TryReleaseTransanction(true, false);
    }
    int LMDBEngine::DiscardWriteBatch(Context& ctx)
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
        int rc = 0;
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        if (0 == local_ctx.AcquireTransanction())
        {
            rc = mdb_drop(local_ctx.txn, dbi, 0);
            CHECK_EXPR(rc);
            local_ctx.TryReleaseTransanction(rc == 0, false);
        }
        return rc;
    }
    int64_t LMDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        MDB_dbi dbi;
        if (!GetDBI(ctx, ns, false, dbi))
        {
            return 0;
        }
        MDB_stat stat;
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        local_ctx.AcquireTransanction();
        mdb_stat(local_ctx.txn, dbi, &stat);
        local_ctx.TryReleaseTransanction(true, false);
        return stat.ms_entries;
    }

    Iterator* LMDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        int rc = local_ctx.AcquireTransanction(true);
        LMDBIterator* iter = NULL;
        NEW(iter, LMDBIterator(this,key.GetNameSpace()));
        MDB_dbi dbi;
        if (!GetDBI(ctx, key.GetNameSpace(), false, dbi))
        {
            iter->MarkValid(false);
            return iter;
        }
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
            iter->MarkValid(false);
            local_ctx.TryReleaseTransanction(true, true);
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
        MDB_envinfo envinfo;
        mdb_env_info(m_env, &envinfo);
        stat_info.append("lmdb_version:").append(mdb_version(NULL, NULL, NULL)).append("\r\n");
        stat_info.append("lmdb_mapsize:").append(stringfromll(envinfo.me_mapsize)).append("\r\n");
        stat_info.append("lmdb_maxreaders:").append(stringfromll(envinfo.me_maxreaders)).append("\r\n");
        stat_info.append("lmdb_numreaders:").append(stringfromll(envinfo.me_numreaders)).append("\r\n");

        DataArray nss;
        ListNameSpaces(ctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            MDB_dbi dbi;
            if (GetDBI(ctx, nss[i], false, dbi))
            {
                stat_info.append("\r\nDB[").append(nss[i].AsString()).append("] Stats:\r\n");
                MDB_stat stat;
                LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
                local_ctx.AcquireTransanction();
                mdb_stat(local_ctx.txn, dbi, &stat);
                local_ctx.TryReleaseTransanction(true, false);
                stat_info.append("ms_psize:").append(stringfromll(stat.ms_psize)).append("\r\n");
                stat_info.append("ms_depth:").append(stringfromll(stat.ms_depth)).append("\r\n");
                stat_info.append("ms_branch_pages:").append(stringfromll(stat.ms_branch_pages)).append("\r\n");
                stat_info.append("ms_leaf_pages:").append(stringfromll(stat.ms_leaf_pages)).append("\r\n");
                stat_info.append("ms_overflow_pages:").append(stringfromll(stat.ms_overflow_pages)).append("\r\n");
                stat_info.append("ms_entries:").append(stringfromll(stat.ms_entries)).append("\r\n");
            }
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
        Slice key_slice = next.Encode(local_ctx.GetEncodeBuferCache(), false);
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
        	if(clone_str)
        	{
        		m_value.CloneStringPart();
        	}
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

    void LMDBIterator::Del()
    {
        if (NULL != m_cursor)
        {
            mdb_cursor_del(m_cursor, 0);
        }
    }

    LMDBIterator::~LMDBIterator()
    {
        if (NULL != m_cursor)
        {
            mdb_cursor_close(m_cursor);
        }
        LMDBLocalContext& local_ctx = g_ctx_local.GetValue();
        local_ctx.TryReleaseTransanction(true, true);
    }
}

