/*
 *Copyright (c) 2016-2016, yinqiwen <yinqiwen@gmail.com>
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

#include "thread/lock_guard.hpp"
#include "util/file_helper.hpp"
#include "db/db.hpp"
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include "db/db_utils.hpp"
#include "tokuft_engine.hpp"

#define CHECK_EXPR(expr)  do{\
    int __rc__ = expr; \
    if (0 != __rc__ && DB_NOTFOUND != __rc__)\
    {           \
       ERROR_LOG("TokuFT operation:%s error:%s at line:%u", #expr,  db_strerror(__rc__), __LINE__); \
    }\
}while(0)

namespace ardb
{
    static int nil_callback(DBT const*, DBT const*, void*)
    {
        //printf("####nil callback\n");
        return 0;
    }
    static int ardb_perconaft_compare(DB* db, const DBT* a, const DBT* b)
    {
        return compare_keys((const char*) a->data, a->size, (const char*) b->data, b->size, false);
    }
    static DB_ENV* g_toku_env = NULL;

    struct LocalTransc
    {
            DB_TXN* txn;
            uint32 ref;bool txn_abort;
            LocalTransc() :
                    txn(NULL), ref(0), txn_abort(false)
            {
            }
            DB_TXN* Get()
            {
                if (NULL == txn)
                {
                    CHECK_EXPR(g_toku_env->txn_begin(g_toku_env, NULL, &txn, 0));
                    ref = 0;
                    txn_abort = false;
                }
                if (NULL != txn)
                {
                    ref++;
                }
                return txn;
            }
            DB_TXN* Peek()
            {
                return txn;
            }
            void Release(bool success)
            {
                if (NULL == txn)
                {
                    return;
                }
                ref--;
                if (!txn_abort)
                {
                    txn_abort = !success;
                }
                if (0 == ref)
                {
                    if (txn_abort)
                    {
                        txn->abort(txn);
                    }
                    else
                    {
                        txn->commit(txn, 0);
                    }
                    txn_abort = false;
                    txn = NULL;
                }
            }
    };

    struct PerconaFTLocalContext: public DBLocalContext
    {
            LocalTransc transc;
//            LoacalWriteBatch batch;
//            LocalSnapshot snapshot;
    };

    static ThreadLocal<PerconaFTLocalContext> g_local_ctx;

    PerconaFTEngine::PerconaFTEngine() :
            m_env(NULL)
    {

    }
    PerconaFTEngine::~PerconaFTEngine()
    {

    }

    static void _err_callback(const DB_ENV *, const char * prefix, const char * content)
    {
        ERROR_LOG("%s %s", prefix, content);
    }

    int PerconaFTEngine::Init(const std::string& dir, const std::string& options)
    {
        Properties props;
        parse_conf_content(options, props);

        uint32 env_open_flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOCK | DB_INIT_LOG | DB_RECOVER;
        int env_open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        db_env_create(&m_env, 0);
        m_env->set_default_bt_compare(m_env, ardb_perconaft_compare);
        m_env->set_errcall(m_env, _err_callback);
        int r = m_env->open(m_env, dir.c_str(), env_open_flags, env_open_mode);
        CHECK_EXPR(r);
        DataArray nss;
        if (0 == r)
        {
            g_toku_env = m_env;
            DB* db = m_env->get_db_for_directory(m_env);
            if (NULL != db)
            {
                PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
                DB_TXN* txn = local_ctx.transc.Get();
                DBC *c = NULL;
                CHECK_EXPR(r = db->cursor(db, txn, &c, 0));
                if (0 == r)
                {
                    r = c->c_getf_first(c, 0, nil_callback, NULL);
                }
                while (0 == r)
                {
                    DBT raw_key;
                    DBT raw_val;
                    memset(&raw_key, 0, sizeof(raw_key));
                    memset(&raw_val, 0, sizeof(raw_key));
                    if (0 == c->c_get(c, &raw_key, &raw_val, DB_CURRENT))
                    {
                        //std::string ns_str
                        Data ns;
                        ns.SetString((const char*) raw_key.data, false);
                        INFO_LOG("TokuFT directory db %s:%s", (const char*) raw_key.data, (const char*) raw_val.data);
                        nss.push_back(ns);
                    }
                    r = c->c_getf_next(c, 0, nil_callback, NULL);
                }
                if (NULL != c)
                {
                    c->c_close(c);
                }
                local_ctx.transc.Release(true);
                r = 0;
            }
        }
        for(size_t i = 0; i < nss.size(); i++)
        {
            Context tmp;
            GetFTDB(tmp, nss[i], true);
        }
        return r;
    }

    DB* PerconaFTEngine::GetFTDB(Context& ctx, const Data& ns, bool create_if_missing)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !create_if_missing);
        FTDBTable::iterator found = m_dbs.find(ns);
        if (found != m_dbs.end())
        {
            return found->second;
        }
        if (!create_if_missing)
        {
            return NULL;
        }
        DB *db = NULL;
        int r = 0;
        CHECK_EXPR(r = db_create(&db, m_env, 0));
        if (0 == r)
        {
            DB_TXN* txn = NULL;
            CHECK_EXPR(m_env->txn_begin(m_env, NULL, &txn, 0));
            uint32 open_flags = DB_CREATE | DB_THREAD;
            int open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
            CHECK_EXPR(r = db->open(db, txn, ns.AsString().c_str(), NULL, DB_BTREE, open_flags, open_mode));
            txn->commit(txn, 0);
        }
        if (0 == r)
        {
            m_dbs[ns] = db;
            INFO_LOG("Success to open db:%s", ns.AsString().c_str());
        }
        else
        {
            free(db);
            db = NULL;
            ERROR_LOG("Failed to open db:%s for reason:(%d)%s", ns.AsString().c_str(), r, db_strerror(r));
        }
        return db;
    }

    static DBT to_dbt(const Slice& s)
    {
        DBT a;
        a.data = (void*) s.data();
        a.size = s.size();
        a.ulen = 0;
        a.flags = 0;
        return a;
    }

    int PerconaFTEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        DB* db = GetFTDB(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ss[2];
        local_ctx.GetSlices(key, value, ss);
        DBT key_slice = to_dbt(ss[0]);
        DBT value_slice = to_dbt(ss[1]);
        DB_TXN* txn = local_ctx.transc.Get();
        int r = 0;
        //CHECK_EXPR(r = m_env->txn_begin(m_env, NULL, &txn, 0));
        CHECK_EXPR(r = db->put(db, txn, &key_slice, &value_slice, 0));
        local_ctx.transc.Release(true);
        //txn->commit(txn, 0);
        return r;
    }

    int PerconaFTEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        DB* db = GetFTDB(ctx, ns, ctx.flags.create_if_notexist);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        DBT key_slice = to_dbt(key);
        DBT value_slice = to_dbt(value);
        DB_TXN* txn = local_ctx.transc.Get();
        int r = 0;
        //CHECK_EXPR(r = m_env->txn_begin(m_env, NULL, &txn, 0));
        CHECK_EXPR(r = db->put(db, txn, &key_slice, &value_slice, 0));
        // txn->commit(txn, 0);
        local_ctx.transc.Release(true);
        return r;
    }

    int PerconaFTEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        values.resize(keys.size());
        errs.resize(keys.size());
        for (size_t i = 0; i < keys.size(); i++)
        {
            int err = Get(ctx, keys[i], values[i]);
            errs[i] = err;
        }
        return 0;
    }
    int PerconaFTEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        DB* db = GetFTDB(ctx, key.GetNameSpace(), false);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ks = local_ctx.GetSlice(key);
        DBT key_slice = to_dbt(ks);
        DB_TXN* txn = local_ctx.transc.Peek();
        int r = 0;
        DBT val_slice;
        memset(&val_slice, 0, sizeof(DBT));
        val_slice.flags = DB_DBT_MALLOC;
        //CHECK_EXPR(r = m_env->txn_begin(m_env, NULL, &txn, 0));
        CHECK_EXPR(r = db->get(db, txn, &key_slice, &val_slice, 0));
        if (0 == r)
        {
            Buffer valBuffer((char*) (val_slice.data), 0, val_slice.size);
            value.Decode(valBuffer, true);
        }
        if (r == -30994)
        {
            abort();
        }
        //txn->commit(txn, 0);
        return r == DB_NOTFOUND ? ERR_ENTRY_NOT_EXIST : r;
    }
    int PerconaFTEngine::Del(Context& ctx, const KeyObject& key)
    {
        DB* db = GetFTDB(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ks = local_ctx.GetSlice(key);
        DBT key_slice = to_dbt(ks);
        DB_TXN* txn = local_ctx.transc.Get();
        int r = 0;
        //CHECK_EXPR(r = m_env->txn_begin(m_env, NULL, &txn, 0));
        CHECK_EXPR(r = db->del(db, txn, &key_slice, 0));
        local_ctx.transc.Release(true);
        return r;
    }

    int PerconaFTEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
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

    bool PerconaFTEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    int PerconaFTEngine::BeginWriteBatch()
    {
        return 0;
    }
    int PerconaFTEngine::CommitWriteBatch()
    {
        return 0;
    }
    int PerconaFTEngine::DiscardWriteBatch()
    {

        return 0;
    }

    int PerconaFTEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        return 0;
    }

    int PerconaFTEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        FTDBTable::iterator it = m_dbs.begin();
        while (it != m_dbs.end())
        {
            nss.push_back(it->first);
            it++;
        }
        return 0;
    }

    int PerconaFTEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        DB* db = GetFTDB(ctx, ns, false);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        //m_env->dbremove();
        return 0;
    }

    int64_t PerconaFTEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return -1;
    }

    void PerconaFTEngine::Stats(Context& ctx, std::string& str)
    {
        std::string stats;
        str.append("tokuft_version:").append(db_version(NULL, NULL, NULL)).append("\r\n");
        str.append(stats);
    }

    Iterator* PerconaFTEngine::Find(Context& ctx, const KeyObject& key)
    {
        PerconaFTIterator* iter = NULL;
        NEW(iter, PerconaFTIterator(this,key.GetNameSpace()));
        DB* db = GetFTDB(ctx, key.GetNameSpace(), false);
        if (NULL == db)
        {
            iter->MarkValid(false);
            return iter;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        DB_TXN* txn = local_ctx.transc.Get();
        int r = 0;
        DBC *c;
//        CHECK_EXPR(r = m_env->txn_begin(m_env, NULL, &txn, 0));

        CHECK_EXPR(r = db->cursor(db, txn, &c, 0));
        if (0 != r)
        {
            local_ctx.transc.Release(false);
            iter->MarkValid(false);
            return iter;
        }
        iter->SetCursor(db, txn, c);
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
    bool PerconaFTIterator::Valid()
    {
        return m_valid && NULL != m_cursor;
    }
    void PerconaFTIterator::Next()
    {
        ClearState();
        int ret = m_cursor->c_getf_next(m_cursor, 0, nil_callback, NULL);
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
        CheckBound();
    }
    void PerconaFTIterator::Prev()
    {
        ClearState();
        int ret = m_cursor->c_getf_prev(m_cursor, 0, nil_callback, NULL);
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    void PerconaFTIterator::DoJump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ks = local_ctx.GetSlice(next);
        DBT key_slice = to_dbt(ks);

        int ret = m_cursor->c_getf_set_range(m_cursor, DB_SET_RANGE, &key_slice, nil_callback, NULL);
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    void PerconaFTIterator::Jump(const KeyObject& next)
    {
        DoJump(next);
        if (Valid())
        {
            CheckBound();
        }
    }
    void PerconaFTIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int ret;
        CHECK_EXPR((ret = m_cursor->c_getf_first(m_cursor, 0, nil_callback, NULL)));
        if (ret != 0)
        {
            m_valid = false;
        }

    }
    void PerconaFTIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int ret = 0;
        if (m_iterate_upper_bound_key.GetType() > 0)
        {
            DoJump(m_iterate_upper_bound_key);
            if (!m_valid)
            {
                CHECK_EXPR((ret = m_cursor->c_getf_last(m_cursor, 0, nil_callback, NULL)));
            }
            if (0 == ret)
            {
                Prev();
            }
        }
        else
        {
            CHECK_EXPR((ret = m_cursor->c_getf_last(m_cursor, 0, nil_callback, NULL)));
            if (ret != 0)
            {
                m_valid = false;
                return;
            }
        }

    }
    KeyObject& PerconaFTIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
        Slice key = RawKey();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_key.Decode(kbuf, clone_str);
        m_key.SetNameSpace(m_ns);
        return m_key;
    }
    ValueObject& PerconaFTIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            return m_value;
        }
        Slice key = RawValue();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice PerconaFTIterator::RawKey()
    {
        if (m_raw_key.data == NULL)
        {
            CHECK_EXPR(m_cursor->c_get(m_cursor, &m_raw_key, &m_raw_val, DB_CURRENT));
        }
        //printf("#####%p %d %d\n", m_raw_key.data, m_raw_key.size, m_raw_key.ulen);
        return Slice((const char*) m_raw_key.data, m_raw_key.size);
    }
    Slice PerconaFTIterator::RawValue()
    {
        if (m_raw_key.data == NULL)
        {
            CHECK_EXPR(m_cursor->c_get(m_cursor, &m_raw_key, &m_raw_val, DB_CURRENT));
        }
        return Slice((const char*) m_raw_val.data, m_raw_val.size);
    }
    void PerconaFTIterator::Del()
    {
        RawKey();
        CHECK_EXPR(m_db->del(m_db, m_txn, &m_raw_key, 0));
    }
    void PerconaFTIterator::ClearState()
    {
        memset(&m_raw_key, 0, sizeof(m_raw_key));
        memset(&m_raw_val, 0, sizeof(m_raw_val));
//        m_raw_key.flags = DB_DBT_MALLOC;
//        m_raw_val.flags = DB_DBT_MALLOC;
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void PerconaFTIterator::CheckBound()
    {
        if (NULL != m_cursor && m_iterate_upper_bound_key.GetType() > 0)
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

    PerconaFTIterator::~PerconaFTIterator()
    {
        if (NULL != m_cursor)
        {
            CHECK_EXPR(m_cursor->c_close(m_cursor));
            PerconaFTLocalContext& local_ctx = g_local_ctx.GetValue();
            local_ctx.transc.Release(true);
            //CHECK_EXPR(m_txn->commit(m_txn, 0));
        }
    }
}

