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

#include "db/codec.hpp"
#include "db/db.hpp"
#include "util/helpers.hpp"
#include "thread/lock_guard.hpp"
#include <string.h>
#include <unistd.h>
#include "forestdb_engine.hpp"

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

#define FORESTDB_ERR(err)  (FDB_RESULT_KEY_NOT_FOUND == err ? ERR_ENTRY_NOT_EXIST:err)
#define FORESTDB_NERR(err)  (FDB_RESULT_KEY_NOT_FOUND == err ? 0:err)

namespace ardb
{
    static int fdb_cmp_callback(void *a, size_t len_a, void *b, size_t len_b)
    {
        return compare_keys((const char*) a, len_a, (const char*) b, len_b, false);
    }

    static fdb_compact_decision ardb_fdb_compaction_callback(fdb_file_handle *fhandle, fdb_compaction_status status, const char *kv_store_name, fdb_doc *doc,
            uint64_t last_oldfile_offset, uint64_t last_newfile_offset, void *ctx)
    {
        return FDB_CS_KEEP_DOC;
    }

    struct ForestDBLocalContext
    {
            fdb_file_handle* fdb;
            typedef TreeMap<Data, fdb_kvs_handle*>::Type KVStoreTable;
            KVStoreTable kv_stores;
            uint32 txn_ref;
            uint32 iter_ref;bool txn_abort;
            Buffer encode_buffer_cache;bool inited;
            ForestDBLocalContext() :
                    fdb(NULL), txn_ref(0), iter_ref(0), txn_abort(false), inited(false)
            {
            }

            void Init()
            {
                if (inited)
                {
                    return;
                }
                fdb_config config = fdb_get_default_config();
                config.multi_kv_instances = true;
                config.buffercache_size = 0;
                config.flags = FDB_OPEN_FLAG_RDONLY;

                std::string dbfile = dir;
                DataArray nss;
                fdb_status fs = fdb_open(&fdb, dbfile.c_str(), &config);
                if (fs == FDB_RESULT_SUCCESS)
                {
                    fdb_kvs_name_list name_list;
                    fdb_get_kvs_name_list(fdb, &name_list);
                    for (size_t i = 0; i < name_list.num_kvs_names; i++)
                    {
                        Data ns;
                        ns.SetString(name_list.kvs_names[i], strlen(name_list.kvs_names[i]), false);
                        nss.push_back(ns);
                    }
                    fdb_free_kvs_name_list(&name_list);
                    fdb_close (m_db);
                }
                config = fdb_get_default_config();
                config.multi_kv_instances = true;
                fs = fdb_open_custom_cmp(&fdb, dbfile.c_str(), &config, nss.size(), NULL, NULL);
                inited = true;
                INFO_LOG("Success to open forestdb at %s", dbfile.c_str());
            }

            int AcquireTransanction(bool from_iterator = false)
            {
                fdb_status rc = 0;
                if (0 == txn_ref)
                {
                    rc = fdb_begin_transaction(fdb, FDB_ISOLATION_READ_COMMITTED);
                    txn_abort = false;
                    txn_ref = 0;
                    iter_ref = 0;
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
                if (txn_ref > 0)
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
                            fdb_abort_transaction(fdb);
                        }
                        else
                        {
                            rc = fdb_end_transaction(fdb, FDB_COMMIT_NORMAL);
                        }
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
            ~ForestDBLocalContext()
            {
            }
    };
    static ThreadLocal<ForestDBLocalContext> g_ctx_local;

    ForestDBEngine::ForestDBEngine()
    {
    }

    ForestDBEngine::~ForestDBEngine()
    {
        //Close();
    }

    fdb_kvs_handle* ForestDBEngine::GetKVStore(Context& ctx, const Data& ns, bool create_if_noexist)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !ctx.flags.create_if_notexist);
        KVStoreTable::iterator found = m_kv_stores.find(ns);
        if (found != m_kv_stores.end())
        {
            return found->second;
        }
        if (!create_if_noexist)
        {
            return NULL;
        }
        fdb_kvs_config config = fdb_get_default_kvs_config();
        config.create_if_missing = true;
        config.custom_cmp = fdb_cmp_callback;
        fdb_kvs_handle* kvs;
        fdb_status fs = fdb_kvs_open(m_db, &kvs, ns.AsString().c_str(), &config);
        if (0 != fs)
        {
            ERROR_LOG("Failed to open kv store:%s for reason:(%d)%s", ns.AsString().c_str(), fs, fdb_error_msg(fs));
            return NULL;
        }
        m_kv_stores[ns] = kvs;
        return kvs;
    }

    int ForestDBEngine::Init(const std::string& dir, const Properties& props)
    {
        fdb_config config = fdb_get_default_config();
        config.multi_kv_instances = true;
        config.buffercache_size = 0;
        config.flags = FDB_OPEN_FLAG_RDONLY;

        std::string dbfile = dir;
        DataArray nss;
        fdb_status fs = fdb_open(&m_db, dbfile.c_str(), &config);
        if (fs == FDB_RESULT_SUCCESS)
        {
            fdb_kvs_name_list name_list;
            fdb_get_kvs_name_list(m_db, &name_list);
            for (size_t i = 0; i < name_list.num_kvs_names; i++)
            {
                Data ns;
                ns.SetString(name_list.kvs_names[i], strlen(name_list.kvs_names[i]), false);
                nss.push_back(ns);
            }
            fdb_free_kvs_name_list(&name_list);
            fdb_close (m_db);
        }
        config = fdb_get_default_config();
        config.multi_kv_instances = true;
        fs = fdb_open_custom_cmp(&m_db, dbfile.c_str(), &config, nss.size(), NULL, NULL);
        INFO_LOG("Success to open forestdb at %s", dir.c_str());
        return 0;
    }

    int ForestDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        fdb_status fs = fdb_set_kv(kv, (const void*) encode_buffer.GetRawBuffer(), key_len, (const void*) (encode_buffer.GetRawBuffer() + key_len), value_len);
        return FORESTDB_ERR(fs);
    }
    int ForestDBEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, ns, ctx.flags.create_if_notexist);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        fdb_status fs = fdb_set_kv(kv, (const void*) key.data(), key.size(), (const void*) value.data(), value.size());
        return FORESTDB_ERR(fs);
    }

    int ForestDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        void* val = NULL;
        size_t val_len = 0;
        fdb_status fs = fdb_get_kv(kv, (const void*) encode_buffer.GetRawBuffer(), key_len, &val, &val_len);
        if (0 == fs)
        {
            Buffer valBuffer((char*) (val), 0, val_len);
            value.Decode(valBuffer, true);
        }
        return FORESTDB_ERR(fs);
    }

    int ForestDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
    {
        return 0;
    }
    int ForestDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        fdb_status fs = fdb_del_kv(kv, (const void*) encode_buffer.GetRawBuffer(), key_len);
        return FORESTDB_NERR(fs);
    }

    int ForestDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
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

    bool ForestDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    int ForestDBEngine::BeginTransaction()
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.AcquireTransanction(false);
    }
    int ForestDBEngine::CommitTransaction()
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.TryReleaseTransanction(true, false);
    }
    int ForestDBEngine::DiscardTransaction()
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.TryReleaseTransanction(false, false);
    }
    int ForestDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        //fdb_compact_upto();
        return ERR_NOTSUPPORTED;
    }
    int ForestDBEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {

        return 0;
    }
    int ForestDBEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        int rc = 0;
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        rc = fdb_kvs_remove(local_ctx.fdb, ns.AsString().c_str());
        return rc;
    }
    int64_t ForestDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return 0;
    }

    Iterator* ForestDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        ForestDBIterator* iter = NULL;
        NEW(iter, ForestDBIterator(this,key.GetNameSpace()));
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        int rc = local_ctx.AcquireTransanction(true);
        if (0 != rc)
        {
            iter->MarkValid(false);
            return iter;
        }
        fdb_iterator* fdb_iter = NULL;
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
        rc = fdb_iterator_init(kv, &fdb_iter, NULL, 0, NULL, 0, FDB_ITR_NO_DELETES | FDB_ITR_SKIP_MAX_KEY);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create cursor for reason:%s", mdb_strerror(rc));
            iter->MarkValid(false);
            local_ctx.TryReleaseTransanction(true, true);
            return iter;
        }
        iter->SetIterator(fdb_iter);
        return iter;
    }

    void ForestDBEngine::Stats(Context& ctx, std::string& stat_info)
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        stat_info.append("forestdb_version:").append(fdb_get_lib_version()).append("\r\n");
        if (local_ctx.fdb != NULL)
        {
            stat_info.append("forestdb_file_version:").append(fdb_get_file_version(local_ctx.fdb)).append("\r\n");
        }

//        DataArray nss;
//        ListNameSpaces(ctx, nss);
//        for (size_t i = 0; i < nss.size(); i++)
//        {
//            MDB_dbi dbi;
//            if (GetDBI(ctx, nss[i], false, dbi))
//            {
//                stat_info.append("\r\nDB[").append(nss[i].AsString()).append("] Stats:\r\n");
//                MDB_stat stat;
//                ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
//                local_ctx.AcquireTransanction();
//                mdb_stat(local_ctx.txn, dbi, &stat);
//                local_ctx.TryReleaseTransanction(true, false);
//                stat_info.append("ms_psize:").append(stringfromll(stat.ms_psize)).append("\r\n");
//                stat_info.append("ms_depth:").append(stringfromll(stat.ms_depth)).append("\r\n");
//                stat_info.append("ms_branch_pages:").append(stringfromll(stat.ms_branch_pages)).append("\r\n");
//                stat_info.append("ms_leaf_pages:").append(stringfromll(stat.ms_leaf_pages)).append("\r\n");
//                stat_info.append("ms_overflow_pages:").append(stringfromll(stat.ms_overflow_pages)).append("\r\n");
//                stat_info.append("ms_entries:").append(stringfromll(stat.ms_entries)).append("\r\n");
//            }
//        }
    }

    void ForestDBIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
        if (NULL != m_raw)
        {
            fdb_doc_free(m_raw);
            m_raw = NULL;
        }
    }
    void ForestDBIterator::CheckBound()
    {
        if (m_valid && NULL != m_iter && m_iterate_upper_bound_key.GetType() > 0)
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
    bool ForestDBIterator::Valid()
    {
        return m_valid && NULL != m_iter;
    }
    void ForestDBIterator::Next()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc;
        rc = fdb_iterator_next(m_iter);
        m_valid = rc == 0;
        CheckBound();
    }
    void ForestDBIterator::Prev()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc;
        rc = fdb_iterator_prev(m_iter);
        m_valid = rc == 0;
    }
    void ForestDBIterator::DoJump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        Slice key_slice = next.Encode(local_ctx.GetEncodeBuferCache());
        int rc = fdb_iterator_seek(m_iter, (const void *) key_slice.data(), key_slice.size(), FDB_ITR_SEEK_HIGHER);
        m_valid = rc == 0;
    }
    void ForestDBIterator::Jump(const KeyObject& next)
    {
        DoJump(next);
        CheckBound();
    }
    void ForestDBIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc = fdb_iterator_seek_to_min(m_iter);
        m_valid = rc == 0;
    }
    void ForestDBIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc = fdb_iterator_seek_to_max(m_iter);
        m_valid = rc == 0;
    }
    KeyObject& ForestDBIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
        if (NULL == m_raw)
        {
            fdb_iterator_get(m_iter, &m_raw);
        }
        if (NULL != m_raw)
        {
            Buffer kbuf((char*) (m_raw->key), 0, m_raw->keylen);
            m_key.Decode(kbuf, clone_str);
        }
        return m_key;
    }
    ValueObject& ForestDBIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            return m_value;
        }
        if (NULL == m_raw)
        {
            fdb_iterator_get(m_iter, &m_raw);
        }
        if (NULL != m_raw)
        {
            Buffer kbuf((char*) (m_raw->key), 0, m_raw->keylen);
            m_value.Decode(kbuf, clone_str);
        }
        return m_value;
    }
    Slice ForestDBIterator::RawKey()
    {
        if (NULL == m_raw)
        {
            fdb_iterator_get(m_iter, &m_raw);
        }
        if (NULL != m_raw)
        {
            return Slice((const char*) m_raw->key, m_raw->keylen);
        }
        return Slice();
    }
    Slice ForestDBIterator::RawValue()
    {
        if (NULL == m_raw)
        {
            fdb_iterator_get(m_iter, &m_raw);
        }
        if (NULL != m_raw)
        {
            return Slice((const char*) m_raw->body, m_raw->bodylen);
        }
        return Slice();
    }

    ForestDBIterator::~ForestDBIterator()
    {
        if (NULL != m_iter)
        {
            fdb_iterator_close(m_iter);
        }
        if (NULL != m_raw)
        {
            fdb_doc_free(m_raw);
        }
    }
}

