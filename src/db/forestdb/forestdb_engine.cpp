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
#include "db/db_utils.hpp"
#include "util/helpers.hpp"
#include "thread/lock_guard.hpp"
#include <string.h>
#include <unistd.h>
#include "forestdb_engine.hpp"

#define CHECK_EXPR(expr)  do{\
    int __rc__ = expr; \
    if (FDB_RESULT_SUCCESS != __rc__)\
    {           \
       ERROR_LOG("forestdb operation:%s error:%s at line:%u", #expr,  fdb_error_msg((fdb_status)__rc__), __LINE__); \
    }\
}while(0)
#define DEFAULT_LMDB_LOCAL_MULTI_CACHE_SIZE 10

#define FORESTDB_ERR(err)  (FDB_RESULT_KEY_NOT_FOUND == err ? ERR_ENTRY_NOT_EXIST:err)
#define FORESTDB_NERR(err)  (FDB_RESULT_KEY_NOT_FOUND == err ? 0:err)

namespace ardb
{
    static int fdb_cmp_callback(void *a, size_t len_a, void *b, size_t len_b)
    {
        return compare_keys((const char*) a, len_a, (const char*) b, len_b, false);
    }

    static fdb_compact_decision ardb_fdb_compaction_callback(fdb_file_handle *fhandle, fdb_compaction_status status, const char *kv_store_name, fdb_doc *doc, uint64_t last_oldfile_offset, uint64_t last_newfile_offset, void *ctx)
    {
        return FDB_CS_KEEP_DOC;
    }

    static ForestDBEngine* g_fdb_engine = NULL;

    struct ForestDBLocalContext
    {
            fdb_file_handle* fdb;
            typedef TreeMap<Data, fdb_kvs_handle*>::Type KVStoreTable;
            KVStoreTable kv_stores;
            uint32 txn_ref;bool txn_abort;
            Buffer encode_buffer_cache;bool inited;
            ForestDBLocalContext() :
                    fdb(NULL), txn_ref(0), txn_abort(false), inited(false)
            {
            }
            fdb_kvs_handle* GetKVStore(const Data& ns, bool create_if_missing)
            {
                KVStoreTable::iterator found = kv_stores.find(ns);
                if (found != kv_stores.end())
                {
                    return found->second;
                }
//                if (!create_if_missing)
//                {
//                    return NULL;
//                }
                fdb_kvs_config config = fdb_get_default_kvs_config();
                config.create_if_missing = create_if_missing;
                config.custom_cmp = fdb_cmp_callback;
                fdb_kvs_handle* kvs;
                fdb_status fs = fdb_kvs_open(fdb, &kvs, ns.AsString().c_str(), &config);
                if (0 != fs)
                {
                    ERROR_LOG("Failed to open kv store:%s for reason:(%d)%s", ns.AsString().c_str(), fs, fdb_error_msg(fs));
                    return NULL;
                }
                INFO_LOG("Success to open db:%s", ns.AsString().c_str());
                kv_stores[ns] = kvs;
                DBHelper::AddNameSpace(ns);
                return kvs;
            }
            bool Init()
            {
                if (inited)
                {
                    return true;
                }
                fdb_config config = fdb_get_default_config();
                config = fdb_get_default_config();
                config.multi_kv_instances = true;
//                config.cleanup_cache_onclose = true;
//                config.auto_commit = true;
                config.compaction_cb = ardb_fdb_compaction_callback;
                //config.compaction_mode = FDB_COMPACTION_AUTO;

                std::string dbfile = g_db->GetConf().data_base_path + "/ardb.fdb";
                DataArray nss;
                DBHelper::ListNameSpaces(nss);
                fdb_status fs;
                if (nss.empty())
                {
                    fs = fdb_open(&fdb, dbfile.c_str(), &config);
                }
                else
                {
                    const char* names[nss.size()];
                    fdb_custom_cmp_variable cmps[nss.size()];
                    for (size_t i = 0; i < nss.size(); i++)
                    {
                        names[i] = nss[i].CStr();
                        cmps[i] = fdb_cmp_callback;
                    }
                    fs = fdb_open_custom_cmp(&fdb, dbfile.c_str(), &config, nss.size(), (char **) names, cmps);
                    if (0 == fs)
                    {
                        for (size_t i = 0; i < nss.size(); i++)
                        {
                            GetKVStore(nss[i], false);
                        }
                    }
                }
                if (0 != fs)
                {
                    ERROR_LOG("Failed to  open db for reason:(%d)%s", fs, fdb_error_msg(fs));
                    return false;
                }
                inited = true;
                INFO_LOG("Success to open forestdb at %s in thread:%d", dbfile.c_str(), pthread_self());
                return true;
            }

            int AcquireTransanction()
            {
                int rc = 0;
                if (0 == txn_ref)
                {
                    rc = fdb_begin_transaction(fdb, FDB_ISOLATION_READ_COMMITTED);
                    txn_abort = false;
                    txn_ref = 0;
                }
                if (0 == rc)
                {
                    txn_ref++;
                }
                return rc;
            }
            int TryReleaseTransanction(bool success)
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
                return rc;
            }
            Buffer& GetEncodeBuferCache()
            {
                encode_buffer_cache.Clear();
                return encode_buffer_cache;
            }
            ~ForestDBLocalContext()
            {
                KVStoreTable::iterator it = kv_stores.begin();
                while (it != kv_stores.end())
                {
                    fdb_kvs_close(it->second);
                    it++;
                }
                fdb_close(fdb);
            }
    };
    static ThreadLocal<ForestDBLocalContext> g_ctx_local;
    static ForestDBLocalContext& GetDBLocalContext()
    {
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        local_ctx.Init();
        return local_ctx;
    }

    ForestDBEngine::ForestDBEngine()
    //:m_meta_db(NULL), m_meta_kv(NULL)
    {
        g_fdb_engine = this;
    }

    ForestDBEngine::~ForestDBEngine()
    {
        //Close();
    }

//    void ForestDBEngine::AddNamespace(const Data& ns)
//    {
//        RWLockGuard<SpinRWLock> guard(m_lock, false);
//        fdb_status fs;
//        std::string key = "ns:" + ns.AsString();
//        std::string val = ns.AsString();
//        CHECK_EXPR(fs = fdb_set_kv(m_meta_kv, (const void* ) key.data(), key.size(), (const void* ) val.data(), val.size()));
//        CHECK_EXPR(fs = fdb_commit(m_meta_db, FDB_COMMIT_NORMAL));
//        m_nss.insert(ns);
//    }
//    int ForestDBEngine::ListNameSpaces(DataArray& nss)
//    {
//        DataSet::iterator it = m_nss.begin();
//        while (it != m_nss.end())
//        {
//            Data ns = *it;
//            nss.push_back(ns);
//            it++;
//        }
//        return 0;
//    }
    fdb_kvs_handle* ForestDBEngine::GetKVStore(Context& ctx, const Data& ns, bool create_if_noexist)
    {
        fdb_kvs_handle* kv = NULL;
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        kv = local_ctx.GetKVStore(ns, create_if_noexist);
        return kv;
    }

    int ForestDBEngine::Init(const std::string& dir, const Properties& props)
    {
//        std::string dbfile = g_db->GetConf().data_base_path + "/ardb.meta.fdb.";
//        fdb_config config = fdb_get_default_config();
//        fdb_status fs;
//        CHECK_EXPR(fs = fdb_open(&m_meta_db, dbfile.c_str(), &config));
//        if (0 != fs)
//        {
//            return fs;
//        }
//        fdb_kvs_config kv_conig = fdb_get_default_kvs_config();
//        CHECK_EXPR(fs = fdb_kvs_open_default(m_meta_db, &m_meta_kv, &kv_conig));
//        if (0 != fs)
//        {
//            return fs;
//        }
//        fdb_iterator* iter = NULL;
//        fdb_iterator_opt_t opt = 0;
//        CHECK_EXPR(fs = fdb_iterator_init(m_meta_kv, &iter, NULL, 0, NULL, 0, 0x0));
//        if (0 != fs)
//        {
//            return fs;
//        }
//        do
//        {
//
//            fdb_doc tmp;
//            fdb_doc* doc = &tmp;
//            CHECK_EXPR(fs = fdb_iterator_get(iter, &doc));
//            if (fs != FDB_RESULT_SUCCESS)
//            {
//                break;
//            }
//            std::string key((const char*) doc->key, doc->keylen);
//            if (has_prefix(key, "ns:"))
//            {
//                Data ns;
//                ns.SetString((const char*) doc->body, doc->bodylen, false);
//                m_nss.insert(ns);
//            }
//            fdb_doc_free(doc);
//        } while (fdb_iterator_next(iter) != FDB_RESULT_ITERATOR_FAIL);
//        fdb_iterator_close(iter);
        return 0;
    }

    int ForestDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        fdb_status fs = fdb_set_kv(kv, (const void*) encode_buffer.GetRawBuffer(), key_len, (const void*) (encode_buffer.GetRawBuffer() + key_len), value_len);
        CHECK_EXPR(fs);
        fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
        return FORESTDB_ERR(fs);
    }
    int ForestDBEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, ns, ctx.flags.create_if_notexist);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        fdb_status fs = fdb_set_kv(kv, (const void*) key.data(), key.size(), (const void*) value.data(), value.size());
        fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
        return FORESTDB_ERR(fs);
    }

    int ForestDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
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
            free(val);
        }
        return FORESTDB_ERR(fs);
    }

    int ForestDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
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
    int ForestDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        if (NULL == kv)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        fdb_status fs = fdb_del_kv(kv, (const void*) encode_buffer.GetRawBuffer(), key_len);
        fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
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
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        return local_ctx.AcquireTransanction();
    }
    int ForestDBEngine::CommitTransaction()
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        return local_ctx.TryReleaseTransanction(true);
    }
    int ForestDBEngine::DiscardTransaction()
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        return local_ctx.TryReleaseTransanction(false);
    }
    int ForestDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
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
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        CHECK_EXPR(rc = fdb_kvs_remove(local_ctx.fdb, ns.AsString().c_str()));
        if(0 == rc)
        {
            DBHelper::DelNamespace(ns);
        }
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
            iter->MarkValid(false);
            return iter;
        }
        const void *start_key = NULL;
        size_t start_keylen = 0;
        const void *end_key = NULL;
        size_t end_keylen = 0;
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        fdb_iterator* fdb_iter = NULL;
        fdb_iterator_opt_t opt = FDB_ITR_NO_DELETES;
        if (key.GetType() > 0)
        {
            Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
            key.Encode(encode_buffer);
            start_keylen = encode_buffer.ReadableBytes();
            if (!ctx.flags.iterate_multi_keys)
            {
                if (!ctx.flags.iterate_no_upperbound)
                {
                    KeyObject upperbound_key;
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
                    upperbound_key.Encode(encode_buffer, false);
                    end_keylen = encode_buffer.ReadableBytes() - start_keylen;
                    end_key = (const void *) (encode_buffer.GetRawBuffer() + start_keylen);
                    opt |= FDB_ITR_SKIP_MAX_KEY;
                }
            }
            start_key = (const void *) encode_buffer.GetRawBuffer();
        }
        else
        {
            //do nothing
        }
        fdb_status rc = fdb_iterator_init(kv, &fdb_iter, start_key, start_keylen, end_key, end_keylen, opt);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create cursor for reason:(%d)%s", rc, fdb_error_msg(rc));
            iter->MarkValid(false);
            return iter;
        }
        iter->SetIterator(fdb_iter);
        return iter;
    }

    void ForestDBEngine::Stats(Context& ctx, std::string& stat_info)
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
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
        //do nothing
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
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
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

