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
       ERROR_LOG("forestdb operation:%s error:(%d)%s at line:%u", #expr, __rc__, fdb_error_msg((fdb_status)__rc__), __LINE__); \
    }\
}while(0)
#define DEFAULT_LMDB_LOCAL_MULTI_CACHE_SIZE 10

#define FDB_PUT_OP 1
#define FDB_DEL_OP 2
#define FDB_CKP_OP 3

namespace ardb
{
    static int kEngineNotFound = FDB_RESULT_KEY_NOT_FOUND;
    static int fdb_cmp_callback(void *a, size_t len_a, void *b, size_t len_b)
    {
        return compare_keys((const char*) a, len_a, (const char*) b, len_b, false);
    }

    static fdb_compact_decision ardb_fdb_compaction_callback(fdb_file_handle *fhandle, fdb_compaction_status status, const char *kv_store_name, fdb_doc *doc,
            uint64_t last_oldfile_offset, uint64_t last_newfile_offset, void *ctx)
    {
        if (doc->bodylen == 0)
        {
            return FDB_CS_DROP_DOC;
        }
        Buffer buffer((char*) (doc->key), 0, doc->keylen);
        KeyObject k;
        if (!k.DecodePrefix(buffer, false))
        {
            abort();
        }
        if (k.GetType() == KEY_META)
        {
            ValueObject meta;
            Buffer val_buffer((char*) (doc->body), 0, doc->bodylen);
            if (!meta.DecodeMeta(val_buffer))
            {
                ERROR_LOG("Failed to decode value of key:%s with type:%u", k.GetKey().AsString().c_str(), meta.GetType());
                return FDB_CS_KEEP_DOC;
            }
            if (meta.GetType() == 0)
            {
                ERROR_LOG("Invalid value for key:%s with type:%u", k.GetKey().AsString().c_str(), k.GetType());
                return FDB_CS_KEEP_DOC;
            }
            if (meta.GetMergeOp() != 0)
            {
                return FDB_CS_KEEP_DOC;
            }
            if (meta.GetTTL() > 0 && meta.GetTTL() <= get_current_epoch_millis())
            {
                if (meta.GetType() != KEY_STRING)
                {
                    Data ns;
                    ns.SetString(kv_store_name, false);
                    g_db->AddExpiredKey(ns, k.GetKey());
                    return FDB_CS_KEEP_DOC;
                }
                else
                {
                    return FDB_CS_DROP_DOC;
                }
            }
        }
        return FDB_CS_KEEP_DOC;
    }

    static ForestDBEngine* g_fdb_engine = NULL;
    static std::string g_fdb_dir;
    static fdb_config g_fdb_config;

    struct ForestDBLocalContext
    {
            fdb_file_handle* fdb;
            fdb_file_handle* metadb;
            fdb_kvs_handle* metakv;
            typedef TreeMap<Data, fdb_kvs_handle*>::Type KVStoreTable;
            typedef TreeSet<ForestDBIterator*>::Type LocalIteratorSet;
            KVStoreTable kv_stores;
            uint32 txn_ref;
            uint32 iter_count;
            Buffer encode_buffer_cache;bool txn_abort;bool inited;
            ForestDBLocalContext() :
                    fdb(NULL), metadb(NULL), metakv(NULL), txn_ref(0), iter_count(0), txn_abort(false), inited(false)
            {
            }
            fdb_kvs_handle* GetKVStore(const Data& ns, bool create_if_missing)
            {
                KVStoreTable::iterator found = kv_stores.find(ns);
                if (found != kv_stores.end())
                {
                    return found->second;
                }
                if (!create_if_missing && !g_fdb_engine->HaveNamespace(ns))
                {
                    return NULL;
                }
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
                g_fdb_engine->AddNamespace(ns);
                std::string ns_key = "ns:" + ns.AsString();
                std::string ns_val = ns.AsString();
                CHECK_EXPR(fs = fdb_set_kv(metakv, (const void* ) ns_key.data(), ns_key.size(), (const void* ) (ns_val.data()), ns_val.size()));
                CHECK_EXPR(fs = fdb_commit(metadb, FDB_COMMIT_NORMAL));
                return kvs;
            }
            int RemoveKVStore(const Data& ns)
            {
                KVStoreTable::iterator found = kv_stores.find(ns);
                if (found != kv_stores.end())
                {
                    fdb_kvs_close(found->second);
                    kv_stores.erase(found);
                }
                int rc = fdb_kvs_remove(fdb, ns.AsString().c_str());
                if (0 == rc)
                {
                    std::string ns_key = "ns:" + ns.AsString();
                    CHECK_EXPR(rc = fdb_del_kv(metakv, (const void* ) ns_key.data(), ns_key.size()));
                    CHECK_EXPR(rc = fdb_commit(metadb, FDB_COMMIT_NORMAL));
                }
                return rc;
            }
            bool Init()
            {
                if (inited)
                {
                    return true;
                }
                fdb_config config = fdb_get_default_config();
                fdb_kvs_config kv_config = fdb_get_default_kvs_config();
                std::string meta_file = g_fdb_dir + "/ardb.meta";
                fdb_status fs;
                fs = fdb_open(&metadb, meta_file.c_str(), &config);
                DataArray nss;
                if (0 == fs)
                {
                    CHECK_EXPR(fs = fdb_kvs_open_default(metadb, &metakv, &kv_config));
                    if (0 != fs)
                    {
                        return false;
                    }
                    fdb_iterator* iter = NULL;
                    fdb_iterator_opt_t opt = FDB_ITR_NONE;
                    CHECK_EXPR(fs = fdb_iterator_init(metakv, &iter, NULL, 0, NULL, 0, opt));
                    if (0 != fs)
                    {
                        return false;
                    }
                    do
                    {
                        fdb_doc* doc = NULL;
                        CHECK_EXPR(fs = fdb_iterator_get(iter, &doc));
                        if (fs != FDB_RESULT_SUCCESS)
                        {
                            break;
                        }
                        std::string key((const char*) doc->key, doc->keylen);
                        if (has_prefix(key, "ns:"))
                        {
                            Data ns;
                            ns.SetString((const char*) doc->body, doc->bodylen, true);
                            g_fdb_engine->AddNamespace(ns);
                            nss.push_back(ns);
                        }
                        fdb_doc_free(doc);
                    } while (fdb_iterator_next(iter) != FDB_RESULT_ITERATOR_FAIL);
                    fdb_iterator_close(iter);
                }
                else
                {
                    ERROR_LOG("Failed to open meta db for reason:%s", fdb_error_msg(fs));
                    return false;
                }
                std::string data_file = g_fdb_dir + "/ardb.data";
                config = g_fdb_config;
                if (nss.empty())
                {
                    fs = fdb_open(&fdb, data_file.c_str(), &config);
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
                    fs = fdb_open_custom_cmp(&fdb, data_file.c_str(), &config, nss.size(), (char **) names, cmps);
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
                INFO_LOG("Success to open forestdb at %s.", data_file.c_str());
                return true;
            }
            bool IterateDel(const KeyObject& key)
            {
                return false;

            }
            void AddIterRef(ForestDBIterator* iter)
            {
                iter_count++;
            }
            void ReleaseIterRef(ForestDBIterator* iter)
            {
                iter_count--;
            }
            int AcquireTransanction()
            {
                int rc = 0;
                if (0 == txn_ref)
                {
                    CHECK_EXPR(rc = fdb_begin_transaction(fdb, FDB_ISOLATION_READ_COMMITTED));
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
                fdb_kvs_close(metakv);
                fdb_close(fdb);
                fdb_close(metadb);
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

    void ForestDBEngine::AddNamespace(const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        m_nss.insert(ns);
    }

    bool ForestDBEngine::HaveNamespace(const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        return m_nss.count(ns) > 0;
    }

    fdb_kvs_handle* ForestDBEngine::GetKVStore(Context& ctx, const Data& ns, bool create_if_noexist)
    {
        fdb_kvs_handle* kv = NULL;
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        kv = local_ctx.GetKVStore(ns, create_if_noexist);
        return kv;
    }

    int ForestDBEngine::Init(const std::string& dir, const std::string& options)
    {
        g_fdb_dir = dir;
        g_fdb_config = fdb_get_default_config();
        g_fdb_config.multi_kv_instances = true;
        g_fdb_config.compaction_cb = ardb_fdb_compaction_callback;
        g_fdb_config.compaction_mode = FDB_COMPACTION_AUTO;
        Properties props;
        parse_conf_content(options, props);
        conf_get_uint16(props, "chunksize", g_fdb_config.chunksize);
        conf_get_uint32(props, "blocksize", g_fdb_config.blocksize);
        conf_get_uint64(props, "buffercache_size", g_fdb_config.buffercache_size);
        conf_get_uint64(props, "wal_threshold", g_fdb_config.wal_threshold);
        conf_get_bool(props, "wal_flush_before_commit", g_fdb_config.wal_flush_before_commit);
        conf_get_bool(props, "auto_commit", g_fdb_config.auto_commit);
        conf_get_uint32(props, "purging_interval", g_fdb_config.purging_interval);
        conf_get_uint32(props, "compaction_buf_maxsize", g_fdb_config.compaction_buf_maxsize);
        conf_get_bool(props, "cleanup_cache_onclose", g_fdb_config.cleanup_cache_onclose);
        conf_get_bool(props, "compress_document_body", g_fdb_config.compress_document_body);
        conf_get_uint64(props, "compaction_minimum_filesize", g_fdb_config.compaction_minimum_filesize);
        conf_get_uint64(props, "compactor_sleep_duration", g_fdb_config.compactor_sleep_duration);
        conf_get_uint64(props, "prefetch_duration", g_fdb_config.prefetch_duration);
        conf_get_uint16(props, "num_wal_partitions", g_fdb_config.num_wal_partitions);
        conf_get_uint16(props, "num_bcache_partitions", g_fdb_config.num_bcache_partitions);
        conf_get_uint32(props, "compaction_cb_mask", g_fdb_config.compaction_cb_mask);
        conf_get_size(props, "max_writer_lock_prob", g_fdb_config.max_writer_lock_prob);
        conf_get_size(props, "num_compactor_threads", g_fdb_config.num_compactor_threads);
        conf_get_size(props, "num_bgflusher_threads", g_fdb_config.num_bgflusher_threads);
        conf_get_uint8(props, "compaction_threshold", g_fdb_config.compaction_threshold);
        ForestDBLocalContext& local_ctx = g_ctx_local.GetValue();
        return local_ctx.Init() ? 0 : -1;
    }

    int ForestDBEngine::Repair(const std::string& dir)
    {
        ERROR_LOG("Repair not supported in forstdb");
        return ERR_NOTSUPPORTED;
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
        if (local_ctx.iter_count == 0 && local_ctx.txn_ref == 0)
        {
            fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
        }
        return ENGINE_ERR(fs);
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
        if (local_ctx.iter_count == 0 && local_ctx.txn_ref == 0)
        {
            fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
        }
        return ENGINE_ERR(fs);
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
        return ENGINE_ERR(fs);
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
        fdb_status fs = FDB_RESULT_SUCCESS;
        CHECK_EXPR(fs = fdb_del_kv(kv, (const void* ) encode_buffer.GetRawBuffer(), key_len));
        if (local_ctx.iter_count == 0 && local_ctx.txn_ref == 0)
        {
            CHECK_EXPR(fs = fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL));
        }
        return ENGINE_NERR(fs);
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

    int ForestDBEngine::BeginWriteBatch(Context& ctx)
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        return local_ctx.AcquireTransanction();
    }
    int ForestDBEngine::CommitWriteBatch(Context& ctx)
    {
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        return local_ctx.TryReleaseTransanction(true);
    }
    int ForestDBEngine::DiscardWriteBatch(Context& ctx)
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
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        DataSet::iterator it = m_nss.begin();
        while (it != m_nss.end())
        {
            nss.push_back(*it);
            it++;
        }
        return 0;
    }
    int ForestDBEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        int rc = 0;
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        CHECK_EXPR(rc = local_ctx.RemoveKVStore(ns));
        return rc;
    }
    int64_t ForestDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return -1;
    }

    const std::string ForestDBEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        return fdb_error_msg((fdb_status) err);
    }

    Iterator* ForestDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        ForestDBIterator* iter = NULL;
        fdb_kvs_handle* kv = GetKVStore(ctx, key.GetNameSpace(), false);
        NEW(iter, ForestDBIterator(kv,key.GetNameSpace()));
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
                }
            }
            start_key = (const void *) encode_buffer.GetRawBuffer();
        }
        else
        {
            //do nothing
        }
        iter->SetMin(start_key, start_keylen);
        iter->SetMax(end_key, end_keylen);
        //fdb_kvs_handle* snapshot = local_ctx.snapshot.Get(kv);
        end_key = NULL;
        end_keylen = 0;

        fdb_status rc = fdb_iterator_init(kv, &fdb_iter, NULL, 0, NULL, 0, opt);
        if (0 != rc)
        {
            ERROR_LOG("Failed to create cursor for reason:(%d)%s", rc, fdb_error_msg(rc));
            iter->MarkValid(false);
            return iter;
        }
        iter->SetIterator(fdb_iter);
        if (key.GetType() > 0)
        {
            iter->Jump(key);
        }
        local_ctx.AddIterRef(iter);
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
    }

    void ForestDBIterator::ClearState()
    {
        ClearRawDoc();
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }

    void ForestDBIterator::ClearRawDoc()
    {
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
        m_valid = (rc == 0);
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
        m_valid = (rc == 0);
    }
    void ForestDBIterator::DoJump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        Slice key_slice = next.Encode(local_ctx.GetEncodeBuferCache(), false);
        int rc = fdb_iterator_seek(m_iter, (const void *) key_slice.data(), key_slice.size(), FDB_ITR_SEEK_HIGHER);
        m_valid = rc == 0;
    }
    void ForestDBIterator::Jump(const KeyObject& next)
    {
        DoJump(next);
        if (Valid())
        {
            CheckBound();
            if (m_valid && !max_key.empty())
            {
                Slice raw = RawKey();
                if (compare_keys(raw.data(), raw.size(), max_key.data(), max_key.size(), false) > 0)
                {
                    ClearRawDoc();
                    m_valid = (0 == fdb_iterator_prev(m_iter));
                }
            }
        }
    }
    void ForestDBIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc = 0;
        if (min_key.empty())
        {
            rc = fdb_iterator_seek_to_min(m_iter);
        }
        else
        {
            rc = fdb_iterator_seek(m_iter, (const void *) min_key.data(), min_key.size(), FDB_ITR_SEEK_HIGHER);
        }
        m_valid = rc == 0;
    }
    void ForestDBIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        int rc = 0;
        if (max_key.empty())
        {
            rc = fdb_iterator_seek_to_max(m_iter);
        }
        else
        {
            rc = fdb_iterator_seek(m_iter, (const void *) max_key.data(), max_key.size(), FDB_ITR_SEEK_LOWER);
        }
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
        GetRaw();
        if (NULL != m_raw)
        {
            Buffer kbuf((char*) (m_raw->key), 0, m_raw->keylen);
            m_key.Decode(kbuf, clone_str);
            m_key.SetNameSpace(m_ns);
        }
        return m_key;
    }
    ValueObject& ForestDBIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
        	if(clone_str)
        	{
        		m_value.CloneStringPart();
        	}
            return m_value;
        }
        GetRaw();
        if (NULL != m_raw)
        {
            Buffer kbuf((char*) (m_raw->body), 0, m_raw->bodylen);
            m_value.Decode(kbuf, clone_str);
        }
        return m_value;
    }

    void ForestDBIterator::GetRaw()
    {
        if (NULL == m_raw)
        {
            CHECK_EXPR(fdb_iterator_get(m_iter, &m_raw));
        }
    }

    Slice ForestDBIterator::RawKey()
    {
        GetRaw();
        return Slice((const char*) m_raw->key, m_raw->keylen);
    }
    Slice ForestDBIterator::RawValue()
    {
        GetRaw();
        return Slice((const char*) m_raw->body, m_raw->bodylen);
    }
    void ForestDBIterator::Del()
    {
        GetRaw();
        if (NULL != m_raw)
        {
            fdb_status fs = fdb_del(m_kv, m_raw);
            CHECK_EXPR(fs);
        }
    }

    ForestDBIterator::~ForestDBIterator()
    {
        ClearRawDoc();
        if (NULL != m_iter)
        {
            fdb_iterator_close(m_iter);
        }
        ForestDBLocalContext& local_ctx = GetDBLocalContext();
        fdb_commit(local_ctx.fdb, FDB_COMMIT_NORMAL);
        local_ctx.ReleaseIterRef(this);
    }
}

