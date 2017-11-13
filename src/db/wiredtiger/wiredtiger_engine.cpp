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

#include "wiredtiger_engine.hpp"
#include "thread/lock_guard.hpp"
#include "util/file_helper.hpp"
#include "db/db.hpp"
#include <string.h>
#include <stdlib.h>

#define CHECK_WT_RETURN(ret)         do{\
                                       int rc = (ret);\
                                       if (0 != rc) \
                                        {             \
                                           ERROR_LOG("WiredTiger Error: %s at %s:%d", wiredtiger_strerror(rc), __FILE__, __LINE__);\
                                           return rc;  \
                                         }\
                                       }while(0)
#define LOG_WTERROR(ret)         do{\
                                       int rc = (ret);\
                                       if (0 != rc) \
                                        {             \
                                           ERROR_LOG("WiredTiger Error: %s at %s:%d", wiredtiger_strerror(rc), __FILE__, __LINE__);\
                                         }\
                                       }while(0)

#define WT_ERR(err)  (WT_NOTFOUND == err ? ERR_ENTRY_NOT_EXIST: (0 == err ? 0 :(err + STORAGE_ENGINE_ERR_OFFSET)))
#define WT_NERR(err)  (WT_NOTFOUND == err ? 0:(0 == err ? 0 :(err + STORAGE_ENGINE_ERR_OFFSET)))

namespace ardb
{
    static WiredTigerEngine* g_wdb = NULL;

    struct WTConfig
    {
            int64 block_size;
            int64 chunk_size;
            int64 cache_size;
            int64 bloom_bits;
            int64 session_max;
            bool mmap;
            std::string compressor;
            WTConfig() :
                    block_size(4096), chunk_size(128 * 1024 * 1024), cache_size(512 * 1024 * 1024), bloom_bits(10), session_max(8192), mmap(false), compressor(
                            "snappy")
            {
            }
    };

    static WTConfig g_wt_conig;

    static const std::string table_url(const Data& ns)
    {
        return "table:" + ns.AsString();
    }
    struct WriteOperation
    {
            WT_CURSOR* cursor;
            std::string key;
            std::string val;
            uint8 op;
            WriteOperation() :
                    cursor(NULL), op(0)
            {
            }
    };

    struct WiredTigerLocalContext
    {
            WT_SESSION* wsession;
            typedef TreeMap<Data, WT_CURSOR*>::Type KVTable;
            KVTable kv_stores;
            uint32 batch_ref;
            bool batch_abort;
            Buffer encode_buffer_cache;
            std::deque<WriteOperation> batch;
            bool inited;
            WiredTigerLocalContext() :
                    wsession(NULL), batch_ref(0), batch_abort(false), inited(false)
            {
            }
            void RecycleCursor(const Data& ns, WT_CURSOR* cursor)
            {
                KVTable::iterator found = kv_stores.find(ns);
                if (found != kv_stores.end())
                {
                    if (NULL == found->second)
                    {
                        found->second = cursor;
                        return;
                    }
                    if (found->second == cursor)
                    {
                        return;
                    }
                }
                cursor->close(cursor);
            }
            WT_CURSOR* GetKVStore(const Data& ns, bool create_if_missing, bool acquire = false)
            {
                WT_CURSOR* c = NULL;
                KVTable::iterator found = kv_stores.find(ns);
                if (found != kv_stores.end() && NULL != found->second)
                {
                    c = found->second;
                    if (acquire)
                    {
                        found->second = NULL;
                    }
                    return c;
                }
                if (found == kv_stores.end() || NULL ==  found->second)
                {
                    /*
                     * other thread may create table
                     */
                    if (!g_wdb->GetTable(ns, create_if_missing))
                    {
                        return NULL;
                    }
                }
                int ret;
                if ((ret = wsession->open_cursor(wsession, table_url(ns).c_str(), NULL, NULL, &c)) != 0)
                {
                    ERROR_LOG("Failed to open cursor on %s: %s\n", ns.AsString().c_str(), wiredtiger_strerror(ret));
                    return NULL;
                }
                kv_stores[ns] = c;
                if (acquire)
                {
                    kv_stores[ns] = NULL;
                }
                return c;
            }
            bool Init()
            {
                if (inited)
                {
                    return true;
                }
                int ret = 0;
                WT_CONNECTION* conn = g_wdb->GetWTConn();
                if ((ret = conn->open_session(conn, NULL, NULL, &wsession)) != 0)
                {
                    ERROR_LOG("Error opening a session for reason:(%d)%s", ret, wiredtiger_strerror(ret));
                    return false;
                }
                inited = true;
                return true;
            }

            int AcquireTransanction()
            {
                int rc = 0;
                if (0 == batch_ref)
                {
                    //rc = fdb_begin_transaction(fdb, FDB_ISOLATION_READ_COMMITTED);
                    batch_abort = false;
                    batch_ref = 0;
                }
                if (0 == rc)
                {
                    batch_ref++;
                }
                return rc;
            }
            int TryReleaseTransanction(bool success)
            {
                int rc = 0;
                if (batch_ref > 0)
                {
                    batch_ref--;
                    //printf("#### %d %d %d \n", txn_ref, txn_abort, write_dispatched);
                    if (!batch_abort)
                    {
                        batch_abort = !success;
                    }
                    if (batch_ref == 0)
                    {
                        if (batch_abort)
                        {
                            //fdb_abort_transaction (fdb);
                        }
                        else
                        {
                            //rc = fdb_end_transaction(fdb, FDB_COMMIT_NORMAL);
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
            ~WiredTigerLocalContext()
            {
            }
    };
    static ThreadLocal<WiredTigerLocalContext> g_ctx_local;
    static WiredTigerLocalContext& GetDBLocalContext()
    {
        WiredTigerLocalContext& local_ctx = g_ctx_local.GetValue();
        local_ctx.Init();
        return local_ctx;
    }

    WiredTigerEngine::WiredTigerEngine() :
            m_db(NULL)
    {

    }
    WiredTigerEngine::~WiredTigerEngine()
    {
    }

    bool WiredTigerEngine::GetTable(const Data& ns, bool create_if_missing)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !create_if_missing);
        if (m_nss.count(ns) > 0)
        {
            return true;
        }
        if (!create_if_missing)
        {
            return false;
        }
        std::stringstream s_table;
        s_table << "collator=ardb_comparator,";
        s_table << "type=lsm,split_pct=100,leaf_item_max=1KB,";
        s_table << "lsm=(chunk_size=100MB,bloom_config=(leaf_page_max=8MB)),";
        s_table << "internal_page_max=" << g_wt_conig.block_size << ",";
        s_table << "leaf_page_max=" << g_wt_conig.block_size << ",";
        s_table << "leaf_item_max=" << g_wt_conig.block_size / 4 << ",";
        if (g_wt_conig.compressor != "none")
        {
            s_table << "block_compressor=" << g_wt_conig.compressor << ",";
        }
        s_table << "lsm=(";
        s_table << "chunk_size=" << g_wt_conig.chunk_size << ",";
        s_table << "bloom_bit_count=" << g_wt_conig.bloom_bits << ",";
        // Approximate the optimal number of hashes
        s_table << "bloom_hash_count=" << (int) (0.6 * g_wt_conig.bloom_bits) << ",";
        s_table << "),";
        int ret;
        std: string table_name = table_url(ns);

        WT_SESSION *session;
        std::string table_config = s_table.str();
        if ((ret = m_db->open_session(m_db, NULL, NULL, &session)) != 0)
        {
            LOG_WTERROR(ret);
            return false;
        }
        if ((ret = session->create(session, table_name.c_str(), table_config.c_str())) != 0)
        {
            LOG_WTERROR(ret);
            return false;
        }
        if ((ret = session->close(session, NULL)) != 0)
        {
            LOG_WTERROR(ret);
            return false;
        }
        m_nss.insert(ns);
        INFO_LOG("Success to open db:%s", ns.AsString().c_str());
        return true;
    }

    int WiredTigerEngine::MaxOpenFiles()
    {
        return 100;
    }

    int WiredTigerEngine::Init(const std::string& dir, const std::string& options)
    {
        Properties props;
        parse_conf_content(options, props);
        conf_get_int64(props, "block_size", g_wt_conig.block_size);
        conf_get_int64(props, "chunk_size", g_wt_conig.chunk_size);
        conf_get_int64(props, "cache_size", g_wt_conig.cache_size);
        conf_get_int64(props, "bloom_bits", g_wt_conig.bloom_bits);
        conf_get_int64(props, "session_max", g_wt_conig.session_max);
        conf_get_bool(props, "mmap", g_wt_conig.mmap);
        conf_get_string(props, "compressor", g_wt_conig.compressor);

        std::stringstream s_conn;
        s_conn << "log=(enabled),checkpoint=(wait=180),checkpoint_sync=false,";
        s_conn << "session_max=" << g_wt_conig.session_max << ",";
        s_conn << "mmap=" << (g_wt_conig.mmap ? "true" : "false") << ",";
        s_conn << "transaction_sync=(enabled=true,method=none),";
        //if (options.error_if_exists)
        s_conn << "create,";
        s_conn << "cache_size=" << g_wt_conig.cache_size;
        s_conn << ",extensions=[local=(entry=ardb_extension_init)]";
        std::string conn_config = s_conn.str();

        //,extensions=[local=(entry=my_extension_init)]
        //WT_CONNECTION *conn;

        int ret = 0;
        CHECK_WT_RETURN(ret = ::wiredtiger_open(dir.c_str(), NULL, conn_config.c_str(), &m_db));

        WT_SESSION* session = NULL;
        WT_CURSOR *cursor = NULL;
        CHECK_WT_RETURN(ret = m_db->open_session(m_db, NULL, "isolation=snapshot", &session));
        WT_CURSOR *c;
        CHECK_WT_RETURN(ret = session->open_cursor(session, "metadata:", NULL, NULL, &c));
        c->set_key(c, "table:");
        /* Position on the first table entry */
        int cmp;
        ret = c->search_near(c, &cmp);
        if (ret != 0 || (cmp < 0 && (ret = c->next(c)) != 0))
        {
            //do nothing
        }
        else
        {
            /* Add entries while we are getting "table" URIs. */
            for (; ret == 0; ret = c->next(c))
            {
                const char *key;
                if ((ret = c->get_key(c, &key)) != 0)
                {
                    break;
                }
                if (strncmp(key, "table:", strlen("table:")) != 0)
                {
                    break;
                }
                Data ns;
                ns.SetString(std::string(key + strlen("table:")), false);
                m_nss.insert(ns);
            }
        }
        c->close(c);
        session->close(session, NULL);
        g_wdb = this;
        INFO_LOG("Open: home %s config %s", dir.c_str(), conn_config.c_str());
        return 0;
    }

    int WiredTigerEngine::Repair(const std::string& dir)
    {
        ERROR_LOG("Repair not supported in WiredTiger");
        return ERR_NOTSUPPORTED;
    }

    int WiredTigerEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == cursor)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Buffer& encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(encode_buffer);
        size_t key_len = encode_buffer.ReadableBytes();
        value.Encode(encode_buffer);
        size_t value_len = encode_buffer.ReadableBytes() - key_len;
        WT_ITEM key_item, value_item;
        key_item.data = (const void *) encode_buffer.GetRawReadBuffer();
        key_item.size = key_len;
        value_item.data = (const void *) (encode_buffer.GetRawReadBuffer() + key_len);
        value_item.size = value_len;
        cursor->set_key(cursor, &key_item);
        cursor->set_value(cursor, &value_item);
        int ret = cursor->insert(cursor);
        local_ctx.RecycleCursor(key.GetNameSpace(), cursor);
        return ret;
    }

    int WiredTigerEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(ns, ctx.flags.create_if_notexist);
        if (NULL == cursor)
        {
            return ERR_ENTRY_NOT_EXIST;
        }

        WT_ITEM key_item, value_item;
        key_item.data = (const void *) key.data();
        key_item.size = key.size();
        value_item.data = (const void *) (value.data());
        value_item.size = value.size();
        cursor->set_key(cursor, &key_item);
        cursor->set_value(cursor, &value_item);
        int ret = cursor->insert(cursor);
        local_ctx.RecycleCursor(ns, cursor);
        return ret;
    }

    int WiredTigerEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(key.GetNameSpace(), false);
        if (NULL == cursor)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Buffer& key_encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(key_encode_buffer);
        WT_ITEM item;
        item.data = (const void *) (key_encode_buffer.GetRawReadBuffer());
        item.size = key_encode_buffer.ReadableBytes();
        cursor->set_key(cursor, &item);
        int ret;
        if ((ret = cursor->search(cursor)) == 0 && (ret = cursor->get_value(cursor, &item)) == 0)
        {
            Buffer valBuffer((char*) item.data, 0, item.size);
            value.Decode(valBuffer, true);
            ret = cursor->reset(cursor);
        }
        local_ctx.RecycleCursor(key.GetNameSpace(), cursor);
        return WT_ERR(ret);
    }

    int WiredTigerEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
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
    int WiredTigerEngine::Del(Context& ctx, const KeyObject& key)
    {
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(key.GetNameSpace(), false);
        if (NULL == cursor)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Buffer& key_encode_buffer = local_ctx.GetEncodeBuferCache();
        key.Encode(key_encode_buffer);
        WT_ITEM item;
        item.data = (const void *) (key_encode_buffer.GetRawReadBuffer());
        item.size = key_encode_buffer.ReadableBytes();
        cursor->set_key(cursor, &item);
        int ret;
        ret = cursor->remove(cursor);
        // Reset the WiredTiger cursor so it doesn't keep any pages pinned.
        // Track failures in debug builds since we don't expect failure, but
        // don't pass failures on - it's not necessary for correct operation.
        cursor->reset(cursor);
        local_ctx.RecycleCursor(key.GetNameSpace(), cursor);
        return WT_NERR(ret);
    }
    int WiredTigerEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
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
    bool WiredTigerEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }
    int WiredTigerEngine::BeginWriteBatch(Context& ctx)
    {
        /*
         * do nothing now
         */
        return 0;
    }
    int WiredTigerEngine::CommitWriteBatch(Context& ctx)
    {
        /*
         * do nothing now
         */
        return 0;
    }
    int WiredTigerEngine::DiscardWriteBatch(Context& ctx)
    {
        /*
         * do nothing now
         */
        return 0;
    }
    int WiredTigerEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        return ERR_NOTSUPPORTED;
    }
    int WiredTigerEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        DataSet::iterator it = m_nss.begin();
        while (it != m_nss.end())
        {
            nss.push_back(*it);
            it++;
        }
        return 0;
    }
    int WiredTigerEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(ns, false, true);
        if (NULL == cursor)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        cursor->close(cursor);
        WT_SESSION *session = local_ctx.wsession;
        int ret = session->drop(session, table_url(ns).c_str(), "force");
        if (0 == ret)
        {
            RWLockGuard<SpinRWLock> guard(m_lock, false);
            m_nss.erase(ns);
        }
        LOG_WTERROR(ret);
        return WT_ERR(ret);
    }
    int64_t WiredTigerEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        return 0;
    }
    const std::string WiredTigerEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        return wiredtiger_strerror(err);
    }
    Iterator* WiredTigerEngine::Find(Context& ctx, const KeyObject& key)
    {
        WiredTigerIterator* iter = NULL;
        NEW(iter, WiredTigerIterator(this,key.GetNameSpace()));
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        WT_CURSOR *cursor = local_ctx.GetKVStore(key.GetNameSpace(), false, true);
        if (NULL == cursor)
        {
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

    static int print_cursor(WT_CURSOR *cursor, std::string& str)
    {
        WT_ITEM desc, pvalue;
        uint64_t value;
        int ret;

        while ((ret = cursor->next(cursor)) == 0 && (ret = cursor->get_value(cursor, &desc, &pvalue, &value)) == 0)
        {
            if (value != 0)
            {
                str.append((const char*) desc.data, desc.size).append(": ").append((const char*) pvalue.data, pvalue.size).append("\r\n");
            }
        }
        return (ret == WT_NOTFOUND ? 0 : ret);
    }

    void WiredTigerEngine::Stats(Context& ctx, std::string& str)
    {
        int major_v, minor_v, patch;
        (void) wiredtiger_version(&major_v, &minor_v, &patch);
        str.append("wiredtiger_version:").append(stringfromll(major_v)).append(".").append(stringfromll(minor_v)).append(".").append(stringfromll(patch)).append(
                "\r\n");
    }

    bool WiredTigerIterator::Valid()
    {
        return m_valid && NULL != m_cursor;
    }
    void WiredTigerIterator::Next()
    {
        ClearState();
        int ret = m_cursor->next(m_cursor);
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
        CheckBound();
    }
    void WiredTigerIterator::Prev()
    {
        ClearState();
        int ret = m_cursor->prev(m_cursor);
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    void WiredTigerIterator::DoJump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        WiredTigerLocalContext& local_ctx = GetDBLocalContext();
        Slice key_slice = next.Encode(local_ctx.GetEncodeBuferCache());
        WT_ITEM item;
        item.data = (const void*) key_slice.data();
        item.size = key_slice.size();
        m_cursor->set_key(m_cursor, &item);
        int cmp;
        int ret = m_cursor->search_near(m_cursor, &cmp);
        if (ret == 0 && cmp < 0)
        {
            ret = m_cursor->next(m_cursor);
        }
        if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    void WiredTigerIterator::Jump(const KeyObject& next)
    {
        DoJump(next);
        if (Valid())
        {
            CheckBound();
        }
    }
    void WiredTigerIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        int ret;
        if ((ret = m_cursor->reset(m_cursor)) != 0)
        {
            m_valid = false;
            return;
        }
        ret = m_cursor->next(m_cursor);
        if (ret == WT_NOTFOUND)
        {
            m_valid = false;
            return;
        }
        else if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    void WiredTigerIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_cursor)
        {
            return;
        }
        if (m_iterate_upper_bound_key.GetType() > 0)
        {
            DoJump(m_iterate_upper_bound_key);
            if (Valid())
            {
                Prev();
                return;
            }
        }
        int ret;
        if ((ret = m_cursor->reset(m_cursor)) != 0)
        {
            m_valid = false;
            return;
        }
        ret = m_cursor->prev(m_cursor);
        if (ret == WT_NOTFOUND)
        {
            m_valid = false;
            return;
        }
        else if (ret != 0)
        {
            m_valid = false;
            return;
        }
    }
    KeyObject& WiredTigerIterator::Key(bool clone_str)
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
    ValueObject& WiredTigerIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
        	if(clone_str)
        	{
        		m_value.CloneStringPart();
        	}
            return m_value;
        }
        Slice key = RawValue();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice WiredTigerIterator::RawKey()
    {
        WT_ITEM item;
        m_cursor->get_key(m_cursor, &item);
        return Slice((const char*) item.data, item.size);
    }
    Slice WiredTigerIterator::RawValue()
    {
        WT_ITEM item;
        m_cursor->get_value(m_cursor, &item);
        return Slice((const char*) item.data, item.size);
    }
    void WiredTigerIterator::Del()
    {
        RawKey();
        m_cursor->remove(m_cursor);
    }
    void WiredTigerIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void WiredTigerIterator::CheckBound()
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

    WiredTigerIterator::~WiredTigerIterator()
    {
        if (NULL != m_cursor)
        {
            WiredTigerLocalContext& local_ctx = GetDBLocalContext();
            local_ctx.RecycleCursor(m_ns, m_cursor);
        }
    }

}

