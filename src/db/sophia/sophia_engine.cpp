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

#include "util/file_helper.hpp"
#include "db/db_utils.hpp"
#include "db/db.hpp"
#include "thread/lock_guard.hpp"
#include <string.h>
#include "sophia_engine.hpp"

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
    static int kEngineNotFound = ERR_ENTRY_NOT_EXIST;

    class LoacalWriteBatch
    {
        private:
            leveldb::WriteBatch batch;
            uint32_t ref;
        public:
            LoacalWriteBatch() :
                    ref(0)
            {
            }
            leveldb::WriteBatch& GetBatch()
            {
                return batch;
            }
            leveldb::WriteBatch* Ref()
            {
                if (ref > 0)
                {
                    return &batch;
                }
                return NULL;
            }
            uint32 AddRef()
            {
                ref++;
                //batch.SetSavePoint();
                return ref;
            }
            uint32 ReleaseRef(bool rollback)
            {
                ref--;
                if (rollback)
                {
                    // batch.RollbackToSavePoint();
                }
                return ref;
            }
            void Clear()
            {
                batch.Clear();
                ref = 0;
            }
    };

#define DEFAULT_LEVELDB_LOCAL_MULTI_CACHE_SIZE 10
    struct SophiaLocalContext: public DBLocalContext
    {
            LoacalWriteBatch batch;
    };

    static ThreadLocal<SophiaLocalContext> g_local_ctx;

    static inline int sophia_err(void* env)
    {
        int size;
        char *err = sp_getstring(env, "sophia.error", &size);
        if (NULL != err)
        {
            free (err);
        }

//        SophiaLocalContext::ErrMap& err_map = g_local_ctx.GetValue().err_map;
//        int err = -1234;
//        if (!err_map.empty())
//        {
//            err = err_map.begin()->first;
//            err--;
//            if (err_map.size() > 10)
//            {
//                err_map.erase(err_map.rbegin()->first);
//            }
//        }
//        err_map[err] = s;
//        return err + STORAGE_ENGINE_ERR_OFFSET;
    }

    static int sophia_comparator(char* a, int a_size, char* b, int b_size, void *arg)
    {
        (void) arg;
        return compare_keys(a, (size_t) a_size, b, (size_t) b_size, false);
    }

    SophiaEngine::SophiaEngine() :
            env_(NULL)
    {

    }
    SophiaEngine::~SophiaEngine()
    {
        sp_destroy(env_);
    }

    int SophiaEngine::Init(const std::string& dir, const std::string& options)
    {
        Properties props;
        parse_conf_content(options, props);
        env_ = sp_env();
        sp_setstring(env_, "sophia.path", "_test", 0);
        int rc = sp_open(env);
        if (rc == -1)
        {
            return -1;
        }
        return 0;
    }

    int SophiaEngine::Repair(const std::string& dir)
    {
        return ERR_NOTSUPPORTED;
    }

    void* SophiaEngine::GetNamespace(const Data& ns, bool create_if_missing)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !create_if_missing);
        DBTable::iterator found = m_dbs.find(ns);
        if (found != m_dbs.end())
        {
            return found->second;
        }
        if (!create_if_missing)
        {
            return false;
        }
        sp_setstring(env_, "db", ns.AsString().c_str(), 0);
        char cfgkey[1024];
        sprintf(cfgkey, "db.%s.index.comparator", ns.AsString().c_str());
        sp_setstring(env_, cfgkey, (char*) (intptr_t) sophia_comparator, 0);
        sprintf(cfgkey, "db.%s.index.comparator_arg", ns.AsString().c_str());
        sp_setstring(env_, cfgkey, NULL, 0);
        sprintf(cfgkey, "db.%s.scheme", ns.AsString().c_str());
        sp_setstring(env_, cfgkey, "document,key(0)", 0);
        sprintf(cfgkey, "db.%s.scheme.key", ns.AsString().c_str());
        sp_setstring(env, cfgkey, "string", 0);
        sprintf(cfgkey, "db.%s", ns.AsString().c_str());
        void* db = sp_getobject(env, cfgkey);
        if (NULL != db)
        {
            m_dbs[ns] = db;
        }
        else
        {
            int size;
            char *err = sp_getstring(env, "sophia.error", &size);
            if (NULL != err)
            {
                ERROR_LOG("Failed to create db with reason:%s", err);
                free(err);
            }
        }
        return db;
    }

    int SophiaEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        void* db = GetNamespace(ns, true);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        SophiaLocalContext& local_ctx = g_local_ctx.GetValue();
        Buffer& encode_buffer = local_ctx.GetEncodeBufferCache();
        ns.Encode(encode_buffer);
        encode_buffer.Write(key.data(), key.size());
        void *o = sp_document(db);
        sp_setstring(o, "key", encode_buffer.GetRawReadBuffer(), encode_buffer.ReadableBytes());
        sp_setstring(o, "value", value.data(), value.size());
        int rc = sp_set(db, o);
        return ENGINE_ERR(rc);
    }

    int SophiaEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        void* db = GetNamespace(key.GetNameSpace(), ctx.flags.create_if_notexist);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        SophiaLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ss[2];
        local_ctx.GetSlices(key, value, ss);
        void *o = sp_document(db);
        sp_setstring(o, "key", ss[0].data(), ss[0].size());
        sp_setstring(o, "value", ss[1].data(), ss[1].size());
        int rc = sp_set(db, o);
        return ENGINE_ERR(rc);
    }
    int SophiaEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
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
    int SophiaEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        void* db = GetNamespace(key.GetNameSpace(), false);
        if (NULL == db)
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        SophiaLocalContext& local_ctx = g_local_ctx.GetValue();
        std::string& valstr = local_ctx.GetStringCache();
        Slice ks = local_ctx.GetSlice(key);
        void *o = sp_document(db);
        sp_setstring(o, "key", ks.data(), ks.size());
        o = sp_get(db, o);
        if (o)
        {
            /* ensure key and value are correct */
            int size;
            char *ptr = sp_getstring(o, "key", &size);
            ptr = sp_getstring(o, "value", &size);
            valstr.assign(ptr, size);
            sp_destroy(o);
            Buffer valBuffer(const_cast<char*>(valstr.data()), 0, valstr.size());
            value.Decode(valBuffer, true);
            return 0;
        }
        else
        {
            return ERR_ENTRY_NOT_EXIST;
        }
    }
    int SophiaEngine::Del(Context& ctx, const KeyObject& key)
    {
        void* db = GetNamespace(key.GetNameSpace(), false);
        if (NULL == db)
        {
            return 0;
        }
        SophiaLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ks = local_ctx.GetSlice(key);
        void *o = sp_document(db);
        sp_setstring(o, "key", ks.data(), ks.size());
        int rc = sp_delete(db, o);
        return ENGINE_ERR(rc);
    }

    int SophiaEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
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

    bool SophiaEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    const std::string SophiaEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        return "sadasdas";
    }

    Iterator* SophiaEngine::Find(Context& ctx, const KeyObject& key)
    {
        SophiaLocalContext& local_ctx = g_local_ctx.GetValue();
        SophiaIterator* iter = NULL;
        NEW(iter, SophiaIterator(this,key.GetNameSpace()));
        void* db = GetNamespace(key.GetNameSpace(), false);
        if (NULL == db)
        {
            iter->MarkValid(false);
            return 0;
        }
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
        }
        void* cursor = sp_cursor(env_);
        void* o = sp_document(db);
        sp_setstring(o, "order", "<", 0);
        while ((o = sp_get(cursor, o)))
        {
            printf("%"PRIu32"\n", *(uint32_t*) sp_getstring(o, "key", NULL));
        }
        iter->SetIterator(cursor);
        if (key.GetType() > 0)
        {
            iter->Jump(key);
        }
        return iter;
    }

    int SophiaEngine::BeginWriteBatch(Context& ctx)
    {
        return 0;
    }
    int SophiaEngine::CommitWriteBatch(Context& ctx)
    {
        return 0;
    }
    int SophiaEngine::DiscardWriteBatch(Context& ctx)
    {
        return 0;
    }

    int SophiaEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        if (!GetNamespace(start.GetNameSpace(), false))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        return ERR_NOTSUPPORTED;
    }

    int SophiaEngine::ListNameSpaces(Context& ctx, DataArray& nss)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, true);
        DBTable::iterator it = m_dbs.begin();
        while (it != m_dbs.end())
        {
            nss.push_back(it->first);
            it++;
        }
        return 0;
    }

    int SophiaEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        DBTable::iterator found = m_dbs.find(ns);
        if (found != m_dbs.end())
        {
            sp_destroy(found->second);
            m_dbs.erase(found);
            return 0;
        }
        return 0;
    }

    int64_t SophiaEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        if (!GetNamespace(ns, ctx.flags.create_if_notexist))
        {
            return 0;
        }
        return -1;
    }

    void SophiaEngine::Stats(Context& ctx, std::string& str)
    {
        std::string stats;
//        str.append("leveldb_version:").append(stringfromll(leveldb::kMajorVersion)).append(".").append(stringfromll(leveldb::kMinorVersion)).append("\r\n");
//        m_db->GetProperty("leveldb.stats", &stats);
        str.append(stats);
    }

    bool SophiaIterator::Valid()
    {
        if (m_valid && NULL != m_iter && m_iter->Valid())
        {
            Data ns;
            GetRawKey(ns);
            return ns == m_ns;
        }
        return false;
    }
    void SophiaIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void SophiaIterator::CheckBound()
    {
        if (NULL != m_iter && m_iterate_upper_bound_key.GetType() > 0)
        {
//            if (m_iter->Valid())
//            {
//                if (Key(false).Compare(m_iterate_upper_bound_key) >= 0)
//                {
//                    m_valid = false;
//                }
//            }
        }
    }
    void SophiaIterator::Next()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
//        m_iter->Next();
        CheckBound();
    }
    void SophiaIterator::Prev()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
//        m_iter->Prev();
    }
    void SophiaIterator::Jump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
//        LevelDBLocalContext& rocks_ctx = g_local_ctx.GetValue();
//        Slice key_slice = next.Encode(rocks_ctx.GetEncodeBufferCache(), false, true);
//        m_iter->Seek(to_leveldb_slice(key_slice));
        CheckBound();
    }
    void SophiaIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
//        m_iter->SeekToFirst();
    }
    void SophiaIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
//        if (m_iterate_upper_bound_key.GetType() > 0)
//        {
//            Jump(m_iterate_upper_bound_key);
//            if (!m_iter->Valid())
//            {
//                m_iter->SeekToLast();
//            }
//            if (m_iter->Valid())
//            {
//                if (!Valid())
//                {
//                    Prev();
//                }
//            }
//        }
//        else
//        {
//            m_iter->SeekToLast();
//        }
    }

    KeyObject& SophiaIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
//        leveldb::Slice key = m_iter->key();
//        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
//        m_key.Decode(kbuf, clone_str, true);
        return m_key;
    }
    ValueObject& SophiaIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            return m_value;
        }
//        leveldb::Slice key = m_iter->value();
//        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
//        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice SophiaIterator::GetRawKey(Data& ns)
    {
//        leveldb::Slice s = m_iter->key();
//        /*
//         * trim namespace header
//         */
//        Buffer buf((char*) s.data(), 0, s.size());
//        ns.Decode(buf, false);
//        return Slice(buf.GetRawReadBuffer(), buf.ReadableBytes());
    }
    Slice SophiaIterator::RawKey()
    {
        Data ns;
        return GetRawKey(ns);
    }
    Slice SophiaIterator::RawValue()
    {
//        return to_ardb_slice(m_iter->value());
    }

    void SophiaIterator::Del()
    {
        if (NULL != m_engine && NULL != m_iter)
        {
//            leveldb::WriteOptions opt;
//            m_engine->m_db->Delete(opt, m_iter->key());
        }
    }

    SophiaIterator::~SophiaIterator()
    {
//        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
//        local_ctx.snapshot.Release();
        if(NULL != m_iter)
        {
            sp_destroy(m_iter);
        }
    }
}

