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

#include "leveldb_engine.hpp"
#include "util/file_helper.hpp"
#include "leveldb/env.h"
#include "db/db_utils.hpp"
#include "db/db.hpp"
#include "thread/lock_guard.hpp"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
    static leveldb::DB* g_leveldb = NULL;
    static inline leveldb::Slice to_leveldb_slice(const Slice& slice)
    {
        return leveldb::Slice(slice.data(), slice.size());
    }
    static inline Slice to_ardb_slice(const leveldb::Slice& slice)
    {
        return Slice(slice.data(), slice.size());
    }
    class LevelDBLogger: public leveldb::Logger
    {
        private:
            void Logv(const char* format, va_list ap)
            {
                char logbuf[1024];
                int len = vsnprintf(logbuf, sizeof(logbuf), format, ap);
                if (len < sizeof(logbuf))
                {
                    INFO_LOG(logbuf);
                }
            }
    };

    class LevelDBComparator: public leveldb::Comparator
    {
        public:
            // Three-way comparison.  Returns value:
            //   < 0 iff "a" < "b",
            //   == 0 iff "a" == "b",
            //   > 0 iff "a" > "b"
            int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
            {
                return compare_keys(a.data(), a.size(), b.data(), b.size(), true);
            }

            // The name of the comparator.  Used to check for comparator
            // mismatches (i.e., a DB created with one comparator is
            // accessed using a different comparator.
            //
            // The client of this package should switch to a new name whenever
            // the comparator implementation changes in a way that will cause
            // the relative ordering of any two keys to change.
            //
            // Names starting with "leveldb." are reserved and should not be used
            // by any clients of this package.
            const char* Name() const
            {
                return "ardb.comparator";
            }

            // Advanced functions: these are used to reduce the space requirements
            // for internal data structures like index blocks.

            // If *start < limit, changes *start to a short string in [start,limit).
            // Simple comparator implementations may return with *start unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            void FindShortestSeparator(std::string* start, const leveldb::Slice& limit) const
            {

            }

            // Changes *key to a short string >= *key.
            // Simple comparator implementations may return with *key unchanged,
            // i.e., an implementation of this method that does nothing is correct.
            void FindShortSuccessor(std::string* key) const
            {

            }
    };

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
    struct LocalSnapshot
    {
            const leveldb::Snapshot* snapshot;
            uint32_t ref;
            LocalSnapshot() :
                    snapshot(NULL), ref(0)
            {
            }
            const leveldb::Snapshot* Get()
            {
                ref++;
                if (snapshot == NULL)
                {
                    snapshot = g_leveldb->GetSnapshot();
                }
                return snapshot;
            }
            const leveldb::Snapshot* Peek()
            {
                return snapshot;
            }
            void Release()
            {
                if (snapshot == NULL)
                {
                    return;
                }
                ref--;
                if (ref <= 0)
                {
                    g_leveldb->ReleaseSnapshot(snapshot);
                    snapshot = NULL;
                    ref = 0;
                }
            }
    };

#define DEFAULT_LEVELDB_LOCAL_MULTI_CACHE_SIZE 10
    struct LevelDBLocalContext: public DBLocalContext
    {
            LoacalWriteBatch batch;
            LocalSnapshot snapshot;
            typedef TreeMap<int, leveldb::Status>::Type ErrMap;
            ErrMap err_map;
    };

    static ThreadLocal<LevelDBLocalContext> g_local_ctx;

    static inline int leveldb_err(const leveldb::Status& s)
    {
        if (s.ok())
        {
            return 0;
        }
        if (s.IsNotFound())
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LevelDBLocalContext::ErrMap& err_map = g_local_ctx.GetValue().err_map;
        int err = -1234;
        if (!err_map.empty())
        {
            err = err_map.begin()->first;
            err--;
            if (err_map.size() > 10)
            {
                err_map.erase(err_map.rbegin()->first);
            }
        }
        err_map[err] = s;
        return err + STORAGE_ENGINE_ERR_OFFSET;
    }

    LevelDBEngine::LevelDBEngine() :
            m_db(NULL)
    {

    }
    LevelDBEngine::~LevelDBEngine()
    {
        DELETE(m_db);
        DELETE(m_options.block_cache);
        DELETE(m_options.filter_policy);
    }

    int LevelDBEngine::MaxOpenFiles()
    {
        return m_cfg.max_open_files;
    }

    int LevelDBEngine::Init(const std::string& dir, const std::string& options)
    {
        Properties props;
        parse_conf_content(options, props);
        conf_get_int64(props, "block_cache_size", m_cfg.block_cache_size);
        conf_get_int64(props, "write_buffer_size", m_cfg.write_buffer_size);
        conf_get_int64(props, "max_open_files", m_cfg.max_open_files);
        conf_get_int64(props, "block_size", m_cfg.block_size);
        conf_get_int64(props, "block_restart_interval", m_cfg.block_restart_interval);
        conf_get_int64(props, "bloom_bits", m_cfg.bloom_bits);
        conf_get_int64(props, "batch_commit_watermark", m_cfg.batch_commit_watermark);
        conf_get_string(props, "compression", m_cfg.compression);
        conf_get_bool(props, "logenable", m_cfg.logenable);

        static LevelDBComparator comparator;
        m_options.create_if_missing = true;
        m_options.comparator = &comparator;
        if (m_cfg.block_cache_size > 0)
        {
            leveldb::Cache* cache = leveldb::NewLRUCache(m_cfg.block_cache_size);
            m_options.block_cache = cache;
        }
        if (m_cfg.block_size > 0)
        {
            m_options.block_size = m_cfg.block_size;
        }
        if (m_cfg.block_restart_interval > 0)
        {
            m_options.block_restart_interval = m_cfg.block_restart_interval;
        }
        if (m_cfg.write_buffer_size > 0)
        {
            m_options.write_buffer_size = m_cfg.write_buffer_size;
        }
        m_options.max_open_files = m_cfg.max_open_files;
        if (m_cfg.bloom_bits > 0)
        {
            m_options.filter_policy = leveldb::NewBloomFilterPolicy(m_cfg.bloom_bits);
        }
        if (!strcasecmp(m_cfg.compression.c_str(), "none"))
        {
            m_options.compression = leveldb::kNoCompression;
        }
        else
        {
            m_options.compression = leveldb::kSnappyCompression;
        }
        if (m_cfg.logenable)
        {
            m_options.info_log = new LevelDBLogger;
        }
        m_cfg.path = dir;
        make_dir(m_cfg.path);
        leveldb::Status status = leveldb::DB::Open(m_options, m_cfg.path.c_str(), &g_leveldb);
        do
        {
            if (status.ok())
            {
                break;
            }
            else if (status.IsCorruption())
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                status = leveldb::RepairDB(m_cfg.path.c_str(), m_options);
                if (!status.ok())
                {
                    ERROR_LOG("Failed to repair:%s for %s", m_cfg.path.c_str(), status.ToString().c_str());
                    return -1;
                }
                status = leveldb::DB::Open(m_options, m_cfg.path.c_str(), &m_db);
            }
            else
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                break;
            }
        }
        while (1);
        m_db = g_leveldb;
        if (status.ok())
        {
            leveldb::ReadOptions opt;
            leveldb::Iterator* iter = m_db->NewIterator(opt);
            if (NULL != iter)
            {
                iter->SeekToFirst();
            }
            KeyObject k;
            while (NULL != iter && iter->Valid())
            {
                leveldb::Slice key = iter->key();
                Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
                k.Decode(kbuf, true, true);
                m_nss.insert(k.GetNameSpace());
                Data next_ns;
                std::string next_ns_str = k.GetNameSpace().AsString();
                next_ns_str.append(1, 0);
                next_ns.SetString(next_ns_str, false);
                k.SetNameSpace(next_ns);
                k.SetType(KEY_META);
                k.SetKey("");

                Buffer buffer;
                k.Encode(buffer, false, true);
                leveldb::Slice next_slice(buffer.GetRawReadBuffer(), buffer.ReadableBytes());
                iter->Seek(next_slice);
            }
            DELETE(iter);
        }
        return status.ok() ? 0 : -1;
    }

    int LevelDBEngine::Repair(const std::string& dir)
    {
        static LevelDBComparator comparator;
        static LevelDBLogger logger;
        m_options.comparator = &comparator;
        m_options.info_log = &logger;
        leveldb::Status status = leveldb::RepairDB(dir, m_options);
        return status.ok() ? 0 : -1;
    }

    bool LevelDBEngine::GetNamespace(const Data& ns, bool create_if_missing)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, !create_if_missing);
        DataSet::iterator found = m_nss.find(ns);
        if (found != m_nss.end())
        {
            return true;
        }
        if (!create_if_missing)
        {
            return false;
        }
        m_nss.insert(ns);
        return true;
    }

    int LevelDBEngine::PutRaw(Context& ctx, const Data& ns, const Slice& key, const Slice& value)
    {
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        leveldb::WriteOptions opt;
        Buffer& encode_buffer = local_ctx.GetEncodeBufferCache();
        ns.Encode(encode_buffer);
        encode_buffer.Write(key.data(), key.size());
        leveldb::Slice key_slice(encode_buffer.GetRawReadBuffer(), encode_buffer.ReadableBytes());
        leveldb::Slice value_slice(value.data(), value.size());
        leveldb::Status s;
        leveldb::WriteBatch* batch = local_ctx.batch.Ref();
        if (NULL != batch)
        {
            batch->Put(key_slice, value_slice);
        }
        else
        {
            s = m_db->Put(opt, key_slice, value_slice);
        }
        {
            RWLockGuard<SpinRWLock> guard(m_lock, false);
            m_nss.insert(ns);
        }
        return leveldb_err(s);
    }

    int LevelDBEngine::Put(Context& ctx, const KeyObject& key, const ValueObject& value)
    {
        leveldb::Status s;
        if (!GetNamespace(key.GetNameSpace(), ctx.flags.create_if_notexist))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        leveldb::WriteOptions opt;
        Slice ss[2];
        local_ctx.GetSlices(key, value, ss);
        leveldb::Slice key_slice(ss[0].data(), ss[0].size());
        leveldb::Slice value_slice(ss[1].data(), ss[1].size());
        leveldb::WriteBatch* batch = local_ctx.batch.Ref();
        if (NULL != batch)
        {
            batch->Put(key_slice, value_slice);
        }
        else
        {
            s = m_db->Put(opt, key_slice, value_slice);
        }
        return leveldb_err(s);
    }
    int LevelDBEngine::MultiGet(Context& ctx, const KeyObjectArray& keys, ValueObjectArray& values, ErrCodeArray& errs)
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
    int LevelDBEngine::Get(Context& ctx, const KeyObject& key, ValueObject& value)
    {
        leveldb::Status s;
        if (!GetNamespace(key.GetNameSpace(), ctx.flags.create_if_notexist))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        leveldb::ReadOptions opt;
        opt.snapshot = local_ctx.snapshot.Peek();
        std::string& valstr = local_ctx.GetStringCache();
        Slice ks = local_ctx.GetSlice(key);
        leveldb::Slice key_slice(ks.data(), ks.size());
        s = m_db->Get(opt, key_slice, &valstr);
        int err = leveldb_err(s);
        if (0 != err)
        {
            return err;
        }
        Buffer valBuffer(const_cast<char*>(valstr.data()), 0, valstr.size());
        value.Decode(valBuffer, true);
        return 0;
    }
    int LevelDBEngine::DelKeySlice(leveldb::WriteBatch* batch, const leveldb::Slice& key_slice)
    {
        leveldb::Status s;
        leveldb::WriteOptions opt;
        if (NULL != batch)
        {
            batch->Delete(key_slice);
        }
        else
        {
            s = m_db->Delete(opt, key_slice);
        }
        return leveldb_err(s);
    }
    int LevelDBEngine::DelKey(Context& ctx, const leveldb::Slice& key)
    {
        leveldb::Status s;
        if (!GetNamespace(ctx.ns, ctx.flags.create_if_notexist))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        return DelKeySlice(local_ctx.batch.Ref(), key);
    }
    int LevelDBEngine::Del(Context& ctx, const KeyObject& key)
    {
        leveldb::Status s;
        if (!GetNamespace(key.GetNameSpace(), ctx.flags.create_if_notexist))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        Slice ks = local_ctx.GetSlice(key);
        leveldb::Slice key_slice(ks.data(), ks.size());
        return DelKeySlice(local_ctx.batch.Ref(), key_slice);
    }

    int LevelDBEngine::Merge(Context& ctx, const KeyObject& key, uint16_t op, const DataArray& args)
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

    bool LevelDBEngine::Exists(Context& ctx, const KeyObject& key)
    {
        ValueObject val;
        return Get(ctx, key, val) == 0;
    }

    const std::string LevelDBEngine::GetErrorReason(int err)
    {
        err = err - STORAGE_ENGINE_ERR_OFFSET;
        LevelDBLocalContext::ErrMap& err_map = g_local_ctx.GetValue().err_map;
        if (err_map.count(err) > 0)
        {
            return err_map[err].ToString();
        }
        return "";
    }

    Iterator* LevelDBEngine::Find(Context& ctx, const KeyObject& key)
    {
        return Find(ctx, key, true);
    }

    Iterator* LevelDBEngine::Find(Context& ctx, const KeyObject& key, bool check_ns)
    {
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        leveldb::ReadOptions opt;
        if (NULL != ctx.engine_snapshot)
        {
            opt.snapshot = (const leveldb::Snapshot*) ctx.engine_snapshot;
        }
        else
        {
            opt.snapshot = local_ctx.snapshot.Get();
        }
        LevelDBIterator* iter = NULL;
        NEW(iter, LevelDBIterator(this,key.GetNameSpace()));
        if (check_ns && !GetNamespace(key.GetNameSpace(), false))
        {
            iter->MarkValid(false);
            return iter;
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
        leveldb::Iterator* rocksiter = m_db->NewIterator(opt);
        iter->SetIterator(rocksiter);
        //if (key.GetType() > 0)
        //{
        iter->Jump(key);
        //}
        //else
        //{
        //    rocksiter->SeekToFirst();
        //}
        return iter;
    }

    int LevelDBEngine::BeginWriteBatch(Context& ctx)
    {
        LevelDBLocalContext& rocks_ctx = g_local_ctx.GetValue();
        rocks_ctx.batch.AddRef();
        return 0;
    }
    int LevelDBEngine::CommitWriteBatch(Context& ctx)
    {
        LevelDBLocalContext& rocks_ctx = g_local_ctx.GetValue();
        if (rocks_ctx.batch.ReleaseRef(false) == 0)
        {
            leveldb::WriteOptions opt;
            m_db->Write(opt, &rocks_ctx.batch.GetBatch());
            rocks_ctx.batch.Clear();
        }
        return 0;
    }
    int LevelDBEngine::DiscardWriteBatch(Context& ctx)
    {
        LevelDBLocalContext& rocks_ctx = g_local_ctx.GetValue();
        if (rocks_ctx.batch.ReleaseRef(true) == 0)
        {
            rocks_ctx.batch.Clear();
        }
        return 0;
    }

    int LevelDBEngine::Compact(Context& ctx, const KeyObject& start, const KeyObject& end)
    {
        if (!GetNamespace(start.GetNameSpace(), false))
        {
            return ERR_ENTRY_NOT_EXIST;
        }
        Buffer start_buffer, end_buffer;
        leveldb::Slice start_key = to_leveldb_slice(start.Encode(start_buffer, false, true));
        leveldb::Slice end_key = to_leveldb_slice(end.Encode(end_buffer, false, true));
        m_db->CompactRange(start.IsValid() ? &start_key : NULL, end.IsValid() ? &end_key : NULL);
        return 0;
    }

    int LevelDBEngine::ListNameSpaces(Context& ctx, DataArray& nss)
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

    int LevelDBEngine::DropNameSpace(Context& ctx, const Data& ns)
    {
        RWLockGuard<SpinRWLock> guard(m_lock, false);
        if (m_nss.count(ns) == 0)
        {
            return 0;
        }
        KeyObject start(ns, KEY_META, "");
        ctx.flags.iterate_multi_keys = 1;

        Iterator* iter = Find(ctx, start, false);
        while (iter->Valid())
        {
            KeyObject& k = iter->Key(false);
            if (k.GetNameSpace() == ns)
            {
                leveldb::WriteOptions opt;
                m_db->Delete(opt, ((LevelDBIterator*) iter)->m_iter->key());
            }
            else
            {
                break;
            }
            iter->Next();
        }
        DELETE(iter);
        m_nss.erase(ns);
        return 0;
    }

    int64_t LevelDBEngine::EstimateKeysNum(Context& ctx, const Data& ns)
    {
        if (!GetNamespace(ns, ctx.flags.create_if_notexist))
        {
            return 0;
        }
        return -1;
    }

    void LevelDBEngine::Stats(Context& ctx, std::string& str)
    {
        std::string stats;
        str.append("leveldb_version:").append(stringfromll(leveldb::kMajorVersion)).append(".").append(
                stringfromll(leveldb::kMinorVersion)).append("\r\n");
        m_db->GetProperty("leveldb.stats", &stats);
        str.append(stats);
    }

    EngineSnapshot LevelDBEngine::CreateSnapshot()
    {
        return m_db->GetSnapshot();
    }
    void LevelDBEngine::ReleaseSnapshot(EngineSnapshot s)
    {
        if (NULL == s)
        {
            return;
        }
        m_db->ReleaseSnapshot((leveldb::Snapshot*) s);
    }

    bool LevelDBIterator::Valid()
    {
        if (m_valid && NULL != m_iter && m_iter->Valid())
        {
            Data ns;
            GetRawKey(ns);
            return ns == m_ns;
        }
        return false;
    }
    void LevelDBIterator::ClearState()
    {
        m_key.Clear();
        m_value.Clear();
        m_valid = true;
    }
    void LevelDBIterator::CheckBound()
    {
        if (NULL != m_iter && m_iterate_upper_bound_key.GetType() > 0)
        {
            if (m_iter->Valid())
            {
                if (Key(false).Compare(m_iterate_upper_bound_key) >= 0)
                {
                    m_valid = false;
                }
            }
        }
    }
    void LevelDBIterator::Next()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        m_iter->Next();
        CheckBound();
    }
    void LevelDBIterator::Prev()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        m_iter->Prev();
    }
    void LevelDBIterator::Jump(const KeyObject& next)
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        LevelDBLocalContext& rocks_ctx = g_local_ctx.GetValue();
        Slice key_slice = next.Encode(rocks_ctx.GetEncodeBufferCache(), false, true);
        m_iter->Seek(to_leveldb_slice(key_slice));
        CheckBound();
    }
    void LevelDBIterator::JumpToFirst()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        m_iter->SeekToFirst();
    }
    void LevelDBIterator::JumpToLast()
    {
        ClearState();
        if (NULL == m_iter)
        {
            return;
        }
        if (m_iterate_upper_bound_key.GetType() > 0)
        {
            Jump(m_iterate_upper_bound_key);
            if (!m_iter->Valid())
            {
                m_iter->SeekToLast();
            }
            if (m_iter->Valid())
            {
                if (!Valid())
                {
                    Prev();
                }
            }
        }
        else
        {
            m_iter->SeekToLast();
        }
    }

    KeyObject& LevelDBIterator::Key(bool clone_str)
    {
        if (m_key.GetType() > 0)
        {
            if (clone_str && m_key.GetKey().IsCStr())
            {
                m_key.CloneStringPart();
            }
            return m_key;
        }
        leveldb::Slice key = m_iter->key();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_key.Decode(kbuf, clone_str, true);
        return m_key;
    }
    ValueObject& LevelDBIterator::Value(bool clone_str)
    {
        if (m_value.GetType() > 0)
        {
            if (clone_str)
            {
                m_value.CloneStringPart();
            }
            return m_value;
        }
        leveldb::Slice key = m_iter->value();
        Buffer kbuf(const_cast<char*>(key.data()), 0, key.size());
        m_value.Decode(kbuf, clone_str);
        return m_value;
    }
    Slice LevelDBIterator::GetRawKey(Data& ns)
    {
        leveldb::Slice s = m_iter->key();
        /*
         * trim namespace header
         */
        Buffer buf((char*) s.data(), 0, s.size());
        ns.Decode(buf, false);
        return Slice(buf.GetRawReadBuffer(), buf.ReadableBytes());
    }
    Slice LevelDBIterator::RawKey()
    {
        Data ns;
        return GetRawKey(ns);
    }
    Slice LevelDBIterator::RawValue()
    {
        return to_ardb_slice(m_iter->value());
    }

    void LevelDBIterator::Del()
    {
        if (NULL != m_engine && NULL != m_iter)
        {
            Context tmpctx;
            tmpctx.ns = m_ns;
            m_engine->DelKey(tmpctx, m_iter->key());
//            leveldb::WriteOptions opt;
//            m_engine->m_db->Delete(opt, m_iter->key());
        }
    }

    LevelDBIterator::~LevelDBIterator()
    {
        LevelDBLocalContext& local_ctx = g_local_ctx.GetValue();
        local_ctx.snapshot.Release();
        DELETE(m_iter);
    }

}

