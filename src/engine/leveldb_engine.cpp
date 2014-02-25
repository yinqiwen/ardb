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
#include "ardb.hpp"
#include "data_format.hpp"
#include "util/helpers.hpp"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
    int LevelDBComparator::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
    {
        return ardb_compare_keys(a.data(), a.size(), b.data(), b.size());
    }

    void LevelDBComparator::FindShortestSeparator(std::string* start, const leveldb::Slice& limit) const
    {
        //DO nothing
    }

    void LevelDBComparator::FindShortSuccessor(std::string* key) const
    {
        //DO nothing
    }

    LevelDBEngineFactory::LevelDBEngineFactory(const Properties& props)
    {
        ParseConfig(props, m_cfg);

    }

    void LevelDBEngineFactory::ParseConfig(const Properties& props, LevelDBConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
        conf_get_int64(props, "leveldb.block_cache_size", cfg.block_cache_size);
        conf_get_int64(props, "leveldb.write_buffer_size", cfg.write_buffer_size);
        conf_get_int64(props, "leveldb.max_open_files", cfg.max_open_files);
        conf_get_int64(props, "leveldb.block_size", cfg.block_size);
        conf_get_int64(props, "leveldb.block_restart_interval", cfg.block_restart_interval);
        conf_get_int64(props, "leveldb.bloom_bits", cfg.bloom_bits);
        conf_get_int64(props, "leveldb.batch_commit_watermark", cfg.batch_commit_watermark);
    }

    KeyValueEngine* LevelDBEngineFactory::CreateDB(const std::string& name)
    {
        LevelDBEngine* engine = new LevelDBEngine();
        LevelDBConfig cfg = m_cfg;
        char tmp[cfg.path.size() + name.size() + 10];
        sprintf(tmp, "%s/%s", cfg.path.c_str(), name.c_str());
        cfg.path = tmp;
        if (engine->Init(cfg) != 0)
        {
            DELETE(engine);
            ERROR_LOG("Failed to create DB:%s", name.c_str());
            return NULL;
        }
        return engine;
    }
    void LevelDBEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        LevelDBEngine* leveldb = (LevelDBEngine*) engine;
        std::string path = leveldb->m_db_path;
        DELETE(engine);
        leveldb::Options options;
        leveldb::DestroyDB(path, options);
    }

    void LevelDBEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }
    void LevelDBIterator::SeekToFirst()
    {
        m_iter->SeekToFirst();
    }
    void LevelDBIterator::SeekToLast()
    {
        m_iter->SeekToLast();
    }
    void LevelDBIterator::Seek(const Slice& target)
    {
        m_iter->Seek(LEVELDB_SLICE(target));
    }
    void LevelDBIterator::Next()
    {
        m_iter->Next();
    }
    void LevelDBIterator::Prev()
    {
        m_iter->Prev();
    }
    Slice LevelDBIterator::Key() const
    {
        return ARDB_SLICE(m_iter->key());
    }
    Slice LevelDBIterator::Value() const
    {
        return ARDB_SLICE(m_iter->value());
    }
    bool LevelDBIterator::Valid()
    {
        return m_iter->Valid();
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
    int LevelDBEngine::Init(const LevelDBConfig& cfg)
    {
        m_cfg = cfg;
        m_options.create_if_missing = true;
        m_options.comparator = &m_comparator;
        if (cfg.block_cache_size > 0)
        {
            leveldb::Cache* cache = leveldb::NewLRUCache(cfg.block_cache_size);
            m_options.block_cache = cache;
        }
        if (cfg.block_size > 0)
        {
            m_options.block_size = cfg.block_size;
        }
        if (cfg.block_restart_interval > 0)
        {
            m_options.block_restart_interval = cfg.block_restart_interval;
        }
        if (cfg.write_buffer_size > 0)
        {
            m_options.write_buffer_size = cfg.write_buffer_size;
        }
        m_options.max_open_files = cfg.max_open_files;
        if (cfg.bloom_bits > 0)
        {
            m_options.filter_policy = leveldb::NewBloomFilterPolicy(cfg.bloom_bits);
        }

        make_dir(cfg.path);
        m_db_path = cfg.path;
        leveldb::Status status = leveldb::DB::Open(m_options, cfg.path.c_str(), &m_db);
        do
        {
            if (status.ok())
            {
                break;
            }
            else if (status.IsCorruption())
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                status = leveldb::RepairDB(cfg.path.c_str(), m_options);
                if (!status.ok())
                {
                    ERROR_LOG("Failed to repair:%s for %s", cfg.path.c_str(), status.ToString().c_str());
                    return -1;
                }
                status = leveldb::DB::Open(m_options, cfg.path.c_str(), &m_db);
            }
            else
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                break;
            }
        } while (1);
        return status.ok() ? 0 : -1;
    }

    int LevelDBEngine::BeginBatchWrite()
    {
        m_context.GetValue().AddRef();
        return 0;
    }
    int LevelDBEngine::CommitBatchWrite()
    {
        ContextHolder& holder = m_context.GetValue();
        holder.ReleaseRef();
        if (holder.EmptyRef())
        {
            return FlushWriteBatch(holder);
        }
        return 0;
    }
    int LevelDBEngine::DiscardBatchWrite()
    {
        ContextHolder& holder = m_context.GetValue();
        holder.ReleaseRef();
        holder.Clear();
        return 0;
    }

    int LevelDBEngine::FlushWriteBatch(ContextHolder& holder)
    {
        leveldb::Status s = m_db->Write(leveldb::WriteOptions(), &holder.batch);
        holder.Clear();
        return s.ok() ? 0 : -1;
    }

    void LevelDBEngine::CompactRange(const Slice& begin, const Slice& end)
    {
        leveldb::Slice s(begin.data(), begin.size());
        leveldb::Slice e(end.data(), end.size());
        leveldb::Slice* start = s.size() > 0 ? &s : NULL;
        leveldb::Slice* endpos = e.size() > 0 ? &e : NULL;
        m_db->CompactRange(start, endpos);
    }

    void LevelDBEngine::ContextHolder::Put(const Slice& key, const Slice& value)
    {
        batch.Put(LEVELDB_SLICE(key), LEVELDB_SLICE(value));
        count++;
    }
    void LevelDBEngine::ContextHolder::Del(const Slice& key)
    {
        batch.Delete(LEVELDB_SLICE(key));
        count++;
    }

    int LevelDBEngine::Put(const Slice& key, const Slice& value)
    {
        leveldb::Status s = leveldb::Status::OK();
        ContextHolder& holder = m_context.GetValue();
        int ret = 0;
        if (!holder.EmptyRef())
        {
            holder.Put(key, value);
            if (holder.count >= (uint32) m_cfg.batch_commit_watermark)
            {
                FlushWriteBatch(holder);
            }
        }
        else
        {
            s = m_db->Put(leveldb::WriteOptions(), LEVELDB_SLICE(key), LEVELDB_SLICE(value));
            if (!s.ok())
            {
                WARN_LOG("Failed to write data for reason:%s", s.ToString().c_str());
                ret = -1;
            }
        }
        return ret;
    }
    int LevelDBEngine::Get(const Slice& key, std::string* value, bool fill_cache)
    {
        leveldb::ReadOptions options;
        options.fill_cache = fill_cache;
        ContextHolder& holder = m_context.GetValue();
        options.snapshot = holder.snapshot;
        leveldb::Status s = m_db->Get(options, LEVELDB_SLICE(key), value);
        return s.ok() ? 0 : -1;
    }
    int LevelDBEngine::Del(const Slice& key)
    {
        leveldb::Status s = leveldb::Status::OK();
        ContextHolder& holder = m_context.GetValue();
        if (!holder.EmptyRef())
        {
            holder.Del(key);
            if (holder.count >= (uint32) m_cfg.batch_commit_watermark)
            {
                FlushWriteBatch(holder);
            }
        }
        else
        {
            s = m_db->Delete(leveldb::WriteOptions(), LEVELDB_SLICE(key));
        }
        return s.ok() ? 0 : -1;
    }

    Iterator* LevelDBEngine::Find(const Slice& findkey, bool cache)
    {
        leveldb::ReadOptions options;
        options.fill_cache = cache;
        ContextHolder& holder = m_context.GetValue();
        if (NULL == holder.snapshot)
        {
            holder.snapshot = m_db->GetSnapshot();
        }
        holder.snapshot_ref++;
        options.snapshot = holder.snapshot;
        leveldb::Iterator* iter = m_db->NewIterator(options);
        iter->Seek(LEVELDB_SLICE(findkey));
        return new LevelDBIterator(this, iter);
    }

    void LevelDBEngine::ReleaseContextSnapshot()
    {
        ContextHolder& holder = m_context.GetValue();
        if (NULL != holder.snapshot)
        {
            holder.snapshot_ref--;
            if (holder.snapshot_ref == 0)
            {
                m_db->ReleaseSnapshot(holder.snapshot);
                holder.snapshot = NULL;
            }
        }
    }

    const std::string LevelDBEngine::Stats()
    {
        std::string str;
        m_db->GetProperty("leveldb.stats", &str);
        return str;
    }

    LevelDBIterator::~LevelDBIterator()
    {
        delete m_iter;
        m_engine->ReleaseContextSnapshot();
    }

}

