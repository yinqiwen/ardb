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

#include "rocksdb_engine.hpp"
#include "codec.hpp"
#include "util/helpers.hpp"
#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"
#include <string.h>
#include <stdarg.h>

#define ROCKSDB_SLICE(slice) rocksdb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
    class RocksDBLogger: public rocksdb::Logger
    {
        private:
            void Logv(const char* format, va_list ap)
            {
                char logbuf[1024];
                int len = vsnprintf(logbuf, sizeof(logbuf), format, ap);
                if(len < sizeof(logbuf))
                {
                    INFO_LOG(logbuf);
                }
            }
    };

    int RocksDBComparator::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const
    {
        return CommonComparator::Compare(a.data(), a.size(), b.data(), b.size());
    }

    void RocksDBComparator::FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const
    {
    }

    void RocksDBComparator::FindShortSuccessor(std::string* key) const
    {
    }

    RocksDBEngineFactory::RocksDBEngineFactory(const Properties& props)
    {
        ParseConfig(props, m_cfg);
    }

    void RocksDBEngineFactory::ParseConfig(const Properties& props, RocksDBConfig& cfg)
    {
        cfg.path = ".";
        conf_get_string(props, "data-dir", cfg.path);
        conf_get_int64(props, "rocksdb.block_cache_size", cfg.block_cache_size);
        conf_get_int64(props, "rocksdb.block_cache_compressed", cfg.block_cache_compressed_size);
        conf_get_int64(props, "rocksdb.write_buffer_size", cfg.write_buffer_size);
        conf_get_int64(props, "rocksdb.max_open_files", cfg.max_open_files);
        conf_get_int64(props, "rocksdb.block_size", cfg.block_size);
        conf_get_int64(props, "rocksdb.block_restart_interval", cfg.block_restart_interval);
        conf_get_int64(props, "rocksdb.bloom_bits", cfg.bloom_bits);
        conf_get_int64(props, "rocksdb.batch_commit_watermark", cfg.batch_commit_watermark);
        conf_get_string(props, "rocksdb.compression", cfg.compression);
        conf_get_bool(props, "rocksdb.logenable", cfg.logenable);
        conf_get_bool(props, "rocksdb.skip_log_error_on_recovery", cfg.skip_log_error_on_recovery);
        conf_get_int64(props, "rocksdb.flush_compact_rate_bytes_per_sec", cfg.flush_compact_rate_bytes_per_sec);
        conf_get_double(props, "rocksdb.hard_rate_limit", cfg.hard_rate_limit);
        conf_get_bool(props, "rocksdb.disableWAL", cfg.disableWAL);
        conf_get_int64(props, "rocksdb.max_manifest_file_size", cfg.max_manifest_file_size);
    }

    KeyValueEngine* RocksDBEngineFactory::CreateDB(const std::string& name)
    {
        RocksDBEngine* engine = new RocksDBEngine();
        RocksDBConfig cfg = m_cfg;
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
    void RocksDBEngineFactory::DestroyDB(KeyValueEngine* engine)
    {
        RocksDBEngine* RocksDB = (RocksDBEngine*) engine;
        std::string path = RocksDB->m_db_path;
        DELETE(engine);
        rocksdb::Options options;
        rocksdb::DestroyDB(path, options);
    }

    void RocksDBEngineFactory::CloseDB(KeyValueEngine* engine)
    {
        DELETE(engine);
    }
    void RocksDBIterator::SeekToFirst()
    {
        m_iter->SeekToFirst();
    }
    void RocksDBIterator::SeekToLast()
    {
        m_iter->SeekToLast();
    }
    void RocksDBIterator::Seek(const Slice& target)
    {
        m_iter->Seek(ROCKSDB_SLICE(target));
    }
    void RocksDBIterator::Next()
    {
        m_iter->Next();
    }
    void RocksDBIterator::Prev()
    {
        m_iter->Prev();
    }
    Slice RocksDBIterator::Key() const
    {
        return ARDB_SLICE(m_iter->key());
    }
    Slice RocksDBIterator::Value() const
    {
        return ARDB_SLICE(m_iter->value());
    }
    bool RocksDBIterator::Valid()
    {
        return m_iter->Valid();
    }

    RocksDBEngine::RocksDBEngine() :
            m_db(NULL)
    {

    }
    RocksDBEngine::~RocksDBEngine()
    {
        DELETE(m_db);
    }
    int RocksDBEngine::Init(const RocksDBConfig& cfg)
    {
        m_cfg = cfg;
        m_options.create_if_missing = true;
        m_options.comparator = &m_comparator;
        if (cfg.block_cache_size > 0)
        {
            m_options.block_cache = rocksdb::NewLRUCache(cfg.block_cache_size);
            //m_options.block_cache_compressed = rocksdb::NewLRUCache(cfg.block_cache_compressed_size);
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
            m_options.filter_policy = rocksdb::NewBloomFilterPolicy(cfg.bloom_bits);
        }

        if (!strcasecmp(cfg.compression.c_str(), "none"))
        {
            m_options.compression = rocksdb::kNoCompression;
        }
        else
        {
            m_options.compression = rocksdb::kSnappyCompression;
        }
        if(cfg.flush_compact_rate_bytes_per_sec > 0)
        {
            m_options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(cfg.flush_compact_rate_bytes_per_sec));
        }
        m_options.hard_rate_limit = cfg.hard_rate_limit;
        if(m_cfg.max_manifest_file_size > 0)
        {
            m_options.max_manifest_file_size = m_cfg.max_manifest_file_size;
        }
        m_options.OptimizeLevelStyleCompaction();
        m_options.IncreaseParallelism();

        if(cfg.logenable)
        {
            m_options.info_log.reset(new RocksDBLogger);
        }
        make_dir(cfg.path);
        m_db_path = cfg.path;
        rocksdb::Status status = rocksdb::DB::Open(m_options, cfg.path.c_str(), &m_db);
        do
        {
            if (status.ok())
            {
                break;
            }
            else if (status.IsCorruption())
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                status = rocksdb::RepairDB(cfg.path.c_str(), m_options);
                if (!status.ok())
                {
                    ERROR_LOG("Failed to repair:%s for %s", cfg.path.c_str(), status.ToString().c_str());
                    return -1;
                }
                status = rocksdb::DB::Open(m_options, cfg.path.c_str(), &m_db);
            }
            else
            {
                ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
                break;
            }
        } while (1);
        return status.ok() ? 0 : -1;
    }

    int RocksDBEngine::BeginBatchWrite()
    {
        m_context.GetValue().AddRef();
        return 0;
    }
    int RocksDBEngine::CommitBatchWrite()
    {
        ContextHolder& holder = m_context.GetValue();
        holder.ReleaseRef();
        if (holder.EmptyRef())
        {
            return FlushWriteBatch(holder);
        }
        return 0;
    }
    int RocksDBEngine::DiscardBatchWrite()
    {
        ContextHolder& holder = m_context.GetValue();
        holder.ReleaseRef();
        holder.Clear();
        return 0;
    }

    int RocksDBEngine::FlushWriteBatch(ContextHolder& holder)
    {
        rocksdb::WriteOptions options;
        options.disableWAL = m_cfg.disableWAL;
        rocksdb::Status s = m_db->Write(options, &holder.batch);
        holder.Clear();
        return s.ok() ? 0 : -1;
    }

    void RocksDBEngine::CompactRange(const Slice& begin, const Slice& end)
    {
        rocksdb::Slice s(begin.data(), begin.size());
        rocksdb::Slice e(end.data(), end.size());
        rocksdb::Slice* start = s.size() > 0 ? &s : NULL;
        rocksdb::Slice* endpos = e.size() > 0 ? &e : NULL;
        m_db->CompactRange(start, endpos);
    }

    void RocksDBEngine::ContextHolder::Put(const Slice& key, const Slice& value)
    {
        batch.Put(ROCKSDB_SLICE(key), ROCKSDB_SLICE(value));
        count++;
    }
    void RocksDBEngine::ContextHolder::Del(const Slice& key)
    {
        batch.Delete(ROCKSDB_SLICE(key));
        count++;
    }

    int RocksDBEngine::Put(const Slice& key, const Slice& value, const Options& options)
    {
        rocksdb::Status s = rocksdb::Status::OK();
        ContextHolder& holder = m_context.GetValue();
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
            rocksdb::WriteOptions options;
            options.disableWAL = m_cfg.disableWAL;
            s = m_db->Put(options, ROCKSDB_SLICE(key), ROCKSDB_SLICE(value));
        }
        return s.ok() ? 0 : -1;
    }
    int RocksDBEngine::Get(const Slice& key, std::string* value, const Options& options)
    {
        rocksdb::ReadOptions read_options;
        read_options.fill_cache = options.read_fill_cache;
        read_options.verify_checksums = false;
        ContextHolder& holder = m_context.GetValue();
        read_options.snapshot = holder.snapshot;
        rocksdb::Status s = m_db->Get(read_options, ROCKSDB_SLICE(key), value);
        return s.ok() ? 0 : -1;
    }
    int RocksDBEngine::Del(const Slice& key, const Options& options)
    {
        rocksdb::Status s = rocksdb::Status::OK();
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
            s = m_db->Delete(rocksdb::WriteOptions(), ROCKSDB_SLICE(key));
        }
        return s.ok() ? 0 : -1;
    }

    Iterator* RocksDBEngine::Find(const Slice& findkey, const Options& options)
    {
        rocksdb::ReadOptions read_options;
        read_options.fill_cache = options.seek_fill_cache;
        ContextHolder& holder = m_context.GetValue();
        if (NULL == holder.snapshot)
        {
            holder.snapshot = m_db->GetSnapshot();
        }
        holder.snapshot_ref++;
        read_options.snapshot = holder.snapshot;
        rocksdb::Iterator* iter = m_db->NewIterator(read_options);
        iter->Seek(ROCKSDB_SLICE(findkey));
        return new RocksDBIterator(this, iter);
    }

    void RocksDBEngine::ReleaseContextSnapshot()
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

    const std::string RocksDBEngine::Stats()
    {
        std::string str, version_info;

        m_db->GetProperty("rocksdb.stats", &str);
        version_info.append("RocksDB version:").append(stringfromll(rocksdb::kMajorVersion)).append(".").append(
                stringfromll(rocksdb::kMinorVersion)).append(".").append(stringfromll(ROCKSDB_PATCH)).append("\r\n");
        return version_info + str;
    }

    int RocksDBEngine::MaxOpenFiles()
    {
        return m_options.max_open_files;
    }

    RocksDBIterator::~RocksDBIterator()
    {
        delete m_iter;
        m_engine->ReleaseContextSnapshot();
    }

}

