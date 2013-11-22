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
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "comparator.hpp"
#include "util/helpers.hpp"
#include <string.h>

#define ROCKSDB_SLICE(slice) rocksdb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
    int RocksDBComparator::Compare(const rocksdb::Slice& a,
		    const rocksdb::Slice& b) const
    {
	return ardb_compare_keys(a.data(), a.size(), b.data(), b.size());
    }

    void RocksDBComparator::FindShortestSeparator(std::string* start,
		    const rocksdb::Slice& limit) const
    {
	Buffer ak_buf(const_cast<char*>(start->data()), 0, start->size());
	Buffer bk_buf(const_cast<char*>(limit.data()), 0, limit.size());
	if (start->size() > 4 && limit.size() > 4)
	{
	    uint32 aheader, bheader;
	    BufferHelper::ReadFixUInt32(ak_buf, aheader);
	    BufferHelper::ReadFixUInt32(bk_buf, bheader);
	    uint8 type1 = aheader & 0xFF;
	    uint8 type2 = bheader & 0xFF;
	    uint32 adb = aheader >> 8;
	    uint32 bdb = bheader >> 8;
	    assert(adb <= bdb);
	    if (adb < bdb || type1 < type2)
	    {
		start->resize(4);
		start->assign(limit.data(), 0, 4);
	    }
	    else
	    {
		uint32 akeysize = 0, bkeysize = 0;
		assert(BufferHelper::ReadVarUInt32(ak_buf, akeysize));
		assert(BufferHelper::ReadVarUInt32(bk_buf, bkeysize));
		assert(akeysize <= bkeysize);
		if (akeysize < bkeysize)
		{
		    start->resize(ak_buf.GetReadIndex());
		    start->assign(limit.data(), ak_buf.GetReadIndex());
		}
		else
		{
		    size_t diff_index = ak_buf.GetReadIndex();
		    while ((diff_index < start->size()
				    && diff_index < limit.size())
				    && ((*start)[diff_index]
						    == limit[diff_index]))
		    {
			diff_index++;
		    }
		    if (diff_index >= start->size())
		    {
			// Do not shorten if one string is a prefix of the other
		    }
		    else
		    {
			if (diff_index >= start->size() || limit.size())
			{
			    return;
			}
			uint8_t diff_byte =
					static_cast<uint8_t>((*start)[diff_index]);
			if (diff_byte < static_cast<uint8_t>(0xff)
					&& diff_byte + 1
							< static_cast<uint8_t>(limit[diff_index]))
			{
			    (*start)[diff_index]++;
			    start->resize(diff_index + 1);
			    assert(Compare(*start, limit) < 0);
			}
		    }
		}
	    }
	}
    }

    void RocksDBComparator::FindShortSuccessor(std::string* key) const
    {
	if (key->size() >= 4)
	{
	    Buffer ak_buf(const_cast<char*>(key->data()), 0, key->size());
	    uint32 aheader;
	    BufferHelper::ReadFixUInt32(ak_buf, aheader);
	    uint8 type = aheader & 0xFF;
	    uint32 adb = aheader >> 8;
	    aheader = (adb << 8) + (type + 1);
	    aheader = htonl(aheader);
	    key->resize(4);
	    key->assign((const char*) (&aheader), 4);
	}
    }

    RocksDBEngineFactory::RocksDBEngineFactory(const Properties& props)
    {
	ParseConfig(props, m_cfg);

    }

    void RocksDBEngineFactory::ParseConfig(const Properties& props,
		    RocksDBConfig& cfg)
    {
	cfg.path = ".";
	conf_get_string(props, "data-dir", cfg.path);
	conf_get_int64(props, "leveldb.block_cache_size", cfg.block_cache_size);
	conf_get_int64(props, "leveldb.write_buffer_size",
			cfg.write_buffer_size);
	conf_get_int64(props, "leveldb.max_open_files", cfg.max_open_files);
	conf_get_int64(props, "leveldb.block_size", cfg.block_size);
	conf_get_int64(props, "leveldb.block_restart_interval",
			cfg.block_restart_interval);
	conf_get_int64(props, "leveldb.bloom_bits", cfg.bloom_bits);
	conf_get_int64(props, "leveldb.batch_commit_watermark",
			cfg.batch_commit_watermark);
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
	    m_options.filter_policy = rocksdb::NewBloomFilterPolicy(
			    cfg.bloom_bits);
	}

	make_dir(cfg.path);
	m_db_path = cfg.path;
	rocksdb::Status status = rocksdb::DB::Open(m_options, cfg.path.c_str(),
			&m_db);
	do
	{
	    if (status.ok())
	    {
		break;
	    }
	    else if (status.IsCorruption())
	    {
		ERROR_LOG("Failed to init engine:%s",
				status.ToString().c_str());
		status = rocksdb::RepairDB(cfg.path.c_str(), m_options);
		if (!status.ok())
		{
		    ERROR_LOG("Failed to repair:%s for %s", cfg.path.c_str(),
				    status.ToString().c_str());
		    return -1;
		}
		status = rocksdb::DB::Open(m_options, cfg.path.c_str(), &m_db);
	    }
	    else
	    {
		ERROR_LOG("Failed to init engine:%s",
				status.ToString().c_str());
		break;
	    }
	}
	while (1);
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
	rocksdb::Status s = m_db->Write(rocksdb::WriteOptions(), &holder.batch);
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

    int RocksDBEngine::Put(const Slice& key, const Slice& value)
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
	    s = m_db->Put(rocksdb::WriteOptions(), ROCKSDB_SLICE(key),
			    ROCKSDB_SLICE(value));
	}
	return s.ok() ? 0 : -1;
    }
    int RocksDBEngine::Get(const Slice& key, std::string* value)
    {
	rocksdb::ReadOptions options;
	ContextHolder& holder = m_context.GetValue();
	options.snapshot = holder.snapshot;
	rocksdb::Status s = m_db->Get(options, ROCKSDB_SLICE(key), value);
	return s.ok() ? 0 : -1;
    }
    int RocksDBEngine::Del(const Slice& key)
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

    Iterator* RocksDBEngine::Find(const Slice& findkey, bool cache)
    {
	rocksdb::ReadOptions options;
	options.fill_cache = cache;
	ContextHolder& holder = m_context.GetValue();
	if (NULL == holder.snapshot)
	{
	    holder.snapshot = m_db->GetSnapshot();
	}
	holder.snapshot_ref++;
	options.snapshot = holder.snapshot;
	rocksdb::Iterator* iter = m_db->NewIterator(options);
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
	std::string str;
	m_db->GetProperty("RocksDB.stats", &str);
	return str;
    }

    RocksDBIterator::~RocksDBIterator()
    {
	delete m_iter;
	m_engine->ReleaseContextSnapshot();
    }

}

