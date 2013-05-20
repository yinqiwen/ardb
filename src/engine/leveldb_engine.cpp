/*
 * leveldb_engine.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "leveldb_engine.hpp"
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "comparator.hpp"
#include "util/helpers.hpp"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define ARDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace ardb
{
	int LevelDBComparator::Compare(const leveldb::Slice& a,
			const leveldb::Slice& b) const
	{
		return ardb_compare_keys(a.data(), a.size(), b.data(), b.size());
	}

	void LevelDBComparator::FindShortestSeparator(std::string* start,
			const leveldb::Slice& limit) const
	{
	}

	void LevelDBComparator::FindShortSuccessor(std::string* key) const
	{

	}

	LevelDBEngineFactory::LevelDBEngineFactory(const Properties& props)
	{
		ParseConfig(props, m_cfg);

	}

	void LevelDBEngineFactory::ParseConfig(const Properties& props,
			LevelDBConfig& cfg)
	{
		cfg.path = ".";
		conf_get_string(props, "dir", cfg.path);
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

	KeyValueEngine* LevelDBEngineFactory::CreateDB(const DBID& db)
	{
		LevelDBEngine* engine = new LevelDBEngine();
		LevelDBConfig cfg = m_cfg;
		char tmp[cfg.path.size() + db.size() + 10];
		sprintf(tmp, "%s/%s", cfg.path.c_str(), db.c_str());
		cfg.path = tmp;
		if (engine->Init(cfg) != 0)
		{
			DELETE(engine);
			ERROR_LOG("Failed to create DB:%s", db.c_str());
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
	}
	int LevelDBEngine::Init(const LevelDBConfig& cfg)
	{
		m_cfg = cfg;
		leveldb::Options options;
		options.create_if_missing = true;
		options.comparator = &m_comparator;
		if (cfg.block_cache_size > 0)
		{
			leveldb::Cache* cache = leveldb::NewLRUCache(cfg.block_cache_size);
			options.block_cache = cache;
		}
		if (cfg.block_size > 0)
		{
			options.block_size = cfg.block_size;
		}
		if (cfg.block_restart_interval > 0)
		{
			options.block_restart_interval = cfg.block_restart_interval;
		}
		if (cfg.write_buffer_size > 0)
		{
			options.write_buffer_size = cfg.write_buffer_size;
		}
		options.max_open_files = cfg.max_open_files;
		if (cfg.bloom_bits > 0)
		{
			options.filter_policy = leveldb::NewBloomFilterPolicy(
					cfg.bloom_bits);
		}

		make_dir(cfg.path);
		m_db_path = cfg.path;
		leveldb::Status status = leveldb::DB::Open(options, cfg.path.c_str(),
				&m_db);
		if (!status.ok())
		{
			ERROR_LOG("Failed to init engine:%s", status.ToString().c_str());
		}
		return status.ok() ? 0 : -1;
	}

	int LevelDBEngine::BeginBatchWrite()
	{
		m_batch_local.GetValue().AddRef();
		return 0;
	}
	int LevelDBEngine::CommitBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		if (holder.EmptyRef())
		{
			return FlushWriteBatch(holder);
		}
		return 0;
	}
	int LevelDBEngine::DiscardBatchWrite()
	{
		BatchHolder& holder = m_batch_local.GetValue();
		holder.ReleaseRef();
		holder.Clear();
		return 0;
	}

	int LevelDBEngine::FlushWriteBatch(BatchHolder& holder)
	{
		leveldb::Status s = m_db->Write(leveldb::WriteOptions(), &holder.batch);
		holder.Clear();
		return s.ok() ? 0 : -1;
	}

	int LevelDBEngine::Put(const Slice& key, const Slice& value)
	{
		leveldb::Status s = leveldb::Status::OK();
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			holder.batch.Put(LEVELDB_SLICE(key), LEVELDB_SLICE(value));
		}
		else
		{
			s = m_db->Put(leveldb::WriteOptions(), LEVELDB_SLICE(key),
					LEVELDB_SLICE(value));
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Get(const Slice& key, std::string* value)
	{
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(),
		LEVELDB_SLICE(key), value);
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Del(const Slice& key)
	{
		leveldb::Status s = leveldb::Status::OK();
		BatchHolder& holder = m_batch_local.GetValue();
		if (!holder.EmptyRef())
		{
			holder.batch.Delete(LEVELDB_SLICE(key));
		}
		else
		{
			s = m_db->Delete(leveldb::WriteOptions(), LEVELDB_SLICE(key));
		}
		return s.ok() ? 0 : -1;
	}

	Iterator* LevelDBEngine::Find(const Slice& findkey)
	{
		leveldb::ReadOptions options;
		options.fill_cache = false;
		leveldb::Iterator* iter = m_db->NewIterator(options);
		iter->Seek(LEVELDB_SLICE(findkey));
		return new LevelDBIterator(iter);
	}

	const std::string LevelDBEngine::Stats()
	{
		std::string str;
		m_db->GetProperty("leveldb.stats", &str);
		return str;
	}

}

