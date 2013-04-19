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
		LevelDBEngine* leveldb = (LevelDBEngine*)engine;
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
		leveldb::Options options;
		options.create_if_missing = true;
		options.comparator = &m_comparator;
		leveldb::Cache* cache = leveldb::NewLRUCache(268435456);
		options.block_cache = cache;
//		options.block_size = 268435456;
//		options.write_buffer_size = 268435456;
		options.filter_policy = leveldb::NewBloomFilterPolicy(16);
		make_dir(cfg.path);
		m_db_path = cfg.path;
		leveldb::Status status = leveldb::DB::Open(options, cfg.path.c_str(),
				&m_db);
		if (!status.ok())
		{
			DEBUG_LOG("Failed to init engine:%s\n", status.ToString().c_str());
		}
		return status.ok() ? 0 : -1;
	}

	int LevelDBEngine::BeginBatchWrite()
	{
		m_batch_stack.push(true);
		return 0;
	}
	int LevelDBEngine::CommitBatchWrite()
	{
		m_batch_stack.pop();
		if (m_batch_stack.empty())
		{
			leveldb::Status s = m_db->Write(leveldb::WriteOptions(), &m_batch);
			m_batch.Clear();
			return s.ok() ? 0 : -1;
		}
		return 0;
	}
	int LevelDBEngine::DiscardBatchWrite()
	{
		m_batch_stack.pop();
		m_batch.Clear();
		return 0;
	}

	int LevelDBEngine::Put(const Slice& key, const Slice& value)
	{
		leveldb::Status s = leveldb::Status::OK();
		if (!m_batch_stack.empty())
		{
			m_batch.Put(LEVELDB_SLICE(key), LEVELDB_SLICE(value));
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
		if (!s.ok())
		{
			//DEBUG_LOG("Failed to find %s", s.ToString().c_str());
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Del(const Slice& key)
	{
		leveldb::Status s = leveldb::Status::OK();
		if (!m_batch_stack.empty())
		{
			m_batch.Delete(LEVELDB_SLICE(key));
		}
		else
		{
			s = m_db->Delete(leveldb::WriteOptions(), LEVELDB_SLICE(key));
		}
		return s.ok() ? 0 : -1;
	}

	Iterator* LevelDBEngine::Find(const Slice& findkey)
	{
		uint64 start_time = get_current_epoch_millis();
		leveldb::ReadOptions options;
		leveldb::Iterator* iter = m_db->NewIterator(options);
		iter->Seek(LEVELDB_SLICE(findkey));
		uint64 stop_time = get_current_epoch_millis();
		//INFO_LOG("Cost %lldms to exec seek:%d", (stop_time-start_time),findkey.size());
		return new LevelDBIterator(iter);
	}

}

