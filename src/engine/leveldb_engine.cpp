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

	LevelDBEngineFactory::LevelDBEngineFactory(const std::string& basepath) :
			m_base_path(basepath)
	{

	}
	KeyValueEngine* LevelDBEngineFactory::CreateDB(DBID db)
	{
		LevelDBEngine* engine = new LevelDBEngine();
		char tmp[m_base_path.size() + 16];
		sprintf(tmp, "%s/%lld", m_base_path.c_str(), db);
		if (engine->Init(tmp) != 0)
		{
			DELETE(engine);
			return NULL;
		}
		return engine;
	}
	void LevelDBEngineFactory::DestroyDB(KeyValueEngine* engine)
	{
		DELETE(engine);
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

	int LevelDBEngine::Init(const std::string& path)
	{
		leveldb::Options options;
		options.create_if_missing = true;
		options.comparator = &m_comparator;
		make_dir(path);
		leveldb::Status status = leveldb::DB::Open(options, path.c_str(),
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
		leveldb::ReadOptions options;
		leveldb::Iterator* iter = m_db->NewIterator(options);
		iter->Seek(LEVELDB_SLICE(findkey));
		if (!iter->Valid())
		{
			iter->SeekToLast();
		}
		return new LevelDBIterator(iter);
	}
}

