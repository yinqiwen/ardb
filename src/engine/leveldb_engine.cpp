/*
 * leveldb_engine.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "leveldb_engine.hpp"
#include "rddb.hpp"

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define RDDB_SLICE(slice) Slice(slice.data(), slice.size())

namespace rddb
{
	int LevelDBComparator::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
	{

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
		return NULL;
	}
	void LevelDBEngineFactory::DestroyDB(KeyValueEngine* engine)
	{

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
		return RDDB_SLICE(m_iter->key());
	}
	Slice LevelDBIterator::Value() const
	{
		return RDDB_SLICE(m_iter->value());
	}
	bool LevelDBIterator::Valid()
	{
		return m_iter->Valid();
	}

	LevelDBEngine::LevelDBEngine() :
			m_db(NULL), m_batch_mode(false)
	{

	}

	int LevelDBEngine::BeginBatchWrite()
	{
		m_batch.Clear();
		m_batch_mode = true;
		return 0;
	}
	int LevelDBEngine::CommitBatchWrite()
	{
		leveldb::Status s = m_db->Write(leveldb::WriteOptions(), &m_batch);
		m_batch.Clear();
		m_batch_mode = false;
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::DiscardBatchWrite()
	{
		m_batch.Clear();
		m_batch_mode = false;
		return 0;
	}

	int LevelDBEngine::Put(const Slice& key, const Slice& value)
	{
		leveldb::Status s = leveldb::Status::OK();
		if (m_batch_mode)
		{
			m_batch.Put(LEVELDB_SLICE(key), LEVELDB_SLICE(value));
		} else
		{
			s = m_db->Put(leveldb::WriteOptions(), LEVELDB_SLICE(key),LEVELDB_SLICE(value));
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Get(const Slice& key, std::string* value)
	{
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), LEVELDB_SLICE(key), value);
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Del(const Slice& key)
	{
		leveldb::Status s = leveldb::Status::OK();
		if (m_batch_mode)
		{
			m_batch.Delete(LEVELDB_SLICE(key));
		} else
		{
			s = m_db->Delete(leveldb::WriteOptions(), LEVELDB_SLICE(key));
		}
		return s.ok() ? 0 : -1;
	}

	Iterator* LevelDBEngine::Find(const Slice& findkey)
	{
		leveldb::ReadOptions options;
		leveldb::Iterator* iter = m_db->NewIterator(options);
		leveldb::Slice k(findkey.data(), findkey.size());
		iter->Seek(k);
		return new LevelDBIterator(iter);
	}
}

