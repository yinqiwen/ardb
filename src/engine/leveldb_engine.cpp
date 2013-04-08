/*
 * leveldb_engine.cpp
 *
 *  Created on: 2013-3-31
 *      Author: wqy
 */
#include "leveldb_engine.hpp"
#include "rddb.hpp"
#include "rddb_data.hpp"
#include "util/helpers.hpp"
#include <string.h>

#define LEVELDB_SLICE(slice) leveldb::Slice(slice.data(), slice.size())
#define RDDB_SLICE(slice) Slice(slice.data(), slice.size())
#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

namespace rddb
{
	int LevelDBComparator::Compare(const leveldb::Slice& a,
			const leveldb::Slice& b) const
	{

		KeyObject* ak = decode_key(RDDB_SLICE(a));
		KeyObject* bk = decode_key(RDDB_SLICE(b));
		if (NULL == ak && NULL == bk)
		{
			return 0;
		}
		if (NULL != ak && NULL == bk)
		{
			DELETE(ak);
			return 1;
		}
		if (NULL == ak && NULL != bk)
		{
			DELETE(bk);
			return -1;
		}
		int ret = 0;
		if (ak->type == bk->type)
		{
			ret = ak->key.compare(bk->key);
			if (ret != 0)
			{
				DELETE(ak);
				DELETE(bk);
				return ret;
			}
			switch (ak->type)
			{
				case HASH_FIELD:
				{
					HashKeyObject* hak = (HashKeyObject*) ak;
					HashKeyObject* hbk = (HashKeyObject*) bk;
					ret = hak->field.compare(hbk->field);
					break;
				}
				case LIST_ELEMENT:
				{
					ListKeyObject* lak = (ListKeyObject*) ak;
					ListKeyObject* lbk = (ListKeyObject*) bk;
					ret = COMPARE_NUMBER(lak->score, lbk->score);
					break;
				}
				case SET_ELEMENT:
				{
					SetKeyObject* sak = (SetKeyObject*) ak;
					SetKeyObject* sbk = (SetKeyObject*) bk;
					ret = sak->value.compare(sbk->value);
					break;
				}
				case ZSET_ELEMENT:
				{
					ZSetKeyObject* zak = (ZSetKeyObject*) ak;
					ZSetKeyObject* zbk = (ZSetKeyObject*) bk;
					int ret = COMPARE_NUMBER(zak->score, zbk->score);
					if (ret == 0)
					{
						ret = zak->value.compare(zbk->value);
					}
					break;
				}
				case ZSET_ELEMENT_SCORE:
				{
					ZSetScoreKeyObject* zak = (ZSetScoreKeyObject*) ak;
					ZSetScoreKeyObject* zbk = (ZSetScoreKeyObject*) bk;
					ret = zak->value.compare(zbk->value);
					break;
				}
				case SET_META:
				case ZSET_META:
				case LIST_META:
				default:
				{
					break;
				}
			}
		} else
		{
			ret = ak->type > bk->type ? 1 : -1;
		}
		DELETE(ak);
		DELETE(bk);
		return ret;
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
			printf("#####Failed to init engine:%s\n",
					status.ToString().c_str());
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
		} else
		{
			s = m_db->Put(leveldb::WriteOptions(), LEVELDB_SLICE(key),LEVELDB_SLICE(value));
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Get(const Slice& key, std::string* value)
	{
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), LEVELDB_SLICE(key), value);
		if(!s.ok())
		{
			printf("Failed to find %s\n", s.ToString().c_str());
		}
		return s.ok() ? 0 : -1;
	}
	int LevelDBEngine::Del(const Slice& key)
	{
		leveldb::Status s = leveldb::Status::OK();
		if (!m_batch_stack.empty())
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
		iter->Seek(LEVELDB_SLICE(findkey));
		return new LevelDBIterator(iter);
	}
}

