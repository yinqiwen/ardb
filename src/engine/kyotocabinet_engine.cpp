/*
 * kyotocabinet_engine.cpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */
#include "kyotocabinet_engine.hpp"
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "comparator.hpp"
#include "util/helpers.hpp"
#include <string.h>

namespace ardb
{
	int32_t KCDBComparator::compare(const char* akbuf, size_t aksiz,
			const char* bkbuf, size_t bksiz)
	{
		return ardb_compare_keys(akbuf, aksiz, bkbuf, bksiz);
	}

	KCDBEngineFactory::KCDBEngineFactory(const Properties& props)
	{
		ParseConfig(props, m_cfg);
	}

	void KCDBEngineFactory::ParseConfig(const Properties& props,
			KCDBConfig& cfg)
	{
		cfg.path = ".";
		conf_get_string(props, "dir", cfg.path);
	}
	KeyValueEngine* KCDBEngineFactory::CreateDB(const DBID& db)
	{
		KCDBEngine* engine = new KCDBEngine();
		char tmp[m_cfg.path.size() + db.size() + 10];
		sprintf(tmp, "%s/%s", m_cfg.path.c_str(), db.c_str());
		KCDBConfig cfg = m_cfg;
		cfg.path = tmp;
		if (engine->Init(cfg) != 0)
		{
			DELETE(engine);
			return NULL;
		}
		DEBUG_LOG("Create DB:%s at path:%s success", db.c_str(), tmp);
		return engine;
	}
	void KCDBEngineFactory::DestroyDB(KeyValueEngine* engine)
	{
		DELETE(engine);
	}

	KCDBEngine::KCDBEngine() :
			m_db(NULL)
	{

	}

	int KCDBEngine::Init(const KCDBConfig& cfg)
	{
		make_file(cfg.path);
		m_db = new kyotocabinet::TreeDB;
		m_db->tune_comparator(&m_comparator);
		if (!m_db->open(cfg.path.c_str(),
				kyotocabinet::TreeDB::OWRITER | kyotocabinet::TreeDB::OCREATE))
		{
			ERROR_LOG("Unable to open DB:%s", cfg.path.c_str());
			DELETE(m_db);
			return -1;
		}
		ERROR_LOG("Open DB:%s success", cfg.path.c_str());
		return 0;
	}

	int KCDBEngine::BeginBatchWrite()
	{
		m_batch_stack.push(true);
		return 0;
	}
	int KCDBEngine::CommitBatchWrite()
	{
		m_batch_stack.pop();
		if (m_batch_stack.empty())
		{
			if (!m_bulk_del_keys.empty())
			{
				std::vector<std::string> ks;
				std::set<std::string>::iterator it = m_bulk_del_keys.begin();
				while (it != m_bulk_del_keys.end())
				{
					ks.push_back(*it);
					it++;
				}
				m_db->remove_bulk(ks);
				m_bulk_del_keys.clear();
			}
			if (!m_bulk_set_kvs.empty())
			{
				m_db->set_bulk(m_bulk_set_kvs);
				m_bulk_set_kvs.clear();
			}
			return 0;
		}
		return 0;
	}
	int KCDBEngine::DiscardBatchWrite()
	{
		m_batch_stack.pop();
		m_bulk_set_kvs.clear();
		m_bulk_del_keys.clear();
		return 0;
	}

	int KCDBEngine::Put(const Slice& key, const Slice& value)
	{
		DEBUG_LOG("Put data:");
		if (!m_batch_stack.empty())
		{
			std::string kstr(key.data(), key.size());
			std::string vstr(value.data(), value.size());
			m_bulk_set_kvs[kstr] = vstr;
			m_bulk_del_keys.erase(kstr);
			return 0;
		}
		bool success = false;
		success = m_db->set(key.data(), key.size(), value.data(), value.size());
		return success ? 0 : -1;
	}
	int KCDBEngine::Get(const Slice& key, std::string* value)
	{
		size_t len;
		bool success = false;
		char* data = m_db->get(key.data(), key.size(), &len);
		success = data != NULL;
		if (success && NULL != value)
		{
			value->assign(data, len);
		}
		DELETE_A(data);
		return success ? 0 : -1;
	}
	int KCDBEngine::Del(const Slice& key)
	{
		if (!m_batch_stack.empty())
		{
			std::string delkey(key.data(), key.size());
			m_bulk_del_keys.insert(delkey);
			m_bulk_set_kvs.erase(delkey);
			return 0;
		}
		return m_db->remove(key.data(), key.size()) ? 0 : -1;
	}

	Iterator* KCDBEngine::Find(const Slice& findkey)
	{
		kyotocabinet::DB::Cursor* cursor = m_db->cursor();
		bool ret = cursor->jump(findkey.data(), findkey.size());
		if (!ret)
		{
			ret = cursor->jump_back();
		}
		return new KCDBIterator(cursor, ret);
	}

	void KCDBIterator::Next()
	{
		m_valid = m_cursor->step();
	}
	void KCDBIterator::Prev()
	{
		m_valid = m_cursor->step_back();
	}
	Slice KCDBIterator::Key() const
	{
		Buffer& tmp = const_cast<Buffer&>(m_key_buffer);
		tmp.Clear();
		size_t len;
		char* key = m_cursor->get_key(&len, false);
		if (NULL != key)
		{
			tmp.Write(key, len);
		}
		DELETE_A(key);
		return Slice(m_key_buffer.GetRawReadBuffer(), len);
	}
	Slice KCDBIterator::Value() const
	{
		Buffer& tmp = const_cast<Buffer&>(m_value_buffer);
		tmp.Clear();
		size_t len;
		char* value = m_cursor->get_value(&len, false);
		if (NULL != value)
		{
			tmp.Write(value, len);
		}
		DELETE_A(value);
		return Slice(m_value_buffer.GetRawReadBuffer(), len);
	}
	bool KCDBIterator::Valid()
	{
		return m_valid;
	}
}

