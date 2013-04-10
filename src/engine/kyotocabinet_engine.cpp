/*
 * kyotocabinet_engine.cpp
 *
 *  Created on: 2013-4-10
 *      Author: wqy
 */
#include "kyotocabinet_engine.hpp"
#include "ardb.hpp"
#include "ardb_data.hpp"
#include "util/helpers.hpp"
#include <string.h>

#define COMPARE_NUMBER(a, b)  (a == b?0:(a>b?1:-1))

namespace ardb
{
	int32_t KCDBComparator::compare(const char* akbuf, size_t aksiz,
			const char* bkbuf, size_t bksiz)
	{
		Slice a(akbuf, aksiz);
		Slice b(bkbuf, bksiz);
		KeyObject* ak = decode_key(a);
		KeyObject* bk = decode_key(b);
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
					ret = COMPARE_NUMBER(zak->score, zbk->score);
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
			ret = COMPARE_NUMBER(ak->type, bk->type);
		}
		DELETE(ak);
		DELETE(bk);
		return ret;
	}

	KCDBEngineFactory::KCDBEngineFactory(const std::string& basepath) :
			m_base_path(basepath)
	{

	}
	KeyValueEngine* KCDBEngineFactory::CreateDB(DBID db)
	{
		KCDBEngine* engine = new KCDBEngine();
		char tmp[m_base_path.size() + 16];
		sprintf(tmp, "%s/%lld", m_base_path.c_str(), db);
		if (engine->Init(tmp) != 0)
		{
			DELETE(engine);
			return NULL;
		}
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

	int KCDBEngine::Init(const std::string& path)
	{
		make_file(path);
		m_db = new kyotocabinet::TreeDB;
		m_db->tune_comparator(&m_comparator);
		if (!m_db->open(path.c_str(),
				kyotocabinet::TreeDB::OWRITER | kyotocabinet::TreeDB::OCREATE))
		{
			ERROR_LOG("Unable to open DB");
			DELETE(m_db);
			return -1;
		}
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

