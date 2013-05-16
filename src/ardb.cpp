/*
 * ardb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string.h>
#include <sstream>
#include "comparator.hpp"

#define  GET_KEY_TYPE(DB, KEY, TYPE)   do{ \
		Iterator* iter = FindValue(DB, KEY);  \
		if (NULL != iter && iter->Valid()) \
		{                                  \
			Slice tmp = iter->Key();       \
			KeyObject* k = decode_key(tmp); \
			if(NULL != k && k->key.compare(KEY.key) == 0)\
			{                                            \
				TYPE = k->type;                          \
	         }                                           \
	    }\
	    DELETE(iter);\
}while(0)

namespace ardb
{
	int ardb_compare_keys(const char* akbuf, size_t aksiz, const char* bkbuf,
			size_t bksiz)
	{
		Buffer ak_buf(const_cast<char*>(akbuf), 0, aksiz);
		Buffer bk_buf(const_cast<char*>(bkbuf), 0, bksiz);
		uint8_t at, bt;
		bool found_a = BufferHelper::ReadFixUInt8(ak_buf, at);
		bool found_b = BufferHelper::ReadFixUInt8(bk_buf, bt);
		COMPARE_EXIST(found_a, found_b);
		RETURN_NONEQ_RESULT(at, bt);
		Slice akey, bkey;
		found_a = BufferHelper::ReadVarSlice(ak_buf, akey);
		found_b = BufferHelper::ReadVarSlice(bk_buf, bkey);
		COMPARE_EXIST(found_a, found_b);
		int ret = akey.compare(bkey);
		if (ret != 0)
		{
			return ret;
		}

		switch (at)
		{
			case HASH_FIELD:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				break;
			}
			case LIST_ELEMENT:
			{
				float ascore, bscore;
				found_a = BufferHelper::ReadFixFloat(ak_buf, ascore);
				found_b = BufferHelper::ReadFixFloat(bk_buf, bscore);
				COMPARE_EXIST(found_a, found_b);
				ret = COMPARE_NUMBER(ascore, bscore);
				break;
			}
			case ZSET_ELEMENT_SCORE:
			case SET_ELEMENT:
			{
				ValueObject av, bv;
				found_a = decode_value(ak_buf, av, false);
				found_b = decode_value(bk_buf, bv, false);
				COMPARE_EXIST(found_a, found_b);
				//DEBUG_LOG("#####A=%s b=%s", av.ToString().c_str(), bv.ToString().c_str());
				ret = av.Compare(bv);
				break;
			}
			case ZSET_ELEMENT:
			{
				double ascore, bscore;
				found_a = BufferHelper::ReadFixDouble(ak_buf, ascore);
				found_b = BufferHelper::ReadFixDouble(bk_buf, bscore);
				ret = COMPARE_NUMBER(ascore, bscore);
				if (ret == 0)
				{
					ValueObject av, bv;
					found_a = decode_value(ak_buf, av);
					found_b = decode_value(bk_buf, bv);
					COMPARE_EXIST(found_a, found_b);
					ret = av.Compare(bv);
				}
				break;
			}
			case TABLE_INDEX:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				if (ret == 0)
				{
					ValueObject kav, kbv;
					found_a = decode_value(ak_buf, kav);
					found_b = decode_value(bk_buf, kbv);
					COMPARE_EXIST(found_a, found_b);
					ret = kav.Compare(kbv);
					if (ret != 0)
					{
						break;
					}
					uint32 alen, blen;
					found_a = BufferHelper::ReadVarUInt32(ak_buf, alen);
					found_b = BufferHelper::ReadVarUInt32(bk_buf, blen);
					COMPARE_EXIST(found_a, found_b);
					ret = COMPARE_NUMBER(alen, blen);
					if (ret == 0)
					{
						for (uint32 i = 0; i < alen; i++)
						{
							ValueObject av, bv;
							found_a = decode_value(ak_buf, av);
							found_b = decode_value(bk_buf, bv);
							COMPARE_EXIST(found_a, found_b);
							ret = av.Compare(bv);
							if (ret != 0)
							{
								break;
							}
						}
					}
				}
				break;
			}
			case TABLE_COL:
			{
				Slice af, bf;
				found_a = BufferHelper::ReadVarSlice(ak_buf, af);
				found_b = BufferHelper::ReadVarSlice(bk_buf, bf);
				COMPARE_EXIST(found_a, found_b);
				ret = af.compare(bf);
				if (ret == 0)
				{
					uint32 alen, blen;
					found_a = BufferHelper::ReadVarUInt32(ak_buf, alen);
					found_b = BufferHelper::ReadVarUInt32(bk_buf, blen);
					COMPARE_EXIST(found_a, found_b);
					ret = COMPARE_NUMBER(alen, blen);
					if (ret == 0)
					{
						for (uint32 i = 0; i < alen; i++)
						{
							ValueObject av, bv;
							found_a = decode_value(ak_buf, av);
							found_b = decode_value(bk_buf, bv);
							COMPARE_EXIST(found_a, found_b);
							ret = av.Compare(bv);
							if (ret != 0)
							{
								break;
							}
						}
					}
				}
				break;
			}
			case SET_META:
			case ZSET_META:
			case LIST_META:
			case TABLE_META:
			default:
			{
				break;
			}
		}
		return ret;
	}

	size_t Ardb::RealPosition(Buffer* buf, int pos)
	{
		if (pos < 0)
		{
			pos = buf->ReadableBytes() + pos;
		}
		if (pos >= buf->ReadableBytes())
		{
			pos = buf->ReadableBytes() - 1;
		}
		if (pos < 0)
		{
			pos = 0;
		}
		return pos;
	}

	Ardb::Ardb(KeyValueEngineFactory* engine, const std::string& path, bool multi_thread) :
			m_engine_factory(engine), m_key_watcher(NULL), m_raw_key_listener(
					NULL), m_path(path)
	{
		LoadAllDBNames();
		m_key_locker.enable = multi_thread;
	}

	Ardb::~Ardb()
	{
		KeyValueEngineTable::iterator it = m_engine_table.begin();
		while (it != m_engine_table.end())
		{
			m_engine_factory->CloseDB(it->second);
			it++;
		}
	}

	void Ardb::LoadAllDBNames()
	{
		std::string file = m_path + "/.db_names";
		Buffer content;
		file_read_full(file, content);
		std::string str = content.AsString();
		std::vector<std::string> ss = split_string(str, "\n");
		for (uint32 i = 0; i < ss.size(); i++)
		{
			if (!ss[i].empty())
			{
				m_all_dbs.insert(ss[i]);
			}
		}
	}
	void Ardb::StoreDBNames()
	{
		std::stringstream ss(std::stringstream::in | std::stringstream::out);
		DBIDSet::iterator it = m_all_dbs.begin();
		while (it != m_all_dbs.end())
		{
			ss << *it << "\n";
			it++;
		}
		std::string file = m_path + "/.db_names";
		std::string content = ss.str();
		file_write_content(file, content);
	}

	void Ardb::Walk(const DBID& db, KeyObject& key, bool reverse,
			WalkHandler* handler)
	{
		bool isFirstElement = true;
		Iterator* iter = FindValue(db, key);
		if (NULL != iter && !iter->Valid() && reverse)
		{
			iter->SeekToLast();
			isFirstElement = false;
		}
		uint32 cursor = 0;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			//fast check key type
			if ((!reverse || !isFirstElement) && tmpkey.data()[0] != key.type)
			{
				break;
			}
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != key.type
					|| kk->key.compare(key.key) != 0)
			{
				DELETE(kk);
				if (reverse && isFirstElement)
				{
					iter->Prev();
					isFirstElement = false;
					continue;
				}
				break;
			}
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
					iter->Value().size());
			decode_value(readbuf, v, false);
			int ret = handler->OnKeyValue(kk, &v, cursor++);
			DELETE(kk);
			if (ret < 0)
			{
				break;
			}
			if (reverse)
			{
				iter->Prev();
			} else
			{
				iter->Next();
			}
		}
		DELETE(iter);
	}

	KeyValueEngine* Ardb::GetDB(const DBID& db)
	{
		LockGuard<ThreadMutex> guard(m_mutex);
		KeyValueEngineTable::iterator found = m_engine_table.find(db);
		if (found != m_engine_table.end())
		{
			return found->second;
		}

		KeyValueEngine* engine = m_engine_factory->CreateDB(db);
		if (NULL != engine)
		{
			engine->id = db;
			m_engine_table[db] = engine;
			m_all_dbs.insert(db);
			StoreDBNames();
		}
		return engine;
	}

	int Ardb::RawSet(const DBID& db, const Slice& key, const Slice& value)
	{
		int ret = GetDB(db)->Put(key, value);
		if (ret == 0 && NULL != m_raw_key_listener)
		{
			m_raw_key_listener->OnKeyUpdated(db, key, value);
		}
		return ret;
	}
	int Ardb::RawDel(const DBID& db, const Slice& key)
	{
		int ret = GetDB(db)->Del(key);
		if (ret == 0 && NULL != m_raw_key_listener)
		{
			m_raw_key_listener->OnKeyDeleted(db, key);
		}
		return ret;
	}

	int Ardb::RawGet(const DBID& db, const Slice& key, std::string* value)
	{
		return GetDB(db)->Get(key, value);
	}

	int Ardb::Type(const DBID& db, const Slice& key)
	{
		if (Exists(db, key))
		{
			return KV;
		}

		int type = -1;
		Slice empty;
		SetKeyObject sk(key, empty);
		GET_KEY_TYPE(db, sk, type);
		if (type < 0)
		{
			ZSetScoreKeyObject zk(key, empty);
			GET_KEY_TYPE(db, zk, type);
			if (type < 0)
			{
				HashKeyObject hk(key, empty);
				GET_KEY_TYPE(db, hk, type);
				if (type < 0)
				{
					KeyObject lk(key, LIST_META);
					GET_KEY_TYPE(db, lk, type);
					if (type < 0)
					{
						KeyObject tk(key, TABLE_META);
						GET_KEY_TYPE(db, tk, type);
					}
				}
			}
		}
		return type;
	}

	void Ardb::VisitDB(const DBID& db, RawValueVisitor* visitor)
	{
		Slice empty;
		Iterator* iter = GetDB(db)->Find(empty);
		while (NULL != iter && iter->Valid())
		{
			visitor->OnRawKeyValue(db, iter->Key(), iter->Value());
			iter->Next();
		}
		DELETE(iter);
	}
	void Ardb::VisitAllDB(RawValueVisitor* visitor)
	{
		DBIDSet sets;
		ListAllDB(sets);
		DBIDSet::iterator it = sets.begin();
		while (it != sets.end())
		{
			VisitDB(*it, visitor);
			it++;
		}
	}

	void Ardb::PrintDB(const DBID& db)
	{
		Slice empty;
		KeyObject start(empty);
		Iterator* iter = FindValue(db, start);
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			Slice tmpval = iter->Value();
			KeyObject* kk = decode_key(tmpkey);
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
					iter->Value().size());
			decode_value(readbuf, v, false);
			if (NULL != kk)
			{
				std::string str;
				DEBUG_LOG(
						"[%d]Key=%s, Value=%s", kk->type, kk->key.data(), v.ToString(str).c_str());
			}
			DELETE(kk);
			iter->Next();
		}
		DELETE(iter);
	}

	void Ardb::ListAllDB(DBIDSet& alldb)
	{
		alldb = m_all_dbs;
	}

	int Ardb::FlushDB(const DBID& db)
	{
		m_engine_factory->DestroyDB(GetDB(db));
		m_engine_table.erase(db);
		if (NULL != m_key_watcher)
		{
			m_key_watcher->OnAllKeyDeleted(db);
		}
		return 0;
	}

	int Ardb::FlushAll()
	{
		DBIDSet sets;
		ListAllDB(sets);
		DBIDSet::iterator it = sets.begin();
		while (it != sets.end())
		{
			if (NULL != m_key_watcher)
			{
				m_key_watcher->OnAllKeyDeleted(*it);
			}
			FlushDB(*it);
			it++;
		}
		m_engine_table.clear();
		return 0;
	}

	int Ardb::CloseAll()
	{
		KeyValueEngineTable::iterator it = m_engine_table.begin();
		while (it != m_engine_table.end())
		{
			m_engine_factory->CloseDB(it->second);
			it++;
		}
		m_engine_table.clear();
		return 0;
	}
}

