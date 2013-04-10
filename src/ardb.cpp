/*
 * ardb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "ardb.hpp"
#include <string.h>

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
}while(0)

namespace ardb
{
//	void Ardb::ClearValueArray(ValueArray& array)
//	{
//		ValueArray::iterator it = array.begin();
//		while (it != array.end())
//		{
//			DELETE(*it);
//			it++;
//		}
//		array.clear();
//	}

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

	Ardb::Ardb(KeyValueEngineFactory* engine) :
			m_engine_factory(engine)
	{
	}

	void Ardb::Walk(DBID db, KeyObject& key, bool reverse, WalkHandler* handler)
	{
		Iterator* iter = FindValue(db, key);
		bool isFirstElement = true;
		while (NULL != iter && iter->Valid())
		{
			Slice tmpkey = iter->Key();
			KeyObject* kk = decode_key(tmpkey);
			if (NULL == kk || kk->type != key.type
					|| kk->key.compare(key.key) != 0)
			{
				if (reverse && isFirstElement)
				{
					DELETE(kk);
					iter->Prev();
					isFirstElement = false;
					continue;
				}
				DELETE(kk);
				break;
			}
			ValueObject v;
			Buffer readbuf(const_cast<char*>(iter->Value().data()), 0,
					iter->Value().size());
			decode_value(readbuf, v);
			int ret = handler->OnKeyValue(kk, &v);
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

	KeyValueEngine* Ardb::GetDB(DBID db)
	{
		KeyValueEngineTable::iterator found = m_engine_table.find(db);
		if (found != m_engine_table.end())
		{
			return found->second;
		}
		KeyValueEngine* engine = m_engine_factory->CreateDB(db);
		if (NULL != engine)
		{
			m_engine_table[db] = engine;
		}
		return engine;
	}

	int Ardb::Type(DBID db, const Slice& key)
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
				}
			}
		}
		return type;
	}

	int Ardb::Sort(DBID db, const Slice& key, const StringArray& args,
			StringArray& values)
	{
		struct SortOptions
		{
				std::string by;
				bool with_limit;
				StringArray get_patterns;
				bool is_desc;
				bool with_alpha;
				std::string store_dst;
		};

		return 0;
	}

}

