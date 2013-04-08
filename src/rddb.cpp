/*
 * rddb.cpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */
#include "rddb.hpp"
#include <string.h>

namespace rddb
{
	void RDDB::ClearValueArray(ValueArray& array)
	{
		ValueArray::iterator it = array.begin();
		while (it != array.end())
		{
			DELETE(*it);
			it++;
		}
		array.clear();
	}

	size_t RDDB::RealPosition(Buffer* buf, int pos)
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

	RDDB::RDDB(KeyValueEngineFactory* engine) :
			m_engine_factory(engine)
	{
	}

	void RDDB::Walk(DBID db, KeyObject& key, bool reverse, WalkHandler* handler)
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
				if(reverse && isFirstElement)
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

	KeyValueEngine* RDDB::GetDB(DBID db)
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

}

