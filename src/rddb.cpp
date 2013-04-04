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

	Buffer* RDDB::ValueObject2RawBuffer(ValueObject& v)
	{
		if (v.type != RAW)
		{
			int64_t iv = v.v.int_v;
			double dv = v.v.double_v;
			v.type = RAW;
			v.v.raw = new Buffer(16);
			if (v.type == INTEGER)
			{
				v.v.raw->Printf("%lld", iv);
			} else if (v.type == DOUBLE)
			{
				v.v.raw->Printf("%f", dv);
			}
		}
		return v.v.raw;
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

