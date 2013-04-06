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
	static const char* kLogLevelNames[] = { "INFO", "WARN", "ERROR", "FATAL" };
	static void StdLogHandler(LogLevel level, const char* filename, int line,
	        const std::string& message)
	{
		printf("[%s][%s:%d]%s\n", kLogLevelNames[level], filename, line,
		        message.c_str());
	}

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
			m_logger(StdLogHandler), m_engine_factory(engine)
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

