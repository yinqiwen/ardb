/*
 * rrdb.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef RRDB_HPP_
#define RRDB_HPP_
#include "leveldb/db.h"
#include <stdint.h>

namespace rddb
{
	enum ValueType
	{
		KV = 0,
		SET_ELEMENT = 1,
		ZSET_ELEMENT = 2,
		HASH_FIELD = 3,
		LIST_ELEMENT = 4
	};
	struct MetaData
	{
			uint8_t type;
	};

	struct KeyValueEngine
	{
			virtual int CreateDB(int db) = 0;
			virtual int Put(const void* key, int keysize, const void* value,
					int valuesize) = 0;
			virtual ~KeyValueEngine()
			{
			}
	};

	class RDDB
	{
		private:
			KeyValueEngine* m_engine;
		public:
			RDDB(KeyValueEngine* engine);
			int SelectDB(uint32_t idx);
	};
}

#endif /* RRDB_HPP_ */
