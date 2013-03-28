/*
 * rrdb.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef RRDB_HPP_
#define RRDB_HPP_
#include <stdint.h>
#include <map>
#include "helpers.hpp"

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

	typedef uint64_t DBID;
	struct KeyValueEngine
	{
			virtual DBID CreateDB(int db) = 0;
			virtual int Put(const DBID& db, const void* key, int keysize,
					const void* value, int valuesize) = 0;
			virtual int Get(const DBID& db, const void* key, int keysize,
					std::string* value) = 0;
			virtual int Del(const DBID& db, const void* key, int keysize) = 0;
			virtual ~KeyValueEngine()
			{
			}
	};

	class RDDB
	{
		private:
			KeyValueEngine* m_engine;
			void EncodeKVKey(Buffer& buf, const void* key, int keysize);
			void EncodeKVValue(Buffer& buf, const void* value, int valuesize);
		public:
			RDDB(KeyValueEngine* engine);
			int Set(DBID db, const void* key, int keysize, const void* value,
					int valuesize);
			int Get(DBID db, const void* key, int keysize, std::string* value);
			int Del(DBID db, const void* key, int keysize);
			bool Exists(DBID db, const void* key, int keysize);
			int Expire(DBID db, const void* key, int keysize, uint32_t secs);

			int HSet(DBID db, const void* key, int keysize, const void* field,
					int fieldsize, const void* value, int valuesize);
			int ZAdd(DBID db, const void* key, int keysize, int64_t score,
					const void* value, int valuesize);
			int SAdd(DBID db, const void* key, int keysize, const void* value,
					int valuesize);
			int RPush(DBID db, const void* key, int keysize, const void* value,
					int valuesize);
	};

}

#endif /* RRDB_HPP_ */
