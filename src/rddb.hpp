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
#include <list>
#include "helpers.hpp"
#include "buffer_helper.hpp"

namespace rddb
{
	enum KeyType
	{
		KV = 0,
		SET_ELEMENT = 1,
		ZSET_ELEMENT = 2,
		HASH_FIELD = 3,
		LIST_ELEMENT = 4
	};

	struct KeyObject
	{
			uint8_t type;
			const void* raw;
			int len;
	};

	enum ValueDataType
	{
		INTEGER = 0, DOUBLE = 1, RAW = 2
	};

	struct ValueObject
	{
			uint8_t type;
			union
			{
					int64_t int_v;
					double double_v;
					char* raw;
			} v;
			int len;
			uint64_t expire;
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
			void EncodeKey(Buffer& buf, KeyObject& key);
			void EncodeValue(Buffer& buf, ValueObject& value);
			int GetValueExpiration(std::string* value, uint64_t& expiration);
			int SetExpiration(DBID db, const void* key, int keysize,
					uint64_t expire);
			int SetValue(DBID db, KeyObject& key,
					ValueObject& value);
		public:
			RDDB(KeyValueEngine* engine);
			int Set(DBID db, const void* key, int keysize, const void* value,
					int valuesize);
			int Get(DBID db, const void* key, int keysize, std::string* value);
			int Del(DBID db, const void* key, int keysize);
			bool Exists(DBID db, const void* key, int keysize);
			int Expire(DBID db, const void* key, int keysize, uint32_t secs);
			int Expireat(DBID db, const void* key, int keysize, uint32_t ts);
			int Keys(DBID db, const std::string& pattern,
					std::list<std::string>& ret)
			{
				return -1;
			}
			int Move(DBID srcdb, const void* key, int keysize, DBID dstdb);

			int Append(DBID db, const void* key, int keysize, const char* value,
					int valuesize);
			int Decr(DBID db, const void* key, int keysize, int64_t& value);

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
