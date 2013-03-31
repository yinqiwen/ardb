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
#include "util/helpers.hpp"
#include "util/buffer_helper.hpp"
#include "util/sds.h"

#define RDDB_OK 0
#define ERR_OUTOFRANGE -9
#define ERR_NOT_EXIST -10

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
			KeyObject(KeyType t, const void* v, size_t vl) :
					type(t), raw(v), len(vl)
			{
			}
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
					Buffer* raw;
			} v;
			uint64_t expire;
			ValueObject() :
					type(RAW), expire(0)
			{
				v.int_v = 0;
			}
			~ValueObject()
			{
				if (type == RAW && v.raw != NULL)
				{
					delete v.raw;
				}
			}
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
			static void EncodeKey(Buffer& buf, const KeyObject& key);
			static void EncodeValue(Buffer& buf, const ValueObject& value);
			static bool DecodeValue(Buffer& buf, ValueObject& value);
			static void FillValueObject(const void* value, int valuesize,
			        ValueObject& valueobj);
			static Buffer* ValueObject2RawBuffer(ValueObject& valueobj);
			static size_t RealPosition(Buffer* buf, int pos);

			int SetExpiration(DBID db, const void* key, int keysize,
			        uint64_t expire);
			int GetValue(DBID db, const KeyObject& key, ValueObject& v);
			int SetValue(DBID db, KeyObject& key, ValueObject& value);
		public:
			RDDB(KeyValueEngine* engine);
			int Set(DBID db, const void* key, int keysize, const void* value,
			        int valuesize);
			int SetNX(DBID db, const void* key, int keysize, const void* value,
						        int valuesize);
			int SetEx(DBID db, const void* key, int keysize, const void* value,
			        int valuesize, uint32_t secs);
			int PSetEx(DBID db, const void* key, int keysize, const void* value,
						        int valuesize, uint32_t ms);
			int Get(DBID db, const void* key, int keysize, ValueObject& value);
			int Del(DBID db, const void* key, int keysize);
			bool Exists(DBID db, const void* key, int keysize);
			int Expire(DBID db, const void* key, int keysize, uint32_t secs);
			int Expireat(DBID db, const void* key, int keysize, uint32_t ts);
			int Persist(DBID db, const void* key, int keysize);
			int Pexpire(DBID db, const void* key, int keysize, uint32_t ms);
			int Strlen(DBID db, const void* key, int keysize);
			int Keys(DBID db, const std::string& pattern,
			        std::list<std::string>& ret)
			{
				return -1;
			}
			int Move(DBID srcdb, const void* key, int keysize, DBID dstdb);

			int Append(DBID db, const void* key, int keysize, const char* value,
			        int valuesize);
			int Decr(DBID db, const void* key, int keysize, int64_t& value);
			int Decrby(DBID db, const void* key, int keysize, int64_t decrement,
			        int64_t& value);
			int Incr(DBID db, const void* key, int keysize, int64_t& value);
			int Incrby(DBID db, const void* key, int keysize, int64_t increment,
			        int64_t& value);
			int IncrbyFloat(DBID db, const void* key, int keysize,
			        double increment, double& value);
			int GetRange(DBID db, const void* key, int keysize, int start,
			        int end, ValueObject& valueobj);
			int SetRange(DBID db, const void* key, int keysize, int start,
			        const void* value, int valuesize);
			int GetSet(DBID db, const void* key, int keysize, const char* value,
			        int valuesize, ValueObject& valueobj);


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
