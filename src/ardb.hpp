/*
 * rrdb.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef ARDB_HPP_
#define ARDB_HPP_
#include <stdint.h>
#include <float.h>
#include <map>
#include <vector>
#include <list>
#include <string>
#include <deque>
#include "common.hpp"
#include "ardb_data.hpp"
#include "slice.hpp"
#include "util/helpers.hpp"
#include "util/buffer_helper.hpp"

#define ARDB_OK 0
#define ERR_INVALID_ARGS -3
#define ERR_INVALID_OPERATION -4
#define ERR_INVALID_STR -5
#define ERR_DB_NOT_EXIST -6
#define ERR_KEY_EXIST -7
#define ERR_INVALID_TYPE -8
#define ERR_OUTOFRANGE -9
#define ERR_NOT_EXIST -10

namespace ardb
{

	struct Iterator
	{
			virtual void Next() = 0;
			virtual void Prev() = 0;
			virtual Slice Key() const = 0;
			virtual Slice Value() const = 0;
			virtual bool Valid() = 0;
			virtual ~Iterator()
			{
			}
	};

	struct KeyValueEngine
	{
			virtual int Get(const Slice& key, std::string* value) = 0;
			virtual int Put(const Slice& key, const Slice& value) = 0;
			virtual int Del(const Slice& key) = 0;
			virtual int BeginBatchWrite() = 0;
			virtual int CommitBatchWrite() = 0;
			virtual int DiscardBatchWrite() = 0;
			virtual Iterator* Find(const Slice& findkey) = 0;
			virtual ~KeyValueEngine()
			{
			}
	};

	class BatchWriteGuard
	{
		private:
			KeyValueEngine* m_engine;
			bool m_success;
		public:
			BatchWriteGuard(KeyValueEngine* engine) :
					m_engine(engine), m_success(true)
			{
				if (NULL != m_engine)
				{
					m_engine->BeginBatchWrite();
				}
			}
			void MarkFailed()
			{
				m_success = false;
			}
			~BatchWriteGuard()
			{
				if (NULL != m_engine)
				{
					if (m_success)
					{
						m_engine->CommitBatchWrite();
					} else
					{
						m_engine->DiscardBatchWrite();
					}
				}
			}
	};

	struct KeyValueEngineFactory
	{
			virtual KeyValueEngine* CreateDB(DBID db) = 0;
			virtual void DestroyDB(KeyValueEngine* engine) = 0;
			virtual ~KeyValueEngineFactory()
			{
			}
	};

	enum AggregateType
	{
		AGGREGATE_SUM = 0, AGGREGATE_MIN = 1, AGGREGATE_MAX = 2,
	};

	struct ArdbQueryOptions
	{
			bool withscores;
			bool withlimit;
			int limit_offset;
			int limit_count;
			ArdbQueryOptions() :
					withscores(false), withlimit(false), limit_offset(0), limit_count(
							0)
			{
			}
	};
	class Ardb
	{
		private:
			static size_t RealPosition(Buffer* buf, int pos);

			KeyValueEngineFactory* m_engine_factory;
			typedef std::map<DBID, KeyValueEngine*> KeyValueEngineTable;
			KeyValueEngineTable m_engine_table;
			KeyValueEngine* GetDB(DBID db);

			int SetExpiration(DBID db, const Slice& key, uint64_t expire);
			int GetValue(DBID db, const KeyObject& key, ValueObject* v,
					uint64* expire = NULL);
			int SetValue(DBID db, KeyObject& key, ValueObject& value,
					uint64 expire = 0);
			int DelValue(DBID db, KeyObject& key);
			Iterator* FindValue(DBID db, KeyObject& key);
			int SetHashValue(DBID db, const Slice& key, const Slice& field,
					ValueObject& value);
			int ListPush(DBID db, const Slice& key, const Slice& value,
					bool athead, bool onlyexist, double withscore = DBL_MAX);
			int ListPop(DBID db, const Slice& key, bool athead,
					std::string& value);
			int GetZSetMetaValue(DBID db, const Slice& key,
					ZSetMetaValue& meta);
			void SetZSetMetaValue(DBID db, const Slice& key,
					ZSetMetaValue& meta);
			int GetSetMetaValue(DBID db, const Slice& key, SetMetaValue& meta);
			void SetSetMetaValue(DBID db, const Slice& key, SetMetaValue& meta);
			int HGetValue(DBID db, const Slice& key, const Slice& field,
					ValueObject* value);
			struct WalkHandler
			{
					virtual int OnKeyValue(KeyObject* key,
							ValueObject* value) = 0;
					virtual ~WalkHandler()
					{
					}
			};
			void Walk(DBID db, KeyObject& key, bool reverse,
					WalkHandler* handler);
			std::string m_err_cause;
			void SetErrorCause(const std::string& cause)
			{
				m_err_cause = cause;
			}
		public:
			Ardb(KeyValueEngineFactory* factory);
			/*
			 * Key-Value operations
			 */
			int Set(DBID db, const Slice& key, const Slice& value);
			int Set(DBID db, const Slice& key, const Slice& value, int ex,
					int px, int nxx);
			int MSet(DBID db, SliceArray& keys, SliceArray& values);
			int MSetNX(DBID db, SliceArray& keys, SliceArray& value);
			int SetNX(DBID db, const Slice& key, const Slice& value);
			int SetEx(DBID db, const Slice& key, const Slice& value,
					uint32_t secs);
			int PSetEx(DBID db, const Slice& key, const Slice& value,
					uint32_t ms);
			int Get(DBID db, const Slice& key, std::string* value);
			int MGet(DBID db, SliceArray& keys, StringArray& value);
			int Del(DBID db, const Slice& key);
			bool Exists(DBID db, const Slice& key);
			int Expire(DBID db, const Slice& key, uint32_t secs);
			int Expireat(DBID db, const Slice& key, uint32_t ts);
			int TTL(DBID db, const Slice& key);
			int PTTL(DBID db, const Slice& key);
			int Persist(DBID db, const Slice& key);
			int Pexpire(DBID db, const Slice& key, uint32_t ms);
			int Strlen(DBID db, const Slice& key);
			int Rename(DBID db, const Slice& key1, const Slice& key2);
			int RenameNX(DBID db, const Slice& key1, const Slice& key2);
			int Keys(DBID db, const std::string& pattern,
					std::list<std::string>& ret)
			{
				return -1;
			}
			int Move(DBID srcdb, const Slice& key, DBID dstdb);

			int Append(DBID db, const Slice& key, const Slice& value);
			int Decr(DBID db, const Slice& key, int64_t& value);
			int Decrby(DBID db, const Slice& key, int64_t decrement,
					int64_t& value);
			int Incr(DBID db, const Slice& key, int64_t& value);
			int XIncrby(DBID db, const Slice& key, int64_t increment);
			int Incrby(DBID db, const Slice& key, int64_t increment,
					int64_t& value);
			int IncrbyFloat(DBID db, const Slice& key, double increment,
					double& value);
			int GetRange(DBID db, const Slice& key, int start, int end,
					std::string& valueobj);
			int SetRange(DBID db, const Slice& key, int start,
					const Slice& value);
			int GetSet(DBID db, const Slice& key, const Slice& value,
					std::string& valueobj);
			int BitCount(DBID db, const Slice& key, int start, int end);
			int GetBit(DBID db, const Slice& key, int offset);
			int SetBit(DBID db, const Slice& key, uint32_t offset,
					uint8_t value);
			int BitOP(DBID db, const Slice& op, const Slice& dstkey,
					SliceArray& keys);

			/*
			 * Hash operations
			 */
			int HSet(DBID db, const Slice& key, const Slice& field,
					const Slice& value);
			int HSetNX(DBID db, const Slice& key, const Slice& field,
					const Slice& value);
			int HDel(DBID db, const Slice& key, const Slice& field);
			bool HExists(DBID db, const Slice& key, const Slice& field);
			int HGet(DBID db, const Slice& key, const Slice& field,
					std::string* value);
			int HIncrby(DBID db, const Slice& key, const Slice& field,
					int64_t increment, int64_t& value);
			int HIncrbyFloat(DBID db, const Slice& key, const Slice& field,
					double increment, double& value);
			int HMGet(DBID db, const Slice& key, const SliceArray& fields,
					StringArray& values);
			int HMSet(DBID db, const Slice& key, const SliceArray& fields,
					const SliceArray& values);
			int HGetAll(DBID db, const Slice& key, StringArray& fields,
					StringArray& values);
			int HKeys(DBID db, const Slice& key, StringArray& fields);
			int HVals(DBID db, const Slice& key, StringArray& values);
			int HLen(DBID db, const Slice& key);
			int HClear(DBID db, const Slice& key);

			/*
			 * List operations
			 */
			int LPush(DBID db, const Slice& key, const Slice& value);
			int LPushx(DBID db, const Slice& key, const Slice& value);
			int RPush(DBID db, const Slice& key, const Slice& value);
			int RPushx(DBID db, const Slice& key, const Slice& value);
			int LPop(DBID db, const Slice& key, std::string& v);
			int RPop(DBID db, const Slice& key, std::string& v);
			int LIndex(DBID db, const Slice& key, uint32_t index,
					std::string& v);
			int LInsert(DBID db, const Slice& key, const Slice& op,
					const Slice& pivot, const Slice& value);
			int LRange(DBID db, const Slice& key, int start, int end,
					StringArray& values);
			int LRem(DBID db, const Slice& key, int count, const Slice& value);
			int LSet(DBID db, const Slice& key, int index, const Slice& value);
			int LTrim(DBID db, const Slice& key, int start, int stop);
			int RPopLPush(DBID db, const Slice& key1, const Slice& key2);
			int LClear(DBID db, const Slice& key);
			int LLen(DBID db, const Slice& key);

			/*
			 * Sorted Set operations
			 */
			int ZAdd(DBID db, const Slice& key, double score,
					const Slice& value);
			int ZCard(DBID db, const Slice& key);
			int ZScore(DBID db, const Slice& key, const Slice& value,
					double& score);
			int ZRem(DBID db, const Slice& key, const Slice& value);
			int ZCount(DBID db, const Slice& key, const std::string& min,
					const std::string& max);
			int ZIncrby(DBID db, const Slice& key, double increment,
					const Slice& value, double& score);
			int ZRank(DBID db, const Slice& key, const Slice& member);
			int ZRevRank(DBID db, const Slice& key, const Slice& member);
			int ZRemRangeByRank(DBID db, const Slice& key, int start, int stop);
			int ZRemRangeByScore(DBID db, const Slice& key,
					const std::string& min, const std::string& max);
			int ZRange(DBID db, const Slice& key, int start, int stop,
					StringArray& values, ArdbQueryOptions& options);
			int ZRangeByScore(DBID db, const Slice& key, const std::string& min,
					const std::string& max, StringArray& values,
					ArdbQueryOptions& options);
			int ZRevRange(DBID db, const Slice& key, int start, int stop,
					StringArray& values, ArdbQueryOptions& options);
			int ZRevRangeByScore(DBID db, const Slice& key,
					const std::string& max, const std::string& min,
					StringArray& values, ArdbQueryOptions& options);
			int ZUnionStore(DBID db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZInterStore(DBID db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZClear(DBID db, const Slice& key);

			/*
			 * Set operations
			 */
			int SAdd(DBID db, const Slice& key, const Slice& value);
			int SCard(DBID db, const Slice& key);
			int SMembers(DBID db, const Slice& key, StringArray& values);
			int SDiff(DBID db, SliceArray& keys, StringArray& values);
			int SDiffStore(DBID db, const Slice& dst, SliceArray& keys);
			int SInter(DBID db, SliceArray& keys, StringArray& values);
			int SInterStore(DBID db, const Slice& dst, SliceArray& keys);
			bool SIsMember(DBID db, const Slice& key, const Slice& value);
			int SRem(DBID db, const Slice& key, const Slice& value);
			int SMove(DBID db, const Slice& src, const Slice& dst,
					const Slice& value);
			int SPop(DBID db, const Slice& key, std::string& value);
			int SRandMember(DBID db, const Slice& key, StringArray& values,
					int count = 1);
			int SUnion(DBID db, SliceArray& keys, StringArray& values);
			int SUnionStore(DBID db, const Slice& dst, SliceArray& keys);
			int SClear(DBID db, const Slice& key);

			/*
			 * Table operations
			 */
			int TCreate(DBID db, const Slice& tableName, SliceArray& keys);
			int TGet(DBID db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, StringArray& values);
			int TSet(DBID db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, const StringArray& values);
			int TDel(DBID db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, const StringArray& values);
			int TClear(DBID db, const Slice& tableName);
			int TCount(DBID db, const Slice& tableName);

			/*
			 * Batch operations
			 */
			int Discard(DBID db);
			int Exec(DBID db);
			int Multi(DBID db);

			int Type(DBID db, const Slice& key);
			int Sort(DBID db, const Slice& key, const StringArray& args,
					StringArray& values);

	};
}

#endif /* RRDB_HPP_ */
