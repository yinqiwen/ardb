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
			virtual KeyValueEngine* CreateDB(const DBID& db) = 0;
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
			KeyValueEngine* GetDB(const DBID& db);

			int SetExpiration(const DBID& db, const Slice& key, uint64_t expire);
			int GetValue(const DBID& db, const KeyObject& key, ValueObject* v,
					uint64* expire = NULL);
			int SetValue(const DBID& db, KeyObject& key, ValueObject& value,
					uint64 expire = 0);
			int DelValue(const DBID& db, KeyObject& key);
			Iterator* FindValue(const DBID& db, KeyObject& key);
			int SetHashValue(const DBID& db, const Slice& key, const Slice& field,
					ValueObject& value);
			int ListPush(const DBID& db, const Slice& key, const Slice& value,
					bool athead, bool onlyexist, double withscore = DBL_MAX);
			int ListPop(const DBID& db, const Slice& key, bool athead,
					std::string& value);
			int GetZSetMetaValue(const DBID& db, const Slice& key,
					ZSetMetaValue& meta);
			void SetZSetMetaValue(const DBID& db, const Slice& key,
					ZSetMetaValue& meta);
			int GetSetMetaValue(const DBID& db, const Slice& key, SetMetaValue& meta);
			void SetSetMetaValue(const DBID& db, const Slice& key, SetMetaValue& meta);
			int HGetValue(const DBID& db, const Slice& key, const Slice& field,
					ValueObject* value);
			struct WalkHandler
			{
					virtual int OnKeyValue(KeyObject* key,
							ValueObject* value) = 0;
					virtual ~WalkHandler()
					{
					}
			};
			void Walk(const DBID& db, KeyObject& key, bool reverse,
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
			int Set(const DBID& db, const Slice& key, const Slice& value);
			int Set(const DBID& db, const Slice& key, const Slice& value, int ex,
					int px, int nxx);
			int MSet(const DBID& db, SliceArray& keys, SliceArray& values);
			int MSetNX(const DBID& db, SliceArray& keys, SliceArray& value);
			int SetNX(const DBID& db, const Slice& key, const Slice& value);
			int SetEx(const DBID& db, const Slice& key, const Slice& value,
					uint32_t secs);
			int PSetEx(const DBID& db, const Slice& key, const Slice& value,
					uint32_t ms);
			int Get(const DBID& db, const Slice& key, std::string* value);
			int MGet(const DBID& db, SliceArray& keys, StringArray& value);
			int Del(const DBID& db, const Slice& key);
			int Del(const DBID& db, const SliceArray& keys);
			bool Exists(const DBID& db, const Slice& key);
			int Expire(const DBID& db, const Slice& key, uint32_t secs);
			int Expireat(const DBID& db, const Slice& key, uint32_t ts);
			int TTL(const DBID& db, const Slice& key);
			int PTTL(const DBID& db, const Slice& key);
			int Persist(const DBID& db, const Slice& key);
			int Pexpire(const DBID& db, const Slice& key, uint32_t ms);
			int Strlen(const DBID& db, const Slice& key);
			int Rename(const DBID& db, const Slice& key1, const Slice& key2);
			int RenameNX(const DBID& db, const Slice& key1, const Slice& key2);
			int Keys(const DBID& db, const std::string& pattern,
					std::list<std::string>& ret)
			{
				return -1;
			}
			int Move(DBID srcdb, const Slice& key, DBID dstdb);

			int Append(const DBID& db, const Slice& key, const Slice& value);
			int Decr(const DBID& db, const Slice& key, int64_t& value);
			int Decrby(const DBID& db, const Slice& key, int64_t decrement,
					int64_t& value);
			int Incr(const DBID& db, const Slice& key, int64_t& value);
			int XIncrby(const DBID& db, const Slice& key, int64_t increment);
			int Incrby(const DBID& db, const Slice& key, int64_t increment,
					int64_t& value);
			int IncrbyFloat(const DBID& db, const Slice& key, double increment,
					double& value);
			int GetRange(const DBID& db, const Slice& key, int start, int end,
					std::string& valueobj);
			int SetRange(const DBID& db, const Slice& key, int start,
					const Slice& value);
			int GetSet(const DBID& db, const Slice& key, const Slice& value,
					std::string& valueobj);
			int BitCount(const DBID& db, const Slice& key, int start, int end);
			int GetBit(const DBID& db, const Slice& key, int offset);
			int SetBit(const DBID& db, const Slice& key, uint32_t offset,
					uint8_t value);
			int BitOP(const DBID& db, const Slice& op, const Slice& dstkey,
					SliceArray& keys);

			/*
			 * Hash operations
			 */
			int HSet(const DBID& db, const Slice& key, const Slice& field,
					const Slice& value);
			int HSetNX(const DBID& db, const Slice& key, const Slice& field,
					const Slice& value);
			int HDel(const DBID& db, const Slice& key, const Slice& field);
			bool HExists(const DBID& db, const Slice& key, const Slice& field);
			int HGet(const DBID& db, const Slice& key, const Slice& field,
					std::string* value);
			int HIncrby(const DBID& db, const Slice& key, const Slice& field,
					int64_t increment, int64_t& value);
			int HIncrbyFloat(const DBID& db, const Slice& key, const Slice& field,
					double increment, double& value);
			int HMGet(const DBID& db, const Slice& key, const SliceArray& fields,
					StringArray& values);
			int HMSet(const DBID& db, const Slice& key, const SliceArray& fields,
					const SliceArray& values);
			int HGetAll(const DBID& db, const Slice& key, StringArray& fields,
					StringArray& values);
			int HKeys(const DBID& db, const Slice& key, StringArray& fields);
			int HVals(const DBID& db, const Slice& key, StringArray& values);
			int HLen(const DBID& db, const Slice& key);
			int HClear(const DBID& db, const Slice& key);

			/*
			 * List operations
			 */
			int LPush(const DBID& db, const Slice& key, const Slice& value);
			int LPushx(const DBID& db, const Slice& key, const Slice& value);
			int RPush(const DBID& db, const Slice& key, const Slice& value);
			int RPushx(const DBID& db, const Slice& key, const Slice& value);
			int LPop(const DBID& db, const Slice& key, std::string& v);
			int RPop(const DBID& db, const Slice& key, std::string& v);
			int LIndex(const DBID& db, const Slice& key, uint32_t index,
					std::string& v);
			int LInsert(const DBID& db, const Slice& key, const Slice& op,
					const Slice& pivot, const Slice& value);
			int LRange(const DBID& db, const Slice& key, int start, int end,
					StringArray& values);
			int LRem(const DBID& db, const Slice& key, int count, const Slice& value);
			int LSet(const DBID& db, const Slice& key, int index, const Slice& value);
			int LTrim(const DBID& db, const Slice& key, int start, int stop);
			int RPopLPush(const DBID& db, const Slice& key1, const Slice& key2);
			int LClear(const DBID& db, const Slice& key);
			int LLen(const DBID& db, const Slice& key);

			/*
			 * Sorted Set operations
			 */
			int ZAdd(const DBID& db, const Slice& key, double score,
					const Slice& value);
			int ZCard(const DBID& db, const Slice& key);
			int ZScore(const DBID& db, const Slice& key, const Slice& value,
					double& score);
			int ZRem(const DBID& db, const Slice& key, const Slice& value);
			int ZCount(const DBID& db, const Slice& key, const std::string& min,
					const std::string& max);
			int ZIncrby(const DBID& db, const Slice& key, double increment,
					const Slice& value, double& score);
			int ZRank(const DBID& db, const Slice& key, const Slice& member);
			int ZRevRank(const DBID& db, const Slice& key, const Slice& member);
			int ZRemRangeByRank(const DBID& db, const Slice& key, int start, int stop);
			int ZRemRangeByScore(const DBID& db, const Slice& key,
					const std::string& min, const std::string& max);
			int ZRange(const DBID& db, const Slice& key, int start, int stop,
					StringArray& values, ArdbQueryOptions& options);
			int ZRangeByScore(const DBID& db, const Slice& key, const std::string& min,
					const std::string& max, StringArray& values,
					ArdbQueryOptions& options);
			int ZRevRange(const DBID& db, const Slice& key, int start, int stop,
					StringArray& values, ArdbQueryOptions& options);
			int ZRevRangeByScore(const DBID& db, const Slice& key,
					const std::string& max, const std::string& min,
					StringArray& values, ArdbQueryOptions& options);
			int ZUnionStore(const DBID& db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZInterStore(const DBID& db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZClear(const DBID& db, const Slice& key);

			/*
			 * Set operations
			 */
			int SAdd(const DBID& db, const Slice& key, const Slice& value);
			int SCard(const DBID& db, const Slice& key);
			int SMembers(const DBID& db, const Slice& key, StringArray& values);
			int SDiff(const DBID& db, SliceArray& keys, StringArray& values);
			int SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys);
			int SInter(const DBID& db, SliceArray& keys, StringArray& values);
			int SInterStore(const DBID& db, const Slice& dst, SliceArray& keys);
			bool SIsMember(const DBID& db, const Slice& key, const Slice& value);
			int SRem(const DBID& db, const Slice& key, const Slice& value);
			int SMove(const DBID& db, const Slice& src, const Slice& dst,
					const Slice& value);
			int SPop(const DBID& db, const Slice& key, std::string& value);
			int SRandMember(const DBID& db, const Slice& key, StringArray& values,
					int count = 1);
			int SUnion(const DBID& db, SliceArray& keys, StringArray& values);
			int SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys);
			int SClear(const DBID& db, const Slice& key);

			/*
			 * Table operations
			 */
			int TCreate(const DBID& db, const Slice& tableName, SliceArray& keys);
			int TGet(const DBID& db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, StringArray& values);
			int TSet(const DBID& db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, const StringArray& values);
			int TDel(const DBID& db, const Slice& tableName, const SliceArray& fields,
					KeyCondition& conds, const StringArray& values);
			int TClear(const DBID& db, const Slice& tableName);
			int TCount(const DBID& db, const Slice& tableName);

			/*
			 * Batch operations
			 */
			int Discard(const DBID& db);
			int Exec(const DBID& db);
			int Multi(const DBID& db);

			int Type(const DBID& db, const Slice& key);
			int Sort(const DBID& db, const Slice& key, const StringArray& args,
					StringArray& values);

	};
}

#endif /* RRDB_HPP_ */
