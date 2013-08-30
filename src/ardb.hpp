/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
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
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#include "util/thread/lock_guard.hpp"
#include "channel/all_includes.hpp"

#define ARDB_OK 0
#define ERR_INVALID_ARGS -3
#define ERR_INVALID_OPERATION -4
#define ERR_INVALID_STR -5
#define ERR_DB_NOT_EXIST -6
#define ERR_KEY_EXIST -7
#define ERR_INVALID_TYPE -8
#define ERR_OUTOFRANGE -9
#define ERR_NOT_EXIST -10

#define ARDB_GLOBAL_DB 0xFFFFFF

using namespace ardb::codec;

namespace ardb
{
	struct Iterator
	{
			virtual void Next() = 0;
			virtual void Prev() = 0;
			virtual Slice Key() const = 0;
			virtual Slice Value() const = 0;
			virtual bool Valid() = 0;
			virtual void SeekToFirst() = 0;
			virtual void SeekToLast() = 0;
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
			virtual Iterator* Find(const Slice& findkey, bool cache) = 0;
			virtual const std::string Stats()
			{
				return "";
			}
			virtual void CompactRange(const Slice& begin, const Slice& end)
			{
			}
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
			virtual const std::string GetName() = 0;
			virtual KeyValueEngine* CreateDB(const std::string& name) = 0;
			virtual void CloseDB(KeyValueEngine* engine) = 0;
			virtual void DestroyDB(KeyValueEngine* engine) = 0;
			virtual ~KeyValueEngineFactory()
			{
			}
	};

	struct KeyWatcher
	{
			virtual int OnKeyUpdated(const DBID& db, const Slice& key) = 0;
			virtual int OnAllKeyDeleted(const DBID& dbid) = 0;
			virtual ~KeyWatcher()
			{
			}
	};

	struct RawKeyListener
	{
			virtual int OnKeyUpdated(const Slice& key, const Slice& value) = 0;
			virtual int OnKeyDeleted(const Slice& key) = 0;
			virtual ~RawKeyListener()
			{
			}
	};

	struct RawValueVisitor
	{
			virtual int OnRawKeyValue(const Slice& key, const Slice& value) = 0;
			virtual ~RawValueVisitor()
			{
			}
	};

	struct WalkHandler
	{
			virtual int OnKeyValue(KeyObject* key, ValueObject* value,
					uint32 cursor) = 0;
			virtual ~WalkHandler()
			{
			}
	};

	class Ardb
	{
		private:
			static size_t RealPosition(Buffer* buf, int pos);

			KeyValueEngineFactory* m_engine_factory;
			KeyValueEngine* m_engine;
			ThreadMutex m_mutex;
			KeyWatcher* m_key_watcher;
			RawKeyListener* m_raw_key_listener;

			Thread* m_expire_check_thread;
			uint64 m_min_expireat;

			bool DBExist(const DBID& db, DBID& nextdb);
			int LastDB(DBID& db);
			int FirstDB(DBID& db);
			void CheckExpireKey(const DBID& db);
			int SetExpiration(const DBID& db, const Slice& key, uint64 expire);
			int GetExpiration(const DBID& db, const Slice& key, uint64& expire);

			int GetValueByPattern(const DBID& db, const Slice& pattern,
					ValueObject& subst, ValueObject& value);
			int GetValue(const DBID& db, const Slice& key, ValueObject* value);
			int GetValue(const KeyObject& key, ValueObject* v);
			int SetValue(KeyObject& key, ValueObject& value);
			int DelValue(KeyObject& key);
			Iterator* FindValue(KeyObject& key, bool cache = false);
			int SetHashValue(const DBID& db, const Slice& key,
					const Slice& field, ValueObject& value);
			int ListPush(const DBID& db, const Slice& key, const Slice& value,
					bool athead, bool onlyexist, float withscore = FLT_MAX);
			int ListPop(const DBID& db, const Slice& key, bool athead,
					std::string& value);
			int GetListMetaValue(const DBID& db, const Slice& key,
					ListMetaValue& meta);
			void SetListMetaValue(const DBID& db, const Slice& key,
					ListMetaValue& meta);
			int GetZSetMetaValue(const DBID& db, const Slice& key,
					ZSetMetaValue& meta);
			void SetZSetMetaValue(const DBID& db, const Slice& key,
					ZSetMetaValue& meta);
			int TryZAdd(const DBID& db, const Slice& key, ZSetMetaValue& meta,
					double score, const Slice& value);
			int GetSetMetaValue(const DBID& db, const Slice& key,
					SetMetaValue& meta);
			void SetSetMetaValue(const DBID& db, const Slice& key,
					SetMetaValue& meta);

			int GetBitSetMetaValue(const DBID& db, const Slice& key,
					BitSetMetaValue& meta);
			void SetBitSetMetaValue(const DBID& db, const Slice& key,
					BitSetMetaValue& meta);
			int GetBitSetElementValue(BitSetKeyObject& key,
					BitSetElementValue& meta);
			void SetBitSetElementValue(BitSetKeyObject& key,
					BitSetElementValue& meta);
			int BitsAnd(const DBID& db, SliceArray& keys,
					BitSetElementValueMap*& result,
					BitSetElementValueMap*& tmp);
			int BitsOr(const DBID& db, SliceArray& keys,
					BitSetElementValueMap*& result, bool isXor);
			int BitsNot(const DBID& db, const Slice& key,
					BitSetElementValueMap*& result);
			int BitOP(const DBID& db, const Slice& op, SliceArray& keys,
					BitSetElementValueMap*& result,
					BitSetElementValueMap*& tmp);

			int GetTableMetaValue(const DBID& db, const Slice& key,
					TableMetaValue& meta);
			void SetTableMetaValue(const DBID& db, const Slice& key,
					TableMetaValue& meta);
			int GetTableSchemaValue(const DBID& db, const Slice& key,
					TableSchemaValue& meta);
			void SetTableSchemaValue(const DBID& db, const Slice& key,
					TableSchemaValue& meta);
			int HGetValue(const DBID& db, const Slice& key, const Slice& field,
					ValueObject* value);
			int TInterRowKeys(const DBID& db, const Slice& tableName,
					TableSchemaValue& schema, Condition& cond,
					SliceSet& prefetch_keyset,
					TableKeyIndexValueTable& interset,
					TableKeyIndexValueTable& results);
			int TUnionRowKeys(const DBID& db, const Slice& tableName,
					TableSchemaValue& schema, Condition& cond,
					SliceSet& prefetch_keyset,
					TableKeyIndexValueTable& results);
			int TGetIndexs(const DBID& db, const Slice& tableName,
					TableSchemaValue& schema, Conditions& conds,
					SliceSet& prefetch_keyset, TableKeyIndexValueTable*& indexs,
					TableKeyIndexValueTable*& temp);
			int TCol(const DBID& db, const Slice& tableName,
					TableSchemaValue& schema, const Slice& col,
					TableKeyIndexValueTable& rs);
			bool TRowExists(const DBID& db, const Slice& tableName,
					TableSchemaValue& schema, ValueArray& rowkey);

			std::string m_err_cause;
			void SetErrorCause(const std::string& cause)
			{
				m_err_cause = cause;
			}

			struct KeyLocker
			{
					typedef btree::btree_set<DBItemKey> DBItemKeySet;
					DBItemKeySet m_locked_keys;
					ThreadMutex m_keys_mutex;
					ThreadMutexLock m_barrier;
					bool enable;
					KeyLocker() :
							enable(true)
					{
					}
					void AddLockKey(const DBID& db, const Slice& key)
					{
						if (!enable)
						{
							return;
						}
						while (true)
						{
							bool insert = false;
							{
								LockGuard<ThreadMutex> guard(m_keys_mutex);
								insert = m_locked_keys.insert(
										DBItemKey(db, key)).second;
							}
							if (insert)
							{
								return;
							} else
							{
								LockGuard<ThreadMutexLock> guard(m_barrier);
								m_barrier.Wait();
							}
						}
					}
					void ClearLockKey(const DBID& db, const Slice& key)
					{
						if (!enable)
						{
							return;
						}
						{
							LockGuard<ThreadMutex> guard(m_keys_mutex);
							m_locked_keys.erase(DBItemKey(db, key));
						}
						LockGuard<ThreadMutexLock> guard(m_barrier);
						m_barrier.NotifyAll();
					}
			};

			struct KeyLockerGuard
			{
					KeyLocker& locker;
					const DBID& db;
					const Slice& key;
					KeyLockerGuard(KeyLocker& loc, const DBID& id,
							const Slice& k) :
							locker(loc), db(id), key(k)
					{
						locker.AddLockKey(db, key);
					}
					~KeyLockerGuard()
					{
						locker.ClearLockKey(db, key);
					}
			};
			KeyLocker m_key_locker;
		public:
			Ardb(KeyValueEngineFactory* factory, bool multi_thread = true);
			~Ardb();

			bool Init(uint32 check_expire_period = 50);

			int RawSet(const Slice& key, const Slice& value);
			int RawDel(const Slice& key);
			int RawGet(const Slice& key, std::string* value);
			/*
			 * Key-Value operations
			 */
			int Set(const DBID& db, const Slice& key, const Slice& value);
			int Set(const DBID& db, const Slice& key, const Slice& value,
					int ex, int px, int nxx);
			int MSet(const DBID& db, SliceArray& keys, SliceArray& values);
			int MSetNX(const DBID& db, SliceArray& keys, SliceArray& value);
			int SetNX(const DBID& db, const Slice& key, const Slice& value);
			int SetEx(const DBID& db, const Slice& key, const Slice& value,
					uint32_t secs);
			int PSetEx(const DBID& db, const Slice& key, const Slice& value,
					uint32_t ms);
			int Get(const DBID& db, const Slice& key, std::string* value);
			int MGet(const DBID& db, SliceArray& keys, ValueArray& values);
			int Del(const DBID& db, const Slice& key);
			int Del(const DBID& db, const SliceArray& keys);
			bool Exists(const DBID& db, const Slice& key);
			int Expire(const DBID& db, const Slice& key, uint32_t secs);
			int Expireat(const DBID& db, const Slice& key, uint32_t ts);
			int64 TTL(const DBID& db, const Slice& key);
			int64 PTTL(const DBID& db, const Slice& key);
			int Persist(const DBID& db, const Slice& key);
			int Pexpire(const DBID& db, const Slice& key, uint32_t ms);
			int Pexpireat(const DBID& db, const Slice& key, uint64_t ms);
			int Strlen(const DBID& db, const Slice& key);
			int Rename(const DBID& db, const Slice& key1, const Slice& key2);
			int RenameNX(const DBID& db, const Slice& key1, const Slice& key2);
			int Keys(const DBID& db, const std::string& pattern,
					StringSet& ret);
			int Move(DBID srcdb, const Slice& key, DBID dstdb);

			int Append(const DBID& db, const Slice& key, const Slice& value);
			int Decr(const DBID& db, const Slice& key, int64_t& value);
			int Decrby(const DBID& db, const Slice& key, int64_t decrement,
					int64_t& value);
			int Incr(const DBID& db, const Slice& key, int64_t& value);
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
			int BitCount(const DBID& db, const Slice& key, int64 start,
					int64 end);
			int GetBit(const DBID& db, const Slice& key, uint64 offset);
			int SetBit(const DBID& db, const Slice& key, uint64 offset,
					uint8 value);
			int BitOP(const DBID& db, const Slice& op, const Slice& dstkey,
					SliceArray& keys);
			int64 BitOPCount(const DBID& db, const Slice& op, SliceArray& keys);
			int BitClear(const DBID& db, const Slice& key);

			/*
			 * Hash operations
			 */
			int HSet(const DBID& db, const Slice& key, const Slice& field,
					const Slice& value);
			int HSetNX(const DBID& db, const Slice& key, const Slice& field,
					const Slice& value);
			int HDel(const DBID& db, const Slice& key, const Slice& field);
			int HDel(const DBID& db, const Slice& key,
					const SliceArray& fields);
			bool HExists(const DBID& db, const Slice& key, const Slice& field);
			int HGet(const DBID& db, const Slice& key, const Slice& field,
					std::string* value);
			int HIncrby(const DBID& db, const Slice& key, const Slice& field,
					int64_t increment, int64_t& value);
			int HMIncrby(const DBID& db, const Slice& key,
					const SliceArray& fields, const Int64Array& increments,
					Int64Array& vs);
			int HIncrbyFloat(const DBID& db, const Slice& key,
					const Slice& field, double increment, double& value);
			int HMGet(const DBID& db, const Slice& key,
					const SliceArray& fields, ValueArray& values);
			int HMSet(const DBID& db, const Slice& key,
					const SliceArray& fields, const SliceArray& values);
			int HGetAll(const DBID& db, const Slice& key, StringArray& fields,
					ValueArray& values);
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
			int LIndex(const DBID& db, const Slice& key, int index,
					std::string& v);
			int LInsert(const DBID& db, const Slice& key, const Slice& op,
					const Slice& pivot, const Slice& value);
			int LRange(const DBID& db, const Slice& key, int start, int end,
					ValueArray& values);
			int LRem(const DBID& db, const Slice& key, int count,
					const Slice& value);
			int LSet(const DBID& db, const Slice& key, int index,
					const Slice& value);
			int LTrim(const DBID& db, const Slice& key, int start, int stop);
			int RPopLPush(const DBID& db, const Slice& key1, const Slice& key2,
					std::string& v);
			int LClear(const DBID& db, const Slice& key);
			int LLen(const DBID& db, const Slice& key);

			/*
			 * Sorted Set operations
			 */
			int ZAdd(const DBID& db, const Slice& key, double score,
					const Slice& value);
			int ZAdd(const DBID& db, const Slice& key, DoubleArray& scores,
					const SliceArray& svs);
			int ZAddLimit(const DBID& db, const Slice& key, DoubleArray& scores,
					const SliceArray& svs, int setlimit, ValueArray& pops);
			int ZCard(const DBID& db, const Slice& key);
			int ZScore(const DBID& db, const Slice& key, const Slice& value,
					double& score);
			int ZRem(const DBID& db, const Slice& key, const Slice& value);
			int ZPop(const DBID& db, const Slice& key, bool reverse, uint32 num,
					ValueArray& pops);
			int ZCount(const DBID& db, const Slice& key, const std::string& min,
					const std::string& max);
			int ZIncrby(const DBID& db, const Slice& key, double increment,
					const Slice& value, double& score);
			int ZRank(const DBID& db, const Slice& key, const Slice& member);
			int ZRevRank(const DBID& db, const Slice& key, const Slice& member);
			int ZRemRangeByRank(const DBID& db, const Slice& key, int start,
					int stop);
			int ZRemRangeByScore(const DBID& db, const Slice& key,
					const std::string& min, const std::string& max);
			int ZRange(const DBID& db, const Slice& key, int start, int stop,
					ValueArray& values, QueryOptions& options);
			int ZRangeByScore(const DBID& db, const Slice& key,
					const std::string& min, const std::string& max,
					ValueArray& values, QueryOptions& options);
			int ZRevRange(const DBID& db, const Slice& key, int start, int stop,
					ValueArray& values, QueryOptions& options);
			int ZRevRangeByScore(const DBID& db, const Slice& key,
					const std::string& max, const std::string& min,
					ValueArray& values, QueryOptions& options);
			int ZUnionStore(const DBID& db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZInterStore(const DBID& db, const Slice& dst, SliceArray& keys,
					WeightArray& weights, AggregateType type = AGGREGATE_SUM);
			int ZClear(const DBID& db, const Slice& key);

			/*
			 * Set operations
			 */
			int SAdd(const DBID& db, const Slice& key, const Slice& value);
			int SAdd(const DBID& db, const Slice& key,
					const SliceArray& values);
			int SCard(const DBID& db, const Slice& key);
			int SMembers(const DBID& db, const Slice& key, ValueArray& values);
			int SRange(const DBID& db, const Slice& key,
					const Slice& value_begin, int count, bool with_begin,
					ValueArray& values);
			int SRevRange(const DBID& db, const Slice& key,
					const Slice& value_end, int count, bool with_end,
					ValueArray& values);
			int SDiff(const DBID& db, SliceArray& keys, ValueSet& values);
			int SDiffCount(const DBID& db, SliceArray& keys, uint32& count);
			int SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys);
			int SInter(const DBID& db, SliceArray& keys, ValueSet& values);
			int SInterCount(const DBID& db, SliceArray& keys, uint32& count);
			int SInterStore(const DBID& db, const Slice& dst, SliceArray& keys);
			bool SIsMember(const DBID& db, const Slice& key,
					const Slice& value);
			int SRem(const DBID& db, const Slice& key, const Slice& value);
			int SRem(const DBID& db, const Slice& key,
					const SliceArray& values);
			int SMove(const DBID& db, const Slice& src, const Slice& dst,
					const Slice& value);
			int SPop(const DBID& db, const Slice& key, std::string& value);
			int SRandMember(const DBID& db, const Slice& key,
					ValueArray& values, int count = 1);
			int SUnionCount(const DBID& db, SliceArray& keys, uint32& count);
			int SUnion(const DBID& db, SliceArray& keys, ValueSet& values);
			int SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys);
			int SClear(const DBID& db, const Slice& key);

			/*
			 * Table operations
			 */
			int TCreate(const DBID& db, const Slice& tableName,
					SliceArray& keys);
			int TGet(const DBID& db, const Slice& tableName,
					TableQueryOptions& options, ValueArray& values,
					std::string& err);
			int TGetAll(const DBID& db, const Slice& tableName,
					ValueArray& values);
			int TUpdate(const DBID& db, const Slice& tableName,
					TableUpdateOptions& options);
			int TInsert(const DBID& db, const Slice& tableName,
					TableInsertOptions& options, bool replace,
					std::string& err);
			int TDel(const DBID& db, const Slice& tableName,
					TableDeleteOptions& conds, std::string& err);
			int TDelCol(const DBID& db, const Slice& tableName,
					const Slice& col);
			int TCreateIndex(const DBID& db, const Slice& tableName,
					const Slice& col);
			int TClear(const DBID& db, const Slice& tableName);
			int TCount(const DBID& db, const Slice& tableName);
			int TDesc(const DBID& db, const Slice& tableName, std::string& str);

			int Type(const DBID& db, const Slice& key);
			int Sort(const DBID& db, const Slice& key, const StringArray& args,
					ValueArray& values);
			int FlushDB(const DBID& db);
			int FlushAll();
			int CompactDB(const DBID& db);
			int CompactAll();

			int GetScript(const std::string& funacname, std::string& funcbody);
			int SaveScript(const std::string& funacname, const std::string& funcbody);
			int FlushScripts();

			void PrintDB(const DBID& db);
			void VisitDB(const DBID& db, RawValueVisitor* visitor,
					Iterator* iter = NULL);
			void VisitAllDB(RawValueVisitor* visitor, Iterator* iter = NULL);
			Iterator* NewIterator();
			Iterator* NewIterator(const DBID& db);

			void Walk(KeyObject& key, bool reverse, WalkHandler* handler);
			void Walk(WalkHandler* handler);

			KeyValueEngine* GetEngine();
			void RegisterKeyWatcher(KeyWatcher* w)
			{
				m_key_watcher = w;
			}
			void RegisterRawKeyListener(RawKeyListener* w)
			{
				m_raw_key_listener = w;
			}
	};
}

#endif /* RRDB_HPP_ */
