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
#include <vector>
#include <list>
#include <string>
#include <deque>
#include "common.hpp"
#include "data_format.hpp"
#include "slice.hpp"
#include "util/helpers.hpp"
#include "buffer/buffer_helper.hpp"
#include "util/thread/thread.hpp"
#include "util/thread/thread_mutex.hpp"
#include "util/thread/thread_mutex_lock.hpp"
#include "util/thread/thread_local.hpp"
#include "util/thread/lock_guard.hpp"
#include "channel/all_includes.hpp"
#include "cache/level1_cache.hpp"
#include "geo/geohash_helper.hpp"

#define ARDB_OK 0
#define ERR_INVALID_ARGS -3
#define ERR_INVALID_OPERATION -4
#define ERR_INVALID_STR -5
#define ERR_DB_NOT_EXIST -6
#define ERR_KEY_EXIST -7
#define ERR_INVALID_TYPE -8
#define ERR_OUTOFRANGE -9
#define ERR_NOT_EXIST -10
#define ERR_TOO_LARGE_RESPONSE -11

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
            virtual void Seek(const Slice& target) = 0;
            virtual ~Iterator()
            {
            }
    };

    struct KeyValueEngine
    {
            virtual int Get(const Slice& key, std::string* value, bool fill_cache) = 0;
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
                    }
                    else
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

    typedef void DataChangeCallback(const DBID& db, const Slice& key, void*);
    struct DBWatcher
    {
            bool data_changed;
            DataChangeCallback* on_key_update;
            void* on_key_update_data;
            void Clear()
            {
                data_changed = false;
            }
            DBWatcher() :
                    data_changed(false), on_key_update(NULL), on_key_update_data(
                    NULL)
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
            virtual int OnKeyValue(KeyObject* key, ValueObject* value, uint32 cursor) = 0;
            virtual ~WalkHandler()
            {
            }
    };

    struct ArdbConfig
    {
            int64 hash_max_ziplist_entries;
            int64 hash_max_ziplist_value;
            int64 list_max_ziplist_entries;
            int64 list_max_ziplist_value;
            int64 zset_max_ziplist_entries;
            int64 zset_max_ziplist_value;
            int64 set_max_ziplist_entries;
            int64 set_max_ziplist_value;

            int64 L1_cache_memory_limit;

            bool check_type_before_set_string;

            bool read_fill_cache;
            bool zset_write_fill_cache;
//            bool string_write_fill_cache;
//            bool hash_write_fill_cache;
//            bool set_write_fill_cache;
//            bool list_write_fill_cache;

            ArdbConfig() :
                    hash_max_ziplist_entries(128), hash_max_ziplist_value(64), list_max_ziplist_entries(128), list_max_ziplist_value(
                            64), zset_max_ziplist_entries(128), zset_max_ziplist_value(64), set_max_ziplist_entries(
                            128), set_max_ziplist_value(64), L1_cache_memory_limit(0), check_type_before_set_string(
                            false), read_fill_cache(true), zset_write_fill_cache(false)
            {
            }
    };

    class DBHelper;
    class L1Cache;
    class Ardb
    {
        private:
            static size_t RealPosition(std::string& buf, int pos);

            KeyValueEngineFactory* m_engine_factory;
            KeyValueEngine* m_engine;
            ThreadMutex m_mutex;

            ThreadLocal<DBWatcher> m_watcher;
            template<typename T>
            int SetKeyValueObject(KeyObject& key, T& obj)
            {
                Buffer buf;
                obj.Encode(buf);
                return SetRawValue(key, buf);
            }
            template<typename T>
            int GetKeyValueObject(KeyObject& key, T& obj)
            {
                std::string buf;
                if (0 == GetRawValue(key, buf))
                {
                    Buffer buffer(const_cast<char*>(buf.data()), 0, buf.size());
                    return obj.Decode(buffer) ? 0 : -1;
                }
                return -1;
            }

            int NextKey(const DBID& db, const std::string& key, std::string& nextkey);
            int LastKey(const DBID& db, std::string& prevkey);

            ListMetaValue* GetListMeta(const DBID& db, const Slice& key, int& err, bool& create);
            SetMetaValue* GetSetMeta(const DBID& db, const Slice& key, int& err, bool& create);
            HashMetaValue* GetHashMeta(const DBID& db, const Slice& key, int& err, bool& create);
            ZSetMetaValue* GetZSetMeta(const DBID& db, const Slice& key, uint8 sort_func, int& err, bool& create);
            CommonMetaValue* GetMeta(const DBID& db, const Slice& key, bool onlyHead);

            int RenameList(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, ListMetaValue* meta);
            int RenameHash(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, HashMetaValue* meta);
            int RenameSet(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, SetMetaValue* meta);
            int RenameZSet(const DBID& db1, const Slice& key1, const DBID& db2, const Slice& key2, ZSetMetaValue* meta);

            int GetZSetZipEntry(ZSetMetaValue* meta, const ValueData& value, ZSetElement& entry, bool remove);
            int InsertZSetZipEntry(ZSetMetaValue* meta, ZSetElement& entry, uint8 sort_func);
            int GetHashZipEntry(HashMetaValue* meta, const ValueData& field, ValueData*& value);
            int HGetValue(HashKeyObject& key, HashMetaValue* meta, CommonValueObject& value);
            int HSetValue(HashKeyObject& key, HashMetaValue* meta, CommonValueObject& value);
            int HIterate(const DBID& db, const Slice& key, ValueVisitCallback* cb, void* cbdata);

            int TryZAdd(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score,
                    const Slice& value, const Slice& attr, bool check_value);
            uint32 ZRankByScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score,
                    bool contains_score);
            int ZGetByRank(const DBID& db, const Slice& key, ZSetMetaValue& meta, uint32 rank, ZSetElement& e);
            int ZInsertRangeScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score);
            int ZDeleteRangeScore(const DBID& db, const Slice& key, ZSetMetaValue& meta, const ValueData& score);
            int ZRangeByScoreRange(const DBID& db, const Slice& key, const ZRangeSpec& range, Iterator*& iter,
                    ZSetQueryOptions& options, bool check_cache, ValueVisitCallback* cb, void* cbdata);
            int ZGetNodeValue(const DBID& db, const Slice& key, const Slice& value, ValueData& score, ValueData& attr);
            CacheItem* GetLoadedCache(const DBID& db, const Slice& key, KeyType type, bool evict_non_loaded,
                    bool create_if_not_exist);

            int GetType(const DBID& db, const Slice& key, KeyType& type);
            int SetMeta(KeyObject& key, CommonMetaValue& meta);
            int SetMeta(const DBID& db, const Slice& key, CommonMetaValue& meta);
            int DelMeta(const DBID& db, const Slice& key, CommonMetaValue* meta);
            int SetExpiration(const DBID& db, const Slice& key, uint64 expire, bool check_exists);
            int GetExpiration(const DBID& db, const Slice& key, uint64& expire);

            int GetValueByPattern(const DBID& db, const Slice& pattern, ValueData& subst, ValueData& value);
            int MatchValueByPattern(const DBID& db, const Slice& key_pattern, const Slice& value_pattern,
                    ValueData& subst, ValueData& value);
            int GetRawValue(const KeyObject& key, std::string& v);
            int SetRawValue(KeyObject& key, Buffer& v);
            int DelValue(KeyObject& key);
            Iterator* FindValue(KeyObject& key, bool cache = false);
            void FindValue(Iterator*, KeyObject& key);

            int ListPush(const DBID& db, const Slice& key, const Slice& value, bool athead, bool onlyexist);
            int ListPop(const DBID& db, const Slice& key, bool athead, std::string& value);

            void FindSetMinMaxValue(const DBID& db, const Slice& key, SetMetaValue* meta);
            //int FindZSetMinMaxScore(const DBID& db, const Slice& key, ZSetMetaValue* meta, double& min, double& max);

            int BitsAnd(const DBID& db, SliceArray& keys, BitSetElementValueMap*& result, BitSetElementValueMap*& tmp);
            int BitsOr(const DBID& db, SliceArray& keys, BitSetElementValueMap*& result, bool isXor);
            int BitsNot(const DBID& db, const Slice& key, BitSetElementValueMap*& result);
            int BitOP(const DBID& db, const Slice& op, SliceArray& keys, BitSetElementValueMap*& result,
                    BitSetElementValueMap*& tmp);

            int SLastMember(const DBID& db, const Slice& key, ValueData& member);
            int SFirstMember(const DBID& db, const Slice& key, ValueData& member);

            int KeysVisit(const DBID& db, const std::string& pattern, const std::string& from, ValueVisitCallback* cb,
                    void* data);

            /*
             * Set operation
             */
            struct SetOperationCallback
            {
                    virtual void OnSubset(ValueDataArray& array) = 0;
                    virtual ~SetOperationCallback()
                    {
                    }
            };
            int SetRange(const DBID& db, const Slice& key, SetMetaValue* meta, const ValueData& value_begin,
                    const ValueData& value_end, int32 limit, bool with_begin, const std::string& pattern,
                    ValueDataArray& values);
            int ZSetRange(const DBID& db, const Slice& key, ZSetMetaValue* meta, const ValueData& value_begin,
                    const ValueData& value_end, uint32 limit, bool with_begin, ZSetElementDeque& values);
            typedef void OnSubResults(ValueDataArray& set);
            int SInter(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num);
            int SUnion(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num);
            int SDiff(const DBID& db, SliceArray& keys, SetOperationCallback* callback, uint32 max_subset_num);

            std::string m_err_cause;
            void SetErrorCause(const std::string& cause)
            {
                m_err_cause = cause;
            }

            struct KeyLocker
            {
                    typedef TreeSet<DBItemStackKey>::Type DBItemKeySet;
                    typedef TreeMap<DBItemStackKey, ThreadMutexLock*>::Type ThreadMutexLockTable;
                    typedef std::list<ThreadMutexLock*> ThreadMutexLockList;
                    SpinRWLock m_barrier_lock;
                    typedef ReadLockGuard<SpinRWLock> SpinReadLockGuard;
                    typedef WriteLockGuard<SpinRWLock> SpinWriteLockGuard;
                    ThreadMutexLockList m_barrier_pool;
                    ThreadMutexLockTable m_barrier_table;
                    bool m_enable;
                    KeyLocker(uint32 thread_num) :
                            m_enable(thread_num > 1)
                    {
                        for (uint32 i = 0; m_enable && i < thread_num; i++)
                        {
                            ThreadMutexLock* lock = new ThreadMutexLock;
                            m_barrier_pool.push_back(lock);
                        }
                    }
                    ~KeyLocker()
                    {
                        ThreadMutexLockList::iterator it = m_barrier_pool.begin();
                        while (it != m_barrier_pool.end())
                        {
                            delete *it;
                            it++;
                        }
                    }
                    void AddLockKey(const DBID& db, const Slice& key)
                    {
                        if (!m_enable)
                        {
                            return;
                        }
                        while (true)
                        {
                            ThreadMutexLock* barrier = NULL;
                            bool inserted = false;
                            {
                                SpinWriteLockGuard guard(m_barrier_lock);
                                /*
                                 * Merge find/insert operations into one 'insert' invocation
                                 */
                                std::pair<ThreadMutexLockTable::iterator, bool> insert = m_barrier_table.insert(
                                        std::make_pair(DBItemStackKey(db, key), (ThreadMutexLock*) NULL));
                                if (!insert.second)
                                {
                                    barrier = insert.first->second;
                                }
                                else
                                {
                                    barrier = m_barrier_pool.front();
                                    m_barrier_pool.pop_front();
                                    insert.first->second = barrier;
                                    inserted = true;
                                }
                            }
                            if (inserted)
                            {
                                return;
                            }
                            else
                            {
                                LockGuard<ThreadMutexLock> guard(*barrier);
                                barrier->Wait();
                            }
                        }
                    }
                    void ClearLockKey(const DBID& db, const Slice& key)
                    {
                        if (!m_enable)
                        {
                            return;
                        }
                        ThreadMutexLock* barrier = NULL;
                        ThreadMutexLockTable::iterator found;
                        {
                            SpinReadLockGuard guard(m_barrier_lock);
                            found = m_barrier_table.find(DBItemStackKey(db, key));
                            if (found != m_barrier_table.end())
                            {
                                barrier = found->second;
                            }
                        }
                        if (NULL != barrier)
                        {
                            {
                                LockGuard<ThreadMutexLock> guard(*barrier);
                                barrier->NotifyAll();
                            }
                            {
                                SpinWriteLockGuard guard(m_barrier_lock);
                                m_barrier_table.erase(found);
                                m_barrier_pool.push_back(barrier);
                            }
                        }
                    }
            };

            struct KeyLockerGuard
            {
                    KeyLocker& locker;
                    const DBID& db;
                    const Slice& key;
                    KeyLockerGuard(KeyLocker& loc, const DBID& id, const Slice& k) :
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
            DBHelper* m_db_helper;
            L1Cache* m_level1_cahce;
            ArdbConfig m_config;

            friend class L1Cache;
        public:
            Ardb(KeyValueEngineFactory* factory, uint32 multi_thread_num = 1);
            ~Ardb();

            bool Init(const ArdbConfig& cfg);
            const ArdbConfig& GetConfig() const
            {
                return m_config;
            }
            L1Cache* GetL1Cache()
            {
                return m_level1_cahce;
            }

            int RawSet(const Slice& key, const Slice& value);
            int RawDel(const Slice& key);
            int RawGet(const Slice& key, std::string* value);
            /*
             * Key-Value operations
             */
            int Set(const DBID& db, const Slice& key, const Slice& value);
            int Set(const DBID& db, const Slice& key, const Slice& value, int ex, int px, int nxx);
            int MSet(const DBID& db, SliceArray& keys, SliceArray& values);
            int MSetNX(const DBID& db, SliceArray& keys, SliceArray& value);
            int SetNX(const DBID& db, const Slice& key, const Slice& value);
            int SetEx(const DBID& db, const Slice& key, const Slice& value, uint32_t secs);
            int PSetEx(const DBID& db, const Slice& key, const Slice& value, uint32_t ms);
            int Get(const DBID& db, const Slice& key, std::string& value);
            int MGet(const DBID& db, SliceArray& keys, StringArray& values);
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
            int Keys(const DBID& db, const std::string& pattern, const std::string& from, uint32 limit,
                    StringArray& ret);
            int KeysCount(const DBID& db, const std::string& pattern, int64& count);
            int Randomkey(const DBID& db, std::string& key);
            int Scan(const DBID& db, const std::string& cursor, const std::string& pattern, uint32 limit,
                    StringArray& ret, std::string& newcursor);

            int Move(DBID srcdb, const Slice& key, DBID dstdb);

            int Append(const DBID& db, const Slice& key, const Slice& value);
            int Decr(const DBID& db, const Slice& key, int64_t& value);
            int Decrby(const DBID& db, const Slice& key, int64_t decrement, int64_t& value);
            int Incr(const DBID& db, const Slice& key, int64_t& value);
            int Incrby(const DBID& db, const Slice& key, int64_t increment, int64_t& value);
            int IncrbyFloat(const DBID& db, const Slice& key, double increment, double& value);
            int GetRange(const DBID& db, const Slice& key, int start, int end, std::string& valueobj);
            int SetRange(const DBID& db, const Slice& key, int start, const Slice& value);
            int GetSet(const DBID& db, const Slice& key, const Slice& value, std::string& valueobj);
            int BitCount(const DBID& db, const Slice& key, int64 start, int64 end);
            int GetBit(const DBID& db, const Slice& key, uint64 offset);
            int SetBit(const DBID& db, const Slice& key, uint64 offset, uint8 value);
            int BitOP(const DBID& db, const Slice& op, const Slice& dstkey, SliceArray& keys);
            int64 BitOPCount(const DBID& db, const Slice& op, SliceArray& keys);
            int BitClear(const DBID& db, const Slice& key);

            /*
             * Hash operations
             */
            int HSet(const DBID& db, const Slice& key, const Slice& field, const Slice& value);
            int HSetNX(const DBID& db, const Slice& key, const Slice& field, const Slice& value);
            int HDel(const DBID& db, const Slice& key, const SliceArray& fields);
            bool HExists(const DBID& db, const Slice& key, const Slice& field);
            int HGet(const DBID& db, const Slice& key, const Slice& field, std::string* value);
            int HIncrby(const DBID& db, const Slice& key, const Slice& field, int64_t increment, int64_t& value);
            int HMIncrby(const DBID& db, const Slice& key, const SliceArray& fields, const Int64Array& increments,
                    Int64Array& vs);
            int HIncrbyFloat(const DBID& db, const Slice& key, const Slice& field, double increment, double& value);
            int HMGet(const DBID& db, const Slice& key, const SliceArray& fields, StringArray& values);
            int HMSet(const DBID& db, const Slice& key, const SliceArray& fields, const SliceArray& values);
            int HGetAll(const DBID& db, const Slice& key, StringArray& fields, StringArray& values);
            int HKeys(const DBID& db, const Slice& key, StringArray& fields);
            int HVals(const DBID& db, const Slice& key, StringArray& values);
            int HLen(const DBID& db, const Slice& key);
            int HClear(const DBID& db, const Slice& key);
            int HScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
                    uint32 limit, ValueDataArray& vs, std::string& newcursor);
            /*
             * List operations
             */
            int LPush(const DBID& db, const Slice& key, const Slice& value);
            int LPushx(const DBID& db, const Slice& key, const Slice& value);
            int RPush(const DBID& db, const Slice& key, const Slice& value);
            int RPushx(const DBID& db, const Slice& key, const Slice& value);
            int LPop(const DBID& db, const Slice& key, std::string& v);
            int RPop(const DBID& db, const Slice& key, std::string& v);
            int LIndex(const DBID& db, const Slice& key, int index, std::string& v);
            int LInsert(const DBID& db, const Slice& key, const Slice& op, const Slice& pivot, const Slice& value);
            int LRange(const DBID& db, const Slice& key, int start, int end, ValueDataArray& values);
            int LRem(const DBID& db, const Slice& key, int count, const Slice& value);
            int LSet(const DBID& db, const Slice& key, int index, const Slice& value);
            int LTrim(const DBID& db, const Slice& key, int start, int stop);
            int RPopLPush(const DBID& db, const Slice& key1, const Slice& key2, std::string& v);
            int LClear(const DBID& db, const Slice& key);
            int LLen(const DBID& db, const Slice& key);

            /*
             * Sorted Set operations
             */
            int ZAdd(const DBID& db, const Slice& key, const ValueData& score, const Slice& value, const Slice& attr =
                    "");
            int ZAdd(const DBID& db, const Slice& key, ValueDataArray& scores, const SliceArray& svs,
                    const SliceArray& attrs);
            int ZAddLimit(const DBID& db, const Slice& key, ValueDataArray& scores, const SliceArray& svs,
                    const SliceArray& attrs, int setlimit, ValueDataArray& pops);
            int ZCard(const DBID& db, const Slice& key);
            int ZScore(const DBID& db, const Slice& key, const Slice& value, ValueData& score);
            int ZRem(const DBID& db, const Slice& key, const Slice& value);
            int ZPop(const DBID& db, const Slice& key, bool reverse, uint32 num, ValueDataArray& pops);
            int ZCount(const DBID& db, const Slice& key, const std::string& min, const std::string& max);
            int ZIncrby(const DBID& db, const Slice& key, const ValueData& increment, const Slice& value,
                    ValueData& score);
            int ZRank(const DBID& db, const Slice& key, const Slice& member);
            int ZRevRank(const DBID& db, const Slice& key, const Slice& member);
            int ZRemRangeByRank(const DBID& db, const Slice& key, int start, int stop);
            int ZRemRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max);
            int ZRange(const DBID& db, const Slice& key, int start, int stop, ValueDataArray& values,
                    ZSetQueryOptions& options);
            int ZRangeByScore(const DBID& db, const Slice& key, const std::string& min, const std::string& max,
                    ValueDataArray& values, ZSetQueryOptions& options);
            int ZRevRange(const DBID& db, const Slice& key, int start, int stop, ValueDataArray& values,
                    ZSetQueryOptions& options);
            int ZRevRangeByScore(const DBID& db, const Slice& key, const std::string& max, const std::string& min,
                    ValueDataArray& values, ZSetQueryOptions& options);
            int ZUnionStore(const DBID& db, const Slice& dst, SliceArray& keys, WeightArray& weights,
                    AggregateType type = AGGREGATE_SUM);
            int ZInterStore(const DBID& db, const Slice& dst, SliceArray& keys, WeightArray& weights,
                    AggregateType type = AGGREGATE_SUM);
            int ZClear(const DBID& db, const Slice& key);
            int ZScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
                    uint32 limit, ValueDataArray& vs, std::string& newcursor);

            /*
             * Set operations
             */
            int SAdd(const DBID& db, const Slice& key, const Slice& value);
            int SAdd(const DBID& db, const Slice& key, const SliceArray& values);
            int SCard(const DBID& db, const Slice& key);
            int SMembers(const DBID& db, const Slice& key, ValueDataArray& values);
            int SRange(const DBID& db, const Slice& key, const Slice& value_begin, int count, bool with_begin,
                    ValueDataArray& values);
            int SRevRange(const DBID& db, const Slice& key, const Slice& value_end, int count, bool with_end,
                    ValueDataArray& values);
            int SScan(const DBID& db, const std::string& key, const std::string& cursor, const std::string& pattern,
                    uint32 limit, ValueDataArray& vs, std::string& newcursor);
            int SDiff(const DBID& db, SliceArray& keys, ValueDataArray& values);
            int SDiffCount(const DBID& db, SliceArray& keys, uint32& count);
            int SDiffStore(const DBID& db, const Slice& dst, SliceArray& keys);
            int SInter(const DBID& db, SliceArray& keys, ValueDataArray& values);
            int SInterCount(const DBID& db, SliceArray& keys, uint32& count);
            int SInterStore(const DBID& db, const Slice& dst, SliceArray& keys);
            bool SIsMember(const DBID& db, const Slice& key, const Slice& value);
            int SRem(const DBID& db, const Slice& key, const Slice& value);
            int SRem(const DBID& db, const Slice& key, const SliceArray& values);
            int SMove(const DBID& db, const Slice& src, const Slice& dst, const Slice& value);
            int SPop(const DBID& db, const Slice& key, std::string& value);
            int SRandMember(const DBID& db, const Slice& key, ValueDataArray& values, int count = 1);
            int SUnionCount(const DBID& db, SliceArray& keys, uint32& count);
            int SUnion(const DBID& db, SliceArray& keys, ValueDataArray& values);
            int SUnionStore(const DBID& db, const Slice& dst, SliceArray& keys);
            int SClear(const DBID& db, const Slice& key);

            int GeoAdd(const DBID& db, const Slice& key, const GeoAddOptions& options);
            int GeoSearch(const DBID& db, const Slice& key, const GeoSearchOptions& options, ValueDataDeque& results);

            int CacheLoad(const DBID& db, const Slice& key);
            int CacheEvict(const DBID& db, const Slice& key);
            int CacheStatus(const DBID& db, const Slice& key, std::string& status);

            int Type(const DBID& db, const Slice& key);
            int Sort(const DBID& db, const Slice& key, const StringArray& args, ValueDataArray& values);
            int FlushDB(const DBID& db);
            int FlushAll();
            int CompactDB(const DBID& db);
            int CompactAll();

            int GetScript(const std::string& funacname, std::string& funcbody);
            int SaveScript(const std::string& funacname, const std::string& funcbody);
            int FlushScripts();

            void PrintDB(const DBID& db);
            void VisitDB(const DBID& db, RawValueVisitor* visitor, Iterator* iter = NULL);
            void VisitAllDB(RawValueVisitor* visitor, Iterator* iter = NULL);
            Iterator* NewIterator();
            Iterator* NewIterator(const DBID& db);

            void Walk(KeyObject& key, bool reverse, bool decodeValue, WalkHandler* handler);

            KeyValueEngine* GetEngine();
            DBWatcher& GetDBWatcher()
            {
                return m_watcher.GetValue();
            }

            bool DBExist(const DBID& db, DBID& nextdb);
            void GetAllDBIDSet(DBIDSet& dbs);
            /*
             * Default max execution time 50ms, max check item number 10000
             */
            void CheckExpireKey(const DBID& db, uint64 maxexec = 50, uint32 maxcheckitems = 10000);
    };
}

#endif /* RRDB_HPP_ */
