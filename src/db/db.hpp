/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
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

#ifndef DB_HPP_
#define DB_HPP_
#include "common/common.hpp"
#include "thread/thread_local.hpp"
#include "thread/spin_rwlock.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "thread/thread_mutex_lock.hpp"
#include "channel/all_includes.hpp"
#include "command/lua_scripting.hpp"
#include "db/engine.hpp"
#include "statistics.hpp"
#include "context.hpp"
#include "config.hpp"
#include "logger.hpp"
#include <stack>
#include <sparsehash/dense_hash_map>

/* Command flags. Please check the command table defined in the redis.c file
 * for more information about the meaning of every flag. */
#define ARDB_CMD_WRITE 1                   /* "w" flag */
#define ARDB_CMD_READONLY 2                /* "r" flag */
//#define ARDB_CMD_DENYOOM 4                 /* "m" flag */
//#define ARDB_CMD_NOT_USED_1 8              /* no longer used flag */
#define ARDB_CMD_ADMIN 16                  /* "a" flag */
#define ARDB_CMD_PUBSUB 32                 /* "p" flag */
#define ARDB_CMD_NOSCRIPT  64              /* "s" flag */
#define ARDB_CMD_RANDOM 128                /* "R" flag */
//#define ARDB_CMD_SORT_FOR_SCRIPT 256       /* "S" flag */
//#define ARDB_CMD_LOADING 512               /* "l" flag */
//#define ARDB_CMD_STALE 1024                /* "t" flag */
//#define ARDB_CMD_SKIP_MONITOR 2048         /* "M" flag */
//#define ARDB_CMD_ASKING 4096               /* "k" flag */

using namespace ardb::codec;

OP_NAMESPACE_BEGIN

    class Ardb
    {
        public:
            typedef int (Ardb::*RedisCommandHandler)(Context&, RedisCommandFrame&);
            struct RedisCommandHandlerSetting
            {
                    const char* name;
                    RedisCommandType type;
                    RedisCommandHandler handler;
                    int min_arity;
                    int max_arity;
                    const char* sflags;
                    int flags;
                    volatile uint64 microseconds;
                    volatile uint64 calls;
                    //CostTrack
            };
            struct RedisCommandHash
            {
                    size_t operator()(const std::string& t) const;
            };
            struct RedisCommandEqual
            {
                    bool operator()(const std::string& s1, const std::string& s2) const;
            };

            struct KeyPrefix
            {
                    Data ns;
                    Data key;
                    bool operator<(const KeyPrefix& other) const
                    {
                        int cmp = ns.Compare(other.ns);
                        if (cmp < 0)
                        {
                            return true;
                        }
                        if (cmp > 0)
                        {
                            return false;
                        }
                        return key < other.key;
                    }
            };

            struct KeyLockGuard
            {
                    KeyObject& k;
                    KeyLockGuard(KeyObject& key);
                    ~KeyLockGuard();
            };

        private:
            Engine* m_engine;
            ArdbConfig m_conf;
            ThreadLocal<LUAInterpreter> m_lua;

            typedef google::dense_hash_map<std::string, RedisCommandHandlerSetting, RedisCommandHash, RedisCommandEqual> RedisCommandHandlerSettingTable;
            RedisCommandHandlerSettingTable m_settings;
            typedef TreeMap<KeyPrefix, ThreadMutexLock*>::Type LockTable;
            typedef std::stack<ThreadMutexLock*> LockPool;
            SpinMutexLock m_locking_keys_lock;
            LockTable m_locking_keys;
            LockPool m_lock_pool;

            void LockKey(KeyObject& key);
            void UnlockKey(KeyObject& key);

            bool GetLongFromProtocol(Context& ctx, const std::string& str, int64_t& v);

            void TryPushSlowCommand(const RedisCommandFrame& cmd, uint64 micros);
            void GetSlowlog(Context& ctx, uint32 len);
            int ObjectLen(Context& ctx, KeyType type, const std::string& key);

            void FillInfoResponse(const std::string& section, std::string& info);

            int SubscribeChannel(Context& ctx, const std::string& channel, bool notify);
            int UnsubscribeChannel(Context& ctx, const std::string& channel, bool notify);
            int UnsubscribeAll(Context& ctx, bool notify);
            int PSubscribeChannel(Context& ctx, const std::string& pattern, bool notify);
            int PUnsubscribeChannel(Context& ctx, const std::string& pattern, bool notify);
            int PUnsubscribeAll(Context& ctx, bool notify);
            int PublishMessage(Context& ctx, const std::string& channel, const std::string& message);

            int StringSet(Context& ctx, const std::string& key, const std::string& value, bool redis_compatible, int64_t px = -1, int8_t nx_xx = -1);

            int ListPop(Context& ctx, RedisCommandFrame& cmd);
            int ListPush(Context& ctx, RedisCommandFrame& cmd);

            bool AdjustMergeOp(uint16& op, DataArray& args);
            int MergeAppend(Context& ctx,KeyObject& key, ValueObject& val, const std::string& append);
            int MergeIncrBy(Context& ctx,KeyObject& key, ValueObject& val, int64_t inc);
            int MergeIncrByFloat(Context& ctx,KeyObject& key, ValueObject& val, double inc);
            int MergeSet(Context& ctx,KeyObject& key, ValueObject& val, uint16_t op, const Data& data, int64_t ttl);
            int MergeSetRange(Context& ctx,KeyObject& key, ValueObject& val, int64_t offset, const std::string& range);
            int MergeHSet(Context& ctx,KeyObject& key, ValueObject& value, uint16_t op, const Data& v);
            int MergeHDel(Context& ctx,KeyObject& key, ValueObject& meta_value);
            int MergeHIncrby(Context& ctx,KeyObject& key, ValueObject& value, uint16_t op, const Data& v);
            int MergeListPop(Context& ctx,KeyObject& key, ValueObject& value, uint16_t op, Data& element);
            int MergeListPush(Context& ctx,KeyObject& key, ValueObject& value, uint16_t op, const DataArray& args);
            int MergeSAdd(Context& ctx,KeyObject& key, ValueObject& value, const DataArray& ms);
            int MergeSRem(Context& ctx,KeyObject& key, ValueObject& value, const DataArray& ms);
            int MergeExpire(Context& ctx, const KeyObject& key, ValueObject& meta, int64 ms);
            int MergeSetBit(Context& ctx, const KeyObject& key, ValueObject& meta, int64 offset, uint8 bit, uint8* oldbit);
            int MergePFAdd(Context& ctx,KeyObject& key, ValueObject& value, const DataArray& ms, int* updated = NULL);

            bool CheckMeta(Context& ctx, const std::string& key, KeyType expected);
            bool CheckMeta(Context& ctx, const std::string& key, KeyType expected, ValueObject& meta);
            bool CheckMeta(Context& ctx, const KeyObject& key, KeyType expected, ValueObject& meta);

            int DelKey(Context& ctx, const KeyObject& meta_key, Iterator*& iter);

            int HIterate(Context& ctx, RedisCommandFrame& cmd);

            int WatchForKey(Context& ctx, const std::string& key);
            int UnwatchKeys(Context& ctx);
            int AbortWatchKey(Context& ctx, const std::string& key);

            int IncrDecrCommand(Context& ctx, RedisCommandFrame& cmd);

            int FireKeyChangedEvent(Context& ctx, const KeyObject& key);

            int Time(Context& ctx, RedisCommandFrame& cmd);
            int FlushDB(Context& ctx, RedisCommandFrame& cmd);
            int FlushAll(Context& ctx, RedisCommandFrame& cmd);
            int Save(Context& ctx, RedisCommandFrame& cmd);
            int LastSave(Context& ctx, RedisCommandFrame& cmd);
            int BGSave(Context& ctx, RedisCommandFrame& cmd);
            int Import(Context& ctx, RedisCommandFrame& cmd);
            int Info(Context& ctx, RedisCommandFrame& cmd);
            int DBSize(Context& ctx, RedisCommandFrame& cmd);
            int Config(Context& ctx, RedisCommandFrame& cmd);
            int SlowLog(Context& ctx, RedisCommandFrame& cmd);
            int Client(Context& ctx, RedisCommandFrame& cmd);
            int Keys(Context& ctx, RedisCommandFrame& cmd);
            int KeysCount(Context& ctx, RedisCommandFrame& cmd);
            int Randomkey(Context& ctx, RedisCommandFrame& cmd);
            int Scan(Context& ctx, RedisCommandFrame& cmd);

            int Multi(Context& ctx, RedisCommandFrame& cmd);
            int Discard(Context& ctx, RedisCommandFrame& cmd);
            int Exec(Context& ctx, RedisCommandFrame& cmd);
            int Watch(Context& ctx, RedisCommandFrame& cmd);
            int UnWatch(Context& ctx, RedisCommandFrame& cmd);

            int Subscribe(Context& ctx, RedisCommandFrame& cmd);
            int UnSubscribe(Context& ctx, RedisCommandFrame& cmd);
            int PSubscribe(Context& ctx, RedisCommandFrame& cmd);
            int PUnSubscribe(Context& ctx, RedisCommandFrame& cmd);
            int Publish(Context& ctx, RedisCommandFrame& cmd);

            int Slaveof(Context& ctx, RedisCommandFrame& cmd);
            int Sync(Context& ctx, RedisCommandFrame& cmd);
            int PSync(Context& ctx, RedisCommandFrame& cmd);
            int ReplConf(Context& ctx, RedisCommandFrame& cmd);
            int RawSet(Context& ctx, RedisCommandFrame& cmd);
            int RawDel(Context& ctx, RedisCommandFrame& cmd);

            int Ping(Context& ctx, RedisCommandFrame& cmd);
            int Echo(Context& ctx, RedisCommandFrame& cmd);
            int Select(Context& ctx, RedisCommandFrame& cmd);
            int Quit(Context& ctx, RedisCommandFrame& cmd);

            int Shutdown(Context& ctx, RedisCommandFrame& cmd);
            int Type(Context& ctx, RedisCommandFrame& cmd);
            int Move(Context& ctx, RedisCommandFrame& cmd);
            int Rename(Context& ctx, RedisCommandFrame& cmd);
            int RenameNX(Context& ctx, RedisCommandFrame& cmd);
            int Sort(Context& ctx, RedisCommandFrame& cmd);

            int CompactDB(Context& ctx, RedisCommandFrame& cmd);
            int CompactAll(Context& ctx, RedisCommandFrame& cmd);

            int Append(Context& ctx, RedisCommandFrame& cmd);

            int Decr(Context& ctx, RedisCommandFrame& cmd);
            int Decrby(Context& ctx, RedisCommandFrame& cmd);
            int Get(Context& ctx, RedisCommandFrame& cmd);
            int GetRange(Context& ctx, RedisCommandFrame& cmd);
            int GetSet(Context& ctx, RedisCommandFrame& cmd);
            int Incr(Context& ctx, RedisCommandFrame& cmd);
            int Incrby(Context& ctx, RedisCommandFrame& cmd);
            int IncrbyFloat(Context& ctx, RedisCommandFrame& cmd);
            int MGet(Context& ctx, RedisCommandFrame& cmd);
            int MSet(Context& ctx, RedisCommandFrame& cmd);
            int MSetNX(Context& ctx, RedisCommandFrame& cmd);
            int PSetEX(Context& ctx, RedisCommandFrame& cmd);
            int SetEX(Context& ctx, RedisCommandFrame& cmd);
            int SetNX(Context& ctx, RedisCommandFrame& cmd);
            int SetRange(Context& ctx, RedisCommandFrame& cmd);
            int Strlen(Context& ctx, RedisCommandFrame& cmd);
            int Set(Context& ctx, RedisCommandFrame& cmd);

            int Del(Context& ctx, RedisCommandFrame& cmd);
            int Exists(Context& ctx, RedisCommandFrame& cmd);
            int Expire(Context& ctx, RedisCommandFrame& cmd);
            int Expireat(Context& ctx, RedisCommandFrame& cmd);
            int Persist(Context& ctx, RedisCommandFrame& cmd);
            int PExpire(Context& ctx, RedisCommandFrame& cmd);
            int PExpireat(Context& ctx, RedisCommandFrame& cmd);
            int PTTL(Context& ctx, RedisCommandFrame& cmd);
            int TTL(Context& ctx, RedisCommandFrame& cmd);

            int Bitcount(Context& ctx, RedisCommandFrame& cmd);
            int Bitop(Context& ctx, RedisCommandFrame& cmd);
            int Bitpos(Context& ctx, RedisCommandFrame& cmd);
            int BitopCount(Context& ctx, RedisCommandFrame& cmd);
            int SetBit(Context& ctx, RedisCommandFrame& cmd);
            int GetBit(Context& ctx, RedisCommandFrame& cmd);

            int HDel(Context& ctx, RedisCommandFrame& cmd);
            int HExists(Context& ctx, RedisCommandFrame& cmd);
            int HGet(Context& ctx, RedisCommandFrame& cmd);
            int HGetAll(Context& ctx, RedisCommandFrame& cmd);
            int HIncrby(Context& ctx, RedisCommandFrame& cmd);
            int HIncrbyFloat(Context& ctx, RedisCommandFrame& cmd);
            int HKeys(Context& ctx, RedisCommandFrame& cmd);
            int HLen(Context& ctx, RedisCommandFrame& cmd);
            int HMGet(Context& ctx, RedisCommandFrame& cmd);
            int HMSet(Context& ctx, RedisCommandFrame& cmd);
            int HSet(Context& ctx, RedisCommandFrame& cmd);
            int HSetNX(Context& ctx, RedisCommandFrame& cmd);
            int HVals(Context& ctx, RedisCommandFrame& cmd);
            int HScan(Context& ctx, RedisCommandFrame& cmd);

            int SAdd(Context& ctx, RedisCommandFrame& cmd);
            int SCard(Context& ctx, RedisCommandFrame& cmd);
            int SDiff(Context& ctx, RedisCommandFrame& cmd);
            int SDiffStore(Context& ctx, RedisCommandFrame& cmd);
            int SInter(Context& ctx, RedisCommandFrame& cmd);
            int SInterStore(Context& ctx, RedisCommandFrame& cmd);
            int SIsMember(Context& ctx, RedisCommandFrame& cmd);
            int SMembers(Context& ctx, RedisCommandFrame& cmd);
            int SMove(Context& ctx, RedisCommandFrame& cmd);
            int SPop(Context& ctx, RedisCommandFrame& cmd);
            int SRandMember(Context& ctx, RedisCommandFrame& cmd);
            int SRem(Context& ctx, RedisCommandFrame& cmd);
            int SUnion(Context& ctx, RedisCommandFrame& cmd);
            int SUnionStore(Context& ctx, RedisCommandFrame& cmd);
            int SUnionCount(Context& ctx, RedisCommandFrame& cmd);
            int SInterCount(Context& ctx, RedisCommandFrame& cmd);
            int SDiffCount(Context& ctx, RedisCommandFrame& cmd);
            int SScan(Context& ctx, RedisCommandFrame& cmd);

            int ZAdd(Context& ctx, RedisCommandFrame& cmd);
            int ZCard(Context& ctx, RedisCommandFrame& cmd);
            int ZCount(Context& ctx, RedisCommandFrame& cmd);
            int ZIncrby(Context& ctx, RedisCommandFrame& cmd);
            int ZRange(Context& ctx, RedisCommandFrame& cmd);
            int ZRangeByScore(Context& ctx, RedisCommandFrame& cmd);
            int ZRank(Context& ctx, RedisCommandFrame& cmd);
            int ZRem(Context& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByRank(Context& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByScore(Context& ctx, RedisCommandFrame& cmd);
            int ZRevRange(Context& ctx, RedisCommandFrame& cmd);
            int ZRevRangeByScore(Context& ctx, RedisCommandFrame& cmd);
            int ZRevRank(Context& ctx, RedisCommandFrame& cmd);
            int ZInterStore(Context& ctx, RedisCommandFrame& cmd);
            int ZUnionStore(Context& ctx, RedisCommandFrame& cmd);
            int ZScore(Context& ctx, RedisCommandFrame& cmd);
            int ZScan(Context& ctx, RedisCommandFrame& cmd);
            int ZLexCount(Context& ctx, RedisCommandFrame& cmd);
            int ZRangeByLex(Context& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByLex(Context& ctx, RedisCommandFrame& cmd);

            int LIndex(Context& ctx, RedisCommandFrame& cmd);
            int LInsert(Context& ctx, RedisCommandFrame& cmd);
            int LLen(Context& ctx, RedisCommandFrame& cmd);
            int LPop(Context& ctx, RedisCommandFrame& cmd);
            int LPush(Context& ctx, RedisCommandFrame& cmd);
            int LPushx(Context& ctx, RedisCommandFrame& cmd);
            int LRange(Context& ctx, RedisCommandFrame& cmd);
            int LRem(Context& ctx, RedisCommandFrame& cmd);
            int LSet(Context& ctx, RedisCommandFrame& cmd);
            int LTrim(Context& ctx, RedisCommandFrame& cmd);
            int RPop(Context& ctx, RedisCommandFrame& cmd);
            int RPopLPush(Context& ctx, RedisCommandFrame& cmd);
            int RPush(Context& ctx, RedisCommandFrame& cmd);
            int RPushx(Context& ctx, RedisCommandFrame& cmd);
            int BLPop(Context& ctx, RedisCommandFrame& cmd);
            int BRPop(Context& ctx, RedisCommandFrame& cmd);
            int BRPopLPush(Context& ctx, RedisCommandFrame& cmd);

            int HClear(Context& ctx, ValueObject& meta);
            int SClear(Context& ctx, ValueObject& meta);
            int ZClear(Context& ctx, ValueObject& meta);
            int LClear(Context& ctx, ValueObject& meta);

            int Eval(Context& ctx, RedisCommandFrame& cmd);
            int EvalSHA(Context& ctx, RedisCommandFrame& cmd);
            int Script(Context& ctx, RedisCommandFrame& cmd);

            int GeoAdd(Context& ctx, RedisCommandFrame& cmd);
            int GeoSearch(Context& ctx, RedisCommandFrame& cmd);

            int Auth(Context& ctx, RedisCommandFrame& cmd);

            int PFAdd(Context& ctx, RedisCommandFrame& cmd);
            int PFCount(Context& ctx, RedisCommandFrame& cmd);
            int PFMerge(Context& ctx, RedisCommandFrame& cmd);

            int DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& cmd);
            RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(RedisCommandFrame& cmd);
            void RenameCommand();

            friend class LUAInterpreter;
        public:
            Ardb();
            int Init(const std::string& conf_file);
            int Call(Context& ctx, RedisCommandFrame& cmd);
            int MergeOperation(KeyObject& key, ValueObject& val, uint16_t op, DataArray& args);
            int MergeOperands(uint16_t left, const DataArray& left_args, uint16_t& right, DataArray& right_args);
            const ArdbConfig& GetConf() const
            {
                return m_conf;
            }
            ~Ardb();
    };
    extern Ardb* g_db;
OP_NAMESPACE_END

#endif /* DB_HPP_ */
