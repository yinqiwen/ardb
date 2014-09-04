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
#include "engine/engine.hpp"
#include "channel/all_includes.hpp"
#include "command/lua_scripting.hpp"
#include "cache/cache.hpp"
#include "concurrent.hpp"
#include "options.hpp"
#include "iterator.hpp"
#include "engine/engine.hpp"
#include "codec.hpp"
#include "statistics.hpp"
#include "context.hpp"
#include "cron.hpp"
#include "config.hpp"
#include "logger.hpp"
#include "replication/master.hpp"
#include "replication/slave.hpp"
#include "util/redis_helper.hpp"

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
#define ERR_INVALID_HLL_TYPE -12
#define ERR_CORRUPTED_HLL_VALUE -13
#define ERR_STORAGE_ENGINE_INTERNAL -14
#define ERR_OVERLOAD -15
#define ERR_NOT_EXIST_IN_CACHE -16

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

#define ARDB_PROCESS_WITHOUT_REPLICATION 1
#define ARDB_PROCESS_REPL_WRITE 2
#define ARDB_PROCESS_FORCE_REPLICATION 4
#define ARDB_PROCESS_FEED_REPLICATION_ONLY 8
#define ARDB_PROCESS_TRANSC 16

#define SCRIPT_KILL_EVENT 1
#define SCRIPT_FLUSH_EVENT 2

using namespace ardb::codec;

#define CHECK_ARDB_RETURN_VALUE(reply, ret) do{\
    switch(ret){\
        case ERR_INVALID_ARGS: ardb::fill_error_reply(reply, "Invalid arguments."); return 0;\
        case ERR_INVALID_TYPE: ardb::fill_error_reply(reply, "Operation against a key holding the wrong kind of value."); return 0;\
        case ERR_INVALID_HLL_TYPE: ardb::fill_fix_error_reply(reply, "WRONGTYPE Key is not a valid HyperLogLog string value."); return 0;\
        case ERR_CORRUPTED_HLL_VALUE: ardb::fill_error_reply(reply, "INVALIDOBJ Corrupted HLL object detected."); return 0;\
        case ERR_STORAGE_ENGINE_INTERNAL: ardb::fill_error_reply(reply, "Storage engine internal error."); return 0;\
        default:break; \
    }\
}while(0)

OP_NAMESPACE_BEGIN

    class BatchWriteGuard
    {
        private:
            KeyValueEngine& m_engine;
            bool m_success;
            bool m_enable;
        public:
            BatchWriteGuard(KeyValueEngine& engine, bool enable = true) :
                    m_engine(engine), m_success(true), m_enable(enable)
            {
                if (m_enable)
                {
                    m_engine.BeginBatchWrite();
                }
            }
            void MarkFailed()
            {
                m_success = false;
            }
            bool Success()
            {
                return m_success;
            }
            ~BatchWriteGuard()
            {
                if (m_enable)
                {
                    if (m_success)
                    {
                        m_engine.CommitBatchWrite();
                    }
                    else
                    {
                        m_engine.DiscardBatchWrite();
                    }
                }
            }
    };

    class RedisRequestHandler;
    class ExpireCheck;
    class ConnectionTimeout;
    class CompactTask;
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
            };
        private:
            ChannelService* m_service;
            KeyValueEngineFactory& m_engine_factory;
            KeyValueEngine* m_engine;

            KeyLocker m_key_lock;

            ArdbConfig m_cfg;
            Properties m_cfg_props;
            SpinRWLock m_cfg_lock;

            L1Cache m_cache;
            CronManager m_cron;
            Statistics m_stat;

            typedef TreeMap<std::string, RedisCommandHandlerSetting>::Type RedisCommandHandlerSettingTable;
            RedisCommandHandlerSettingTable m_settings;

            ThreadLocal<LUAInterpreter> m_lua;

            ThreadLocal<RedisReplyPool> m_reply_pool;
            /*
             * Transction watched keys
             */
            typedef TreeMap<DBItemKey, ContextSet>::Type WatchedContextTable;
            WatchedContextTable* m_watched_ctx;
            SpinRWLock m_watched_keys_lock;

            typedef TreeMap<std::string, ContextSet>::Type PubsubContextTable;
            PubsubContextTable m_pubsub_channels;
            PubsubContextTable m_pubsub_patterns;
            SpinRWLock m_pubsub_ctx_lock;

            typedef TreeMap<DBItemKey, ContextDeque>::Type BlockContextTable;
            BlockContextTable m_block_context_table;
            SpinRWLock m_block_ctx_lock;

            ContextTable m_clients;
            SpinMutexLock m_clients_lock;

            void AddBlockKey(Context& ctx, const std::string& key);
            void ClearBlockKeys(Context& ctx);


            void TryPushSlowCommand(const RedisCommandFrame& cmd, uint64 micros);
            void GetSlowlog(Context& ctx, uint32 len);

            RedisDumpFile m_redis_dump;
            ArdbDumpFile m_ardb_dump;

            ReplBacklog m_repl_backlog;
            Master m_master;
            Slave m_slave;

            time_t m_starttime;
            bool m_compacting;
            time_t m_last_compact_start_time;
            uint64 m_last_compact_duration;

            SpinMutexLock m_cached_dbids_lock;
            DBIDSet m_cached_dbids;

            DataDumpFile& GetDataDumpFile();
            void FillInfoResponse(const std::string& section, std::string& info);

            int SubscribeChannel(Context& ctx, const std::string& channel, bool notify);
            int UnsubscribeChannel(Context& ctx, const std::string& channel, bool notify);
            int UnsubscribeAll(Context& ctx,  bool notify);
            int PSubscribeChannel(Context& ctx, const std::string& pattern, bool notify);
            int PUnsubscribeChannel(Context& ctx, const std::string& pattern, bool notify);
            int PUnsubscribeAll(Context& ctx,  bool notify);
            int PublishMessage(Context& ctx, const std::string& channel, const std::string& message);

            KeyValueEngine& GetKeyValueEngine();
            int SetRaw(Context& ctx, const Slice& key, const Slice& value);
            int GetRaw(Context& ctx, const Slice& key, std::string& value);
            int DelRaw(Context& ctx, const Slice& key);
            int SetKeyValue(Context& ctx, ValueObject& value);
            int GetKeyValue(Context& ctx, KeyObject& key, ValueObject* kv);
            int DelKeyValue(Context& ctx, KeyObject& key);
            int DeleteKey(Context& ctx, const Slice& key);
            Iterator* IteratorKeyValue(KeyObject& from, bool match_key);
            void IteratorSeek(Iterator* iter, KeyObject& target);

            void RewriteClientCommand(Context& ctx, RedisCommandFrame& cmd);

            bool GetDoubleValue(Context& ctx, const std::string& str, double& v);
            bool GetInt64Value(Context& ctx, const std::string& str, int64& v);

            int IncrDecrCommand(Context& ctx, const Slice& key, int64 v);
            int StringGet(Context& ctx, const std::string& key, ValueObject& value);

            int GetMetaValue(Context& ctx, const Slice& key, KeyType expected_type, ValueObject& v);

            int HashSet(Context& ctx, ValueObject& meta, const Data& field, Data& value);
            int HashMultiSet(Context& ctx, ValueObject& meta, DataMap& fs);
            int HashGet(Context& ctx, ValueObject& meta, Data& field, Data& value);
            int HashGet(Context& ctx, const std::string& key, const std::string& field, Data& v);
            int HashIter(Context& ctx, ValueObject& meta, const std::string& from, HashIterator& iter, bool readonly);
            int HashLen(Context& ctx, const Slice& key);
            int HashGetAll(Context& ctx, const Slice& key, RedisReply& r);

            int ZipListConvert(Context& ctx, ValueObject& meta);
            int ListIter(Context& ctx, ValueObject& meta, ListIterator& iter, bool reverse);
            int ListPop(Context& ctx, const std::string& key, bool lpop);
            int ListInsert(Context& ctx, const std::string& key, const std::string* match, const std::string& value,
                    bool head, bool abort_nonexist);
            int ListInsert(Context& ctx, ValueObject& meta, const std::string* match, const std::string& value,
                    bool head, bool abort_nonexist);
            int ListLen(Context& ctx, const Slice& key);
            int ListRange(Context& ctx, const Slice& key, int64 start, int64 end);
            bool WakeBlockList(Context& ctx, const Slice& key, const std::string& value);


            int SetLen(Context& ctx, const Slice& key);
            int SetMembers(Context& ctx, const Slice& key);
            int SetIter(Context& ctx, ValueObject& meta, Data& from, SetIterator& iter, bool readonly);
            int SetAdd(Context& ctx, ValueObject& meta, const std::string& value, bool& meta_change);
            int SetDiff(Context& ctx, const std::string& first, StringSet& keys, const std::string* store,
                    int64* count);
            int SetInter(Context& ctx, StringSet& keys, const std::string* store, int64* count);
            int SetUnion(Context& ctx, StringSet& keys, const std::string* store, int64* count);
            bool SetIsMember(Context& ctx, ValueObject& meta, Data& element);
            int GetSetMinMax(Context& ctx, ValueObject& meta, Data& min, Data& max);

            int ZSetDeleteElement(Context& ctx, ValueObject& meta, const Data& element,const Data& score);
            int ZSetScoreIter(Context& ctx, ValueObject& meta, const Data& from, ZSetIterator& iter, bool readonly);
            int ZSetValueIter(Context& ctx, ValueObject& meta, Data& from, ZSetIterator& iter, bool readonly);
            int ZSetAdd(Context& ctx, ValueObject& meta, const Data& element, const Data& score, Data* old_score);
            int ZSetScore(Context& ctx, ValueObject& meta, const Data& value, Data& score, Location* loc = NULL);
            int ZSetRankByScore(Context& ctx, ValueObject& meta, Data& score, uint32& rank);
            int ZSetRange(Context& ctx, const Slice& key, int64 start, int64 stop, bool withscores, bool reverse,
                    DataOperation op);
            int ZSetRangeByLex(Context& ctx, const std::string& key, const ZSetRangeByLexOptions& options,
                    DataOperation op);
            int ZSetRangeByScore(Context& ctx, ValueObject& meta, const ZRangeSpec& range,
                    ZSetRangeByScoreOptions& options, ZSetIterator*& iter);
            int ZSetRank(Context& ctx, const std::string& key, const std::string& value, bool reverse);
            int ZSetRangeByScore(Context& ctx, RedisCommandFrame& cmd);
            int ZSetRem(Context& ctx, ValueObject& meta, const std::string& value);
            int ZSetLen(Context& ctx, const Slice& key);

            int BitGet(Context& ctx, const std::string& key, uint64 offset);
            int BitsetIter(Context& ctx, ValueObject& meta, int64 index, BitsetIterator& iter);
            int BitOP(Context& ctx, const std::string& op, const SliceArray& keys, const std::string* targetkey);
            int BitsAnd(Context& ctx, const SliceArray& keys);

            int HyperloglogAdd(Context& ctx, const std::string& key, const SliceArray& members);
            int HyperloglogCountKey(Context& ctx, const std::string& key, uint64& v);
            int HyperloglogCount(Context& ctx, const StringArray& keys, uint64& card);
            int HyperloglogMerge(Context& ctx, const std::string& destkey, const StringArray& srckeys);

            int GetScript(const std::string& funacname, std::string& funcbody);
            int SaveScript(const std::string& funacname, const std::string& funcbody);
            int FlushScripts(Context& ctx);

            int WatchForKey(Context& ctx, const std::string& key);
            int UnwatchKeys(Context& ctx);
            int AbortWatchKey(Context& ctx, const std::string& key);

            int MatchValueByPattern(Context& ctx, const Slice& key_pattern, const Slice& value_pattern, Data& subst,
                    Data& value);
            int GetValueByPattern(Context& ctx, const Slice& pattern, Data& subst, Data& value, ValueObjectMap* meta_cache = NULL);
            int SortCommand(Context& ctx, const Slice& key, SortOptions& options, DataArray& values);

            int GetType(Context& ctx, const Slice& key, KeyType& type);

            int GenericGet(Context& ctx, const Slice& key, ValueObject& str, GenericGetOptions& options);
            int GenericSet(Context& ctx, const Slice& key, const Slice& value, GenericSetOptions& options);
            int GenericExpire(Context& ctx, const Slice& key, uint64 ms);
            int GenericTTL(Context& ctx, const Slice& key, uint64& ms);
            int KeysOperation(Context& ctx, const KeysOptions& options);

            int RenameString(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);
            int RenameSet(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);
            int RenameList(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);
            int RenameHash(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);
            int RenameZSet(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);
            int RenameBitset(Context& ctx, DBID srcdb, const std::string& srckey, DBID dstdb, const std::string& dstkey);

            int GeoSearchByOptions(Context& ctx, ValueObject& meta, GeoSearchOptions& options);

            int DoCompact(Context& ctx, DBID db, bool sync);
            int DoCompact(const std::string& start, const std::string& end);

            int FlushAllData(Context& ctx);
            int FlushDBData(Context& ctx);
            void GetAllDBIDSet(DBIDSet& ids);

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
            int BitopCount(Context& ctx, RedisCommandFrame& cmd);
            int SetBit(Context& ctx, RedisCommandFrame& cmd);
            int GetBit(Context& ctx, RedisCommandFrame& cmd);

            int HDel(Context& ctx, RedisCommandFrame& cmd);
            int HExists(Context& ctx, RedisCommandFrame& cmd);
            int HGet(Context& ctx, RedisCommandFrame& cmd);
            int HGetAll(Context& ctx, RedisCommandFrame& cmd);
            int HIncrby(Context& ctx, RedisCommandFrame& cmd);
            int HMIncrby(Context& ctx, RedisCommandFrame& cmd);
            int HIncrbyFloat(Context& ctx, RedisCommandFrame& cmd);
            int HKeys(Context& ctx, RedisCommandFrame& cmd);
            int HLen(Context& ctx, RedisCommandFrame& cmd);
            int HMGet(Context& ctx, RedisCommandFrame& cmd);
            int HMSet(Context& ctx, RedisCommandFrame& cmd);
            int HSet(Context& ctx, RedisCommandFrame& cmd);
            int HSetNX(Context& ctx, RedisCommandFrame& cmd);
            int HVals(Context& ctx, RedisCommandFrame& cmd);
            int HScan(Context& ctx, RedisCommandFrame& cmd);
            int HReplace(Context& ctx, RedisCommandFrame& cmd);

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
            int SReplace(Context& ctx, RedisCommandFrame& cmd);

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
            int BitClear(Context& ctx, ValueObject& meta);

            int Eval(Context& ctx, RedisCommandFrame& cmd);
            int EvalSHA(Context& ctx, RedisCommandFrame& cmd);
            int Script(Context& ctx, RedisCommandFrame& cmd);

            int GeoAdd(Context& ctx, RedisCommandFrame& cmd);
            int GeoSearch(Context& ctx, RedisCommandFrame& cmd);

            int Cache(Context& ctx, RedisCommandFrame& cmd);
            int Auth(Context& ctx, RedisCommandFrame& cmd);

            int PFAdd(Context& ctx, RedisCommandFrame& cmd);
            int PFCount(Context& ctx, RedisCommandFrame& cmd);
            int PFMerge(Context& ctx, RedisCommandFrame& cmd);

            int DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& cmd);
            RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(RedisCommandFrame& cmd);
            bool ParseConfig(const Properties& props);
            void RenameCommand();
            void FreeClientContext(Context& ctx);
            void AddClientContext(Context& ctx);
            RedisReplyPool& GetRedisReplyPool();
            friend class RedisRequestHandler;
            friend class LUAInterpreter;
            friend class Slave;
            friend class Master;
            friend class ReplBacklog;
            friend class RedisDumpFile;
            friend class ArdbDumpFile;
            friend class ZSetIterator;
            friend class ExpireCheck;
            friend class ConnectionTimeout;
            friend class CompactTask;
            friend class L1Cache;
        public:
            Ardb(KeyValueEngineFactory& factory);
            int Init(const ArdbConfig& cfg);
            void Start();
            ArdbConfig& GetConfig()
            {
                return m_cfg;
            }
            ChannelService& GetChannelService()
            {
                return *m_service;
            }
            Timer& GetTimer()
            {
                return m_service->GetTimer();
            }
            Statistics& GetStatistics()
            {
                return m_stat;
            }
            int Call(Context& ctx, RedisCommandFrame& cmd, int flags);
            static void WakeBlockedConnCallback(Channel* ch, void * data);
            ~Ardb();

    };
    extern Ardb* g_db;
OP_NAMESPACE_END

#endif /* DB_HPP_ */
