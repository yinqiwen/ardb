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
#include "util/lru.hpp"
#include "command/lua_scripting.hpp"
#include "db/engine.hpp"
#include "statistics.hpp"
#include "context.hpp"
#include "config.hpp"
#include "logger.hpp"
#include <stack>
#include <sparsehash/dense_hash_map>

#define TTL_DB_NSMAESPACE "__TTL_DB__"

using namespace ardb::codec;

OP_NAMESPACE_BEGIN

    class ObjectIO;
    class ObjectBuffer;
    class Snapshot;
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
                    bool IsAllowedInScript() const;
                    bool IsWriteCommand() const;
            };
            struct RedisCommandHash
            {
                    size_t operator()(const std::string& t) const;
            };
            struct RedisCommandEqual
            {
                    bool operator()(const std::string& s1, const std::string& s2) const;
            };

            struct KeyLockGuard
            {
                    Context& ctx;
                    KeyPrefix lk;
                    bool lock;

                    KeyLockGuard(Context& cctx, const KeyObject& key, bool _lock = true);
                    ~KeyLockGuard();
            };
            struct KeysLockGuard
            {
                    Context& ctx;
                    KeyPrefixSet ks;
                    KeysLockGuard(Context& cctx, const KeyObjectArray& keys);
                    KeysLockGuard(Context& cctx, const KeyObject& key1, const KeyObject& key2);
                    ~KeysLockGuard();
            };

        private:
            Engine* m_engine;
            time_t m_starttime;
            bool m_loading_data;
            bool m_compacting_data;
            uint32 m_prepare_snapshot_num;  /* if the server is prepare saving snapshot, if > 0, the server would block all write command a while  */
            volatile uint32 m_write_caller_num;
            volatile uint32 m_db_caller_num;
            ThreadMutexLock m_write_latch;
            ArdbConfig m_conf;
            ThreadLocal<LUAInterpreter> m_lua;

            typedef TreeSet<KeyPrefix>::Type ExpireKeySet;
            SpinMutexLock m_expires_lock;
            ExpireKeySet m_expires;

            typedef google::dense_hash_map<std::string, RedisCommandHandlerSetting, RedisCommandHash, RedisCommandEqual> RedisCommandHandlerSettingTable;
            RedisCommandHandlerSettingTable m_settings;
            typedef TreeMap<KeyPrefix, ThreadMutexLock*>::Type LockTable;
            typedef std::stack<ThreadMutexLock*> LockPool;
            SpinMutexLock m_locking_keys_lock;
            LockTable m_locking_keys;
            LockPool m_lock_pool;

            SpinMutexLock m_redis_cursor_lock;
            typedef LRUCache<uint64, std::string> RedisCursorCache;
            uint64 m_redis_cursor_seed;
            RedisCursorCache m_redis_cursor_cache;

            typedef TreeMap<std::string, ContextSet>::Type PubSubChannelTable;
            SpinRWLock m_pubsub_lock;
            PubSubChannelTable m_pubsub_channels;
            PubSubChannelTable m_pubsub_patterns;

            SpinMutexLock m_watched_keys_lock;
            typedef TreeMap<KeyPrefix, ContextSet>::Type WatchedContextTable;
            WatchedContextTable* m_watched_ctxs;

            SpinMutexLock m_block_keys_lock;
            typedef TreeMap<KeyPrefix, ContextSet>::Type BlockedContextTable;
            typedef TreeSet<KeyPrefix>::Type ReadyKeySet;
            BlockedContextTable m_blocked_ctxs;
            ReadyKeySet* m_ready_keys;

            SpinRWLock m_monitors_lock;
            ContextSet* m_monitors;

            ThreadLocal<ClientIdSet> m_clients;
            ThreadLocal<ClientId> m_last_scan_clientid;
            SpinMutexLock m_clients_lock;
            ContextSet m_all_clients;

            SpinMutexLock m_restoring_lock;
            DataSet* m_restoring_nss;

            int64_t m_min_ttl;

            static void MigrateCoroTask(void* data);
            static void MigrateDBCoroTask(void* data);

            bool IsLoadingData();

            bool MarkRestoring(Context& ctx, bool enable);
            bool IsRestoring(Context& ctx, const Data& ns);

            void OpenWriteLatchByWriteCaller();
            void CloseWriteLatchByWriteCaller();
            void OpenWriteLatchAfterSnapshotPrepare();
            void CloseWriteLatchBeforeSnapshotPrepare();

            void SaveTTL(Context& ctx, const Data& ns, const std::string& key, int64 old_ttl, int64_t new_ttl);
            void ScanTTLDB();
            void FeedReplicationBacklog(Context& ctx,const Data& ns, RedisCommandFrame& cmd);
            void FeedMonitors(Context& ctx,const Data& ns, RedisCommandFrame& cmd);

            int WriteReply(Context& ctx, RedisReply* r, bool async);

            bool LockKey(const KeyPrefix& key, int wait_limit = -1);
            void UnlockKey(const KeyPrefix& key);
            void LockKeys(const KeyPrefixSet& key);
            void UnlockKeys(const KeyPrefixSet& key);

            Engine* GetEngine()
            {
                return m_engine;
            }
            int SetKeyValue(Context& ctx, const KeyObject& key, const ValueObject& val);
            int MergeKeyValue(Context& ctx, const KeyObject& key, uint16 op, const DataArray& args);
            int RemoveKey(Context& ctx, const KeyObject& key);
            int IteratorDel(Context& ctx, const KeyObject& key, Iterator* iter);
            int FlushDB(Context& ctx, const Data& ns);
            int FlushAll(Context& ctx);
            int CompactDB(Context& ctx, const Data& ns);
            int CompactAll(Context& ctx);

            bool GetLongFromProtocol(Context& ctx, const std::string& str, int64_t& v);

            int FindElementByRedisCursor(const std::string& cursor, std::string& element);
            uint64 GetNewRedisCursor(const std::string& element);

            int GetValueByPattern(Context& ctx, const Slice& pattern, Data& subst, Data& value);

            void TryPushSlowCommand(const RedisCommandFrame& cmd, uint64 micros);
            void GetSlowlog(Context& ctx, uint32 len);
            int ObjectLen(Context& ctx, KeyType type, const std::string& key);

            void FillInfoResponse(Context& ctx, const std::string& section, std::string& info);

            int SubscribeChannel(Context& ctx, const std::string& channel, bool is_pattern);
            int UnsubscribeChannel(Context& ctx, const std::string& channel, bool is_pattern, bool notify);
            int UnsubscribeAll(Context& ctx, bool is_pattern, bool notify);
            int PublishMessage(Context& ctx, const std::string& channel, const std::string& message);

            int SetString(Context& ctx, const std::string& key, const std::string& value, bool redis_compatible, int64_t px = -1, int8_t nx_xx = -1);

            int ListPop(Context& ctx, RedisCommandFrame& cmd, bool lock_key = true);
            int ListPush(Context& ctx, RedisCommandFrame& cmd, bool lock_key = true);

            bool AdjustMergeOp(uint16& op, DataArray& args);
            int MergeAppend(Context& ctx, const KeyObject& key, ValueObject& val, const std::string& append);
            int MergeIncrBy(Context& ctx, const KeyObject& key, ValueObject& val, int64_t inc);
            int MergeIncrByFloat(Context& ctx, const KeyObject& key, ValueObject& val, double inc);
            int MergeSet(Context& ctx, const KeyObject& key, ValueObject& val, uint16_t op, const Data& data, int64_t ttl);
            int MergeSetRange(Context& ctx, const KeyObject& key, ValueObject& val, int64_t offset, const std::string& range);
            int MergeHSet(Context& ctx, const KeyObject& key, ValueObject& value, uint16_t op, const Data& v);
            int MergeHIncrby(Context& ctx, const KeyObject& key, ValueObject& value, uint16_t op, const Data& v);
            int MergeExpire(Context& ctx, const KeyObject& key, ValueObject& meta, int64 ms);
            int MergeSetBit(Context& ctx, const KeyObject& key, ValueObject& meta, int64 offset, uint8 bit, uint8* oldbit);
            int MergePFAdd(Context& ctx, const KeyObject& key, ValueObject& value, const DataArray& ms, int* updated = NULL);

            bool CheckMeta(Context& ctx, const std::string& key, KeyType expected);
            bool CheckMeta(Context& ctx, const std::string& key, KeyType expected, ValueObject& meta);
            bool CheckMeta(Context& ctx, const KeyObject& key, KeyType expected, ValueObject& meta, bool fetch = true);

            int GetMinMax(Context& ctx, const KeyObject& key, ValueObject& meta, Iterator*& iter);
            int GetMinMax(Context& ctx, const KeyObject& key, KeyType ele_type, ValueObject& meta, Iterator*& iter);

            int DelKey(Context& ctx, const KeyObject& meta_key, Iterator*& iter);
            int DelKey(Context& ctx, const std::string& key);
            int DelKey(Context& ctx, const KeyObject& key);
            int MoveKey(Context& ctx, RedisCommandFrame& cmd);

            int HIterate(Context& ctx, RedisCommandFrame& cmd);
            int ZIterateByRank(Context& ctx, RedisCommandFrame& cmd);
            int ZIterateByScore(Context& ctx, RedisCommandFrame& cmd);
            int ZIterateByLex(Context& ctx, RedisCommandFrame& cmd);

            int WatchForKey(Context& ctx, const std::string& key);
            int UnwatchKeys(Context& ctx);
            int TouchWatchedKeysOnFlush(Context& ctx, const Data& ns);
            int DiscardTransaction(Context& ctx);

            int BlockForKeys(Context& ctx, const StringArray& keys, const std::string& target, uint32 timeout);
            static void AsyncUnblockKeysCallback(Channel* ch, void * data);
            int UnblockKeys(Context& ctx, bool use_lock = true);
            int WakeClientsBlockingOnList(Context& ctx);
            int SignalListAsReady(Context& ctx, const std::string& key);
            int ServeClientBlockedOnList(Context& ctx, const KeyPrefix& key, const std::string& value);

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
            int Pubsub(Context& ctx, RedisCommandFrame& cmd);

            int Slaveof(Context& ctx, RedisCommandFrame& cmd);
            int Sync(Context& ctx, RedisCommandFrame& cmd);
            int PSync(Context& ctx, RedisCommandFrame& cmd);
            int ReplConf(Context& ctx, RedisCommandFrame& cmd);

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

            int Eval(Context& ctx, RedisCommandFrame& cmd);
            int EvalSHA(Context& ctx, RedisCommandFrame& cmd);
            int Script(Context& ctx, RedisCommandFrame& cmd);

            int GeoAdd(Context& ctx, RedisCommandFrame& cmd);
            int GeoRadius(Context& ctx, RedisCommandFrame& cmd);
            int GeoRadiusByMember(Context& ctx, RedisCommandFrame& cmd);
            int GeoDist(Context& ctx, RedisCommandFrame& cmd);
            int GeoHash(Context& ctx, RedisCommandFrame& cmd);
            int GeoPos(Context& ctx, RedisCommandFrame& cmd);

            int Auth(Context& ctx, RedisCommandFrame& cmd);

            int PFAdd(Context& ctx, RedisCommandFrame& cmd);
            int PFCount(Context& ctx, RedisCommandFrame& cmd);
            int PFMerge(Context& ctx, RedisCommandFrame& cmd);

            int Monitor(Context& ctx, RedisCommandFrame& cmd);
            int Dump(Context& ctx, RedisCommandFrame& cmd);
            int Restore(Context& ctx, RedisCommandFrame& cmd);
            int Migrate(Context& ctx, RedisCommandFrame& cmd);
            int MigrateDB(Context& ctx, RedisCommandFrame& cmd);
            int RestoreDB(Context& ctx, RedisCommandFrame& cmd);
            int RestoreChunk(Context& ctx, RedisCommandFrame& cmd);
            int Debug(Context& ctx, RedisCommandFrame& cmd);
            int Touch(Context& ctx, RedisCommandFrame& cmd);

            int DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& cmd);
            RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(RedisCommandFrame& cmd);
            void RenameCommand();

            friend class LUAInterpreter;
            friend class ObjectIO;
            friend class ObjectBuffer;
            friend class Snapshot;
            friend class Master;
            friend class Slave;
        public:
            Ardb();
            int Init(const std::string& conf_file);
            int Repair(const std::string& dir);
            int Call(Context& ctx, RedisCommandFrame& cmd);
            int MergeOperation(const KeyObject& key, ValueObject& val, uint16_t op, DataArray& args);
            int MergeOperands(uint16_t left, const DataArray& left_args, uint16_t& right, DataArray& right_args);
            void AddExpiredKey(const Data& ns, const Data& key);
            void FeedReplicationDelOperation(Context& ctx,  const Data& ns, const std::string& key);
            int TouchWatchKey(Context& ctx, const KeyObject& key);
            void FreeClient(Context& ctx);
            void AddClient(Context& ctx);
            void ScanClients();
            int64 ScanExpiredKeys();
            const ArdbConfig& GetConf() const
            {
                return m_conf;
            }
            ArdbConfig& GetMutableConf()
            {
                return m_conf;
            }
            uint32 MaxOpenFiles();
            ~Ardb();
    };
    extern Ardb* g_db;
    extern const char* g_engine_name;
OP_NAMESPACE_END

#endif /* DB_HPP_ */
