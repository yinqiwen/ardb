/*
 *Copyright (c) 2013-2016, yinqiwen <yinqiwen@gmail.com>
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
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <limits.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sstream>
#include "db.hpp"
#include "repl/repl.hpp"
#include "statistics.hpp"
#if defined __USE_LMDB__
#include "lmdb/lmdb_engine.hpp"
const char* ardb::g_engine_name = "lmdb";
#elif defined __USE_ROCKSDB__
#include "rocksdb/rocksdb_engine.hpp"
const char* ardb::g_engine_name ="rocksdb";
#elif defined __USE_FORESTDB__
#include "forestdb/forestdb_engine.hpp"
const char* ardb::g_engine_name ="forestdb";
#elif defined __USE_LEVELDB__
#include "leveldb/leveldb_engine.hpp"
const char* ardb::g_engine_name = "leveldb";
#elif defined __USE_WIREDTIGER__
#include "wiredtiger/wiredtiger_engine.hpp"
const char* ardb::g_engine_name ="wiredtiger";
#elif defined __USE_PERCONAFT__
#include "perconaft/perconaft_engine.hpp"
const char* ardb::g_engine_name ="perconaft";
#else
const char* ardb::g_engine_name = "unknown";
#endif

/* Command flags. Please check the command table defined in the redis.c file
 * for more information about the meaning of every flag. */
#define ARDB_CMD_WRITE 1                   /* "w" flag */
#define ARDB_CMD_READONLY 2                /* "r" flag */
#define ARDB_CMD_DENYOOM 4                 /* "m" flag */
#define ARDB_CMD_NOT_USED_1 8              /* no longer used flag */
#define ARDB_CMD_ADMIN 16                  /* "a" flag */
#define ARDB_CMD_PUBSUB 32                 /* "p" flag */
#define ARDB_CMD_NOSCRIPT  64              /* "s" flag */
#define ARDB_CMD_RANDOM 128                /* "R" flag */
#define ARDB_CMD_SORT_FOR_SCRIPT 256       /* "S" flag */
#define ARDB_CMD_LOADING 512               /* "l" flag */
#define ARDB_CMD_STALE 1024                /* "t" flag */
#define ARDB_CMD_SKIP_MONITOR 2048         /* "M" flag */
#define ARDB_CMD_ASKING 4096               /* "k" flag */
#define ARDB_CMD_FAST 8192                 /* "F" flag */

OP_NAMESPACE_BEGIN
    Ardb* g_db = NULL;
    Engine* g_engine = NULL;

    bool Ardb::RedisCommandHandlerSetting::IsAllowedInScript() const
    {
        return (flags & ARDB_CMD_NOSCRIPT) == 0;
    }
    bool Ardb::RedisCommandHandlerSetting::IsWriteCommand() const
    {
        return (flags & ARDB_CMD_WRITE) > 0;
    }

    size_t Ardb::RedisCommandHash::operator ()(const std::string& t) const
    {
        unsigned int hash = 5381;
        size_t len = t.size();
        const char* buf = t.data();
        while (len--)
            hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
        return hash;
    }
    bool Ardb::RedisCommandEqual::operator()(const std::string& s1, const std::string& s2) const
    {
        return strcasecmp(s1.c_str(), s2.c_str()) == 0 ? true : false;
    }

    Ardb::KeyLockGuard::KeyLockGuard(Context& cctx, const KeyObject& key, bool _lock)
            : ctx(cctx), lock(_lock)
    {
        if (lock)
        {
            ctx.keyslocked = true;
            lk.key = key.GetKey();
            lk.ns = key.GetNameSpace();
            g_db->LockKey(lk);
        }

    }

    Ardb::KeyLockGuard::~KeyLockGuard()
    {
        if (lock)
        {
            g_db->UnlockKey(lk);
            ctx.keyslocked = false;
        }
    }

    Ardb::KeysLockGuard::KeysLockGuard(Context& cctx, const KeyObjectArray& keys)
            : ctx(cctx)
    {
        ctx.keyslocked = true;
        for (size_t i = 0; i < keys.size(); i++)
        {
            KeyPrefix lk;
            lk.key = keys[i].GetKey();
            lk.ns = keys[i].GetNameSpace();
            ks.insert(lk);
        }
        g_db->LockKeys(ks);
    }
    Ardb::KeysLockGuard::KeysLockGuard(Context& cctx, const KeyObject& key1, const KeyObject& key2)
            : ctx(cctx)
    {
        KeyPrefix lk1, lk2;
        lk1.key = key1.GetKey();
        lk1.ns = key1.GetNameSpace();
        lk2.key = key2.GetKey();
        lk2.ns = key2.GetNameSpace();
        ks.insert(lk1);
        ks.insert(lk2);
        g_db->LockKeys(ks);
    }
    Ardb::KeysLockGuard::~KeysLockGuard()
    {
        g_db->UnlockKeys(ks);
        ctx.keyslocked = false;
    }

    static CostTrack g_cmd_cost_tracks[REDIS_CMD_MAX];

    Ardb::Ardb()
            : m_engine(NULL), m_starttime(0), m_loading_data(false), m_compacting_data(false), m_prepare_snapshot_num(
                    0), m_write_caller_num(0), m_db_caller_num(0), m_redis_cursor_seed(0), m_watched_ctxs(NULL), m_ready_keys(
                    NULL), m_monitors(
            NULL), m_restoring_nss(
            NULL), m_min_ttl(-1)
    {
        g_db = this;
        m_settings.set_empty_key("");
        m_settings.set_deleted_key("\n");

        struct RedisCommandHandlerSetting settingTable[] =
        {
        { "ping", REDIS_CMD_PING, &Ardb::Ping, 0, 0, "rtF", 0, 0, 0 },
        { "multi", REDIS_CMD_MULTI, &Ardb::Multi, 0, 0, "rsF", 0, 0, 0 },
        { "discard", REDIS_CMD_DISCARD, &Ardb::Discard, 0, 0, "rsF", 0, 0, 0 },
        { "exec", REDIS_CMD_EXEC, &Ardb::Exec, 0, 0, "sM", 0, 0, 0 },
        { "watch", REDIS_CMD_WATCH, &Ardb::Watch, 0, -1, "rsF", 0, 0, 0 },
        { "unwatch", REDIS_CMD_UNWATCH, &Ardb::UnWatch, 0, 0, "rsF", 0, 0, 0 },
        { "subscribe", REDIS_CMD_SUBSCRIBE, &Ardb::Subscribe, 1, -1, "rpslt", 0, 0, 0 },
        { "psubscribe", REDIS_CMD_PSUBSCRIBE, &Ardb::PSubscribe, 1, -1, "rpslt", 0, 0, 0 },
        { "unsubscribe", REDIS_CMD_UNSUBSCRIBE, &Ardb::UnSubscribe, 0, -1, "rpslt", 0, 0, 0 },
        { "punsubscribe", REDIS_CMD_PUNSUBSCRIBE, &Ardb::PUnSubscribe, 0, -1, "rpslt", 0, 0, 0 },
        { "publish", REDIS_CMD_PUBLISH, &Ardb::Publish, 2, 2, "pltrF", 0, 0, 0 },
        { "pubsub", REDIS_CMD_PUBSUB, &Ardb::Pubsub, 1, -1, "pltrF", 0, 0, 0 },
        { "info", REDIS_CMD_INFO, &Ardb::Info, 0, 1, "rlt", 0, 0, 0 },
        { "save", REDIS_CMD_SAVE, &Ardb::Save, 0, 1, "ars", 0, 0, 0 },
        { "bgsave", REDIS_CMD_BGSAVE, &Ardb::BGSave, 0, 1, "ar", 0, 0, 0 },
        { "import", REDIS_CMD_IMPORT, &Ardb::Import, 1, 1, "aws", 0, 0, 0 },
        { "lastsave", REDIS_CMD_LASTSAVE, &Ardb::LastSave, 0, 0, "r", 0, 0, 0 },
        { "slowlog", REDIS_CMD_SLOWLOG, &Ardb::SlowLog, 1, 2, "r", 0, 0, 0 },
        { "dbsize", REDIS_CMD_DBSIZE, &Ardb::DBSize, 0, 0, "r", 0, 0, 0 },
        { "config", REDIS_CMD_CONFIG, &Ardb::Config, 1, 3, "ar", 0, 0, 0 },
        { "client", REDIS_CMD_CLIENT, &Ardb::Client, 1, -1, "ar", 0, 0, 0 },
        { "flushdb", REDIS_CMD_FLUSHDB, &Ardb::FlushDB, 0, 0, "w", 0, 0, 0 },
        { "flushall", REDIS_CMD_FLUSHALL, &Ardb::FlushAll, 0, 0, "w", 0, 0, 0 },
        { "compactdb", REDIS_CMD_COMPACTDB, &Ardb::CompactDB, 0, 0, "ar", 0, 0, 0 },
        { "compactall", REDIS_CMD_COMPACTALL, &Ardb::CompactAll, 0, 0, "ar", 0, 0, 0 },
        { "time", REDIS_CMD_TIME, &Ardb::Time, 0, 0, "ar", 0, 0, 0 },
        { "echo", REDIS_CMD_ECHO, &Ardb::Echo, 1, 1, "r", 0, 0, 0 },
        { "quit", REDIS_CMD_QUIT, &Ardb::Quit, 0, 0, "rs", 0, 0, 0 },
        { "shutdown", REDIS_CMD_SHUTDOWN, &Ardb::Shutdown, 0, 1, "arlt", 0, 0, 0 },
        { "slaveof", REDIS_CMD_SLAVEOF, &Ardb::Slaveof, 2, -1, "ast", 0, 0, 0 },
        { "replconf", REDIS_CMD_REPLCONF, &Ardb::ReplConf, 0, -1, "arslt", 0, 0, 0 },
        { "sync", REDIS_CMD_SYNC, &Ardb::Sync, 0, 2, "ars", 0, 0, 0 },
        { "psync", REDIS_CMD_PSYNC, &Ardb::PSync, 2, -1, "ars", 0, 0, 0 },
        { "select", REDIS_CMD_SELECT, &Ardb::Select, 1, 1, "r", 0, 0, 0 },
        { "append", REDIS_CMD_APPEND, &Ardb::Append, 2, 2, "w", 0, 0, 0 },
        { "append2", REDIS_CMD_APPEND2, &Ardb::Append, 2, 2, "w", 0, 0, 0 },
        { "get", REDIS_CMD_GET, &Ardb::Get, 1, 1, "rF", 0, 0, 0 },
        { "set", REDIS_CMD_SET, &Ardb::Set, 2, 7, "w", 0, 0, 0 },
        { "set2", REDIS_CMD_SET2, &Ardb::Set, 2, 7, "w", 0, 0, 0 },
        { "del", REDIS_CMD_DEL, &Ardb::Del, 1, -1, "w", 0, 0, 0 },
        { "exists", REDIS_CMD_EXISTS, &Ardb::Exists, 1, 1, "r", 0, 0, 0 },
        { "expire", REDIS_CMD_EXPIRE, &Ardb::Expire, 2, 2, "w", 0, 0, 0 },
        { "pexpire", REDIS_CMD_PEXPIRE, &Ardb::PExpire, 2, 2, "w", 0, 0, 0 },
        { "expireat", REDIS_CMD_EXPIREAT, &Ardb::Expireat, 2, 2, "w", 0, 0, 0 },
        { "pexpireat", REDIS_CMD_PEXPIREAT, &Ardb::PExpireat, 2, 2, "w", 0, 0, 0 },
        { "persist", REDIS_CMD_PERSIST, &Ardb::Persist, 1, 1, "w", 1, 0, 0 },
        { "ttl", REDIS_CMD_TTL, &Ardb::TTL, 1, 1, "r", 0, 0, 0 },
        { "pttl", REDIS_CMD_PTTL, &Ardb::PTTL, 1, 1, "r", 0, 0, 0 },
        { "type", REDIS_CMD_TYPE, &Ardb::Type, 1, 1, "r", 0, 0, 0 },
        { "bitcount", REDIS_CMD_BITCOUNT, &Ardb::Bitcount, 1, 3, "r", 0, 0, 0 },
        { "bitop", REDIS_CMD_BITOP, &Ardb::Bitop, 3, -1, "w", 1, 0, 0 },
        { "bitopcount", REDIS_CMD_BITOPCUNT, &Ardb::BitopCount, 2, -1, "r", 0, 0, 0 },
        { "decr", REDIS_CMD_DECR, &Ardb::Decr, 1, 1, "w", 1, 0, 0 },
        { "decr2", REDIS_CMD_DECR2, &Ardb::Decr, 1, 1, "w", 1, 0, 0 },
        { "decrby", REDIS_CMD_DECRBY, &Ardb::Decrby, 2, 2, "w", 1, 0, 0 },
        { "decrby2", REDIS_CMD_DECRBY2, &Ardb::Decrby, 2, 2, "w", 1, 0, 0 },
        { "getbit", REDIS_CMD_GETBIT, &Ardb::GetBit, 2, 2, "r", 0, 0, 0 },
        { "getrange", REDIS_CMD_GETRANGE, &Ardb::GetRange, 3, 3, "r", 0, 0, 0 },
        { "getset", REDIS_CMD_GETSET, &Ardb::GetSet, 2, 2, "w", 1, 0, 0 },
        { "incr", REDIS_CMD_INCR, &Ardb::Incr, 1, 1, "w", 1, 0, 0 },
        { "incr2", REDIS_CMD_INCR2, &Ardb::Incr, 1, 1, "w", 1, 0, 0 },
        { "incrby", REDIS_CMD_INCRBY, &Ardb::Incrby, 2, 2, "w", 1, 0, 0 },
        { "incrby2", REDIS_CMD_INCRBY2, &Ardb::Incrby, 2, 2, "w", 1, 0, 0 },
        { "incrbyfloat", REDIS_CMD_INCRBYFLOAT, &Ardb::IncrbyFloat, 2, 2, "w", 0, 0, 0 },
        { "incrbyfloat2", REDIS_CMD_INCRBYFLOAT2, &Ardb::IncrbyFloat, 2, 2, "w", 0, 0, 0 },
        { "mget", REDIS_CMD_MGET, &Ardb::MGet, 1, -1, "r", 0, 0, 0 },
        { "mset", REDIS_CMD_MSET, &Ardb::MSet, 2, -1, "w", 0, 0, 0 },
        { "mset2", REDIS_CMD_MSET2, &Ardb::MSet, 2, -1, "w", 0, 0, 0 },
        { "msetnx", REDIS_CMD_MSETNX, &Ardb::MSetNX, 2, -1, "w", 0, 0, 0 },
        { "msetnx2", REDIS_CMD_MSETNX2, &Ardb::MSetNX, 2, -1, "w", 0, 0, 0 },
        { "psetex", REDIS_CMD_PSETEX, &Ardb::PSetEX, 3, 3, "w", 0, 0, 0 },
        { "setbit", REDIS_CMD_SETBIT, &Ardb::SetBit, 3, 3, "w", 0, 0, 0 },
        { "setbit2", REDIS_CMD_SETBIT2, &Ardb::SetBit, 3, 3, "w", 0, 0, 0 },
        { "setex", REDIS_CMD_SETEX, &Ardb::SetEX, 3, 3, "w", 0, 0, 0 },
        { "setnx", REDIS_CMD_SETNX, &Ardb::SetNX, 2, 2, "w", 0, 0, 0 },
        { "setnx2", REDIS_CMD_SETNX2, &Ardb::SetNX, 2, 2, "w", 0, 0, 0 },
        { "setrange", REDIS_CMD_SETRANGE, &Ardb::SetRange, 3, 3, "w", 0, 0, 0 },
        { "setrange2", REDIS_CMD_SETRANGE2, &Ardb::SetRange, 3, 3, "w", 0, 0, 0 },
        { "strlen", REDIS_CMD_STRLEN, &Ardb::Strlen, 1, 1, "r", 0, 0, 0 },
        { "hdel", REDIS_CMD_HDEL, &Ardb::HDel, 2, -1, "w", 0, 0, 0 },
        { "hdel2", REDIS_CMD_HDEL2, &Ardb::HDel, 2, -1, "w", 0, 0, 0 },
        { "hexists", REDIS_CMD_HEXISTS, &Ardb::HExists, 2, 2, "r", 0, 0, 0 },
        { "hget", REDIS_CMD_HGET, &Ardb::HGet, 2, 2, "r", 0, 0, 0 },
        { "hgetall", REDIS_CMD_HGETALL, &Ardb::HGetAll, 1, 1, "r", 0, 0, 0 },
        { "hincrby", REDIS_CMD_HINCR, &Ardb::HIncrby, 3, 3, "w", 0, 0, 0 },
        { "hincrby2", REDIS_CMD_HINCR2, &Ardb::HIncrby, 3, 3, "w", 0, 0, 0 },
        { "hincrbyfloat", REDIS_CMD_HINCRBYFLOAT, &Ardb::HIncrbyFloat, 3, 3, "w", 0, 0, 0 },
        { "hincrbyfloat2", REDIS_CMD_HINCRBYFLOAT2, &Ardb::HIncrbyFloat, 3, 3, "w", 0, 0, 0 },
        { "hkeys", REDIS_CMD_HKEYS, &Ardb::HKeys, 1, 1, "r", 0, 0, 0 },
        { "hlen", REDIS_CMD_HLEN, &Ardb::HLen, 1, 1, "r", 0, 0, 0 },
        { "hvals", REDIS_CMD_HVALS, &Ardb::HVals, 1, 1, "r", 0, 0, 0 },
        { "hmget", REDIS_CMD_HMGET, &Ardb::HMGet, 2, -1, "r", 0, 0, 0 },
        { "hset", REDIS_CMD_HSET, &Ardb::HSet, 3, -1, "w", 0, 0, 0 },
        { "hset2", REDIS_CMD_HSET2, &Ardb::HSet, 3, -1, "w", 0, 0, 0 },
        { "hsetnx", REDIS_CMD_HSETNX, &Ardb::HSetNX, 3, 3, "w", 0, 0, 0 },
        { "hsetnx2", REDIS_CMD_HSETNX2, &Ardb::HSetNX, 3, 3, "w", 0, 0, 0 },
        { "hmset", REDIS_CMD_HMSET, &Ardb::HMSet, 3, -1, "w", 0, 0, 0 },
        { "hmset2", REDIS_CMD_HMSET2, &Ardb::HMSet, 3, -1, "w", 0, 0, 0 },
        { "hscan", REDIS_CMD_HSCAN, &Ardb::HScan, 2, 6, "r", 0, 0, 0 },
        { "scard", REDIS_CMD_SCARD, &Ardb::SCard, 1, 1, "r", 0, 0, 0 },
        { "sadd", REDIS_CMD_SADD, &Ardb::SAdd, 2, -1, "w", 0, 0, 0 },
        { "sadd2", REDIS_CMD_SADD2, &Ardb::SAdd, 2, -1, "w", 0, 0, 0 },
        { "sdiff", REDIS_CMD_SDIFF, &Ardb::SDiff, 2, -1, "r", 0, 0, 0 },
        { "sdiffcount", REDIS_CMD_SDIFFCOUNT, &Ardb::SDiffCount, 2, -1, "r", 0, 0, 0 },
        { "sdiffstore", REDIS_CMD_SDIFFSTORE, &Ardb::SDiffStore, 3, -1, "w", 0, 0, 0 },
        { "sinter", REDIS_CMD_SINTER, &Ardb::SInter, 2, -1, "r", 0, 0, 0 },
        { "sintercount", REDIS_CMD_SINTERCOUNT, &Ardb::SInterCount, 2, -1, "r", 0, 0, 0 },
        { "sinterstore", REDIS_CMD_SINTERSTORE, &Ardb::SInterStore, 3, -1, "w", 0, 0, 0 },
        { "sismember", REDIS_CMD_SISMEMBER, &Ardb::SIsMember, 2, 2, "r", 0, 0, 0 },
        { "smembers", REDIS_CMD_SMEMBERS, &Ardb::SMembers, 1, 1, "r", 0, 0, 0 },
        { "smove", REDIS_CMD_SMOVE, &Ardb::SMove, 3, 3, "w", 0, 0, 0 },
        { "spop", REDIS_CMD_SPOP, &Ardb::SPop, 1, 2, "wR", 0, 0, 0 },
        { "srandmember", REDIS_CMD_SRANMEMEBER, &Ardb::SRandMember, 1, 2, "rR", 0, 0, 0 },
        { "srem", REDIS_CMD_SREM, &Ardb::SRem, 2, -1, "w", 1, 0, 0 },
        { "srem2", REDIS_CMD_SREM2, &Ardb::SRem, 2, -1, "w", 1, 0, 0 },
        { "sunion", REDIS_CMD_SUNION, &Ardb::SUnion, 2, -1, "r", 0, 0, 0 },
        { "sunionstore", REDIS_CMD_SUNIONSTORE, &Ardb::SUnionStore, 3, -1, "w", 0, 0, 0 },
        { "sunioncount", REDIS_CMD_SUNIONCOUNT, &Ardb::SUnionCount, 2, -1, "r", 0, 0, 0 },
        { "sscan", REDIS_CMD_SSCAN, &Ardb::SScan, 2, 6, "r", 0, 0, 0 },
        { "zadd", REDIS_CMD_ZADD, &Ardb::ZAdd, 3, -1, "w", 0, 0, 0 },
        { "zcard", REDIS_CMD_ZCARD, &Ardb::ZCard, 1, 1, "r", 0, 0, 0 },
        { "zcount", REDIS_CMD_ZCOUNT, &Ardb::ZCount, 3, 3, "r", 0, 0, 0 },
        { "zincrby", REDIS_CMD_ZINCRBY, &Ardb::ZIncrby, 3, 3, "w", 0, 0, 0 },
        { "zrange", REDIS_CMD_ZRANGE, &Ardb::ZRange, 3, 4, "r", 0, 0, 0 },
        { "zrangebyscore", REDIS_CMD_ZRANGEBYSCORE, &Ardb::ZRangeByScore, 3, 7, "r", 0, 0, 0 },
        { "zrank", REDIS_CMD_ZRANK, &Ardb::ZRank, 2, 2, "r", 0, 0, 0 },
        { "zrem", REDIS_CMD_ZREM, &Ardb::ZRem, 2, -1, "w", 0, 0, 0 },
        { "zremrangebyrank", REDIS_CMD_ZREMRANGEBYRANK, &Ardb::ZRemRangeByRank, 3, 3, "w", 0, 0, 0 },
        { "zremrangebyscore", REDIS_CMD_ZREMRANGEBYSCORE, &Ardb::ZRemRangeByScore, 3, 3, "w", 0, 0, 0 },
        { "zrevrange", REDIS_CMD_ZREVRANGE, &Ardb::ZRevRange, 3, 4, "r", 0, 0, 0 },
        { "zrevrangebyscore", REDIS_CMD_ZREVRANGEBYSCORE, &Ardb::ZRevRangeByScore, 3, 7, "r", 0, 0, 0 },
        { "zinterstore", REDIS_CMD_ZINTERSTORE, &Ardb::ZInterStore, 3, -1, "w", 0, 0, 0 },
        { "zunionstore", REDIS_CMD_ZUNIONSTORE, &Ardb::ZUnionStore, 3, -1, "w", 0, 0, 0 },
        { "zrevrank", REDIS_CMD_ZREVRANK, &Ardb::ZRevRank, 2, 2, "r", 0, 0, 0 },
        { "zscore", REDIS_CMD_ZSCORE, &Ardb::ZScore, 2, 2, "r", 0, 0, 0 },
        { "zscan", REDIS_CMD_ZSCAN, &Ardb::ZScan, 2, 6, "r", 0, 0, 0 },
        { "zlexcount", REDIS_CMD_ZLEXCOUNT, &Ardb::ZLexCount, 3, 3, "r", 0, 0, 0 },
        { "zrangebylex", REDIS_CMD_ZRANGEBYLEX, &Ardb::ZRangeByLex, 3, 6, "r", 0, 0, 0 },
        { "zrevrangebylex", REDIS_CMD_ZREVRANGEBYLEX, &Ardb::ZRangeByLex, 3, 6, "r", 0, 0, 0 },
        { "zremrangebylex", REDIS_CMD_ZREMRANGEBYLEX, &Ardb::ZRemRangeByLex, 3, 3, "w", 0, 0, 0 },
        { "lindex", REDIS_CMD_LINDEX, &Ardb::LIndex, 2, 2, "r", 0, 0, 0 },
        { "linsert", REDIS_CMD_LINSERT, &Ardb::LInsert, 4, 4, "w", 0, 0, 0 },
        { "llen", REDIS_CMD_LLEN, &Ardb::LLen, 1, 1, "r", 0, 0, 0 },
        { "lpop", REDIS_CMD_LPOP, &Ardb::LPop, 1, 1, "w", 0, 0, 0 },
        { "lpush", REDIS_CMD_LPUSH, &Ardb::LPush, 2, -1, "w", 0, 0, 0 },
        { "lpushx", REDIS_CMD_LPUSHX, &Ardb::LPushx, 2, 2, "w", 0, 0, 0 },
        { "lrange", REDIS_CMD_LRANGE, &Ardb::LRange, 3, 3, "r", 0, 0, 0 },
        { "lrem", REDIS_CMD_LREM, &Ardb::LRem, 3, 3, "w", 0, 0, 0 },
        { "lset", REDIS_CMD_LSET, &Ardb::LSet, 3, 3, "w", 0, 0, 0 },
        { "ltrim", REDIS_CMD_LTRIM, &Ardb::LTrim, 3, 3, "w", 0, 0, 0 },
        { "rpop", REDIS_CMD_RPOP, &Ardb::RPop, 1, 1, "w", 0, 0, 0 },
        { "rpush", REDIS_CMD_RPUSH, &Ardb::RPush, 2, -1, "w", 0, 0, 0 },
        { "rpushx", REDIS_CMD_RPUSHX, &Ardb::RPushx, 2, 2, "w", 0, 0, 0 },
        { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Ardb::RPopLPush, 2, 2, "w", 0, 0, 0 },
        { "blpop", REDIS_CMD_BLPOP, &Ardb::BLPop, 2, -1, "ws", 0, 0, 0 },
        { "brpop", REDIS_CMD_BRPOP, &Ardb::BRPop, 2, -1, "ws", 0, 0, 0 },
        { "brpoplpush", REDIS_CMD_BRPOPLPUSH, &Ardb::BRPopLPush, 3, 3, "ws", 0, 0, 0 },
        { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Ardb::RPopLPush, 2, 2, "w", 0, 0, 0 },
        { "move", REDIS_CMD_MOVE, &Ardb::Move, 2, 2, "w", 0, 0, 0 },
        { "rename", REDIS_CMD_RENAME, &Ardb::Rename, 2, 2, "w", 0, 0, 0 },
        { "renamenx", REDIS_CMD_RENAMENX, &Ardb::RenameNX, 2, 2, "w", 0, 0, 0 },
        { "sort", REDIS_CMD_SORT, &Ardb::Sort, 1, -1, "w", 0, 0, 0 },
        { "keys", REDIS_CMD_KEYS, &Ardb::Keys, 1, 6, "r", 0, 0, 0 },
        { "keyscount", REDIS_CMD_KEYSCOUNT, &Ardb::KeysCount, 1, 6, "r", 0, 0, 0 },
        { "eval", REDIS_CMD_EVAL, &Ardb::Eval, 2, -1, "s", 0, 0, 0 },
        { "evalsha", REDIS_CMD_EVALSHA, &Ardb::EvalSHA, 2, -1, "s", 0, 0, 0 },
        { "script", REDIS_CMD_SCRIPT, &Ardb::Script, 1, -1, "rs", 0, 0, 0 },
        { "randomkey", REDIS_CMD_RANDOMKEY, &Ardb::Randomkey, 0, 0, "r", 0, 0, 0 },
        { "scan", REDIS_CMD_SCAN, &Ardb::Scan, 1, 5, "r", 0, 0, 0 },
        { "geoadd", REDIS_CMD_GEO_ADD, &Ardb::GeoAdd, 4, -1, "w", 0, 0, 0 },
        { "georadius", REDIS_CMD_GEO_RADIUS, &Ardb::GeoRadius, 5, -1, "w", 0, 0, 0 },
        { "georadiusbymember", REDIS_CMD_GEO_RADIUSBYMEMBER, &Ardb::GeoRadiusByMember, 4, 10, "w", 0, 0, 0 },
        { "geohash", REDIS_CMD_GEO_HASH, &Ardb::GeoHash, 2, -1, "r", 0, 0, 0 },
        { "geodist", REDIS_CMD_GEO_DIST, &Ardb::GeoDist, 3, 4, "r", 0, 0, 0 },
        { "geopos", REDIS_CMD_GEO_POS, &Ardb::GeoPos, 2, -1, "r", 0, 0, 0 },
        { "auth", REDIS_CMD_AUTH, &Ardb::Auth, 1, 1, "rsltF", 0, 0, 0 },
        { "pfadd", REDIS_CMD_PFADD, &Ardb::PFAdd, 2, -1, "w", 0, 0, 0 },
        { "pfadd2", REDIS_CMD_PFADD2, &Ardb::PFAdd, 2, -1, "w", 0, 0, 0 },
        { "pfcount", REDIS_CMD_PFCOUNT, &Ardb::PFCount, 1, -1, "r", 0, 0, 0 },
        { "pfmerge", REDIS_CMD_PFMERGE, &Ardb::PFMerge, 2, -1, "w", 0, 0, 0 },
        { "dump", REDIS_CMD_DUMP, &Ardb::Dump, 1, 1, "r", 0, 0, 0 },
        { "restore", REDIS_CMD_RESTORE, &Ardb::Restore, 3, 4, "w", 0, 0, 0 },
        { "migrate", REDIS_CMD_MIGRATE, &Ardb::Migrate, 5, -1, "w", 0, 0, 0 },
        { "migratedb", REDIS_CMD_MIGRATEDB, &Ardb::MigrateDB, 4, 4, "w", 0, 0, 0 },
        { "restorechunk", REDIS_CMD_RESTORECHUNK, &Ardb::RestoreChunk, 1, 1, "wl", 0, 0, 0 },
        { "restoredb", REDIS_CMD_RESTOREDB, &Ardb::RestoreDB, 1, 1, "wl", 0, 0, 0 },
        { "monitor", REDIS_CMD_MONITOR, &Ardb::Monitor, 0, 0, "ars", 0, 0, 0 },
        { "debug", REDIS_CMD_DEBUG, &Ardb::Debug, 2, -1, "ars", 0, 0, 0 },
        { "touch", REDIS_CMD_TOUCH, &Ardb::Touch, 1, -2, "rF", 0, 0, 0 },};

        CostRanges cmdstat_ranges;
        cmdstat_ranges.push_back(CostRange(0, 1000));
        cmdstat_ranges.push_back(CostRange(1000, 5000));
        cmdstat_ranges.push_back(CostRange(5000, 10000));
        cmdstat_ranges.push_back(CostRange(10000, 20000));
        cmdstat_ranges.push_back(CostRange(20000, 50000));
        cmdstat_ranges.push_back(CostRange(50000, 100000));
        cmdstat_ranges.push_back(CostRange(100000, 200000));
        cmdstat_ranges.push_back(CostRange(200000, 500000));
        cmdstat_ranges.push_back(CostRange(500000, 1000000));
        cmdstat_ranges.push_back(CostRange(1000000, UINT64_MAX));
        uint32 arraylen = arraysize(settingTable);
        for (uint32 i = 0; i < arraylen; i++)
        {
            settingTable[i].flags = 0;
            const char* f = settingTable[i].sflags;

            CostTrack& cost_track = g_cmd_cost_tracks[settingTable[i].type];
            cost_track.dump_flags = STAT_DUMP_INFO_CMD | STAT_DUMP_PERIOD | STAT_DUMP_PERIOD_CLEAR;
            cost_track.name = settingTable[i].name;

            cost_track.SetCostRanges(cmdstat_ranges);
            Statistics::GetSingleton().AddTrack(&cost_track);

            while (*f != '\0')
            {
                switch (*f)
                {
                    case 'w':
                        settingTable[i].flags |= ARDB_CMD_WRITE;
                        break;
                    case 'r':
                        settingTable[i].flags |= ARDB_CMD_READONLY;
                        break;
                    case 'a':
                        settingTable[i].flags |= ARDB_CMD_ADMIN;
                        break;
                    case 'p':
                        settingTable[i].flags |= ARDB_CMD_PUBSUB;
                        break;
                    case 's':
                        settingTable[i].flags |= ARDB_CMD_NOSCRIPT;
                        break;
                    case 'R':
                        settingTable[i].flags |= ARDB_CMD_RANDOM;
                        break;
                    case 'S':
                        settingTable[i].flags |= ARDB_CMD_SORT_FOR_SCRIPT;
                        break;
                    case 'l':
                        settingTable[i].flags |= ARDB_CMD_LOADING;
                        break;
                    case 't':
                        settingTable[i].flags |= ARDB_CMD_STALE;
                        break;
                    case 'M':
                        settingTable[i].flags |= ARDB_CMD_SKIP_MONITOR;
                        break;
                    case 'k':
                        settingTable[i].flags |= ARDB_CMD_ASKING;
                        break;
                    case 'F':
                        settingTable[i].flags |= ARDB_CMD_FAST;
                        break;
                    default:
                        break;
                }
                f++;
            }
            m_settings[settingTable[i].name] = settingTable[i];
        }
    }

    Ardb::~Ardb()
    {
        DELETE(m_engine);
        DELETE(m_ready_keys);
        DELETE(m_watched_ctxs);
        ArdbLogger::DestroyDefaultLogger();
    }

    static void daemonize(void)
    {
        int fd;

        if (fork() != 0)
        {
            exit(0); /* parent exits */
        }
        setsid(); /* create a new session */

        if ((fd = open("/dev/null", O_RDWR, 0)) != -1)
        {
            dup2(fd, STDIN_FILENO);
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            if (fd > STDERR_FILENO)
            {
                close(fd);
            }
        }
    }

    static int signal_setting()
    {
        signal(SIGPIPE, SIG_IGN);
        return 0;
    }

    static Engine* create_engine()
    {
        Engine* engine = NULL;
#if defined __USE_LMDB__
        NEW(engine, LMDBEngine);
#elif defined __USE_ROCKSDB__
        NEW(engine, RocksDBEngine);
#elif defined __USE_LEVELDB__
        NEW(engine, LevelDBEngine);
#elif defined __USE_FORESTDB__
        NEW(engine, ForestDBEngine);
#elif defined __USE_WIREDTIGER__
        NEW(engine, WiredTigerEngine);
#elif defined __USE_PERCONAFT__
        NEW(engine, PerconaFTEngine);
#else
        ERROR_LOG("Unsupported storage engine specified at compile time.");
        return NULL;
#endif
        return engine;
    }

    uint32 Ardb::MaxOpenFiles()
    {
        /*
         * 1000 is reserved open files in Ardb(pipes/logs/dump/etc.)
         */
        uint32 maxfiles = g_db->GetConf().max_clients + 1000;
        if (g_engine->MaxOpenFiles() > 0)
        {
            maxfiles += g_engine->MaxOpenFiles();
        }
        return maxfiles;
    }

    int Ardb::Init(const std::string& conf_file)
    {
        Properties props;
        if (!conf_file.empty())
        {
            if (!parse_conf_file(conf_file, props, " "))
            {
                printf("Error: Failed to parse conf file:%s\n", conf_file.c_str());
                return -1;
            }
        }

        if (!m_conf.Parse(props))
        {
            printf("Failed to parse config file:%s\n", conf_file.c_str());
            return -1;
        }
        if (m_conf.daemonize && !m_conf.servers.empty())
        {
            daemonize();
        }
        signal_setting();

        RenameCommand();

        /*
         * save pid into file
         */
        if (!m_conf.pidfile.empty())
        {
            char tmp[200];
            sprintf(tmp, "%d", getpid());
            std::string content = tmp;
            file_write_content(m_conf.pidfile, content);
        }
        chdir(GetConf().home.c_str());
        ArdbLogger::InitDefaultLogger(m_conf.loglevel, m_conf.logfile);

        std::string dbdir = GetConf().data_base_path + "/" + g_engine_name;
        make_dir(dbdir);
        int err = 0;
        m_engine = create_engine();
        if (NULL == m_engine)
        {
            return -1;
        }
        std::string options_key = g_engine_name;
        options_key.append(".options");
        std::string options_value;
        conf_get_string(props, options_key, options_value);
        err = m_engine->Init(dbdir, options_value);
        if (0 != err)
        {
            DELETE(m_engine);
            ERROR_LOG("Failed to init database engine:%s.", g_engine_name);
            return -1;
        }
        m_starttime = time(NULL);
        g_engine = m_engine;
        INFO_LOG("Ardb init engine:%s success.", g_engine_name);
        return 0;
    }

    int Ardb::Repair(const std::string& dir)
    {
        m_engine = create_engine();
        if (NULL == m_engine)
        {
            return -1;
        }
        return m_engine->Repair(dir);
    }

    void Ardb::RenameCommand()
    {
        StringStringMap::const_iterator it = GetConf().rename_commands.begin();
        while (it != GetConf().rename_commands.end())
        {
            std::string cmd = string_tolower(it->first);
            RedisCommandHandlerSettingTable::iterator found = m_settings.find(cmd);
            if (found != m_settings.end())
            {
                RedisCommandHandlerSetting setting = found->second;
                m_settings.erase(found);
                m_settings[it->second] = setting;
            }
            it++;
        }
    }

    void Ardb::OpenWriteLatchByWriteCaller()
    {
        LockGuard<ThreadMutexLock> guard(m_write_latch);
        while (m_prepare_snapshot_num > 0)
        {
            m_write_latch.Wait(1);
        }
        atomic_add_uint32(&m_write_caller_num, 1);
    }

    void Ardb::CloseWriteLatchByWriteCaller()
    {
        atomic_sub_uint32(&m_write_caller_num, 1);
    }

    void Ardb::OpenWriteLatchAfterSnapshotPrepare()
    {
        LockGuard<ThreadMutexLock> guard(m_write_latch);
        m_prepare_snapshot_num--;
        if (m_prepare_snapshot_num == 0)
        {
            m_write_latch.NotifyAll();
        }
        INFO_LOG("Open write latch for snapshot save preparing complete.");
    }

    void Ardb::CloseWriteLatchBeforeSnapshotPrepare()
    {
        /*
         * wait until all writing commands complete & wal log appended
         */
        LockGuard<ThreadMutexLock> guard(m_write_latch);
        m_prepare_snapshot_num++;
        while (m_write_caller_num != 0)
        {
            m_write_latch.Wait(1);
        }
        while (g_repl->GetReplLog().WALQueueSize() != 0)
        {
            m_write_latch.Wait(1);
            if (g_repl->GetIOService().IsInLoopThread())
            {
                g_repl->GetIOService().Continue();
            }
        }
        INFO_LOG("Close write latch for snapshot save preparing start.");
    }

    int Ardb::SetKeyValue(Context& ctx, const KeyObject& key, const ValueObject& val)
    {
        int ret = 0;
        ret = m_engine->Put(ctx, key, val);
        if (0 == ret)
        {
            TouchWatchKey(ctx, key);
            ctx.dirty = 1;
        }
        return ret;
    }
    int Ardb::MergeKeyValue(Context& ctx, const KeyObject& key, uint16 op, const DataArray& args)
    {
        int ret = m_engine->Merge(ctx, key, op, args);
        if (0 == ret)
        {
            TouchWatchKey(ctx, key);
            ctx.dirty++;
        }
        return ret;
    }
    int Ardb::RemoveKey(Context& ctx, const KeyObject& key)
    {
        int ret = m_engine->Del(ctx, key);
        if (0 == ret)
        {
            TouchWatchKey(ctx, key);
            ctx.dirty++;
        }
        return ret;
    }

    int Ardb::IteratorDel(Context& ctx, const KeyObject& key, Iterator* iter)
    {
        if (NULL != iter)
        {
            iter->Del();
            TouchWatchKey(ctx, key);
            ctx.dirty++;
        }
        return 0;
    }

    int Ardb::CompactDB(Context& ctx, const Data& ns)
    {
        if (m_compacting_data)
        {
            WARN_LOG("Can NOT launch compact task since server is compacting db.");
            return -1;
        }
        KeyObject start, end;
        start.SetNameSpace(ctx.ns);
        m_compacting_data = true;
        m_engine->Compact(ctx, start, end);
        m_compacting_data = false;
        return 0;
    }
    int Ardb::CompactAll(Context& ctx)
    {
        if (m_compacting_data)
        {
            WARN_LOG("Can NOT launch compact task since server is compacting db.");
            return -1;
        }
        m_compacting_data = true;
        m_engine->CompactAll(ctx);
        m_compacting_data = false;
        return 0;
    }

    int Ardb::FlushDB(Context& ctx, const Data& ns)
    {
        m_engine->DropNameSpace(ctx, ns);
        ctx.dirty += 1000; //makesure all
        TouchWatchedKeysOnFlush(ctx, ns);
        return 0;
    }
    int Ardb::FlushAll(Context& ctx)
    {
        DataArray nss;
        m_engine->ListNameSpaces(ctx, nss);
        for (size_t i = 0; i < nss.size(); i++)
        {
            m_engine->DropNameSpace(ctx, nss[i]);
        }
        ctx.dirty += 1000;
        Data empty_ns; //indicate all namespaces
        TouchWatchedKeysOnFlush(ctx, empty_ns);
        return 0;
    }

    bool Ardb::LockKey(const KeyPrefix& lk, int wait_limit)
    {
//        KeyPrefix lk;
//        lk.ns = key.GetNameSpace();
//        lk.key = key.GetKey();
        int wait_counter = 0;
        while (wait_limit <= 0 || wait_counter < wait_limit)
        {
            ThreadMutexLock* lock = NULL;
            {
                LockGuard<SpinMutexLock> guard(m_locking_keys_lock);
                std::pair<LockTable::iterator, bool> ret = m_locking_keys.insert(LockTable::value_type(lk, NULL));
                if (!ret.second && NULL != ret.first->second)
                {
                    /*
                     * already locked by other thread, wait until unlocked
                     */
                    lock = ret.first->second;
                }
                else
                {
                    /*
                     * no other thread lock on the key
                     */
                    if (!m_lock_pool.empty())
                    {
                        lock = m_lock_pool.top();
                        m_lock_pool.pop();
                    }
                    else
                    {
                        NEW(lock, ThreadMutexLock);
                    }
                    ret.first->second = lock;
                    return true;
                }
            }

            if (NULL != lock)
            {
                LockGuard<ThreadMutexLock> guard(*lock);
                lock->Wait(1, MILLIS);
                wait_counter++;
            }
        }
        return false;
    }
    void Ardb::UnlockKey(const KeyPrefix& lk)
    {
//        KeyPrefix lk;
//        lk.ns = key.GetNameSpace();
//        lk.key = key.GetKey();
        {
            LockGuard<SpinMutexLock> guard(m_locking_keys_lock);
            LockTable::iterator ret = m_locking_keys.find(lk);
            if (ret != m_locking_keys.end())
            {
                ThreadMutexLock* lock = ret->second;
                m_locking_keys.erase(ret);
                if(NULL != lock)
                {
                    m_lock_pool.push(lock);
                    LockGuard<ThreadMutexLock> guard(*lock);
                    lock->Notify();
                }
            }
        }
    }

    void Ardb::LockKeys(const KeyPrefixSet& ks)
    {
        while(true)
        {
            std::vector<KeyPrefix> locked_keys;
            KeyPrefixSet::const_iterator kit = ks.begin();
            while (kit != ks.end())
            {
                if(!LockKey(*kit, 1))
                {
                    break;
                }
                locked_keys.push_back(*kit);
                kit++;
            }
            if(locked_keys.size() == ks.size())
            {
                return;
            }
            //release locked keys, and try again
            for(size_t i = 0; i < locked_keys.size(); i++)
            {
                UnlockKey(locked_keys[i]);
            }
        }

//        for (size_t i = 0; i < ks.size(); i++)
//        {
//            LockKey(ks[i]);
//        }
    }
    void Ardb::UnlockKeys(const KeyPrefixSet& ks)
    {
        KeyPrefixSet::const_iterator kit = ks.begin();
        while (kit != ks.end())
        {
            UnlockKey(*kit);
            kit++;
        }
    }

    void Ardb::FeedReplicationDelOperation(Context& ctx, const Data& ns, const std::string& key)
    {
        if (!g_repl->IsInited())
        {
            return;
        }
        /*
         * generate 'del' command for master instance
         */
        RedisCommandFrame del("del");
        del.AddArg(key);
        FeedReplicationBacklog(ctx, ns, del);
    }

    void Ardb::FeedReplicationBacklog(Context& ctx, const Data& ns, RedisCommandFrame& cmd)
    {
        if (!g_repl->IsInited())
        {
            return;
        }
        /*
         * Since this method may be invoked by multi threads, there is a potential risk that replication commands may have wrong sequence
         * if one thread do db operation first but feed replication log later.
         * eg:
         * Thread A: lpush mylist a (1)  Thread B: lpush mylist b (2)
         *                   |                        |
         *                  \|/                      \|/
         *                     Replication Log Thread
         *                      lpush mylist b
         *                      lpush mylist a
         * This risk only has data inconsistent effect when multi-threads operating on same key at the same time which is also a rare condition.
         * If multi-threads operating on different keys at the same time,there is no any risk.
         *
         *
         */
//        if (!ctx.keyslocked)
//        {
//            ERROR_LOG("Can NOT feed replication wal log without key locked");
//            return;
//        }
        g_repl->GetReplLog().WriteWAL(ns, cmd);
    }

    void Ardb::SaveTTL(Context& ctx, const Data& ns, const std::string& key, int64 old_ttl, int64_t new_ttl)
    {
        /*
         * only works with engine that has no compactfilter support
         */
        if (m_engine->GetFeatureSet().support_compactfilter)
        {
            return;
        }
        if (new_ttl > 0)
        {
            if (m_min_ttl <= 0 || m_min_ttl > new_ttl)
            {
                m_min_ttl = new_ttl;
            }
            Data tll_ns(TTL_DB_NSMAESPACE, false);
            KeyObject new_ttl_key(tll_ns, KEY_TTL_SORT, "");
            new_ttl_key.SetTTL(new_ttl);
            new_ttl_key.SetTTLKeyNamespace(ns);
            new_ttl_key.SetTTLKey(key);
            ValueObject v;
            v.SetType(KEY_TTL_SORT);
            ctx.flags.create_if_notexist = 1;
            m_engine->Put(ctx, new_ttl_key, v);
        }
        if (old_ttl > 0)
        {
            Data tll_ns(TTL_DB_NSMAESPACE, false);
            KeyObject old_ttl_key(tll_ns, KEY_TTL_SORT, "");
            old_ttl_key.SetTTL(old_ttl);
            old_ttl_key.SetTTLKeyNamespace(ns);
            old_ttl_key.SetTTLKey(key);
            m_engine->Del(ctx, old_ttl_key);
        }
    }

    void Ardb::ScanTTLDB()
    {
        /*
         * only works with engine that has no compactfilter support
         */
        if (m_engine->GetFeatureSet().support_compactfilter || !GetConf().master_host.empty())
        {
            return;
        }
        if (0 == m_min_ttl)
        {
            return;
        }
        uint32 max_scan_keys_one_iter = 1000;
        uint32 scaned_keys = 0;
        uint32 total_expired_keys = 0;
        Context scan_ctx;
        Data tll_ns(TTL_DB_NSMAESPACE, false);
        KeyObject scan_key(tll_ns, KEY_TTL_SORT, "");
        scan_key.SetTTL(m_min_ttl);
        Iterator* iter = m_engine->Find(scan_ctx, scan_key);
        if (!iter->Valid())
        {
            m_min_ttl = 0; //no expire key
        }
        uint64 start_time = get_current_epoch_millis();
        while (iter->Valid() && scaned_keys < max_scan_keys_one_iter)
        {
            KeyObject& k = iter->Key(false);
            m_min_ttl = k.GetTTL();
            if (k.GetTTL() > get_current_epoch_millis())
            {
                break;
            }
            scaned_keys++;
            ValueObject meta;
            KeyObject meta_key(k.GetElement(1), KEY_META, k.GetElement(2));
            KeyLockGuard guard(scan_ctx, meta_key);
            if (0 == m_engine->Get(scan_ctx, meta_key, meta))
            {
                if (meta.GetTTL() == k.GetTTL())
                {
                    //delete whole key
                    if (meta.GetType() == KEY_STRING)
                    {
                        m_engine->Del(scan_ctx, meta_key);
                    }
                    else
                    {
                        DelKey(scan_ctx, meta_key);
                    }
                    total_expired_keys++;
                    FeedReplicationDelOperation(scan_ctx, meta_key.GetNameSpace(), meta_key.GetKey().AsString());
                }
            }
            else
            {

            }
            iter->Del();
            iter->Next();
        }
        DELETE(iter);
        uint64 end_time = get_current_epoch_millis();
        if (total_expired_keys > 0)
        {
            INFO_LOG("Cost %llums to delete %u keys.", (end_time - start_time), total_expired_keys);
        }
    }

    int64 Ardb::ScanExpiredKeys()
    {
        /*
         * do not do scan expire on slaves
         */
        if (!GetConf().master_host.empty())
        {
            return 0;
        }
        if (!m_engine->GetFeatureSet().support_compactfilter)
        {
            ScanTTLDB();
            return 0;
        }
        KeyPrefix scan_key;
        Context scan_ctx;
        int64 total_expired_keys = 0;
        uint64 start_time = get_current_epoch_millis();
        while (true)
        {
            {
                LockGuard<SpinMutexLock> guard(m_expires_lock);
                if (!m_expires.empty())
                {
                    scan_key = *(m_expires.begin());
                    m_expires.erase(m_expires.begin());
                }
                else
                {
                    break;
                }
            }
            if (scan_key.IsNil())
            {
                break;
            }
            KeyObject key(scan_key.ns, KEY_META, scan_key.key);
            KeyLockGuard keylocker(scan_ctx, key);
            ValueObject meta;
            bool toremove = false;
            int err = m_engine->Get(scan_ctx, key, meta);
            if (ERR_ENTRY_NOT_EXIST == err)
            {
                toremove = true;
            }
            else if (0 == err)
            {
                if (0 == m_engine->Get(scan_ctx, key, meta))
                {
                    uint64 ttl = meta.GetTTL();
                    if (ttl == 0 || ttl > get_current_epoch_millis())
                    {
                        continue;
                    }
                    toremove = true;
                    total_expired_keys++;
                }
            }
            else
            {
                continue;
            }
            total_expired_keys++;
            if (toremove)
            {
                /*
                 * generate 'del' command for master instance
                 */
                FeedReplicationDelOperation(scan_ctx, key.GetNameSpace(), key.GetKey().AsString());
            }
            if (KEY_STRING == meta.GetType())
            {
                RemoveKey(scan_ctx, key);
            }
            else if (meta.GetType() > 0)
            {
                DelKey(scan_ctx, key);
            }
        }
        uint64 end_time = get_current_epoch_millis();
        if (total_expired_keys > 0)
        {
            INFO_LOG("Cost %llums to delete %lld keys.", (end_time - start_time), total_expired_keys);
        }
        return 0;
    }
    void Ardb::AddExpiredKey(const Data& ns, const Data& key)
    {
        KeyPrefix k;
        k.key = key;
        k.ns = ns;
        k.key.ToMutableStr();
        k.ns.ToMutableStr();
        LockGuard<SpinMutexLock> guard(m_expires_lock);
        m_expires.insert(k);
    }

    int Ardb::FindElementByRedisCursor(const std::string& cursor, std::string& element)
    {
        uint64 cursor_int = 0;
        element.clear();
        if (!string_touint64(cursor, cursor_int))
        {
            element = cursor;
            return -1;
        }
        if (0 == cursor_int)
        {
            return 0;
        }
        LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
        return m_redis_cursor_cache.Get(cursor_int, element) ? 0 : -1;
    }
    uint64 Ardb::GetNewRedisCursor(const std::string& element)
    {
        LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
        m_redis_cursor_seed++;
        RedisCursorCache::CacheEntry entry;
        m_redis_cursor_cache.Insert(m_redis_cursor_seed, element, entry);
        return m_redis_cursor_seed;
    }

    bool Ardb::GetLongFromProtocol(Context& ctx, const std::string& str, int64_t& v)
    {
        RedisReply& reply = ctx.GetReply();
        if (!string_toint64(str, v))
        {
            reply.SetErrCode(ERR_INVALID_INTEGER_ARGS);
            return false;
        }
        if (v < LONG_MIN || v > LONG_MAX)
        {
            reply.SetErrCode(ERR_OUTOFRANGE);
            return false;
        }
        return true;
    }

    bool Ardb::CheckMeta(Context& ctx, const std::string& key, KeyType expected)
    {
        ValueObject meta_value;
        return CheckMeta(ctx, key, expected, meta_value);
    }

    bool Ardb::CheckMeta(Context& ctx, const std::string& key, KeyType expected, ValueObject& meta_value)
    {
        KeyObject meta_key(ctx.ns, KEY_META, key);
        return CheckMeta(ctx, meta_key, expected, meta_value);
    }

    bool Ardb::CheckMeta(Context& ctx, const KeyObject& key, KeyType expected, ValueObject& meta, bool fetch)
    {
        RedisReply& reply = ctx.GetReply();
        int err = 0;
        if (fetch)
        {
            err = m_engine->Get(ctx, key, meta);
            if (err != 0 && err != ERR_ENTRY_NOT_EXIST)
            {
                reply.SetErrCode(err);
                return false;
            }
        }
        if (meta.GetType() > 0)
        {
            if (expected > 0 && meta.GetType() != expected)
            {
                reply.SetErrCode(ERR_WRONG_TYPE);
                return false;
            }
            if (meta.GetTTL() > 0 && meta.GetTTL() < get_current_epoch_millis())
            {
                if (GetConf().master_host.empty() || !GetConf().slave_readonly)
                {
                    KeyLockGuard keylocker(ctx, key, ctx.keyslocked ? false : true);
                    int old_dirty = ctx.dirty;
                    if (meta.GetType() == KEY_STRING)
                    {
                        RemoveKey(ctx, key);
                    }
                    else
                    {
                        DelKey(ctx, key);
                    }
                    if (GetConf().master_host.empty())
                    {
                        /*
                         * master generate 'del' command for replication & resume dirty after delete kvs
                         */
                        ctx.dirty = old_dirty;
                        FeedReplicationDelOperation(ctx, key.GetNameSpace(), key.GetKey().AsString());
                    }
                }
                meta.Clear();
                return true;
            }
        }
        return true;
    }

    static void async_write_reply_callback(Channel* ch, void * data)
    {
        RedisReply* r = (RedisReply*) data;
        if (NULL != ch && NULL != r)
        {
            if (!ch->Write(*r))
            {
                ch->Close();
            }
        }
        DELETE(r);
    }

    int Ardb::WriteReply(Context& ctx, RedisReply* r, bool async)
    {
        if (NULL == ctx.client || NULL == ctx.client->client || NULL == r)
        {
            if (async)
            {
                DELETE(r);
            }
            return -1;
        }
        Channel* ch = ctx.client->client;
        if (!async)
        {
            ch->Write(*r);
        }
        else
        {
            ch->GetService().AsyncIO(ch->GetID(), async_write_reply_callback, r);
        }
        return 0;
    }

    void Ardb::FreeClient(Context& ctx)
    {
        UnwatchKeys(ctx);
        UnsubscribeAll(ctx, true, false);
        UnsubscribeAll(ctx, false, false);
        if (NULL != ctx.client)
        {
            m_clients.GetValue().erase(ctx.client->clientid);
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            m_all_clients.erase(&ctx);
        }
        UnblockKeys(ctx, true);
        MarkRestoring(ctx, false);
        {
            WriteLockGuard<SpinRWLock> guard(m_monitors_lock);
            if (NULL != m_monitors)
            {
                m_monitors->erase(&ctx);
                if (m_monitors->empty())
                {
                    DELETE(m_monitors);
                }
            }
        }
    }

#define CLIENTS_CRON_MIN_ITERATIONS 5
    void Ardb::ScanClients()
    {
        std::vector<Context*> to_close;
        uint64 now = get_current_epoch_micros();
        ClientIdSet& local_clients = m_clients.GetValue();
        if (local_clients.empty())
        {
            return;
        }
        int64 numclients = local_clients.size();
        int64 iterations = numclients / GetConf().hz;

        /* Process at least a few clients while we are at it, even if we need
         * to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
         * of processing each client once per second. */
        if (iterations < CLIENTS_CRON_MIN_ITERATIONS) iterations =
                (numclients < CLIENTS_CRON_MIN_ITERATIONS) ? numclients : CLIENTS_CRON_MIN_ITERATIONS;
        ClientId& last_scan_clientid = m_last_scan_clientid.GetValue();
        ClientIdSet::iterator it = local_clients.lower_bound(last_scan_clientid);
        while (it != local_clients.end() && iterations--)
        {
            ClientId& clientid = *it;
            Context* client = clientid.ctx;
            if (GetConf().tcp_keepalive > 0)
            {
                if (!client->IsBlocking() && !client->IsSubscribed())
                {
                    if (now - client->client->last_interaction_ustime >= GetConf().tcp_keepalive * 1000 * 1000)
                    {
                        //timeout;
                        to_close.push_back(client);
                        client = NULL;
                    }
                }
            }
            if (NULL != client && client->IsBlocking())
            {
                if (client->GetBPop().timeout > 0 && now >= client->GetBPop().timeout)
                {
                    //timeout;
                    RedisReply empty_bulk;
                    empty_bulk.ReserveMember(-1);
                    client->client->client->Write(empty_bulk);
                    to_close.push_back(client);
                }
            }
            if (NULL != client && NULL != client->client && NULL != client->client->client)
            {
                if (client->client->resume_ustime > 0 && now <= client->client->resume_ustime)
                {
                    client->client->client->AttachFD();
                    client->client->resume_ustime = -1;
                }
            }
            it++;
        }
        for (size_t i = 0; i < to_close.size(); i++)
        {
            to_close[i]->client->client->Close();
        }
        if (it == local_clients.end())
        {
            last_scan_clientid.id = 0;
            last_scan_clientid.ctx = NULL;
        }
        else
        {
            last_scan_clientid = (*it);
        }
    }

    void Ardb::AddClient(Context& ctx)
    {
        if (NULL != ctx.client)
        {
            m_clients.GetValue().insert(ctx.client->clientid);
            LockGuard<SpinMutexLock> guard(m_clients_lock);
            m_all_clients.insert(&ctx);
        }
    }

    bool Ardb::IsLoadingData()
    {
        return g_repl->GetSlave().IsLoading() || m_loading_data;
    }

    int Ardb::DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& args)
    {
        uint64 start_time = get_current_epoch_micros();
        /*
         * 1. commands recv from slave replication connection always in incompatible mode
         * 2. command type > REDIS_CMD_EXTEND_BEGIN
         * 3. configuration 'redis_compatible = no'
         */
        if ((args.GetType() < REDIS_CMD_EXTEND_BEGIN && GetConf().redis_compatible) || ctx.flags.slave)
        {
            ctx.flags.redis_compatible = 1;
        }
        else
        {
            ctx.flags.redis_compatible = 0;
        }
        if (NULL != ctx.client)
        {
            ctx.client->last_interaction_ustime = start_time;
        }
        ctx.last_cmdtype = setting.type;

        if (NULL != m_monitors && !IsLoadingData() && !(setting.flags & (ARDB_CMD_SKIP_MONITOR | ARDB_CMD_ADMIN)))
        {
            /*
             * feed monitors
             */
            FeedMonitors(ctx, ctx.ns, args);
        }
        if (setting.IsWriteCommand())
        {
            OpenWriteLatchByWriteCaller();
        }
        atomic_add_uint32(&m_db_caller_num, 1);
        int ret = (this->*(setting.handler))(ctx, args);
        atomic_sub_uint32(&m_db_caller_num, 1);
        if (!ctx.flags.lua)
        {
            uint64 stop_time = get_current_epoch_micros();
            atomic_add_uint64(&(setting.calls), 1);
            atomic_add_uint64(&(setting.microseconds), stop_time - start_time);
            g_cmd_cost_tracks[setting.type].AddCost((stop_time - start_time));
            TryPushSlowCommand(args, stop_time - start_time);
            DEBUG_LOG("Process recved cmd cost %lluus", stop_time - start_time);
        }

        /*
         * Sometimes read only command may modify the key/value pair, do NOT propagate it to replication log
         */
        if (!ctx.flags.no_wal && ctx.dirty > 0 && setting.IsWriteCommand())
        {
            FeedReplicationBacklog(ctx, ctx.ns, args);
        }
        if (setting.IsWriteCommand())
        {
            CloseWriteLatchByWriteCaller();
        }
        return ret;
    }

    Ardb::RedisCommandHandlerSetting* Ardb::FindRedisCommandHandlerSetting(RedisCommandFrame& args)
    {
        std::string& cmd = args.GetMutableCommand();
        lower_string(cmd);
        RedisCommandHandlerSettingTable::iterator found = m_settings.find(cmd);
        if (found == m_settings.end())
        {
            return NULL;
        }
        args.SetType(found->second.type);
        return &(found->second);
    }

    int Ardb::Call(Context& ctx, RedisCommandFrame& args)
    {
        RedisReply& reply = ctx.GetReply();
        RedisCommandHandlerSetting* found = FindRedisCommandHandlerSetting(args);
        if (NULL == found)
        {
            ctx.AbortTransaction();
            reply.SetErrorReason("unknown command:" + args.GetCommand());
            return -1;
        }
        DEBUG_LOG("Process recved cmd:%s", args.ToString().c_str());
        RedisCommandHandlerSetting& setting = *found;
        bool valid_cmd = true;
        if (setting.min_arity > 0)
        {
            valid_cmd = args.GetArguments().size() >= (uint32) setting.min_arity;
        }
        if (setting.max_arity >= 0 && valid_cmd)
        {
            valid_cmd = args.GetArguments().size() <= (uint32) setting.max_arity;
        }

        if (!valid_cmd)
        {
            ctx.AbortTransaction();
            reply.SetErrorReason("wrong number of arguments for command:" + args.GetCommand());
            return 0;
        }

//        if (err == ERR_OVERLOAD)
//        {
//            /*
//             * block overloaded connection
//             */
//            if (NULL != ctx.client && !ctx.client->IsDetached())
//            {
//                ctx.client->DetachFD();
//                uint64 now = get_current_epoch_millis();
//                uint64 next = 1000 - (now % 1000);
//                ChannelService& serv = ctx.client->GetService();
//                ctx.client->GetService().GetTimer().ScheduleHeapTask(new ResumeOverloadConnection(serv, ctx.client->GetID()), next == 0 ? 1 : next, -1, MILLIS);
//            }
//        }
        ctx.current_cmd = &args;

        int ret = 0;
        /*
         * Check if the connection is authenticated
         */
        if (!ctx.authenticated && setting.type != REDIS_CMD_AUTH && setting.type != REDIS_CMD_QUIT)
        {
            ctx.AbortTransaction();
            reply.SetErrCode(ERR_AUTH_FAILED);
            //fill_fix_error_reply(ctx.reply, "NOAUTH Authentication required");
            return ret;
        }

        /* Don't accept write commands if there are not enough good slaves and
         * user configured the min-slaves-to-write option. */
        if (GetConf().master_host.empty() && GetConf().repl_min_slaves_to_write > 0
                && GetConf().repl_min_slaves_max_lag > 0 && (setting.flags & ARDB_CMD_WRITE) > 0
                && g_repl->GetMaster().GoodSlavesCount() < GetConf().repl_min_slaves_to_write)
        {
            ctx.AbortTransaction();
            reply.SetErrCode(ERR_NOREPLICAS);
            return 0;
        }

        /* Don't accept write commands if this is a read only slave. But
         * accept write commands if this is our master. */
        if (!GetConf().master_host.empty() && (setting.flags & ARDB_CMD_WRITE) > 0)
        {
            if (!ctx.flags.slave) // from network
            {
                if (GetConf().slave_readonly)
                {
                    reply.SetErrCode(ERR_READONLY_SLAVE);
                    return 0;
                }
                else
                {
                    ctx.flags.no_wal = 1; //do not feed wal in slave node
                }
            }
        }

        if (GetConf().slave_ignore_del && ctx.flags.slave && setting.type == REDIS_CMD_DEL)
        {
            return 0;
        }

        /* Only allow INFO and SLAVEOF when slave-serve-stale-data is no and
         * we are a slave with a broken link with master. */
        if (!GetConf().master_host.empty() && !g_repl->GetSlave().IsSynced() && !GetConf().repl_serve_stale_data
                && !(setting.flags & ARDB_CMD_STALE))
        {
            ctx.AbortTransaction();
            reply.SetErrCode(ERR_MASTER_DOWN);
            return 0;
        }

        /* Loading DB? Return an error if the command has not the
         * CMD_LOADING flag. */
        if ((IsLoadingData() || IsRestoring(ctx, ctx.ns)) && !(setting.flags & ARDB_CMD_LOADING))
        {
            reply.SetErrCode(ERR_LOADING);
            return 0;
        }
        if (ctx.InTransaction())
        {
            if (setting.type != REDIS_CMD_MULTI && setting.type != REDIS_CMD_EXEC && setting.type != REDIS_CMD_DISCARD
                    && setting.type != REDIS_CMD_QUIT)
            {
                reply.SetStatusCode(STATUS_QUEUED);
                args.ClearRawProtocolData();
                ctx.GetTransaction().cached_cmds.push_back(args);
                return 0;
            }
        }
        else if (ctx.IsSubscribed())
        {
            if (setting.type != REDIS_CMD_SUBSCRIBE && setting.type != REDIS_CMD_PSUBSCRIBE
                    && setting.type != REDIS_CMD_PUNSUBSCRIBE && setting.type != REDIS_CMD_UNSUBSCRIBE
                    && setting.type != REDIS_CMD_QUIT)
            {
                reply.SetErrorReason("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
                return 0;
            }
        }
        ret = DoCall(ctx, setting, args);
        WakeClientsBlockingOnList(ctx);
        return ret;
    }

OP_NAMESPACE_END
