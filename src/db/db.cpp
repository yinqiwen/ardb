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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sstream>
#include "db.hpp"
#include "rocksdb_engine.hpp"

OP_NAMESPACE_BEGIN
    Ardb* g_db = NULL;

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

    Ardb::KeyLockGuard::KeyLockGuard(KeyObject& key) :
            k(key)
    {
        g_db->LockKey(k);
    }

    Ardb::KeyLockGuard::~KeyLockGuard()
    {
        g_db->UnlockKey(k);
    }

    Ardb::Ardb() :
            m_engine(NULL)
    {
        g_db = this;
        m_settings.set_empty_key("");
        m_settings.set_deleted_key("\n");

        struct RedisCommandHandlerSetting settingTable[] =
        {
        { "ping", REDIS_CMD_PING, &Ardb::Ping, 0, 0, "r", 0, 0, 0 },
        { "multi", REDIS_CMD_MULTI, &Ardb::Multi, 0, 0, "rs", 0, 0, 0 },
        { "discard", REDIS_CMD_DISCARD, &Ardb::Discard, 0, 0, "r", 0, 0, 0 },
        { "exec", REDIS_CMD_EXEC, &Ardb::Exec, 0, 0, "r", 0, 0, 0 },
        { "watch", REDIS_CMD_WATCH, &Ardb::Watch, 0, -1, "rs", 0, 0, 0 },
        { "unwatch", REDIS_CMD_UNWATCH, &Ardb::UnWatch, 0, 0, "rs", 0, 0, 0 },
        { "subscribe", REDIS_CMD_SUBSCRIBE, &Ardb::Subscribe, 1, -1, "pr", 0, 0, 0 },
        { "psubscribe", REDIS_CMD_PSUBSCRIBE, &Ardb::PSubscribe, 1, -1, "pr", 0, 0, 0 },
        { "unsubscribe", REDIS_CMD_UNSUBSCRIBE, &Ardb::UnSubscribe, 0, -1, "pr", 0, 0, 0 },
        { "punsubscribe", REDIS_CMD_PUNSUBSCRIBE, &Ardb::PUnSubscribe, 0, -1, "pr", 0, 0, 0 },
        { "publish", REDIS_CMD_PUBLISH, &Ardb::Publish, 2, 2, "pr", 0, 0, 0 },
        { "info", REDIS_CMD_INFO, &Ardb::Info, 0, 1, "r", 0, 0, 0 },
        { "save", REDIS_CMD_SAVE, &Ardb::Save, 0, 0, "ars", 0, 0, 0 },
        { "bgsave", REDIS_CMD_BGSAVE, &Ardb::BGSave, 0, 0, "ar", 0, 0, 0 },
        { "import", REDIS_CMD_IMPORT, &Ardb::Import, 0, 1, "ars", 0, 0, 0 },
        { "lastsave", REDIS_CMD_LASTSAVE, &Ardb::LastSave, 0, 0, "r", 0, 0, 0 },
        { "slowlog", REDIS_CMD_SLOWLOG, &Ardb::SlowLog, 1, 2, "r", 0, 0, 0 },
        { "dbsize", REDIS_CMD_DBSIZE, &Ardb::DBSize, 0, 0, "r", 0, 0, 0 },
        { "config", REDIS_CMD_CONFIG, &Ardb::Config, 1, 3, "ar", 0, 0, 0 },
        { "client", REDIS_CMD_CLIENT, &Ardb::Client, 1, 3, "ar", 0, 0, 0 },
        { "flushdb", REDIS_CMD_FLUSHDB, &Ardb::FlushDB, 0, 0, "w", 0, 0, 0 },
        { "flushall", REDIS_CMD_FLUSHALL, &Ardb::FlushAll, 0, 0, "w", 0, 0, 0 },
        { "compactdb", REDIS_CMD_COMPACTDB, &Ardb::CompactDB, 0, 0, "ar", 0, 0, 0 },
        { "compactall", REDIS_CMD_COMPACTALL, &Ardb::CompactAll, 0, 0, "ar", 0, 0, 0 },
        { "time", REDIS_CMD_TIME, &Ardb::Time, 0, 0, "ar", 0, 0, 0 },
        { "echo", REDIS_CMD_ECHO, &Ardb::Echo, 1, 1, "r", 0, 0, 0 },
        { "quit", REDIS_CMD_QUIT, &Ardb::Quit, 0, 0, "rs", 0, 0, 0 },
        { "shutdown", REDIS_CMD_SHUTDOWN, &Ardb::Shutdown, 0, 1, "ar", 0, 0, 0 },
        { "slaveof", REDIS_CMD_SLAVEOF, &Ardb::Slaveof, 2, -1, "as", 0, 0, 0 },
        { "replconf", REDIS_CMD_REPLCONF, &Ardb::ReplConf, 0, -1, "ars", 0, 0, 0 },
        { "sync", EWDIS_CMD_SYNC, &Ardb::Sync, 0, 2, "ars", 0, 0, 0 },
        { "psync", REDIS_CMD_PSYNC, &Ardb::PSync, 2, 2, "ars", 0, 0, 0 },
        { "apsync", REDIS_CMD_PSYNC, &Ardb::PSync, 2, -1, "ars", 0, 0, 0 },
        { "select", REDIS_CMD_SELECT, &Ardb::Select, 1, 1, "r", 0, 0, 0 },
        { "append", REDIS_CMD_APPEND, &Ardb::Append, 2, 2, "w", 0, 0, 0 },
        { "get", REDIS_CMD_GET, &Ardb::Get, 1, 1, "r", 0, 0, 0 },
        { "set", REDIS_CMD_SET, &Ardb::Set, 2, 7, "w", 0, 0, 0 },
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
        { "bitopcount", REDIS_CMD_BITOPCUNT, &Ardb::BitopCount, 2, -1, "w", 0, 0, 0 },
        { "decr", REDIS_CMD_DECR, &Ardb::Decr, 1, 1, "w", 1, 0, 0 },
        { "decrby", REDIS_CMD_DECRBY, &Ardb::Decrby, 2, 2, "w", 1, 0, 0 },
        { "getbit", REDIS_CMD_GETBIT, &Ardb::GetBit, 2, 2, "r", 0, 0, 0 },
        { "getrange", REDIS_CMD_GETRANGE, &Ardb::GetRange, 3, 3, "r", 0, 0, 0 },
        { "getset", REDIS_CMD_GETSET, &Ardb::GetSet, 2, 2, "w", 1, 0, 0 },
        { "incr", REDIS_CMD_INCR, &Ardb::Incr, 1, 1, "w", 1, 0, 0 },
        { "incrby", REDIS_CMD_INCRBY, &Ardb::Incrby, 2, 2, "w", 1, 0, 0 },
        { "incrbyfloat", REDIS_CMD_INCRBYFLOAT, &Ardb::IncrbyFloat, 2, 2, "w", 0, 0, 0 },
        { "mget", REDIS_CMD_MGET, &Ardb::MGet, 1, -1, "w", 0, 0, 0 },
        { "mset", REDIS_CMD_MSET, &Ardb::MSet, 2, -1, "w", 0, 0, 0 },
        { "msetnx", REDIS_CMD_MSETNX, &Ardb::MSetNX, 2, -1, "w", 0, 0, 0 },
        { "psetex", REDIS_CMD_PSETEX, &Ardb::MSetNX, 3, 3, "w", 0, 0, 0 },
        { "setbit", REDIS_CMD_SETBIT, &Ardb::SetBit, 3, 3, "w", 0, 0, 0 },
        { "setex", REDIS_CMD_SETEX, &Ardb::SetEX, 3, 3, "w", 0, 0, 0 },
        { "setnx", REDIS_CMD_SETNX, &Ardb::SetNX, 2, 2, "w", 0, 0, 0 },
        { "setrange", REDIS_CMD_SETEANGE, &Ardb::SetRange, 3, 3, "w", 0, 0, 0 },
        { "strlen", REDIS_CMD_STRLEN, &Ardb::Strlen, 1, 1, "r", 0, 0, 0 },
        { "hdel", REDIS_CMD_HDEL, &Ardb::HDel, 2, -1, "w", 0, 0, 0 },
        { "hexists", REDIS_CMD_HEXISTS, &Ardb::HExists, 2, 2, "r", 0, 0, 0 },
        { "hget", REDIS_CMD_HGET, &Ardb::HGet, 2, 2, "r", 0, 0, 0 },
        { "hgetall", REDIS_CMD_HGETALL, &Ardb::HGetAll, 1, 1, "r", 0, 0, 0 },
        { "hincrby", REDIS_CMD_HINCR, &Ardb::HIncrby, 3, 3, "w", 0, 0, 0 },
        { "hincrbyfloat", REDIS_CMD_HINCRBYFLOAT, &Ardb::HIncrbyFloat, 3, 3, "w", 0, 0, 0 },
        { "hkeys", REDIS_CMD_HKEYS, &Ardb::HKeys, 1, 1, "r", 0, 0, 0 },
        { "hlen", REDIS_CMD_HLEN, &Ardb::HLen, 1, 1, "r", 0, 0, 0 },
        { "hvals", REDIS_CMD_HVALS, &Ardb::HVals, 1, 1, "r", 0, 0, 0 },
        { "hmget", REDIS_CMD_HMGET, &Ardb::HMGet, 2, -1, "r", 0, 0, 0 },
        { "hset", REDIS_CMD_HSET, &Ardb::HSet, 3, 3, "w", 0, 0, 0 },
        { "hsetnx", REDIS_CMD_HSETNX, &Ardb::HSetNX, 3, 3, "w", 0, 0, 0 },
        { "hmset", REDIS_CMD_HMSET, &Ardb::HMSet, 3, -1, "w", 0, 0, 0 },
        { "hscan", REDIS_CMD_HSCAN, &Ardb::HScan, 2, 6, "r", 0, 0, 0 },
        { "scard", REDIS_CMD_SCARD, &Ardb::SCard, 1, 1, "r", 0, 0, 0 },
        { "sadd", REDIS_CMD_SADD, &Ardb::SAdd, 2, -1, "w", 0, 0, 0 },
        { "sdiff", REDIS_CMD_SDIFF, &Ardb::SDiff, 2, -1, "r", 0, 0, 0 },
        { "sdiffcount", REDIS_CMD_SDIFFCOUNT, &Ardb::SDiffCount, 2, -1, "r", 0, 0, 0 },
        { "sdiffstore", REDIS_CMD_SDIFFSTORE, &Ardb::SDiffStore, 3, -1, "w", 0, 0, 0 },
        { "sinter", REDIS_CMD_SINTER, &Ardb::SInter, 2, -1, "r", 0, 0, 0 },
        { "sintercount", REDIS_CMD_SINTERCOUNT, &Ardb::SInterCount, 2, -1, "r", 0, 0, 0 },
        { "sinterstore", REDIS_CMD_SINTERSTORE, &Ardb::SInterStore, 3, -1, "r", 0, 0, 0 },
        { "sismember", REDIS_CMD_SISMEMBER, &Ardb::SIsMember, 2, 2, "r", 0, 0, 0 },
        { "smembers", REDIS_CMD_SMEMBERS, &Ardb::SMembers, 1, 1, "r", 0, 0, 0 },
        { "smove", REDIS_CMD_SMOVE, &Ardb::SMove, 3, 3, "w", 0, 0, 0 },
        { "spop", REDIS_CMD_SPOP, &Ardb::SPop, 1, 1, "w", 0, 0, 0 },
        { "srandmember", REDIS_CMD_SRANMEMEBER, &Ardb::SRandMember, 1, 2, "r", 0, 0, 0 },
        { "srem", REDIS_CMD_SREM, &Ardb::SRem, 2, -1, "w", 1, 0, 0 },
        { "sunion", REDIS_CMD_SUNION, &Ardb::SUnion, 2, -1, "r", 0, 0, 0 },
        { "sunionstore", REDIS_CMD_SUNIONSTORE, &Ardb::SUnionStore, 3, -1, "r", 0, 0, 0 },
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
        { "zscan", REDIS_CMD_ZSCORE, &Ardb::ZScan, 2, 6, "r", 0, 0, 0 },
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
        { "blpop", REDIS_CMD_BLPOP, &Ardb::BLPop, 2, -1, "w", 0, 0, 0 },
        { "brpop", REDIS_CMD_BRPOP, &Ardb::BRPop, 2, -1, "w", 0, 0, 0 },
        { "brpoplpush", REDIS_CMD_BRPOPLPUSH, &Ardb::BRPopLPush, 3, 3, "w", 0, 0, 0 },
        { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Ardb::RPopLPush, 2, 2, "w", 0, 0, 0 },
        { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Ardb::RPopLPush, 2, 2, "w", 0, 0, 0 },
        { "move", REDIS_CMD_MOVE, &Ardb::Move, 2, 2, "w", 0, 0, 0 },
        { "rename", REDIS_CMD_RENAME, &Ardb::Rename, 2, 2, "w", 0, 0, 0 },
        { "renamenx", REDIS_CMD_RENAMENX, &Ardb::RenameNX, 2, 2, "w", 0, 0, 0 },
        { "sort", REDIS_CMD_SORT, &Ardb::Sort, 1, -1, "w", 0, 0, 0 },
        { "keys", REDIS_CMD_KEYS, &Ardb::Keys, 1, 6, "r", 0, 0, 0 },
        { "keyscount", REDIS_CMD_KEYSCOUNT, &Ardb::KeysCount, 1, 6, "r", 0, 0, 0 },
        { "__set__", REDIS_CMD_RAWSET, &Ardb::RawSet, 2, 2, "w", 0, 0, 0 },
        { "__del__", REDIS_CMD_RAWDEL, &Ardb::RawDel, 1, 1, "w", 0, 0, 0 },
        { "eval", REDIS_CMD_EVAL, &Ardb::Eval, 2, -1, "s", 0, 0, 0 },
        { "evalsha", REDIS_CMD_EVALSHA, &Ardb::EvalSHA, 2, -1, "s", 0, 0, 0 },
        { "script", REDIS_CMD_SCRIPT, &Ardb::Script, 1, -1, "s", 0, 0, 0 },
        { "randomkey", REDIS_CMD_RANDOMKEY, &Ardb::Randomkey, 0, 0, "r", 0, 0, 0 },
        { "scan", REDIS_CMD_SCAN, &Ardb::Scan, 1, 5, "r", 0, 0, 0 },
        { "geoadd", REDIS_CMD_GEO_ADD, &Ardb::GeoAdd, 5, -1, "w", 0, 0, 0 },
        { "geosearch", REDIS_CMD_GEO_SEARCH, &Ardb::GeoSearch, 5, -1, "r", 0, 0, 0 },
        { "auth", REDIS_CMD_AUTH, &Ardb::Auth, 1, 1, "r", 0, 0, 0 },
        { "pfadd", REDIS_CMD_PFADD, &Ardb::PFAdd, 2, -1, "w", 0, 0, 0 },
        { "pfcount", REDIS_CMD_PFCOUNT, &Ardb::PFCount, 1, -1, "w", 0, 0, 0 },
        { "pfmerge", REDIS_CMD_PFMERGE, &Ardb::PFMerge, 2, -1, "w", 0, 0, 0 }, };

        uint32 arraylen = arraysize(settingTable);
        for (uint32 i = 0; i < arraylen; i++)
        {
            settingTable[i].flags = 0;
            const char* f = settingTable[i].sflags;
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
                    default:
                        break;
                }
                f++;
            }
            m_settings[settingTable[i].name] = settingTable[i];
        }
        RenameCommand();
    }

    Ardb::~Ardb()
    {
        DELETE(m_engine);
    }

    int Ardb::Init()
    {
        if(g_config->engine == "rocksdb")
        {
            RocksDBEngine* rocks = NULL;
            NEW(rocks, RocksDBEngine);
            if(0 != rocks->Init(g_config->data_base_path, ""))
            {
                ERROR_LOG("Failed to init rocksdb.");
                DELETE(rocks);
                return -1;
            }
            m_engine = rocks;
            return 0;
        }
        ERROR_LOG("Unsupported storage engine:%s", g_config->engine.c_str());
        return -1;
    }

    void Ardb::RenameCommand()
    {
        StringStringMap::iterator it = g_config->rename_commands.begin();
        while (it != g_config->rename_commands.end())
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

    void Ardb::LockKey(KeyObject& key)
    {
        KeyPrefix lk;
        while (true)
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
                    return;
                }
            }

            if (NULL != lock)
            {
                LockGuard<ThreadMutexLock> guard(*lock);
                lock->Wait(1, MILLIS);
            }
        }
    }
    void Ardb::UnlockKey(KeyObject& key)
    {
        KeyPrefix lk;
        {
            LockGuard<SpinMutexLock> guard(m_locking_keys_lock);
            LockTable::iterator ret = m_locking_keys.find(lk);
            if (ret != m_locking_keys.end() && ret->second != NULL)
            {
                ThreadMutexLock* lock = ret->second;
                m_locking_keys.erase(ret);
                m_lock_pool.push(lock);
                LockGuard<ThreadMutexLock> guard(*lock);
                lock->Notify();
            }
        }
    }

    bool Ardb::CheckMeta(Context& ctx, const std::string& key, KeyType expected)
    {
        ValueObject meta_value;
        return CheckMeta(ctx, key, expected, meta_value);
    }

    bool Ardb::CheckMeta(Context& ctx, const std::string& key, KeyType expected, ValueObject& meta_value)
    {
        RedisReply& reply = ctx.GetReply();
        KeyObject meta_key(ctx.ns, KEY_META, key);
        int err = m_engine->Get(ctx, meta_key, meta_value);
        if (err != 0 && err != ERR_ENTRY_NOT_EXIST)
        {
            reply.SetErrCode(err);
            return false;
        }
        if (meta_value.GetType() > 0 && meta_value.GetType() != expected)
        {
            reply.SetErrCode(ERR_INVALID_TYPE);
            return false;
        }
        return true;
    }

    int Ardb::MergeOperation(KeyObject& key, ValueObject& val, uint16_t op, const DataArray& args)
    {
        int err = 0;
        switch (op)
        {
            case REDIS_CMD_HMSET:
            case REDIS_CMD_HMSET2:
            {
                for (size_t i = 0; i < args.size(); i += 2)
                {
                    err = MergeHSet(key, val, args[i], args[i + 1], false, false);
                    if (0 != err)
                    {
                        return err;
                    }
                }
                break;
            }
        }
        return 0;
    }

    int Ardb::DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& args)
    {
        uint64 start_time = get_current_epoch_micros();
        int ret = (this->*(setting.handler))(ctx, args);
        uint64 stop_time = get_current_epoch_micros();
        atomic_add_uint64(&(setting.calls), 1);
        atomic_add_uint64(&(setting.microseconds), stop_time - start_time);
        TryPushSlowCommand(args, stop_time - start_time);
        DEBUG_LOG("Process recved cmd[%lld] cost %lluus", ctx.sequence, stop_time - start_time);
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

    struct ResumeOverloadConnection: public Runnable
    {
            ChannelService& chs;
            uint32 channle_id;
            ResumeOverloadConnection(ChannelService& serv, uint32 id) :
                    chs(serv), channle_id(id)
            {
            }
            void Run()
            {
                Channel* ch = chs.GetChannel(channle_id);
                if (NULL != ch)
                {
                    ch->AttachFD();
                }
            }
    };

    int Ardb::Call(Context& ctx, RedisCommandFrame& args)
    {
        RedisReply& reply = ctx.GetReply();
        RedisCommandHandlerSetting* found = FindRedisCommandHandlerSetting(args);
        if (NULL == found)
        {
            reply.SetErrorReason("unknown command:" + args.GetCommand());
            return -1;
        }

        //ctx.ClearState();

        DEBUG_LOG("Process recved cmd[%lld]:%s", ctx.sequence, args.ToString().c_str());
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
        RedisCommandHandlerSetting& setting = *found;
        int ret = 0;

        /*
         * Check if the connection is authenticated
         */
        if (!ctx.authenticated && setting.type != REDIS_CMD_AUTH && setting.type != REDIS_CMD_QUIT)
        {
            reply.SetErrCode(ERR_AUTH_FAILED);
            //fill_fix_error_reply(ctx.reply, "NOAUTH Authentication required");
            return ret;
        }
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
            reply.SetErrorReason("wrong number of arguments for command:" + args.GetCommand());
            return 0;
        }
        if (ctx.InTransaction())
        {
            if (setting.type != REDIS_CMD_MULTI && setting.type != REDIS_CMD_EXEC && setting.type != REDIS_CMD_DISCARD && setting.type != REDIS_CMD_QUIT)
            {
                reply.SetStatusCode(STATUS_QUEUED);
                ctx.GetTransaction().cached_cmds.push_back(args);
                return 0;
            }
        }
        else if (ctx.IsSubscribed())
        {
            if (setting.type != REDIS_CMD_SUBSCRIBE && setting.type != REDIS_CMD_PSUBSCRIBE && setting.type != REDIS_CMD_PUNSUBSCRIBE
                    && setting.type != REDIS_CMD_UNSUBSCRIBE && setting.type != REDIS_CMD_QUIT)
            {
                reply.SetErrorReason("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
                return 0;
            }
        }
        ret = DoCall(ctx, setting, args);
        return ret;
    }

OP_NAMESPACE_END
