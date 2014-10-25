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
#include "ardb.hpp"
#include "network.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

OP_NAMESPACE_BEGIN
    Ardb* g_db = NULL;
    Ardb::Ardb(KeyValueEngineFactory& factory) :
            m_service(NULL), m_engine_factory(factory), m_engine(NULL), m_cache(m_cfg), m_watched_ctx(NULL), m_slave(
                    this), m_starttime(0), m_compacting(false), m_last_compact_start_time(0), m_last_compact_duration(0)
    {
        g_db = this;
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
                { "hmincrby", REDIS_CMD_HMINCRBY, &Ardb::HMIncrby, 3, -1, "w", 0, 0, 0 },
                { "hincrbyfloat", REDIS_CMD_HINCRBYFLOAT, &Ardb::HIncrbyFloat, 3, 3, "w", 0, 0, 0 },
                { "hkeys", REDIS_CMD_HKEYS, &Ardb::HKeys, 1, 1, "r", 0, 0, 0 },
                { "hlen", REDIS_CMD_HLEN, &Ardb::HLen, 1, 1, "r", 0, 0, 0 },
                { "hvals", REDIS_CMD_HVALS, &Ardb::HVals, 1, 1, "r", 0, 0, 0 },
                { "hmget", REDIS_CMD_HMGET, &Ardb::HMGet, 2, -1, "r", 0, 0, 0 },
                { "hset", REDIS_CMD_HSET, &Ardb::HSet, 3, 3, "w", 0, 0, 0 },
                { "hsetnx", REDIS_CMD_HSETNX, &Ardb::HSetNX, 3, 3, "w", 0, 0, 0 },
                { "hmset", REDIS_CMD_HMSET, &Ardb::HMSet, 3, -1, "w", 0, 0, 0 },
                { "hreplace", REDIS_CMD_HREPLACE, &Ardb::HReplace, 3, -1, "w", 0, 0, 0 },
                { "hscan", REDIS_CMD_HSCAN, &Ardb::HScan, 2, 6, "r", 0, 0, 0 },
                { "scard", REDIS_CMD_SCARD, &Ardb::SCard, 1, 1, "r", 0, 0, 0 },
                { "sadd", REDIS_CMD_SADD, &Ardb::SAdd, 2, -1, "w", 0, 0, 0 },
                { "sreplace", REDIS_CMD_SADD, &Ardb::SReplace, 2, -1, "w", 0, 0, 0 },
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
                { "cache", REDIS_CMD_CACHE, &Ardb::Cache, 2, 2, "r", 0, 0, 0 },
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
    }

    Ardb::~Ardb()
    {
        DELETE(m_engine);
    }

    KeyValueEngine& Ardb::GetKeyValueEngine()
    {
        return *m_engine;
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
    static void ServerEventCallback(ChannelService* serv, uint32 ev, void* data)
    {
        switch (ev)
        {
            case SCRIPT_FLUSH_EVENT:
            case SCRIPT_KILL_EVENT:
            {
                LUAInterpreter::ScriptEventCallback(serv, ev, data);
                break;
            }
            default:
            {
                break;
            }
        }
    }

    void Ardb::RewriteClientCommand(Context& ctx, RedisCommandFrame& cmd)
    {
        if (NULL != ctx.current_cmd)
        {
            *(ctx.current_cmd) = cmd;
        }
    }

    void Ardb::RenameCommand()
    {
        StringStringMap::iterator it = m_cfg.rename_commands.begin();
        while (it != m_cfg.rename_commands.end())
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

    int Ardb::Init(const ArdbConfig& cfg)
    {
        m_cfg = cfg;
        if (m_cfg.daemonize)
        {
            daemonize();
        }
        if (!m_cfg.pidfile.empty())
        {
            char tmp[200];
            sprintf(tmp, "%d", getpid());
            std::string content = tmp;
            file_write_content(m_cfg.pidfile, content);
        }
        make_dir(m_cfg.home);
        if (0 != chdir(m_cfg.home.c_str()))
        {
            ERROR_LOG("Faild to change dir to home:%s", m_cfg.home.c_str());
            return -1;
        }

        INFO_LOG("Start init storage engine.");
        m_engine = m_engine_factory.CreateDB(m_engine_factory.GetName().c_str());
        if (NULL == m_engine)
        {
            ERROR_LOG("Faild to open db:%s", m_cfg.home.c_str());
            return -1;
        }
        RenameCommand();

        m_stat.Init();

        return 0;
    }

    void Ardb::Start()
    {
        ArdbLogger::InitDefaultLogger(m_cfg.loglevel, m_cfg.logfile);
        uint32 worker_count = 0;
        for (uint32 i = 0; i < m_cfg.thread_pool_sizes.size(); i++)
        {
            if (m_cfg.thread_pool_sizes[i] == 0)
            {
                m_cfg.thread_pool_sizes[i] = 1;
            }
            else if (m_cfg.thread_pool_sizes[i] < 0)
            {
                m_cfg.thread_pool_sizes[i] = available_processors();
            }
            worker_count += m_cfg.thread_pool_sizes[i];
        }
        m_service = new ChannelService(m_cfg.max_clients + 32 + GetKeyValueEngine().MaxOpenFiles());
        m_service->SetThreadPoolSize(worker_count);
        m_key_lock.Init(worker_count + 8);

        ChannelOptions ops;
        ops.tcp_nodelay = true;
        if (m_cfg.tcp_keepalive > 0)
        {
            ops.keep_alive = m_cfg.tcp_keepalive;
        }
        for (uint32 i = 0; i < m_cfg.listen_addresses.size(); i++)
        {
            const std::string& address = m_cfg.listen_addresses[i];
            ServerSocketChannel* server = NULL;
            if (address.find(":") == std::string::npos)
            {
                SocketUnixAddress unix_address(address);
                server = m_service->NewServerSocketChannel();
                if (!server->Bind(&unix_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
                chmod(address.c_str(), m_cfg.unixsocketperm);
            }
            else
            {
                std::vector<std::string> ss = split_string(address, ":");
                uint32 port;
                if (ss.size() != 2 || !string_touint32(ss[1], port))
                {
                    ERROR_LOG("Invalid server socket address %s", address.c_str());
                    goto sexit;
                }
                SocketHostAddress socket_address(ss[0], port);
                server = m_service->NewServerSocketChannel();
                if (!server->Bind(&socket_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
                if (m_cfg.primary_port == 0)
                {
                    m_cfg.primary_port = port;
                }
            }
            server->Configure(ops);
            server->SetChannelPipelineInitializor(RedisRequestHandler::PipelineInit, this);
            server->SetChannelPipelineFinalizer(RedisRequestHandler::PipelineDestroy, NULL);
            uint32 min = 0;
            for (uint32 j = 0; j < i; j++)
            {
                min += min + m_cfg.thread_pool_sizes[j];
            }
            server->BindThreadPool(min, min + m_cfg.thread_pool_sizes[i]);
        }
        m_ardb_dump.Init(this);
        m_redis_dump.Init(this);
        if (0 == m_repl_backlog.Init(this))
        {
            if (0 != m_master.Init())
            {
                goto sexit;
            }
            if (0 != m_slave.Init())
            {
                goto sexit;
            }
        }

        if (!m_cfg.master_host.empty())
        {
            m_slave.SetIncludeDBs(m_cfg.repl_includes);
            m_slave.ConnectMaster(m_cfg.master_host, m_cfg.master_port);
        }
        m_service->RegisterUserEventCallback(ServerEventCallback, this);

        m_cron.Start();
        m_cache.Init();
        m_cache.Start();

        INFO_LOG("Server started, Ardb version %s", ARDB_VERSION);
        INFO_LOG("The server is now ready to accept connections on  %s",
                string_join_container(m_cfg.listen_addresses, ",").c_str());

        m_starttime = time(NULL);
        m_service->Start();
        sexit: m_master.Stop();
        m_cron.StopSelf();
        m_cache.StopSelf();
        DELETE(m_service);
        DELETE(m_engine);
        ArdbLogger::DestroyDefaultLogger();
    }

    bool Ardb::GetInt64Value(Context& ctx, const std::string& str, int64& v)
    {
        if (!string_toint64(str, v))
        {
            fill_error_reply(ctx.reply, "value is not a integer or out of range");
            return false;
        }
        return true;
    }

    bool Ardb::GetDoubleValue(Context& ctx, const std::string& str, double& v)
    {
        if (!string_todouble(str, v))
        {
            fill_error_reply(ctx.reply, "value is not a float or out of range");
            return false;
        }
        return true;
    }
    int Ardb::SetRaw(Context& ctx, const Slice& key, const Slice& value)
    {
        Options options;
        uint64 start = get_current_epoch_micros();
        int ret = GetKeyValueEngine().Put(key, value, options);
        uint64 end = get_current_epoch_micros();
        m_stat.StatWriteLatency(end - start);
        ctx.data_change = true;
        return ret;
    }
    int Ardb::GetRaw(Context& ctx, const Slice& key, std::string& value)
    {
        Options options;
        uint64 start = get_current_epoch_micros();
        int ret = GetKeyValueEngine().Get(key, &value, options);
        uint64 end = get_current_epoch_micros();
        m_stat.StatReadLatency(end - start);
        return ret;
    }
    int Ardb::DelRaw(Context& ctx, const Slice& key)
    {
        Options options;
        uint64 start = get_current_epoch_micros();
        int ret = GetKeyValueEngine().Del(key, options);
        uint64 end = get_current_epoch_micros();
        m_stat.StatWriteLatency(end - start);
        ctx.data_change = true;
        return ret;
    }
    int Ardb::SetKeyValue(Context& ctx, ValueObject& value)
    {
        if (!value.key.encode_buf.Readable())
        {
            value.key.Encode();
        }
        Slice kbuf(value.key.encode_buf.GetRawReadBuffer(), value.key.encode_buf.ReadableBytes());
        value.Encode();
        Slice vbuf(value.encode_buf.GetRawReadBuffer(), value.encode_buf.ReadableBytes());
        int ret = SetRaw(ctx, kbuf, vbuf);
        if (0 == ret)
        {
            CacheSetOptions options;
            options.from_read_result = false;
            options.cmd = ctx.current_cmd_type;
            m_cache.Put(value.key, value, options);
        }
        return ret;
    }

    void Ardb::IteratorSeek(Iterator* iter, KeyObject& target)
    {
        target.Encode();
        Slice kbuf(target.encode_buf.GetRawReadBuffer(), target.encode_buf.ReadableBytes());

        uint64 start = get_current_epoch_micros();
        iter->Seek(kbuf);
        uint64 end = get_current_epoch_micros();
        m_stat.StatSeekLatency(end - start);
    }

    Iterator* Ardb::IteratorKeyValue(KeyObject& from, bool match_key)
    {
        if (!from.encode_buf.Readable())
        {
            from.Encode();
        }
        Slice kbuf(from.encode_buf.GetRawReadBuffer(), from.encode_buf.ReadableBytes());
        Options options;
        options.read_fill_cache = false;
        uint64 start = get_current_epoch_micros();
        Iterator* it = GetKeyValueEngine().Find(kbuf, options);
        uint64 end = get_current_epoch_micros();
        m_stat.StatSeekLatency(end - start);
        if (NULL != it && it->Valid())
        {
            if (match_key)
            {
                KeyObject kk;
                if (!decode_key(it->Key(), kk))
                {
                    WARN_LOG("Invalid entry.");
                    DELETE(it);
                    return NULL;
                }
                if (kk.db != from.db || kk.key != from.key)
                {
                    DELETE(it);
                    return NULL;
                }
            }
        }
        else
        {
            DELETE(it);
        }
        return it;
    }

    int Ardb::GetKeyValue(Context& ctx, KeyObject& key, ValueObject* kv)
    {
        if (NULL != kv)
        {
            int err = m_cache.Get(key, *kv);
            if (0 == err || ERR_NOT_EXIST == err)
            {
                return err;
            }
        }

        if (!key.encode_buf.Readable())
        {
            key.Encode();
        }
        Slice kbuf(key.encode_buf.GetRawReadBuffer(), key.encode_buf.ReadableBytes());
        std::string v;
        int ret = GetRaw(ctx, kbuf, v);
        if (0 == ret)
        {
            if (NULL != kv)
            {
                if (!decode_value(v, *kv))
                {
                    ERROR_LOG("Failed to decode value for key %s", key.key.data());
                    return -1;
                }
                bool is_read_ctx = (ctx.cmd_setting_flags & ARDB_CMD_READONLY) != 0;
                CacheSetOptions options;
                options.from_read_result = is_read_ctx;
                options.cmd = ctx.current_cmd_type;
                m_cache.Put(key, *kv, options);
            }
            return 0;
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::DelKeyValue(Context& ctx, KeyObject& key)
    {
        if (!key.encode_buf.Readable())
        {
            key.Encode();
        }
        Slice kbuf(key.encode_buf.GetRawReadBuffer(), key.encode_buf.ReadableBytes());
        int ret = DelRaw(ctx, kbuf);
        if (0 == ret)
        {
            m_cache.Del(key, key.meta_type);
        }
        return ret;
    }

    int Ardb::GetMetaValue(Context& ctx, const Slice& key, KeyType expected_type, ValueObject& v)
    {
        v.key.db = ctx.currentDB;
        v.key.type = KEY_META;
        v.key.key = key;
        v.type = expected_type;
        int ret = GetKeyValue(ctx, v.key, &v);
        if (0 == ret)
        {
            v.key.meta_type = v.type;
            if (v.meta.expireat > 0 && v.meta.expireat <= get_current_epoch_millis())
            {
                //should delete this key in master
                if (m_cfg.master_host.empty())
                {
                    DeleteKey(ctx, key);
                    RedisCommandFrame del("del");
                    std::string keystr;
                    keystr.assign(key.data(), key.size());
                    del.AddArg(keystr);
                    m_master.FeedSlaves(ctx.currentDB, del);
                }
                return ERR_NOT_EXIST;
            }
            if (expected_type != KEY_END)
            {
                if (v.type == expected_type)
                {
                    return 0;
                }
                else
                {
                    return ERR_INVALID_TYPE;
                }
            }
            return 0;
        }
        switch (expected_type)
        {
            case LIST_META:
            {
                v.meta.SetEncoding(COLLECTION_ENCODING_ZIPLIST);
                break;
            }
            case SET_META:
            {
                v.meta.SetEncoding(COLLECTION_ENCODING_ZIPSET);
                break;
            }
            case HASH_META:
            {
                v.meta.SetEncoding(COLLECTION_ENCODING_ZIPMAP);
                break;
            }
            case ZSET_META:
            {
                v.meta.SetEncoding(COLLECTION_ENCODING_ZIPZSET);
                break;
            }
            default:
            {
                break;
            }
        }
        return ERR_NOT_EXIST;
    }

    int Ardb::GetType(Context& ctx, const Slice& key, KeyType& type)
    {
        ValueObject meta;
        meta.key.type = KEY_META;
        meta.key.db = ctx.currentDB;
        meta.key.key = key;
        if (0 == GetKeyValue(ctx, meta.key, &meta))
        {
            type = (KeyType) meta.type;
            return 0;
        }
        return -1;
    }

    void Ardb::FreeClientContext(Context& ctx)
    {
        UnwatchKeys(ctx);
        UnsubscribeAll(ctx, false);
        PUnsubscribeAll(ctx, false);
        ClearBlockKeys(ctx);

        LockGuard<SpinMutexLock> guard(m_clients_lock);
        m_clients.erase(ctx.client->GetID());
    }

    void Ardb::AddClientContext(Context& ctx)
    {
        LockGuard<SpinMutexLock> guard(m_clients_lock);
        m_clients[ctx.client->GetID()] = &ctx;
    }

    RedisReplyPool& Ardb::GetRedisReplyPool()
    {
        return m_reply_pool.GetValue();
    }

    uint64 Ardb::GetNewRedisCursor(const std::string& element)
    {
        LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
        RedisCursor cursor;
        cursor.element = element;
        cursor.ts = time(NULL);
        if (m_redis_cursor_table.empty())
        {
            cursor.cursor = 1;
        }
        else
        {
            cursor.cursor = (m_redis_cursor_table.rbegin()->second.cursor + 1);
        }
        m_redis_cursor_table.insert(RedisCursorTable::value_type(cursor.cursor, cursor));
        return cursor.cursor;
    }
    int Ardb::FindElementByRedisCursor(const std::string& cursor, std::string& element)
    {
        uint64 cursor_int = 0;
        if (!string_touint64(cursor, cursor_int))
        {
            return -1;
        }
        LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
        RedisCursorTable::iterator it = m_redis_cursor_table.find(cursor_int);
        if (it == m_redis_cursor_table.end())
        {
            return -1;
        }
        element = it->second.element;
        return 0;
    }

    void Ardb::ClearExpireRedisCursor()
    {
        if (!m_cfg.scan_redis_compatible)
        {
            return;
        }
        LockGuard<SpinMutexLock> guard(m_redis_cursor_lock);
        while (!m_redis_cursor_table.empty())
        {
            RedisCursorTable::iterator it = m_redis_cursor_table.begin();
            if (time(NULL) - it->second.ts >= m_cfg.scan_cursor_expire_after)
            {
                m_redis_cursor_table.erase(it);
            }
            else
            {
                return;
            }
        }
    }

    int Ardb::DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& args)
    {
        uint64 start_time = get_current_epoch_micros();
        ctx.last_interaction_ustime = start_time;
        ctx.cmd_setting_flags = setting.flags;
        int ret = (this->*(setting.handler))(ctx, args);
        uint64 stop_time = get_current_epoch_micros();
        ctx.last_interaction_ustime = stop_time;
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

    int Ardb::Call(Context& ctx, RedisCommandFrame& args, int flags)
    {
        RedisCommandHandlerSetting* found = FindRedisCommandHandlerSetting(args);
        if (NULL == found)
        {
            ERROR_LOG("No handler found for:%s with size:%u", args.GetCommand().c_str(), args.GetCommand().size());
            ERROR_LOG("Invalid command's ascii codes:%s", ascii_codes(args.GetCommand()).c_str());
            fill_error_reply(ctx.reply, "unknown command '%s'", args.GetCommand().c_str());
            return 0;
        }
        ctx.ClearState();
        int err = m_stat.IncRecvCommands(ctx.server_address, ctx.sequence);
        DEBUG_LOG("Process recved cmd[%lld]:%s with flags:%d", ctx.sequence, args.ToString().c_str(), flags);
        if (err == ERR_OVERLOAD)
        {
            /*
             * block overloaded connection
             */
            if (NULL != ctx.client && !ctx.client->IsDetached())
            {
                ctx.client->DetachFD();
                uint64 now = get_current_epoch_millis();
                uint64 next = 1000 - (now % 1000);
                ChannelService& serv = ctx.client->GetService();
                ctx.client->GetService().GetTimer().ScheduleHeapTask(
                        new ResumeOverloadConnection(serv, ctx.client->GetID()), next == 0 ? 1 : next, -1, MILLIS);
            }
        }
        ctx.current_cmd = &args;
        ctx.current_cmd_type = args.GetType();
        RedisCommandHandlerSetting& setting = *found;
        int ret = 0;

        //Check if the user is authenticated
        if (!ctx.authenticated && setting.type != REDIS_CMD_AUTH && setting.type != REDIS_CMD_QUIT)
        {
            fill_fix_error_reply(ctx.reply, "NOAUTH Authentication required");
            return ret;
        }

        if (!m_cfg.slave_serve_stale_data && !m_cfg.master_host.empty() && !m_slave.IsSynced())
        {
            if (setting.type == REDIS_CMD_INFO || setting.type == REDIS_CMD_SLAVEOF)
            {
                //continue
            }
            else
            {
                fill_error_reply(ctx.reply, "SYNC with master in progress");
                return ret;
            }
        }

        /**
         * Return error if ardb is readonly slave for write command
         */
        if (!m_cfg.master_host.empty() && (setting.flags & ARDB_CMD_WRITE) && !(flags & ARDB_PROCESS_REPL_WRITE))
        {
            if (m_cfg.slave_readonly)
            {
                fill_error_reply(ctx.reply, "server is read only slave");
                return ret;
            }
            else
            {
                if (m_slave.IsSyncing())
                {
                    fill_error_reply(ctx.reply, "SYNC with master in progress, not available for write");
                    return ret;
                }
                /*
                 * Do NOT feed replication log while current instance is not a readonly slave
                 */
                flags |= ARDB_PROCESS_WITHOUT_REPLICATION;
            }
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
            fill_error_reply(ctx.reply, "wrong number of arguments for '%s' command", args.GetCommand().c_str());
        }
        else
        {
            if (ctx.InTransc()
                    && (args.GetCommand() != "multi" && args.GetCommand() != "exec" && args.GetCommand() != "discard"
                            && args.GetCommand() != "quit"))
            {
                ctx.GetTransc().cached_cmds.push_back(args);
                fill_status_reply(ctx.reply, "QUEUED");
            }
            else if (ctx.IsSubscribedConn()
                    && (args.GetCommand() != "subscribe" && args.GetCommand() != "psubscribe"
                            && args.GetCommand() != "unsubscribe" && args.GetCommand() != "punsubscribe"
                            && args.GetCommand() != "quit"))
            {
                fill_error_reply(ctx.reply, "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
            }
            else
            {
                if (!(flags & ARDB_PROCESS_FEED_REPLICATION_ONLY))
                {
                    ret = DoCall(ctx, setting, args);
                }
            }
        }

        /*
         * Feed commands which modified data
         */
        if (m_repl_backlog.IsInited() && (flags & ARDB_PROCESS_WITHOUT_REPLICATION) == 0)
        {
            if ((flags & ARDB_PROCESS_FEED_REPLICATION_ONLY) || (flags & ARDB_PROCESS_FORCE_REPLICATION))
            {
                //feed to replication
                m_master.FeedSlaves(ctx.currentDB, *(ctx.current_cmd));
            }
            else if (ctx.data_change)
            {
                if (args.GetType() == REDIS_CMD_EXEC)
                {
                    //feed all transaction cmd to replication
                    if (!ctx.GetTransc().cached_cmds.empty())
                    {
                        RedisCommandFrame multi("MULTI");
                        m_master.FeedSlaves(ctx.currentDB, multi);
                        for (uint32 i = 0; i < ctx.GetTransc().cached_cmds.size(); i++)
                        {
                            RedisCommandFrame& transc_cmd = ctx.GetTransc().cached_cmds.at(i);
                            m_master.FeedSlaves(ctx.currentDB, transc_cmd);
                        }
                        RedisCommandFrame exec("EXEC");
                        m_master.FeedSlaves(ctx.currentDB, exec);
                    }
                }
                else if ((setting.flags & ARDB_CMD_WRITE))
                {
                    m_master.FeedSlaves(ctx.currentDB, *(ctx.current_cmd));
                }
            }
        }

        if (args.GetType() == REDIS_CMD_EXEC)
        {
            ctx.ClearTransc();
        }
        return ret;
    }

OP_NAMESPACE_END
