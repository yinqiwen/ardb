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

#include "ardb_server.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <fnmatch.h>
#include <sstream>
#include "util/file_helper.hpp"
#include "channel/zookeeper/zookeeper_client.hpp"

namespace ardb
{
    static bool verify_config(const ArdbServerConfig& cfg)
    {
        if (!cfg.master_host.empty() && cfg.repl_backlog_size <= 0)
        {
            ERROR_LOG("[Config]Invalid value for 'slaveof' since 'repl-backlog-size' is not set correctly.");
            return false;
        }
        if (cfg.requirepass.size() > ARDB_AUTHPASS_MAX_LEN)
        {
            ERROR_LOG("[Config]Password is longer than ARDB_AUTHPASS_MAX_LEN.");
            return false;
        }
        return true;
    }

    int ArdbServer::ParseConfig(const Properties& props, ArdbServerConfig& cfg)
    {
        conf_get_string(props, "home", cfg.home);
        if (cfg.home.empty())
        {
            cfg.home = "../ardb";
        }
        make_dir(cfg.home);
        cfg.home = real_path(cfg.home);

        setenv("ARDB_HOME", cfg.home.c_str(), 1);
        replace_env_var(const_cast<Properties&>(props));

        conf_get_string(props, "pidfile", cfg.pidfile);

        std::string ports;
        conf_get_string(props, "port", ports);
        if (!ports.empty())
        {
            std::vector<std::string> ss = split_string(ports, ",");
            for (uint32 i = 0; i < ss.size(); i++)
            {
                uint32 port = 0;
                if (string_touint32(ss[i], port) && port > 0 && port < 65535)
                {
                    cfg.listen_ports.insert((uint16) port);
                }
                else
                {
                    WARN_LOG("Invalid 'port' config.");
                }
            }
        }

        conf_get_int64(props, "tcp-keepalive", cfg.tcp_keepalive);
        conf_get_int64(props, "timeout", cfg.timeout);
        conf_get_int64(props, "unixsocketperm", cfg.unixsocketperm);
        conf_get_int64(props, "slowlog-log-slower-than", cfg.slowlog_log_slower_than);
        conf_get_int64(props, "slowlog-max-len", cfg.slowlog_max_len);
        conf_get_int64(props, "maxclients", cfg.max_clients);
        conf_get_string(props, "bind", cfg.listen_host);
        conf_get_string(props, "unixsocket", cfg.listen_unix_path);

        conf_get_string(props, "data-dir", cfg.data_base_path);
        conf_get_string(props, "backup-dir", cfg.backup_dir);
        conf_get_string(props, "repl-dir", cfg.repl_data_dir);
        make_dir(cfg.repl_data_dir);
        make_dir(cfg.backup_dir);
        cfg.repl_data_dir = real_path(cfg.repl_data_dir);

        conf_get_string(props, "zookeeper-servers", cfg.zookeeper_servers);

        conf_get_string(props, "loglevel", cfg.loglevel);
        conf_get_string(props, "logfile", cfg.logfile);
        conf_get_bool(props, "daemonize", cfg.daemonize);

        conf_get_int64(props, "thread-pool-size", cfg.worker_count);
        if (cfg.worker_count <= 0)
        {
            cfg.worker_count = available_processors();
        }

        conf_get_int64(props, "repl-backlog-size", cfg.repl_backlog_size);
        conf_get_int64(props, "repl-ping-slave-period", cfg.repl_ping_slave_period);
        conf_get_int64(props, "repl-timeout", cfg.repl_timeout);
        conf_get_int64(props, "repl-state-persist-period", cfg.repl_state_persist_period);
        conf_get_int64(props, "lua-time-limit", cfg.lua_time_limit);

        conf_get_int64(props, "hash-max-ziplist-entries", cfg.db_cfg.hash_max_ziplist_entries);
        conf_get_int64(props, "hash_max-ziplist-value", cfg.db_cfg.hash_max_ziplist_value);
        conf_get_int64(props, "set-max-ziplist-entries", cfg.db_cfg.set_max_ziplist_entries);
        conf_get_int64(props, "set-max-ziplist-value", cfg.db_cfg.set_max_ziplist_value);
        conf_get_int64(props, "list-max-ziplist-entries", cfg.db_cfg.list_max_ziplist_entries);
        conf_get_int64(props, "list-max-ziplist-value", cfg.db_cfg.list_max_ziplist_value);
        conf_get_int64(props, "zset-max-ziplist-entries", cfg.db_cfg.zset_max_ziplist_entries);
        conf_get_int64(props, "zset_max_ziplist_value", cfg.db_cfg.zset_max_ziplist_value);

        conf_get_int64(props, "L1-cache-max-memory", cfg.db_cfg.L1_cache_memory_limit);
        conf_get_bool(props, "zset-write-fill-cache", cfg.db_cfg.zset_write_fill_cache);
        conf_get_bool(props, "read-fill-cache", cfg.db_cfg.read_fill_cache);

        conf_get_int64(props, "hll-sparse-max-bytes", cfg.db_cfg.hll_sparse_max_bytes);

        conf_get_bool(props, "slave-read-only", cfg.slave_readonly);
        conf_get_bool(props, "slave-serve-stale-data", cfg.slave_serve_stale_data);
        conf_get_int64(props, "slave-priority", cfg.slave_priority);

        std::string slaveof;
        if (conf_get_string(props, "slaveof", slaveof))
        {
            std::vector<std::string> ss = split_string(slaveof, ":");
            if (ss.size() == 2)
            {
                cfg.master_host = ss[0];
                if (!string_touint32(ss[1], cfg.master_port))
                {
                    cfg.master_host = "";
                    WARN_LOG("Invalid 'slaveof' config.");
                }
                if (cfg.listen_ports.count((uint16) cfg.master_port) > 0 && is_local_ip(cfg.master_host))
                {
                    cfg.master_host = "";
                    WARN_LOG("Invalid 'slaveof' config.");
                }
            }
            else
            {
                WARN_LOG("Invalid 'slaveof' config.");
            }
        }

        std::string include_dbs, exclude_dbs;
        cfg.repl_includes.clear();
        cfg.repl_excludes.clear();
        conf_get_string(props, "replicate-include-db", include_dbs);
        conf_get_string(props, "replicate-exclude-db", exclude_dbs);
        if (0 != split_uint32_array(include_dbs, "|", cfg.repl_includes))
        {
            ERROR_LOG("Invalid 'replicate-include-db' config.");
            cfg.repl_includes.clear();
        }
        if (0 != split_uint32_array(exclude_dbs, "|", cfg.repl_excludes))
        {
            ERROR_LOG("Invalid 'replicate-exclude-db' config.");
            cfg.repl_excludes.clear();
        }

        if (cfg.data_base_path.empty())
        {
            cfg.data_base_path = ".";
        }
        make_dir(cfg.data_base_path);
        cfg.data_base_path = real_path(cfg.data_base_path);

        conf_get_string(props, "additional-misc-info", cfg.additional_misc_info);

        conf_get_string(props, "requirepass", cfg.requirepass);

        Properties::const_iterator fit = props.find("rename-command");
        if (fit != props.end())
        {
            cfg.rename_commands.clear();
            StringSet newcmdset;
            const ConfItemsArray& cs = fit->second;
            ConfItemsArray::const_iterator cit = cs.begin();
            while (cit != cs.end())
            {
                if (cit->size() != 2 || newcmdset.count(cit->at(1)) > 0)
                {
                    ERROR_LOG("Invalid 'rename-command' config.");
                }
                else
                {
                    cfg.rename_commands[cit->at(0)] = cit->at(1);
                    newcmdset.insert(cit->at(1));
                }
                cit++;
            }
        }

        if (!verify_config(cfg))
        {
            return -1;
        }

        ArdbLogger::SetLogLevel(cfg.loglevel);
        return 0;
    }

    ArdbServer::ArdbServer(KeyValueEngineFactory& engine) :
            m_service(NULL), m_db(NULL), m_engine(engine), m_slowlog_handler(m_cfg), m_master_serv(this), m_slave_client(
                    this), m_watch_mutex(
            PTHREAD_MUTEX_RECURSIVE), m_ctx_local(false)
    {
        struct RedisCommandHandlerSetting settingTable[] =
            {
                { "ping", REDIS_CMD_PING, &ArdbServer::Ping, 0, 0, "r", 0 },
                { "multi", REDIS_CMD_MULTI, &ArdbServer::Multi, 0, 0, "rs", 0 },
                { "discard", REDIS_CMD_DISCARD, &ArdbServer::Discard, 0, 0, "r", 0 },
                { "exec", REDIS_CMD_EXEC, &ArdbServer::Exec, 0, 0, "r", 0 },
                { "watch", REDIS_CMD_WATCH, &ArdbServer::Watch, 0, -1, "rs", 0 },
                { "unwatch", REDIS_CMD_UNWATCH, &ArdbServer::UnWatch, 0, 0, "rs", 0 },
                { "subscribe", REDIS_CMD_SUBSCRIBE, &ArdbServer::Subscribe, 1, -1, "pr", 0 },
                { "psubscribe", REDIS_CMD_PSUBSCRIBE, &ArdbServer::PSubscribe, 1, -1, "pr", 0 },
                { "unsubscribe", REDIS_CMD_UNSUBSCRIBE, &ArdbServer::UnSubscribe, 0, -1, "pr", 0 },
                { "punsubscribe", REDIS_CMD_PUNSUBSCRIBE, &ArdbServer::PUnSubscribe, 0, -1, "pr", 0 },
                { "publish", REDIS_CMD_PUBLISH, &ArdbServer::Publish, 2, 2, "pr", 0 },
                { "info", REDIS_CMD_INFO, &ArdbServer::Info, 0, 1, "r", 0 },
                { "save", REDIS_CMD_SAVE, &ArdbServer::Save, 0, 0, "ars", 0 },
                { "bgsave", REDIS_CMD_BGSAVE, &ArdbServer::BGSave, 0, 0, "ar", 0 },
                { "import", REDIS_CMD_IMPORT, &ArdbServer::Import, 0, 1, "ars", 0 },
                { "lastsave", REDIS_CMD_LASTSAVE, &ArdbServer::LastSave, 0, 0, "r", 0 },
                { "slowlog", REDIS_CMD_SLOWLOG, &ArdbServer::SlowLog, 1, 2, "r", 0 },
                { "dbsize", REDIS_CMD_DBSIZE, &ArdbServer::DBSize, 0, 0, "r", 0 },
                { "config", REDIS_CMD_CONFIG, &ArdbServer::Config, 1, 3, "ar", 0 },
                { "client", REDIS_CMD_CLIENT, &ArdbServer::Client, 1, 3, "ar", 0 },
                { "flushdb", REDIS_CMD_FLUSHDB, &ArdbServer::FlushDB, 0, 0, "w", 0 },
                { "flushall", REDIS_CMD_FLUSHALL, &ArdbServer::FlushAll, 0, 0, "w", 0 },
                { "compactdb", REDIS_CMD_COMPACTDB, &ArdbServer::CompactDB, 0, 0, "ar", 0 },
                { "compactall", REDIS_CMD_COMPACTALL, &ArdbServer::CompactAll, 0, 0, "ar", 0 },
                { "time", REDIS_CMD_TIME, &ArdbServer::Time, 0, 0, "ar", 0 },
                { "echo", REDIS_CMD_ECHO, &ArdbServer::Echo, 1, 1, "r", 0 },
                { "quit", REDIS_CMD_QUIT, &ArdbServer::Quit, 0, 0, "rs", 0 },
                { "shutdown", REDIS_CMD_SHUTDOWN, &ArdbServer::Shutdown, 0, 1, "ar", 0 },
                { "slaveof", REDIS_CMD_SLAVEOF, &ArdbServer::Slaveof, 2, -1, "as", 0 },
                { "replconf", REDIS_CMD_REPLCONF, &ArdbServer::ReplConf, 0, -1, "ars", 0 },
                { "sync", EWDIS_CMD_SYNC, &ArdbServer::Sync, 0, 2, "ars", 0 },
                { "psync", REDIS_CMD_PSYNC, &ArdbServer::PSync, 2, 2, "ars", 0 },
                { "apsync", REDIS_CMD_PSYNC, &ArdbServer::PSync, 2, -1, "ars", 0 },
                { "select", REDIS_CMD_SELECT, &ArdbServer::Select, 1, 1, "r", 0 },
                { "append", REDIS_CMD_APPEND, &ArdbServer::Append, 2, 2, "w", 0 },
                { "get", REDIS_CMD_GET, &ArdbServer::Get, 1, 1, "r", 0 },
                { "set", REDIS_CMD_SET, &ArdbServer::Set, 2, 7, "w", 0 },
                { "del", REDIS_CMD_DEL, &ArdbServer::Del, 1, -1, "w", 0 },
                { "exists", REDIS_CMD_EXISTS, &ArdbServer::Exists, 1, 1, "r", 0 },
                { "expire", REDIS_CMD_EXPIRE, &ArdbServer::Expire, 2, 2, "w", 0 },
                { "pexpire", REDIS_CMD_PEXPIRE, &ArdbServer::PExpire, 2, 2, "w", 0 },
                { "expireat", REDIS_CMD_EXPIREAT, &ArdbServer::Expireat, 2, 2, "w", 0 },
                { "pexpireat", REDIS_CMD_PEXPIREAT, &ArdbServer::PExpireat, 2, 2, "w", 0 },
                { "persist", REDIS_CMD_PERSIST, &ArdbServer::Persist, 1, 1, "w", 1 },
                { "ttl", REDIS_CMD_TTL, &ArdbServer::TTL, 1, 1, "r", 0 },
                { "pttl", REDIS_CMD_PTTL, &ArdbServer::PTTL, 1, 1, "r", 0 },
                { "type", REDIS_CMD_TYPE, &ArdbServer::Type, 1, 1, "r", 0 },
                { "bitcount", REDIS_CMD_BITCOUNT, &ArdbServer::Bitcount, 1, 3, "r", 0 },
                { "bitop", REDIS_CMD_BITOP, &ArdbServer::Bitop, 3, -1, "w", 1 },
                { "bitopcount", REDIS_CMD_BITOPCUNT, &ArdbServer::BitopCount, 2, -1, "w", 0 },
                { "decr", REDIS_CMD_DECR, &ArdbServer::Decr, 1, 1, "w", 1 },
                { "decrby", REDIS_CMD_DECRBY, &ArdbServer::Decrby, 2, 2, "w", 1 },
                { "getbit", REDIS_CMD_GETBIT, &ArdbServer::GetBit, 2, 2, "r", 0 },
                { "getrange", REDIS_CMD_GETRANGE, &ArdbServer::GetRange, 3, 3, "r", 0 },
                { "getset", REDIS_CMD_GETSET, &ArdbServer::GetSet, 2, 2, "w", 1 },
                { "incr", REDIS_CMD_INCR, &ArdbServer::Incr, 1, 1, "w", 1 },
                { "incrby", REDIS_CMD_INCRBY, &ArdbServer::Incrby, 2, 2, "w", 1 },
                { "incrbyfloat", REDIS_CMD_INCRBYFLOAT, &ArdbServer::IncrbyFloat, 2, 2, "w", 0 },
                { "mget", REDIS_CMD_MGET, &ArdbServer::MGet, 1, -1, "w", 0 },
                { "mset", REDIS_CMD_MSET, &ArdbServer::MSet, 2, -1, "w", 0 },
                { "msetnx", REDIS_CMD_MSETNX, &ArdbServer::MSetNX, 2, -1, "w", 0 },
                { "psetex", REDIS_CMD_PSETEX, &ArdbServer::MSetNX, 3, 3, "w", 0 },
                { "setbit", REDIS_CMD_SETBIT, &ArdbServer::SetBit, 3, 3, "w", 0 },
                { "setex", REDIS_CMD_SETEX, &ArdbServer::SetEX, 3, 3, "w", 0 },
                { "setnx", REDIS_CMD_SETNX, &ArdbServer::SetNX, 2, 2, "w", 0 },
                { "setrange", REDIS_CMD_SETEANGE, &ArdbServer::SetRange, 3, 3, "w", 0 },
                { "strlen", REDIS_CMD_STRLEN, &ArdbServer::Strlen, 1, 1, "r", 0 },
                { "hdel", REDIS_CMD_HDEL, &ArdbServer::HDel, 2, -1, "w", 0 },
                { "hexists", REDIS_CMD_HEXISTS, &ArdbServer::HExists, 2, 2, "r", 0 },
                { "hget", REDIS_CMD_HGET, &ArdbServer::HGet, 2, 2, "r", 0 },
                { "hgetall", REDIS_CMD_HGETALL, &ArdbServer::HGetAll, 1, 1, "r", 0 },
                { "hincr", REDIS_CMD_HINCR, &ArdbServer::HIncrby, 3, 3, "w", 0 },
                { "hmincrby", REDIS_CMD_HMINCRBY, &ArdbServer::HMIncrby, 3, -1, "w", 0 },
                { "hincrbyfloat", REDIS_CMD_HINCRBYFLOAT, &ArdbServer::HIncrbyFloat, 3, 3, "w", 0 },
                { "hkeys", REDIS_CMD_HKEYS, &ArdbServer::HKeys, 1, 1, "r", 0 },
                { "hlen", REDIS_CMD_HLEN, &ArdbServer::HLen, 1, 1, "r", 0 },
                { "hvals", REDIS_CMD_HVALS, &ArdbServer::HVals, 1, 1, "r", 0 },
                { "hmget", REDIS_CMD_HMGET, &ArdbServer::HMGet, 2, -1, "r", 0 },
                { "hset", REDIS_CMD_HSET, &ArdbServer::HSet, 3, 3, "w", 0 },
                { "hsetnx", REDIS_CMD_HSETNX, &ArdbServer::HSetNX, 3, 3, "w", 0 },
                { "hmset", REDIS_CMD_HMSET, &ArdbServer::HMSet, 3, -1, "w", 0 },
                { "hscan", REDIS_CMD_HSCAN, &ArdbServer::HScan, 2, 6, "r", 0 },
                { "scard", REDIS_CMD_SCARD, &ArdbServer::SCard, 1, 1, "r", 0 },
                { "sadd", REDIS_CMD_SADD, &ArdbServer::SAdd, 2, -1, "w", 0 },
                { "sdiff", REDIS_CMD_SDIFF, &ArdbServer::SDiff, 2, -1, "r", 0 },
                { "sdiffcount", REDIS_CMD_SDIFFCOUNT, &ArdbServer::SDiffCount, 2, -1, "r", 0 },
                { "sdiffstore", REDIS_CMD_SDIFFSTORE, &ArdbServer::SDiffStore, 3, -1, "w", 0 },
                { "sinter", REDIS_CMD_SINTER, &ArdbServer::SInter, 2, -1, "r", 0 },
                { "sintercount", REDIS_CMD_SINTERCOUNT, &ArdbServer::SInterCount, 2, -1, "r", 0 },
                { "sinterstore", REDIS_CMD_SINTERSTORE, &ArdbServer::SInterStore, 3, -1, "r", 0 },
                { "sismember", REDIS_CMD_SISMEMBER, &ArdbServer::SIsMember, 2, 2, "r", 0 },
                { "smembers", REDIS_CMD_SMEMBERS, &ArdbServer::SMembers, 1, 1, "r", 0 },
                { "smove", REDIS_CMD_SMOVE, &ArdbServer::SMove, 3, 3, "w", 0 },
                { "spop", REDIS_CMD_SPOP, &ArdbServer::SPop, 1, 1, "w", 0 },
                { "srandmember", REDIS_CMD_SRANMEMEBER, &ArdbServer::SRandMember, 1, 2, "r", 0 },
                { "srem", REDIS_CMD_SREM, &ArdbServer::SRem, 2, -1, "w", 1 },
                { "sunion", REDIS_CMD_SUNION, &ArdbServer::SUnion, 2, -1, "r", 0 },
                { "sunionstore", REDIS_CMD_SUNIONSTORE, &ArdbServer::SUnionStore, 3, -1, "r", 0 },
                { "sunioncount", REDIS_CMD_SUNIONCOUNT, &ArdbServer::SUnionCount, 2, -1, "r", 0 },
                { "sscan", REDIS_CMD_SSCAN, &ArdbServer::SScan, 2, 6, "r", 0 },
                { "zadd", REDIS_CMD_ZADD, &ArdbServer::ZAdd, 3, -1, "w", 0 },
                { "zcard", REDIS_CMD_ZCARD, &ArdbServer::ZCard, 1, 1, "r", 0 },
                { "zcount", REDIS_CMD_ZCOUNT, &ArdbServer::ZCount, 3, 3, "r", 0 },
                { "zincrby", REDIS_CMD_ZINCRBY, &ArdbServer::ZIncrby, 3, 3, "w", 0 },
                { "zrange", REDIS_CMD_ZRANGE, &ArdbServer::ZRange, 3, 4, "r", 0 },
                { "zrangebyscore", REDIS_CMD_ZRANGEBYSCORE, &ArdbServer::ZRangeByScore, 3, 7, "r", 0 },
                { "zrank", REDIS_CMD_ZRANK, &ArdbServer::ZRank, 2, 2, "r", 0 },
                { "zrem", REDIS_CMD_ZREM, &ArdbServer::ZRem, 2, -1, "w", 0 },
                { "zpop", REDIS_CMD_ZPOP, &ArdbServer::ZPop, 1, 2, "w", 0 },
                { "zrpop", REDIS_CMD_ZRPOP, &ArdbServer::ZPop, 2, 2, "w", 0 },
                { "zremrangebyrank", REDIS_CMD_ZREMRANGEBYRANK, &ArdbServer::ZRemRangeByRank, 3, 3, "w", 0 },
                { "zremrangebyscore", REDIS_CMD_ZREMRANGEBYSCORE, &ArdbServer::ZRemRangeByScore, 3, 3, "w", 0 },
                { "zrevrange", REDIS_CMD_ZREVRANGE, &ArdbServer::ZRevRange, 3, 4, "r", 0 },
                { "zrevrangebyscore", REDIS_CMD_ZREVRANGEBYSCORE, &ArdbServer::ZRevRangeByScore, 3, 7, "r", 0 },
                { "zinterstore", REDIS_CMD_ZINTERSTORE, &ArdbServer::ZInterStore, 3, -1, "w", 0 },
                { "zunionstore", REDIS_CMD_ZUNIONSTORE, &ArdbServer::ZUnionStore, 3, -1, "w", 0 },
                { "zrevrank", REDIS_CMD_ZREVRANK, &ArdbServer::ZRevRank, 2, 2, "r", 0 },
                { "zscore", REDIS_CMD_ZSCORE, &ArdbServer::ZScore, 2, 2, "r", 0 },
                { "zscan", REDIS_CMD_ZSCORE, &ArdbServer::ZScan, 2, 6, "r", 0 },
                { "zlexcount", REDIS_CMD_ZLEXCOUNT, &ArdbServer::ZLexCount, 3, 3, "r", 0 },
                { "zrangebylex", REDIS_CMD_ZRANGEBYLEX, &ArdbServer::ZRangeByLex, 3, 6, "r", 0 },
                { "zrevrangebylex", REDIS_CMD_ZREVRANGEBYLEX, &ArdbServer::ZRangeByLex, 3, 6, "r", 0 },
                { "zremrangebylex", REDIS_CMD_ZREMRANGEBYLEX, &ArdbServer::ZRemRangeByLex, 3, 3, "w", 0 },
                { "lindex", REDIS_CMD_LINDEX, &ArdbServer::LIndex, 2, 2, "r", 0 },
                { "linsert", REDIS_CMD_LINSERT, &ArdbServer::LInsert, 4, 4, "w", 0 },
                { "llen", REDIS_CMD_LLEN, &ArdbServer::LLen, 1, 1, "r", 0 },
                { "lpop", REDIS_CMD_LPOP, &ArdbServer::LPop, 1, 1, "w", 0 },
                { "lpush", REDIS_CMD_LPUSH, &ArdbServer::LPush, 2, -1, "w", 0 },
                { "lpushx", REDIS_CMD_LPUSHX, &ArdbServer::LPushx, 2, 2, "w", 0 },
                { "lrange", REDIS_CMD_LRANGE, &ArdbServer::LRange, 3, 3, "r", 0 },
                { "lrem", REDIS_CMD_LREM, &ArdbServer::LRem, 3, 3, "w", 0 },
                { "lset", REDIS_CMD_LSET, &ArdbServer::LSet, 3, 3, "w", 0 },
                { "ltrim", REDIS_CMD_LTRIM, &ArdbServer::LTrim, 3, 3, "w", 0 },
                { "rpop", REDIS_CMD_RPOP, &ArdbServer::RPop, 1, 1, "w", 0 },
                { "rpush", REDIS_CMD_RPUSH, &ArdbServer::RPush, 2, -1, "w", 0 },
                { "rpushx", REDIS_CMD_RPUSHX, &ArdbServer::RPushx, 2, 2, "w", 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &ArdbServer::RPopLPush, 2, 2, "w", 0 },
                { "blpop", REDIS_CMD_BLPOP, &ArdbServer::BLPop, 2, -1, "w", 0 },
                { "brpop", REDIS_CMD_BRPOP, &ArdbServer::BRPop, 2, -1, "w", 0 },
                { "brpoplpush", REDIS_CMD_BRPOPLPUSH, &ArdbServer::BRPopLPush, 3, 3, "w", 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &ArdbServer::RPopLPush, 2, 2, "w", 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &ArdbServer::RPopLPush, 2, 2, "w", 0 },
                { "hclear", REDIS_CMD_HCLEAR, &ArdbServer::HClear, 1, 1, "w", 0 },
                { "zclear", REDIS_CMD_ZCLEAR, &ArdbServer::ZClear, 1, 1, "w", 0 },
                { "sclear", REDIS_CMD_SCLEAR, &ArdbServer::SClear, 1, 1, "w", 0 },
                { "lclear", REDIS_CMD_LCLEAR, &ArdbServer::LClear, 1, 1, "w", 0 },
                { "move", REDIS_CMD_MOVE, &ArdbServer::Move, 2, 2, "w", 0 },
                { "rename", REDIS_CMD_RENAME, &ArdbServer::Rename, 2, 2, "w", 0 },
                { "renamenx", REDIS_CMD_RENAMENX, &ArdbServer::RenameNX, 2, 2, "w", 0 },
                { "sort", REDIS_CMD_SORT, &ArdbServer::Sort, 1, -1, "w", 0 },
                { "keys", REDIS_CMD_KEYS, &ArdbServer::Keys, 1, 6, "r", 0 },
                { "keyscount", REDIS_CMD_KEYSCOUNT, &ArdbServer::KeysCount, 1, 6, "r", 0 },
                { "__set__", REDIS_CMD_RAWSET, &ArdbServer::RawSet, 2, 2, "w", 0 },
                { "__del__", REDIS_CMD_RAWDEL, &ArdbServer::RawDel, 1, 1, "w", 0 },
                { "eval", REDIS_CMD_EVAL, &ArdbServer::Eval, 2, -1, "s", 0 },
                { "evalsha", REDIS_CMD_EVALSHA, &ArdbServer::EvalSHA, 2, -1, "s", 0 },
                { "script", REDIS_CMD_SCRIPT, &ArdbServer::Script, 1, -1, "s", 0 },
                { "randomkey", REDIS_CMD_RANDOMKEY, &ArdbServer::Randomkey, 0, 0, "r", 0 },
                { "scan", REDIS_CMD_SCAN, &ArdbServer::Scan, 1, 5, "r", 0 },
                { "geoadd", REDIS_CMD_GEO_ADD, &ArdbServer::GeoAdd, 5, -1, "w", 0 },
                { "geosearch", REDIS_CMD_GEO_SEARCH, &ArdbServer::GeoSearch, 5, -1, "r", 0 },
                { "cache", REDIS_CMD_CACHE, &ArdbServer::Cache, 2, 2, "r", 0 },
                { "auth", REDIS_CMD_AUTH, &ArdbServer::Auth, 1, 1, "r", 0 },
                { "pfadd", REDIS_CMD_PFADD, &ArdbServer::PFAdd, 2, -1, "w", 0 },
                { "pfcount", REDIS_CMD_PFCOUNT, &ArdbServer::PFCount, 1, -1, "w", 0 },
                { "pfmerge", REDIS_CMD_PFMERGE, &ArdbServer::PFMerge, 2, -1, "w", 0 }, };

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
            m_handler_table[settingTable[i].name] = settingTable[i];
        }
    }
    ArdbServer::~ArdbServer()
    {

    }

    LUAInterpreter*
    ArdbServer::LUAInterpreterCreator(void* data)
    {
        ArdbServer* server = (ArdbServer*) data;
        return new LUAInterpreter(server);
    }

    ArdbServer::RedisCommandHandlerSetting *
    ArdbServer::FindRedisCommandHandlerSetting(const std::string & cmd)
    {
        RedisCommandHandlerSettingTable::iterator found = m_handler_table.find(cmd);
        if (found != m_handler_table.end())
        {
            return &(found->second);
        }
        return NULL;
    }

    int ArdbServer::ProcessRedisCommand(ArdbConnContext& ctx, RedisCommandFrame& args, int flags)
    {
        m_ctx_local.SetValue(&ctx);
        if (m_cfg.timeout > 0)
        {
            TouchIdleConn(ctx.conn);
        }
        std::string& cmd = args.GetMutableCommand();
        lower_string(cmd);
        RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(cmd);
        DEBUG_LOG("Process recved cmd:%s with flags:%d", args.ToString().c_str(), flags);
        ServerStat::GetSingleton().IncRecvCommands();

        int ret = 0;
        if (NULL != setting)
        {
            args.SetType(setting->type);
            //Check if the user is authenticated
            if (!ctx.authenticated && setting->type != REDIS_CMD_AUTH && setting->type != REDIS_CMD_QUIT)
            {
                fill_fix_error_reply(ctx.reply, "NOAUTH Authentication required");
                return ret;
            }

            if (!m_cfg.slave_serve_stale_data && !m_cfg.master_host.empty() && !m_slave_client.IsSynced())
            {
                if (setting->type == REDIS_CMD_INFO || setting->type == REDIS_CMD_SLAVEOF)
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
            if (!m_cfg.master_host.empty() && (setting->flags & ARDB_CMD_WRITE) && !(flags & ARDB_PROCESS_REPL_WRITE))
            {
                if (m_cfg.slave_readonly)
                {
                    fill_error_reply(ctx.reply, "server is read only slave");
                    return ret;
                }
                else
                {
                    /*
                     * Do NOT feed replication log while current instance is not a readonly slave
                     */
                    flags |= ARDB_PROCESS_WITHOUT_REPLICATION;
                }
            }
            bool valid_cmd = true;
            if (setting->min_arity > 0)
            {
                valid_cmd = args.GetArguments().size() >= (uint32) setting->min_arity;
            }
            if (setting->max_arity >= 0 && valid_cmd)
            {
                valid_cmd = args.GetArguments().size() <= (uint32) setting->max_arity;
            }

            if (!valid_cmd)
            {
                fill_error_reply(ctx.reply, "wrong number of arguments for '%s' command", cmd.c_str());
            }
            else
            {
                if (ctx.IsInTransaction() && (cmd != "multi" && cmd != "exec" && cmd != "discard" && cmd != "quit"))
                {
                    ctx.GetTransc().transaction_cmds.push_back(args);
                    fill_status_reply(ctx.reply, "QUEUED");
                }
                else if (ctx.IsSubscribedConn()
                        && (cmd != "subscribe" && cmd != "psubscribe" && cmd != "unsubscribe" && cmd != "punsubscribe"
                                && cmd != "quit"))
                {
                    fill_error_reply(ctx.reply, "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
                }
                else
                {
                    if (!(flags & ARDB_PROCESS_FEED_REPLICATION_ONLY))
                    {
                        ret = DoRedisCommand(ctx, setting, args);
                    }
                }
            }
        }
        else
        {
            ERROR_LOG("No handler found for:%s with size:%u", cmd.c_str(), cmd.size());
            ERROR_LOG("Invalid command's ascii codes:%s", ascii_codes(cmd).c_str());
            fill_error_reply(ctx.reply, "unknown command '%s'", cmd.c_str());
            return 0;
        }

        /*
         * Feed commands which modified data
         */
        DBWatcher& watcher = m_db->GetDBWatcher();
        if (m_repl_backlog.IsInited() && (flags & ARDB_PROCESS_WITHOUT_REPLICATION) == 0)
        {
            if ((flags & ARDB_PROCESS_FEED_REPLICATION_ONLY) || (flags & ARDB_PROCESS_FORCE_REPLICATION))
            {
                //feed to replication
                m_master_serv.FeedSlaves(ctx.conn, ctx.currentDB, args);
            }
            else if (watcher.data_changed)
            {
                if (args.GetType() == REDIS_CMD_EXEC)
                {
                    //feed to replication
                    if (!ctx.GetTransc().transaction_cmds.empty())
                    {
                        RedisCommandFrame multi("MULTI");
                        m_master_serv.FeedSlaves(ctx.conn, ctx.currentDB, multi);
                        for (uint32 i = 0; i < ctx.GetTransc().transaction_cmds.size(); i++)
                        {
                            RedisCommandFrame& transc_cmd = ctx.GetTransc().transaction_cmds.at(i);
                            m_master_serv.FeedSlaves(ctx.conn, ctx.currentDB, transc_cmd);
                        }
                        RedisCommandFrame exec("EXEC");
                        m_master_serv.FeedSlaves(ctx.conn, ctx.currentDB, exec);
                    }
                }
                else if ((setting->flags & ARDB_CMD_WRITE))
                {
                    m_master_serv.FeedSlaves(ctx.conn, ctx.currentDB, args);
                }
            }
        }
        watcher.Clear();
        if (args.GetType() == REDIS_CMD_EXEC)
        {
            ctx.ClearTransaction();
        }
        return ret;
    }

    int ArdbServer::DoRedisCommand(ArdbConnContext& ctx, RedisCommandHandlerSetting* setting, RedisCommandFrame& args)
    {
        const std::string& cmd = args.GetCommand();
        if (m_clients_holder.IsStatEnable())
        {
            m_clients_holder.TouchConn(ctx.conn, cmd);
        }

        uint64 start_time = get_current_epoch_micros();
        int ret = (this->*(setting->handler))(ctx, args);
        uint64 stop_time = get_current_epoch_micros();

        if (m_cfg.slowlog_log_slower_than && (stop_time - start_time) > (uint64) m_cfg.slowlog_log_slower_than)
        {
            m_slowlog_handler.PushSlowCommand(args, stop_time - start_time);
        }
        return ret;
    }

    static void conn_pipeline_init(ChannelPipeline* pipeline, void* data)
    {
        ArdbServer* serv = (ArdbServer*) data;
        pipeline->AddLast("decoder", new RedisCommandDecoder);
        pipeline->AddLast("encoder", new RedisReplyEncoder);
        pipeline->AddLast("handler", new RedisRequestHandler(serv));
    }

    static void conn_pipeline_finallize(ChannelPipeline* pipeline, void* data)
    {
        ChannelHandler* handler = pipeline->Get("decoder");
        DELETE(handler);
        handler = pipeline->Get("encoder");
        DELETE(handler);
        RedisRequestHandler* rhandler = (RedisRequestHandler*) pipeline->Get("handler");
        if (NULL != rhandler && rhandler->processing)
        {
            rhandler->delete_after_processing = true;
        }
        else
        {
            DELETE(handler);
        }

    }

    void RedisRequestHandler::MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e)
    {
        ardbctx.conn = ctx.GetChannel();
        processing = true;
        RedisCommandFrame* cmd = e.GetMessage();
        ChannelService& serv = ardbctx.conn->GetService();
        uint32 channel_id = ardbctx.conn_id;
        int ret = server->ProcessRedisCommand(ardbctx, *cmd, 0);
        if (ret >= 0 && ardbctx.reply.type != 0)
        {
            ardbctx.conn->Write(ardbctx.reply);
            ardbctx.reply.Clear();
        }
        if (ret < 0 && serv.GetChannel(channel_id) != NULL)
        {
            ardbctx.conn->Close();
        }
        processing = false;
        if (delete_after_processing)
        {
            delete this;
        }
    }
    void RedisRequestHandler::ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        server->m_clients_holder.EraseConn(ctx.GetChannel());
        server->ClearClosedConnContext(ardbctx);
        ServerStat::GetSingleton().DecAcceptedClient();
        ardbctx.conn_id = 0;
    }

    void RedisRequestHandler::ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e)
    {
        if (server->m_cfg.timeout > 0)
        {
            server->TouchIdleConn(ctx.GetChannel());
        }
        ServerStat::GetSingleton().IncAcceptedClient();
        if (!server->m_cfg.requirepass.empty())
        {
            ardbctx.authenticated = false;
        }
        ardbctx.conn_id = ctx.GetChannel()->GetID();
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

    Timer& ArdbServer::GetTimer()
    {
        return m_service->GetTimer();
    }

    void ArdbServer::ServerEventCallback(ChannelService* serv, uint32 ev, void* data)
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

    void ArdbServer::RenameCommand()
    {
        StringStringMap::iterator it = m_cfg.rename_commands.begin();
        while (it != m_cfg.rename_commands.end())
        {
            std::string cmd = string_tolower(it->first);
            RedisCommandHandlerSettingTable::iterator found = m_handler_table.find(cmd);
            if (found != m_handler_table.end())
            {
                RedisCommandHandlerSetting setting = found->second;
                m_handler_table.erase(found);
                m_handler_table[it->second] = setting;
            }
            it++;
        }
    }

    int ArdbServer::Start(const Properties& props)
    {
        m_cfg_props = props;
        if (ParseConfig(props, m_cfg) < 0)
        {
            ERROR_LOG("Failed to parse configurations.");
            return -1;
        }
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

        if (0 != chdir(m_cfg.home.c_str()))
        {
            ERROR_LOG("Faild to change dir to home:%s", m_cfg.home.c_str());
            return -1;
        }

        RenameCommand();

        m_db = new Ardb(&m_engine, (uint32) m_cfg.worker_count);
        if (!m_db->Init(m_cfg.db_cfg))
        {
            ERROR_LOG("Failed to init DB.");
            return -1;
        }
        m_service = new ChannelService(m_cfg.max_clients + 32);
        ChannelOptions ops;
        ops.tcp_nodelay = true;
        if (m_cfg.tcp_keepalive > 0)
        {
            ops.keep_alive = m_cfg.tcp_keepalive;
        }
        if (m_cfg.listen_host.empty() && m_cfg.listen_unix_path.empty())
        {
            m_cfg.listen_host = "0.0.0.0";
            if (m_cfg.listen_ports.empty())
            {
                m_cfg.listen_ports.insert(6379);
            }
        }

        uint32 thread_pool_size = m_cfg.worker_count;
        if (!m_cfg.listen_host.empty())
        {
            thread_pool_size = thread_pool_size * m_cfg.listen_ports.size();
        }

        if (!m_cfg.listen_host.empty())
        {
            PortSet::iterator pit = m_cfg.listen_ports.begin();
            uint32 j = 0;
            while (pit != m_cfg.listen_ports.end())
            {
                SocketHostAddress address(m_cfg.listen_host.c_str(), *pit);
                ServerSocketChannel* server = m_service->NewServerSocketChannel();
                if (!server->Bind(&address))
                {
                    ERROR_LOG("Failed to bind on %s:%d", m_cfg.listen_host.c_str(), *pit);
                    goto sexit;
                }
                server->Configure(ops);
                server->SetChannelPipelineInitializor(conn_pipeline_init, this);
                server->SetChannelPipelineFinalizer(conn_pipeline_finallize, NULL);
                server->BindThreadPool(j * m_cfg.worker_count, (j+1) * m_cfg.worker_count);
                j++;
                pit++;
            }
        }
        if (!m_cfg.listen_unix_path.empty())
        {
            SocketUnixAddress address(m_cfg.listen_unix_path);
            ServerSocketChannel* server = m_service->NewServerSocketChannel();
            if (!server->Bind(&address))
            {
                ERROR_LOG("Failed to bind on %s", m_cfg.listen_unix_path.c_str());
                goto sexit;
            }
            server->Configure(ops);
            server->SetChannelPipelineInitializor(conn_pipeline_init, this);
            server->SetChannelPipelineFinalizer(conn_pipeline_finallize, NULL);
            chmod(m_cfg.listen_unix_path.c_str(), m_cfg.unixsocketperm);
        }
        ArdbLogger::InitDefaultLogger(m_cfg.loglevel, m_cfg.logfile);

        m_rdb.Init(m_db);
        if (0 == m_repl_backlog.Init(this))
        {
            if (0 != m_master_serv.Init())
            {
                goto sexit;
            }
            if (0 != m_slave_client.Init())
            {
                goto sexit;
            }
        }

        if (!m_cfg.master_host.empty())
        {
            m_slave_client.SetIncludeDBs(m_cfg.repl_includes);
            m_slave_client.ConnectMaster(m_cfg.master_host, m_cfg.master_port);
        }

        if (0 != m_ha_agent.Init(this))
        {
            goto sexit;
        }
        m_service->GetTimer().Schedule(&(ServerStat::GetSingleton()), 1, 1, SECONDS);
        m_service->SetThreadPoolSize(thread_pool_size);
        m_service->RegisterUserEventCallback(ArdbServer::ServerEventCallback, this);
        INFO_LOG("Server started, Ardb version %s", ARDB_VERSION);
        INFO_LOG("The server is now ready to accept connections on port %s", string_join_container(m_cfg.listen_ports, ",").c_str());

        m_service->Start();
        sexit: m_master_serv.Stop();
        m_ha_agent.Close();
        DELETE(m_service);
        DELETE(m_db);
        ArdbLogger::DestroyDefaultLogger();
        return 0;
    }
}

