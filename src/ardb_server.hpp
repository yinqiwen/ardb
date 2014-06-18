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

#ifndef ARDB_SERVER_HPP_
#define ARDB_SERVER_HPP_
#include <string>
#include "channel/all_includes.hpp"
#include "util/config_helper.hpp"
#include "util/thread/thread_local.hpp"
#include "util/redis_helper.hpp"
#include "db.hpp"
#include "replication/slave.hpp"
#include "replication/master.hpp"
#include "ha/agent.hpp"
#include "lua_scripting.hpp"
#include "stat.hpp"

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

#define ARDB_AUTHPASS_MAX_LEN 512

#define SCRIPT_KILL_EVENT 1
#define SCRIPT_FLUSH_EVENT 2

#define CHECK_ARDB_RETURN_VALUE(reply, ret) do{\
    switch(ret){\
        case ERR_INVALID_ARGS: ardb::fill_error_reply(reply, "Invalid arguments."); return 0;\
        case ERR_INVALID_TYPE: ardb::fill_error_reply(reply, "Operation against a key holding the wrong kind of value."); return 0;\
        case ERR_INVALID_HLL_TYPE: ardb::fill_fix_error_reply(reply, "WRONGTYPE Key is not a valid HyperLogLog string value."); return 0;\
        case ERR_CORRUPTED_HLL_VALUE: ardb::fill_error_reply(reply, "INVALIDOBJ Corrupted HLL object detected."); return 0;\
        case ERR_STORAGE_ENGINE_INTERNAL: ardb::fill_error_reply(reply, "Storage engine internal error:%s.", m_db->LastError().c_str()); return 0;\
        default:break; \
    }\
}while(0)

using namespace ardb::codec;
namespace ardb
{
    typedef std::set<uint16> PortSet;
    struct ArdbServerConfig
    {
            bool daemonize;
            std::string listen_host;

            PortSet listen_ports;
            std::string listen_unix_path;
            int64 unixsocketperm;
            int64 max_clients;
            int64 tcp_keepalive;
            int64 timeout;
            std::string home;
            std::string data_base_path;
            int64 slowlog_log_slower_than;
            int64 slowlog_max_len;

            std::string repl_data_dir;
            std::string backup_dir;
            bool backup_redis_format;

            int64 repl_ping_slave_period;
            int64 repl_timeout;
            int64 repl_backlog_size;
            int64 repl_state_persist_period;
            int64 repl_backlog_time_limit;
            bool slave_cleardb_before_fullresync;
            bool slave_readonly;
            bool slave_serve_stale_data;
            int64 slave_priority;

            int64 lua_time_limit;

            std::string master_host;
            uint32 master_port;

            DBIDArray repl_includes;
            DBIDArray repl_excludes;

            int64 worker_count;
            std::string loglevel;
            std::string logfile;

            std::string pidfile;

            std::string zookeeper_servers;

            std::string additional_misc_info;

            std::string requirepass;

            StringStringMap rename_commands;

            ArdbConfig db_cfg;
            ArdbServerConfig() :
                    daemonize(false), unixsocketperm(755), max_clients(10000), tcp_keepalive(0), timeout(0), slowlog_log_slower_than(
                            10000), slowlog_max_len(128), repl_data_dir("./repl"), backup_dir("./backup"), backup_redis_format(
                            false), repl_ping_slave_period(10), repl_timeout(60), repl_backlog_size(100 * 1024 * 1024), repl_state_persist_period(
                            1), repl_backlog_time_limit(3600), slave_cleardb_before_fullresync(true), slave_readonly(
                            true), slave_serve_stale_data(true), slave_priority(100), lua_time_limit(0), master_port(0), worker_count(
                            1), loglevel("INFO")
            {
            }
    };
    struct ArdbConnContext;
    typedef TreeSet<ArdbConnContext*>::Type ContextSet;
    typedef std::deque<ArdbConnContext*> ContextDeque;

    struct ArdbConncetion
    {
            Channel* conn;
            std::string name;
            std::string addr;
            uint32 birth;
            uint32 lastTs;
            DBID currentDB;
            std::string lastCmd;
            ArdbConncetion() :
                    conn(NULL), birth(0), lastTs(0), currentDB(0)
            {
            }
    };

    class ClientConnHolder
    {
        private:
            typedef TreeMap<uint32, ArdbConncetion>::Type ArdbConncetionTable;
            ArdbConncetionTable m_conn_table;
            bool m_client_stat_enable;
            ThreadMutex m_mutex;
        public:
            ClientConnHolder() :
                    m_client_stat_enable(false)
            {
            }
            void TouchConn(Channel* conn, const std::string& currentCmd);
            void ChangeCurrentDB(Channel* conn, const DBID& dbid);
            Channel* GetConn(const std::string& addr);
            void SetName(Channel* conn, const std::string& name);
            const std::string& GetName(Channel* conn);
            void List(RedisReply& reply);
            void EraseConn(Channel* conn)
            {
                LockGuard<ThreadMutex> guard(m_mutex);
                m_conn_table.erase(conn->GetID());
            }
            bool IsStatEnable()
            {
                return m_client_stat_enable;
            }
            void SetStatEnable(bool on)
            {
                m_client_stat_enable = on;
            }
    };

    class SlowLogHandler
    {
        private:
            const ArdbServerConfig& m_cfg;
            struct SlowLog
            {
                    uint64 id;
                    uint64 ts;
                    uint64 costs;
                    RedisCommandFrame cmd;
            };
            std::deque<SlowLog> m_cmds;
            ThreadMutex m_mutex;
        public:
            SlowLogHandler(const ArdbServerConfig& cfg) :
                    m_cfg(cfg)
            {
            }
            void PushSlowCommand(const RedisCommandFrame& cmd, uint64 micros);
            void GetSlowlog(uint32 len, RedisReply& reply);
            void Clear()
            {
                m_cmds.clear();
            }
            uint32 Size()
            {
                return m_cmds.size();
            }
    };

    struct WatchKey
    {
            DBID db;
            std::string key;
            WatchKey() :
                    db(0)
            {
            }
            WatchKey(const DBID& id, const std::string& k) :
                    db(id), key(k)
            {
            }
            inline bool operator<(const WatchKey& other) const
            {
                if (db > other.db)
                {
                    return false;
                }
                if (db == other.db)
                {
                    return key < other.key;
                }
                return true;
            }
    };

    typedef std::deque<RedisCommandFrame> TransactionCommandQueue;
    typedef TreeSet<WatchKey>::Type WatchKeySet;
    typedef TreeSet<std::string>::Type PubSubChannelSet;

    struct LUAConnContext
    {
            uint64 lua_time_start;
            bool lua_timeout;
            bool lua_kill;
            const char* lua_executing_func;

            LUAConnContext() :
                    lua_time_start(0), lua_timeout(false), lua_kill(false), lua_executing_func(NULL)
            {
            }
    };

    struct PubSubContext
    {
            PubSubChannelSet pubsub_channle_set;
            PubSubChannelSet pattern_pubsub_channle_set;
    };

    struct BlockListContext
    {
            std::string dest_key;
            int32 blocking_timer_task_id;
            uint8 pop_type;
            WatchKeySet keys;
            BlockListContext() :
                    blocking_timer_task_id(-1), pop_type(0)
            {
            }
    };

    struct BlockConnContext
    {
            ArdbConnContext* ctx;
            ChannelService* eventService;
            uint32 connId;
            BlockConnContext() :
                    ctx(NULL), eventService(NULL), connId(0)
            {
            }
    };
    struct BlockConnWakeContext
    {
            ArdbConnContext* ctx;
            WatchKey key;
            ArdbServer* server;
            BlockConnWakeContext() :
                    ctx(NULL), server(NULL)
            {
            }
    };
    typedef std::deque<BlockConnContext> BlockConnContextDeque;
    typedef TreeMap<WatchKey, BlockConnContextDeque>::Type BlockContextTable;

    struct TransactionContext
    {
            bool in_transaction;
            bool fail_transc;
            TransactionCommandQueue transaction_cmds;
            WatchKeySet watch_key_set;
            TransactionContext() :
                    in_transaction(false), fail_transc(false)
            {
            }
    };

    struct ArdbConnContext
    {
            DBID currentDB;
            Channel* conn;
            RedisReply reply;
            bool is_slave_conn;
            TransactionContext* transc;
            PubSubContext* pubsub;
            LUAConnContext* lua;
            BlockListContext* block;

            bool authenticated;
            uint32 conn_id;

            ArdbConnContext() :
                    currentDB(0), conn(NULL), is_slave_conn(false), transc(NULL), pubsub(
                    NULL), lua(
                    NULL), block(NULL), authenticated(true), conn_id(0)
            {
            }
            LUAConnContext& GetLua()
            {
                if (NULL == lua)
                {
                    NEW(lua, LUAConnContext);
                }
                return *lua;
            }
            TransactionContext& GetTransc()
            {
                if (NULL == transc)
                {
                    NEW(transc, TransactionContext);
                }
                return *transc;
            }
            PubSubContext& GetPubSub()
            {
                if (NULL == pubsub)
                {
                    NEW(pubsub, PubSubContext);
                }
                return *pubsub;
            }
            BlockListContext& GetBlockList()
            {
                if (NULL == block)
                {
                    NEW(block, BlockListContext);
                }
                return *block;
            }
            void ClearBlockList()
            {
                DELETE(block);
            }
            void ClearPubSub()
            {
                DELETE(pubsub);
            }
            uint64 SubChannelSize()
            {
                uint32 size = 0;
                if (NULL != pubsub)
                {
                    size = pubsub->pattern_pubsub_channle_set.size();
                    size += pubsub->pubsub_channle_set.size();
                }
                return size;
            }
            bool IsSubscribedConn()
            {
                return NULL != pubsub;
            }
            bool IsInTransaction()
            {
                return NULL != transc && transc->in_transaction;
            }
            bool IsInScripting()
            {
                return lua != NULL && lua->lua_executing_func != NULL;
            }
            void ClearTransaction()
            {
                DELETE(transc);
            }
            ~ArdbConnContext()
            {
                DELETE(transc);
                DELETE(pubsub);
                DELETE(block);
                DELETE(lua);
            }
    };

    class ArdbServer;
    struct RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
    {
            ArdbServer* server;
            ArdbConnContext ardbctx;
            bool processing;
            bool delete_after_processing;
            void MessageReceived(ChannelHandlerContext& ctx, MessageEvent<RedisCommandFrame>& e);
            void ChannelClosed(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            void ChannelConnected(ChannelHandlerContext& ctx, ChannelStateEvent& e);
            RedisRequestHandler(ArdbServer* s) :
                    server(s), processing(false), delete_after_processing(false)
            {
            }
    };

    class ArdbServer
    {
        public:
            typedef int (ArdbServer::*RedisCommandHandler)(ArdbConnContext&, RedisCommandFrame&);

            static LUAInterpreter* LUAInterpreterCreator(void* data);
            struct RedisCommandHandlerSetting
            {
                    const char* name;
                    RedisCommandType type;
                    RedisCommandHandler handler;
                    int min_arity;
                    int max_arity;
                    const char* sflags;
                    int flags;
            };
        private:
            ArdbServerConfig m_cfg;
            Properties m_cfg_props;
            ChannelService* m_service;
            Ardb* m_db;
            KeyValueEngineFactory& m_engine;

            typedef TreeMap<std::string, RedisCommandHandlerSetting>::Type RedisCommandHandlerSettingTable;
            typedef TreeMap<WatchKey, ContextSet>::Type WatchKeyContextTable;
            typedef TreeMap<std::string, ContextSet>::Type PubSubContextTable;

            struct BlockTimeoutTask: public Runnable
            {
                    ArdbServer* server;
                    ArdbConnContext* ctx;
                    ChannelService* event_service;
                    uint32 conn_id;
                    BlockTimeoutTask(ArdbServer* s, ArdbConnContext* c) :
                            server(s), ctx(c), event_service(NULL), conn_id(0)
                    {
                        event_service = &(c->conn->GetService());
                        conn_id = ctx->conn_id;
                    }
                    void Run();
            };

            RedisCommandHandlerSettingTable m_handler_table;
            SlowLogHandler m_slowlog_handler;
            ClientConnHolder m_clients_holder;
            ReplBacklog m_repl_backlog;
            Master m_master_serv;
            Slave m_slave_client;
            ArdbDumpFile m_rdb;
            RedisDumpFile m_redis_rdb;

            ZKAgent m_ha_agent;

            WatchKeyContextTable m_watch_context_table;
            ThreadMutex m_watch_mutex;
            PubSubContextTable m_pubsub_context_table;
            PubSubContextTable m_pattern_pubsub_context_table;
            ThreadMutex m_pubsub_mutex;
            BlockContextTable m_blocking_conns;
            ThreadMutex m_block_mutex;

            ThreadLocal<ArdbConnContext*> m_ctx_local;
            ThreadLocal<LUAInterpreter> m_ctx_lua;

            RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(const std::string& cmd);
            int DoRedisCommand(ArdbConnContext& ctx, RedisCommandHandlerSetting* setting, RedisCommandFrame& cmd);
            int ProcessRedisCommand(ArdbConnContext& ctx, RedisCommandFrame& cmd, int flags);

            void HandleReply(ArdbConnContext* ctx);

            //static void AsyncWriteBlockListReply(Channel* ch, void * data);
            static void WakeBlockedConnOnList(Channel* ch, void * data);

            friend class RedisRequestHandler;
            friend class Slave;
            friend class Master;
            friend class Backup;
            friend class ReplBacklog;
            friend class LUAInterpreter;
            friend class ZKAgent;

            void FillInfoResponse(const std::string& section, std::string& content);

            static void OnKeyUpdated(const DBID& dbid, const Slice& key, void* data);
            //int OnAllKeyDeleted(const DBID& dbid);
            void ClearWatchKeys(ArdbConnContext& ctx);
            void ClearSubscribes(ArdbConnContext& ctx);
            void ClearClosedConnContext(ArdbConnContext& ctx);

            void TouchIdleConn(Channel* ch);
            void BlockConn(ArdbConnContext& ctx, uint32 timeout);
            void UnblockConn(ArdbConnContext& ctx);
            void CheckBlockingConnectionsByListKey(const WatchKey& key);

            DataDumpFile& GetDataDumpFile();

            int Time(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int FlushDB(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int FlushAll(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Save(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LastSave(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int BGSave(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Import(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Info(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int DBSize(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Config(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SlowLog(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Client(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Keys(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int KeysCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Randomkey(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Scan(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Multi(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Discard(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Exec(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Watch(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int UnWatch(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Subscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int UnSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PUnSubscribe(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Publish(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Slaveof(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Sync(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PSync(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ReplConf(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RawSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RawDel(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Ping(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Echo(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Select(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Quit(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Shutdown(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Type(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Move(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Rename(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RenameNX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Sort(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int CompactDB(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int CompactAll(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Append(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Decr(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Decrby(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Get(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int GetRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int GetSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Incr(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Incrby(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int IncrbyFloat(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int MGet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int MSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int MSetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PSetEX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SetEX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SetRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Strlen(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Set(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Del(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Exists(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Expire(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Expireat(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Persist(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PExpire(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PExpireat(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PTTL(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int TTL(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Bitcount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Bitop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int BitopCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SetBit(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int GetBit(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int HDel(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HExists(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HGet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HGetAll(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HMIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HIncrbyFloat(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HKeys(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HLen(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HMGet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HMSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HSetNX(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HVals(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int HScan(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int SAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SCard(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SDiff(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SDiffStore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SInter(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SInterStore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SIsMember(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SMembers(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SMove(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SRandMember(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SRem(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SUnion(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SUnionStore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SUnionCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SInterCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SDiffCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SScan(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int ZAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZCard(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZIncrby(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRank(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRem(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByRank(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRevRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRevRangeByScore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRevRank(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZInterStore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZUnionStore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZScore(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZScan(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZLexCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRangeByLex(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZRemRangeByLex(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int LIndex(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LInsert(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LLen(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LPush(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LPushx(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LRem(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LSet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LTrim(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RPopLPush(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RPush(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int RPushx(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int BLPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int BRPop(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int BRPopLPush(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int HClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int SClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int ZClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int LClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int Eval(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int EvalSHA(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Script(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int GeoAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int GeoSearch(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int AreaAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int AreaDel(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int AreaClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int AreaLocate(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Cache(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int Auth(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            int PFAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PFCount(ArdbConnContext& ctx, RedisCommandFrame& cmd);
            int PFMerge(ArdbConnContext& ctx, RedisCommandFrame& cmd);

            Timer& GetTimer();

            static void ServerEventCallback(ChannelService* serv, uint32 ev, void* data);
            void RenameCommand();
        public:
            static int ParseConfig(const Properties& props, ArdbServerConfig& cfg);
            ArdbServer(KeyValueEngineFactory& engine);
            const ArdbServerConfig& GetServerConfig()
            {
                return m_cfg;
            }
            ArdbConnContext* GetCurrentContext()
            {
                return m_ctx_local.GetValue();
            }
            int Start(const Properties& props);

            Master& GetMaster()
            {
                return m_master_serv;
            }
            Ardb& GetDB()
            {
                return *m_db;
            }
            const ArdbServerConfig& GetConfig()
            {
                return m_cfg;
            }
            ~ArdbServer();
    };
}

#endif /* ARDB_SERVER_HPP_ */
