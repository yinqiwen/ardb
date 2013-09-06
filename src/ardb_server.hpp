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
#include <btree_map.h>
#include <btree_set.h>
#include "channel/all_includes.hpp"
#include "util/config_helper.hpp"
#include "util/thread/thread_local.hpp"
#include "ardb.hpp"
#include "replication/slave.hpp"
#include "replication/master.hpp"
#include "lua_scripting.hpp"

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

using namespace ardb::codec;
namespace ardb
{
	struct ArdbServerConfig
	{
			bool daemonize;
			std::string listen_host;
			int64 listen_port;
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

			int64 repl_ping_slave_period;
			int64 repl_timeout;
			int64 repl_backlog_size;
			int64 repl_state_persist_period;

			int64 lua_time_limit;

			std::string master_host;
			uint32 master_port;

			DBIDSet syncdbs;

			int64 worker_count;
			std::string loglevel;
			std::string logfile;
			ArdbServerConfig() :
					daemonize(false), listen_port(0), unixsocketperm(755), max_clients(10000), tcp_keepalive(0), timeout(0), slowlog_log_slower_than(10000), slowlog_max_len(128), repl_data_dir("./repl"), backup_dir("./backup"), repl_ping_slave_period(10), repl_timeout(60), repl_backlog_size(100 * 1024 * 1024), repl_state_persist_period(1), lua_time_limit(0), master_port(0), worker_count(1), loglevel(
							"INFO")
			{
			}
	};
	class ArdbConnContext;
	typedef btree::btree_set<ArdbConnContext*> ContextSet;

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
			typedef btree::btree_map<uint32, ArdbConncetion> ArdbConncetionTable;
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
	typedef std::set<WatchKey> WatchKeySet;
	typedef std::set<std::string> PubSubChannelSet;

	struct ArdbConnContext
	{
			DBID currentDB;
			Channel* conn;
			RedisReply reply;
			bool in_transaction;
			bool fail_transc;
			bool is_slave_conn;
			TransactionCommandQueue* transaction_cmds;
			WatchKeySet* watch_key_set;
			PubSubChannelSet* pubsub_channle_set;
			PubSubChannelSet* pattern_pubsub_channle_set;

			uint64 lua_time_start;
			bool lua_timeout;
			bool lua_kill;
			const char* lua_executing_func;

			ArdbConnContext() :
					currentDB(0), conn(NULL), in_transaction(false), fail_transc(false), is_slave_conn(false), transaction_cmds(
					NULL), watch_key_set(NULL), pubsub_channle_set(
					NULL), pattern_pubsub_channle_set(NULL), lua_time_start(0), lua_timeout(false), lua_kill(false), lua_executing_func(
					NULL)
			{
			}
			uint64 SubChannelSize()
			{
				uint32 size = 0;
				if (NULL != pubsub_channle_set)
				{
					size = pubsub_channle_set->size();
				}
				if (NULL != pattern_pubsub_channle_set)
				{
					size += pattern_pubsub_channle_set->size();
				}
				return size;
			}
			bool IsSubscribedConn()
			{
				return NULL != pubsub_channle_set || NULL != pattern_pubsub_channle_set;
			}
			bool IsInTransaction()
			{
				return in_transaction && NULL != transaction_cmds;
			}
			bool IsInScripting()
			{
				return lua_executing_func != NULL;
			}
			void ClearTransactionContex()
			{
				in_transaction = false;
				fail_transc = false;
				DELETE(transaction_cmds);
			}
			~ArdbConnContext()
			{
				DELETE(transaction_cmds);
				DELETE(watch_key_set);
				DELETE(pubsub_channle_set);
				DELETE(pattern_pubsub_channle_set);
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

			typedef btree::btree_map<std::string, RedisCommandHandlerSetting> RedisCommandHandlerSettingTable;
			typedef btree::btree_map<WatchKey, ContextSet> WatchKeyContextTable;
			typedef btree::btree_map<std::string, ContextSet> PubSubContextTable;

			RedisCommandHandlerSettingTable m_handler_table;
			SlowLogHandler m_slowlog_handler;
			ClientConnHolder m_clients_holder;
			Master m_master_serv;
			Slave m_slave_client;
			ArdbDumpFile m_rdb;

			WatchKeyContextTable m_watch_context_table;
			ThreadMutex m_watch_mutex;
			PubSubContextTable m_pubsub_context_table;
			PubSubContextTable m_pattern_pubsub_context_table;
			ThreadMutex m_pubsub_mutex;
			//ArdbConnContext* m_current_ctx;
			ThreadLocal<ArdbConnContext*> m_ctx_local;
			ThreadLocal<LUAInterpreter> m_ctx_lua;

			RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(std::string& cmd);
			int DoRedisCommand(ArdbConnContext& ctx, RedisCommandHandlerSetting* setting, RedisCommandFrame& cmd);
			int ProcessRedisCommand(ArdbConnContext& ctx, RedisCommandFrame& cmd, int flags);

			void HandleReply(ArdbConnContext* ctx);

			friend class RedisRequestHandler;
			friend class Slave;
			friend class Master;
			friend class Backup;
			friend class LUAInterpreter;

			void FillInfoResponse(const std::string& section, std::string& content);

			static void OnKeyUpdated(const DBID& dbid, const Slice& key, void* data);
			//int OnAllKeyDeleted(const DBID& dbid);
			void ClearWatchKeys(ArdbConnContext& ctx);
			void ClearSubscribes(ArdbConnContext& ctx);

			void TouchIdleConn(Channel* ch);

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
			int SRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int SRevRange(ArdbConnContext& ctx, RedisCommandFrame& cmd);

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

			int TCreate(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TLen(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TInsert(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TGet(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TGetAll(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TUpdate(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TDel(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TDelCol(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TCreateIndex(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int TDesc(ArdbConnContext& ctx, RedisCommandFrame& cmd);

			int HClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int SClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int ZClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int LClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);

			int Eval(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int EvalSHA(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int Script(ArdbConnContext& ctx, RedisCommandFrame& cmd);

			Timer& GetTimer();
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
			~ArdbServer();
	};
}

#endif /* ARDB_SERVER_HPP_ */
