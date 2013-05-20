/*
 * ardb_server.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
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
#include "replication.hpp"

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
			std::string data_base_path;
			int64 slowlog_log_slower_than;
			int64 slowlog_max_len;

			std::string repl_data_dir;
			std::string backup_dir;

			int64 repl_ping_slave_period;
			int64 repl_timeout;
			int64 rep_backlog_size;
			int64 repl_syncstate_persist_period;

			std::string master_host;
			uint32 master_port;

			bool repl_log_enable;
			int64 worker_count;
			std::string loglevel;
			std::string logfile;
			ArdbServerConfig() :
					daemonize(false), listen_port(0), unixsocketperm(755), max_clients(
							10000), tcp_keepalive(0), slowlog_log_slower_than(
							10000), slowlog_max_len(128), repl_data_dir(
							"./repl"), backup_dir("./backup"), repl_ping_slave_period(
							10), repl_timeout(60), rep_backlog_size(1000000), repl_syncstate_persist_period(
							1), master_port(0), repl_log_enable(false), worker_count(1), loglevel(
							"INFO")
			{
			}
	};

	struct WatchKey
	{
			DBID db;
			std::string key;
			WatchKey()
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
			ArdbConnContext() :
					currentDB("0"), conn(NULL), in_transaction(false), fail_transc(
							false), is_slave_conn(false), transaction_cmds(
							NULL), watch_key_set(NULL), pubsub_channle_set(
							NULL), pattern_pubsub_channle_set(NULL)
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
				return NULL != pubsub_channle_set
						|| NULL != pattern_pubsub_channle_set;
			}
			bool IsInTransaction()
			{
				return in_transaction && NULL != transaction_cmds;
			}
			~ArdbConnContext()
			{
				DELETE(transaction_cmds);
				DELETE(watch_key_set);
				DELETE(pubsub_channle_set);
				DELETE(pattern_pubsub_channle_set);
			}
	};

	typedef btree::btree_set<ArdbConnContext*> ContextSet;

	struct ArdbConncetion
	{
			Channel* conn;
			std::string name;
			std::string addr;
			uint32 birth;
			uint32 lastTs;
			std::string currentDB;
			std::string lastCmd;
			ArdbConncetion() :
					conn(NULL), birth(0), lastTs(0), currentDB("0")
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
			void ChangeCurrentDB(Channel* conn, const std::string& dbid);
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

	class ArdbServer;
	struct RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
	{
			ArdbServer* server;
			ArdbConnContext ardbctx;
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			RedisRequestHandler(ArdbServer* s) :
					server(s)
			{
			}
	};

	class ReplicationService;
	class OpLogs;
	class ArdbServer: public KeyWatcher
	{
		public:
			typedef int (ArdbServer::*RedisCommandHandler)(ArdbConnContext&,
					ArgumentArray&);

			struct RedisCommandHandlerSetting
			{
					const char* name;
					RedisCommandHandler handler;
					int min_arity;
					int max_arity;
					int read_write_cmd; //0:read 1:write 2:unknown
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
			ReplicationService m_repli_serv;
			SlaveClient m_slave_client;

			WatchKeyContextTable m_watch_context_table;
			ThreadMutex m_watch_mutex;
			PubSubContextTable m_pubsub_context_table;
			PubSubContextTable m_pattern_pubsub_context_table;
			ThreadMutex m_pubsub_mutex;
			//ArdbConnContext* m_current_ctx;
			ThreadLocal<ArdbConnContext*> m_ctx_local;

			RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(
					std::string& cmd);
			int DoRedisCommand(ArdbConnContext& ctx,
					RedisCommandHandlerSetting* setting,
					RedisCommandFrame& cmd);
			void ProcessRedisCommand(ArdbConnContext& ctx,
					RedisCommandFrame& cmd);

			void HandleReply(ArdbConnContext* ctx);

			friend class ReplicationService;
			friend class OpLogs;
			friend class RedisRequestHandler;
			friend class SlaveClient;

			int OnKeyUpdated(const DBID& dbid, const Slice& key);
			int OnAllKeyDeleted(const DBID& dbid);
			void ClearWatchKeys(ArdbConnContext& ctx);
			void ClearSubscribes(ArdbConnContext& ctx);

			int Time(ArdbConnContext& ctx, ArgumentArray& cmd);
			int FlushDB(ArdbConnContext& ctx, ArgumentArray& cmd);
			int FlushAll(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Save(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LastSave(ArdbConnContext& ctx, ArgumentArray& cmd);
			int BGSave(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Info(ArdbConnContext& ctx, ArgumentArray& cmd);
			int DBSize(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Config(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SlowLog(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Client(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Keys(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Multi(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Discard(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Exec(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Watch(ArdbConnContext& ctx, ArgumentArray& cmd);
			int UnWatch(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Subscribe(ArdbConnContext& ctx, ArgumentArray& cmd);
			int UnSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PUnSubscribe(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Publish(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Slaveof(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Sync(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ARSync(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ReplConf(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RawSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RawDel(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Ping(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Echo(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Select(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Quit(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Shutdown(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Type(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Move(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Rename(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RenameNX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Sort(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Append(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Bitcount(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Bitop(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Decr(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Decrby(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Get(ArdbConnContext& ctx, ArgumentArray& cmd);
			int GetBit(ArdbConnContext& ctx, ArgumentArray& cmd);
			int GetRange(ArdbConnContext& ctx, ArgumentArray& cmd);
			int GetSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Incr(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Incrby(ArdbConnContext& ctx, ArgumentArray& cmd);
			int IncrbyFloat(ArdbConnContext& ctx, ArgumentArray& cmd);
			int MGet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int MSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int MSetNX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PSetEX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SetBit(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SetEX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SetNX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SetRange(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Strlen(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Set(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Del(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Exists(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Expire(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Expireat(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Persist(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PExpire(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PExpireat(ArdbConnContext& ctx, ArgumentArray& cmd);
			int PTTL(ArdbConnContext& ctx, ArgumentArray& cmd);
			int TTL(ArdbConnContext& ctx, ArgumentArray& cmd);

			int HDel(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HExists(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HGet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HGetAll(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HIncrby(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HMIncrby(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HIncrbyFloat(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HKeys(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HLen(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HMGet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HMSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HSetNX(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HVals(ArdbConnContext& ctx, ArgumentArray& cmd);

			int SAdd(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SCard(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SDiff(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SDiffStore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SInter(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SInterStore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SIsMember(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SMembers(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SMove(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SPop(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SRandMember(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SRem(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SUnion(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SUnionStore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SUnionCount(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SInterCount(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SDiffCount(ArdbConnContext& ctx, ArgumentArray& cmd);

			int ZAdd(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZCard(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZCount(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZIncrby(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRange(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRank(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRem(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRemRangeByRank(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRemRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRevRange(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRevRangeByScore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZRevRank(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZInterStore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZUnionStore(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZScore(ArdbConnContext& ctx, ArgumentArray& cmd);

			int LIndex(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LInsert(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LLen(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LPop(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LPush(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LPushx(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LRange(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LRem(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LSet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LTrim(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RPop(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RPopLPush(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RPush(ArdbConnContext& ctx, ArgumentArray& cmd);
			int RPushx(ArdbConnContext& ctx, ArgumentArray& cmd);

			int HClear(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SClear(ArdbConnContext& ctx, ArgumentArray& cmd);
			int ZClear(ArdbConnContext& ctx, ArgumentArray& cmd);
			int LClear(ArdbConnContext& ctx, ArgumentArray& cmd);
			int TClear(ArdbConnContext& ctx, ArgumentArray& cmd);

			Timer& GetTimer();
		public:
			static int ParseConfig(const Properties& props,
					ArdbServerConfig& cfg);
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
