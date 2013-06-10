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
			int64 timeout;
			std::string data_base_path;
			int64 slowlog_log_slower_than;
			int64 slowlog_max_len;

			std::string repl_data_dir;
			std::string backup_dir;

			int64 repl_ping_slave_period;
			int64 repl_timeout;
			int64 repl_backlog_size;
			int64 repl_syncstate_persist_period;

			std::string master_host;
			uint32 master_port;

			DBIDSet syncdbs;

			bool repl_log_enable;
			int64 worker_count;
			std::string loglevel;
			std::string logfile;
			ArdbServerConfig() :
					daemonize(false), listen_port(0), unixsocketperm(755), max_clients(
							10000), tcp_keepalive(0), timeout(0), slowlog_log_slower_than(
							10000), slowlog_max_len(128), repl_data_dir(
							"./repl"), backup_dir("./backup"), repl_ping_slave_period(
							10), repl_timeout(60), repl_backlog_size(1000000), repl_syncstate_persist_period(
							1), master_port(0), repl_log_enable(true), worker_count(
							1), loglevel("INFO")
			{
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
			ArdbConnContext() :
					currentDB(0), conn(NULL), in_transaction(false), fail_transc(
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

	class ArdbServer;
	struct RedisRequestHandler: public ChannelUpstreamHandler<RedisCommandFrame>
	{
			ArdbServer* server;
			ArdbConnContext ardbctx;
			void MessageReceived(ChannelHandlerContext& ctx,
					MessageEvent<RedisCommandFrame>& e);
			void ChannelClosed(ChannelHandlerContext& ctx,
					ChannelStateEvent& e);
			void ChannelConnected(ChannelHandlerContext& ctx,
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
					RedisCommandFrame&);

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

			void TouchIdleConn(Channel* ch);

			int Time(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int FlushDB(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int FlushAll(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int Save(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int LastSave(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int BGSave(ArdbConnContext& ctx, RedisCommandFrame& cmd);
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
			int ARSync(ArdbConnContext& ctx, RedisCommandFrame& cmd);
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
			int TDesc(ArdbConnContext& ctx, RedisCommandFrame& cmd);

			int HClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int SClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int ZClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);
			int LClear(ArdbConnContext& ctx, RedisCommandFrame& cmd);

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
