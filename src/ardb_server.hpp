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
#include "ardb.hpp"

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

			bool batch_write_enable;
			int64 batch_flush_period;
			std::string loglevel;
			std::string logfile;
			ArdbServerConfig() :
					daemonize(false), listen_port(0), unixsocketperm(755), max_clients(
							10000), tcp_keepalive(0), slowlog_log_slower_than(
							10000), slowlog_max_len(128), batch_write_enable(
							true), batch_flush_period(1)
			{
			}
	};

	struct ArdbConnContext
	{
			DBID currentDB;
			Channel* conn;
			RedisReply reply;
			ArdbConnContext() :
					currentDB("0"), conn(NULL)
			{
			}
	};

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
			bool m_stat_enable;
		public:
			ClientConnHolder() :
					m_stat_enable(false)
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
				return m_stat_enable;
			}
			void SetStatEnable(bool on)
			{
				m_stat_enable = on;
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

	class ArdbServer:public Runnable
	{
		private:
			ArdbServerConfig m_cfg;
			Properties m_cfg_props;
			ChannelService* m_service;
			Ardb* m_db;
			KeyValueEngineFactory* m_engine;
			typedef int (ArdbServer::*RedisCommandHandler)(ArdbConnContext&,
					ArgumentArray&);

			struct RedisCommandHandlerSetting
			{
					const char* name;
					RedisCommandHandler handler;
					int min_arity;
					int max_arity;
					int read_write_cmd;  //0:read 1:write 2:unknown
			};
			typedef btree::btree_map<std::string, RedisCommandHandlerSetting> RedisCommandHandlerSettingTable;
			typedef btree::btree_set<DBID> DBIDSet;
			RedisCommandHandlerSettingTable m_handler_table;
			SlowLogHandler m_slowlog_handler;
			ClientConnHolder m_clients_holder;

			DBIDSet m_period_batch_dbids;

			RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(
					std::string& cmd);
			void ProcessRedisCommand(ArdbConnContext& ctx,
					RedisCommandFrame& cmd);

			void Run();
			void BatchWriteFlush();
			void InsertBatchWriteDBID(const DBID& id);

			int Time(ArdbConnContext& ctx, ArgumentArray& cmd);
			int FlushDB(ArdbConnContext& ctx, ArgumentArray& cmd);
			int FlushAll(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Info(ArdbConnContext& ctx, ArgumentArray& cmd);
			int DBSize(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Config(ArdbConnContext& ctx, ArgumentArray& cmd);
			int SlowLog(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Client(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Keys(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Ping(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Echo(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Select(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Quit(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Slaveof(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Sync(ArdbConnContext& ctx, ArgumentArray& cmd);
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

			int HDel(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HExists(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HGet(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HGetAll(ArdbConnContext& ctx, ArgumentArray& cmd);
			int HIncrby(ArdbConnContext& ctx, ArgumentArray& cmd);
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
		public:
			static int ParseConfig(const Properties& props,
					ArdbServerConfig& cfg);
			ArdbServer();
			int Start(const Properties& props);
			~ArdbServer();
	};
}

#endif /* ARDB_SERVER_HPP_ */
