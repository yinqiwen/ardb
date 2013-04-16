/*
 * ardb_server.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef ARDB_SERVER_HPP_
#define ARDB_SERVER_HPP_
#include <string>
#include <tr1/unordered_map>
#include "channel/all_includes.hpp"
#include "ardb.hpp"
#include "util/config_helper.hpp"

using namespace ardb::codec;
namespace ardb
{
	struct ArdbServerConfig
	{
			bool daemonize;
			std::string listen_host;
			int64 listen_port;
			std::string listen_unix_path;
			int64 max_clients;
			std::string data_base_path;
			ArdbServerConfig() :
					daemonize(false), listen_port(0), max_clients(10000)
			{
			}
	};

	struct ArdbReply
	{
			int type;
			std::string str;
			int64_t integer;
			double double_value;
			std::deque<ArdbReply> elements;
			ArdbReply() :
					type(0), integer(0),double_value(0)
			{
			}
			void Clear()
			{
				type = 4;
				integer = 0;
				double_value = 0;
				str.clear();
				elements.clear();
			}
	};

	struct ArdbConnContext
	{
			DBID currentDB;
			Channel* conn;
			ArdbReply reply;
			ArdbConnContext() :
					currentDB("0"), conn(NULL)
			{
			}
	};

	class ArdbServer
	{
		private:
			ArdbServerConfig m_cfg;
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
			};
			typedef std::tr1::unordered_map<std::string, RedisCommandHandlerSetting> RedisCommandHandlerSettingTable;
			typedef std::tr1::unordered_map<uint32, RedisCommandHandlerSetting> RedisCommandHandlerSetting4BytesTable;
			typedef std::tr1::unordered_map<uint64, RedisCommandHandlerSetting> RedisCommandHandlerSetting8BytesTable;
			RedisCommandHandlerSettingTable m_handler_table;
			RedisCommandHandlerSetting4BytesTable m_4byte_handler_table;
			RedisCommandHandlerSetting8BytesTable m_8byte_handler_table;

			RedisCommandHandlerSetting* FindRedisCommandHandlerSetting(std::string* cmd);
			void ProcessRedisCommand(ArdbConnContext& ctx,
					RedisCommandFrame& cmd);
			int Ping(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Echo(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Select(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Quit(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Slaveof(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Shutdown(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Type(ArdbConnContext& ctx, ArgumentArray& cmd);

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
		public:
			static int ParseConfig(const Properties& props,
					ArdbServerConfig& cfg);
			ArdbServer();
			int Start(const Properties& props);
			~ArdbServer();
	};
}

#endif /* ARDB_SERVER_HPP_ */
