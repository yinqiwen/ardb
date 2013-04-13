/*
 * ardb_server.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef ARDB_SERVER_HPP_
#define ARDB_SERVER_HPP_
#include <string>
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
			std::deque<ArdbReply> elements;
			ArdbReply() :
					type(0), integer(0)
			{
			}
			void Clear()
			{
				type = 0;
				integer = 0;
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
			typedef std::map<std::string, RedisCommandHandlerSetting> RedisCommandHandlerSettingTable;
			RedisCommandHandlerSettingTable m_handler_table;

			void ProcessRedisCommand(ArdbConnContext& ctx,
			        RedisCommandFrame& cmd);
			int Ping(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Echo(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Select(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Quit(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Slaveof(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Shutdown(ArdbConnContext& ctx, ArgumentArray& cmd);

			int Append(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Get(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Set(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Del(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Exists(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Expire(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Expireat(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Persist(ArdbConnContext& ctx, ArgumentArray& cmd);
		public:
			static int ParseConfig(const Properties& props,
			        ArdbServerConfig& cfg);
			ArdbServer();
			int Start(const Properties& props);
			~ArdbServer();
	};
}

#endif /* ARDB_SERVER_HPP_ */
