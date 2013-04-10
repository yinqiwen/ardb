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

using namespace ardb::codec;
namespace ardb
{
	struct ArdbServerConfig
	{
			std::string listen_host;
			int64 listen_port;
			std::string listen_unix_path;
			int64 max_clients;
			std::string data_base_path;
			ArdbServerConfig() :
					listen_port(0), max_clients(10000)
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
					currentDB(0), conn(NULL)
			{
			}
	};

	class ArdbServer
	{
		private:
			ArdbServerConfig m_cfg;
			ChannelService* m_service;
			typedef int (ArdbServer::*RedisCommandHandler)(ArdbConnContext&,
					ArgumentArray&);

			struct RedisCommandHandlerSetting{
					const char* name;
					RedisCommandHandler handler;
					int arity;
			};
			typedef std::map<std::string, RedisCommandHandlerSetting> RedisCommandHandlerSettingTable;
			RedisCommandHandlerSettingTable m_handler_table;

			void ProcessRedisCommand(ArdbConnContext& ctx,
					RedisCommandFrame& cmd);
			int Ping(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Echo(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Select(ArdbConnContext& ctx, ArgumentArray& cmd);
			int Quit(ArdbConnContext& ctx, ArgumentArray& cmd);
		public:
			static int ParseConfig(const std::string& file, ArdbServerConfig& cfg);
			ArdbServer();
			int Start(const ArdbServerConfig& cfg);
			~ArdbServer();
	};
}

#endif /* ARDB_SERVER_HPP_ */
