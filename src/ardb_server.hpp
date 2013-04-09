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
	class ARDBServer
	{
		private:
			ChannelService* m_server;
            void ProcessRedisCommand(Channel* conn, RedisCommandFrame& cmd);
		public:
			ARDBServer();
			int Ping();
			int Echo(const std::string& message);
			int Select(DBID id);
			int Start();
	};
}

#endif /* ARDB_SERVER_HPP_ */
