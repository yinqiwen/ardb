/*
 * ardb_server.hpp
 *
 *  Created on: 2013-3-27
 *      Author: wqy
 */

#ifndef ARDB_SERVER_HPP_
#define ARDB_SERVER_HPP_
#include <string>
#include "channel/channel_service.hpp"
#include "ardb.hpp"
namespace ardb
{
	class ARDBServer
	{
		private:
			ChannelService* m_server;
		public:
			ARDBServer();
			int Ping();
			int Echo(const std::string& message);
			int Select(DBID id);

			int Start();
	};
}

#endif /* ARDB_SERVER_HPP_ */
