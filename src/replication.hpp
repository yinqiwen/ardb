/*
 * replication.hpp
 *
 *  Created on: 2013-4-22
 *      Author: wqy
 */

#ifndef REPLICATION_HPP_
#define REPLICATION_HPP_
#include "channel/all_includes.hpp"
#include "ardb.hpp"

using namespace ardb::codec;

namespace ardb
{
	struct SlaveConn{
			Channel* conn;
			uint32 synced_checkpoint;
			uint32 synced_cmd_seq;
	};
    class ArdbServer;
	class ReplicationService:public Thread
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			bool m_in_checkpoint;
			void Run();
		public:
			ReplicationService(ArdbServer* serv);
			int CheckPoint();
			int WriteBinLog(Channel* sourceConn, RedisCommandFrame& cmd);
			int ServSync(Channel* conn, RedisCommandFrame& syncCmd);
			void SyncMaster(const std::string& host, int port);
			void HandleSyncedCommand(RedisCommandFrame& syncCmd);
	};
}

#endif /* REPLICATION_HPP_ */
