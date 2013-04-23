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
#include <stdio.h>

using namespace ardb::codec;

namespace ardb
{
	struct SlaveConn
	{
			Channel* conn;
			uint32 synced_checkpoint;
			uint32 synced_cmd_seq;
	};

	class BinLogWriter
	{
		private:
			FILE* m_index_log;
			FILE* m_bin_log;
	};

	class ArdbServer;
	class ReplicationService: public Thread
	{
		private:
			ChannelService m_serv;
			ArdbServer* m_server;
			bool m_in_backup;
			void Run();
			int Backup(const std::string& dstfile);
		public:
			ReplicationService(ArdbServer* serv);
			int Start();
			int CheckPoint();
			int WriteBinLog(Channel* sourceConn, RedisCommandFrame& cmd);
			int ServSync(Channel* conn, RedisCommandFrame& syncCmd);
			void SyncMaster(const std::string& host, int port);
			void HandleSyncedCommand(RedisCommandFrame& syncCmd);

	};
}

#endif /* REPLICATION_HPP_ */
